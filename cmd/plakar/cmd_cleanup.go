/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/PlakarLabs/plakar/packfile"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/storage/locking"
)

func init() {
	registerCommand("cleanup", cmd_cleanup)
}

func cmd_cleanup(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("cleanup", flag.ExitOnError)
	flags.Parse(args)

	lock := locking.New(ctx.Hostname,
		ctx.Username,
		ctx.MachineID,
		os.Getpid(),
		true)
	currentLockID, err := snapshot.PutLock(*repository, lock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}
	defer repository.DeleteLock(currentLockID)

	locksID, err := repository.GetLocks()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	for _, lockID := range locksID {
		if lockID == currentLockID {
			continue
		}
		if lock, err := snapshot.GetLock(repository, lockID); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		} else if err == nil {
			if !lock.Expired(time.Minute * 15) {
				fmt.Fprintf(os.Stderr, "can't put exclusive lock: %s has ongoing operations\n", repository.Location)
				return 1
			}
		}
	}

	// the cleanup algorithm is a bit tricky and needs to be done in the correct sequence,
	// here's what it has to do:
	//
	// 1. fetch all snapshot indexes to figure out which blobs, objects and chunks are used
	chunks := make(map[[32]byte]uint32)
	objects := make(map[[32]byte]uint32)
	blobs := make(map[[32]byte]struct{})

	snapshotIDs, err := repository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}
	for _, snapshotID := range snapshotIDs {
		hdr, _, err := snapshot.GetSnapshot(repository, snapshotID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		}

		idx, _, err := snapshot.GetIndex(repository, hdr.Index[0].Checksum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		}

		for offset, _ := range idx.ChunksList {
			chunks[idx.ChunksChecksumList[offset]] = uint32(offset)
		}

		for offset, _ := range idx.ObjectsList {
			objects[idx.ObjectsChecksumList[offset]] = uint32(offset)
		}

		for _, blob := range hdr.Index {
			blobs[blob.Checksum] = struct{}{}
		}

		for _, blob := range hdr.VFS {
			blobs[blob.Checksum] = struct{}{}
		}

		for _, blob := range hdr.Metadata {
			blobs[blob.Checksum] = struct{}{}
		}

	}

	// list anything contained in the repository index to try matching with the above
	// THIS IS TRICKY: we can't just remove chunks and objects, we need to identify
	// their packfile and determione if a packfile can be removed or not.
	// a chunk MAY be part of multiple packfiles !
	// an object MAY be part of multiple packfiles !
	// a packfile MAY contain multiple chunks and objects from multiple snapshots !
	// LOTS OF FUN.
	//
	// The algorithm to generate the packfile may be optimized to favour locality
	// of chunks and objects. It already does it through a naive approach, but
	// it can be improved.
	//

	packfiles := make(map[[32]byte]struct{})

	repoIndex := repository.GetRepositoryIndex()
	for checksum, checksumID := range repoIndex.Checksums {
		if _, ok := repoIndex.Chunks[checksumID]; ok {
			if _, ok := chunks[checksum]; !ok {
				if packfileChecksum, exists := repoIndex.GetPackfileForChunk(checksum); exists {
					packfiles[packfileChecksum] = struct{}{}
				}
			}
		}
		if _, ok := repoIndex.Objects[checksumID]; ok {
			if _, ok := objects[checksum]; !ok {
				if packfileChecksum, exists := repoIndex.GetPackfileForObject(checksum); exists {
					packfiles[packfileChecksum] = struct{}{}
				}
			}
		}
	}

	for packfileChecksum, _ := range packfiles {
		if packfileData, err := repository.GetPackfile(packfileChecksum); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		} else {
			if p, err := packfile.NewFromBytes(packfileData); err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				return 1
			} else {
				shouldDelete := true
				for _, pc := range p.Index {
					if pc.DataType == packfile.TYPE_CHUNK {
						if _, ok := chunks[pc.Checksum]; ok {
							shouldDelete = false
							break
						}
					}
					if pc.DataType == packfile.TYPE_OBJECT {
						if _, ok := objects[pc.Checksum]; ok {
							shouldDelete = false
							break
						}
					}
				}
				if shouldDelete {
					fmt.Println("delete packfile", packfileChecksum)

					// XXX
					// we need to delete the chunks and objects from repoIndex
					// and save it !!!!
					for _, chunkID := range chunks {
						delete(repoIndex.Chunks, chunkID)
					}
					for _, objectID := range objects {
						delete(repoIndex.Objects, objectID)
					}
					for checksum, checksumID := range repoIndex.Checksums {
						if _, ok := repoIndex.Chunks[checksumID]; !ok {
							if _, ok := repoIndex.Objects[checksumID]; !ok {
								delete(repoIndex.Checksums, checksum)
							}
						}
					}

					repository.SetRepositoryIndex(repoIndex)
					repository.DeletePackfile(packfileChecksum)
				}
			}
		}
	}

	// 2. blobs that are no longer in use can be be removed
	blobChecksums, err := repository.GetBlobs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}
	for _, blobChecksum := range blobChecksums {
		if _, ok := blobs[blobChecksum]; !ok {
			// ignore error, best effort, not a big deal if it fails
			repository.DeleteBlob(blobChecksum)
		}
	}

	// 6. update indexes to reflect the new packfile
	// 7. save the new index

	return 0
}
