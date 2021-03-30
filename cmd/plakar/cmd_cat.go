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
	"log"
	"os"
	"strings"

	"github.com/poolpOrg/plakar"
	"github.com/poolpOrg/plakar/repository"
)

func cmd_cat(ctx plakar.Plakar, store repository.Store, args []string) {
	if len(args) == 0 {
		log.Fatalf("%s: need at least one snapshot ID and file or object to cat", flag.CommandLine.Name())
	}

	snapshots, err := store.Snapshots()
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
		if pattern == "" {
			log.Fatalf("%s: missing filename.", flag.CommandLine.Name())
		}

		if !strings.HasPrefix(pattern, "/") {
			objects := make([]string, 0)
			snapshot, err := store.Snapshot(res[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
			}
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			if len(res) == 0 {
				log.Fatalf("%s: no object has prefix: %s", flag.CommandLine.Name(), pattern)
			} else if len(res) > 1 {
				log.Fatalf("%s: object ID is ambigous: %s (matches %d objects)", flag.CommandLine.Name(), pattern, len(res))
			}
		}
	}

	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := store.Snapshot(res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		var checksum string
		if strings.HasPrefix(pattern, "/") {
			tmp, ok := snapshot.Sums[pattern]
			if !ok {
				log.Fatalf("%s: %s: no such file in snapshot.", flag.CommandLine.Name(), pattern)
			}
			checksum = tmp
		} else {
			objects := make([]string, 0)
			snapshot, err := store.Snapshot(res[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
			}
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			checksum = res[0]
		}

		object, err := snapshot.ObjectGet(checksum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open object", flag.CommandLine.Name())
			continue
		}

		for _, chunk := range object.Chunks {
			data, err := snapshot.ChunkGet(chunk.Checksum)
			if err != nil {
				log.Fatalf("%s: %s: failed to obtain chunk %s.", flag.CommandLine.Name(), pattern, chunk.Checksum)
			}
			os.Stdout.Write(data)
		}
	}
}
