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

package fs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/poolpOrg/plakar"
	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/storage"

	"github.com/google/uuid"
	"github.com/iafan/cwalk"
)

type FSStore struct {
	config storage.StoreConfig

	Repository string
	root       string

	SkipDirs []string

	Ctx *plakar.Plakar

	storage.Store
}

type FSTransaction struct {
	Uuid     string
	store    *FSStore
	prepared bool

	SkipDirs []string

	storage.Transaction
}

func (store *FSStore) Create(config storage.StoreConfig) error {
	store.root = store.Repository

	err := os.Mkdir(store.root, 0700)
	if err != nil {
		return err
	}
	os.MkdirAll(fmt.Sprintf("%s/chunks", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/transactions", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/purge", store.root), 0700)

	f, err := os.Create(fmt.Sprintf("%s/CONFIG", store.root))
	if err != nil {
		return err
	}
	defer f.Close()

	jconfig, err := json.Marshal(config)
	if err != nil {
		return err
	}

	_, err = f.Write(compression.Deflate(jconfig))
	if err != nil {
		return err
	}

	return nil
}

func (store *FSStore) Init() {
	store.SkipDirs = append(store.SkipDirs, path.Clean(store.Repository))
	store.root = store.Repository
}

func (store *FSStore) Open() error {
	store.root = store.Repository

	compressed, err := ioutil.ReadFile(fmt.Sprintf("%s/CONFIG", store.root))
	if err != nil {
		return err
	}

	jconfig, err := compression.Inflate(compressed)
	if err != nil {
		return err
	}

	config := storage.StoreConfig{}
	err = json.Unmarshal(jconfig, &config)
	if err != nil {
		return err
	}

	store.config = config

	return nil
}

func (store *FSStore) Configuration() storage.StoreConfig {
	return store.config
}

func (store *FSStore) Context() *plakar.Plakar {
	return store.Ctx
}

func (store *FSStore) Transaction() storage.Transaction {
	tx := &FSTransaction{}
	tx.Uuid = uuid.New().String()
	tx.store = store
	tx.prepared = false
	tx.SkipDirs = store.SkipDirs
	return tx
}

func (store *FSStore) Snapshot(Uuid string) (*storage.Snapshot, error) {

	var index []byte
	cacheHit := false
	if store.Ctx.Cache != nil {
		tmp, err := store.Ctx.Cache.SnapshotGet(Uuid)
		if err == nil {
			cacheHit = true
		}
		index = tmp
	}

	if !cacheHit {
		tmp, err := store.IndexGet(Uuid)
		if err != nil {
			return nil, err
		}
		index = tmp
		if store.Ctx.Cache != nil {
			store.Ctx.Cache.SnapshotPut(Uuid, index)
		}
	}

	snapshot := storage.Snapshot{}
	return snapshot.FromBuffer(store, index)
}

func (store *FSStore) ObjectExists(checksum string) bool {
	return pathnameExists(store.PathObject(checksum))
}

func (store *FSStore) ChunkExists(checksum string) bool {
	return pathnameExists(store.PathChunk(checksum))
}

func (store *FSStore) Snapshots() ([]string, error) {
	ret := make([]string, 0)

	buckets, err := ioutil.ReadDir(store.PathSnapshots())
	if err != nil {
		return ret, nil
	}

	for _, bucket := range buckets {
		snapshots, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", store.PathSnapshots(), bucket.Name()))
		if err != nil {
			return ret, err
		}
		for _, snapshot := range snapshots {
			_, err = uuid.Parse(snapshot.Name())
			if err != nil {
				return ret, nil
			}
			ret = append(ret, snapshot.Name())
		}
	}
	return ret, nil

}

func (store *FSStore) IndexGet(Uuid string) ([]byte, error) {
	_, err := uuid.Parse(Uuid)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(fmt.Sprintf("%s/INDEX", store.PathSnapshot(Uuid)))
}

func (store *FSStore) ObjectGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(store.PathObject(checksum))
}

func (store *FSStore) ChunkGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(store.PathChunk(checksum))
}

func (store *FSStore) Purge(id string) error {
	dest := fmt.Sprintf("%s/%s", store.PathPurge(), id)
	err := os.Rename(store.PathSnapshot(id), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}

	store.Tidy()

	return nil
}

func (store *FSStore) Tidy() {
	cwalk.Walk(store.PathObjects(), func(path string, f os.FileInfo, err error) error {
		object := fmt.Sprintf("%s/%s", store.PathObjects(), path)
		if filepath.Clean(object) == filepath.Clean(store.PathObjects()) {
			return nil
		}
		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(object)
			}
		}
		return nil
	})

	cwalk.Walk(store.PathChunks(), func(path string, f os.FileInfo, err error) error {
		chunk := fmt.Sprintf("%s/%s", store.PathChunks(), path)
		if filepath.Clean(chunk) == filepath.Clean(store.PathChunks()) {
			return nil
		}

		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(chunk)
			}
		}
		return nil
	})
}

func pathnameExists(pathname string) bool {
	_, err := os.Stat(pathname)
	return !os.IsNotExist(err)
}

func (transaction *FSTransaction) prepare() {
	os.MkdirAll(transaction.store.root, 0700)
	os.MkdirAll(fmt.Sprintf("%s/%s", transaction.store.PathTransactions(),
		transaction.Uuid[0:2]), 0700)
	os.MkdirAll(transaction.Path(), 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", transaction.Path()), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", transaction.Path()), 0700)
}

func (transaction *FSTransaction) Snapshot() *storage.Snapshot {
	return &storage.Snapshot{
		Uuid:         transaction.Uuid,
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Hostname:     transaction.store.Ctx.Hostname,
		Username:     transaction.store.Ctx.Username,
		Directories:  make(map[string]*storage.FileInfo),
		Files:        make(map[string]*storage.FileInfo),
		NonRegular:   make(map[string]*storage.FileInfo),
		Sums:         make(map[string]string),
		Objects:      make(map[string]*storage.Object),
		Chunks:       make(map[string]*storage.Chunk),

		BackingTransaction: transaction,
		BackingStore:       transaction.store,
		SkipDirs:           transaction.SkipDirs,

		WrittenChunks:  make(map[string]bool),
		InflightChunks: make(map[string]*storage.Chunk),

		WrittenObjects:  make(map[string]bool),
		InflightObjects: make(map[string]*storage.Object),
	}
}

func (transaction *FSTransaction) ObjectsCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ObjectExists(key)
	}

	return ret
}

func (transaction *FSTransaction) ChunksMark(keys []string) []bool {
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := make([]bool, 0)
	for _, key := range keys {
		os.Mkdir(transaction.PathChunkBucket(key), 0700)
		err := os.Link(transaction.store.PathChunk(key), transaction.PathChunk(key))
		if err != nil {
			if os.IsNotExist(err) {
				ret = append(ret, false)
			} else {
				ret = append(ret, true)
			}
		} else {
			ret = append(ret, true)
		}
	}

	return ret
}

func (transaction *FSTransaction) ChunksCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ChunkExists(key)
	}

	return ret
}

func (transaction *FSTransaction) ObjectsMark(keys []string) []bool {
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := make([]bool, 0)
	for _, key := range keys {
		os.Mkdir(transaction.PathObjectBucket(key), 0700)
		err := os.Link(transaction.store.PathObject(key), transaction.PathObject(key))
		if err != nil {
			if os.IsNotExist(err) {
				ret = append(ret, false)
			} else {
				ret = append(ret, true)
			}
		} else {
			ret = append(ret, true)
		}
	}

	return ret
}

func (transaction *FSTransaction) ObjectRecord(checksum string, buf string) (bool, error) {
	if !transaction.prepared {
		transaction.prepare()
	}
	err := error(nil)
	recorded := false
	if transaction.ChunkExists(checksum) {
		err = transaction.ObjectLink(checksum)
	} else {
		err = transaction.ObjectPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (transaction *FSTransaction) ObjectPut(checksum string, buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
	f, err := os.Create(transaction.PathObject(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) ObjectLink(checksum string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
	os.Link(transaction.store.PathObject(checksum), transaction.PathObject(checksum))
	return nil
}

func (transaction *FSTransaction) ChunkRecord(checksum string, buf string) (bool, error) {
	if !transaction.prepared {
		transaction.prepare()
	}
	err := error(nil)
	recorded := false
	if transaction.ChunkExists(checksum) {
		err = transaction.ChunkLink(checksum)
	} else {
		err = transaction.ChunkPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (transaction *FSTransaction) ChunkPut(checksum string, buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
	f, err := os.Create(transaction.PathChunk(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) ChunkExists(checksum string) bool {
	return transaction.store.ChunkExists(checksum)
}

func (transaction *FSTransaction) ChunkLink(checksum string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
	os.Link(transaction.store.PathChunk(checksum), transaction.PathChunk(checksum))
	return nil
}

func (transaction *FSTransaction) IndexPut(buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	f, err := os.Create(fmt.Sprintf("%s/INDEX", transaction.Path()))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) Commit(snapshot *storage.Snapshot) (*storage.Snapshot, error) {
	if !transaction.prepared {
		transaction.prepare()
	}

	var wg sync.WaitGroup

	// first pass, link chunks to store
	parallelChunksMax := make(chan int, 64)
	for chunk := range snapshot.Chunks {
		parallelChunksMax <- 1
		wg.Add(1)
		go func(chunk string) {
			if !transaction.store.ChunkExists(chunk) {
				os.Mkdir(transaction.store.PathChunkBucket(chunk), 0700)
				os.Rename(transaction.PathChunk(chunk), transaction.store.PathChunk(chunk))
			} else {
				os.Remove(transaction.PathChunk(chunk))
			}
			os.Link(transaction.store.PathChunk(chunk), transaction.PathChunk(chunk))
			<-parallelChunksMax
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	// second pass, link objects to store
	parallelObjectsMax := make(chan int, 64)
	for object := range snapshot.Objects {
		parallelObjectsMax <- 1
		wg.Add(1)
		go func(object string) {
			if !transaction.store.ObjectExists(object) {
				os.Mkdir(transaction.store.PathObjectBucket(object), 0700)
				os.Rename(transaction.PathObject(object), transaction.store.PathObject(object))
			} else {
				os.Remove(transaction.PathObject(object))
			}
			os.Link(transaction.store.PathObject(object), transaction.PathObject(object))
			<-parallelObjectsMax
			wg.Done()
		}(object)
	}
	wg.Wait()

	// final pass, move snapshot to store
	os.Mkdir(transaction.store.PathSnapshotBucket(snapshot.Uuid), 0700)
	os.Rename(transaction.Path(), transaction.store.PathSnapshot(snapshot.Uuid))

	return snapshot, nil
}
