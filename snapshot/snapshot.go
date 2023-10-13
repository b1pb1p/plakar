package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/ebfe/signify"
	"fmt"
	"time"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/index"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/metadata"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Snapshot struct {
	repository  *storage.Repository
	transaction *storage.Transaction

	privateKey *signify.PrivateKey

	SkipDirs []string

	Metadata   *metadata.Metadata
	Index      *index.Index
	Filesystem *vfs.Filesystem
}

func New(repository *storage.Repository, indexID uuid.UUID, privateKey *signify.PrivateKey, publicKey *signify.PublicKey) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Create", time.Since(t0))
	}()

	tx, err := repository.Transaction(indexID)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository:  repository,
		transaction: tx,

		privateKey: privateKey,
		Metadata:   metadata.NewMetadata(indexID, publicKey),
		Index:      index.NewIndex(),
		Filesystem: vfs.NewFilesystem(),
	}

	logger.Trace("snapshot", "%s: New()", snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func Load(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Load", time.Since(t0))
	}()

	metadata, metadataChecksum, _, err := GetMetadata(repository, indexID)
	if err != nil {
		return nil, err
	}

	if metadata.PublicKey != "" {
		signature, _, err := GetSignature(repository, indexID)
		if err != nil {
			return nil, fmt.Errorf("signed snapshot but couldn't load signature")
		}

		encodedKey, err := base64.RawStdEncoding.DecodeString(metadata.PublicKey)
		if err != nil {
			return nil, err
		}
		publicKey, err := signify.ParsePublicKey(encodedKey)
		if err != nil {
			return nil, err
		}
		if !signify.Verify(publicKey, metadataChecksum, signature) {
			return nil, fmt.Errorf("snapshot signature mismatch")
		}
	}

	index, checksum, err := GetIndex(repository, indexID)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(checksum, metadata.IndexChecksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	filesystem, checksum, err := GetFilesystem(repository, indexID)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(checksum, metadata.FilesystemChecksum) {
		return nil, fmt.Errorf("filesystem mismatches metadata checksum")
	}

	snapshot := &Snapshot{}
	snapshot.repository = repository
	snapshot.Metadata = metadata
	snapshot.Index = index
	snapshot.Filesystem = filesystem

	logger.Trace("snapshot", "%s: Load()", snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func Fork(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Fork", time.Since(t0))
	}()

	metadata, _, _, err := GetMetadata(repository, indexID)
	if err != nil {
		return nil, err
	}

	index, checksum, err := GetIndex(repository, indexID)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(checksum, metadata.IndexChecksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	filesystem, checksum, err := GetFilesystem(repository, indexID)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(checksum, metadata.FilesystemChecksum) {
		return nil, fmt.Errorf("filesystem mismatches metadata checksum")
	}

	tx, err := repository.Transaction(uuid.Must(uuid.NewRandom()))
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository:  repository,
		transaction: tx,

		Metadata:   metadata,
		Index:      index,
		Filesystem: filesystem,
	}
	snapshot.Metadata.IndexID = tx.GetUuid()

	logger.Trace("snapshot", "%s: Fork(): %s", indexID, snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func GetSignature(repository *storage.Repository, indexID uuid.UUID) (*signify.Signature, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetSignature", time.Since(t0))
	}()

	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot", "cache.GetSignature(%s)", indexID)
		tmp, err := cache.GetSignature(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetSignature(%s)", indexID)
			tmp, err = repository.GetSignature(indexID)
			if err != nil {
				return nil, false, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetSignature(%s)", indexID)
		tmp, err := repository.GetSignature(indexID)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		logger.Trace("snapshot", "cache.PutSignature(%s)", indexID)
		cache.PutSignature(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

<<<<<<< HEAD
	secret := repository.GetSecret()
	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	sig, err := signify.ParseSignature(buffer)
=======
	metadata, err := metadata.NewMetadataFromBytes(buffer)
>>>>>>> main
	if err != nil {
		return nil, false, err
	}
	return sig, false, nil
}

func GetMetadata(repository *storage.Repository, indexID uuid.UUID) (*metadata.Metadata, []byte, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetMetada", time.Since(t0))
	}()

	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot", "cache.GetMetadata(%s)", indexID)
		tmp, err := cache.GetMetadata(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetMetadata(%s)", indexID)
			tmp, err = repository.GetMetadata(indexID)
			if err != nil {
				return nil, nil, false, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetMetadata(%s)", indexID)
		tmp, err := repository.GetMetadata(indexID)
		if err != nil {
			return nil, nil, false, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		logger.Trace("snapshot", "cache.PutMetadata(%s)", indexID)
		cache.PutMetadata(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	secret := repository.GetSecret()
	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, nil, false, err
		}
		buffer = tmp
	}

	if repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, nil, false, err
		}
		buffer = tmp
	}

	metadata, err := metadata.NewMetadataFromBytes(buffer)
	if err != nil {
		return nil, nil, false, err
	}

	checksum := sha256.Sum256(buffer)

	return metadata, checksum[:], false, nil
}

func GetIndex(repository *storage.Repository, indexID uuid.UUID) (*index.Index, []byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetIndex", time.Since(t0))
	}()
	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot", "cache.GetIndex(%s)", indexID)
		tmp, err := cache.GetIndex(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetIndex(%s)", indexID)
			tmp, err = repository.GetIndex(indexID)
			if err != nil {
				return nil, nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetIndex(%s)", indexID)
		tmp, err := repository.GetIndex(indexID)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		logger.Trace("snapshot", "cache.PutIndex(%s)", indexID)
		cache.PutIndex(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	index, err := index.NewIndexFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	indexHasher := encryption.GetHasher(repository.Configuration().Hashing)
	indexHasher.Write(buffer)
	checksum := indexHasher.Sum(nil)

	return index, checksum[:], nil
}

func GetFilesystem(repository *storage.Repository, indexID uuid.UUID) (*vfs.Filesystem, []byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetFilesystem", time.Since(t0))
	}()
	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot", "cache.GetFilesystem(%s)", indexID)
		tmp, err := cache.GetFilesystem(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetFilesystem(%s)", indexID)
			tmp, err = repository.GetFilesystem(indexID)
			if err != nil {
				return nil, nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetFilesystem(%s)", indexID)
		tmp, err := repository.GetFilesystem(indexID)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		logger.Trace("snapshot", "cache.PutFilesystem(%s)", indexID)
		cache.PutFilesystem(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	filesystem, err := vfs.NewFilesystemFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	fsHasher := encryption.GetHasher(repository.Configuration().Hashing)
	fsHasher.Write(buffer)
	checksum := fsHasher.Sum(nil)

	return filesystem, checksum[:], nil
}

func List(repository *storage.Repository) ([]uuid.UUID, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.List", time.Since(t0))
	}()
	return repository.GetIndexes()
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutChunk", time.Since(t0))
	}()

	logger.Trace("snapshot", "%s: PutChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)
	return snapshot.repository.PutChunk(checksum, data)
}

func (snapshot *Snapshot) Repository() *storage.Repository {
	return snapshot.repository
}

func (snapshot *Snapshot) PutObject(object *objects.Object) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: PutObject(%064x)", snapshot.Metadata.GetIndexShortID(), object.Checksum)

	data, err := msgpack.Marshal(object)
	if err != nil {
		return 0, err
	}
	return snapshot.repository.PutObject(object.Checksum, data)
}

func (snapshot *Snapshot) PutSignature(data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutSignature", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutSignature()", snapshot.Metadata.GetIndexShortID())
	secret := snapshot.repository.GetSecret()

	buffer := data

	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	if cache != nil {
		cache.PutSignature(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.GetIndexID().String(), buffer)
	}

	err := snapshot.transaction.PutSignature(buffer)
	if err != nil {
		return 0, err
	}

	return len(buffer), nil
}

func (snapshot *Snapshot) PutMetadata(data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutMetadata", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutMetadata()", snapshot.Metadata.GetIndexShortID())

	if cache != nil {
		cache.PutMetadata(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.GetIndexID().String(), data)
	}

	return snapshot.transaction.PutMetadata(data)
}

func (snapshot *Snapshot) PutIndex(data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutIndex", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutIndex()", snapshot.Metadata.GetIndexShortID())

	if cache != nil {
		cache.PutIndex(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.GetIndexID().String(), data)
	}

	return snapshot.transaction.PutIndex(data)
}

func (snapshot *Snapshot) PutFilesystem(data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutFilesystem", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutFilesystem()", snapshot.Metadata.GetIndexShortID())

	if cache != nil {
		cache.PutFilesystem(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.GetIndexID().String(), data)
	}

	return snapshot.transaction.PutFilesystem(data)
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: GetChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)

	return snapshot.repository.GetChunk(checksum)
}

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) (bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)
	exists, err := snapshot.repository.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) GetObject(checksum [32]byte) (*objects.Object, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: GetObject(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)

	buffer, err := snapshot.repository.GetObject(checksum)
	if err != nil {
		return nil, err
	}

	object := &objects.Object{}
	err = msgpack.Unmarshal(buffer, &object)
	return object, err
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) (bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckObject(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)
	return snapshot.repository.CheckObject(checksum)
}

func (snapshot *Snapshot) Commit() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Commit", time.Since(t0))
	}()

	serializedIndex, err := snapshot.Index.Serialize()
	if err != nil {
		return err
	}
	nbytes, err := snapshot.PutIndex(serializedIndex)
	if err != nil {
		return err
	}

	indexHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	indexHasher.Write(serializedIndex)
	indexChecksum := indexHasher.Sum(nil)

	snapshot.Metadata.IndexChecksum = indexChecksum[:]
	snapshot.Metadata.IndexMemorySize = uint64(len(serializedIndex))
	snapshot.Metadata.IndexDiskSize = uint64(nbytes)

	serializedFilesystem, err := snapshot.Filesystem.Serialize()
	if err != nil {
		return err
	}
	nbytes, err = snapshot.PutFilesystem(serializedFilesystem)
	if err != nil {
		return err
	}

	fsHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	fsHasher.Write(serializedFilesystem)
	filesystemChecksum := fsHasher.Sum(nil)

	snapshot.Metadata.FilesystemChecksum = filesystemChecksum[:]
	snapshot.Metadata.FilesystemMemorySize = uint64(len(serializedFilesystem))
	snapshot.Metadata.FilesystemDiskSize = uint64(nbytes)

	serializedMetadata, err := snapshot.Metadata.Serialize()
	if err != nil {
		return err
	}
	_, err = snapshot.PutMetadata(serializedMetadata)
	if err != nil {
		return err
	}

	if snapshot.privateKey != nil {
		metadataChecksum := sha256.Sum256(serializedMetadata)
		fmt.Printf("IN PUT CHECKSUM: %x\n", metadataChecksum)
		signature := signify.Sign(snapshot.privateKey, metadataChecksum[:])
		_, err := snapshot.PutSignature(signify.MarshalSignature(signature))
		if err != nil {
			return err
		}
	}

	logger.Trace("snapshot", "%s: Commit()", snapshot.Metadata.GetIndexShortID())
	return snapshot.transaction.Commit()
}

func (snapshot *Snapshot) NewReader(pathname string) (*storage.Reader, error) {
	return snapshot.repository.NewReader(snapshot.Index, pathname)
}
