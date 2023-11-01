package metadata

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/profiler"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION string = "0.0.1"

type Item struct {
	Category uint32
	Key      uint32
	Value    uint32
}

type Metadata struct {
	muChecksums   sync.Mutex
	checksumsMap  map[[32]byte]uint32
	ChecksumsList [][32]byte

	muStrings   sync.Mutex
	stringsMap  map[string]uint32
	StringsList []string

	muItems   sync.Mutex
	itemsMap  map[Item]uint32
	ItemsList []Item
}

func New() *Metadata {
	return &Metadata{
		checksumsMap:  make(map[[32]byte]uint32),
		ChecksumsList: make([][32]byte, 0),

		stringsMap:  make(map[string]uint32),
		StringsList: make([]string, 0),

		itemsMap:  make(map[Item]uint32),
		ItemsList: make([]Item, 0),
	}
}

func NewFromBytes(serialized []byte) (*Metadata, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("md.NewFromBytes", time.Since(t0))
		logger.Trace("metadata", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var md Metadata
	if err := msgpack.Unmarshal(serialized, &md); err != nil {
		return nil, err
	}

	md.checksumsMap = make(map[[32]byte]uint32)
	for checksumID, checksum := range md.ChecksumsList {
		//		fmt.Printf("deserialize checksumsMap: %d = %016x\n", checksumID, checksum)
		md.checksumsMap[checksum] = uint32(checksumID)
	}

	md.stringsMap = make(map[string]uint32)
	for stringID, str := range md.StringsList {
		//		fmt.Printf("deserialize stringsMap: %d = %s\n", stringID, str)
		md.stringsMap[str] = uint32(stringID)
	}

	md.itemsMap = make(map[Item]uint32)
	for offset, item := range md.ItemsList {
		//fmt.Println("deserialize", offset, item)
		md.itemsMap[item] = uint32(offset)
	}

	return &md, nil
}

func (md *Metadata) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("md.Serialize", time.Since(t0))
		logger.Trace("metadata", "Serialize(): %s", time.Since(t0))
	}()

	newMd := &Metadata{
		checksumsMap:  make(map[[32]byte]uint32),
		ChecksumsList: make([][32]byte, len(md.ChecksumsList)),

		stringsMap:  make(map[string]uint32),
		StringsList: make([]string, len(md.StringsList)),

		itemsMap:  make(map[Item]uint32),
		ItemsList: make([]Item, len(md.ItemsList)),
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newMd.ChecksumsList, md.ChecksumsList)
		sort.Slice(newMd.ChecksumsList, func(i, j int) bool {
			return bytes.Compare(newMd.ChecksumsList[i][:], newMd.ChecksumsList[j][:]) < 0
		})
		for offset, checksum := range newMd.ChecksumsList {
			//			fmt.Printf("serialize checksumsMap: %d = %016x\n", offset, checksum)
			newMd.checksumsMap[checksum] = uint32(offset)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newMd.StringsList, md.StringsList)
		sort.Slice(newMd.StringsList, func(i, j int) bool {
			return newMd.StringsList[i] < newMd.StringsList[j]
		})
		for offset, str := range newMd.StringsList {
			//	fmt.Printf("serialize stringsMap: %d = %s\n", offset, str)
			newMd.stringsMap[str] = uint32(offset)
		}
	}()
	wg.Wait()

	for offset, item := range md.ItemsList {
		newMd.ItemsList[offset] = Item{
			Category: newMd.stringsMap[md.StringsList[item.Category]],
			Key:      newMd.stringsMap[md.StringsList[item.Key]],
			Value:    newMd.checksumsMap[md.ChecksumsList[item.Value]],
		}
	}
	sort.Slice(newMd.ItemsList, func(i, j int) bool {
		if newMd.ItemsList[i].Category < newMd.ItemsList[j].Category {
			return true
		}
		if newMd.ItemsList[i].Key < newMd.ItemsList[j].Key {
			return true
		}
		return newMd.ItemsList[i].Value < newMd.ItemsList[j].Value
	})
	for offset, value := range newMd.ItemsList {
		fmt.Println("serializing", offset, value)
		newMd.itemsMap[value] = uint32(offset)
	}

	serialized, err := msgpack.Marshal(newMd)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// checksums
func (md *Metadata) addChecksum(checksum [32]byte) uint32 {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.checksumsMap[checksum]; !exists {
		checksumID = uint32(len(md.ChecksumsList))
		md.ChecksumsList = append(md.ChecksumsList, checksum)
		md.checksumsMap[checksum] = checksumID
		return checksumID
	} else {
		return checksumID
	}
}

func (md *Metadata) lookupChecksum(checksum [32]byte) (uint32, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.checksumsMap[checksum]; !exists {
		return checksumID, false
	} else {
		return checksumID, true
	}
}

func (md *Metadata) lookupChecksumID(checksumID uint32) ([32]byte, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if int(checksumID) >= len(md.ChecksumsList) {
		return [32]byte{}, false
	}

	return md.ChecksumsList[int(checksumID)], true
}

// strings
func (md *Metadata) addString(value string) (uint32, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if checksumID, exists := md.stringsMap[value]; !exists {
		checksumID = uint32(len(md.stringsMap))
		md.StringsList = append(md.StringsList, value)
		md.stringsMap[value] = checksumID
		return checksumID, true
	} else {
		return checksumID, false
	}
}

func (md *Metadata) lookupString(stringID uint32) (string, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if int(stringID) >= len(md.StringsList) {
		return "", false
	}
	return md.StringsList[stringID], true
}

// md
func (md *Metadata) AddMetadata(mdType string, mdKey string, value [32]byte) {
	mdTypeID, _ := md.addString(mdType)
	mdKeyID, _ := md.addString(mdKey)
	externalID := md.addChecksum(value)

	item := Item{
		Category: mdTypeID,
		Key:      mdKeyID,
		Value:    externalID,
	}

	md.muItems.Lock()
	if _, exists := md.itemsMap[item]; !exists {
		itemID := uint32(len(md.ItemsList))
		md.ItemsList = append(md.ItemsList, item)
		md.itemsMap[item] = itemID
	}
	md.muItems.Unlock()
}

func (md *Metadata) ListKeys(mdType string) []string {
	mdTypeID, _ := md.addString(mdType)

	ret := make([]string, 0)
	md.muItems.Lock()
	for _, item := range md.ItemsList {
		if item.Category == mdTypeID {
			key, _ := md.lookupString(item.Key)
			ret = append(ret, key)
		}
	}
	md.muItems.Unlock()
	return ret
}

func (md *Metadata) ListValues(mdType string, mdKey string) [][32]byte {
	mdTypeID, _ := md.addString(mdType)
	mdKeyID, _ := md.addString(mdKey)

	ret := make([][32]byte, 0)
	md.muItems.Lock()
	for _, item := range md.ItemsList {
		if item.Category == mdTypeID && item.Key == mdKeyID {
			value, _ := md.lookupChecksumID(item.Value)
			ret = append(ret, value)
		}
	}
	md.muItems.Unlock()
	return ret
}

func (md *Metadata) LookupKeyForValue(mdType string, value [32]byte) (string, bool) {
	mdTypeID, _ := md.addString(mdType)

	valueId, exists := md.lookupChecksum(value)
	if !exists {
		return "", false
	}

	md.muItems.Lock()
	defer md.muItems.Unlock()
	for _, item := range md.ItemsList {
		if item.Category == mdTypeID && item.Value == valueId {
			return md.StringsList[item.Key], true
		}
	}
	return "", false
}
