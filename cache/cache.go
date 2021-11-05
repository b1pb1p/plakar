package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Cache struct {
	conn *sql.DB

	mu_snapshots sync.Mutex
	snapshots    map[string][]byte

	mu_pathnames sync.Mutex
	pathnames    map[string][]byte
}

func New(cacheDir string) *Cache {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s/cache.db", cacheDir))
	if err != nil {
		log.Fatal(err)
	}

	cache := &Cache{}
	cache.conn = conn
	cache.snapshots = make(map[string][]byte)
	cache.pathnames = make(map[string][]byte)

	statement, err := conn.Prepare(`CREATE TABLE IF NOT EXISTS snapshots (
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement2, err := conn.Prepare(`CREATE TABLE IF NOT EXISTS pathnames (
		"checksum"	VARCHAR NOT NULL PRIMARY KEY,
		"blob"		BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement2.Close()
	statement2.Exec()

	return cache
}

func (cache *Cache) PutPath(checksum string, data []byte) error {
	cache.mu_pathnames.Lock()
	cache.pathnames[checksum] = data
	cache.mu_pathnames.Unlock()
	return nil
}

func (cache *Cache) GetPath(checksum string) ([]byte, error) {
	cache.mu_pathnames.Lock()
	ret, exists := cache.pathnames[checksum]
	cache.mu_pathnames.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM pathnames WHERE checksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutSnapshot(checksum string, data []byte) error {
	cache.mu_snapshots.Lock()
	cache.snapshots[checksum] = data
	cache.mu_snapshots.Unlock()
	return nil
}

func (cache *Cache) GetSnapshot(Uuid string) ([]byte, error) {
	cache.mu_snapshots.Lock()
	ret, exists := cache.snapshots[Uuid]
	cache.mu_snapshots.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM snapshots WHERE uuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) Commit() error {
	// XXX - to handle parallel use, New() needs to open a read-only version of the database
	// and Commit needs to re-open for writes so that cache.db is not locked for too long.
	//

	statement, err := cache.conn.Prepare(`INSERT OR REPLACE INTO pathnames("checksum", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for checksum, data := range cache.pathnames {
		statement.Exec(checksum, data)
	}
	statement.Close()

	statement, err = cache.conn.Prepare(`INSERT OR REPLACE INTO snapshots("uuid", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for checksum, data := range cache.snapshots {
		statement.Exec(checksum, data)
	}
	statement.Close()

	cache.conn.Close()

	return nil
}
