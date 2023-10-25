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

package ui

import (
	_ "embed"
	"fmt"
	"html/template"
	"math"
	"math/rand"
	"mime"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/header"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

var lrepository *storage.Repository
var lcache *snapshot.Snapshot

//go:embed base.tmpl
var baseTemplate string

//go:embed repository.tmpl
var repositoryTemplate string

//go:embed browse.tmpl
var browseTemplate string

//go:embed object.tmpl
var objectTemplate string

//go:embed search.tmpl
var searchTemplate string

var templates map[string]*template.Template

type SnapshotSummary struct {
	Header      *header.Header
	Roots       uint64
	Directories uint64
	Files       uint64
	NonRegular  uint64
	Pathnames   uint64
	Objects     uint64
	Chunks      uint64

	Size uint64
}

type TemplateFunctions struct {
	HumanizeBytes func(uint64) string
}

func templateFunctions() TemplateFunctions {
	return TemplateFunctions{
		HumanizeBytes: func(nbytes uint64) string {
			return humanize.Bytes(nbytes)
		},
	}
}

func getSnapshots(repository *storage.Repository) ([]*snapshot.Snapshot, error) {
	snapshotsList, err := snapshot.List(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*snapshot.Snapshot, 0)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, snapshotUuid := range snapshotsList {
		wg.Add(1)
		go func(snapshotUuid uuid.UUID) {
			defer wg.Done()
			snapshotInstance, err := snapshot.Load(repository, snapshotUuid)
			if err != nil {
				return
			}
			mu.Lock()
			result = append(result, snapshotInstance)
			mu.Unlock()
		}(snapshotUuid)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		return result[i].Header.CreationTime.Before(result[j].Header.CreationTime)
	})

	return result, nil
}

func getHeaders(repository *storage.Repository) ([]*header.Header, error) {
	snapshotsList, err := snapshot.List(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*header.Header, 0)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, snapshotUuid := range snapshotsList {
		wg.Add(1)
		go func(snapshotUuid uuid.UUID) {
			defer wg.Done()
			hdr, _, err := snapshot.GetSnapshot(repository, snapshotUuid)
			if err != nil {
				return
			}
			mu.Lock()
			result = append(result, hdr)
			mu.Unlock()
		}(snapshotUuid)
	}
	wg.Wait()
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreationTime.Before(result[j].CreationTime)
	})
	return result, nil

}

func (summary *SnapshotSummary) HumanSize() string {
	return humanize.Bytes(summary.Size)
}

func SnapshotToSummary(snapshot *snapshot.Snapshot) *SnapshotSummary {
	ss := &SnapshotSummary{}
	ss.Header = snapshot.Header
	ss.Roots = uint64(len(snapshot.Header.ScannedDirectories))
	ss.Directories = uint64(len(snapshot.Filesystem.ListDirectories()))
	ss.Files = uint64(len(snapshot.Filesystem.ListFiles()))
	ss.NonRegular = uint64(len(snapshot.Filesystem.ListNonRegular()))
	ss.Pathnames = uint64(len(snapshot.Filesystem.ListStat()))
	ss.Objects = uint64(len(snapshot.Index.ListObjects()))
	ss.Chunks = uint64(len(snapshot.Index.ListChunks()))
	return ss
}

func viewRepository(w http.ResponseWriter, r *http.Request) {

	hdrs, _ := getHeaders(lrepository)

	totalFiles := uint64(0)

	kinds := make(map[string]uint64)
	types := make(map[string]uint64)
	extensions := make(map[string]uint64)

	kindsPct := make(map[string]float64)
	typesPct := make(map[string]float64)
	extensionsPct := make(map[string]float64)

	res := make([]*header.Header, 0)
	for _, hdr := range hdrs {
		res = append(res, hdr)
		totalFiles += hdr.FilesCount

		for key, value := range hdr.FileKind {
			if _, exists := kinds[key]; !exists {
				kinds[key] = 0
			}
			kinds[key] += value
		}

		for key, value := range hdr.FileType {
			if _, exists := types[key]; !exists {
				types[key] = 0
			}
			types[key] += value
		}

		for key, value := range hdr.FileExtension {
			if _, exists := extensions[key]; !exists {
				extensions[key] = 0
			}
			extensions[key] += value
		}
	}

	for key, value := range kinds {
		kindsPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	for key, value := range types {
		typesPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	for key, value := range extensions {
		extensionsPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	ctx := &struct {
		Repository    storage.RepositoryConfig
		Headers       []*header.Header
		MajorTypes    map[string]uint64
		MimeTypes     map[string]uint64
		Extensions    map[string]uint64
		MajorTypesPct map[string]float64
		MimeTypesPct  map[string]float64
		ExtensionsPct map[string]float64
	}{
		lrepository.Configuration(),
		res,
		kinds,
		types,
		extensions,
		kindsPct,
		typesPct,
		extensionsPct,
	}

	templates["repository"].Execute(w, ctx)
}

func browse(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	var snap *snapshot.Snapshot
	if lcache == nil || lcache.Header.IndexID.String() != id {
		tmp, err := snapshot.Load(lrepository, uuid.Must(uuid.Parse(id)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	if path == "" {
		path = "/"
	}

	_, exists := snap.Filesystem.LookupInode(path)
	if !exists {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	directories := make([]*vfs.FileInfo, 0)
	files := make([]*vfs.FileInfo, 0)
	symlinks := make([]*vfs.FileInfo, 0)
	symlinksResolve := make(map[string]string)
	others := make([]*vfs.FileInfo, 0)

	children, _ := snap.Filesystem.LookupChildren(path)
	for _, childname := range children {
		fileinfo, _ := snap.Filesystem.LookupInode(fmt.Sprintf("%s/%s", path, childname))
		if fileinfo.Mode().IsDir() {
			directories = append(directories, fileinfo)
		} else if fileinfo.Mode().IsRegular() {
			files = append(files, fileinfo)
		} else {
			pathname := fmt.Sprintf("%s/%s", path, fileinfo.Name())
			if _, exists := snap.Filesystem.Symlinks[pathname]; exists {
				symlinks = append(symlinks, fileinfo)
				symlinksResolve[fileinfo.Name()] = snap.Filesystem.Symlinks[pathname]
			} else {
				others = append(others, fileinfo)
			}
		}
	}

	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i].Name(), directories[j].Name()) < 0
	})
	sort.Slice(files, func(i, j int) bool {
		return strings.Compare(files[i].Name(), files[j].Name()) < 0
	})
	sort.Slice(symlinks, func(i, j int) bool {
		return strings.Compare(symlinks[i].Name(), symlinks[j].Name()) < 0
	})
	sort.Slice(others, func(i, j int) bool {
		return strings.Compare(others[i].Name(), others[j].Name()) < 0
	})

	nav := make([]string, 0)
	navLinks := make(map[string]string)
	atoms := strings.Split(path, "/")[1:]
	for offset, atom := range atoms {
		nav = append(nav, atom)
		navLinks[atom] = "/" + strings.Join(atoms[:offset+1], "/")
	}

	ctx := &struct {
		Snapshot        *snapshot.Snapshot
		Directories     []*vfs.FileInfo
		Files           []*vfs.FileInfo
		Symlinks        []*vfs.FileInfo
		SymlinksResolve map[string]string
		Others          []*vfs.FileInfo
		Path            string
		Scanned         []string
		Navigation      []string
		NavigationLinks map[string]string
	}{snap, directories, files, symlinks, symlinksResolve, others, path, snap.Header.ScannedDirectories, nav, navLinks}
	templates["browse"].Execute(w, ctx)

}

func object(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	var snap *snapshot.Snapshot
	if lcache == nil || lcache.Header.IndexID.String() != id {
		tmp, err := snapshot.Load(lrepository, uuid.Must(uuid.Parse(id)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	pathnameID := snap.Filesystem.GetPathnameID(path)
	object := snap.Index.LookupObjectForPathname(pathnameID)
	if object == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	object.ContentType, _ = snap.Metadata.LookupKeyForValue("Content-Type", object.Checksum)

	info, _ := snap.Filesystem.LookupInode(path)

	chunks := make([]*objects.Chunk, 0)
	for _, chunkChecksum := range object.Chunks {
		chunks = append(chunks, snap.Index.LookupChunk(chunkChecksum))
	}

	root := ""
	for _, atom := range strings.Split(path, "/") {
		root = root + atom + "/"
		if _, ok := snap.Filesystem.LookupInodeForDirectory(root); ok {
			break
		}
	}

	nav := make([]string, 0)
	navLinks := make(map[string]string)
	atoms := strings.Split(path, "/")[1:]
	for offset, atom := range atoms {
		nav = append(nav, atom)
		navLinks[atom] = "/" + strings.Join(atoms[:offset+1], "/")
	}

	enableViewer := false
	if strings.HasPrefix(object.ContentType, "text/") ||
		strings.HasPrefix(object.ContentType, "image/") ||
		strings.HasPrefix(object.ContentType, "audio/") ||
		strings.HasPrefix(object.ContentType, "video/") ||
		object.ContentType == "application/pdf" ||
		object.ContentType == "application/javascript" ||
		object.ContentType == "application/x-sql" ||
		object.ContentType == "application/x-tex" {
		enableViewer = true
	}

	ctx := &struct {
		Snapshot        *snapshot.Snapshot
		Object          *objects.Object
		ObjectChecksum  string
		Chunks          []*objects.Chunk
		Info            *vfs.FileInfo
		Root            string
		Path            string
		Navigation      []string
		NavigationLinks map[string]string
		EnableViewer    bool
	}{snap, object, fmt.Sprintf("%016x", object.Checksum), chunks, info, root, path, nav, navLinks, enableViewer}
	templates["object"].Execute(w, ctx)
}

func raw(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]
	download := r.URL.Query().Get("download")
	highlight := r.URL.Query().Get("highlight")

	var snap *snapshot.Snapshot
	if lcache == nil || lcache.Header.IndexID.String() != id {
		tmp, err := snapshot.Load(lrepository, uuid.Must(uuid.Parse(id)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	pathnameID := snap.Filesystem.GetPathnameID(path)
	object := snap.Index.LookupObjectForPathname(pathnameID)
	if object == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	object.ContentType, _ = snap.Metadata.LookupKeyForValue("Content-Type", object.Checksum)

	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType == "" {
		contentType = object.ContentType
	}

	if contentType == "application/x-tex" {
		contentType = "text/plain"
	}

	if !strings.HasPrefix(object.ContentType, "text/") || highlight == "" {
		w.Header().Add("Content-Type", contentType)
		if download != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(path)))
		}
		for _, chunkChecksum := range object.Chunks {
			data, err := snap.GetChunk(chunkChecksum)
			if err != nil {
			}
			w.Write(data)
		}
		return
	}

	content := []byte("")
	for _, chunkChecksum := range object.Chunks {
		data, err := snap.GetChunk(chunkChecksum)
		if err != nil {
		}
		content = append(content, data...)
	}

	lexer := lexers.Match(path)
	if lexer == nil {
		lexer = lexers.Analyse(string(content))
	}
	if lexer == nil {
		w.Header().Add("Content-Type", contentType)
		w.Write(content)
		return
	}

	w.Header().Add("Content-Type", "text/html")
	style := styles.Get("dracula")
	if style == nil {
		style = styles.Fallback
	}
	formatter := formatters.Get("html")
	if formatter == nil {
		formatter = formatters.Fallback
	}
	iterator, err := lexer.Tokenise(nil, string(content))
	err = formatter.Format(w, style, iterator)
	if err != nil {
		w.Header().Add("Content-Type", contentType)
		w.Write(content)
	}
	return
}

func search_snapshots(w http.ResponseWriter, r *http.Request) {
	urlParams := r.URL.Query()
	q := urlParams["q"][0]
	queryKind := urlParams["kind"]
	queryMime := urlParams["mime"]
	queryExt := urlParams["ext"]

	kind := ""
	if queryKind != nil {
		kind = queryKind[0]
	} else {
		kind = ""
	}
	mime := ""
	if queryMime != nil {
		mime = queryMime[0]
	} else {
		mime = ""
	}
	ext := ""
	if queryExt != nil {
		ext = queryExt[0]
	} else {
		ext = ""
	}

	snapshots, err := snapshot.List(lrepository)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	snapshotsList := make([]*snapshot.Snapshot, 0)
	for _, indexID := range snapshots {
		snapshot, err := snapshot.Load(lrepository, indexID)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snapshot)
	}
	sort.Slice(snapshotsList, func(i, j int) bool {
		return snapshotsList[i].Header.CreationTime.Before(snapshotsList[j].Header.CreationTime)
	})

	directories := make([]struct {
		Snapshot string
		Date     string
		Path     string
	}, 0)
	files := make([]struct {
		Snapshot string
		Date     string
		Mime     string
		Path     string
	}, 0)
	for _, snap := range snapshotsList {
		if kind == "" && mime == "" && ext == "" {
			for _, directory := range snap.Filesystem.ListDirectories() {
				if strings.Contains(directory, q) {
					directories = append(directories, struct {
						Snapshot string
						Date     string
						Path     string
					}{snap.Header.IndexID.String(), snap.Header.CreationTime.String(), directory})
				}
			}
		}
		for _, file := range snap.Filesystem.ListStat() {
			if strings.Contains(file, q) {
				pathnameID := snap.Filesystem.GetPathnameID(file)
				object := snap.Index.LookupObjectForPathname(pathnameID)
				if object != nil {
					object.ContentType, _ = snap.Metadata.LookupKeyForValue("Content-Type", object.Checksum)
					if kind != "" && !strings.HasPrefix(object.ContentType, kind+"/") {
						continue
					}
					if mime != "" && !strings.HasPrefix(object.ContentType, mime) {
						continue
					}
					if ext != "" && filepath.Ext(file) != ext {
						continue
					}

					files = append(files, struct {
						Snapshot string
						Date     string
						Mime     string
						Path     string
					}{snap.Header.IndexID.String(), snap.Header.CreationTime.String(), object.ContentType, file})
				}
			}
		}
	}
	sort.Slice(directories, func(i, j int) bool {
		return directories[i].Date < directories[j].Date && strings.Compare(directories[i].Path, directories[j].Path) < 0
	})
	sort.Slice(files, func(i, j int) bool {
		return files[i].Date < files[j].Date && strings.Compare(files[i].Path, files[j].Path) < 0
	})

	ctx := &struct {
		SearchTerms string
		Directories []struct {
			Snapshot string
			Date     string
			Path     string
		}
		Files []struct {
			Snapshot string
			Date     string
			Mime     string
			Path     string
		}
	}{q, directories, files}
	templates["search"].Execute(w, ctx)
}

func Ui(repository *storage.Repository, addr string, spawn bool) error {
	lrepository = repository
	lcache = nil

	templates = make(map[string]*template.Template)

	t, err := template.New("repository").Funcs(template.FuncMap{
		"humanizeBytes": humanize.Bytes,
	}).Parse(baseTemplate + repositoryTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("browse").Parse(baseTemplate + browseTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("object").Parse(baseTemplate + objectTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("search").Parse(baseTemplate + searchTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	var url string
	if addr != "" {
		url = fmt.Sprintf("http://%s", addr)
	} else {
		var port uint16
		for {
			port = uint16(rand.Uint32() % 0xffff)
			if port >= 1024 {
				break
			}
		}
		addr = fmt.Sprintf("localhost:%d", port)
		url = fmt.Sprintf("http://%s", addr)
	}
	fmt.Println("lauching browser UI pointing at", url)
	if spawn {
		switch runtime.GOOS {
		case "windows":
			err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		case "darwin":
			err = exec.Command("open", url).Start()
		default: // "linux", "freebsd", "openbsd", "netbsd"
			err = exec.Command("xdg-open", url).Start()
		}
		if err != nil {
			return err
		}
	}

	r := mux.NewRouter()
	r.HandleFunc("/", viewRepository)
	r.HandleFunc("/snapshot/{snapshot}:/", browse)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}/", browse)
	r.HandleFunc("/raw/{snapshot}:{path:.+}", raw)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}", object)

	r.HandleFunc("/search", search_snapshots)

	return http.ListenAndServe(addr, r)
}
