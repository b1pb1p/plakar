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
	"io/ioutil"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("search", cmd_search)
}

func cmd_search(ctx Plakar, repository *storage.Repository, args []string) int {
	var opt_index bool
	flags := flag.NewFlagSet("search", flag.ExitOnError)
	flags.BoolVar(&opt_index, "index", false, "")
	flags.Parse(args)

	index, err := bleve.Open("/tmp/plakar-bleeve")
	if err != nil {
		mapping := bleve.NewIndexMapping()
		index, err = bleve.New("/tmp/plakar-bleeve", mapping)
		if err != nil {
			return 1
		}
	}

	if opt_index {
		snapshots, _ := getSnapshots(repository, nil)
		for _, snapshotIndex := range snapshots {
			for contentType := range snapshotIndex.Index.ContentTypes {
				if strings.HasPrefix(contentType, "text/") {
					for _, object := range snapshotIndex.Index.LookupObjectsForContentType(contentType) {
						objectID, _ := snapshotIndex.Index.GetChecksumID(object)
						for _, pathnameID := range snapshotIndex.Index.ObjectToPathnames[objectID] {
							pathname, _ := snapshotIndex.Index.GetPathname(pathnameID)
							fmt.Println(pathname)
							rd, err := snapshotIndex.NewReader(pathname)
							if err != nil {
								fmt.Println(err)
								continue
							}
							data, err := ioutil.ReadAll(rd)
							if err != nil {
								fmt.Println(err)
								continue
							}
							err = index.Index(fmt.Sprintf("%s:%s", snapshotIndex.Metadata.IndexID, pathname), string(data))
							if err != nil {
								fmt.Println(err)
								continue
							}

						}
					}
				}
			}
		}
	}

	// search for some text
	queryString := flags.Arg(0)
	fmt.Println("query: " + queryString)
	query := bleve.NewMatchQuery(queryString)
	search := bleve.NewSearchRequest(query)
	searchResults, err := index.Search(search)
	fmt.Println(searchResults)

	return 0
}
