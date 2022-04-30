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
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("search", cmd_search)
}

func cmd_search(ctx Plakar, repository *storage.Repository, args []string) int {
	var opt_index bool
	var opt_query string

	flags := flag.NewFlagSet("search", flag.ExitOnError)
	flags.BoolVar(&opt_index, "index", false, "")
	flags.StringVar(&opt_query, "query", "", "")
	flags.Parse(args)

	searchIndex, err := bleve.Open("/tmp/plakar-bleeve")
	if err != nil {
		mapping := bleve.NewIndexMapping()
		searchIndex, err = bleve.New("/tmp/plakar-bleeve", mapping)
		if err != nil {
			return 1
		}
	}

	if opt_index {
		indexesID, _ := repository.GetIndexes()
		wg := sync.WaitGroup{}
		maxConcurrency := make(chan bool, 512)
		for _, _indexID := range indexesID {
			maxConcurrency <- true
			wg.Add(1)
			go func(indexID uuid.UUID) {
				snap, err := snapshot.Load(repository, indexID)
				if err != nil {
					return
				}
				for contentType := range snap.Index.ContentTypes {
					if strings.HasPrefix(contentType, "text/") {
						for _, object := range snap.Index.LookupObjectsForContentType(contentType) {
							objectID, _ := snap.Index.GetChecksumID(object)
							for _, pathnameID := range snap.Index.ObjectToPathnames[objectID] {
								_pathname, _ := snap.Index.GetPathname(pathnameID)

								maxConcurrency <- true
								wg.Add(1)
								go func(pathname string) {
									fmt.Println(pathname)

									rd, err := snap.NewReader(pathname)
									if err != nil {
										fmt.Println(err)
										return
									}

									data, err := ioutil.ReadAll(rd)
									if err != nil {
										fmt.Println(err)
										return
									}
									err = searchIndex.Index(fmt.Sprintf("%s:%s", snap.Metadata.IndexID, pathname), string(data))
									if err != nil {
										fmt.Println(err)
										return
									}
									wg.Done()
									<-maxConcurrency
								}(_pathname)

							}
						}
					}
				}
				wg.Done()
				<-maxConcurrency
			}(_indexID)
		}
		wg.Wait()
	}

	if opt_query != "" {
		// search for some text
		queryString := opt_query
		fmt.Println("query: " + queryString)
		query := bleve.NewMatchQuery(queryString)
		search := bleve.NewSearchRequest(query)
		searchResults, _ := searchIndex.Search(search)
		fmt.Println(searchResults)
	}
	return 0
}
