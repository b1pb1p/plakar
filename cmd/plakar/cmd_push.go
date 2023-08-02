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
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/ebfe/signify"
	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("push", cmd_push)
}

func cmd_push(ctx Plakar, repository *storage.Repository, args []string) int {
	var opt_progress bool
	var opt_signkey string
	var opt_tags string

	flags := flag.NewFlagSet("push", flag.ExitOnError)
	flags.BoolVar(&opt_progress, "progress", false, "display progress bar")
	flags.StringVar(&opt_signkey, "sign", "", "keyfile to use for snapshot signing")
	flags.StringVar(&opt_tags, "tag", "", "tag to assign to this snapshot")
	flags.Parse(args)

	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	var skp SerializedKeypair
	var privateKey *signify.PrivateKey
	var publicKey *signify.PublicKey

	if opt_signkey != "" {
		data, err := ioutil.ReadFile(opt_signkey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		err = json.Unmarshal(data, &skp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		encryptedKey, err := base64.RawStdEncoding.DecodeString(skp.PrivateKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		encodedPublicKey, err := base64.RawStdEncoding.DecodeString(skp.PublicKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		for {
			passphrase, err := helpers.GetPassphrase("signify")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				return 1
			}
			tmp, err := signify.ParsePrivateKey(encryptedKey, passphrase)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
				continue
			}
			privateKey = tmp
			break
		}
		publicKey, err = signify.ParsePublicKey(encodedPublicKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
	}

	snap, err := snapshot.New(repository, uuid.Must(uuid.NewRandom()), privateKey, publicKey)
	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	snap.Metadata.Hostname = ctx.Hostname
	snap.Metadata.Username = ctx.Username
	snap.Metadata.OperatingSystem = runtime.GOOS
	snap.Metadata.MachineID = ctx.MachineID
	snap.Metadata.CommandLine = ctx.CommandLine

	var tags []string
	if opt_tags == "" {
		tags = []string{}
	} else {
		tags = []string{opt_tags}
	}
	snap.Metadata.Tags = tags

	if flags.NArg() == 0 {
		err = snap.Push([]string{dir}, opt_progress)
	} else {
		err = snap.Push(flags.Args(), opt_progress)
	}

	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	logger.Info("created snapshot %s", snap.Metadata.GetIndexShortID())
	return 0
}
