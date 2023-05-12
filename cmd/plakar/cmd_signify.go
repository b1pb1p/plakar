/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ebfe/signify"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/storage"
)

type SerializedKeypair struct {
	PublicKey  string
	PrivateKey string
}

func init() {
	registerCommand("signify", cmd_signify)
}

func cmd_signify(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("signify", flag.ExitOnError)
	flags.Parse(args)

	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "%s: subcommand required for %s\n", flag.CommandLine.Name(), flags.Name())
		return 1
	}

	switch args[0] {
	case "generate":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "%s: subcommand generate requires pathname\n", flag.CommandLine.Name())
			return 1
		}

		publicKey, privateKey, err := signify.GenerateKey(rand.Reader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		var passphrase []byte
		for {
			tmp, err := helpers.GetPassphraseConfirm("signify")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			passphrase = tmp
			break
		}

		encryptedBytes, err := signify.MarshalPrivateKey(privateKey, rand.Reader, passphrase, 42)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		encodedPublicKey := signify.MarshalPublicKey(publicKey)

		skp := SerializedKeypair{}
		skp.PublicKey = base64.RawStdEncoding.EncodeToString(encodedPublicKey)
		skp.PrivateKey = base64.RawStdEncoding.EncodeToString(encryptedBytes)

		data, err := json.Marshal(&skp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		err = os.WriteFile(args[1], data, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		fmt.Println("generated keypair in", args[1])
	}
	return 0
}
