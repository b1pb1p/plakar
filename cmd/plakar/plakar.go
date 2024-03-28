package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/cache"
	"github.com/PlakarLabs/plakar/config"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/helpers"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/denisbrodbeck/machineid"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"

	_ "github.com/PlakarLabs/plakar/storage/backends/database"
	_ "github.com/PlakarLabs/plakar/storage/backends/fs"
	_ "github.com/PlakarLabs/plakar/storage/backends/http"
	_ "github.com/PlakarLabs/plakar/storage/backends/null"
	_ "github.com/PlakarLabs/plakar/storage/backends/plakard"
	_ "github.com/PlakarLabs/plakar/storage/backends/s3"

	_ "github.com/PlakarLabs/plakar/vfs/importer/fs"
	_ "github.com/PlakarLabs/plakar/vfs/importer/imap"
	_ "github.com/PlakarLabs/plakar/vfs/importer/s3"
)

type Plakar struct {
	NumCPU      int
	Hostname    string
	Username    string
	Repository  string
	CommandLine string
	MachineID   string

	Cache  *cache.Cache
	Config *config.ConfigAPI

	KeyFromFile string
}

var commands map[string]func(Plakar, *storage.Repository, []string) int = make(map[string]func(Plakar, *storage.Repository, []string) int)

func registerCommand(command string, fn func(Plakar, *storage.Repository, []string) int) {
	commands[command] = fn
}

func executeCommand(ctx Plakar, repository *storage.Repository, command string, args []string) (int, error) {
	fn, exists := commands[command]
	if !exists {
		return 1, fmt.Errorf("unknown command: %s", command)
	}

	repositoryIndex, err := loadRepositoryIndex(repository)
	if err != nil {
		return 0, err
	}
	repository.SetRepositoryIndex(repositoryIndex)

	return fn(ctx, repository, args), nil
}

func main() {
	os.Exit(entryPoint())
}

func entryPoint() int {
	// default values
	opt_cpuDefault := runtime.GOMAXPROCS(0)
	if opt_cpuDefault != 1 {
		opt_cpuDefault = opt_cpuDefault - 1
	}

	opt_userDefault, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: go away casper !\n", flag.CommandLine.Name())
		return 1
	}

	opt_hostnameDefault, err := os.Hostname()
	if err != nil {
		opt_hostnameDefault = "localhost"
	}

	opt_machineIdDefault, err := machineid.ID()
	if err != nil {
		opt_machineIdDefault = uuid.NewSHA1(uuid.Nil, []byte(opt_hostnameDefault)).String()
	}
	opt_machineIdDefault = strings.ToLower(opt_machineIdDefault)

	opt_usernameDefault := opt_userDefault.Username
	opt_repositoryDefault := path.Join(opt_userDefault.HomeDir, ".plakar")
	opt_cacheDefault := path.Join(opt_userDefault.HomeDir, ".plakar-cache")
	opt_configDefault := path.Join(opt_userDefault.HomeDir, ".plakarconfig")

	// command line overrides
	var opt_cpuCount int
	var opt_cachedir string
	var opt_configfile string
	var opt_username string
	var opt_hostname string
	var opt_cpuProfile string
	var opt_memProfile string
	var opt_nocache bool
	var opt_time bool
	var opt_trace string
	var opt_verbose bool
	var opt_profiling bool
	var opt_keyfile string
	var opt_stats int

	flag.StringVar(&opt_configfile, "config", opt_configDefault, "configuration file")
	flag.StringVar(&opt_cachedir, "cache", opt_cacheDefault, "default cache directory")
	flag.IntVar(&opt_cpuCount, "cpu", opt_cpuDefault, "limit the number of usable cores")
	flag.StringVar(&opt_username, "username", opt_usernameDefault, "default username")
	flag.StringVar(&opt_hostname, "hostname", opt_hostnameDefault, "default hostname")
	flag.StringVar(&opt_cpuProfile, "profile-cpu", "", "profile CPU usage")
	flag.StringVar(&opt_memProfile, "profile-mem", "", "profile MEM usage")
	flag.BoolVar(&opt_nocache, "no-cache", false, "disable caching")
	flag.BoolVar(&opt_time, "time", false, "display command execution time")
	flag.StringVar(&opt_trace, "trace", "", "display trace logs")
	flag.BoolVar(&opt_verbose, "verbose", false, "display verbose logs")
	flag.BoolVar(&opt_profiling, "profiling", false, "display profiling logs")
	flag.StringVar(&opt_keyfile, "keyfile", "", "use passphrase from key file when prompted")
	flag.IntVar(&opt_stats, "stats", 0, "display statistics")
	flag.Parse()

	// setup from default + override
	if opt_cpuCount > runtime.NumCPU() {
		fmt.Fprintf(os.Stderr, "%s: can't use more cores than available: %d\n", flag.CommandLine.Name(), runtime.NumCPU())
		return 1
	}

	runtime.GOMAXPROCS(opt_cpuCount)

	if opt_cpuProfile != "" {
		f, err := os.Create(opt_cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create CPU profile: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not start CPU profile: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	var secretFromKeyfile string
	if opt_keyfile != "" {
		data, err := os.ReadFile(opt_keyfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not read key file: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		secretFromKeyfile = strings.TrimSuffix(string(data), "\n")
	}

	ctx := Plakar{}
	ctx.NumCPU = opt_cpuCount
	ctx.Username = opt_username
	ctx.Hostname = opt_hostname
	ctx.Repository = opt_repositoryDefault
	ctx.CommandLine = strings.Join(os.Args, " ")
	ctx.MachineID = opt_machineIdDefault
	ctx.KeyFromFile = secretFromKeyfile
	ctx.Config = config.NewConfigAPI(opt_configfile)

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: a command must be provided\n", flag.CommandLine.Name())
		return 1
	}

	// start logging
	if opt_verbose {
		logger.EnableInfo()
	}
	if opt_trace != "" {
		logger.EnableTrace(opt_trace)
	}
	if opt_profiling {
		logger.EnableProfiling()
	}
	loggerWait := logger.Start()

	command, args := flag.Args()[0], flag.Args()[1:]
	if flag.Arg(0) == "on" {
		if len(flag.Args()) < 2 {
			log.Fatalf("%s: missing plakar repository", flag.CommandLine.Name())
		}
		if len(flag.Args()) < 3 {
			log.Fatalf("%s: missing command", flag.CommandLine.Name())
		}
		ctx.Repository = flag.Arg(1)
		command, args = flag.Arg(2), flag.Args()[3:]
	}

	if strings.HasPrefix(ctx.Repository, "@") {
		if location, err := ctx.Config.GetRepositoryParameter(ctx.Repository[1:], "location"); err != nil {
			log.Fatalf("%s: unknown repository alias: %s", flag.CommandLine.Name(), ctx.Repository)
		} else {
			ctx.Repository = location
		}
	}

	// cmd_create must be ran after workdir.New() but before other commands
	if command == "create" {
		return cmd_create(ctx, args)
	}

	if command == "config" {
		return cmd_config(ctx, args)
	}

	if command == "version" {
		return cmd_version(ctx, args)
	}

	// special case, server does not need a cache but does not return immediately either
	skipPassphrase := false
	if command == "server" || command == "stdio" {
		opt_nocache = true
		skipPassphrase = true
	}

	if !opt_nocache {
		cache.Create(opt_cachedir)
		ctx.Cache = cache.New(opt_cachedir)
	}

	repository, err := storage.Open(ctx.Repository)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return 1
	}

	if repository.Configuration().Version != storage.VERSION {
		fmt.Fprintf(os.Stderr, "%s: incompatible repository version: %s != %s\n",
			flag.CommandLine.Name(), repository.Configuration().Version, storage.VERSION)
		return 1
	}

	var secret []byte
	if !skipPassphrase {
		if repository.Configuration().Encryption != "" {
			if ctx.KeyFromFile == "" {
				for {
					passphrase, err := helpers.GetPassphrase("repository")
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s\n", err)
						continue
					}

					secret, err = encryption.DeriveSecret(passphrase, repository.Configuration().EncryptionKey)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s\n", err)
						continue
					}

					break
				}
			} else {
				secret, err = encryption.DeriveSecret([]byte(ctx.KeyFromFile), repository.Configuration().EncryptionKey)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err)
					os.Exit(1)
				}
			}
		}
	}

	//
	repository.SetSecret(secret)
	repository.SetCache(ctx.Cache)
	repository.SetUsername(ctx.Username)
	repository.SetHostname(ctx.Hostname)
	repository.SetCommandLine(ctx.CommandLine)
	repository.SetMachineID(ctx.MachineID)

	done := make(chan bool, 1)
	if opt_stats > 0 {
		go func() {
			iterCount := 0

			avgGoroutines := 0
			maxGoroutines := 0
			totalGoroutines := 0

			maxCgoCalls := int64(0)
			maxMemAlloc := uint64(0)
			avgMemAlloc := uint64(0)

			t0 := time.Now()

			lastrbytes := uint64(0)
			lastwbytes := uint64(0)

			for {
				if iterCount != 0 {

					elapsedSeconds := time.Since(t0).Seconds()

					rbytes := repository.GetRBytes()
					wbytes := repository.GetWBytes()

					rbytesAvg := rbytes / uint64(elapsedSeconds)
					wbytesAvg := wbytes / uint64(elapsedSeconds)

					diffrbytes := rbytes - lastrbytes
					diffwbytes := wbytes - lastwbytes
					lastrbytes = rbytes
					lastwbytes = wbytes

					var memStats runtime.MemStats
					runtime.ReadMemStats(&memStats)

					if runtime.NumGoroutine() > maxGoroutines {
						maxGoroutines = runtime.NumGoroutine()
					}
					totalGoroutines += runtime.NumGoroutine()
					avgGoroutines = totalGoroutines / int(elapsedSeconds)

					if runtime.NumCgoCall() > maxCgoCalls {
						maxCgoCalls = runtime.NumCgoCall()
					}
					if memStats.Alloc > maxMemAlloc {
						maxMemAlloc = memStats.Alloc
					}
					avgMemAlloc = memStats.TotalAlloc / uint64(iterCount)

					logger.Printf("[stats] cpu: goroutines: %d (μ %d, <= %d), cgocalls: %d (<= %d) | mem: %s (μ %s, <= %s, += %s), gc: %d | storage: rd: %s (μ %s, += %s), wr: %s (μ %s, += %s)",
						runtime.NumGoroutine(),
						avgGoroutines,
						maxGoroutines,
						runtime.NumCgoCall(),
						maxCgoCalls,
						humanize.Bytes(memStats.Alloc),
						humanize.Bytes(avgMemAlloc),
						humanize.Bytes(maxMemAlloc),
						humanize.Bytes(memStats.TotalAlloc),
						memStats.NumGC,
						humanize.Bytes(diffrbytes), humanize.Bytes(rbytesAvg), humanize.Bytes(rbytes),
						humanize.Bytes(diffwbytes), humanize.Bytes(wbytesAvg), humanize.Bytes(wbytes))
				}

				select {
				case <-time.After(time.Duration(opt_stats) * time.Second):
					iterCount++
					continue
				case <-done:
					return
				}
			}
		}()
	}

	// commands below all operate on an open repository
	t0 := time.Now()
	status, err := executeCommand(ctx, repository, command, args)
	t1 := time.Since(t0)
	done <- true

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
	}

	err = repository.Close()
	if err != nil {
		logger.Warn("could not close repository: %s", err)
	}

	if ctx.Cache != nil {
		ctx.Cache.Commit()
	}

	if opt_profiling {
		profiler.Display()
	}

	loggerWait()

	if opt_time {
		fmt.Println("time:", t1)
	}

	if opt_memProfile != "" {
		f, err := os.Create(opt_memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write MEM profile: %d\n", flag.CommandLine.Name(), err)
			return 1
		}
	}

	return status
}
