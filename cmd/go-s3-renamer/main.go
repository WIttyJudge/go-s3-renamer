package main

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/wittyjudge/go-s3-renamer/internal/services"
)

var (
	wg      sync.WaitGroup
	client  = services.NewS3Client()
	limiter = make(chan struct{}, 50)
)

func renameFile(key string) {
	var dest string

	reg := regexp.MustCompile(`.block.lz4$|.blockPATH_GENERATOR_SUFFIX$`)
	dest = reg.ReplaceAllString(key, ".block.tar.lz4")

	client.RenameFile("streaming-tron", key, dest)
}

func skipProcessing(value string) bool {
	match, _ := regexp.MatchString(`.block.tar.lz4$`, value)
	return match
}

func process(key string) {
	defer wg.Done()

	ok := skipProcessing(key)
	if ok {
		return
	}

	renameFile(key)
}

func main() {
	start := time.Now()

	wg.Add(1)
	go client.ListAllObjects("streaming-tron", &wg)

	for key := range client.Keys {
		limiter <- struct{}{}

		wg.Add(1)

		go func(key string) {
			process(key)

			<-limiter
		}(key)

	}

	wg.Wait()

	end := time.Since(start)
	fmt.Println("Processed in", end)
}
