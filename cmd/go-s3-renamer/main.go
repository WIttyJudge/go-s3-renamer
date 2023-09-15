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
	workers = 7
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

func process(wg *sync.WaitGroup) {
	defer wg.Done()

	for key := range client.Keys {
		ok := skipProcessing(key)
		if ok {
			continue
		}

		renameFile(key)
	}
}

func main() {
	start := time.Now()

	wg.Add(1)
	go client.ListAllObjects("streaming-tron", &wg)

	for i := 0; i < workers; i++ {
		go process(&wg)
	}

	wg.Wait()

	end := time.Since(start)
	fmt.Println("Processed in", end)
}
