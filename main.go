package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const chunkDir = "/tmp/chunkit"

func chunkIt(index int, chunk []any) error {
	encoded, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	return os.WriteFile(fmt.Sprintf("%s/%d_chunkit.json", chunkDir, index), encoded, os.ModePerm)
}

func main() {
	var filePath string
	var chunkSize int
	flag.StringVar(&filePath, "file", "", "path to json file")
	flag.IntVar(&chunkSize, "size", 50, "chunk size")
	flag.Parse()

	if filePath == "" {
		fmt.Println("need -file flag")
		return
	}

	if filepath.Ext(filePath) != ".json" {
		fmt.Println("need json file")
		return
	}

	if chunkSize <= 0 {
		fmt.Println("chunk size can not be smaller than 1")
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	// create chunk directory if not exist
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		if err := os.Mkdir(chunkDir, os.ModePerm); err != nil {
			fmt.Println(fmt.Sprintf("can not create chunk dir %s", chunkDir))
			return
		}
	}

	decoder := json.NewDecoder(file)

	token, err := decoder.Token()
	if err != nil {
		fmt.Println(err)
		return
	}

	if token != json.Delim('[') {
		fmt.Println("need a top level array in json file")
		return
	}

	var chunks [][]any
	var chunk []any
	for decoder.More() {
		var val any
		err := decoder.Decode(&val)
		if err != nil {
			fmt.Println(err)
			return
		}

		chunk = append(chunk, val)
		if len(chunk) == chunkSize {
			chunks = append(chunks, chunk)
			chunk = []any{}
		}
	}

	// get leftover
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}

	wg := sync.WaitGroup{}
	for i, chk := range chunks {
		wg.Add(1)
		go func(index int, chunk []any) {
			defer wg.Done()
			err := chunkIt(index, chunk)
			if err != nil {
				fmt.Println(err)
			}
		}(i, chk)
	}

	wg.Wait()

	fmt.Println(fmt.Sprintf("created files in %s", chunkDir))
}
