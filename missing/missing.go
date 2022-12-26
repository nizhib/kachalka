package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"github.com/goware/urlx"
	"github.com/schollz/progressbar/v3"
	"io"
	"log"
	"os"
	"path"
)

func normalizeUrl(url string) (string, error) {
	parsed, err := urlx.Parse(url)
	if err != nil {
		return "", err
	}
	normalized, err := urlx.Normalize(parsed)
	return normalized, err
}

func urlToPath(url string, root string) string {
	hash := md5.Sum([]byte(url))
	name := hex.EncodeToString(hash[:6])
	p1 := hex.EncodeToString(hash[:1])
	p2 := hex.EncodeToString(hash[1:2])
	return path.Join(root, p1, p2, name) + ".jpg"
}

func main() {
	inputPath := flag.String("i", "", "input file path")
	outputPath := flag.String("o", "", "output file path")
	pathPrefix := flag.String("p", "", "images path prefix")
	urlField := flag.Int("u", -1, "url field")
	flag.Parse()
	if *inputPath == "" {
		log.Fatalln("input file path required")
	}
	if *outputPath == "" {
		log.Fatalln("output file path required")
	}
	if *pathPrefix == "" {
		log.Fatalln("images path prefix required")
	}

	inputFile, err := os.Open(*inputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(*outputPath)
	if err != nil {
		log.Fatal(err)
	}

	csvReader := csv.NewReader(inputFile)
	csvWriter := csv.NewWriter(outputFile)

	bar := progressbar.Default(-1)
	for i := 0; ; i++ {
		record, err := csvReader.Read()
		bar.Add(1)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println(i, err)
		} else {
			if len(record) == 0 {
				log.Println(i, "empty csv record")
			}
			urlPart := record[(len(record)+*urlField)%len(record)]
			imageUrl, err := normalizeUrl(urlPart)
			if err != nil {
				continue
			}
			imagePath := urlToPath(imageUrl, *pathPrefix)
			_, err = os.Stat(imagePath)
			if err == nil {
				continue
			}
			err = csvWriter.Write(record)
			if err != nil {
				log.Println(i, err)
				continue
			}
		}
	}

	err = outputFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}
