package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"github.com/goware/urlx"
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
	var inputPath string
	var outputPath string
	var pathPrefix string
	var urlField int
	var header bool
	flag.StringVar(&inputPath, "i", "", "input file path")
	flag.StringVar(&outputPath, "o", "", "output file path")
	flag.StringVar(&pathPrefix, "p", "", "images path prefix")
	flag.IntVar(&urlField, "u", -1, "url field")
	flag.BoolVar(&header, "h", false, "use the first line as a header")
	flag.Parse()
	if inputPath == "" {
		log.Fatalln("input file path required")
	}
	if outputPath == "" {
		log.Fatalln("output file path required")
	}
	if pathPrefix == "" {
		log.Fatalln("images path prefix required")
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()
	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}

	csvReader := csv.NewReader(inputFile)
	csvWriter := csv.NewWriter(outputFile)

	i := -1
	if header {
		i++
		record, err := csvReader.Read()
		if err != nil {
			log.Println(i, err)
		} else {
			err = csvWriter.Write(append(record, "path"))
			if err != nil {
				log.Println(i, err)
			}
		}
	}
	for {
		i++
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println(i, err)
		} else {
			if len(record) == 0 {
				log.Println(i, "empty csv record")
			}
			urlPart := record[(len(record)+urlField)%len(record)]
			imageUrl, err := normalizeUrl(urlPart)
			if err != nil {
				log.Println(i, err)
				continue
			}
			imagePath := urlToPath(imageUrl, pathPrefix)
			err = csvWriter.Write(append(record, imagePath))
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
