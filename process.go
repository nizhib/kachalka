package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"github.com/nfnt/resize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/image/webp"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

const defaultJpegQuality = 90
const defaultMaxSize = 640

type ProcessOptions struct {
	outputRoot  string
	idFields    string
	urlField    int
	jpegQuality int
	maxSize     uint
	resume      bool
}

type Item struct {
	id, url string
}

func recordToItem(record []string, idFields string, urlField int) Item {
	var parts []string
	for _, field := range strings.Split(idFields, ",") {
		idx, err := strconv.Atoi(field)
		if err != nil {
			log.Fatal(err)
		}
		if idx > len(record) {
			log.Fatalf("Field index %d is out of bounds", idx)
		}
		parts = append(parts, record[idx])
	}
	itemId := strings.Join(parts, "$")
	itemPath := record[(len(record)+urlField)%len(record)]
	return Item{itemId, itemPath}
}

func UrlToPath(url string, root string) string {
	hash := md5.Sum([]byte(url))
	name := hex.EncodeToString(hash[:6])
	p1 := hex.EncodeToString(hash[:1])
	p2 := hex.EncodeToString(hash[1:2])
	return path.Join(root, p1, p2, name) + ".jpg"
}

func fetch(url string) ([]byte, error) {
	var body []byte

	resp, err := http.Get(url)
	if err != nil {
		return body, err
	}
	//goland:noinspection GoUnhandledErrorResult
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)

	return body, err
}

func removeTransparency(img *image.Image) *image.RGBA {
	bounds := (*img).Bounds()
	rgb := image.NewRGBA(bounds)
	draw.Draw(rgb, bounds, &image.Uniform{C: color.White}, image.Point{}, draw.Src)
	draw.Draw(rgb, bounds, *img, image.Point{}, draw.Over)
	return rgb
}

func process(record []string, options ProcessOptions, busy *sync.WaitGroup) (string, error) {
	item := recordToItem(record, options.idFields, options.urlField)
	filePath := UrlToPath(item.url, options.outputRoot)

	// create all the directories
	err := os.MkdirAll(path.Dir(filePath), os.ModePerm)
	if err != nil {
		return item.url, err
	}

	// finish if we are resuming and the file exists
	if _, err := os.Stat(filePath); options.resume && err == nil {
		return item.url, errors.New("process: skipping")
	}

	// fetch the image from the url
	body, err := fetch(item.url)
	if err != nil {
		return item.url, err
	}

	// create an image from the response
	img, _, err := image.Decode(bytes.NewBuffer(body))
	if err != nil {
		if err.Error() == "image: unknown format" {

			img, err = webp.Decode(bytes.NewBuffer(body))
			if err != nil {
				return item.url, err
			}
		} else {
			return item.url, err
		}
	}

	// make it no larger than maxSize x maxSize
	tmb := resize.Thumbnail(options.maxSize, options.maxSize, img, resize.MitchellNetravali)

	// remove the transparency
	rgb := removeTransparency(&tmb)

	// open a file for saving the result
	file, err := os.Create(filePath)
	if err != nil {
		return item.url, err
	}

	// save the resulting image as JPEG
	busy.Add(1) // enter the critical part
	err = jpeg.Encode(file, rgb, &jpeg.Options{Quality: options.jpegQuality})
	busy.Done() // leave the critical part
	if err != nil {
		return item.url, err
	}

	// return, propagating errors, if any
	return item.url, file.Close()
}
