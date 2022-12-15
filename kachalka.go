package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
)

const defaultIdFields = "0"
const defaultUrlField = -1

type Bar = progressbar.ProgressBar

func worker(jobs <-chan []string, opt ProcessOptions, done <-chan struct{}, busy *sync.WaitGroup, bar *Bar) {
	for row := range jobs {
		select {
		case <-done:
			log.Debugln("Stopping worker")
			return

		default:
			itemUrl, err := process(row, opt, busy)
			if err != nil {
				if err.Error() == "process: skipping" {
					log.Infof("Skip %s\n", itemUrl)
				} else {
					log.Warnf("%s: %s\n", err, itemUrl)
				}
			} else {
				log.Infof("Save %s\n", itemUrl)
			}
			if bar != nil {
				//goland:noinspection GoUnhandledErrorResult
				bar.Add(1)
			}
		}
	}
}

func main() {
	// parse command line arguments
	filePath := flag.String("i", "", "index file path")
	outputRoot := flag.String("id", defaultIdFields, "id fields")
	idFields := flag.String("o", "", "images output root")
	urlField := flag.Int("url", defaultUrlField, "url field")
	jpegQuality := flag.Int("quality", defaultJpegQuality, "output images quality")
	maxSize := flag.Uint("maxSize", defaultMaxSize, "output images size limit")
	workerCount := flag.Int("w", 2*runtime.NumCPU(), "concurrent workers")
	resume := flag.Bool("resume", false, "resume the last run if any")
	verbose := flag.Bool("verbose", false, "log the results")
	progress := flag.Bool("progress", false, "show progressbar")
	flag.Parse()
	// check the arguments
	if *filePath == "" {
		log.Fatalln("index file path required")
	}
	if *outputRoot == "" {
		log.Fatalln("images output root required")
	}
	if *verbose && *progress {
		log.Fatalln("using both verbose and progress will make a mess")
	}
	// just log if progress bar is absent
	if !*progress {
		*verbose = true
	}
	// show no less than warnings otherwise
	if !*verbose {
		log.SetLevel(log.WarnLevel)
	}

	// compile the options
	options := ProcessOptions{
		*outputRoot,
		*idFields,
		*urlField,
		*jpegQuality,
		*maxSize,
		*resume,
	}

	// open the input file
	indexFile, err := os.Open(*filePath)
	if err != nil {
		log.Fatal(err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer indexFile.Close()

	// calculate the index size
	log.Infoln("Calculating the index size... ")
	fileScanner := bufio.NewScanner(indexFile)
	lineCount := int64(0)
	for fileScanner.Scan() {
		lineCount++
	}
	_, err = indexFile.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	log.Infoln("Done!")

	// set up the communication
	var workers sync.WaitGroup           // workers synchronization
	var busy sync.WaitGroup              // entering critical parts
	done := make(chan struct{})          // preventing new jobs launched
	interrupt := make(chan os.Signal, 1) // interrupt signals channel
	signal.Notify(interrupt, os.Interrupt)
	defer func() {
		signal.Stop(interrupt)
	}()
	go func() {
		select {
		case <-interrupt:
			log.SetLevel(log.InfoLevel)
			log.Infoln("Gradually stopping the workers...")
			close(done)
			// signal.Stop(interrupt) // halt on the second SIGINT
			busy.Wait()
			os.Exit(0)
		}
	}()

	// create input channel for the workers
	jobs := make(chan []string, *workerCount)

	// define a progressbar for the workers
	var bar *Bar
	if *progress {
		bar = progressbar.Default(lineCount)
	}

	// launch the workers
	workers.Add(*workerCount)
	for i := 0; i < *workerCount; i++ {
		go func() {
			defer workers.Done()
			worker(jobs, options, done, &busy, bar)
		}()
	}

	// retranslate csv index to the channel
	go func() {
		csvReader := csv.NewReader(indexFile)
		finished := false
		for {
			if finished {
				break
			}
			select {
			case <-done:
				log.Infoln("Stopping the csv reader...")
				finished = true

			default:
				record, err := csvReader.Read()
				if err == io.EOF {
					finished = true
				} else if err != nil {
					log.Warnln(err)
				} else {
					if len(record) == 0 {
						log.Errorln("csv line is empty")
					}
					jobs <- record
				}
			}
		}
		close(jobs)
	}()

	// wait for workers to finish
	workers.Wait()
}
