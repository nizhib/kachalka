# Kachalka

Kachalka is your favorite tool for downloading images.

## Installation
```bash
go get -u github.com/nizhib/kachalka
```

## Usage
```
Usage: kachalka -i <index path> -o <images root>

Options:
  -id        Id fields. Default is "0".
  -url       Url field. Default is -1.
  -quality   Output images quality. Default is 90.
  -maxSize   Output images size limit. Default is 640.
  -resume    Resume the last run if any.
  -verbose   Log the results.
  -progress  Show progressbar.
  -w         Concurrent workers. Default is 2*NumCPU.
```
