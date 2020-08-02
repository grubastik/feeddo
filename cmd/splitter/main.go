package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/jessevdk/go-flags"
)

func main() {
	file, count, offset, err := parseArgs()
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to parse flags: %w", err))
	}

	readCloser, err := os.Open(file.Hostname() + file.EscapedPath())
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to read file `%v` because of %w", file, err))
	}

	defer readCloser.Close()
	// try to unmarshal stream.
	// If this stream is not represent expected schema - result will be empty.
	items := make([]heureka.Item, count, count)
	counter := 0
	d := xml.NewDecoder(readCloser)
	for {
		token, err := d.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				log.Fatal(fmt.Errorf("Failed to read node element: %w", err))
			}
		}
		var item *heureka.Item
		switch startElem := token.(type) {
		case xml.StartElement:
			if startElem.Name.Local == "SHOPITEM" {
				item = &heureka.Item{}
				err = d.DecodeElement(item, &startElem)
				if err != nil {
					log.Fatal(fmt.Errorf("Failed to unmarshal xml node: %w", err))
				}
			}
		default:
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				log.Fatal(fmt.Errorf("Failed to unmarshal xml: %w", err))
			}
		}
		if item != nil {
			if counter >= offset {
				if counter >= offset+count {
					break
				}
				items[counter-offset] = *item
			}
			counter++
		}
	}
	shop := heureka.Shop{
		ShopItem: items,
	}
	shopXML, err := xml.Marshal(shop)
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to marshal result because of %w", err))
	}
	writeCloser, err := os.Create(file.Hostname() + file.EscapedPath() + strconv.Itoa(offset) + "-" + strconv.Itoa(count) + ".xml")
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to create file `%v` because of %w", file, err))
	}
	_, err = writeCloser.Write(shopXML)
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to write result because of %w", err))
	}
}

func parseArgs() (*url.URL, int, int, error) {
	var opts struct {
		// list of feeds' urls
		File   string `short:"f" long:"file" description:"Original file" required:"true"`
		Count  int    `short:"c" long:"count" description:"Number of items to extract" required:"true"`
		Offset int    `short:"o" long:"offset" description:"Number of items to skip" optional:"true"`
	}
	parser := flags.NewParser(&opts, flags.PassDoubleDash|flags.IgnoreUnknown)
	_, err := parser.Parse()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("Unable to parse flags: %w", err)
	}
	if opts.File == "" {
		return nil, 0, 0, fmt.Errorf("File is required")
	}

	file, err := url.Parse(strings.TrimSpace(opts.File))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("Unable to parse file '%s' because of %w", file, err)
	}

	if opts.Count <= 0 {
		return nil, 0, 0, fmt.Errorf("count argument is required and should be greater than zero")
	}

	if opts.Offset < 0 {
		return nil, 0, 0, fmt.Errorf("offset argument is required and should be greater or equal than zero")
	}

	return file, opts.Count, opts.Offset, nil
}
