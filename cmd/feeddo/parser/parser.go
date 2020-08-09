package parser

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"

	"github.com/grubastik/feeddo/internal/pkg/heureka"
)

// Decoder implements xml decode interface
type Decoder interface {
	Token() (xml.Token, error)
	DecodeElement(v interface{}, start *xml.StartElement) error
}

// ProcessFeed loop through the channel and retrieve item from it
func ProcessFeed(readCloser io.ReadCloser) (<-chan heureka.Item, <-chan error) {
	// try to unmarshal stream.
	// If this stream is not represent expected schema - result will be empty.
	chanItemProducer := make(chan heureka.Item)
	chanItemError := make(chan error, 1)
	go func() {
		defer func() {
			close(chanItemProducer)
			close(chanItemError)
		}()
		d := xml.NewDecoder(readCloser)
		for {
			item, err := getItemFromStream(d)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					// in case of error - skip this item
					chanItemError <- fmt.Errorf("Failed to get item from stream: %w", err)
					err = d.Skip()
					if err != nil {
						chanItemError <- fmt.Errorf("Failed to skip bad part: %w", err)
						break
					}
				}
			}
			if item != nil {
				chanItemProducer <- *item
			}
		}
	}()
	return chanItemProducer, chanItemError
}

// getItemFromStream retrieves next item from xml
// item can be nil if start tag of next element in feed will be not recognized
// in this case error not provided and also will be nil
func getItemFromStream(d Decoder) (*heureka.Item, error) {
	token, err := d.Token()
	if err != nil {
		return nil, fmt.Errorf("Failed to read node element: %w", err)
	}
	switch startElem := token.(type) {
	case xml.StartElement:
		if startElem.Name.Local == "SHOPITEM" {
			item := &heureka.Item{}
			err = d.DecodeElement(item, &startElem)
			if err != nil {
				return nil, fmt.Errorf("Failed to unmarshal xml node: %w", err)
			}
			return item, nil
		}
	default:
	}
	return nil, nil
}
