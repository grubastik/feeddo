package parser

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type decoderTokenErr struct{}

func (t decoderTokenErr) Token() (xml.Token, error) {
	return nil, errors.New("Test token")
}

func (t decoderTokenErr) DecodeElement(v interface{}, start *xml.StartElement) error {
	return nil
}

type decoderDecodeErr struct{}

func (t decoderDecodeErr) Token() (xml.Token, error) {
	return xml.StartElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderDecodeErr) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderDecodeWrongElement struct{}

func (t decoderDecodeWrongElement) Token() (xml.Token, error) {
	return xml.EndElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderDecodeWrongElement) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderDecodeWrongTagName struct{}

func (t decoderDecodeWrongTagName) Token() (xml.Token, error) {
	return xml.EndElement{Name: xml.Name{Local: "SOMEITEM"}}, nil
}

func (t decoderDecodeWrongTagName) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderHappyPath struct{}

func (t decoderHappyPath) Token() (xml.Token, error) {
	return xml.StartElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderHappyPath) DecodeElement(v interface{}, start *xml.StartElement) error {
	ir, ok := v.(*heureka.Item)
	if !ok {
		return errors.New("Can not cast interface to heureka item")
	}
	*ir = heureka.Item{Product: "Test", ProductName: "TestName"}
	return nil
}

func TestGetItemFromStream(t *testing.T) {
	tests := []struct {
		name    string
		decoder Decoder
		err     string
		item    *heureka.Item
	}{
		{
			name:    "token error",
			decoder: decoderTokenErr{},
			err:     "Failed to read node element: Test token",
			item:    nil,
		},
		{
			name:    "decoding error",
			decoder: decoderDecodeErr{},
			err:     "Failed to unmarshal xml node: Test decode error",
			item:    nil,
		},
		{
			name:    "decoding without results",
			decoder: decoderDecodeWrongElement{},
			err:     "",
			item:    nil,
		},
		{
			name:    "decoding with wrong tag name",
			decoder: decoderDecodeWrongTagName{},
			err:     "",
			item:    nil,
		},
		{
			name:    "happy path",
			decoder: decoderHappyPath{},
			err:     "",
			item:    &heureka.Item{Product: "Test", ProductName: "TestName"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := getItemFromStream(tt.decoder)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.item, i)
			}
		})
	}
}

func TestProcessFeed(t *testing.T) {
	tests := []struct {
		name string
		xml  string
		err  string
		item heureka.Item
	}{
		{"Xml with error", "<SHOPITEM><ITEM_ID>123abc</ITEM_ID><PRODUCT></SHOPITEM>", "Failed to get item from stream: Failed to unmarshal xml node: XML syntax error on line 1: element <PRODUCT> closed by </SHOPITEM>", heureka.Item{}},
		{"Xml with error", "<SHOPITEM><ITEM_ID>123abc</ITEM_ID></SHOPITEM>", "", heureka.Item{ID: "123abc"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stringReader := strings.NewReader(tt.xml)
			stringReadCloser := ioutil.NopCloser(stringReader)
			chanItem, chanError := ProcessFeed(stringReadCloser)
			if tt.err != "" {
				err := <-chanError //only one error possible here before close
				<-chanError        //on close channel should be unblocked
				<-chanItem         // wait for close
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				item := <-chanItem //we have only one item in stream
				err := <-chanError //wait for close
				<-chanError        //on close channel should be unblocked
				<-chanItem         //we have only one item in stream
				require.NoError(t, err)
				require.NotEmpty(t, item)
				assert.Equal(t, tt.item.ID, item.ID)
			}
		})
	}
}
