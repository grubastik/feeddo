package heureka

import (
	"encoding/xml"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriceUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		xml      string
		err      string
		expected decimal.Decimal
	}{
		{
			"empty value", "<SHOPITEM></SHOPITEM>", "", decimal.Decimal{},
		},
		{
			"random value", "<SHOPITEM><PRICE_VAT>werwer</PRICE_VAT></SHOPITEM>",
			"Unmarshal of price 'werwer' failed: error decoding string 'werwer': can't convert werwer to decimal: exponent is not numeric", decimal.Decimal{},
		},
		{
			"zero value", "<SHOPITEM><PRICE_VAT>0</PRICE_VAT></SHOPITEM>", "", decimal.Zero,
		},
		{
			"happy path value with space", "<SHOPITEM><PRICE_VAT>1 000</PRICE_VAT></SHOPITEM>", "", decimal.New(1, 3),
		},
		{
			"happy path value with comma", "<SHOPITEM><PRICE_VAT>1000,32</PRICE_VAT></SHOPITEM>", "", decimal.New(100032, -2),
		},
		{
			"happy path value with space and comma", "<SHOPITEM><PRICE_VAT>1 000,32</PRICE_VAT></SHOPITEM>", "", decimal.New(100032, -2),
		},
		{
			"happy path value with space and dot", "<SHOPITEM><PRICE_VAT>1 000.32</PRICE_VAT></SHOPITEM>", "", decimal.New(100032, -2),
		},
		{
			"value with multiple dots", "<SHOPITEM><PRICE_VAT>1.000.32</PRICE_VAT></SHOPITEM>", "Unmarshal of price '1.000.32' failed: error decoding string '1.000.32': can't convert 1.000.32 to decimal: too many .s", decimal.New(0, 0),
		},
		{
			"value with multiple commas", "<SHOPITEM><PRICE_VAT>1.000.32</PRICE_VAT></SHOPITEM>", "Unmarshal of price '1,000,32' failed: error decoding string '1.000.32': can't convert 1.000.32 to decimal: too many .s", decimal.New(0, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{}
			err := xml.Unmarshal([]byte(tt.xml), &item)
			if tt.err == "" {
				require.NoError(t, err)
				assert.True(t, tt.expected.Equal(item.PriceVAT.Decimal))
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestIDUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		xml      string
		err      string
		expected string
		attr     string
	}{
		{"No value", "<SHOPITEM></SHOPITEM>", "", "", ""},
		{"Empty node", "<SHOPITEM><ITEM_ID /></SHOPITEM>", "ID could not be unamarshaled. Check for ID requirements: ''", "", ""},
		{"Empty value", "<SHOPITEM><ITEM_ID></ITEM_ID></SHOPITEM>", "ID could not be unamarshaled. Check for ID requirements: ''", "", ""},
		{"Space value", "<SHOPITEM><ITEM_ID> </ITEM_ID></SHOPITEM>", "ID could not be unamarshaled. Check for ID requirements: ' '", "", ""},
		{"Value with space", "<SHOPITEM><ITEM_ID>dnmfms ndb</ITEM_ID></SHOPITEM>", "ID could not be unamarshaled. Check for ID requirements: 'dnmfms ndb'", "", ""},
		{"long value", "<SHOPITEM><ITEM_ID>dnmfmsnmbasmdbmsdbfmnsdbfmnsdvbmfnbnmzvmncxbvmzbcvmbmzxcvmndb</ITEM_ID></SHOPITEM>", "ID could not be unamarshaled. Check for ID requirements: 'dnmfmsnmbasmdbmsdbfmnsdbfmnsdvbmfnbnmzvmncxbvmzbcvmbmzxcvmndb'", "", ""},
		{"Happy path", "<SHOPITEM><ITEM_ID>abc123</ITEM_ID></SHOPITEM>", "", "abc123", ""},
		{"Happy path attr", "<SHOPITEM><ITEM_ID>abc123</ITEM_ID><GIFT ID=\"abc123 \"></GIFT></SHOPITEM>", "", "abc123", "abc123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{}
			err := xml.Unmarshal([]byte(tt.xml), &item)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, string(item.ID))
				if tt.attr != "" {
					assert.Equal(t, tt.attr, string(item.Gifts[0].ID))
				}
			}
		})
	}
}

func TestURLUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		xml      string
		err      string
		expected string
	}{
		{"No value", "<SHOPITEM></SHOPITEM>", "", ""},
		{"Empty node", "<SHOPITEM><URL /></SHOPITEM>", "", ""},
		{"Empty value", "<SHOPITEM><URL></URL></SHOPITEM>", "", ""},
		{"Space value", "<SHOPITEM><URL> </URL></SHOPITEM>", "", ""},
		{"Wrong value", "<SHOPITEM><URL>*^%$#@!)(</URL></SHOPITEM>", "The following URL '*^%$#@!)(' is wrong: parse \"*^%$\": invalid URL escape \"%$\"", ""},
		{"Relative URL", "<SHOPITEM><URL>/nsdnfm</URL></SHOPITEM>", "The following URL '/nsdnfm' is not absolute", ""},
		{"happy Path", "<SHOPITEM><URL>http://test.com/abc123</URL></SHOPITEM>", "", "http://test.com/abc123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{}
			err := xml.Unmarshal([]byte(tt.xml), &item)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, item.URL.String())
			}
		})
	}
}

func TestPercentUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		xml      string
		err      string
		expected string
	}{
		{"No value", "<SHOPITEM></SHOPITEM>", "", ""},
		{"Empty node", "<SHOPITEM><VAT /></SHOPITEM>", "", ""},
		{"Empty value", "<SHOPITEM><VAT></VAT></SHOPITEM>", "", ""},
		{"Space value", "<SHOPITEM><VAT> </VAT></SHOPITEM>", "", ""},
		{"Wrong value", "<SHOPITEM><VAT>*^%$#@!)(</VAT></SHOPITEM>", "Persentage value is incorrect: '*^%$#@!)('", ""},
		{"1000%", "<SHOPITEM><VAT>1000%</VAT></SHOPITEM>", "Persentage value is incorrect: '1000%'", ""},
		{"10", "<SHOPITEM><VAT>10</VAT></SHOPITEM>", "Persentage value is incorrect: '10'", ""},
		{"happy Path", "<SHOPITEM><VAT>10%</VAT></SHOPITEM>", "", "10%"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{}
			err := xml.Unmarshal([]byte(tt.xml), &item)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, string(item.VAT))
			}
		})
	}
}
