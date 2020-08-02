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
			name:     "empty value",
			xml:      "<SHOPITEM></SHOPITEM>",
			err:      "",
			expected: decimal.Decimal{},
		},
		{
			name:     "random value",
			xml:      "<SHOPITEM><PRICE_VAT>werwer</PRICE_VAT></SHOPITEM>",
			err:      "Unmarshal of price 'werwer' failed: error decoding string 'werwer': can't convert werwer to decimal: exponent is not numeric",
			expected: decimal.Decimal{},
		},
		{
			name:     "zero value",
			xml:      "<SHOPITEM><PRICE_VAT>0</PRICE_VAT></SHOPITEM>",
			err:      "",
			expected: decimal.Zero,
		},
		{
			name:     "happy path value with space",
			xml:      "<SHOPITEM><PRICE_VAT>1 000</PRICE_VAT></SHOPITEM>",
			err:      "",
			expected: decimal.New(1, 3),
		},
		{
			name:     "happy path value with comma",
			xml:      "<SHOPITEM><PRICE_VAT>1000,32</PRICE_VAT></SHOPITEM>",
			err:      "",
			expected: decimal.New(100032, -2),
		},
		{
			name:     "happy path value with space and comma",
			xml:      "<SHOPITEM><PRICE_VAT>1 000,32</PRICE_VAT></SHOPITEM>",
			err:      "",
			expected: decimal.New(100032, -2),
		},
		{
			name:     "happy path value with space and dot",
			xml:      "<SHOPITEM><PRICE_VAT>1 000.32</PRICE_VAT></SHOPITEM>",
			err:      "",
			expected: decimal.New(100032, -2),
		},
		{
			name:     "value with multiple dots",
			xml:      "<SHOPITEM><PRICE_VAT>1.000.32</PRICE_VAT></SHOPITEM>",
			err:      "Unmarshal of price '1.000.32' failed: error decoding string '1.000.32': can't convert 1.000.32 to decimal: too many .s",
			expected: decimal.New(0, 0),
		},
		{
			name:     "value with multiple commas",
			xml:      "<SHOPITEM><PRICE_VAT>1.000.32</PRICE_VAT></SHOPITEM>",
			err:      "Unmarshal of price '1,000,32' failed: error decoding string '1.000.32': can't convert 1.000.32 to decimal: too many .s",
			expected: decimal.New(0, 0),
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
