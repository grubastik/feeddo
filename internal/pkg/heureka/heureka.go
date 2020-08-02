package heureka

import (
	"bytes"
	"encoding/xml"
	"fmt"

	"github.com/shopspring/decimal"
)

// Shop contains list of available shop items
type Shop struct {
	XMLName  xml.Name `xml:"SHOP"`
	ShopItem []Item   `xml:"SHOPITEM"`
}

// Item - describes single shop item
type Item struct {
	XMLName           xml.Name    `xml:"SHOPITEM"`
	ID                string      `xml:"ITEM_ID" json:"id"`
	ProductName       string      `xml:"PRODUCTNAME" json:"name"`
	Product           string      `xml:"PRODUCT" json:"product"`
	Description       string      `xml:"DESCRIPTION" json:"description"`
	URL               string      `xml:"URL" json:"url"`
	ImgURL            string      `xml:"IMGURL" json:"imageUrl"`
	ImgURLAlternative []string    `xml:"IMGURL_ALTERNATIVE" json:"imageUrlsAlternate"`
	VideoURL          string      `xml:"VIDEO_URL" json:"videoUrl"`
	PriceVAT          Price       `xml:"PRICE_VAT" json:"priceWithVat"`
	VAT               string      `xml:"VAT,omitempty" json:"vat"`
	Type              string      `xml:"ITEM_TYPE,omitempty" json:"type"`
	HeurekaCPC        Price       `xml:"HEUREKA_CPC,omitempty" json:"cpc"`
	Manufacturer      string      `xml:"MANUFACTURER" json:"manufacterer"`
	CategoryText      string      `xml:"CATEGORYTEXT" json:"category"`
	EAN               string      `xml:"EAN" json:"ean"`
	ISBN              string      `xml:"ISBN" json:"isbn"`
	Parameters        []Parameter `xml:"PARAM" json:"parameters"`
	DeliveryDate      string      `xml:"DELIVERY_DATE" json:"deliveryDay"`
	Deliveries        []Delivery  `xml:"DELIVERY" json:"deliveries"`
	GroupID           string      `xml:"ITEMGROUP_ID" json:"groupId"`
	Accessories       []string    `xml:"ACCESSORY" json:"accessories"`
	Dues              Price       `xml:"DUES" json:"dues"`
	Gifts             []Gift      `xml:"GIFT" json:"gifts"`
}

// Parameter - describes product parameter
type Parameter struct {
	Name  string `xml:"PARAM_NAME" json:"name"`
	Value string `xml:"VAL" json:"value"`
}

// Delivery - describes delivery option
type Delivery struct {
	ID       string `xml:"DELIVERY_ID" json:"id"`
	Price    Price  `xml:"DELIVERY_PRICE" json:"price"`
	PriceCOD Price  `xml:"DELIVERY_PRICE_COD" json:"priceCod"`
}

// Gift - describes item which will be added to the order as free item
type Gift struct {
	Name string `xml:",chardata" json:"name"`
	ID   string `xml:"ID,attr" json:"id"`
}

// Price - represents price in app
type Price struct {
	decimal.Decimal
}

// UnmarshalText unmarshall price with all rules or returns error
func (p *Price) UnmarshalText(text []byte) error {
	text = bytes.ReplaceAll(text, []byte(" "), []byte{})    // remove spaces
	text = bytes.ReplaceAll(text, []byte(","), []byte(".")) //replace commas with dots

	err := p.Decimal.UnmarshalText(text)
	if err != nil {
		return fmt.Errorf("Unmarshal of price '%s' failed: %v", string(text), err)
	}

	return nil
}
