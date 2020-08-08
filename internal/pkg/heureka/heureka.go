package heureka

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/url"
	"regexp"

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
	ID                ID          `xml:"ITEM_ID" json:"id"`
	ProductName       string      `xml:"PRODUCTNAME" json:"name"`
	Product           string      `xml:"PRODUCT" json:"product"`
	Description       string      `xml:"DESCRIPTION" json:"description"`
	URL               URL         `xml:"URL" json:"url"`
	ImgURL            URL         `xml:"IMGURL" json:"imageUrl"`
	ImgURLAlternative []URL       `xml:"IMGURL_ALTERNATIVE" json:"imageUrlsAlternate"`
	VideoURL          URL         `xml:"VIDEO_URL" json:"videoUrl"`
	PriceVAT          Price       `xml:"PRICE_VAT" json:"priceWithVat"`
	VAT               Percent     `xml:"VAT,omitempty" json:"vat"`
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
	ID   ID     `xml:"ID,attr" json:"id"`
}

// Percent contains percentage value
type Percent string

// UnmarshalText - validates percentage value
func (p *Percent) UnmarshalText(text []byte) error {
	if len(bytes.TrimSpace(text)) == 0 {
		return nil
	}
	re := regexp.MustCompile(`^1?\d?\d%$`)
	if !re.Match(bytes.TrimSpace(text)) {
		return fmt.Errorf("Persentage value is incorrect: '%s'", text)
	}
	*p = Percent(bytes.TrimSpace(text))
	return nil
}

// URL validates url
type URL struct {
	url.URL
}

// UnmarshalText - unmarshall url with validation
func (u *URL) UnmarshalText(text []byte) error {
	if len(bytes.TrimSpace(text)) == 0 {
		return nil
	}
	ur, err := url.Parse(string(bytes.TrimSpace(text)))
	if err != nil {
		return fmt.Errorf("The following URL '%s' is wrong: %w", text, err)
	}
	if !ur.IsAbs() {
		return fmt.Errorf("The following URL '%s' is not absolute", text)
	}

	*u = URL{URL: *ur}
	return nil
}

// ID - contains validated ID
type ID string

// UnmarshalText - unmarshal and vaidate ID
func (id *ID) UnmarshalText(text []byte) error {
	re := regexp.MustCompile(`^[\w-_]{1,36}$`)
	if !re.Match(bytes.TrimSpace(text)) {
		return fmt.Errorf("ID could not be unamarshaled. Check for ID requirements: '%s'", text)
	}
	*id = ID(bytes.TrimSpace(text))
	return nil
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
