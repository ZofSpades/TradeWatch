package rules

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"

	"go.yaml.in/yaml/v3"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// Taxonomy maps rule taxonomy codes to the market-abuse framework they
// refer to. TradeWatch uses an indicative mapping to the EU Market Abuse
// Regulation (MAR); it is an engineering aid, not legal advice.
type Taxonomy struct {
	Framework string
	Note      string
	Codes     map[string]*TaxonomyCode
}

// TaxonomyCode is one entry of the taxonomy.
type TaxonomyCode struct {
	Code       string   `yaml:"code"`
	Name       string   `yaml:"name"`
	AbuseType  string   `yaml:"abuse_type"`
	References []string `yaml:"references"`
}

// AbuseTypes maps taxonomy abuse types to their protobuf values.
var AbuseTypes = map[string]twpb.AbuseType{
	"wash_trading":      twpb.AbuseType_ABUSE_TYPE_WASH_TRADING,
	"spoofing":          twpb.AbuseType_ABUSE_TYPE_SPOOFING,
	"momentum_ignition": twpb.AbuseType_ABUSE_TYPE_MOMENTUM_IGNITION,
	"quote_stuffing":    twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING,
	"marking_the_close": twpb.AbuseType_ABUSE_TYPE_MARKING_THE_CLOSE,
}

// Proto returns the code's abuse type.
func (c *TaxonomyCode) Proto() twpb.AbuseType { return AbuseTypes[c.AbuseType] }

var codePattern = regexp.MustCompile(`^MAR-[A-Z]+$`)

func parseTaxonomy(src []byte) (*Taxonomy, error) {
	var doc struct {
		APIVersion string          `yaml:"apiVersion"`
		Kind       string          `yaml:"kind"`
		Framework  string          `yaml:"framework"`
		Note       string          `yaml:"note"`
		Codes      []*TaxonomyCode `yaml:"codes"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(src))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if doc.APIVersion != "tradewatch/v1" || doc.Kind != "Taxonomy" {
		return nil, errors.New("expected apiVersion tradewatch/v1 and kind Taxonomy")
	}
	if doc.Framework == "" || len(doc.Codes) == 0 {
		return nil, errors.New("framework and codes are required")
	}
	t := &Taxonomy{Framework: doc.Framework, Note: doc.Note, Codes: map[string]*TaxonomyCode{}}
	for _, c := range doc.Codes {
		switch {
		case !codePattern.MatchString(c.Code):
			return nil, fmt.Errorf("invalid code %q", c.Code)
		case c.Name == "" || len(c.References) == 0:
			return nil, fmt.Errorf("code %s needs a name and at least one reference", c.Code)
		case t.Codes[c.Code] != nil:
			return nil, fmt.Errorf("duplicate code %s", c.Code)
		}
		if _, ok := AbuseTypes[c.AbuseType]; !ok {
			return nil, fmt.Errorf("code %s: unknown abuse type %q", c.Code, c.AbuseType)
		}
		t.Codes[c.Code] = c
	}
	return t, nil
}
