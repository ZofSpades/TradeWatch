package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// Instrument is a simulated listed symbol. All symbols are fictional.
type Instrument struct {
	Symbol   string
	Name     string
	Tier     twpb.LiquidityTier
	RefPrice int64 // opening reference price, in price units
	TickSize int64
	LotSize  int64
	// DailyVol is the daily volatility of the symbol's fair value.
	DailyVol float64
}

// Role is the simulator's internal classification of an account. It is
// never published: reference data only exposes the account kind and its
// ownership, as a real venue would.
type Role uint8

const (
	RoleMarketMaker Role = iota + 1
	RoleRetail
	RoleInstitutional
	RoleProp

	// Roles used only by the manipulation injector.

	// RoleWasher owns two accounts that trade with each other.
	RoleWasher
	// RoleRingMember colludes in a circular trading ring; every member of a
	// ring is a different beneficial owner.
	RoleRingMember
	RoleSpoofer
	RoleMomentumIgniter
	RoleQuoteStuffer
	RoleCloseMarker
)

// Account is a trading account in the simulated universe.
type Account struct {
	ID              string
	BeneficialOwner string
	Entity          string
	Kind            twpb.AccountKind
	Name            string
	Role            Role
	// Firm groups the accounts of one legal entity, in generation order.
	Firm int
	// Ring is the trading ring a RoleRingMember account belongs to, from 1.
	Ring int
}

// Universe is the fixed set of instruments and accounts a simulation runs
// over. It is identical across sessions so that reference data is stable.
type Universe struct {
	Instruments []Instrument
	Accounts    []*Account
	byRole      map[Role][]*Account
	byID        map[string]*Account
}

// ByRole returns the accounts with the given role, in a stable order.
func (u *Universe) ByRole(r Role) []*Account { return u.byRole[r] }

// Rings returns the members of each trading ring, ring 1 first.
func (u *Universe) Rings() [][]*Account {
	var rings [][]*Account
	for _, a := range u.byRole[RoleRingMember] {
		for len(rings) < a.Ring {
			rings = append(rings, nil)
		}
		rings[a.Ring-1] = append(rings[a.Ring-1], a)
	}
	return rings
}

// Account returns the account with the given ID.
func (u *Universe) Account(id string) (*Account, bool) {
	a, ok := u.byID[id]
	return a, ok
}

// Instrument returns the instrument with the given symbol.
func (u *Universe) Instrument(symbol string) (Instrument, bool) {
	for _, in := range u.Instruments {
		if in.Symbol == symbol {
			return in, true
		}
	}
	return Instrument{}, false
}

const priceUnit = 10_000 // price units per currency unit

func px(dollars float64) int64 { return int64(dollars*priceUnit + 0.5) }

// defaultInstruments are eight fictional symbols across three liquidity
// tiers.
func defaultInstruments() []Instrument {
	high, med, low := twpb.LiquidityTier_LIQUIDITY_TIER_HIGH, twpb.LiquidityTier_LIQUIDITY_TIER_MEDIUM, twpb.LiquidityTier_LIQUIDITY_TIER_LOW
	tick := int64(100) // 0.01
	return []Instrument{
		{Symbol: "QZAP", Name: "Quartz Applied Systems", Tier: high, RefPrice: px(125.40), TickSize: tick, LotSize: 1, DailyVol: 0.018},
		{Symbol: "VNTR", Name: "Ventra Logistics", Tier: high, RefPrice: px(48.20), TickSize: tick, LotSize: 1, DailyVol: 0.022},
		{Symbol: "ORBT", Name: "Orbital Grid Corp", Tier: med, RefPrice: px(77.50), TickSize: tick, LotSize: 1, DailyVol: 0.025},
		{Symbol: "KRYL", Name: "Krylon Materials", Tier: med, RefPrice: px(22.10), TickSize: tick, LotSize: 1, DailyVol: 0.028},
		{Symbol: "MOSS", Name: "Mossbank Holdings", Tier: med, RefPrice: px(310.40), TickSize: tick, LotSize: 1, DailyVol: 0.016},
		{Symbol: "TALX", Name: "Talix Biosciences", Tier: low, RefPrice: px(9.85), TickSize: tick, LotSize: 1, DailyVol: 0.035},
		{Symbol: "WREN", Name: "Wren Coastal Energy", Tier: low, RefPrice: px(14.60), TickSize: tick, LotSize: 1, DailyVol: 0.032},
		{Symbol: "YUMA", Name: "Yuma Precision Tools", Tier: low, RefPrice: px(63.30), TickSize: tick, LotSize: 1, DailyVol: 0.030},
	}
}

// firmSpec describes a group of accounts owned by one beneficial owner.
type firmSpec struct {
	role     Role
	kind     twpb.AccountKind
	name     string
	accounts int
	ring     int
}

var marketMakerFirms = []string{"Northbeam Liquidity", "Halcyon Markets", "Ferro Quant Partners"}
var institutionalFirms = []string{"Bluewater Pension Trust", "Juniper Ridge Asset Mgmt", "Calloway Index Funds", "Sterling Oak Advisors", "Meridian Endowment", "Ashgrove Capital"}
var propFirms = []string{"Arclight Trading", "Quillfeather Capital", "Driftwood Prop", "Copperline Securities"}

// Firms whose accounts only the manipulation injector uses. Their kinds look
// like any other participant's and nothing in reference data marks them.
var (
	washFirms     = []string{"Blackfen Holdings", "Oriel Street Partners", "Tamsin Ventures", "Grayhook Capital"}
	ringMembers   = []string{"Lumen Bay Trading", "Kestrel Row LLC", "Pallas Point", "Cobalt Wharf", "Ivory Gate Partners", "Saltmarsh Capital", "Nimbus Lane", "Fernhill Markets", "Quarry Hill Assets"}
	spoofers      = []string{"Vantage Arc Capital", "Redline Prop", "Halberd Trading", "Mistral Quant"}
	momentumFirms = []string{"Zephyr Prop", "Corvid Capital", "Juno Rapids", "Brightwater Trading"}
	quoteStuffers = []string{"Tachyon Systems", "Flicker Labs", "Hyperion Microstructure"}
	closeMarkers  = []string{"Westgate Asset Mgmt", "Ostrava Partners", "Pinecrest Advisory", "Silverthorn Capital", "Harrowgate Funds", "Marlowe Street Capital", "Eastvale Partners", "Tidewater Investors"}
	ringSize      = 3
)

const retailAccounts = 300

// DefaultUniverse returns the standard universe: eight symbols, three
// market-making firms, six institutional firms with two desks each, four
// proprietary trading firms and 300 retail clients. Account, owner and
// entity identifiers are assigned through a fixed permutation, so an ID
// reveals nothing about the account's role.
func DefaultUniverse() *Universe {
	var firms []firmSpec
	for _, n := range marketMakerFirms {
		firms = append(firms, firmSpec{role: RoleMarketMaker, kind: twpb.AccountKind_ACCOUNT_KIND_MARKET_MAKER, name: n, accounts: 1})
	}
	for _, n := range institutionalFirms {
		firms = append(firms, firmSpec{role: RoleInstitutional, kind: twpb.AccountKind_ACCOUNT_KIND_INSTITUTIONAL, name: n, accounts: 2})
	}
	for _, n := range propFirms {
		firms = append(firms, firmSpec{role: RoleProp, kind: twpb.AccountKind_ACCOUNT_KIND_PROPRIETARY, name: n, accounts: 1})
	}
	for i := 1; i <= retailAccounts; i++ {
		firms = append(firms, firmSpec{role: RoleRetail, kind: twpb.AccountKind_ACCOUNT_KIND_RETAIL, name: fmt.Sprintf("Retail client %03d", i), accounts: 1})
	}
	prop, retail, inst := twpb.AccountKind_ACCOUNT_KIND_PROPRIETARY, twpb.AccountKind_ACCOUNT_KIND_RETAIL, twpb.AccountKind_ACCOUNT_KIND_INSTITUTIONAL
	for i, n := range washFirms {
		kind := prop
		if i%2 == 1 {
			kind = retail
		}
		firms = append(firms, firmSpec{role: RoleWasher, kind: kind, name: n, accounts: 2})
	}
	for i, n := range ringMembers {
		firms = append(firms, firmSpec{role: RoleRingMember, kind: retail, name: n, accounts: 1, ring: 1 + i/ringSize})
	}
	for _, n := range spoofers {
		firms = append(firms, firmSpec{role: RoleSpoofer, kind: prop, name: n, accounts: 1})
	}
	for _, n := range momentumFirms {
		firms = append(firms, firmSpec{role: RoleMomentumIgniter, kind: prop, name: n, accounts: 1})
	}
	for _, n := range quoteStuffers {
		firms = append(firms, firmSpec{role: RoleQuoteStuffer, kind: prop, name: n, accounts: 1})
	}
	for i, n := range closeMarkers {
		kind := inst
		if i%2 == 1 {
			kind = prop
		}
		firms = append(firms, firmSpec{role: RoleCloseMarker, kind: kind, name: n, accounts: 1})
	}
	return buildUniverse(defaultInstruments(), firms)
}

func buildUniverse(instruments []Instrument, firms []firmSpec) *Universe {
	nAccounts := 0
	for _, f := range firms {
		nAccounts += f.accounts
	}
	// A fixed permutation decouples identifiers from generation order.
	perm := rand.New(rand.NewPCG(0x7d3a_11c5, 0x5eed)).Perm
	accountIDs := perm(nAccounts)
	ownerIDs := perm(len(firms))

	u := &Universe{
		Instruments: instruments,
		byRole:      map[Role][]*Account{},
		byID:        map[string]*Account{},
	}
	next := 0
	for fi, f := range firms {
		owner := fmt.Sprintf("BO-%04d", 1+ownerIDs[fi])
		entity := fmt.Sprintf("ENT-%04d", 1+ownerIDs[fi])
		if f.role == RoleRetail {
			// Retail clients are distinct owners trading through one of
			// three brokers.
			entity = fmt.Sprintf("ENT-RB%d", 1+fi%3)
		}
		for k := 0; k < f.accounts; k++ {
			name := f.name
			if f.accounts > 1 {
				name = fmt.Sprintf("%s desk %d", f.name, k+1)
			}
			a := &Account{
				ID:              fmt.Sprintf("ACC-%04d", 1+accountIDs[next]),
				BeneficialOwner: owner,
				Entity:          entity,
				Kind:            f.kind,
				Name:            name,
				Role:            f.role,
				Firm:            fi,
				Ring:            f.ring,
			}
			next++
			u.Accounts = append(u.Accounts, a)
			u.byRole[f.role] = append(u.byRole[f.role], a)
			u.byID[a.ID] = a
		}
	}
	return u
}

// RefData returns the universe as reference-data records, accounts first in
// ID order, then instruments.
func (u *Universe) RefData() []*twpb.RefDataRecord {
	out := make([]*twpb.RefDataRecord, 0, len(u.Accounts)+len(u.Instruments))
	accounts := append([]*Account(nil), u.Accounts...)
	slices.SortFunc(accounts, func(a, b *Account) int { return strings.Compare(a.ID, b.ID) })
	for _, a := range accounts {
		out = append(out, &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Account{Account: &twpb.Account{
			AccountId: a.ID, BeneficialOwnerId: a.BeneficialOwner, EntityId: a.Entity, Kind: a.Kind, DisplayName: a.Name,
		}}})
	}
	for _, in := range u.Instruments {
		out = append(out, &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Instrument{Instrument: &twpb.Instrument{
			Symbol: in.Symbol, Name: in.Name, TickSize: in.TickSize, LotSize: in.LotSize,
			LiquidityTier: in.Tier, ReferencePrice: in.RefPrice,
		}}})
	}
	return out
}
