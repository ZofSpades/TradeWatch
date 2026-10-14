package eval

import (
	"fmt"
	"html"
	"math"
	"strings"
)

// SVG draws the sweep: recall, precision and F1 for each parameter value,
// with the production value marked. The chart has its own white background
// so that it stays legible in dark-mode viewers.
func (sw SweepResult) SVG() string {
	const width, height = 640.0, 360.0
	const left, right, top, bottom = 56.0, 24.0, 64.0, 56.0
	pw, ph := width-left-right, height-top-bottom
	n := len(sw.Values)
	x := func(i int) float64 {
		if n == 1 {
			return left + pw/2
		}
		return left + float64(i)*pw/float64(n-1)
	}
	y := func(v float64) float64 { return top + (1-v)*ph }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#1f2937">`+"\n",
		width, height, width, height)
	fmt.Fprintf(&b, `<rect width="%.0f" height="%.0f" fill="#ffffff"/>`+"\n", width, height)
	fmt.Fprintf(&b, `<text x="%.0f" y="24" font-size="15" font-weight="bold">%s: %s</text>`+"\n", left, html.EscapeString(sw.RuleID), html.EscapeString(sw.Param))
	for i := 0; i <= 5; i++ {
		v := float64(i) / 5
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="#e5e7eb"/>`+"\n", left, width-right, y(v), y(v))
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" dominant-baseline="middle">%.1f</text>`+"\n", left-8, y(v), v)
	}
	for i, v := range sw.Values {
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle">%s</text>`+"\n", x(i), height-bottom+20, html.EscapeString(v))
		if v == sw.Production {
			fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="#6b7280" stroke-dasharray="4 4"/>`+"\n", x(i), x(i), top, height-bottom)
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#6b7280">production</text>`+"\n", x(i), top-6)
		}
	}
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle">%s</text>`+"\n", left+pw/2, height-14, html.EscapeString(sw.Param))

	series := []struct {
		name, color, dash string
		value             func(*TypeStats) float64
	}{
		{"recall", "#2563eb", "", func(s *TypeStats) float64 { return rateValue(s.Recall()) }},
		{"precision", "#ea580c", "", func(s *TypeStats) float64 { return rateValue(s.Precision()) }},
		{"F1", "#16a34a", "6 4", func(s *TypeStats) float64 { return s.F1() }},
	}
	for k, se := range series {
		var path strings.Builder
		pen := false
		for i := range sw.Values {
			v := se.value(sw.Points[i].stats(sw.Type))
			if math.IsNaN(v) {
				pen = false
				continue
			}
			cmd := "L"
			if !pen {
				cmd = "M"
			}
			fmt.Fprintf(&path, "%s%.1f %.1f ", cmd, x(i), y(v))
			pen = true
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="%s"/>`+"\n", x(i), y(v), se.color)
		}
		dash := ""
		if se.dash != "" {
			dash = fmt.Sprintf(` stroke-dasharray="%s"`, se.dash)
		}
		fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2"%s/>`+"\n", strings.TrimSpace(path.String()), se.color, dash)
		lx := width - right - 300 + float64(k)*100
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="40" y2="40" stroke="%s" stroke-width="2"%s/>`+"\n", lx, lx+22, se.color, dash)
		fmt.Fprintf(&b, `<text x="%.1f" y="40" dominant-baseline="middle">%s</text>`+"\n", lx+28, se.name)
	}
	b.WriteString("</svg>\n")
	return b.String()
}

func rateValue(r Rate) float64 {
	if !r.Defined() {
		return math.NaN()
	}
	return r.Value
}
