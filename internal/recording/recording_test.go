package recording

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

func sampleEntries() []*twpb.RecordingEntry {
	return []*twpb.RecordingEntry{
		{Entry: &twpb.RecordingEntry_Refdata{Refdata: &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Account{
			Account: &twpb.Account{AccountId: "ACC-0001", BeneficialOwnerId: "BO-0001"}}}}},
		{Entry: &twpb.RecordingEntry_Event{Event: &twpb.MarketEvent{Body: &twpb.MarketEvent_Order{Order: &twpb.OrderEvent{
			Header: &twpb.EventHeader{SessionId: "S", Symbol: "QZAP", Seq: 1}, OrderId: 1, Quantity: 100}}}}},
		{Entry: &twpb.RecordingEntry_Event{Event: &twpb.MarketEvent{Body: &twpb.MarketEvent_Trade{Trade: &twpb.TradeEvent{
			Header: &twpb.EventHeader{SessionId: "S", Symbol: "QZAP", Seq: 2}, TradeId: 1, Quantity: 100}}}}},
		{Entry: &twpb.RecordingEntry_Label{Label: &twpb.GroundTruthLabel{LabelId: "L1", Params: map[string]string{"a": "1"}}}},
	}
}

func writeAll(t *testing.T, w *Writer, entries []*twpb.RecordingEntry) {
	t.Helper()
	for _, e := range entries {
		var err error
		switch v := e.Entry.(type) {
		case *twpb.RecordingEntry_Refdata:
			err = w.WriteRefData(v.Refdata)
		case *twpb.RecordingEntry_Event:
			err = w.WriteEvent(v.Event)
		case *twpb.RecordingEntry_Label:
			err = w.WriteLabel(v.Label)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoundTripFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.twrec.gz")
	w, err := Create(path, &twpb.RecordingHeader{SessionId: "S", ConfigJson: `{"seed":1}`})
	if err != nil {
		t.Fatal(err)
	}
	want := sampleEntries()
	writeAll(t, w, want)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if h := r.Header(); h.GetSessionId() != "S" || h.GetFormatVersion() != FormatVersion {
		t.Fatalf("header = %v", h)
	}
	for i, w := range want {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
		if !proto.Equal(got, w) {
			t.Fatalf("entry %d: got %v, want %v", i, got, w)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func encode(t *testing.T, entries []*twpb.RecordingEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, &twpb.RecordingHeader{SessionId: "S"})
	if err != nil {
		t.Fatal(err)
	}
	writeAll(t, w, entries)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gzipped(raw []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(raw)
	gz.Close()
	return buf.Bytes()
}

func TestRejectsMalformedInput(t *testing.T) {
	// Decompressed payload of a valid recording, used to build broken ones.
	gzr, _ := gzip.NewReader(bytes.NewReader(encode(t, sampleEntries())))
	raw, _ := io.ReadAll(gzr)

	huge := binary.AppendUvarint(nil, MaxEntrySize+1)

	cases := map[string][]byte{
		"not gzip":         []byte("plain text"),
		"empty stream":     gzipped(nil),
		"truncated entry":  gzipped(raw[:len(raw)-3]),
		"oversized length": gzipped(append(append([]byte(nil), raw...), huge...)),
		"first not header": gzipped(append(binary.AppendUvarint(nil, 2), 0x12, 0x00)),
	}
	for name, data := range cases {
		r, err := NewReader(bytes.NewReader(data))
		if err == nil {
			for err == nil {
				_, err = r.Next()
			}
			if errors.Is(err, io.EOF) {
				t.Errorf("%s: read to EOF without an error", name)
				continue
			}
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: error %v is not ErrCorrupt", name, err)
		}
	}
}

// A recording that was flushed but never closed (the process crashed) must
// still yield every flushed entry before reporting the truncation.
func TestFlushedEntriesSurviveACrash(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, &twpb.RecordingHeader{SessionId: "S"})
	if err != nil {
		t.Fatal(err)
	}
	entries := sampleEntries()
	writeAll(t, w, entries)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// No Close: simulate a crash after the flush.
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if _, err := r.Next(); err != nil {
			t.Fatalf("entry %d unreadable after flush: %v", i, err)
		}
	}
	if _, err := r.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("missing gzip trailer should be reported as corruption, got %v", err)
	}
}

func TestRejectsUnknownVersion(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, &twpb.RecordingHeader{FormatVersion: 99})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := NewReader(&buf); err == nil {
		t.Fatal("version 99 accepted")
	}
}

// FuzzReader feeds arbitrary decompressed payloads to the reader. It must
// never panic or allocate beyond MaxEntrySize, and every failure must be
// reported as an error.
func FuzzReader(f *testing.F) {
	gzr, _ := gzip.NewReader(bytes.NewReader(encode(&testing.T{}, sampleEntries())))
	raw, _ := io.ReadAll(gzr)
	f.Add(raw)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Fuzz(func(t *testing.T, payload []byte) {
		r, err := NewReader(bytes.NewReader(gzipped(payload)))
		if err != nil {
			return
		}
		for i := 0; i < 10_000; i++ {
			if _, err := r.Next(); err != nil {
				return
			}
		}
	})
}
