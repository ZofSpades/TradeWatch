// Package recording reads and writes session recordings.
//
// A recording is a gzip-compressed stream of tradewatch.v1.RecordingEntry
// messages, each preceded by its length as an unsigned varint. The first
// entry is always a RecordingHeader. The entries that follow are reference
// data, market events and ground-truth labels, in the order they were
// produced. Replaying the market events through the pipeline reproduces the
// session exactly (see cmd/replay).
package recording

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// FormatVersion is the version written into new recordings.
const FormatVersion = 1

// MaxEntrySize bounds a single entry, so a corrupt length prefix cannot
// trigger a huge allocation.
const MaxEntrySize = 16 << 20

// ErrCorrupt reports a malformed recording.
var ErrCorrupt = errors.New("recording: corrupt")

// Writer appends entries to a recording.
type Writer struct {
	dst   io.Writer
	close io.Closer
	gz    *gzip.Writer
	bw    *bufio.Writer
	buf   []byte
	err   error
}

// Create creates the recording file at path and writes its header.
func Create(path string, header *twpb.RecordingHeader) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w, err := NewWriter(f, header)
	if err != nil {
		f.Close()
		return nil, err
	}
	w.close = f
	return w, nil
}

// NewWriter starts a recording on dst and writes its header.
func NewWriter(dst io.Writer, header *twpb.RecordingHeader) (*Writer, error) {
	gz := gzip.NewWriter(dst)
	w := &Writer{dst: dst, gz: gz, bw: bufio.NewWriterSize(gz, 256<<10)}
	if header.GetFormatVersion() == 0 {
		header.FormatVersion = FormatVersion
	}
	if err := w.write(&twpb.RecordingEntry{Entry: &twpb.RecordingEntry_Header{Header: header}}); err != nil {
		return nil, err
	}
	return w, nil
}

// WriteRefData appends a reference-data record.
func (w *Writer) WriteRefData(r *twpb.RefDataRecord) error {
	return w.write(&twpb.RecordingEntry{Entry: &twpb.RecordingEntry_Refdata{Refdata: r}})
}

// WriteEvent appends a market event.
func (w *Writer) WriteEvent(ev *twpb.MarketEvent) error {
	return w.write(&twpb.RecordingEntry{Entry: &twpb.RecordingEntry_Event{Event: ev}})
}

// WriteLabel appends a ground-truth label.
func (w *Writer) WriteLabel(l *twpb.GroundTruthLabel) error {
	return w.write(&twpb.RecordingEntry{Entry: &twpb.RecordingEntry_Label{Label: l}})
}

func (w *Writer) write(e *twpb.RecordingEntry) error {
	if w.err != nil {
		return w.err
	}
	var err error
	w.buf, err = events.MarshalAppend(w.buf[:0], e)
	if err == nil && len(w.buf) > MaxEntrySize {
		err = fmt.Errorf("recording: entry of %d bytes exceeds the %d byte limit", len(w.buf), MaxEntrySize)
	}
	if err == nil {
		var n [binary.MaxVarintLen64]byte
		if _, err = w.bw.Write(n[:binary.PutUvarint(n[:], uint64(len(w.buf)))]); err == nil {
			_, err = w.bw.Write(w.buf)
		}
	}
	w.err = err
	return err
}

// Flush writes buffered entries through to the destination, so that a crash
// loses at most what was written after the last flush. Everything before a
// flush stays readable even if the recording is never closed.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if err := w.bw.Flush(); err != nil {
		w.err = err
		return err
	}
	if err := w.gz.Flush(); err != nil {
		w.err = err
		return err
	}
	return nil
}

// Close flushes the recording and closes the underlying file, if Create
// opened it.
func (w *Writer) Close() error {
	err := w.err
	if e := w.bw.Flush(); err == nil {
		err = e
	}
	if e := w.gz.Close(); err == nil {
		err = e
	}
	if w.close != nil {
		if e := w.close.Close(); err == nil {
			err = e
		}
	}
	return err
}

// Reader reads entries from a recording.
type Reader struct {
	br     *bufio.Reader
	gz     *gzip.Reader
	close  io.Closer
	header *twpb.RecordingHeader
	buf    []byte
}

// Open opens the recording at path and reads its header.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	r.close = f
	return r, nil
}

// NewReader reads a recording from src and validates its header.
func NewReader(src io.Reader) (*Reader, error) {
	gz, err := gzip.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	r := &Reader{br: bufio.NewReaderSize(gz, 256<<10), gz: gz}
	first, err := r.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: missing header", ErrCorrupt)
		}
		return nil, err
	}
	h := first.GetHeader()
	if h == nil {
		return nil, fmt.Errorf("%w: first entry is not a header", ErrCorrupt)
	}
	if h.GetFormatVersion() != FormatVersion {
		return nil, fmt.Errorf("recording: unsupported format version %d", h.GetFormatVersion())
	}
	r.header = h
	return r, nil
}

// Header returns the recording header.
func (r *Reader) Header() *twpb.RecordingHeader { return r.header }

// Next returns the next entry, or io.EOF after the last one.
func (r *Reader) Next() (*twpb.RecordingEntry, error) {
	n, err := binary.ReadUvarint(r.br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: length prefix: %v", ErrCorrupt, err)
	}
	if n > MaxEntrySize {
		return nil, fmt.Errorf("%w: entry length %d exceeds the limit", ErrCorrupt, n)
	}
	if uint64(cap(r.buf)) < n {
		r.buf = make([]byte, n)
	}
	r.buf = r.buf[:n]
	if _, err := io.ReadFull(r.br, r.buf); err != nil {
		return nil, fmt.Errorf("%w: truncated entry: %v", ErrCorrupt, err)
	}
	e := &twpb.RecordingEntry{}
	if err := events.Unmarshal(r.buf, e); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return e, nil
}

// Close releases the reader and closes the file, if Open opened it.
func (r *Reader) Close() error {
	err := r.gz.Close()
	if r.close != nil {
		if e := r.close.Close(); err == nil {
			err = e
		}
	}
	return err
}
