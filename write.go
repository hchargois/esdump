package main

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/charmbracelet/log"
)

func (d *dumper) write(ctx context.Context) error {
	var buf bytes.Buffer
	var stop bool
	for hit := range d.scrolledCh {
		if ctx.Err() != nil || stop {
			continue
		}

		// Elasticsearch returns the document's _source exactly as it was
		// indexed: if it was indexed with newlines, it will return newlines.
		// But for the JSONL format, each hit must be on its own line.
		// So we need to check if there are newlines, and remove them.
		if bytes.IndexByte(hit, '\n') != -1 {
			err := json.Compact(&buf, hit)
			if err != nil {
				log.Error("compacting hit into single-line JSON", "err", err)
				return err
			}
			hit = buf.Bytes()
		}

		_, err := d.out.Write(hit)
		if err != nil {
			log.Error("writing to stdout", "err", err)
			return err
		}
		err = d.out.WriteByte('\n')
		if err != nil {
			log.Error("writing to stdout", "err", err)
			return err
		}
		buf.Reset()

		dumped := atomic.AddUint64(&d.dumped, 1)
		if d.count > 0 && dumped >= d.count {
			stop = true
		}
	}
	return nil
}
