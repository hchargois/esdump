package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/mattn/go-isatty"
)

type obj map[string]any

func (d *dumper) createQuery() {
	q := make(obj)

	if !isatty.IsTerminal(os.Stdin.Fd()) {
		in, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal("reading from stdin", "err", err)
		}
		log.Info("read query from stdin", "bytes", len(in))

		err = json.Unmarshal(in, &q)
		if err != nil {
			log.Fatal("parsing query from stdin", "err", err)
		}
	}

	if _, ok := q["_source"]; !ok && d.fields != "" {
		exclude := false
		if strings.HasPrefix(d.fields, "^") {
			exclude = true
			d.fields = d.fields[1:]
		}
		fieldsList := strings.Split(d.fields, ",")
		if exclude {
			q["_source"] = obj{
				"excludes": fieldsList,
			}
		} else {
			q["_source"] = fieldsList
		}
	}
	if d.metadataOnly {
		q["_source"] = false
	}
	if _, ok := q["size"]; !ok {
		q["size"] = d.size
	}
	if _, ok := q["sort"]; !ok {
		q["sort"] = []string{"_doc"}
	}
	if _, ok := q["query"]; !ok {
		if d.queryString != "" {
			q["query"] = obj{
				"query_string": obj{
					"query": d.queryString,
				},
			}
		} else {
			q["query"] = obj{
				"match_all": obj{},
			}
		}
	}
	if d.random {
		q["query"] = obj{
			"function_score": obj{
				"query":        q["query"],
				"random_score": obj{},
				"boost_mode":   "replace",
			},
		}
		q["sort"] = []string{"_score"}
	}
	d.query = q
}
