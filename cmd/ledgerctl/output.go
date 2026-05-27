package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// printer dispatches output to JSON or table format based on the --output flag.
type printer struct {
	w      io.Writer
	format string
}

func newPrinter() *printer {
	return &printer{w: os.Stdout, format: outputFormat}
}

// print dispatches to JSON or table based on --output flag.
// If the value is a protobuf message, it uses protojson for JSON output.
// tableFunc is called only for table format.
func (p *printer) print(v any, tableFunc func(*printer)) error {
	if p.format == "json" {
		if msg, ok := v.(proto.Message); ok {
			return p.printProtoJSON(msg)
		}
		return p.printJSON(v)
	}
	tableFunc(p)
	return nil
}

func (p *printer) printJSON(v any) error {
	enc := json.NewEncoder(p.w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func (p *printer) printProtoJSON(msg proto.Message) error {
	opts := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}
	b, err := opts.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(p.w, string(b))
	return err
}

// printTable prints rows with headers using tabwriter.
func (p *printer) printTable(headers []string, rows [][]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for i, h := range headers {
		if i > 0 {
			fmt.Fprint(tw, "\t")
		}
		fmt.Fprint(tw, h)
	}
	fmt.Fprintln(tw)
	for _, row := range rows {
		for i, col := range row {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, col)
		}
		fmt.Fprintln(tw)
	}
	tw.Flush()
}

// printSingle prints a single key-value pair list (for detail views).
func (p *printer) printSingle(pairs [][2]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for _, pair := range pairs {
		fmt.Fprintf(tw, "%s\t%s\n", pair[0], pair[1])
	}
	tw.Flush()
}
