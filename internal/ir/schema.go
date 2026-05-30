package ir

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// SchemaEnforcementMode controls how schema validation is applied.
type SchemaEnforcementMode string

const (
	SchemaStrict    SchemaEnforcementMode = "strict"
	SchemaBestEffort SchemaEnforcementMode = "best_effort"
	SchemaDisabled  SchemaEnforcementMode = "disabled"
)

// ChartOfAccounts represents a tree of valid account patterns.
// Each node can be a fixed segment, a variable segment ($name), or a leaf (.self).
type ChartOfAccounts struct {
	Root map[string]*ChartNode `json:"root"`
}

// ChartNode represents a single node in the chart of accounts tree.
type ChartNode struct {
	// Children are sub-segments. Key is literal name or "$varName".
	Children map[string]*ChartNode `json:",omitempty"`
	// Pattern is a regex constraint for variable segments.
	Pattern string `json:".pattern,omitempty"`
	// compiledPattern is the pre-compiled regex for Pattern, set at parse time.
	compiledPattern *regexp.Regexp
	// IsLeaf marks this as a valid terminal account.
	IsLeaf bool `json:".self,omitempty"`
	// DefaultMetadata is applied when the account is first created.
	DefaultMetadata map[string]any `json:".metadata,omitempty"`
}

// ParseChartOfAccounts parses a JSON chart of accounts document.
func ParseChartOfAccounts(data []byte) (*ChartOfAccounts, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing chart of accounts: %w", err)
	}

	root := make(map[string]*ChartNode)
	for key, val := range raw {
		node, err := parseNode(val)
		if err != nil {
			return nil, fmt.Errorf("parsing node %q: %w", key, err)
		}
		root[key] = node
	}

	return &ChartOfAccounts{Root: root}, nil
}

func parseNode(data json.RawMessage) (*ChartNode, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	node := &ChartNode{
		Children: make(map[string]*ChartNode),
	}

	for key, val := range raw {
		switch key {
		case ".pattern":
			var p string
			if err := json.Unmarshal(val, &p); err != nil {
				return nil, fmt.Errorf("unmarshaling .pattern: %w", err)
			}
			node.Pattern = p
			compiled, err := regexp.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("compiling pattern %q: %w", p, err)
			}
			node.compiledPattern = compiled
		case ".self":
			node.IsLeaf = true
		case ".metadata":
			var m map[string]any
			if err := json.Unmarshal(val, &m); err != nil {
				return nil, fmt.Errorf("unmarshaling .metadata: %w", err)
			}
			node.DefaultMetadata = m
		default:
			child, err := parseNode(val)
			if err != nil {
				return nil, fmt.Errorf("child %q: %w", key, err)
			}
			node.Children[key] = child
		}
	}

	// A node with children but no explicit .self is also a leaf if it has no children at all (implicit leaf).
	if len(node.Children) == 0 && !node.IsLeaf {
		node.IsLeaf = true
	}

	return node, nil
}

// ValidateAccount checks if an account address is valid according to the chart.
// Returns nil if valid, error if the address doesn't match any path in the chart.
func (c *ChartOfAccounts) ValidateAccount(address string) error {
	if c == nil || len(c.Root) == 0 {
		return nil // no chart = all valid
	}

	segments := strings.Split(address, ":")
	return c.validateSegments(segments, c.Root, address)
}

func (c *ChartOfAccounts) validateSegments(segments []string, nodes map[string]*ChartNode, fullAddr string) error {
	if len(segments) == 0 {
		return fmt.Errorf("account %q: incomplete address for chart", fullAddr)
	}

	seg := segments[0]
	remaining := segments[1:]

	// Try exact match first.
	if node, ok := nodes[seg]; ok {
		return c.matchNode(remaining, node, fullAddr)
	}

	// Try variable matches.
	for key, node := range nodes {
		if !strings.HasPrefix(key, "$") {
			continue
		}
		// Check pattern constraint if any.
		if node.compiledPattern != nil {
			if !node.compiledPattern.MatchString(seg) {
				continue
			}
		}
		return c.matchNode(remaining, node, fullAddr)
	}

	return fmt.Errorf("account %q: segment %q not allowed by chart of accounts", fullAddr, seg)
}

func (c *ChartOfAccounts) matchNode(remaining []string, node *ChartNode, fullAddr string) error {
	if len(remaining) == 0 {
		if node.IsLeaf || len(node.Children) == 0 {
			return nil
		}
		return fmt.Errorf("account %q: not a valid leaf in chart of accounts", fullAddr)
	}

	if len(node.Children) == 0 {
		return fmt.Errorf("account %q: too many segments for chart of accounts", fullAddr)
	}

	return c.validateSegments(remaining, node.Children, fullAddr)
}

// ValidatePosting checks if both source and destination of a posting are valid.
func (c *ChartOfAccounts) ValidatePosting(source, destination string) error {
	if err := c.ValidateAccount(source); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := c.ValidateAccount(destination); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	return nil
}
