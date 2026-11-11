package causaltimeline

import (
	"fmt"

	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

// depRef is one task reference in a depends expression.
type depRef struct {
	Task   string
	Result common.TaskResult
}

// mustParseRefs tokenizes a depends expression through the shared grammar in
// workflow/common so the analyzer reads exactly the same references as
// validation and the DAG evaluator.
func mustParseRefs(expr string) []depRef {
	refs, err := common.ParseDepends(expr)
	if err != nil {
		return nil
	}
	out := make([]depRef, 0, len(refs))
	for _, r := range refs {
		out = append(out, depRef{Task: r.Task, Result: r.Result})
	}
	return out
}

// supportersOf returns the references whose observed task result actually
// makes the depends expression true. For an OR expression only disjuncts with
// observed support are returned: ancestors that did not contribute are never
// force-linked. The second result is true when some literal could not be
// evaluated from status evidence, in which case an empty support list must be
// treated as unknown rather than proven false.
func supportersOf(expr string, observed map[string]observedResult) ([]depRef, bool) {
	tree, err := parseBoolExpr(expr)
	if err != nil {
		return nil, true
	}
	_, unknown, refs := tree.eval(observed)
	return dedupRefs(refs), unknown
}

func refText(r depRef) string {
	if r.Result == "" {
		return r.Task
	}
	return r.Task + "." + string(r.Result)
}

func dedupRefs(in []depRef) []depRef {
	seen := map[string]bool{}
	var out []depRef
	for _, r := range in {
		key := refText(r)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out
}

type boolExpr interface {
	eval(observed map[string]observedResult) (value bool, unknown bool, supporters []depRef)
	refs() []depRef
}

type literalExpr struct{ ref depRef }
type notExpr struct{ inner boolExpr }
type andExpr struct{ parts []boolExpr }
type orExpr struct{ parts []boolExpr }

func (e literalExpr) refs() []depRef { return []depRef{e.ref} }

func (e notExpr) refs() []depRef { return e.inner.refs() }

func (e andExpr) refs() []depRef {
	var out []depRef
	for _, part := range e.parts {
		out = append(out, part.refs()...)
	}
	return out
}

func (e orExpr) refs() []depRef {
	var out []depRef
	for _, part := range e.parts {
		out = append(out, part.refs()...)
	}
	return out
}

func (e literalExpr) eval(observed map[string]observedResult) (bool, bool, []depRef) {
	o, ok := observed[e.ref.Task]
	if !ok || !o.exists {
		return false, true, nil
	}
	if o.matches(e.ref.Result) {
		return true, false, []depRef{e.ref}
	}
	return false, false, nil
}

func (e notExpr) eval(observed map[string]observedResult) (bool, bool, []depRef) {
	value, unknown, _ := e.inner.eval(observed)
	if unknown {
		return false, true, nil
	}
	// When the negation holds, the observed facts about its inner references
	// are the supporting evidence.
	return !value, false, e.inner.refs()
}

func (e andExpr) eval(observed map[string]observedResult) (bool, bool, []depRef) {
	var supporters []depRef
	unknown := false
	for _, part := range e.parts {
		value, u, refs := part.eval(observed)
		supporters = append(supporters, refs...)
		if u {
			unknown = true
		}
		if !value {
			return false, unknown, nil
		}
	}
	return true, unknown, supporters
}

func (e orExpr) eval(observed map[string]observedResult) (bool, bool, []depRef) {
	var supporters []depRef
	unknown := false
	for _, part := range e.parts {
		value, u, refs := part.eval(observed)
		if u {
			unknown = true
		}
		if value {
			supporters = append(supporters, refs...)
		}
	}
	if len(supporters) > 0 {
		return true, false, supporters
	}
	return false, unknown, nil
}

// tokenKind enumerates the small expression token set used by depends.
type tokenKind int

const (
	tokenIdentifier tokenKind = iota
	tokenAnd
	tokenOr
	tokenNot
	tokenOpen
	tokenClose
)

type token struct {
	kind tokenKind
	text string
}

// tokenize performs structural tokenization; identifier spelling is validated
// against the shared grammar's parse afterwards.
func tokenize(expr string) ([]token, error) {
	var tokens []token
	for i := 0; i < len(expr); {
		switch expr[i] {
		case ' ', '\t', '\n':
			i++
		case '(':
			tokens = append(tokens, token{kind: tokenOpen, text: "("})
			i++
		case ')':
			tokens = append(tokens, token{kind: tokenClose, text: ")"})
			i++
		case '!':
			tokens = append(tokens, token{kind: tokenNot, text: "!"})
			i++
		case '&':
			if i+1 >= len(expr) || expr[i+1] != '&' {
				return nil, fmt.Errorf("unexpected '&' in depends expression %q", expr)
			}
			tokens = append(tokens, token{kind: tokenAnd, text: "&&"})
			i += 2
		case '|':
			if i+1 >= len(expr) || expr[i+1] != '|' {
				return nil, fmt.Errorf("unexpected '|' in depends expression %q", expr)
			}
			tokens = append(tokens, token{kind: tokenOr, text: "||"})
			i += 2
		default:
			start := i
			for i < len(expr) {
				c := expr[i]
				if c == ' ' || c == '\t' || c == '\n' || c == '(' || c == ')' || c == '!' || c == '&' || c == '|' {
					break
				}
				i++
			}
			tokens = append(tokens, token{kind: tokenIdentifier, text: expr[start:i]})
		}
	}
	return tokens, nil
}

type boolParser struct {
	tokens []token
	pos    int
	refs   map[string]depRef
}

func parseBoolExpr(expr string) (boolExpr, error) {
	structure, err := tokenize(expr)
	if err != nil {
		return nil, err
	}
	refs := mustParseRefs(expr)
	byText := make(map[string]depRef, len(refs))
	for _, r := range refs {
		byText[refText(r)] = r
	}
	p := &boolParser{tokens: structure, refs: byText}
	tree, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.tokens) {
		return nil, fmt.Errorf("trailing tokens in depends expression %q", expr)
	}
	return tree, nil
}

func (p *boolParser) parseOr() (boolExpr, error) {
	var parts []boolExpr
	for {
		part, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
		if p.accept(tokenOr) == nil {
			break
		}
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return orExpr{parts: parts}, nil
}

func (p *boolParser) parseAnd() (boolExpr, error) {
	var parts []boolExpr
	for {
		part, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
		if p.accept(tokenAnd) == nil {
			break
		}
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return andExpr{parts: parts}, nil
}

func (p *boolParser) parseAtom() (boolExpr, error) {
	if p.accept(tokenNot) != nil {
		inner, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		return notExpr{inner: inner}, nil
	}
	if p.accept(tokenOpen) != nil {
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.accept(tokenClose) == nil {
			return nil, fmt.Errorf("missing closing parenthesis in depends expression")
		}
		return inner, nil
	}
	tok := p.accept(tokenIdentifier)
	if tok == nil {
		return nil, fmt.Errorf("expected task reference in depends expression, got %v", p.tokens)
	}
	ref, ok := p.refs[tok.text]
	if !ok {
		return nil, fmt.Errorf("reference %q is not a valid depends reference", tok.text)
	}
	return literalExpr{ref: ref}, nil
}

func (p *boolParser) accept(kind tokenKind) *token {
	if p.pos >= len(p.tokens) || p.tokens[p.pos].kind != kind {
		return nil
	}
	tok := p.tokens[p.pos]
	p.pos++
	return &tok
}
