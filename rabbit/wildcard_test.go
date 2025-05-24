package rabbit

import (
	"testing"
)

func TestRabbitMQWildcardToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		matches []string
		noMatch []string
	}{
		{
			pattern: "#",
			matches: []string{"stock.nyse.usd", "stock.nasdaq.usd", "", "a", "a.b.c"},
			noMatch: []string{},
		},
		{
			pattern: "stock.*.usd",
			matches: []string{"stock.nyse.usd", "stock.nasdaq.usd"},
			noMatch: []string{"stock.nyse.eur", "stock.nyse.usd.east", "stock..usd", "stock.nyse."},
		},
		{
			pattern: "log.#",
			matches: []string{"log", "log.info", "log.error.critical"},
			noMatch: []string{"logs.error", "foo.log", "log."},
		},
		{
			pattern: "*.critical.#",
			matches: []string{"app.critical", "app.critical.error", "db.critical.error.disk"},
			noMatch: []string{"critical.app", "app.error.critical", ".critical", "app..critical"},
		},
		{
			pattern: "a.b.c",
			matches: []string{"a.b.c"},
			noMatch: []string{"a.b", "a.b.c.d", "a.b.d"},
		},
		{
			pattern: "*",
			matches: []string{"word", "anything", "abc123"},
			noMatch: []string{"a.b", "a.b.c"},
		},
		{
			pattern: "a.#.c",
			matches: []string{"a.b.c", "a.b.d.e.c", "a.c"},
			noMatch: []string{"a.b", "a.b.c.d"},
		},
		{
			pattern: "#.b.c",
			matches: []string{"a.b.c", "b.c", "x.y.z.b.c"},
			noMatch: []string{"a.b", "b.c.d"},
		},
		{
			pattern: "a.b.#",
			matches: []string{"a.b", "a.b.c", "a.b.c.d.e", "a.b.c.d.e.fg"},
			noMatch: []string{"a.bc"},
		},
		{
			pattern: "a.*.*",
			matches: []string{"a.b.c", "a.x.y"},
			noMatch: []string{"a.b", "a.b.c.d", "a..c"},
		},
		{
			pattern: "",
			matches: []string{""},
			noMatch: []string{"a", ".", "a.b"},
		},
	}

	for _, test := range tests {
		re, err := rabbitMQWildcardToRegex(test.pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range test.matches {
			if !re.MatchString(m) {
				t.Errorf("pattern %q should match %q", test.pattern, m)
			}
		}
		for _, nm := range test.noMatch {
			if re.MatchString(nm) {
				t.Errorf("pattern %q should NOT match %q", test.pattern, nm)
			}
		}
	}
}
