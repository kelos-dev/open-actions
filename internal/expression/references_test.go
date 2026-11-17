package expression

import (
	"slices"
	"testing"
)

func TestContextReferences(t *testing.T) {
	for _, test := range []struct {
		input string
		names []string
		all   bool
	}{
		{input: "plain secrets.NAME"},
		{input: "${{ 'secrets.NAME' }}"},
		{input: "${{ github.token }}"},
		{input: "${{ secrets.Token }}", names: []string{"TOKEN"}},
		{input: "${{ SECRETS['Token'] }}", names: []string{"TOKEN"}},
		{input: "${{ secrets.TOKEN }}-${{ secrets.token }}", names: []string{"TOKEN"}},
		{input: "${{ false && secrets.TOKEN }}", names: []string{"TOKEN"}},
		{input: "${{ !secrets.TOKEN || format('{0}', secrets.OTHER) }}", names: []string{"OTHER", "TOKEN"}},
		{input: "${{ secrets.TOKEN.nested }}", names: []string{"TOKEN"}},
		{input: "${{ secrets['TOKEN'][env.NAME] }}", names: []string{"TOKEN"}},
		{input: "${{ env[secrets.TOKEN] }}", names: []string{"TOKEN"}},
		{input: "${{ secrets[env.NAME] }}", all: true},
		{input: "${{ secrets[secrets.NAME] }}", all: true},
		{input: "${{ secrets[format('{0}', 'TOKEN')] }}", all: true},
		{input: "${{ secrets.* }}", all: true},
		{input: "${{ secrets[*] }}", all: true},
		{input: "${{ toJSON(secrets) }}", all: true},
		{input: "${{ secrets }}", all: true},
	} {
		t.Run(test.input, func(t *testing.T) {
			program, err := Parse(test.input)
			if err != nil {
				t.Fatal(err)
			}
			names, all := program.ContextReferences("secrets")
			if !slices.Equal(names, test.names) || all != test.all {
				t.Fatalf("ContextReferences() = %v, %t; want %v, %t", names, all, test.names, test.all)
			}
		})
	}
}
