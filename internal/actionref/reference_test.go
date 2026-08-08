package actionref

import "testing"

func TestParse(t *testing.T) {
	reference, err := Parse("owner/repository/subdirectory@feature/test")
	if err != nil {
		t.Fatal(err)
	}
	if reference.Owner != "owner" || reference.Repository != "repository" || reference.Path != "subdirectory" || reference.Ref != "feature/test" {
		t.Errorf("reference = %#v", reference)
	}
}

func TestParseRejectsUnsafeReferences(t *testing.T) {
	for _, value := range []string{
		"./action",
		"docker://image@latest",
		"owner/repository",
		"owner/repository/../action@v1",
		"owner/repository@--upload-pack=command",
		"owner/repository@refs/heads/main lock",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := Parse(value); err == nil {
				t.Fatalf("Parse(%q) succeeded", value)
			}
		})
	}
}
