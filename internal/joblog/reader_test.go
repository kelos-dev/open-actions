package joblog

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestWithoutTimestampsPreservesOutput(t *testing.T) {
	stamp := "2026-08-10T12:34:56.123456789Z "
	long := strings.Repeat("x", 256<<10) + " 2026-08-10T12:34:56Z literal timestamp"
	input := stamp + "first\r\n" + stamp + "\n" + "untimestamped text\n" + stamp + long + "\n" + stamp + "last"
	want := "first\r\n\nuntimestamped text\n" + long + "\nlast"
	output, err := io.ReadAll(WithoutTimestamps(iotest.OneByteReader(strings.NewReader(input))))
	if err != nil || string(output) != want {
		t.Fatalf("output bytes = %d, want %d; error = %v", len(output), len(want), err)
	}
}

func TestWithoutTimestampsPreservesReadErrorAndPartialLine(t *testing.T) {
	failure := errors.New("incomplete archive")
	source := io.MultiReader(strings.NewReader("2026-08-10T12:34:56Z partial line"), iotest.ErrReader(failure))
	reader := WithoutTimestamps(source)
	var output strings.Builder
	buffer := make([]byte, 3)
	for {
		n, err := reader.Read(buffer)
		output.Write(buffer[:n])
		if err != nil {
			if !errors.Is(err, failure) {
				t.Fatalf("read error = %v", err)
			}
			break
		}
	}
	if output.String() != "partial line" {
		t.Fatalf("output = %q", output.String())
	}
}
