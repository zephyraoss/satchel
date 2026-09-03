package units

import "testing"

func TestParseBytes(t *testing.T) {
	cases := map[string]int64{"64MiB": 64 << 20, "1G": 1 << 30, " 4096 ": 4096, "2KiB": 2048, "7B": 7}
	for input, want := range cases {
		got, err := ParseBytes(input)
		if err != nil || got != want {
			t.Fatalf("ParseBytes(%q) = %d, %v; want %d", input, got, err, want)
		}
	}
	for _, input := range []string{"", "-1", "0", "abc", "9223372036854775807GiB", "1TiB"} {
		if _, err := ParseBytes(input); err == nil {
			t.Fatalf("ParseBytes(%q) accepted", input)
		}
	}
}
