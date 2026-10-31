package logsanitize

import "testing"

func TestSanitize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "clean value passes through unchanged", in: "ORD-2026-000042", want: "ORD-2026-000042"},
		{name: "newline becomes space (forged log line)", in: "evil\nINFO order shipped", want: "evil INFO order shipped"},
		{name: "carriage return becomes space", in: "evil\rFAKE", want: "evil FAKE"},
		{name: "CRLF injection becomes two spaces", in: "a\r\nb", want: "a  b"},
		{name: "tab becomes space", in: "col1\tcol2", want: "col1 col2"},
		{name: "NUL and other C0 controls become spaces", in: "a\x00\x01\x1fb", want: "a   b"},
		{name: "DEL becomes space", in: "a\x7fb", want: "a b"},
		{name: "non-ASCII printable runes pass through", in: "síté-Ünïcode-倉庫", want: "síté-Ünïcode-倉庫"},
		{name: "empty string stays empty", in: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Sanitize(tc.in); got != tc.want {
				t.Fatalf("Sanitize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
