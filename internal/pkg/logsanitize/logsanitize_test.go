package logsanitize

import "testing"

func TestSanitize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "clean value passes through unchanged", in: "ORD-2026-000042", want: "ORD-2026-000042"},
		{name: "newline is stripped (forged log line)", in: "evil\nINFO order shipped", want: "evilINFO order shipped"},
		{name: "carriage return is stripped", in: "evil\rFAKE", want: "evilFAKE"},
		{name: "CRLF injection is stripped", in: "a\r\nb", want: "ab"},
		{name: "tab passes through (cannot forge a line)", in: "col1\tcol2", want: "col1\tcol2"},
		{name: "other C0 controls pass through", in: "a\x00\x01\x1fb", want: "a\x00\x01\x1fb"},
		{name: "DEL passes through", in: "a\x7fb", want: "a\x7fb"},
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
