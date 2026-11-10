package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, input string, maxBulk int) (cmds [][]string, err error) {
	t.Helper()
	r := NewReader(strings.NewReader(input), maxBulk)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return cmds, err
		}
		cmd := make([]string, len(args))
		for i, a := range args {
			cmd[i] = string(a)
		}
		cmds = append(cmds, cmd)
	}
}

func TestReadCommand(t *testing.T) {
	input := "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" + // array form
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n" + // empty argument
		"*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n" + // binary-safe: CRLF inside an argument
		"PING\r\n" + // inline
		"  SET  k   v \n" + // inline, bare LF, extra spaces
		"\r\n" + // blank line
		"*0\r\n" + // empty array
		"*1\r\n$4\r\nPING\r\n"

	cmds, err := readAll(t, input, 1024)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("final error = %v; want io.EOF", err)
	}
	want := [][]string{
		{"GET", "k"},
		{"SET", "k", ""},
		{"SET", "a\r\nb"},
		{"PING"},
		{"SET", "k", "v"},
		{},
		{},
		{"PING"},
	}
	if len(cmds) != len(want) {
		t.Fatalf("read %d commands %q; want %d", len(cmds), cmds, len(want))
	}
	for i := range want {
		if strings.Join(cmds[i], "\x00") != strings.Join(want[i], "\x00") || len(cmds[i]) != len(want[i]) {
			t.Errorf("command %d = %q; want %q", i, cmds[i], want[i])
		}
	}
}

func TestReadCommandErrors(t *testing.T) {
	var perr *ProtocolError
	for name, input := range map[string]string{
		"bad array length":       "*x\r\n",
		"huge array length":      "*99999999\r\n",
		"missing bulk marker":    "*1\r\n:5\r\n",
		"bad bulk length":        "*1\r\n$x\r\n",
		"negative bulk length":   "*1\r\n$-1\r\n",
		"bulk over the limit":    "*1\r\n$17\r\n" + strings.Repeat("a", 17) + "\r\n",
		"bulk without CRLF":      "*1\r\n$3\r\nabcde\r\n",
		"line longer than limit": strings.Repeat("a", maxInlineLen+10) + "\r\n",
	} {
		if _, err := readAll(t, input, 16); !errors.As(err, &perr) {
			t.Errorf("%s: err = %v; want a ProtocolError", name, err)
		}
	}

	// A connection that closes mid-command is not a clean EOF.
	for name, input := range map[string]string{
		"in array header": "*2",
		"in bulk header":  "*2\r\n$3",
		"in bulk body":    "*2\r\n$3\r\nGE",
		"between args":    "*2\r\n$3\r\nGET\r\n",
	} {
		if _, err := readAll(t, input, 16); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("truncated %s: err = %v; want io.ErrUnexpectedEOF", name, err)
		}
	}
}

func TestBuffered(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\nPING\r\n"), 16)
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if r.Buffered() == 0 {
		t.Fatal("Buffered = 0 with a pipelined command pending")
	}
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if r.Buffered() != 0 {
		t.Fatalf("Buffered = %d after the last command; want 0", r.Buffered())
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SimpleString("OK")
	w.Error("ERR bad\r\nthing")
	w.Int(-42)
	w.Bulk([]byte("a\r\nb"))
	w.Bulk(nil)
	w.BulkString("hi")
	w.Null()
	w.ArrayLen(2)
	if buf.Len() != 0 {
		t.Fatal("replies reached the connection before Flush")
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n" +
		"-ERR bad  thing\r\n" +
		":-42\r\n" +
		"$4\r\na\r\nb\r\n" +
		"$0\r\n\r\n" +
		"$2\r\nhi\r\n" +
		"$-1\r\n" +
		"*2\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("wrote %q; want %q", got, want)
	}
}
