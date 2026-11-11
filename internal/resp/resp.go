// Package resp reads client commands and writes replies in RESP2, the Redis
// wire protocol.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	// maxArgs bounds the number of arguments in one command.
	maxArgs = 1 << 20
	// maxInlineLen bounds the length of an inline command and of any
	// protocol line.
	maxInlineLen = 64 << 10
)

// ProtocolError means the client sent bytes that are not valid RESP. The
// connection cannot be resynchronised and should be closed after reporting
// it.
type ProtocolError struct{ msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.msg }

func protoErr(format string, a ...any) error {
	return &ProtocolError{msg: fmt.Sprintf(format, a...)}
}

// Reader reads commands from a client connection.
type Reader struct {
	br      *bufio.Reader
	maxBulk int
}

// NewReader returns a Reader that rejects arguments larger than maxBulk
// bytes.
func NewReader(r io.Reader, maxBulk int) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, maxInlineLen), maxBulk: maxBulk}
}

// Buffered reports how many bytes of input are already in memory. Zero means
// the client is waiting for replies, so it is time to flush them.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads the next command: either a RESP array of bulk strings,
// or an inline command (a plain line of space-separated words, as typed into
// telnet or nc). The result is empty for an empty array or a blank line.
func (r *Reader) ReadCommand() ([][]byte, error) {
	first, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] != '*' {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		fields := bytes.Fields(line)
		args := make([][]byte, len(fields))
		for i, f := range fields {
			args[i] = bytes.Clone(f)
		}
		return args, nil
	}

	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	n, err := parseLen(line[1:])
	if err != nil || n > maxArgs {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil
	}
	args := make([][]byte, 0, min(n, 64))
	for i := 0; i < n; i++ {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, protoErr("expected '$', got %q", firstByte(line))
		}
		size, err := parseLen(line[1:])
		if err != nil || size < 0 || size > r.maxBulk {
			return nil, protoErr("invalid bulk length")
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, unexpectedEOF(err)
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, protoErr("bulk string is not terminated by CRLF")
		}
		args = append(args, buf[:size:size])
	}
	return args, nil
}

// readLine returns the next line without its line ending. The result is only
// valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, protoErr("too big inline request")
	}
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

// unexpectedEOF reports a connection that closed in the middle of a command.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func parseLen(b []byte) (int, error) {
	n, err := strconv.ParseInt(string(b), 10, 32)
	return int(n), err
}

func firstByte(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b[:1])
}

// Writer writes replies to a client connection. Write errors are remembered
// and returned by Flush.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
	errors  uint64
}

// NewWriter returns a buffered Writer; nothing reaches w until Flush.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 64<<10)}
}

// Flush sends buffered replies to the client.
func (w *Writer) Flush() error { return w.bw.Flush() }

// ErrorCount returns how many error replies have been written.
func (w *Writer) ErrorCount() uint64 { return w.errors }

// SimpleString writes a status reply such as +OK. s must not contain CR or
// LF.
func (w *Writer) SimpleString(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Error writes an error reply. By convention msg starts with an upper-case
// code such as "ERR ". CR and LF are replaced so the reply stays one line.
func (w *Writer) Error(msg string) {
	w.errors++
	w.bw.WriteByte('-')
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		w.bw.WriteByte(c)
	}
	w.bw.WriteString("\r\n")
}

// Int writes an integer reply.
func (w *Writer) Int(n int64) { w.header(':', n) }

// Bulk writes a bulk string reply. A nil slice is written as an empty
// string; use Null for "no value".
func (w *Writer) Bulk(b []byte) {
	w.header('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// BulkString is Bulk for a string.
func (w *Writer) BulkString(s string) {
	w.header('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Null writes the null bulk string, the reply for a missing value.
func (w *Writer) Null() { w.bw.WriteString("$-1\r\n") }

// ArrayLen starts an array reply of n elements; the caller writes the n
// elements next.
func (w *Writer) ArrayLen(n int) { w.header('*', int64(n)) }

func (w *Writer) header(kind byte, n int64) {
	w.scratch = append(w.scratch[:0], kind)
	w.scratch = strconv.AppendInt(w.scratch, n, 10)
	w.scratch = append(w.scratch, '\r', '\n')
	w.bw.Write(w.scratch)
}
