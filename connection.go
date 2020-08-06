package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrClosed is throw when an error with the sonic server
	// come from the state of the connection.

	// ErrChanName is throw when the channel name is not supported
	// by sonic server.
	ErrChanName = errors.New("invalid channel name")
)

// Channel refer to the list of channels available.
type Channel string

const (
	// Search  is used for querying the search index.
	Search Channel = "search"

	// Ingest is used for altering the search index (push, pop and flush).
	Ingest Channel = "ingest"

	// Control is used for administration purposes.
	Control Channel = "control"
)

// Conn 链接实例
type Conn struct {
	createdAt time.Time
	usedAt    int64 // atomic

	pooled      bool
	Reader      *bufio.Reader
	netConn     net.Conn
	cmdMaxBytes int
	closed      bool
}

// GetUsedAt ...
func (cn *Conn) GetUsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

// SetUsedAt ...
func (cn *Conn) setUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// GetCreatedAt ...
func (cn *Conn) GetCreatedAt() time.Time {
	return cn.createdAt
}

// Buffered ...
func (cn *Conn) Buffered() int {
	return cn.Reader.Buffered()
}

// NewConn ...
func NewConn(netConn net.Conn, ch Channel, password string) (conn *Conn, err error) {

	conn = &Conn{
		netConn:   netConn,
		usedAt:    time.Now().Unix(),
		createdAt: time.Now(),
		Reader:    bufio.NewReader(netConn),
	}

	conn.setUsedAt(time.Now())

	err = conn.write(fmt.Sprintf("START %s %s", ch, password))
	if err != nil {
		return nil, err
	}

	_, err = conn.read()     // CONNECTED
	line, err := conn.read() // STARTED
	if err != nil {
		return nil, err
	}

	ss := strings.FieldsFunc(line, func(r rune) bool {
		if unicode.IsSpace(r) || r == '(' || r == ')' {
			return true
		}
		return false
	})
	bufferSize, err := strconv.Atoi(ss[len(ss)-1])
	if err != nil {
		return nil, fmt.Errorf("Unable to parse STARTED response: %s", line)
	}
	conn.cmdMaxBytes = bufferSize

	return conn, nil
}

// Read read line from conn
func (cn *Conn) Read() (string, error) {
	if cn.closed {
		return "", ErrClosed
	}
	buffer := bytes.Buffer{}
	for {
		line, isPrefix, err := cn.Reader.ReadLine()
		buffer.Write(line)
		if err != nil {
			if err == io.EOF {
				cn.Close()
			}
			return "", err
		}
		if !isPrefix {
			break
		}
	}

	str := buffer.String()
	if strings.HasPrefix(str, "ERR ") {
		return "", errors.New(str[4:])
	}

	return str, nil
}

// Write write with end of "\r\n"
func (cn *Conn) Write(str string) error {
	if cn.closed {
		return ErrClosed
	}

	buf := bytes.Buffer{}
	buf.WriteString(str)
	buf.WriteString("\r\n")

	_, err := cn.netConn.Write(buf.Bytes())
	return err
}

// ---------------------finished-----------------
// --------------------unfinished----------------

type searchClient struct {
	*Conn
}

type ingestClient struct {
	*Conn
}

type controlClient struct {
	*Conn
}

func (c *Conn) search(password string) (*searchClient, error) {
	err := c.write(fmt.Sprintf("START %s %s", Search, password))
	if err != nil {
		return nil, err
	}
	_, err = c.read()
	_, err = c.read()
	if err != nil {
		return nil, err
	}
	return &searchClient{c}, nil
}

func (c *Conn) ingest(password string) (*ingestClient, error) {
	err := c.write(fmt.Sprintf("START %s %s", Ingest, password))
	if err != nil {
		return nil, err
	}
	_, err = c.read()
	_, err = c.read()
	if err != nil {
		return nil, err
	}
	return &ingestClient{c}, nil
}

func (c *Conn) control(password string) (*controlClient, error) {
	err := c.write(fmt.Sprintf("START %s %s", Control, password))
	if err != nil {
		return nil, err
	}
	_, err = c.read()
	_, err = c.read()
	if err != nil {
		return nil, err
	}
	return &controlClient{c}, nil
}

func newConnection(endpoint, password string, channel Channel) (*Conn, error) {
	c := &Conn{}
	c.Close()
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}

	c.closed = false
	c.netConn = conn
	c.Reader = bufio.NewReader(c.netConn)

	return c, nil
}

func (c *Conn) read() (string, error) {
	if c.closed {
		return "", ErrClosed
	}
	buffer := bytes.Buffer{}
	for {
		line, isPrefix, err := c.Reader.ReadLine()
		buffer.Write(line)
		if err != nil {
			if err == io.EOF {
				c.Close()
			}
			return "", err
		}
		if !isPrefix {
			break
		}
	}

	str := buffer.String()
	if strings.HasPrefix(str, "ERR ") {
		return "", errors.New(str[4:])
	}

	return str, nil
}

func (c Conn) write(str string) error {
	if c.closed {
		return ErrClosed
	}
	_, err := c.netConn.Write([]byte(str + "\r\n"))
	return err
}

func (c *Conn) Close() (err error) {
	if c.netConn != nil {
		err = c.netConn.Close()
		c.netConn = nil
	}
	c.closed = true
	c.Reader = nil
	return err
}

// Ensure splitting on a valid leading byte
// Slicing the string directly is more efficient than converting to []byte and back because
// since a string is immutable and a []byte isn't,
// the data must be copied to new memory upon conversion,
// taking O(n) time (both ways!),
// whereas slicing a string simply returns a new string header backed by the same array as the original
// (taking constant time).
func (c *Conn) splitText(longString string) []string {
	var splits []string

	var l, r int
	for l, r = 0, c.cmdMaxBytes/2; r < len(longString); l, r = r, r+c.cmdMaxBytes/2 {
		for !utf8.RuneStart(longString[r]) {
			r--
		}
		splits = append(splits, longString[l:r])
	}
	splits = append(splits, longString[l:])
	return splits
}

func (c *Conn) push(collection, bucket, object, text string) (err error) {
	patterns := []struct {
		Pattern     string
		Replacement string
	}{{"\\", "\\\\"},
		{"\n", "\\n"},
		{"\"", "\\\""}}
	for _, v := range patterns {
		text = strings.Replace(text, v.Pattern, v.Replacement, -1)
	}

	chunks := c.splitText(text)
	// split chunks with partial success will yield single error
	for _, chunk := range chunks {
		err = c.write(fmt.Sprintf("%s %s %s %s \"%s\"", push, collection, bucket, object, chunk))

		if err != nil {
			return err
		}

		// sonic should sent OK
		_, err = c.read()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Conn) Pop(collection, bucket, object, text string) (err error) {
	err = c.write(fmt.Sprintf("%s %s %s %s \"%s\"", pop, collection, bucket, object, text))
	if err != nil {
		return err
	}

	// sonic should sent OK
	_, err = c.read()
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) Count(collection, bucket, object string) (cnt int, err error) {
	err = c.write(fmt.Sprintf("%s %s %s", count, collection, buildCountQuery(bucket, object)))
	if err != nil {
		return 0, err
	}

	// RESULT NUMBER
	r, err := c.read()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(r[7:])
}

func buildCountQuery(bucket, object string) string {
	builder := strings.Builder{}
	if bucket != "" {
		builder.WriteString(bucket)
		if object != "" {
			builder.WriteString(" " + object)
		}
	}
	return builder.String()
}

func (c *Conn) FlushCollection(collection string) (err error) {
	err = c.write(fmt.Sprintf("%s %s", flushc, collection))
	if err != nil {
		return err
	}

	// sonic should sent OK
	_, err = c.read()
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) FlushBucket(collection, bucket string) (err error) {
	err = c.write(fmt.Sprintf("%s %s %s", flushb, collection, bucket))
	if err != nil {
		return err
	}

	// sonic should sent OK
	_, err = c.read()
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) FlushObject(collection, bucket, object string) (err error) {
	err = c.write(fmt.Sprintf("%s %s %s %s", flusho, collection, bucket, object))
	if err != nil {
		return err
	}

	// sonic should sent OK
	_, err = c.read()
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) Query(collection, bucket, term string, limit, offset int) (results []string, err error) {
	err = c.write(fmt.Sprintf("%s %s %s \"%s\" LIMIT(%d) OFFSET(%d)", query, collection, bucket, term, limit, offset))
	if err != nil {
		return nil, err
	}

	// pending, should be PENDING ID_EVENT
	_, err = c.read()
	if err != nil {
		return nil, err
	}

	// event query, should be EVENT QUERY ID_EVENT RESULT1 RESULT2 ...
	read, err := c.read()
	if err != nil {
		return nil, err
	}
	return getSearchResults(read, string(query)), nil
}

func (c *Conn) Suggest(collection, bucket, word string, limit int) (results []string, err error) {
	err = c.write(fmt.Sprintf("%s %s %s \"%s\" LIMIT(%d)", suggest, collection, bucket, word, limit))
	if err != nil {
		return nil, err
	}

	// pending, should be PENDING ID_EVENT
	_, err = c.read()
	if err != nil {
		return nil, err
	}

	// event query, should be EVENT SUGGEST ID_EVENT RESULT1 RESULT2 ...
	read, err := c.read()
	if err != nil {
		return nil, err
	}
	return getSearchResults(read, string(suggest)), nil
}

func getSearchResults(line string, eventType string) []string {
	if strings.HasPrefix(line, "EVENT "+eventType) {
		return strings.Split(line, " ")[3:]
	}
	return []string{}
}
