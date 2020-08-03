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
	"unicode"
	"unicode/utf8"
)

var (
	// ErrClosed is throw when an error with the sonic server
	// come from the state of the connection.
	ErrClosed = errors.New("sonic connection is closed")

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

type ingesterCommands string

const (
	push   ingesterCommands = "PUSH"
	pop    ingesterCommands = "POP"
	count  ingesterCommands = "COUNT"
	flushb ingesterCommands = "FLUSHB"
	flushc ingesterCommands = "FLUSHC"
	flusho ingesterCommands = "FLUSHO"
)

type searchCommands string

const (
	query   searchCommands = "QUERY"
	suggest searchCommands = "SUGGEST"
)

type connection struct {
	reader      *bufio.Reader
	conn        net.Conn
	cmdMaxBytes int
	closed      bool
}

type searchClient struct {
	*connection
}

type ingestClient struct {
	*connection
}

type controlClient struct {
	*connection
}

func (c *connection) search(password string) (*searchClient, error) {
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

func (c *connection) ingest(password string) (*ingestClient, error) {
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

func (c *connection) control(password string) (*controlClient, error) {
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

func newConnection(endpoint, password string, channel Channel) (*connection, error) {
	c := &connection{}
	c.close()
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}

	c.closed = false
	c.conn = conn
	c.reader = bufio.NewReader(c.conn)

	return c, nil
}

func (c *connection) read() (string, error) {
	if c.closed {
		return "", ErrClosed
	}
	buffer := bytes.Buffer{}
	for {
		line, isPrefix, err := c.reader.ReadLine()
		buffer.Write(line)
		if err != nil {
			if err == io.EOF {
				c.close()
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
	if strings.HasPrefix(str, "STARTED ") {

		ss := strings.FieldsFunc(str, func(r rune) bool {
			if unicode.IsSpace(r) || r == '(' || r == ')' {
				return true
			}
			return false
		})
		bufferSize, err := strconv.Atoi(ss[len(ss)-1])
		if err != nil {
			return "", fmt.Errorf("Unable to parse STARTED response: %s", str)
		}
		c.cmdMaxBytes = bufferSize
	}
	return str, nil
}

func (c connection) write(str string) error {
	if c.closed {
		return ErrClosed
	}
	_, err := c.conn.Write([]byte(str + "\r\n"))
	return err
}

func (c *connection) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.closed = true
	c.reader = nil
}

// Ensure splitting on a valid leading byte
// Slicing the string directly is more efficient than converting to []byte and back because
// since a string is immutable and a []byte isn't,
// the data must be copied to new memory upon conversion,
// taking O(n) time (both ways!),
// whereas slicing a string simply returns a new string header backed by the same array as the original
// (taking constant time).
func splitText(longString string, maxLen int) []string {
	var splits []string

	var l, r int
	for l, r = 0, maxLen; r < len(longString); l, r = r, r+maxLen {
		for !utf8.RuneStart(longString[r]) {
			r--
		}
		splits = append(splits, longString[l:r])
	}
	splits = append(splits, longString[l:])
	return splits
}

func (c *connection) push(collection, bucket, object, text string) (err error) {
	patterns := []struct {
		Pattern     string
		Replacement string
	}{{"\\", "\\\\"},
		{"\n", "\\n"},
		{"\"", "\\\""}}
	for _, v := range patterns {
		text = strings.Replace(text, v.Pattern, v.Replacement, -1)
	}

	chunks := splitText(text, c.cmdMaxBytes/2)
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

func (c *connection) Pop(collection, bucket, object, text string) (err error) {
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

func (c *connection) Count(collection, bucket, object string) (cnt int, err error) {
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

func (c *connection) FlushCollection(collection string) (err error) {
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

func (c *connection) FlushBucket(collection, bucket string) (err error) {
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

func (c *connection) FlushObject(collection, bucket, object string) (err error) {
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

func (c *connection) Query(collection, bucket, term string, limit, offset int) (results []string, err error) {
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

func (c *connection) Suggest(collection, bucket, word string, limit int) (results []string, err error) {
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
