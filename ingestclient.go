package client

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"strings"
	"time"
)

// PATTERNS ...
var PATTERNS = []struct {
	Pattern     string
	Replacement string
}{{"\\", "\\\\"},
	{"\n", "\\n"},
	{"\"", "\\\""}}

type ingesteCommands string

const (
	push   ingesteCommands = "PUSH"
	pop    ingesteCommands = "POP"
	count  ingesteCommands = "COUNT"
	flushb ingesteCommands = "FLUSHB"
	flushc ingesteCommands = "FLUSHC"
	flusho ingesteCommands = "FLUSHO"
)

// IngestClient ...
type IngestClient struct {
	channel  string
	endpoint string
	password string
	port     int
	pool     *ConnPool
}

// NewIngestCient ...
func NewIngestCient(endpoint, password string, port int) (client *IngestClient) {

	dialer := func(ctx context.Context) (netConn net.Conn, err error) {
		netConn, err = net.Dial("tcp", endpoint)
		if err != nil {
			return nil, err
		}
		return
	}

	connector := func(ctx context.Context, netConn net.Conn) (conn *Conn, err error) {
		return NewConn(netConn, Ingest, password)
	}

	opt := &Options{
		Dialer:    dialer,
		OnClose:   nil,
		Connector: connector,

		PoolSize:           40,
		MinIdleConns:       10,
		MaxConnAge:         time.Minute,
		PoolTimeout:        time.Second * 2,
		IdleTimeout:        time.Second * 30,
		IdleCheckFrequency: time.Second * 2,
	}

	client = &IngestClient{
		pool:     NewConnPool(opt),
		endpoint: endpoint,
		password: password,
		port:     port,
	}

	return
}

func patternReplace(text string) string {
	for _, v := range PATTERNS {
		text = strings.Replace(text, v.Pattern, v.Replacement, -1)
	}
	return text
}

// Push ...
func (c *IngestClient) Push(ctx context.Context, collection, bucket, object, text string) (err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	text = patternReplace(text)

	chunks := conn.splitText(text)

	var buf bytes.Buffer
	// split chunks with partial success will yield single error
	for _, chunk := range chunks {
		buf.Reset()

		buf.WriteString(string(push))
		buf.WriteString(" ")
		buf.WriteString(collection)
		buf.WriteString(" ")
		buf.WriteString(bucket)
		buf.WriteString(" ")
		buf.WriteString(object)
		buf.WriteString("\"")
		buf.WriteString(chunk)
		buf.WriteString("\"")

		err = conn.write(buf.String())

		if err != nil {
			return err
		}

		// sonic should sent OK
		_, err = conn.read()
		if err != nil {
			return err
		}
	}

	return nil
}

// Pop ...
func (c *IngestClient) Pop(ctx context.Context, collection, bucket, object, text string) (err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	var buf bytes.Buffer

	buf.WriteString(string(pop))
	buf.WriteString(" ")
	buf.WriteString(collection)
	buf.WriteString(" ")
	buf.WriteString(bucket)
	buf.WriteString(" ")
	buf.WriteString(object)
	buf.WriteString("\"")
	buf.WriteString(text)
	buf.WriteString("\"")

	err = conn.write(buf.String())
	if err != nil {
		return err
	}

	// sonic should sent OK
	_, err = conn.read()
	if err != nil {
		return err
	}
	return nil
}

// Count ...
func (c *IngestClient) Count(ctx context.Context, collection, bucket, object string) (cnt int, err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return
	}

	var buf bytes.Buffer

	buf.WriteString(string(count))
	buf.WriteString(" ")
	buf.WriteString(collection)
	buf.WriteString(" ")
	buf.WriteString(bucket)
	buf.WriteString(" ")
	buf.WriteString(object)

	err = conn.write(buf.String())
	if err != nil {
		return 0, err
	}

	// RESULT NUMBER
	r, err := conn.read()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(r[7:])
}

// FlushB ...
func (c *IngestClient) FlushB(ctx context.Context, collection, bucket string) (cnt int, err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return
	}

	var buf bytes.Buffer

	buf.WriteString(string(flushb))
	buf.WriteString(" ")
	buf.WriteString(collection)
	buf.WriteString(" ")
	buf.WriteString(bucket)

	err = conn.write(buf.String())
	if err != nil {
		return 0, err
	}

	// RESULT NUMBER
	r, err := conn.read()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(r[7:])
}

// FlushC ...
func (c *IngestClient) FlushC(ctx context.Context, collection string) (cnt int, err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return
	}

	var buf bytes.Buffer

	buf.WriteString(string(flushc))
	buf.WriteString(" ")
	buf.WriteString(collection)

	err = conn.write(buf.String())
	if err != nil {
		return 0, err
	}

	// RESULT NUMBER
	r, err := conn.read()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(r[7:])
}

// FlushO ...
func (c *IngestClient) FlushO(ctx context.Context, collection, bucket, object string) (cnt int, err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return
	}

	var buf bytes.Buffer

	buf.WriteString(string(flusho))
	buf.WriteString(" ")
	buf.WriteString(collection)
	buf.WriteString(" ")
	buf.WriteString(bucket)
	buf.WriteString(" ")
	buf.WriteString(object)

	err = conn.write(buf.String())
	if err != nil {
		return 0, err
	}

	// RESULT NUMBER
	r, err := conn.read()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(r[7:])
}
