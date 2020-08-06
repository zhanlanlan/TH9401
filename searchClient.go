package client

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"time"
)

type searchCommands string

const (
	query   searchCommands = "QUERY"
	suggest searchCommands = "SUGGEST"
)

// SearchClient ...
type SearchClient struct {
	channel  string
	endpoint string
	password string
	port     int
	pool     *ConnPool
}

// NweSearchClient ...
func NweSearchClient(endpoint, password string, port int) (client *SearchClient) {

	dialer := func(ctx context.Context) (netConn net.Conn, err error) {
		netConn, err = net.Dial("tcp", endpoint)
		if err != nil {
			return nil, err
		}
		return
	}

	connector := func(ctx context.Context, netConn net.Conn) (conn *Conn, err error) {
		return NewConn(netConn, Search, password)
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

	client = &SearchClient{
		pool:     NewConnPool(opt),
		endpoint: endpoint,
		password: password,
		port:     port,
	}

	return
}

// Query ...
func (c *SearchClient) Query(ctx context.Context, collection, bucket, term string, limit, offset int) (results []string, err error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(string(query))
	buf.WriteString(" ")
	buf.WriteString(collection)
	buf.WriteString(" ")
	buf.WriteString(bucket)
	buf.WriteString(" ")
	buf.WriteString("\"")
	buf.WriteString(term)
	buf.WriteString("\"")
	buf.WriteString("LIMIT(")
	buf.WriteString(strconv.Itoa(limit))
	buf.WriteString(") OFFSET(")
	buf.WriteString(strconv.Itoa(offset))
	buf.WriteString(")")

	err = conn.write(buf.String())
	if err != nil {
		return nil, err
	}

	// pending, should be PENDING ID_EVENT
	_, err = conn.read()
	if err != nil {
		return nil, err
	}

	// event query, should be EVENT QUERY ID_EVENT RESULT1 RESULT2 ...
	read, err := conn.read()
	if err != nil {
		return nil, err
	}
	return getSearchResults(read, string(query)), nil
}
