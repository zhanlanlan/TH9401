package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// fuck shutup
var (
	ErrClosed      = errors.New("sonic: client is closed")
	ErrPoolTimeout = errors.New("sonic: connection pool timeout")
	BadConnError   = errors.New("sonic: bad connection")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

// Pooler pool interface
type Pooler interface {
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	Get(context.Context) (*Conn, error)
	Put(*Conn)
	Remove(*Conn, error)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

// Options pool options
type Options struct {
	Dialer    func(context.Context) (net.Conn, error)        // 发起网络连接的对象
	OnClose   func(*Conn) error                              // 关闭连接时执行的操作
	Connector func(context.Context, net.Conn) (*Conn, error) // 建立链接

	PoolSize           int           // 连接池大小
	MinIdleConns       int           // 最小连接数
	MaxConnAge         time.Duration // 链接的最大寿命
	PoolTimeout        time.Duration // 获取链接的等待时间
	IdleTimeout        time.Duration // 链接空闲时间
	IdleCheckFrequency time.Duration // 检查链接的间隔
}

type lastDialErrorWrap struct {
	err error
}

// ConnPool connection pool
type ConnPool struct {
	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	queue chan struct{}

	connsMu      sync.Mutex
	conns        []*Conn
	idleConns    []*Conn
	poolSize     int
	idleConnsLen int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

var _ Pooler = (*ConnPool)(nil)

// NewConnPool new connection poll
func NewConnPool(opt *Options) *ConnPool {

	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	// 如果连接池已经关闭
	if p.closed() {
		return nil, ErrClosed
	}

	// 首先拿到牌子
	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	// 尝试从空闲连接池中拿到一个连接
	for {
		p.connsMu.Lock()
		cn := p.popIdle() // 取出一个空闲链接
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		if p.isStaleConn(cn) { // 是否是稳定链接
			_ = p.CloseConn(cn) // 不是就关了
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1) // 找到空闲链接数 + 1
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1) // 找不到空闲链接 + 1

	newcn, err := p.newConn(ctx, true) // 找不到就自己弄个链接吧
	if err != nil {
		p.freeTurn() // 还牌子
		return nil, err
	}

	return newcn, nil
}

// Put 用完之后把链接还回来
func (p *ConnPool) Put(cn *Conn) {
	if cn.Buffered() > 0 { // 如果还有未读数据则扔掉该链接 并标记为坏链接
		fmt.Printf("Conn has unread data")
		p.Remove(cn, BadConnError)
		return
	}

	if !cn.pooled {
		p.Remove(cn, nil)
		return
	}

	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	p.freeTurn()
}

// Close 关闭连接池
func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

// NewConn 新建一个链接
func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

// checkMinIdleConns 将连接数增加到最小链接数
func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++
		p.idleConnsLen++
		go func() {
			err := p.addIdleConn()
			if err != nil {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

// addIdleConn 添加空闲链接
func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.connsMu.Unlock()
	return nil
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()
	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn, err := p.opt.Connector(ctx, netConn)
	if err != nil {
		return nil, err
	}
	cn.pooled = pooled
	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

// 拿牌子
func (p *ConnPool) waitTurn(ctx context.Context) error {
	// 快速路线
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	// 慢速路线
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

// 还牌子
func (p *ConnPool) freeTurn() {
	<-p.queue
}

func (p *ConnPool) popIdle() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn
}

// Remove 移除一个链接
func (p *ConnPool) Remove(cn *Conn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

// CloseConn ...
func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

func (p *ConnPool) removeConn(cn *Conn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				p.poolSize--
				p.checkMinIdleConns()
			}
			return
		}
	}
}

func (p *ConnPool) closeConn(cn *Conn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

// Stats 返回连接池状态
func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

// Filter ...
func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	p.connsMu.Unlock()
	return firstErr
}

func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns()
			if err != nil {
				fmt.Printf("ReapStaleConns failed: %s", err)
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

// ReapStaleConns 淘汰陈旧链接
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()
		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

// reapStaleConn 淘汰一个陈旧链接
// 淘汰队列中队首的链接
func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn) // 连接池中移除

	return cn
}

// isStaleConn 是否是陈旧链接
func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.GetUsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.GetCreatedAt()) >= p.opt.MaxConnAge {
		return true
	}

	return false
}
