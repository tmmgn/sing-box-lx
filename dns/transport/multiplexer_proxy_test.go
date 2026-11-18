package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	mDNS "github.com/miekg/dns"
)

const testVLESSUser = "b831381d-6324-4d53-ad4f-8cda48b30811"

type testForwardHandler struct {
	target string
}

func (h *testForwardHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	upstream, err := net.Dial("tcp", h.target)
	if err != nil {
		conn.Close()
		return
	}
	go func() {
		io.Copy(upstream, conn)
		upstream.Close()
	}()
	go func() {
		io.Copy(conn, upstream)
		conn.Close()
	}()
}

func (h *testForwardHandler) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	conn.Close()
}

func serveTestConns(t *testing.T, handle func(net.Conn)) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go handle(conn)
		}
	}()
	return listener
}

// A pipelining TCP DNS server that counts streams it could not frame.
func servePipelinedTestDNS(t *testing.T, malformed *atomic.Int32) net.Listener {
	return serveTestConns(t, func(conn net.Conn) {
		defer conn.Close()
		for {
			request, err := ReadMessage(conn)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					malformed.Add(1)
				}
				return
			}
			response := new(mDNS.Msg)
			response.SetReply(request)
			if WriteMessage(conn, request.Id, response) != nil {
				return
			}
		}
	})
}

type testListenerDialer struct {
	address string
}

func (d testListenerDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return net.Dial("tcp", d.address)
}

func (d testListenerDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

// Queries racing on a fresh shared conn must not corrupt the stream when the
// conn is a proxy conn that writes its protocol header with the first payload.
func TestMultiplexerConcurrentWritesOnVLESSConn(t *testing.T) {
	t.Parallel()
	var malformed atomic.Int32
	dnsListener := servePipelinedTestDNS(t, &malformed)
	service := vless.NewService[int](logger.NOP(), &testForwardHandler{target: dnsListener.Addr().String()})
	service.UpdateUsers([]int{0}, []string{testVLESSUser}, []string{""})
	proxyListener := serveTestConns(t, func(conn net.Conn) {
		if service.NewConnection(context.Background(), conn, M.SocksaddrFromNet(conn.RemoteAddr()), nil) != nil {
			conn.Close()
		}
	})
	client, err := vless.NewClient(testVLESSUser, "", logger.NOP())
	if err != nil {
		t.Fatal(err)
	}
	destination := M.ParseSocksaddr(dnsListener.Addr().String())
	runConcurrentTestQueries(t, &malformed, func(ctx context.Context) (net.Conn, error) {
		conn, dialErr := net.Dial("tcp", proxyListener.Addr().String())
		if dialErr != nil {
			return nil, dialErr
		}
		return client.DialEarlyConn(conn, destination)
	})
}

func TestMultiplexerConcurrentWritesOnMuxConn(t *testing.T) {
	t.Parallel()
	var malformed atomic.Int32
	dnsListener := servePipelinedTestDNS(t, &malformed)
	service, err := mux.NewService(mux.ServiceOptions{
		NewStreamContext: func(ctx context.Context, _ net.Conn) context.Context { return ctx },
		Logger:           logger.NOP(),
		HandlerEx:        &testForwardHandler{target: dnsListener.Addr().String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	proxyListener := serveTestConns(t, func(conn net.Conn) {
		service.NewConnectionEx(context.Background(), conn, M.SocksaddrFromNet(conn.RemoteAddr()), M.Socksaddr{}, nil)
	})
	destination := M.ParseSocksaddr(dnsListener.Addr().String())
	runConcurrentTestQueries(t, &malformed, func(ctx context.Context) (net.Conn, error) {
		client, clientErr := mux.NewClient(mux.Options{
			Dialer:     testListenerDialer{address: proxyListener.Addr().String()},
			Logger:     logger.NOP(),
			Protocol:   "h2mux",
			MaxStreams: 4,
		})
		if clientErr != nil {
			return nil, clientErr
		}
		return client.DialContext(ctx, N.NetworkTCP, destination)
	})
}

func runConcurrentTestQueries(t *testing.T, malformed *atomic.Int32, dial func(ctx context.Context) (net.Conn, error)) {
	const (
		rounds  = 20
		queries = 16
	)
	var failed []error
	for range rounds {
		multiplexer := newQueryMultiplexer(queryMultiplexerOptions{
			dial: dial,
			write: func(conn net.Conn, message *mDNS.Msg, queryId uint16) error {
				return WriteMessage(conn, queryId, message)
			},
			readNext: func(conn net.Conn) (*mDNS.Msg, error) {
				return ReadMessage(conn)
			},
		})
		var (
			wg     sync.WaitGroup
			access sync.Mutex
		)
		for range queries {
			wg.Go(func() {
				message := new(mDNS.Msg)
				message.SetQuestion("example.com.", mDNS.TypeA)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, exchangeErr := multiplexer.Exchange(ctx, message)
				if exchangeErr != nil {
					access.Lock()
					failed = append(failed, exchangeErr)
					access.Unlock()
				}
			})
		}
		wg.Wait()
		multiplexer.Close()
	}
	if len(failed) > 0 {
		kinds := make(map[string]int)
		for _, failure := range failed {
			kinds[failure.Error()]++
		}
		t.Fatalf("%d of %d queries failed, server saw %d malformed streams; errors: %v", len(failed), rounds*queries, malformed.Load(), kinds)
	}
}
