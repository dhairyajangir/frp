package vhost

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListenerCloseReleasesPendingConnection(t *testing.T) {
	require := require.New(t)
	routed := make(chan struct{})
	resumeCtx, resume := context.WithCancel(context.Background())
	t.Cleanup(resume)
	mux := &Muxer{
		timeout:        time.Second,
		registryRouter: NewRouters(),
		vhostFunc: func(c net.Conn) (net.Conn, map[string]string, error) {
			return c, map[string]string{"Host": "example.com"}, nil
		},
		successHook: func(net.Conn, map[string]string) error {
			close(routed)
			<-resumeCtx.Done()
			return nil
		},
	}
	l, err := mux.Listen(context.Background(), &RouteConfig{Domain: "example.com"})
	require.NoError(err)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = client.Close() })
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		mux.handle(server)
	}()
	select {
	case <-routed:
	case <-time.After(time.Second):
		t.Fatal("connection was not routed")
	}
	require.NoError(client.SetReadDeadline(time.Now().Add(time.Second)))
	require.NoError(l.Close())
	resume()
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("closed listener did not release its connection handler")
	}
	_, err = client.Read(make([]byte, 1))
	require.ErrorIs(err, io.EOF, "connection rejected by the closed listener must be closed")
}

func TestListenerCloseIsIdempotent(t *testing.T) {
	require := require.New(t)
	mux := &Muxer{registryRouter: NewRouters()}
	cfg := &RouteConfig{Domain: "example.com"}
	l, err := mux.Listen(context.Background(), cfg)
	require.NoError(err)
	start := make(chan struct{})
	closed := make(chan error, 8)
	for range 8 {
		go func() {
			<-start
			closed <- l.Close()
		}()
	}
	close(start)
	for range 8 {
		select {
		case err := <-closed:
			require.NoError(err)
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not return")
		}
	}
	replacement, err := mux.Listen(context.Background(), cfg)
	require.NoError(err)
	t.Cleanup(func() { _ = replacement.Close() })
	require.NoError(l.Close())
	got, ok := mux.getListener(cfg.Domain, "", "")
	require.True(ok, "closing the old listener again must preserve its replacement")
	require.Same(replacement, got)
}

func TestListenerCloseReleasesAccept(t *testing.T) {
	require := require.New(t)
	mux := &Muxer{registryRouter: NewRouters()}
	l, err := mux.Listen(context.Background(), &RouteConfig{Domain: "example.com"})
	require.NoError(err)
	accepted := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		accepted <- err
	}()
	require.NoError(l.Close())
	select {
	case err := <-accepted:
		require.Error(err)
	case <-time.After(time.Second):
		t.Fatal("closed listener did not release Accept")
	}
	conn, err := l.Accept()
	require.Error(err)
	require.Nil(conn)
}

func TestListenerClosePreservesAcceptedConnection(t *testing.T) {
	require := require.New(t)
	mux := &Muxer{
		timeout:        time.Second,
		registryRouter: NewRouters(),
		vhostFunc: func(c net.Conn) (net.Conn, map[string]string, error) {
			return c, map[string]string{"Host": "example.com"}, nil
		},
	}
	l, err := mux.Listen(context.Background(), &RouteConfig{Domain: "example.com"})
	require.NoError(err)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = client.Close() })
	go mux.handle(server)
	conn, err := l.Accept()
	require.NoError(err)
	require.NoError(l.Close())
	require.NoError(client.SetReadDeadline(time.Now().Add(time.Second)))
	written := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("tunnel data"))
		written <- err
	}()
	data := make([]byte, len("tunnel data"))
	_, err = io.ReadFull(client, data)
	require.NoError(err)
	require.Equal("tunnel data", string(data))
	require.NoError(<-written)
}
