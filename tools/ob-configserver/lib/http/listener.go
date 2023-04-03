/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

package http

import (
	"context"
	"net"
	"net/http"

	log "github.com/sirupsen/logrus"
)

type Listener struct {
	tcpListener  *net.TCPListener
	unixListener *net.UnixListener
	mux          *http.ServeMux
	srv          *http.Server
}

func NewListener() *Listener {
	mux := http.NewServeMux()
	return &Listener{
		mux: mux,
		srv: &http.Server{Handler: mux},
	}
}

func (l *Listener) StartTCP(addr string) error {
	tcpListener, err := NewTcpListener(addr)
	if err != nil {
		return err
	}
	go func() {
		_ = l.srv.Serve(tcpListener)
		log.Info("http tcp server exited")
	}()
	l.tcpListener = tcpListener
	return nil
}

func NewTcpListener(addr string) (*net.TCPListener, error) {
	cfg := net.ListenConfig{}
	listener, err := cfg.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	return listener.(*net.TCPListener), nil
}

func (l *Listener) StartSocket(path string) error {
	listener, err := NewSocketListener(path)
	if err != nil {
		return err
	}

	go func() {
		_ = l.srv.Serve(listener)
		log.Info("http socket server exited")
	}()
	l.unixListener = listener
	return nil
}

func NewSocketListener(path string) (*net.UnixListener, error) {
	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		return nil, err
	}
	return net.ListenUnix("unix", addr)
}

func (l *Listener) AddHandler(path string, h http.Handler) {
	l.mux.Handle(path, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Connection", "close")
		h.ServeHTTP(writer, request)
	}))
}

func (l *Listener) Close() {
	//var err error
	_ = l.srv.Close()
	if l.tcpListener != nil {
		_ = l.tcpListener.Close()
		//if err != nil {
		//	log.WithError(err).Warn("close tcpListener got error")
		//}
	}
	if l.unixListener != nil {
		_ = l.unixListener.Close()
		//if err != nil {
		//	log.WithError(err).Warn("close unixListener got error")
		//}
	}
}
