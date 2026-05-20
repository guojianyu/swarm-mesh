/*
Copyright 2026 The Guojianyu Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package pkg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	uuid "github.com/satori/go.uuid"
)

const (
	RegisterClient = 0
	OpenMessage    = 1
	DataMessage    = 2
	CloseMessage   = 3
	PingMessage    = 4
	PongMessage    = 5
)

type Message struct {
	Type uint8  //1 byte
	ID   string //36 bytes
	Data []byte //len(Data) 4 bytes
}

const (
	MessageHeaderSize = 41 // 1 + 36 + 4
)

type Connect struct {
	net.Conn
	Closed bool
	Mu     sync.Mutex
}

type EndPoint struct {
	Port       uint32 `json:"port,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
	TargetIP   string `json:"targetIp,omitempty"`
	TargetPort uint32 `json:"targetPort,omitempty"`
}

type TlsConfig struct {
	EnableTLS bool
	CertFile  string
	KeyFile   string
	CaFile    string
}

type Client struct {
	ClientID string
	Conn     *Connect
	Context  context.Context
	Cancel   context.CancelFunc
	Ready    bool
	Sessions sync.Map // map[string]*Session (UUID -> Session)
}

func NewClient(clientId string, c net.Conn) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		ClientID: clientId,
		Conn: &Connect{
			Conn: c,
			Mu:   sync.Mutex{},
		},
		Context:  ctx,
		Cancel:   cancel,
		Ready:    false,
		Sessions: sync.Map{},
	}
}

type Session struct {
	Up      *Connect
	Down    *Connect
	ID      string
	Ep      *EndPoint
	Context context.Context
	Cancel  context.CancelFunc
	Ready   bool
}

func NewSession(id string, up, down *Connect, ep *EndPoint, ctx context.Context, cancel context.CancelFunc) *Session {
	//ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		Up:      up,
		Down:    down,
		ID:      id,
		Ep:      ep,
		Context: ctx,
		Cancel:  cancel,
		Ready:   false,
	}

}

func (s *Session) IsReady() bool {
	return s.Ready
}

func (s *Session) UpDownStreamCopy() error {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-s.Context.Done():
			return nil
		default:
			n, err := s.Up.Read(buf)
			if err != nil {
				s.Down.WriteMessage(Message{Type: CloseMessage, ID: s.ID, Data: []byte(fmt.Sprintf("Read from upstream %d error: %v", s.ID, err))})
				s.Up.Close()
				return fmt.Errorf("read from session[%v] upstream error: %v", s.ID, err)
			}
			// copy data
			data := make([]byte, n)
			copy(data, buf[:n])
			if err := s.Down.WriteMessage(Message{Type: DataMessage, ID: s.ID, Data: data}); err != nil {
				return fmt.Errorf("send data to tunnel error: %v", err)
			}
			//log.Printf("Forwarded %d bytes from user %d to tunnel", n, s.ID)
		}

	}
}

func (s *Session) DownUpStreamCopy() error {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-s.Context.Done():
			return nil
		default:
			n, err := s.Down.Read(buf)
			if err != nil {
				s.Up.WriteMessage(Message{Type: CloseMessage, ID: s.ID})
				s.Down.Close()
				return fmt.Errorf("read from session[%v] downstream error: %v", s.ID, err)
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			if err := s.Up.WriteMessage(Message{Type: DataMessage, ID: s.ID, Data: data}); err != nil {
				return fmt.Errorf("send data to server error: %v", err)
			}
			//log.Printf("Forwarded %d bytes from local service to server (connID=%d)", n, s.ID)
		}

	}
}

func IDGenerator() string {
	return uuid.NewV4().String()
}

func (c *Connect) Close() error {
	if c.Closed {
		return nil
	}
	c.Closed = true
	return c.Conn.Close()
}

func (c *Connect) WriteMessage(m Message) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	hdr := make([]byte, MessageHeaderSize)
	hdr[0] = m.Type
	IDBytes := []byte(m.ID)
	// if m.Type != RegisterClient && len(IDBytes) != 36 {
	// 	return fmt.Errorf("invalid UUID length: %d", len(IDBytes))
	// }
	copy(hdr[1:37], IDBytes)
	binary.BigEndian.PutUint32(hdr[37:41], uint32(len(m.Data)))
	if _, err := c.Write(hdr); err != nil {
		return err
	}
	if len(m.Data) > 0 {
		_, err := c.Write(m.Data)
		return err
	}
	return nil
}

func (c *Connect) ReadMessage() (Message, error) {
	hdr := make([]byte, MessageHeaderSize)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return Message{}, err
	}
	n := binary.BigEndian.Uint32(hdr[37:41])
	data := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(c, data); err != nil {
			return Message{}, err
		}
	}
	return Message{Type: hdr[0], ID: string(hdr[1:37]), Data: data}, nil
}

func (client *Client) IsReady() bool {
	return client.Ready
}

func (client *Client) Close() error {
	client.Cancel()
	if err := client.Conn.Close(); err != nil {
		return fmt.Errorf("close client error: %v", err)
	}
	return nil
}

func (client *Client) AddOrUpdateSession(session *Session) {
	client.Sessions.Store(session.ID, session)
}

func (client *Client) RemoveSession(sessionId string) {
	if _, ok := client.Sessions.Load(sessionId); ok {
		client.Sessions.Delete(sessionId)
	}
}

func (client *Client) GetSession(sessionId string) (*Session, bool) {
	if val, ok := client.Sessions.Load(sessionId); ok {
		return val.(*Session), true
	}
	return nil, false
}

func (client *Client) RemoveAndCloseAllSessionsUp() {
	client.Sessions.Range(func(key, value interface{}) bool {
		session := value.(*Session)
		session.Up.Close()
		client.Sessions.Delete(key)
		return true
	})
}

func (client *Client) RemoveAndCloseAllSessionsDown() {
	client.Sessions.Range(func(key, value interface{}) bool {
		session := value.(*Session)
		session.Up.Close()
		client.Sessions.Delete(key)
		return true
	})
}

func (tlsConfig *TlsConfig) NewServerTlsConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(
		tlsConfig.CertFile,
		tlsConfig.KeyFile,
	)
	if err != nil {
		return nil, fmt.Errorf("load cert and key file error: %v", err)
	}
	caCert, err := os.ReadFile(tlsConfig.CaFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file error: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCert)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		// verify the client certificate
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caPool,
		MinVersion: tls.VersionTLS12,
	}
	return cfg, nil
}

func (tlsConfig *TlsConfig) NewClientTlsConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(
		tlsConfig.CertFile,
		tlsConfig.KeyFile,
	)
	if err != nil {
		return nil, fmt.Errorf("load cert and key file error: %v", err)
	}
	caCert, err := os.ReadFile(tlsConfig.CaFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file error: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCert)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}
	return cfg, nil
}
