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
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	meshPkg "github.com/guojianyu/swarm-mesh/pkg"

	"k8s.io/klog"
)

type Client struct {
	ClientID   string
	client     *meshPkg.Client
	TunnelAddr string
	tlsConfig  *meshPkg.TlsConfig
}

func NewTunnelAgent(address, clientID string) *Client {
	return &Client{
		ClientID:   clientID,
		TunnelAddr: address,
		tlsConfig:  &meshPkg.TlsConfig{},
	}
}

func (c *Client) WithTLS(cafile, certFile, keyFile string) {
	c.tlsConfig = &meshPkg.TlsConfig{
		EnableTLS: true,
		CaFile:    cafile,
		CertFile:  certFile,
		KeyFile:   keyFile,
	}
}

func (c *Client) Run(ctx context.Context) {
	for {
		if err := c.run(ctx); err != nil {
			log.Printf("[ERROR] Connection failed: %v, reconnecting in 3s...", err)
			time.Sleep(3 * time.Second)
		}
	}
}

func (c *Client) run(ctx context.Context) error {
	var conn net.Conn
	var err error
	if c.tlsConfig.EnableTLS {
		var tlscfg *tls.Config
		tlscfg, err = c.tlsConfig.NewClientTlsConfig()
		if err == nil {
			conn, err = tls.Dial("tcp", c.TunnelAddr, tlscfg)
			klog.V(4).Infof("TLS Connected to server: %s", c.TunnelAddr)
		}
	} else {
		conn, err = net.Dial("tcp", c.TunnelAddr)
		klog.V(4).Infof("Connected to server: %s", c.TunnelAddr)
	}
	if err != nil {
		return err
	}
	c.client = meshPkg.NewClient(c.ClientID, conn)
	defer func() {
		c.client.Close()
		c.client.RemoveAndCloseAllSessionsDown()
	}()
	klog.Infof("[INFO] Connected to server: %s", c.TunnelAddr)
	// send ping messages
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.PingMessage}); err != nil {
					klog.V(4).Infof("Heartbeat error: %v", err)
					return
				}
			}
		}
	}()
	err = c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.RegisterClient, Data: []byte(c.ClientID)})
	if err != nil {
		klog.V(4).Infof("Write messages to server error: %v", err)
		return fmt.Errorf("write messages to server error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			f, err := c.client.Conn.ReadMessage()
			if err != nil {
				return err
			}
			switch f.Type {
			case meshPkg.OpenMessage:
				klog.V(4).Infof("Opening new local connection: %v", f.ID)
				go c.handleLocalConnection(f.ID, f.Data)

			case meshPkg.DataMessage:
				session, ok := c.client.GetSession(f.ID)
				if !ok {
					klog.V(4).Infof("The client local session[%v] still not found", f.ID)
					session.Up.WriteMessage(meshPkg.Message{Type: meshPkg.CloseMessage, ID: f.ID, Data: []byte(fmt.Sprintf("The client local session[%v] still not found", f.ID))})
					continue
				}
				if session.Down != nil {
					n, err := session.Down.Write(f.Data)
					if err != nil {
						klog.V(4).Infof("Write to client local service error: %v", err)
						session.Down.Close()
						c.client.RemoveSession(f.ID)
						session.Up.WriteMessage(meshPkg.Message{Type: meshPkg.CloseMessage, ID: f.ID, Data: []byte(fmt.Sprintf("Write to client local service error: %v", err))})
					} else {
						klog.V(5).Infof("Forwarded %d bytes to local service (sessionID=%v)", n, f.ID)
					}
				}
			case meshPkg.CloseMessage:
				if session, ok := c.client.GetSession(f.ID); ok {
					session.Down.Close()
					c.client.RemoveSession(f.ID)
					klog.V(4).Infof("The session[%v] is closed by server,reason:%v", f.ID, string(f.Data))
				}
			case meshPkg.PingMessage:
				if err := c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.PongMessage}); err != nil {
					klog.V(4).Infof("Send pong error: %v", err)
					return err
				}
			case meshPkg.PongMessage:
				//ignore
			}
		}
	}
}

func (c *Client) handleLocalConnection(sessionId string, data []byte) {
	var endpoint meshPkg.EndPoint
	if err := json.Unmarshal(data, &endpoint); err != nil {
		klog.V(4).Infof("Failed to unmarshal to endpoint data: %v", err)
		c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.CloseMessage, ID: sessionId})
		return
	}
	localAddr := fmt.Sprintf("%s:%v", endpoint.TargetIP, endpoint.TargetPort)
	localConn, err := net.DialTimeout("tcp", localAddr, 5*time.Second)
	if err != nil {
		klog.V(4).Infof("Failed to connect to local service: %v", err)
		c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.CloseMessage, ID: sessionId, Data: []byte(fmt.Sprintf("[ERROR] Failed to connect to local service: %v", err))})
		return
	}
	if tcpConn, ok := localConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	session := meshPkg.NewSession(sessionId, c.client.Conn, &meshPkg.Connect{Conn: localConn, Mu: sync.Mutex{}}, &endpoint, c.client.Context, c.client.Cancel)
	c.client.AddOrUpdateSession(session)
	defer func() {
		session.Down.Close()
		c.client.RemoveSession(sessionId)
		c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.CloseMessage, ID: sessionId})
		klog.V(4).Infof("Local connection cleaned up: %v", sessionId)
	}()
	klog.V(4).Infof("Connected to local service: %s (sessionID=%v)", localAddr, sessionId)
	//The session established success when the message send successfully.
	if err := c.client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.OpenMessage, ID: session.ID}); err != nil {
		klog.V(4).Infof("Failed to send open message: %v", err)
		return
	}
	//read data from the local service and send data to the remote server
	if err := session.DownUpStreamCopy(); err != nil {
		klog.V(4).Infof("Failed to copy down and up data stream: %v", err)
	}
}
