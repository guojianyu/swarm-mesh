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

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	meshPkg "github.com/guojianyu/swarm-mesh/pkg"

	"k8s.io/klog"
)

type Server struct {
	clients     sync.Map
	endPoints   sync.Map //port -> EndPoint
	endPointsLn sync.Map //port -> Listen
	TunnelAddr  string
	tlsConfig   *meshPkg.TlsConfig
}

func NewTunnelServer(address string) *Server {
	return &Server{
		TunnelAddr: address,
		tlsConfig:  &meshPkg.TlsConfig{},
	}
}

func (s *Server) addOrUpdateClient(client *meshPkg.Client) {
	s.clients.Store(client.ClientID, client)
}

func (s *Server) removeClient(clientId string) {
	if val, ok := s.clients.Load(clientId); ok {
		client := val.(*meshPkg.Client)
		client.Close()
		client.RemoveAndCloseAllSessionsUp()
		s.clients.Delete(clientId)
	}
}

func (s *Server) getClient(clientId string) (*meshPkg.Client, bool) {
	if val, ok := s.clients.Load(clientId); ok {
		return val.(*meshPkg.Client), true
	}
	return nil, false
}

func (s *Server) WithTLS(cafile, certFile, keyFile string) {
	s.tlsConfig = &meshPkg.TlsConfig{
		EnableTLS: true,
		CaFile:    cafile,
		CertFile:  certFile,
		KeyFile:   keyFile,
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.clients = sync.Map{}
	defer func() {
		//close all clients and ports mapping
		s.clients.Range(func(key, value interface{}) bool {
			s.removeClient(key.(string))
			return true
		})
		s.endPoints.Range(func(key, value interface{}) bool {
			s.DeletePortMapping(value.(meshPkg.EndPoint).Port)
			return true
		})
	}()
	return s.acceptClient(ctx)
}

func (s *Server) DeletePortMapping(port uint32) error {
	ep := meshPkg.EndPoint{}
	if value, ok := s.endPoints.Load(port); ok {
		ep = value.(meshPkg.EndPoint)
		s.endPoints.Delete(port)
	} else {
		return fmt.Errorf("do not find port mapping")
	}
	client, exist := s.getClient(ep.ClientID)
	if exist {
		client.Sessions.Range(func(key, value interface{}) bool {
			session := value.(*meshPkg.Session)
			if session.Ep.Port == port {
				session.Up.Close()
				client.Sessions.Delete(key)
			}
			return true
		})
	}
	if ln, ok := s.endPointsLn.Load(port); ok {
		s.endPointsLn.Delete(port)
		ln.(net.Listener).Close()
	}
	klog.V(4).Infof("delete port mapping %v", ep.Port)
	return nil
}

func (s *Server) AddPortMapping(port uint32, clientID string, targetIP string, targetPort uint32) error {
	ep := meshPkg.EndPoint{
		Port:       port,
		ClientID:   clientID,
		TargetIP:   targetIP,
		TargetPort: targetPort,
	}
	if _, ok := s.endPoints.Load(ep.Port); ok {
		return fmt.Errorf("the port has been used")
	}
	if ep.TargetIP == "" {
		ep.TargetIP = "localhost"
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", ep.Port))
	if err != nil {
		return fmt.Errorf("failed to listen port: %v", ep.Port)
	}
	klog.V(4).Infof("listener on %v map [%v]-[%v:%v]", ep.Port, ep.ClientID, ep.TargetIP, ep.TargetPort)
	s.endPoints.Store(ep.Port, ep)
	s.endPointsLn.Store(ep.Port, ln)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				klog.V(4).Infof("port: %v accept error: %v", ep.Port, err)
				return
			}
			go s.applySession(c, &ep)
		}
	}()
	return nil
}

func (s *Server) acceptClient(ctx context.Context) error {
	var ln net.Listener
	var err error
	if s.tlsConfig.EnableTLS {
		var tlscfg *tls.Config
		tlscfg, err = s.tlsConfig.NewServerTlsConfig()
		if err == nil {
			ln, err = tls.Listen("tcp", s.TunnelAddr, tlscfg)
			klog.V(4).Infof("TLS tunnel listening on %s", s.TunnelAddr)
		} else {
			klog.V(4).Infof("TLS tunnel listening err: %v", err)
		}
	} else {
		ln, err = net.Listen("tcp", s.TunnelAddr)
		klog.V(4).Infof("tunnel listening on %s", s.TunnelAddr)
	}
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			ln.Close()
			return nil
		default:
			c, err := ln.Accept()
			if err != nil {
				klog.V(4).Infof("accept error: %v", err)
				return fmt.Errorf("accept error: %v", err)
			}
			klog.V(4).Infof("tunnel client connected: %s", c.RemoteAddr())
			go s.readFromClient(c, ctx)
		}
	}
}

func (s *Server) readFromClient(c net.Conn, ctx context.Context) error {
	client := meshPkg.NewClient("", c)
	defer func() {
		s.removeClient(client.ClientID)
		klog.V(4).Infof("Client %v connection closed", client.ClientID)
	}()

	//send ping message
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.PingMessage}); err != nil {
					klog.V(4).Infof("The clientId[%v] Heartbeat error: %v", client.ClientID, err)
					return
				}
			}
		}
	}()
	//check valid of connection
	go func() {
		<-time.After(30 * time.Second)
		if !client.IsReady() {
			client.Conn.Close()
			klog.V(4).Infof(
				"The connection[%v] is cleared because registration not completed",
				client.Conn.RemoteAddr(),
			)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			f, err := client.Conn.ReadMessage()
			if err != nil {
				klog.V(4).Infof("Read from tunnel error: %v", err)
				return err
			}
			switch f.Type {
			case meshPkg.RegisterClient:
				clientID := string(f.Data)
				if _, ok := s.getClient(clientID); ok {
					s.removeClient(clientID)
				}
				client.ClientID = clientID
				client.Ready = true
				s.addOrUpdateClient(client)
				klog.V(4).Infof("The client %v is registered", client.ClientID)
			case meshPkg.OpenMessage:
				//The session have been established then set status as ready
				if session, ok := client.GetSession(f.ID); ok {
					session.Ready = true
					client.AddOrUpdateSession(session)
					klog.V(4).Infof("The session %v is created", session.ID)
					//copy up steam to down stream
					go func() {
						err := session.UpDownStreamCopy()
						klog.V(4).Infof("The session %v is closed,reason: %v", session.ID, err)
						client.RemoveSession(f.ID)
					}()
				} else {
					klog.V(4).Infof("The session[%v] not found", f.ID)
				}
			case meshPkg.DataMessage:
				if session, ok := client.GetSession(f.ID); ok {
					n, err := session.Up.Write(f.Data)
					if err != nil {
						klog.V(4).Infof("Write to session[%v] upstream error: %v", f.ID, err)
						session.Up.Close()
						client.RemoveSession(f.ID)
					} else {
						klog.V(5).Infof("Forwarded %d bytes from tunnel to session[%v] upstream", n, f.ID)
					}
				} else {
					klog.V(4).Infof("The session[%v] not found", f.ID)
				}

			case meshPkg.CloseMessage:
				if session, ok := client.GetSession(f.ID); ok {
					session.Up.Write(f.Data)
					session.Up.Close()
					client.RemoveSession(f.ID)
					klog.V(4).Infof("The session[%v] is closed by client,reason:%v", f.ID, string(f.Data))
				}
			case meshPkg.PingMessage:
				if err := client.Conn.WriteMessage(meshPkg.Message{Type: meshPkg.PongMessage}); err != nil {
					klog.V(5).Infof("Send pong error: %v", err)
					return err
				}
			case meshPkg.PongMessage:
				// ignore
			default:
				klog.V(4).Infof("Receive an undefined message type: %d", f.Type)
			}
		}
	}
}

func (s *Server) applySession(c net.Conn, ep *meshPkg.EndPoint) {
	client, exist := s.getClient(ep.ClientID)
	if !exist {
		klog.V(4).Infof("Client %v is unregistered", ep.ClientID)
		c.Write([]byte(fmt.Sprintf("Client %v is unregistered", ep.ClientID)))
		c.Close()
		return
	}
	id := meshPkg.IDGenerator()
	session := meshPkg.NewSession(id, &meshPkg.Connect{Conn: c, Mu: sync.Mutex{}}, client.Conn, ep, client.Context, client.Cancel)
	client.AddOrUpdateSession(session)
	if err := func() error {
		data, err := json.Marshal(ep)
		if err != nil {
			klog.Errorf("Failed to marshal data:%v", err)
			return fmt.Errorf("failed to marshal data:%v", err)
		}
		if err := session.Down.WriteMessage(meshPkg.Message{Type: meshPkg.OpenMessage, ID: session.ID, Data: data}); err != nil {
			return fmt.Errorf("failed to send open message: %v", err)
		}
		interval := time.Duration(1) * time.Second
		timer := time.NewTimer(interval)
		exit := time.NewTimer(30 * interval)
		for {
			select {
			case <-session.Context.Done():
				return fmt.Errorf("the session context done")
			case <-timer.C:
				if session, ok := client.GetSession(session.ID); ok {
					if session.Ready {
						return nil
					}
				} else {
					return fmt.Errorf("the session lose")
				}
			case <-exit.C:
				return fmt.Errorf("the session connection timeout")
			}
			timer.Reset(interval)
		}
	}(); err != nil {
		session.Up.Close()
		client.RemoveSession(session.ID)
	}

}
