// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package pty

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/creack/pty"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/daytonaio/daemon/pkg/common"
	log "github.com/sirupsen/logrus"
)

type PTYController struct {
	workDir string
}

func NewPTYController(workDir string) *PTYController {
	return &PTYController{workDir: workDir}
}

var ptyUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ----- Manager -----

var ptyManager = &PTYManager{
	sessions: make(map[string]*PTYSession),
}

type PTYManager struct {
	mu       sync.RWMutex
	sessions map[string]*PTYSession
}

func (m *PTYManager) Add(s *PTYSession) {
	m.mu.Lock()
	m.sessions[s.info.ID] = s
	m.mu.Unlock()
}

func (m *PTYManager) Get(id string) (*PTYSession, bool) {
	m.mu.RLock()
	s, ok := m.sessions[id]
	m.mu.RUnlock()
	return s, ok
}

func (m *PTYManager) Delete(id string) (*PTYSession, bool) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	return s, ok
}

func (m *PTYManager) List() []PTYSessionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]PTYSessionInfo, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s.Info())
	}
	return out
}

// ---- Session ----

type wsClient struct {
	id   string
	conn *websocket.Conn
	send chan []byte // outbound queue for this client (PTY -> WS)
}

type PTYSession struct {
	info PTYSessionInfo

	cmd    *exec.Cmd
	ptmx   *os.File
	ctx    context.Context
	cancel context.CancelFunc

	// multi-attach
	clients   map[string]*wsClient
	clientsMu sync.RWMutex

	// funnel of all client inputs -> single PTY writer (preserves ordering)
	inCh chan []byte

	// guards general session fields (info/cmd/ptmx)
	mu sync.Mutex
}

type PTYSessionInfo struct {
	ID        string            `json:"id"`
	Cwd       string            `json:"cwd"`
	Envs      map[string]string `json:"envs"`
	Cols      uint16            `json:"cols"`
	Rows      uint16            `json:"rows"`
	CreatedAt time.Time         `json:"createdAt"`
	Active    bool              `json:"active"`
}

func (s *PTYSession) Info() PTYSessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info
}

type PTYControlMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

type PTYResizeData struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// ---- API types ----

type PTYCreateRequest struct {
	ID   string            `json:"id"`
	Cwd  string            `json:"cwd,omitempty"`
	Envs map[string]string `json:"envs,omitempty"`
	Cols uint16            `json:"cols"`
	Rows uint16            `json:"rows"`
} // @name PTYCreateRequest

type PTYCreateResponse struct {
	SessionID string `json:"sessionId"`
} // @name PTYCreateResponse

type PTYListResponse struct {
	Sessions []PTYSessionInfo `json:"sessions"`
} // @name PTYListResponse

// ---- HTTP Handlers ----

func (p *PTYController) CreatePTYSession(c *gin.Context) {
	var req PTYCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Defaults
	if req.Cwd == "" {
		req.Cwd = p.workDir
	}
	if req.Envs == nil {
		req.Envs = make(map[string]string, 1)
	}
	if req.Envs["TERM"] == "" {
		req.Envs["TERM"] = "xterm-256color"
	}
	if req.Cols <= 0 {
		req.Cols = 80
	}
	if req.Rows <= 0 {
		req.Rows = 24
	}
	// clamp extremes to avoid ioctl errors
	if req.Cols > 1000 {
		req.Cols = 1000
	}
	if req.Rows > 1000 {
		req.Rows = 1000
	}

	session := &PTYSession{
		info: PTYSessionInfo{
			ID:        req.ID,
			Cwd:       req.Cwd,
			Envs:      req.Envs,
			Cols:      req.Cols,
			Rows:      req.Rows,
			CreatedAt: time.Now(),
			Active:    false,
		},
	}

	if err := session.start(); err != nil {
		log.WithError(err).Error("failed to start PTY at create")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start PTY session"})
		return
	}

	ptyManager.Add(session)
	c.JSON(http.StatusCreated, PTYCreateResponse{SessionID: req.ID})
}

func (p *PTYController) ListPTYSessions(c *gin.Context) {
	c.JSON(http.StatusOK, PTYListResponse{Sessions: ptyManager.List()})
}

func (p *PTYController) GetPTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	if s, ok := ptyManager.Get(id); ok {
		c.JSON(http.StatusOK, s.Info())
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "PTY session not found"})
}

func (p *PTYController) DeletePTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	if s, ok := ptyManager.Delete(id); ok {
		s.kill()
		log.Infof("Deleted PTY session %s", id)
		c.JSON(http.StatusOK, gin.H{"message": "PTY session deleted"})
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "PTY session not found"})
}

// WebSocket: multi-attach enabled (many clients share same PTY)
func (p *PTYController) ConnectPTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	session, ok := ptyManager.Get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "PTY session not found"})
		return
	}

	ws, err := ptyUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.WithError(err).Error("ws upgrade failed")
		return
	}

	// If the PTY is not active, try to (re)start.
	if !session.Info().Active {
		if err := session.start(); err != nil {
			log.WithError(err).Error("failed to (re)start PTY on connect")
			_ = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "failed to start PTY"))
			_ = ws.Close()
			return
		}
	}

	session.attachWebSocket(ws)
}

// ---- Session lifecycle ----

func (s *PTYSession) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// already running?
	if s.info.Active && s.cmd != nil && s.ptmx != nil {
		return nil
	}

	// (re)init multi-attach state
	if s.clients == nil {
		s.clients = make(map[string]*wsClient)
	}
	if s.inCh == nil {
		s.inCh = make(chan []byte, 1024)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	shell := common.GetShell()
	if shell == "" {
		return errors.New("no shell resolved")
	}

	cmd := exec.CommandContext(ctx, shell)
	cmd.Dir = s.info.Cwd

	// Env
	cmd.Env = os.Environ()
	for k, v := range s.info.Envs {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: s.info.Rows, Cols: s.info.Cols})
	if err != nil {
		cancel()
		return fmt.Errorf("pty.StartWithSize: %w", err)
	}

	s.cmd = cmd
	s.ptmx = ptmx
	s.info.Active = true

	log.Infof("Started PTY session %s with PID %d", s.info.ID, s.cmd.Process.Pid)

	// 1) PTY -> clients broadcaster
	go s.ptyReadLoop()

	// 2) clients -> PTY writer
	go s.inputWriteLoop()

	// Reap the process; mark inactive on exit and send exit event
	go func() {
		err := s.cmd.Wait()
		var exitCode int
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
			} else {
				exitCode = 1
			}
		} else {
			exitCode = 0
		}
		
		s.mu.Lock()
		s.info.Active = false
		s.mu.Unlock()
		
		// Send exit message to all clients
		s.sendExitMessage(exitCode)
		
		log.Infof("PTY session %s process exited with code %d", s.info.ID, exitCode)
	}()

	return nil
}

func (s *PTYSession) kill() {
	// Send exit message before killing
	s.sendExitMessage(128) // 128 = killed signal
	
	// kill process and PTY
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	if s.ptmx != nil {
		_ = s.ptmx.Close()
		s.ptmx = nil
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	s.info.Active = false
	s.mu.Unlock()

	// close all clients (outside of s.mu)
	s.clientsMu.Lock()
	for id, cl := range s.clients {
		_ = cl.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, "PTY session killed",
		))
		_ = cl.conn.Close()
		close(cl.send)
		delete(s.clients, id)
	}
	s.clientsMu.Unlock()
}

// ---- WebSocket pumps (multi-attach) ----

const (
	writeWait = 10 * time.Second
	readLimit = 64 * 1024
)

func (s *PTYSession) attachWebSocket(ws *websocket.Conn) {
	cl := &wsClient{
		id:   uuid.NewString(),
		conn: ws,
		send: make(chan []byte, 256), // if full, drop slow client
	}

	// register
	s.clientsMu.Lock()
	s.clients[cl.id] = cl
	count := len(s.clients)
	s.clientsMu.Unlock()
	log.Infof("Client %s attached to PTY session %s (clients=%d)", cl.id, s.info.ID, count)

	// writer (PTY -> this client) with ping
	go s.clientWriter(cl)

	// reader (this client -> PTY); blocks until disconnect
	s.clientReader(cl)

	// on exit, unregister
	s.clientsMu.Lock()
	delete(s.clients, cl.id)
	s.clientsMu.Unlock()

	close(cl.send)
	_ = cl.conn.Close()

	s.clientsMu.RLock()
	remaining := len(s.clients)
	s.clientsMu.RUnlock()
	log.Infof("Client %s detached from PTY session %s (clients=%d)", cl.id, s.info.ID, remaining)
}

func (s *PTYSession) clientWriter(cl *wsClient) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case b, ok := <-cl.send:
			if !ok {
				return
			}
			_ = cl.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := cl.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
				return
			}
		}
	}
}

func (s *PTYSession) clientReader(cl *wsClient) {
	conn := cl.conn
	conn.SetReadLimit(readLimit)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Debug("ws read error:", err)
			}
			return
		}
		switch msgType {
		case websocket.TextMessage:
			// control or terminal input
			var ctrl PTYControlMessage
			if json.Unmarshal(data, &ctrl) == nil && ctrl.Type != "" {
				s.handleControlMessage(ctrl)
				continue
			}
			s.sendToPTY(data)
		case websocket.BinaryMessage:
			s.sendToPTY(data)
		default:
			// ignore
		}
	}
}

// ---- PTY/IO loops and helpers ----

func (s *PTYSession) ptyReadLoop() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.ptmx.Read(buf)
		if n > 0 {
			b := make([]byte, n)
			copy(b, buf[:n])
			s.broadcast(b)
		}
		if err != nil {
			return
		}
	}
}

func (s *PTYSession) inputWriteLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case data := <-s.inCh:
			if s.ptmx == nil {
				return
			}
			if _, err := s.ptmx.Write(data); err != nil {
				return
			}
		}
	}
}

func (s *PTYSession) broadcast(b []byte) {
	// send to each client; drop slow clients to avoid stalling the PTY
	s.clientsMu.RLock()
	for id, cl := range s.clients {
		select {
		case cl.send <- b:
		default:
			// client's outbound queue is full -> drop the client
			go func(id string, cl *wsClient) {
				log.Warnf("Dropping slow client %s on session %s", id, s.info.ID)
				_ = cl.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
					websocket.ClosePolicyViolation, "slow consumer",
				))
				_ = cl.conn.Close()
			}(id, cl)
		}
	}
	s.clientsMu.RUnlock()
}

func (s *PTYSession) sendToPTY(data []byte) {
	select {
	case s.inCh <- data:
	case <-s.ctx.Done():
	}
}

func (s *PTYSession) sendExitMessage(exitCode int) {
	exitMsg := PTYControlMessage{
		Type: "exit",
		Data: json.RawMessage(fmt.Sprintf(`{"code": %d}`, exitCode)),
	}
	msgBytes, err := json.Marshal(exitMsg)
	if err != nil {
		log.WithError(err).Error("failed to marshal exit message")
		return
	}
	
	// Send control message as text, not binary
	s.clientsMu.RLock()
	for _, cl := range s.clients {
		_ = cl.conn.SetWriteDeadline(time.Now().Add(writeWait))
		if err := cl.conn.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
			log.WithError(err).Debug("failed to send exit message to client")
		}
	}
	s.clientsMu.RUnlock()
}

// ---- Control & resize ----

func (s *PTYSession) handleControlMessage(msg PTYControlMessage) {
	switch msg.Type {
	case "resize":
		var r PTYResizeData
		if err := json.Unmarshal(msg.Data, &r); err == nil && r.Cols > 0 && r.Rows > 0 {
			s.resize(r.Cols, r.Rows)
		}
	case "exit":
		s.kill()
	default:
		// ignore unknowns
	}
}

func (s *PTYSession) resize(cols, rows uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cols > 1000 {
		cols = 1000
	}
	if rows > 1000 {
		rows = 1000
	}
	s.info.Cols = cols
	s.info.Rows = rows

	if s.ptmx != nil {
		if err := pty.Setsize(s.ptmx, &pty.Winsize{Cols: cols, Rows: rows}); err != nil {
			log.Debug("PTY resize error:", err)
		}
	}
}
