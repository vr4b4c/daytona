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

type PTYResizeRequest struct {
	Cols uint16 `json:"cols" binding:"required,min=1,max=1000"`
	Rows uint16 `json:"rows" binding:"required,min=1,max=1000"`
} // @name PTYResizeRequest

// ---- HTTP Handlers ----

func (p *PTYController) CreatePTYSession(c *gin.Context) {
	var req PTYCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate session ID
	if req.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session ID is required"})
		return
	}

	// Check if session with this ID already exists
	if _, exists := ptyManager.Get(req.ID); exists {
		c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("PTY session with ID '%s' already exists", req.ID)})
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

	// Add to manager first to prevent race conditions
	ptyManager.Add(session)

	if err := session.start(); err != nil {
		// If start fails, remove from manager
		ptyManager.Delete(req.ID)
		log.WithError(err).Error("failed to start PTY at create")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start PTY session"})
		return
	}

	c.JSON(http.StatusCreated, PTYCreateResponse{SessionID: req.ID})
}

func (p *PTYController) ListPTYSessions(c *gin.Context) {
	c.JSON(http.StatusOK, PTYListResponse{Sessions: ptyManager.List()})
}

func (p *PTYController) GetPTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session ID is required"})
		return
	}

	if s, ok := ptyManager.Get(id); ok {
		c.JSON(http.StatusOK, s.Info())
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "PTY session not found"})
}

func (p *PTYController) DeletePTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session ID is required"})
		return
	}

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
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session ID is required"})
		return
	}

	// Always upgrade to WebSocket first, then handle errors via WebSocket
	ws, err := ptyUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.WithError(err).Error("ws upgrade failed")
		return
	}

	// Now check for session existence and send structured error via WebSocket if needed
	session, ok := ptyManager.Get(id)
	if !ok {
		log.Warnf("PTY session %s not found", id)
		// Send error via WebSocket close with structured reason
		errorData := map[string]string{"error": "PTY session not found"}
		errorJSON, _ := json.Marshal(errorData)
		_ = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, string(errorJSON),
		))
		_ = ws.Close()
		return
	}

	// Check if session is active - inactive sessions are removed from manager
	if !session.Info().Active {
		log.Warnf("PTY session %s is inactive", id)
		// Send error via WebSocket close with structured reason
		errorData := map[string]string{"error": fmt.Sprintf("PTY session '%s' has terminated and is no longer available", id)}
		errorJSON, _ := json.Marshal(errorData)
		_ = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, string(errorJSON),
		))
		_ = ws.Close()
		return
	}

	session.attachWebSocket(ws)
}

func (p *PTYController) ResizePTYSession(c *gin.Context) {
	id := c.Param("sessionId")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session ID is required"})
		return
	}

	session, ok := ptyManager.Get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "PTY session not found"})
		return
	}

	// Check if session is active
	if !session.Info().Active {
		c.JSON(http.StatusGone, gin.H{"error": fmt.Sprintf("PTY session '%s' has terminated and cannot be resized", id)})
		return
	}

	var req PTYResizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := session.resize(req.Cols, req.Rows); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Infof("Resized PTY session %s to %dx%d", id, req.Cols, req.Rows)
	
	// Return updated session info
	updatedInfo := session.Info()
	c.JSON(http.StatusOK, updatedInfo)
}

// ---- Session lifecycle ----

func (s *PTYSession) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// already running?
	if s.info.Active && s.cmd != nil && s.ptmx != nil {
		return nil
	}

	// Prevent restarting - once a session exits, it should be removed from manager
	if s.cmd != nil {
		return errors.New("PTY session has already been used and cannot be restarted")
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
		var exitReason string
		
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
				// Analyze the exit code to provide meaningful context
				if exitCode == 137 {
					exitReason = " (SIGKILL)"
				} else if exitCode == 130 {
					exitReason = " (SIGINT - Ctrl+C)"
				} else if exitCode == 143 {
					exitReason = " (SIGTERM)"
				} else if exitCode > 128 {
					sigNum := exitCode - 128
					exitReason = fmt.Sprintf(" (signal %d)", sigNum)
				} else {
					exitReason = " (non-zero exit)"
				}
			} else {
				exitCode = 1
				exitReason = " (process error)"
			}
		} else {
			exitCode = 0
			exitReason = " (clean exit)"
		}
		
		s.mu.Lock()
		s.info.Active = false
		sessionID := s.info.ID
		s.mu.Unlock()
		
		// Close WebSocket connections with exit code and reason
		s.closeClientsWithExitCode(exitCode, exitReason)
		
		// Remove session from manager - process has exited and won't be reused
		ptyManager.Delete(sessionID)
		
		log.Infof("PTY session %s process exited with code %d%s and cleaned up", sessionID, exitCode, exitReason)
	}()

	return nil
}

func (s *PTYSession) kill() {
	// kill process and PTY
	s.mu.Lock()
	// Check if already killed to prevent double-kill
	if !s.info.Active {
		s.mu.Unlock()
		return
	}
	
	sessionID := s.info.ID
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

	// Close WebSocket connections with kill exit code - 137 = 128 + 9 (SIGKILL)
	s.closeClientsWithExitCode(137, " (SIGKILL)")
	
	// Remove session from manager - manually killed
	ptyManager.Delete(sessionID)
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
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Debug("ws read error:", err)
			}
			return
		}
		// Send all message data to PTY (text or binary)
		if err := s.sendToPTY(data); err != nil {
			// Send error to client and close connection
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
				websocket.CloseInternalServerErr, "PTY session unavailable",
			))
			return
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

func (s *PTYSession) sendToPTY(data []byte) error {
	// Check if inCh is available to prevent panic
	if s.inCh == nil {
		return fmt.Errorf("PTY session input channel not available")
	}
	
	select {
	case s.inCh <- data:
		return nil
	case <-s.ctx.Done():
		return fmt.Errorf("PTY session input channel closed")
	}
}

// closeClientsWithExitCode closes all WebSocket connections with structured exit data
func (s *PTYSession) closeClientsWithExitCode(exitCode int, exitReason string) {
	var wsCloseCode int
	var exitReasonStr *string
	
	// Map PTY exit codes to WebSocket close codes
	if exitCode == 0 {
		wsCloseCode = websocket.CloseNormalClosure
		exitReasonStr = nil // undefined for clean exit
	} else {
		wsCloseCode = websocket.CloseInternalServerErr
		// Set human-readable reason for non-zero exits
		switch {
		case exitCode == 130:
			reason := "Ctrl+C"
			exitReasonStr = &reason
		case exitCode == 137:
			reason := "SIGKILL"
			exitReasonStr = &reason
		case exitCode == 143:
			reason := "SIGTERM"
			exitReasonStr = &reason
		case exitCode > 128:
			sigNum := exitCode - 128
			reason := fmt.Sprintf("signal %d", sigNum)
			exitReasonStr = &reason
		default:
			reason := "non-zero exit"
			exitReasonStr = &reason
		}
	}

	// Create structured close data as JSON
	type CloseData struct {
		ExitCode   int     `json:"exitCode"`
		ExitReason *string `json:"exitReason,omitempty"`
	}
	
	closeData := CloseData{
		ExitCode:   exitCode,
		ExitReason: exitReasonStr,
	}
	
	closeJSON, _ := json.Marshal(closeData)

	s.clientsMu.Lock()
	for id, cl := range s.clients {
		_ = cl.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			wsCloseCode, string(closeJSON),
		))
		_ = cl.conn.Close()
		close(cl.send)
		delete(s.clients, id)
	}
	s.clientsMu.Unlock()
}

// ---- Control & resize ----

func (s *PTYSession) resize(cols, rows uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if session is still active
	if !s.info.Active {
		return errors.New("cannot resize inactive PTY session")
	}

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
			return err
		}
	} else {
		return errors.New("PTY file descriptor is not available")
	}
	return nil
}
