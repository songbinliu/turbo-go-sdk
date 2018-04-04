package wsocket

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/golang/glog"
	"github.com/gorilla/websocket"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	deliverWait = 20 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = 60 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 8192
)

type ConnectionConfig struct {
	url              string
	user             string
	password         string
	scheme           string
	handshakeTimeout time.Duration
}

func NewConnectionConfig(host, path, user, passwd string) (*ConnectionConfig, error) {
	addr, err := url.Parse(host)
	if err != nil {
		glog.Errorf("Failed to parse host(%v): %v", host, err)
		return nil, err
	}

	scheme := "ws"
	if addr.Scheme == "https" {
		scheme = "wss"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   addr.Host,
		Path:   path,
	}

	return &ConnectionConfig{
		url:              u.String(),
		user:             user,
		password:         passwd,
		scheme:           scheme,
		handshakeTimeout: time.Second * 60,
	}, nil
}

type WSconnection struct {
	wsocket    *websocket.Conn

	mux        sync.Mutex
	closed     bool
	stop       chan struct{}

	//indicate whether the Write Pump has started
	writeStart bool

	// Buffered channel of outbound messages.
	send       chan []byte

	// Buffered channel of inbound messages.
	received   chan []byte
}

func getAuthHeader(user, password string) http.Header {
	dat := []byte(fmt.Sprintf("%s:%s", user, password))
	header := http.Header{
		"Authorization": {"Basic " + base64.StdEncoding.EncodeToString(dat)},
	}

	return header
}

func NewConnection(conf *ConnectionConfig) *WSconnection {

	//1. set up dialer
	d := &websocket.Dialer{
		HandshakeTimeout: conf.handshakeTimeout,
	}
	if conf.scheme == "wss" {
		d.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	//2. auth header
	header := getAuthHeader(conf.user, conf.password)

	//3. connect it
	c, _, err := d.Dial(conf.url, header)
	if err != nil {
		glog.Errorf("Failed to connect to server(%s): %v", conf.url, err)
		return nil
	}

	result := &WSconnection{
		wsocket:  c,
		closed:   false,
		stop: make(chan struct{}),
		writeStart: false,
		send:     make(chan []byte),
		received: make(chan []byte),
	}
	return result
}

// SendRecv() should be called before Start()
func (ws *WSconnection) SendRecv(req []byte, waitSeconds int) ([]byte, error) {
	//1. send request
	err := ws.write(websocket.BinaryMessage, req)
	if err != nil {
		glog.Errorf("Failed to send request via websocket: %v", err)
		return nil, err
	}

	//2. receive response
	if waitSeconds > 0 {
		du := time.Second * time.Duration(waitSeconds)
		ws.wsocket.SetReadDeadline(time.Now().Add(du))
	}

	mt, resp, err := ws.wsocket.ReadMessage()
	glog.V(3).Infof("Received websocket msg: type=%d", mt)
	if err != nil {
		glog.Errorf("Failed to receive response via websocket: %v", err)
		return nil, err
	}

	return resp, nil
}

func (ws *WSconnection) write(mtype int, payload []byte) error {
	ws.wsocket.SetWriteDeadline(time.Now().Add(writeWait))
	return ws.wsocket.WriteMessage(mtype, payload)
}

func (ws *WSconnection) sendPing() error {
	glog.V(3).Infof("begin to send Ping message")
	if err := ws.write(websocket.PingMessage, []byte{}); err != nil {
		glog.Errorf("Failed to send PingMessage to server:%v", err)
		return err
	}
	glog.V(3).Infof("Sent ping message success")
	return nil
}

func (ws *WSconnection) writePump() {
	ws.writeStart = true
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		glog.V(1).Infof("websocket writePump stops")
		ticker.Stop()
		close(ws.send)
		ws.Stop()
		ws.write(websocket.CloseMessage, []byte{})
	}()
	ws.sendPing()

	for {
		select {
		case <- ws.stop:
			glog.V(1).Infof("Write pump is stopped.")
			return
		case message, ok := <-ws.send:
			if !ok {
				glog.Errorf("sending channel is closed or error")
				return
			}
			if err := ws.write(websocket.BinaryMessage, message); err != nil {
				glog.Errorf("Failed to send message to server:%v", err)
				return
			}
		case <-ticker.C:
			if err := ws.sendPing(); err != nil {
				glog.Errorf("Failed to send PingMsg")
				return
			}
		}
	}
}

func (ws *WSconnection) GetReceived() (chan []byte, error) {
	if ws.IsClosed() {
		return nil, fmt.Errorf("websocket is closed")
	}

	return ws.received, nil
}

func (ws *WSconnection) PushSend(dat []byte, timeout time.Duration) error {
	if ws.IsClosed() {
		glog.Errorf("Send data failed: web socket is closed.")
		return fmt.Errorf("websocket is closed")
	}

	timer := time.NewTimer(timeout)
	select {
	case ws.send <- dat:
		glog.V(3).Infof("pushed dat into sending queue")
		return nil
	case <-timer.C:
		err := fmt.Errorf("timeout when pushing data into sending queue")
		glog.Errorf(err.Error())
		return err
	}
}

func (ws *WSconnection) readPump() {
	defer func() {
		glog.V(2).Infof("websocket readPump stops")
		close(ws.received)
		ws.Stop()
	}()

	ws.wsocket.SetReadLimit(maxMessageSize)
	for {
		glog.V(2).Infof("Begin to wait for next websocket msg.")
		mt, msg, err := ws.wsocket.ReadMessage()
		if err != nil {
			glog.Errorf("stop websocket reader because of read error: %v", err)
			return
		}
		glog.V(2).Infof("Got msg(type=%d) from websocket, begin to deliver it", mt)


		timer := time.NewTimer(deliverWait)
		select {
		case <- ws.stop:
			glog.V(1).Infof("Read pump is stoppped.")
			return
		case ws.received <- msg:
			glog.V(2).Infof("msg is received, and sent to receiving queue.")
		case <-timer.C:
			glog.Errorf("receiving channel is blocked, discard the two blocked messages")
			<-ws.received
		}
	}
}

func (ws *WSconnection) IsClosed() bool {
	ws.mux.Lock()
	defer ws.mux.Unlock()
	return ws.closed
}

//this function is synchronized
func (ws *WSconnection) Stop() {
	ws.mux.Lock()
	defer ws.mux.Unlock()

	if ws.closed {
		return
	}

	glog.V(2).Infof("Begin to stop websocket read/write pumps, and close connection.")
	ws.closed = true
	close(ws.stop)

	if !ws.writeStart {
		// send a close-frame before closing
		ws.write(websocket.CloseMessage, []byte{})
		ws.wsocket.Close()
	}
}

func (ws *WSconnection) SetupPingPong() {
	h := func(message string) error {
		glog.V(3).Infof("Recevied ping msg")
		ws.wsocket.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(writeWait))
		return nil
	}
	ws.wsocket.SetPingHandler(h)

	h2 := func(message string) error {
		glog.V(3).Infof("Received pong msg")
		return nil
	}
	ws.wsocket.SetPongHandler(h2)
}

func (ws *WSconnection) Start() error {
	glog.V(2).Infof("Begin to start websocket read/write pumps.")
	ws.SetupPingPong()

	go ws.writePump()
	go ws.readPump()
	return nil
}
