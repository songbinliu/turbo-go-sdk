package mediationcontainer

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/websocket"
	"sync"
)

const (
	Closed TransportStatus = "closed"
	Ready  TransportStatus = "ready"
)

type TransportStatus string

type WebSocketConnectionConfig MediationContainerConfig

func CreateWebSocketConnectionConfig(connConfig *MediationContainerConfig) (*WebSocketConnectionConfig, error) {
	_, err := url.ParseRequestURI(connConfig.LocalAddress)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse local URL from %s when create WebSocketConnectionConfig.", connConfig.LocalAddress)
	}
	// Change URL scheme from ws to http or wss to https.
	serverURL, err := url.ParseRequestURI(connConfig.TurboServer)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse turboServer URL from %s when create WebSocketConnectionConfig.", connConfig.TurboServer)
	}
	switch serverURL.Scheme {
	case "http":
		serverURL.Scheme = "ws"
	case "https":
		serverURL.Scheme = "wss"
	}
	wsConfig := WebSocketConnectionConfig(*connConfig)
	wsConfig.TurboServer = serverURL.String()
	return &wsConfig, nil
}

// ============================ ClientWebSocketTransport - Start, Connect, Close ======================================
// Implementation of the ITransport for WebSocket communication to send and receive serialized protobuf message bytes
type ClientWebSocketTransport struct {
	// current status of the transport layer.
	status                   TransportStatus
	ws                       *websocket.Conn // created during Connect()
	connConfig               *WebSocketConnectionConfig
	inputStreamCh            chan []byte // unbuffered channel
	closeRequested           bool
	stopListenerCh           chan struct{}
	mux sync.Mutex
	connClosedNotificationCh chan bool // channel where the transport connection error will be notified
}

// Instantiate a new ClientWebSocketTransport endpoint for the client
func CreateClientWebSocketTransport(connConfig *WebSocketConnectionConfig) *ClientWebSocketTransport {
	transport := &ClientWebSocketTransport{
		connConfig:               connConfig,
	}
	return transport
}

// WebSocket connection is established with the server
func (clientTransport *ClientWebSocketTransport) Connect() error {
	// Close any previous connected WebSocket connection and set current connection to nil.
	clientTransport.closeAndResetWebSocket()

	// loop till server is up or close received
	// TODO: give an optional timeout to wait for server in performWebSocketConnection()
	err := clientTransport.performWebSocketConnection() // Blocks or till transport is closed
	if err != nil {
		return fmt.Errorf("Cannot connect with server websocket %s", err)
	}

	glog.V(4).Infof("[Connect] Connected to server " + clientTransport.GetConnectionId())

	clientTransport.stopListenerCh = make(chan struct{}) // Channel to stop the routine that listens for messages
	clientTransport.inputStreamCh = make(chan []byte)   // Message Queue
	clientTransport.connClosedNotificationCh = make(chan bool)
	clientTransport.closeRequested = false

	// Message handler for received messages
	clientTransport.ListenForMessages() // spawns a new routine
	return nil
}

func (clientTransport *ClientWebSocketTransport) NotifyClosed() chan bool {
	return clientTransport.connClosedNotificationCh
}

func (clientTransport *ClientWebSocketTransport) GetConnectionId() string {
	if clientTransport.status == Closed {
		return ""
	}
	return clientTransport.ws.RemoteAddr().String() + "::" + clientTransport.ws.LocalAddr().String()
}

// Close the WebSocket Transport point
func (clientTransport *ClientWebSocketTransport) CloseTransportPoint() {
	clientTransport.mux.Lock()
	defer clientTransport.mux.Unlock()
	if clientTransport.closeRequested {
		return
	}

	glog.V(4).Infof("[CloseTransportPoint] closing transport endpoint and listener routine")
	clientTransport.closeRequested = true

	// close channels
	close(clientTransport.stopListenerCh)
	close(clientTransport.connClosedNotificationCh)
	close(clientTransport.inputStreamCh)

	clientTransport.closeAndResetWebSocket()
}

// Close current WebSocket connection and set it to nil.
func (clientTransport *ClientWebSocketTransport) closeAndResetWebSocket() {
	if clientTransport.status == Closed {
		return
	}
	// close WebSocket
	if clientTransport.ws != nil {
		clientTransport.ws.Close()
		clientTransport.ws = nil
	}
	clientTransport.status = Closed
}

// ================================================= Message Listener =============================================

// Routine to listen for messages on the websocket.
// The websocket is continuously checked for messages and queued on the clientTransport.inputStream channel
// Routine exits when a message is sent on clientTransport.stopListenerCh.
//
func (clientTransport *ClientWebSocketTransport) ListenForMessages() {
	glog.V(3).Infof("[ListenForMessages] %s : ENTER  ", time.Now())

	go func() {
		for {
			glog.V(4).Info("[ListenForMessages] waiting for messages on websocket transport")
			glog.V(4).Infof("[ListenForMessages] waiting for messages on websocket transport : %++v", clientTransport)
			select {
			case <-clientTransport.stopListenerCh:
				glog.V(2).Infof("[ListenForMessages] stop listen for new msg from websocket.")
				return
			default:
				if clientTransport.status != Ready {
					glog.Errorf("WebSocket transport layer status is %s", clientTransport.status)
					glog.Errorf("WebSocket is not ready.")
					continue
				}
				glog.V(2).Infof("[ListenForMessages]: connected, waiting for server response ...")
				var data []byte = make([]byte, 1024)
				err := websocket.Message.Receive(clientTransport.ws, &data)
				// Notify errors and break
				if err != nil {
					//TODO handle err according to possible type. Now reconnect when never there is an error, which may not necessary.
					if err == io.EOF {
						glog.Errorf("[ListenForMessages] received EOF on websocket %s", err)
					}

					glog.Errorf("[ListenForMessages] error during receive %v", err)
					glog.Errorf("Close websocket connection now.")
					clientTransport.CloseTransportPoint()
					glog.V(2).Infof("[ListenForMessages] error notified, will re-establish websocket connection")
					break
				}
				// write the message on the channel
				glog.V(3).Infof("[ListenForMessages] received message on websocket")
				clientTransport.queueRawMessage(data) // Note: this will block till the message is read
				glog.V(4).Infof("[ListenForMessages] delivered websocket message, continue listening for server messages...")
			} //end select
		} //end for
	}()
	glog.V(4).Infof("[ListenForMessages] : END")
}

func (clientTransport *ClientWebSocketTransport) queueRawMessage(data []byte) {
	//TODO: this read should be accumulative - see onMessageReceived in AbstractWebsocketTransport
	clientTransport.inputStreamCh <- data
	// TODO: how to ensure that the channel is open to write
}

func (clientTransport *ClientWebSocketTransport) RawMessageReceiver() chan []byte {
	return clientTransport.inputStreamCh
}

// ==================================================== Message Sender ===============================================
// Send serialized protobuf message bytes
func (clientTransport *ClientWebSocketTransport) Send(messageToSend *TransportMessage) error {
	if clientTransport.closeRequested {
		glog.Errorf("Cannot send message : transport endpoint is closed")
		return errors.New("Cannot send message: transport endpoint is closed")
	}

	if clientTransport.status != Ready {
		glog.Errorf("WebSocket transport layer status is %s", clientTransport.status)
		return errors.New("Cannot send message: web socket is not ready")
	}
	if messageToSend == nil { //.RawMsg == nil {
		glog.Errorf("Cannot send message : marshalled msg is nil")
		return errors.New("Cannot send message: marshalled msg is nil")
	}
	if clientTransport.ws.IsClientConn() {
		glog.V(4).Infof("Sending message on client transport %+v", clientTransport.ws)
	}
	err := websocket.Message.Send(clientTransport.ws, messageToSend.RawMsg)
	if err != nil {
		glog.Errorf("Error sending message on client transport: %s", err)
		return fmt.Errorf("Error sending message on client transport: %s", err)
	}
	glog.V(4).Infof("Successfully sent message on client transport")
	return nil
}

// ====================================== Websocket Connection =========================================================
// Establish connection to server websocket until connected or until the transport endpoint is closed
func (clientTransport *ClientWebSocketTransport) performWebSocketConnection() error {
	connRetryIntervalSeconds := time.Second * 30 // TODO: use ConnectionRetry parameter from the connConfig or default
	connConfig := clientTransport.connConfig
	// WebSocket URL
	vmtServerUrl := connConfig.TurboServer + connConfig.WebSocketPath
	glog.V(2).Infof("[performWebSocketConnection]: %s", vmtServerUrl)

	for !clientTransport.closeRequested { // only set when CloseTransportPoint() is called
		ws, err := openWebSocketConn(connConfig, vmtServerUrl)

		if err != nil {
			// print at debug level after some time
			glog.V(3).Infof("[performWebSocketConnection] %v : unable to connect to %s. Retrying in %v\n", time.Now(), vmtServerUrl, connRetryIntervalSeconds)

			time.Sleep(connRetryIntervalSeconds)
		} else {
			clientTransport.ws = ws
			clientTransport.status = Ready
			glog.V(2).Infof("[performWebSocketConnection]*********** Connected to server " + clientTransport.GetConnectionId())
			glog.V(2).Infof("WebSocket transport layer is ready.")

			return nil
		}
	}
	glog.V(4).Infof("[performWebSocketConnection] exit connect routine, close = %s ", clientTransport.closeRequested)
	return errors.New("Abort client socket connect, transport is closed")
}

func openWebSocketConn(connConfig *WebSocketConnectionConfig, vmtServerUrl string) (*websocket.Conn, error) {
	config, err := websocket.NewConfig(vmtServerUrl, connConfig.LocalAddress)
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	usrpasswd := []byte(connConfig.WebSocketUsername + ":" + connConfig.WebSocketPassword)

	config.Header = http.Header{
		"Authorization": {"Basic " + base64.StdEncoding.EncodeToString(usrpasswd)},
	}
	config.TlsConfig = &tls.Config{InsecureSkipVerify: true}
	webs, err := websocket.DialConfig(config)

	if err != nil {
		glog.Error(err)
		if webs == nil {
			glog.Error("[openWebSocketConn] websocket is null, reset")
		}
		return nil, err
	}

	glog.V(2).Infof("[openWebSocketConn] created webSocket : %s+v", vmtServerUrl)
	return webs, nil
}
