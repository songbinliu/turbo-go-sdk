package mediationcontainer

import (
	"time"

	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/wsocket"
	"github.com/turbonomic/turbo-go-sdk/pkg/version"

	protobuf "github.com/golang/protobuf/proto"

	"github.com/golang/glog"
	"fmt"
)

const (
	waitResponseTimeOut = time.Second * 30
)

// Abstraction to establish session using the specified protocol with the server
// and handle server messages for the different probes in the Mediation Container
type remoteMediationClient struct {
	// All the probes
	allProbes map[string]*ProbeProperties
	// The container info containing the communication config for all the registered probes
	containerConfig *MediationContainerConfig
	// Associated Transport
	Transport ITransport
	// Map of Message Handlers to receive server messages
	MessageHandlers  map[RequestType]RequestHandler
	stopMsgHandlerCh chan bool
	// Channel for receiving responses from the registered probes to be sent to the server
	probeResponseChan chan *proto.MediationClientMessage
	// Channel to stop the mediation client and the underlying transport and message handling
	stopMediationClientCh chan struct{}
	//  Channel to stop the routine that monitors the underlying transport connection
	closeWatcherCh chan bool

	//new items
	wsconn *wsocket.WSconnection
	wsConfig *wsocket.ConnectionConfig
	wsRetryDuration time.Duration
	protocolVersion string
	shouldStop bool
}

func CreateRemoteMediationClient(allProbes map[string]*ProbeProperties,
	containerConfig *MediationContainerConfig) *remoteMediationClient {
	remoteMediationClient := &remoteMediationClient{
		MessageHandlers:       make(map[RequestType]RequestHandler),
		allProbes:             allProbes,
		containerConfig:       containerConfig,
		probeResponseChan:     make(chan *proto.MediationClientMessage),
		stopMediationClientCh: make(chan struct{}),
	}

	glog.V(4).Infof("Created channels : probeResponseChan %s, stopMediationClientCh %s\n",
		remoteMediationClient.probeResponseChan, remoteMediationClient.stopMediationClientCh)

	// Create message handlers
	remoteMediationClient.createMessageHandlers(remoteMediationClient.probeResponseChan)

	glog.V(2).Infof("Created remote mediation client")

	return remoteMediationClient
}

// Establish connection with the Turbo server -  Blocks till WebSocket connection is open
// Complete the probe registration protocol with the server and then wait for server messages
func (remoteMediationClient *remoteMediationClient) Init(probeRegisteredMsgCh chan bool) {
	// TODO: Assert that the probes are registered before starting the handshake ??

	//// --------- Create WebSocket Transport
	connConfig, err := CreateWebSocketConnectionConfig(remoteMediationClient.containerConfig)
	if err != nil {
		glog.Errorf("Initialization of remote mediation client failed, null transport : " + err.Error())
		// TODO: handle error
		//remoteMediationClient.Stop()
		//probeRegisteredMsg <- false
		return
	}

	// Sdk Protocol handler
	sdkProtocolHandler := CreateSdkClientProtocolHandler(remoteMediationClient.allProbes,
		remoteMediationClient.containerConfig.Version)
	// ------ Websocket transport

	transport := CreateClientWebSocketTransport(connConfig) //, transportClosedNotificationCh)
	remoteMediationClient.closeWatcherCh = make(chan bool, 1)

	err = transport.Connect() // TODO: blocks till websocket connection is open or until transport is closed

	// handle WebSocket creation errors
	if err != nil { //transport.ws == nil {
		glog.Errorf("Initialization of remote mediation client failed, null transport")
		remoteMediationClient.Stop()
		probeRegisteredMsgCh <- false
		return
	}

	remoteMediationClient.Transport = transport

	// -------- Start protocol handler separate thread
	// Initiate protocol to connect to server
	glog.V(2).Infof("Start sdk client protocol ........")
	sdkProtocolDoneCh := make(chan bool, 1) // TODO: using a channel so we can add timeout or
	// wait till message is received from the Protocol handler
	go sdkProtocolHandler.handleClientProtocol(remoteMediationClient.Transport, sdkProtocolDoneCh)

	status := <-sdkProtocolDoneCh

	glog.V(4).Infof("Sdk client protocol complete, status = ", status)
	if !status {
		glog.Errorf("Registration with server failed")
		probeRegisteredMsgCh <- status
		remoteMediationClient.Stop()
		return
	}

	// Routine to monitor the websocket connection
	go func() {
		glog.V(3).Infof("[Reconnect] start monitoring the transport connection")
		for {
			select {
			case <-remoteMediationClient.closeWatcherCh:
				glog.V(4).Infof("[Reconnect] Exit routine *************")
				return
			case <-transport.NotifyClosed():
				glog.V(2).Infof("[Reconnect] transport endpoint is closed, starting reconnect ...")

				// stop server messages listener
				remoteMediationClient.stopMessageHandler()
				// Reconnect
				err := transport.Connect()
				// handle WebSocket creation errors
				if err != nil { //transport.ws == nil {
					glog.Errorf("[Reconnect] Initialization of remote mediation client failed, null transport")
					remoteMediationClient.Stop()
					break
				}
				// sdk registration protocol
				transportReady := make(chan bool, 1)
				sdkProtocolHandler.handleClientProtocol(transport, transportReady)
				endProtocol := <-transportReady
				if !endProtocol {
					glog.Errorf("[Reconnect] Registration with server failed")
					remoteMediationClient.Stop()
					break
				}
				// start listener for server messages
				remoteMediationClient.stopMsgHandlerCh = make(chan bool)
				go remoteMediationClient.RunServerMessageHandler(remoteMediationClient.Transport)
				glog.V(3).Infof("[Reconnect] transport endpoint connect complete")
			} //end select
		} // end for
	}() // end go routine

	// --------- Listen for server messages
	remoteMediationClient.stopMsgHandlerCh = make(chan bool)
	go remoteMediationClient.RunServerMessageHandler(remoteMediationClient.Transport)

	// Send registration status to the upper layer
	defer close(probeRegisteredMsgCh)
	defer close(sdkProtocolDoneCh)
	probeRegisteredMsgCh <- status
	glog.V(3).Infof("Sent registration status on channel %s\n", probeRegisteredMsgCh)

	glog.V(3).Infof("Remote mediation initialization complete")
	// --------- Wait for exit notification
	select {
	case <-remoteMediationClient.stopMediationClientCh:
		glog.V(4).Infof("[Init] Exit routine *************")
		return
	}
}

func(m *remoteMediationClient) Start() {
	for {
		glog.V(2).Infof("Begin protocol handshake process ...")
		flag := m.ProtocolHandShake()
		if !flag {
			err := fmt.Errorf("MediationClient failed to do protocol hand shake, terminating.")
			glog.Errorf(err.Error())
			return err
		}

		glog.V(2).Infof("begin to server Turbo requests ...")

		if m.shouldStop {
			glog.V(1).Infof("Mediation client is stopped.")
			return
		}

		du := m.wsRetryDuration
		glog.Errorf("websocket is closed. Will re-connect in %v seconds.", du.Seconds())
		time.Sleep(du)
	}
}

// Stop the remote mediation client by closing the underlying transport and message handler routines
func (m *remoteMediationClient) Stop() {
	// First stop the transport connection monitor
	close(m.closeWatcherCh)
	// Stop the server message listener
	m.stopMessageHandler()
	// Close the transport
	if m.Transport != nil {
		m.Transport.CloseTransportPoint()
	}
	// Notify the client to stop
	close(m.stopMediationClientCh)

	// new staff
	m.shouldStop = true
	if m.wsconn != nil {
		m.wsconn.Stop()
	}
}

func (m *remoteMediationClient) ProtocolHandShake() bool {

	for {
		glog.V(2).Infof("begin to connect to server, and do protocol hand shake.")
		m.buildWSConnection()

		glog.V(2).Infof("begin to do protocol hand shake")
		flag, err := m.doProtocolHandShake()
		if err == nil {
			return flag
		}

		if !flag {
			return false
		}

		du := time.Second * 20
		glog.Errorf("protocolHandShake failed, will retry in %v seconds", du.Seconds())
		time.Sleep(du)
	}
}


func (m *remoteMediationClient) WaitServerRequests() {
	m.wsconn.Start()

	for {
		//1. check whether MediationClient should be stopped
		if m.shouldStop {
			glog.V(1).Info("Stop waiting for server request: MediationClient should be stopped.")
			return
		}

		//2. check whether the underlying websocket is stopped
		if m.wsconn.IsClosed() {
			glog.V(1).Info("Stop waiting for server request: websocket is closed.")
			return
		}

		//3. get request from server, and handle it
		datch, err := m.wsconn.GetReceived()
		if err != nil {
			glog.Errorf("Stop waiting for server request: %v", err)
			return
		}

		timer := time.NewTimer(time.Second * 10)
		select {
		case dat := <-datch:
			if m.wsconn.IsClosed() {
				glog.V(1).Info("Stop waiting for server request: websocket is closed.")
				return
			}
			go m.handleServerRequest(dat)
		case <-timer.C:
			continue
		}
	}
}

func (m *remoteMediationClient) handleServerRequest(dat []byte) error {
	return nil
}

func (m *remoteMediationClient) buildWSConnection() error {

	if m.wsconn != nil {
		m.wsconn.Stop()
		m.wsconn = nil
	}

	for {
		if m.shouldStop {
			return fmt.Errorf("Stopped")
		}

		wsconn := wsocket.NewConnection(m.wsConfig)
		if wsconn == nil {
			glog.Errorf("Failed to build websocket connection: %++v", m.wsConfig)
			glog.Errorf("Will Retry in %v seconds", m.wsRetryDuration.Seconds())
			time.Sleep(m.wsRetryDuration)
			continue
		}

		m.wsconn = wsconn
		break
	}

	return nil
}

func (m *remoteMediationClient) negotiationVersion() (bool, error) {
	//1. negotiation protocol version
	request := &version.NegotiationRequest{
		ProtocolVersion: &m.protocolVersion,
	}

	dat_in, err := protobuf.Marshal(request)
	if err != nil {
		glog.Errorf("Failed to marshal Negotiation request(%++v): %v", request, err)
		return false, err
	}

	//2. send request and get answer
	dat_out, err := m.wsconn.SendRecv(dat_in, waitResponseTimeOut)
	if err != nil {
		glog.Errorf("Failed to get negotiation response: %v", err)
		// will retry
		return true, err
	}

	//3. parse the answer
	resp := &version.NegotiationAnswer{}
	if err := protobuf.Unmarshal(dat_out, resp); err != nil {
		glog.Errorf("Failed to unmarshal negotiaonAnswer(%s): %v", string(dat_out), err)
		//will retry
		return true, err
	}

	result := resp.GetNegotiationResult()
	if result != version.NegotiationAnswer_ACCEPTED {
		glog.Errorf("Protocol Version(%v) Negotiation is not accepted: %v", m.protocolVersion, resp.GetDescription())
		return false, nil
	}
	glog.V(2).Infof("Protocol Version Negotiaion success: %v", m.protocolVersion)

	return true, nil
}

func (m *remoteMediationClient) makeContainerInfo () (*proto.ContainerInfo, error) {
	var probes []*proto.ProbeInfo

	for k, v := range m.allProbes {
		glog.V(2).Infof("SdkClientProtocol] Creating Probe Info for", k)
		turboProbe := v.Probe
		var probeInfo *proto.ProbeInfo
		var err error
		probeInfo, err = turboProbe.GetProbeInfo()

		if err != nil {
			return nil, err
		}
		probes = append(probes, probeInfo)
	}

	return &proto.ContainerInfo{
		Probes: probes,
	}, nil
}

func (m *remoteMediationClient) registerProbe() (bool, error) {
	//1. probe info
	request, err := m.makeContainerInfo()
	if err != nil {
		glog.Errorf("Failed to get container info: %v", err)
		return false, err
	}

	dat_in, err := protobuf.Marshal(request)
	if err != nil {
		glog.Errorf("Failed to marshal probeInfo (%++v): %v", request, err)
		return false, err
	}

	//2. send request and get response
	dat_out, err := m.wsconn.SendRecv(dat_in, waitResponseTimeOut)
	if err != nil {
		glog.Errorf("Failed to get registration response: %v", err)
		return true, err
	}

	//3. parse the answer
	resp := &proto.Ack{}
	if err := protobuf.Unmarshal(dat_out, resp); err != nil {
		glog.Errorf("Failed to unmarshl registration ack(%s): %v", string(dat_out), err)
		return false, err
	}

	return true, nil
}

func (m *remoteMediationClient) doProtocolHandShake() (bool, error) {

	//1. protocol version negotiation
	flag, err := m.negotiationVersion()
	if err != nil {
		glog.Errorf("protocolHandShake failed: %v", err)
		return flag, err
	}

	if !flag {
		glog.Errorf("protocolHandShake is not accepted: %s is not accepted", m.protocolVersion)
		return false, nil
	}
	glog.V(3).Infof("probe protocol version negotiation success")

	//2. register probe info
	flag, err = m.registerProbe()
	if err != nil {
		glog.Errorf("protocolHandShake failed: %v", err)
		return flag, err
	}
	glog.V(3).Infof("probe registration success")

	return true, nil
}



// ======================== Listen for server messages ===================
// Sends message to the server message listener to close the protobuf endpoint and message listener
func (remoteMediationClient *remoteMediationClient) stopMessageHandler() {
	if remoteMediationClient.stopMsgHandlerCh != nil {
		close(remoteMediationClient.stopMsgHandlerCh)
	}
}

// Checks for incoming server messages received by the ProtoBuf endpoint created to handle server requests
func (remoteMediationClient *remoteMediationClient) RunServerMessageHandler(transport ITransport) {
	glog.V(2).Infof("[handleServerMessages] %s : ENTER  ", time.Now())

	// Create Protobuf Endpoint to handle server messages
	protoMsg := &MediationRequest{} // parser for the server requests
	endpoint := CreateClientProtoBufEndpoint("ServerRequestEndpoint", transport, protoMsg, false)
	logPrefix := "[handleServerMessages][" + endpoint.GetName() + "] : "

	// Spawn a new go routine that serves as a Callback for Probes when their response is ready
	go remoteMediationClient.runProbeCallback(endpoint) // this also exits using the stopMsgHandlerCh

	// main loop for listening to server message.
	for {
		glog.V(2).Infof(logPrefix + "waiting for parsed server message .....") // make debug
		// Wait for the server request to be received and parsed by the protobuf endpoint
		select {
		case <-remoteMediationClient.stopMsgHandlerCh:
			glog.V(4).Infof(logPrefix + "Exit routine ***************")
			endpoint.CloseEndpoint() //to stop the message listener and close the channel
			return
		case parsedMsg, ok := <-endpoint.MessageReceiver(): // block till a message appears on the endpoint's message channel
			if !ok {
				glog.Errorf(logPrefix + "endpoint message channel is closed")
				break // return or continue ?
			}
			glog.V(3).Infof(logPrefix+"received: %++v\n", parsedMsg)

			// Handler response - find the handler to handle the message
			serverRequest := parsedMsg.ServerMsg
			requestType := getRequestType(serverRequest)

			requestHandler := remoteMediationClient.MessageHandlers[requestType]
			if requestHandler == nil {
				glog.Errorf(logPrefix + "cannot find message handler for request type " + string(requestType))
			} else {
				// Dispatch on a new thread
				// TODO: create MessageOperationRunner to handle this request for a specific message id
				go requestHandler.HandleMessage(serverRequest, remoteMediationClient.probeResponseChan)
				glog.Infof(logPrefix + "message dispatched, waiting for next one")
			}
		} //end select
	} //end for
	glog.Infof(logPrefix + "DONE")
}

// Run probe callback to the probe response to the server.
// Probe responses put on the probeResponseChan by the different message handlers are sent to the server
func (remoteMediationClient *remoteMediationClient) runProbeCallback(endpoint ProtobufEndpoint) {
	glog.V(4).Infof("[runProbeCallback] %s : ENTER  ", time.Now())
	for {
		glog.V(4).Infof("[probeCallback] waiting for probe responses")
		select {
		case <-remoteMediationClient.stopMsgHandlerCh:
			glog.V(4).Infof("[probeCallback] Exit routine *************")
			return
		case msg := <-remoteMediationClient.probeResponseChan:
			glog.V(4).Infof("[probeCallback] received response on probe channel %v\n ", remoteMediationClient.probeResponseChan)
			endMsg := &EndpointMessage{
				ProtobufMessage: msg,
			}
			endpoint.Send(endMsg)
		} // end select
	}
	glog.V(4).Infof("[probeCallback] DONE")
}

// ======================== Message Handlers ============================
type RequestType string

const (
	DISCOVERY_REQUEST  RequestType = "Discovery"
	VALIDATION_REQUEST RequestType = "Validation"
	INTERRUPT_REQUEST  RequestType = "Interrupt"
	ACTION_REQUEST     RequestType = "Action"
	UNKNOWN_REQUEST    RequestType = "Unknown"
)

func getRequestType(serverRequest proto.MediationServerMessage) RequestType {
	if serverRequest.GetValidationRequest() != nil {
		return VALIDATION_REQUEST
	} else if serverRequest.GetDiscoveryRequest() != nil {
		return DISCOVERY_REQUEST
	} else if serverRequest.GetActionRequest() != nil {
		return ACTION_REQUEST
	} else if serverRequest.GetInterruptOperation() > 0 {
		return INTERRUPT_REQUEST
	} else {
		return UNKNOWN_REQUEST
	}
}

type RequestHandler interface {
	HandleMessage(serverRequest proto.MediationServerMessage, probeMsgChan chan *proto.MediationClientMessage)
}

func (remoteMediationClient *remoteMediationClient) createMessageHandlers(probeMsgChan chan *proto.MediationClientMessage) {
	allProbes := remoteMediationClient.allProbes
	remoteMediationClient.MessageHandlers[DISCOVERY_REQUEST] = &DiscoveryRequestHandler{
		probes: allProbes,
	}
	remoteMediationClient.MessageHandlers[VALIDATION_REQUEST] = &ValidationRequestHandler{
		probes: allProbes,
	}
	remoteMediationClient.MessageHandlers[INTERRUPT_REQUEST] = &InterruptMessageHandler{
		probes: allProbes,
	}
	remoteMediationClient.MessageHandlers[ACTION_REQUEST] = &ActionMessageHandler{
		probes: allProbes,
	}

	var keys []RequestType
	for k := range remoteMediationClient.MessageHandlers {
		keys = append(keys, k)
	}
	glog.V(4).Infof("Created message handlers for server message types : [%s]", keys)
}

// -------------------------------- Discovery Request Handler -----------------------------------
type DiscoveryRequestHandler struct {
	probes map[string]*ProbeProperties
}

func (discReqHandler *DiscoveryRequestHandler) HandleMessage(serverRequest proto.MediationServerMessage,
	probeMsgChan chan *proto.MediationClientMessage) {
	request := serverRequest.GetDiscoveryRequest()
	probeType := request.ProbeType

	probeProps, exist := discReqHandler.probes[*probeType]
	if !exist {
		glog.Errorf("Received: discovery request for unknown probe type: %s", *probeType)
		return
	}
	glog.V(3).Infof("Received: discovery for probe type: %s", *probeType)

	turboProbe := probeProps.Probe
	msgID := serverRequest.GetMessageID()

	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		for {
			discReqHandler.keepDiscoveryAlive(msgID, probeMsgChan)

			t := time.NewTimer(time.Second * 10)
			select {
			case <-stopCh:
				glog.V(4).Infof("Cancel keep alive for msgID ", msgID)
				return
			case <-t.C:
			}
		}

	}()

	accountValues := request.GetAccountValue()
	var discoveryResponse *proto.DiscoveryResponse
	switch requestType := request.GetDiscoveryType(); requestType {
	case proto.DiscoveryType_FULL:
		discoveryResponse = turboProbe.DiscoverTarget(accountValues)
	case proto.DiscoveryType_INCREMENTAL:
		discoveryResponse = turboProbe.DiscoverTargetIncremental(accountValues)
	case proto.DiscoveryType_PERFORMANCE:
		discoveryResponse = turboProbe.DiscoverTargetPerformance(accountValues)
	default:
		discoveryResponse = turboProbe.DiscoverTarget(accountValues)
	}

	clientMsg := NewClientMessageBuilder(msgID).SetDiscoveryResponse(discoveryResponse).Create()

	// Send the response on the callback channel to send to the server
	probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(3).Infof("Sent discovery response for %d:%s", clientMsg.GetMessageID(), request.GetDiscoveryType())

	// Send empty response to signal completion of discovery
	discoveryResponse = &proto.DiscoveryResponse{}
	clientMsg = NewClientMessageBuilder(msgID).SetDiscoveryResponse(discoveryResponse).Create()

	probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(2).Infof("Discovery has finished for %d:%s", clientMsg.GetMessageID(), request.GetDiscoveryType())

	// Cancel keep alive
	// Note  : Keep alive routine is cancelled when the stopCh is closed at the end of this method
	// when the discovery response is out on the probeMsgCha
}

// Send the KeepAlive message to server in order to inform server the discovery is stil ongoing. Prevent timeout.
func (discReqHandler *DiscoveryRequestHandler) keepDiscoveryAlive(msgID int32, probeMsgChan chan *proto.MediationClientMessage) {
	keepAliveMsg := new(proto.KeepAlive)
	clientMsg := NewClientMessageBuilder(msgID).SetKeepAlive(keepAliveMsg).Create()

	// Send the response on the callback channel to send to the server
	probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(3).Infof("Sent keep alive response %d", clientMsg.GetMessageID())
}

// -------------------------------- Validation Request Handler -----------------------------------
type ValidationRequestHandler struct {
	probes map[string]*ProbeProperties //TODO: synchronize access to the probes map
}

func (valReqHandler *ValidationRequestHandler) HandleMessage(serverRequest proto.MediationServerMessage,
	probeMsgChan chan *proto.MediationClientMessage) {
	request := serverRequest.GetValidationRequest()
	probeType := request.ProbeType
	probeProps, exist := valReqHandler.probes[*probeType]
	if !exist {
		glog.Errorf("Received: validation request for unknown probe type : %s", *probeType)
		return
	}
	glog.V(3).Infof("Received: validation for probe type: %s\n ", *probeType)
	turboProbe := probeProps.Probe

	var validationResponse *proto.ValidationResponse
	validationResponse = turboProbe.ValidateTarget(request.GetAccountValue())

	msgID := serverRequest.GetMessageID()
	clientMsg := NewClientMessageBuilder(msgID).SetValidationResponse(validationResponse).Create()

	// Send the response on the callback channel to send to the server
	probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(3).Infof("Sent validation response %d", clientMsg.GetMessageID())
}

// -------------------------------- Action Request Handler -----------------------------------
// Message handler that will receive the Action Request for entities in the TurboProbe.
// Action request will be delegated to the right TurboProbe. Multiple ActionProgress and final ActionResult
// responses are sent back to the server.
type ActionMessageHandler struct {
	probes map[string]*ProbeProperties
}

func (actionReqHandler *ActionMessageHandler) HandleMessage(serverRequest proto.MediationServerMessage,
	probeMsgChan chan *proto.MediationClientMessage) {
	glog.V(4).Infof("[ActionMessageHandler] Received: action %s request", serverRequest)
	request := serverRequest.GetActionRequest()
	probeType := request.ProbeType
	if actionReqHandler.probes[*probeType] == nil {
		glog.Errorf("Received: Action request for unknown probe type : %s", *probeType)
		return
	}

	glog.V(3).Infof("Received: action %s request for probe type: %s\n ",
		request.ActionExecutionDTO.ActionType, *probeType)
	probeProps := actionReqHandler.probes[*probeType]
	turboProbe := probeProps.Probe

	msgID := serverRequest.GetMessageID()
	worker := NewActionResponseWorker(msgID, turboProbe,
		request.ActionExecutionDTO, request.GetAccountValue(), probeMsgChan)
	worker.start()
}

// Worker Object that will receive multiple action progress responses from the TurboProbe
// before the final result. Action progress and result are sent to the server as responses for the action request.
// It implements the ActionProgressTracker interface.
type ActionResponseWorker struct {
	msgId              int32
	turboProbe         *probe.TurboProbe
	actionExecutionDto *proto.ActionExecutionDTO
	accountValues      []*proto.AccountValue
	probeMsgChan       chan *proto.MediationClientMessage
}

func NewActionResponseWorker(msgId int32, turboProbe *probe.TurboProbe,
	actionExecutionDto *proto.ActionExecutionDTO, accountValues []*proto.AccountValue,
	probeMsgChan chan *proto.MediationClientMessage) *ActionResponseWorker {
	worker := &ActionResponseWorker{
		msgId:              msgId,
		turboProbe:         turboProbe,
		actionExecutionDto: actionExecutionDto,
		accountValues:      accountValues,
		probeMsgChan:       probeMsgChan,
	}
	glog.V(4).Infof("New ActionResponseProtocolWorker for %s %s %s", msgId, turboProbe,
		actionExecutionDto.ActionType)
	return worker
}

func (actionWorker *ActionResponseWorker) start() {
	var actionResult *proto.ActionResult
	// Execute the action
	actionResult = actionWorker.turboProbe.ExecuteAction(actionWorker.actionExecutionDto, actionWorker.accountValues, actionWorker)
	clientMsg := NewClientMessageBuilder(actionWorker.msgId).SetActionResponse(actionResult).Create()

	// Send the response on the callback channel to send to the server
	actionWorker.probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(3).Infof("Sent action response for %d.", clientMsg.GetMessageID())
}

func (actionWorker *ActionResponseWorker) UpdateProgress(actionState proto.ActionResponseState,
	description string, progress int32) {
	// Build ActionProgress
	actionResponse := &proto.ActionResponse{
		ActionResponseState: &actionState,
		ResponseDescription: &description,
		Progress:            &progress,
	}

	actionProgress := &proto.ActionProgress{
		Response: actionResponse,
	}

	clientMsg := NewClientMessageBuilder(actionWorker.msgId).SetActionProgress(actionProgress).Create()
	// Send the response on the callback channel to send to the server
	actionWorker.probeMsgChan <- clientMsg // This will block till the channel is ready to receive
	glog.V(3).Infof("Sent action progress for %d.", clientMsg.GetMessageID())

}

// -------------------------------- Interrupt Request Handler -----------------------------------
type InterruptMessageHandler struct {
	probes map[string]*ProbeProperties
}

func (intMsgHandler *InterruptMessageHandler) HandleMessage(serverRequest proto.MediationServerMessage,
	probeMsgChan chan *proto.MediationClientMessage) {

	msgID := serverRequest.GetMessageID()
	glog.V(3).Infof("Received: Interrupt Message for message ID: %d, %s\n ", msgID, serverRequest)
}
