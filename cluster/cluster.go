package cluster

import (
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/name5566/leaf/chanrpc"
	"github.com/name5566/leaf/conf"
	"github.com/name5566/leaf/log"
	"github.com/name5566/leaf/network"
	lgob "github.com/name5566/leaf/network/gob"
)

var (
	NeedWaitRequestTimes = 5
)

var (
	closing      bool
	closeSig     = make(chan bool, 1)
	wg           sync.WaitGroup
	server       *network.TCPServer
	clientsMutex sync.Mutex
	clients      = map[string]*network.TCPClient{}
	agentsMutex  sync.RWMutex
	agents       = map[string]*Agent{}
	AgentChanRPC *chanrpc.Server
)

func Init() {
	if conf.ListenAddr != "" {
		server = new(network.TCPServer)
		server.Addr = conf.ListenAddr
		server.MaxConnNum = int(math.MaxInt32)
		server.PendingWriteNum = conf.PendingWriteNum
		server.LenMsgLen = 4
		server.MaxMsgLen = math.MaxUint32
		server.NewAgent = newAgent

		server.Start()
	}

	for serverName, addr := range conf.ConnAddrs {
		AddClient(serverName, addr)
	}

	if conf.HeartBeatInterval <= 0 {
		conf.HeartBeatInterval = 5
		log.Release("invalid HeartBeatInterval, reset to %v", conf.HeartBeatInterval)
	}

	wg.Add(1)
	go run()
}

func run() {
	defer wg.Done()

	msg := &S2S_HeartBeat{ServerName: conf.ServerName}
	timer := time.NewTicker(time.Duration(conf.HeartBeatInterval) * time.Second)

	for {
		select {
		case <-closeSig:
			return
		case <-timer.C:
			agentsMutex.RLock()
			for _, agent := range agents {
				v := agent.heartBeatWaitTimes[conf.ServerName]
				if atomic.AddInt32(&v, 1) >= 2 {
					agent.conn.Destroy()
				} else {
					agent.WriteMsg(msg)
				}
			}
			agentsMutex.RUnlock()
		}
	}
}

func AddClient(serverName, addr string) {
	clientsMutex.Lock()
	defer clientsMutex.Unlock()

	_removeClient(serverName)

	client := new(network.TCPClient)
	client.Addr = addr
	client.ConnNum = 1
	client.ConnectInterval = 3 * time.Second
	client.PendingWriteNum = conf.PendingWriteNum
	client.LenMsgLen = 4
	client.MaxMsgLen = math.MaxUint32
	client.NewAgent = newAgent
	client.AutoReconnect = true

	client.Start()
	clients[serverName] = client
}

func _removeClient(serverName string) {
	client, ok := clients[serverName]
	if ok {
		client.Close()
		delete(clients, serverName)
	}
}

func RemoveClient(serverName string) {
	clientsMutex.Lock()
	defer clientsMutex.Unlock()

	_removeClient(serverName)
}

func addAgent(serverName string, agent *Agent) {
	agentsMutex.Lock()
	defer agentsMutex.Unlock()

	_removeAgent(serverName)

	agent.ServerName = serverName
	agent.heartBeatWaitTimes[conf.ServerName] = 0
	agents[agent.ServerName] = agent
	log.Release("%s server is online >>>[%s]", serverName, conf.ServerName)

	if AgentChanRPC != nil {
		AgentChanRPC.Go("NewServerAgent", serverName, agent)
	}
}

func _removeAgent(serverName string) {
	agent, ok := agents[serverName]
	if ok {
		delete(agents, serverName)
		agent.Destroy()
		log.Release("%s server is offline >>>[%s]", serverName, conf.ServerName)

		if AgentChanRPC != nil {
			AgentChanRPC.Go("CloseServerAgent", serverName, agent)
		}
	}
}

func removeAgent(serverName string) {
	agentsMutex.Lock()
	defer agentsMutex.Unlock()

	_removeAgent(serverName)
}

func Destroy() {
	closing = true
	waitRequestTimes := 0
	for {
		time.Sleep(time.Second)

		requestCount := GetRequestCount()
		if requestCount == 0 {
			waitRequestTimes += 1
			if waitRequestTimes >= NeedWaitRequestTimes {
				break
			} else {
				log.Release("wait request count down %v", NeedWaitRequestTimes-waitRequestTimes)
			}
		} else {
			waitRequestTimes = 0
			log.Release("has %v request", requestCount)
		}
	}

	closeSig <- true
	wg.Wait()

	if server != nil {
		server.Close()
	}

	clientsMutex.Lock()
	for _, client := range clients {
		client.Close()
	}
	clientsMutex.Unlock()
}

type Agent struct {
	ServerName         string
	conn               *network.TCPConn
	userData           interface{}
	heartBeatWaitTimes map[string]int32

	encMutex sync.Mutex
	encoder  *lgob.Encoder
	decoder  *lgob.Decoder

	sync.Mutex
	requestID  uint32
	requestMap map[uint32]*RequestInfo
}

func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn
	a.requestMap = make(map[uint32]*RequestInfo)
	a.heartBeatWaitTimes = make(map[string]int32)

	a.encoder = lgob.NewEncoder()
	a.decoder = lgob.NewDecoder()

	msg := &S2S_NotifyServerName{ServerName: conf.ServerName}
	a.WriteMsg(msg)
	return a
}

func (a *Agent) GetRequestCount() int {
	a.Lock()
	defer a.Unlock()
	return len(a.requestMap)
}

func (a *Agent) registerRequest(request *RequestInfo) uint32 {
	a.Lock()
	defer a.Unlock()

	reqID := a.requestID
	a.requestMap[reqID] = request
	a.requestID += 1
	return reqID
}

func (a *Agent) popRequest(requestID uint32) *RequestInfo {
	a.Lock()
	defer a.Unlock()

	request, ok := a.requestMap[requestID]
	if ok {
		delete(a.requestMap, requestID)
		return request
	} else {
		return nil
	}
}

func (a *Agent) clearRequest(err error) {
	a.Lock()
	defer a.Unlock()

	for _, request := range a.requestMap {
		ret := &chanrpc.RetInfo{Err: err, Cb: request.cb}
		request.chanRet <- ret
	}
	a.requestMap = make(map[uint32]*RequestInfo)
}

func (a *Agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			if err.Error() != "EOF" && strings.ContainsAny(err.Error(), "use of closed network connection") == false {
				log.Debug("read message: %v", err)
			}
			break
		}

		if Processor != nil {
			msg, err := Processor.Unmarshal(a.decoder, data)
			if err != nil {
				log.Debug("unmarshal message error: %v", err)
				break
			}
			err = Processor.Route(msg, a)
			if err != nil {
				log.Debug("route message error: %v", err)
				break
			}
		}
	}
}

func (a *Agent) OnClose() {
	removeAgent(a.ServerName)
	a.clearRequest(fmt.Errorf("%v server is offline", a.ServerName))
}

func (a *Agent) WriteMsg(msg interface{}) {
	if Processor != nil {
		a.encMutex.Lock()
		data, err := Processor.Marshal(a.encoder, msg)
		a.encMutex.Unlock()
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		err = a.conn.WriteMsg(data...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

func (a *Agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *Agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *Agent) Close() {
	a.conn.Close()
}

func (a *Agent) Destroy() {
	a.conn.Destroy()
}

func (a *Agent) UserData() interface{} {
	return a.userData
}

func (a *Agent) SetUserData(data interface{}) {
	a.userData = data
}

func (a *Agent) Go(id interface{}, args ...interface{}) {
	msg := &S2S_RequestMsg{MsgID: id, CallType: callNotForResult, Args: args}
	a.WriteMsg(msg)
}

func (a *Agent) Call0(id interface{}, args ...interface{}) error {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := a.registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return ri.Err
}

func (a *Agent) Call1(id interface{}, args ...interface{}) (interface{}, error) {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := a.registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return ri.Ret, ri.Err
}

func (a *Agent) CallN(id interface{}, args ...interface{}) ([]interface{}, error) {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := a.registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return chanrpc.Assert(ri.Ret), ri.Err
}

func (a *Agent) AsynCall(chanAsynRet chan *chanrpc.RetInfo, id interface{}, args ...interface{}) {
	if len(args) < 1 {
		panic(fmt.Sprintf("%v asyn call of callback function not found", id))
	}

	lastIndex := len(args) - 1
	cb := args[lastIndex]
	args = args[:lastIndex]

	var callType uint8
	switch cb.(type) {
	case func(error):
		callType = callForResult
	case func(interface{}, error):
		callType = callForResult
	case func([]interface{}, error):
		callType = callForResult
	default:
		panic(fmt.Sprintf("%v asyn call definition of callback function is invalid", id))
	}

	request := &RequestInfo{cb: cb, chanRet: chanAsynRet}
	requestID := a.registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callType, Args: args}
	a.WriteMsg(msg)
}
