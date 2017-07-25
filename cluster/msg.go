package cluster

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/name5566/leaf/chanrpc"
	"github.com/name5566/leaf/conf"
	"github.com/name5566/leaf/log"
	lgob "github.com/name5566/leaf/network/gob"
)

var (
	Processor = lgob.NewProcessor()
)

const (
	callNotForResult = iota
	callForResult
)

func init() {
	gob.Register([]interface{}{})
}

type S2S_NotifyServerName struct {
	ServerName string
}

type S2S_HeartBeat struct {
	ServerName string
}

type S2S_RequestMsg struct {
	RequestID uint32
	MsgID     interface{}
	CallType  uint8
	Args      []interface{}
}

type S2S_ResponseMsg struct {
	RequestID uint32
	Ret       interface{}
	Err       string
}

func handleNotifyServerName(args []interface{}) {
	msg := args[0].(*S2S_NotifyServerName)
	agent := args[1].(*Agent)
	addAgent(msg.ServerName, agent)
}

func handleHeartBeat(args []interface{}) {
	msg := args[0].(*S2S_HeartBeat)
	agent := args[1].(*Agent)
	if v, ok := agent.heartBeatWaitTimes[msg.ServerName]; ok {
		atomic.StoreInt32(&v, 0)
	}
}

func handleRequestMsg(args []interface{}) {
	recvMsg := args[0].(*S2S_RequestMsg)
	agent := args[1].(*Agent)

	sendMsg := &S2S_ResponseMsg{RequestID: recvMsg.RequestID}
	if closing && recvMsg.CallType == callForResult {
		sendMsg.Err = fmt.Sprintf("%v server is closing", conf.ServerName)
		agent.WriteMsg(sendMsg)
		return
	}

	msgID := recvMsg.MsgID
	client, ok := routeMap[msgID]
	if !ok {
		err := fmt.Sprintf("%v msg is not set route", msgID)
		log.Error(err)

		if recvMsg.CallType == callForResult {
			sendMsg.Err = err
			agent.WriteMsg(sendMsg)
		}
		return
	}

	args = recvMsg.Args
	if recvMsg.CallType == callNotForResult {
		args = append(args, nil)
		client.RpcCall(msgID, args...)
	} else {
		sendMsgFunc := func(ret *chanrpc.RetInfo) {
			sendMsg.Ret = ret.Ret
			if ret.Err != nil {
				sendMsg.Err = ret.Err.Error()
			}
			agent.WriteMsg(sendMsg)
		}

		args = append(args, sendMsgFunc)
		client.RpcCall(msgID, args...)
	}
}

func handleResponseMsg(args []interface{}) {
	msg := args[0].(*S2S_ResponseMsg)
	agent := args[1].(*Agent)

	request := agent.popRequest(msg.RequestID)
	if request == nil {
		log.Error("%v: request id %v is not exist", agent.ServerName, msg.RequestID)
		return
	}

	ret := &chanrpc.RetInfo{Ret: msg.Ret, Cb: request.cb}
	if msg.Err != "" {
		ret.Err = errors.New(msg.Err)
	}
	request.chanRet <- ret
}

func init() {
	Processor.Register(&S2S_NotifyServerName{})
	Processor.Register(&S2S_HeartBeat{})
	Processor.Register(&S2S_RequestMsg{})
	Processor.Register(&S2S_ResponseMsg{})

	Processor.SetHandler(&S2S_NotifyServerName{}, handleNotifyServerName)
	Processor.SetHandler(&S2S_HeartBeat{}, handleHeartBeat)
	Processor.SetHandler(&S2S_RequestMsg{}, handleRequestMsg)
	Processor.SetHandler(&S2S_ResponseMsg{}, handleResponseMsg)
}
