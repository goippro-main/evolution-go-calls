package call_service

import (
	"context"
	"errors"
	"fmt"

	call_registry "github.com/evolution-foundation/evolution-go/pkg/call/registry"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gomessguii/logger"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type CallService interface {
	RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error
	AnswerCall(data *AnswerCallStruct, instance *instance_model.Instance) (*meowcaller.Call, error)
	HangupCall(data *HangupCallStruct, instance *instance_model.Instance) error
	GetActiveCall(instanceId, callId string) (*meowcaller.Call, error)
	ForgetActiveCall(instanceId, callId string, call *meowcaller.Call)
	IsOutgoingCall(instanceId, callId string) bool
	DialCall(data *DialCallStruct, instance *instance_model.Instance) (*meowcaller.Call, error)
}

var ErrCallNotFound = errors.New("call not found")

type callService struct {
	clientPointer    map[string]*whatsmeow.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	callRegistry     *call_registry.CallRegistry
	loggerWrapper    *logger_wrapper.LoggerManager
}

type RejectCallStruct struct {
	CallCreator types.JID `json:"callCreator"`
	CallID      string    `json:"callId"`
}

type AnswerCallStruct struct {
	CallCreator types.JID `json:"callCreator"`
	CallID      string    `json:"callId"`
}

type HangupCallStruct struct {
	CallID string `json:"callId"`
}

// DialCallStruct is a spike/experimental addition, outside the reviewed
// answer-call plan: it places an OUTBOUND call rather than answering an
// inbound one. Number accepts anything meowcaller.Client.Call accepts (a
// phone number, a phone JID, or an @lid JID).
type DialCallStruct struct {
	Number string `json:"number"`
}

func (c *callService) RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error {
	call, ok := c.callRegistry.Get(instance.Id, data.CallID)
	if !ok {
		return fmt.Errorf("%w: no pending call with that id", ErrCallNotFound)
	}

	if err := call.Reject(); err != nil {
		logger.LogError("[%s] error reject call: %v", instance.Id, err)
		return err
	}
	c.callRegistry.DeleteIf(instance.Id, data.CallID, call)

	return nil
}

func (c *callService) AnswerCall(data *AnswerCallStruct, instance *instance_model.Instance) (*meowcaller.Call, error) {
	call, ok := c.callRegistry.Get(instance.Id, data.CallID)
	if !ok {
		return nil, fmt.Errorf("%w: no pending call with that id", ErrCallNotFound)
	}

	// Answer negotiates the audio offer. Video upgrades are intentionally outside
	// this branch's transport contract.
	if err := call.Answer(); err != nil {
		logger.LogError("[%s] error answering call: %v", instance.Id, err)
		return nil, err
	}

	return call, nil
}

func (c *callService) HangupCall(data *HangupCallStruct, instance *instance_model.Instance) error {
	call, ok := c.callRegistry.Get(instance.Id, data.CallID)
	if !ok {
		return fmt.Errorf("%w: no active call with that id", ErrCallNotFound)
	}

	if err := call.Hangup(); err != nil {
		logger.LogError("[%s] error hanging up call: %v", instance.Id, err)
		return err
	}
	c.callRegistry.DeleteIf(instance.Id, data.CallID, call)
	return nil
}

func (c *callService) GetActiveCall(instanceId, callId string) (*meowcaller.Call, error) {
	call, ok := c.callRegistry.Get(instanceId, callId)
	if !ok {
		return nil, fmt.Errorf("%w: no active call with that id", ErrCallNotFound)
	}
	return call, nil
}

func (c *callService) ForgetActiveCall(instanceID, callID string, call *meowcaller.Call) {
	c.callRegistry.DeleteIf(instanceID, callID, call)
}

func (c *callService) IsOutgoingCall(instanceId, callId string) bool {
	return c.callRegistry.IsOutgoing(instanceId, callId)
}

// DialCall places an outbound call and registers it so /call/stream and
// /call/hangup can act on it just like an answered inbound call.
func (c *callService) DialCall(data *DialCallStruct, instance *instance_model.Instance) (*meowcaller.Call, error) {
	meowcallerClient, err := c.whatsmeowService.GetMeowcallerClient(instance.Id)
	if err != nil {
		return nil, err
	}

	call, err := meowcallerClient.Call(context.Background(), data.Number)
	if err != nil {
		logger.LogError("[%s] error dialing call: %v", instance.Id, err)
		return nil, err
	}

	c.callRegistry.StoreOutgoing(instance.Id, call)

	return call, nil
}

func NewCallService(
	clientPointer map[string]*whatsmeow.Client,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	callRegistry *call_registry.CallRegistry,
	loggerWrapper *logger_wrapper.LoggerManager,
) CallService {
	return &callService{
		clientPointer:    clientPointer,
		whatsmeowService: whatsmeowService,
		callRegistry:     callRegistry,
		loggerWrapper:    loggerWrapper,
	}
}
