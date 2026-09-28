package call_handler

import (
	"errors"
	"net/http"
	"strings"

	call_service "github.com/evolution-foundation/evolution-go/pkg/call/service"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
)

type CallHandler interface {
	RejectCall(ctx *gin.Context)
	AnswerCall(ctx *gin.Context)
	HangupCall(ctx *gin.Context)
	DialCall(ctx *gin.Context)
}

type callHandler struct {
	callService call_service.CallService
}

func writeCallError(ctx *gin.Context, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, call_service.ErrCallNotFound) {
		status = http.StatusNotFound
	}
	ctx.JSON(status, gin.H{"error": err.Error()})
}

func requireCallID(ctx *gin.Context, callID string) bool {
	if strings.TrimSpace(callID) != "" {
		return true
	}
	ctx.JSON(http.StatusBadRequest, gin.H{"error": "callId is required"})
	return false
}

// Reject call
// @Summary Reject call
// @Description Reject call
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.RejectCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/reject [post]
func (g *callHandler) RejectCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.RejectCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if data == nil || !requireCallID(ctx, data.CallID) {
		return
	}

	err = g.callService.RejectCall(data, instance)
	if err != nil {
		writeCallError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// Answer call
// @Summary Answer call
// @Description Answer an incoming audio call
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.AnswerCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/answer [post]
func (g *callHandler) AnswerCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.AnswerCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if data == nil || !requireCallID(ctx, data.CallID) {
		return
	}

	_, err = g.callService.AnswerCall(data, instance)
	if err != nil {
		writeCallError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// Hangup call
// @Summary Hangup call
// @Description Hangup an active call
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.HangupCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/hangup [post]
func (g *callHandler) HangupCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.HangupCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if data == nil || !requireCallID(ctx, data.CallID) {
		return
	}

	err = g.callService.HangupCall(data, instance)
	if err != nil {
		writeCallError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// Dial call
// @Summary Place an outbound call
// @Description Spike/experimental: places an outbound call, not part of the reviewed answer-call plan
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.DialCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/dial [post]
func (g *callHandler) DialCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.DialCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if data == nil || strings.TrimSpace(data.Number) == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "number is required"})
		return
	}

	call, err := g.callService.DialCall(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "callId": call.ID()})
}

func NewCallHandler(
	callService call_service.CallService,
) CallHandler {
	return &callHandler{
		callService: callService,
	}
}
