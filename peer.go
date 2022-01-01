// Package broadcaster
//
// @author: xwc1125
package broadcaster

import (
	"context"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
)

const (
	p2pMsgSendQueue = 32 // 每个节点的发送队列
)

// peer 单个peer
type peer struct {
	log    logger.Logger
	ctx    context.Context
	cancel context.CancelFunc

	peerId     models.P2PID
	p2pService protocol.P2PService
	config     protocol.Config

	queuedMsg chan *models.P2PMessage
	txManager *txManager
}

func newPeer(rootCtx context.Context, peerId models.P2PID, p2pService protocol.P2PService, config protocol.Config) *peer {
	ctx, cancel := context.WithCancel(rootCtx)
	peer := &peer{
		log:        logger.New("broadcaster"),
		ctx:        ctx,
		cancel:     cancel,
		peerId:     peerId,
		p2pService: p2pService,

		queuedMsg: make(chan *models.P2PMessage, p2pMsgSendQueue),
		txManager: newTxManager(peerId, p2pService, config),
	}

	go peer.broadcast()

	return peer
}

// broadcast 循环广播p2p消息
func (p *peer) broadcast() {
	for {
		select {
		case msg := <-p.queuedMsg:
			if err := p.p2pService.Send(p.peerId, msg); err != nil {
				p.log.Error("consensus peer broadcast msg", "error", err)
				return
			}
		case txs := <-p.txManager.queuedTxs:
			// 队列中添加了交易就会触发发送交易
			if err := p.txManager.SendTransactions(txs); err != nil {
				p.log.Error("sendTransactions is error", "err", err)
				break
			}
			if p.isMetrics(3) {
				p.log.Trace("Broadcast transactions", "count", txs.Len())
			}
		case <-p.ctx.Done():
			return
		}
	}
}

// close 关闭当前节点通信
func (p *peer) close() {
	p.cancel()
}

func (p *peer) asyncSend(mType uint, payload []byte) {
	msg := &models.P2PMessage{
		Type: mType,
		Data: payload,
	}

	select {
	case p.queuedMsg <- msg:
		return
	default:
		if p.isMetrics(3) {
			p.log.Trace("consensus peer dropping msg", "peerId", p.peerId)
		}
	}
}

func (p *peer) isMetrics(_metricsLevel uint64) bool {
	return p.config.BroadcasterConfig().IsMetrics(_metricsLevel)
}
