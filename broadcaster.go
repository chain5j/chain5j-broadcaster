// Package broadcaster
//
// @author: xwc1125
package broadcaster

import (
	"context"
	"github.com/chain5j/chain5j-pkg/event"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	"sync"
)

var (
	_ protocol.Broadcaster = new(broadcaster)
)

// broadcaster 处理与其他节点之间的消息发送，接收、消息广播。
type broadcaster struct {
	log     logger.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	localId models.P2PID // 当前的peerID

	config     protocol.Config
	p2pService protocol.P2PService

	// 节点信息
	trustPeersList []models.P2PID
	trustPeers     *sync.Map // 可信的peers。models.P2PID-->string
	connectedPeers *sync.Map // 已经连接的peer。models.P2PID-->*peer

	// p2p节点变更时，更新connectedPeer
	newPeerSub  event.Subscription
	newPeerCh   chan models.P2PID
	dropPeerSub event.Subscription
	dropPeerCh  chan models.P2PID
}

// NewBroadcaster 创建新的Broadcaster
func NewBroadcaster(rootCtx context.Context, opts ...option) (protocol.Broadcaster, error) {
	ctx, cancel := context.WithCancel(rootCtx)
	b := &broadcaster{
		log:    logger.New("broadcaster"),
		ctx:    ctx,
		cancel: cancel,

		trustPeers:     new(sync.Map),
		connectedPeers: new(sync.Map),
	}
	if err := apply(b, opts...); err != nil {
		b.log.Error("apply is error", "err", err)
		return nil, err
	}
	b.localId = b.p2pService.Id()
	b.newPeerCh = make(chan models.P2PID, 1)
	b.dropPeerCh = make(chan models.P2PID, 1)
	b.newPeerSub = b.p2pService.SubscribeNewPeer(b.newPeerCh)
	b.dropPeerSub = b.p2pService.SubscribeDropPeer(b.dropPeerCh)

	if b.trustPeers != nil {
		for _, peer := range b.trustPeersList {
			b.trustPeers.Store(peer, "")
		}
	}

	return b, nil
}

// Start 启动
func (b *broadcaster) Start() error {
	go b.peerSubLoop()
	return nil
}

// Stop 停止connector服务
func (b *broadcaster) Stop() error {
	b.cancel()
	b.closeAllPeer()
	return nil
}

// peerSubLoop 监控共识节点变化
func (b *broadcaster) peerSubLoop() {
	for {
		select {
		case id := <-b.newPeerCh:
			// 来新的peer
			b.addConnectPeer(id)
		case id := <-b.dropPeerCh:
			// 删除peer
			b.removeConnectPeer(id)
		case <-b.newPeerSub.Err():
			b.log.Error("subscribe new peer error")
			return
		case <-b.dropPeerSub.Err():
			b.log.Error("subscribe drop peer error")
			return
		case <-b.ctx.Done():
			return
		}
	}
}

// addConnectPeer 添加已连接节点信息
func (b *broadcaster) addConnectPeer(id models.P2PID) {
	// 需要peers中有的peerId才会被添加
	if _, ok := b.trustPeers.Load(id); ok {
		if _, exists := b.connectedPeers.Load(id); exists {
			// 已经连接，那么无需再添加
			return
		}
		b.connectedPeers.Store(id, newPeer(b.ctx, id, b.p2pService, b.config))
	}
}

// removeConnectPeer 删除已连接节点信息
func (b *broadcaster) removeConnectPeer(id models.P2PID) {
	// 从已连接的peer中移除
	if p, ok := b.connectedPeers.Load(id); ok {
		p.(*peer).close()
		b.connectedPeers.Delete(id)
	}
}

// closeAllPeer 关闭所有的peer
func (b *broadcaster) closeAllPeer() {
	b.connectedPeers.Range(func(key, value interface{}) bool {
		value.(*peer).close()
		return true
	})
}

// RegisterTrustPeer 注册peerId
func (b *broadcaster) RegisterTrustPeer(id models.P2PID) error {
	// 可信任中已经存在，无需再添加
	if _, ok := b.trustPeers.Load(id); ok {
		return nil
	}
	b.trustPeers.Store(id, "")
	return nil
}

// DeregisterTrustPeer 注销peer
func (b *broadcaster) DeregisterTrustPeer(id models.P2PID) error {
	// 不存在，那么无需操作
	if _, ok := b.trustPeers.Load(id); !ok {
		return nil
	}
	b.trustPeers.Delete(id)
	b.removeConnectPeer(id)

	return nil
}

// Broadcast 广播数据，如果peers 为nil, 则广播给所有节点
func (b *broadcaster) Broadcast(peerIds []models.P2PID, mType uint, payload []byte) error {
	if peerIds == nil {
		b.connectedPeers.Range(func(key, value interface{}) bool {
			value.(*peer).asyncSend(mType, payload)
			return true
		})
	} else {
		for _, id := range peerIds {
			if p, ok := b.connectedPeers.Load(id); ok {
				p.(*peer).asyncSend(mType, payload)
			}
		}
	}

	return nil
}

// SubscribeMsg 根据消息类型订阅消息
func (b *broadcaster) SubscribeMsg(msgType uint, ch chan<- *models.P2PMessage) event.Subscription {
	return b.p2pService.SubscribeMsg(msgType, ch)
}

// SubscribeNewPeer 订阅新的peer接入时的消息
func (b *broadcaster) SubscribeNewPeer(newPeerCh chan models.P2PID) event.Subscription {
	return b.p2pService.SubscribeNewPeer(newPeerCh)
}

// SubscribeDropPeer 订阅peer被drop时的消息
// dropPeerCh := make(chan models.P2PID)
// dropPeerSub := t.broadcaster.SubscribeDropPeer(dropPeerCh)
func (b *broadcaster) SubscribeDropPeer(dropPeerCh chan models.P2PID) event.Subscription {
	return b.p2pService.SubscribeDropPeer(dropPeerCh)
}

func (b *broadcaster) isMetrics(_metricsLevel uint64) bool {
	return b.config.BroadcasterConfig().IsMetrics(_metricsLevel)
}
