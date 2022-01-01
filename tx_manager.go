// Package broadcaster
//
// @author: xwc1125
package broadcaster

import (
	"github.com/chain5j/chain5j-pkg/codec"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/hexutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	mapset "github.com/deckarep/golang-set"
)

const (
	maxKnownTxs  = 32768 // 保存最大的可知交易 (prevent DOS)
	maxQueuedTxs = 128   // 最大的交易发送队列
)

type txManager struct {
	log logger.Logger

	id         models.P2PID
	config     protocol.Config
	p2pService protocol.P2PService

	knownTxs  mapset.Set               // 节点已经拥有的tx
	queuedTxs chan models.Transactions // 发送交易的队列
}

func newTxManager(id models.P2PID, p2pService protocol.P2PService, config protocol.Config) *txManager {
	return &txManager{
		log:        logger.New("broadcaster"),
		id:         id,
		p2pService: p2pService,
		config:     config,
		knownTxs:   mapset.NewSet(),
		queuedTxs:  make(chan models.Transactions, maxQueuedTxs),
	}
}

// MarkTransaction 标记Hash为已知。如果队列超过最大值，那么将旧数据清理
func (tm *txManager) MarkTransaction(hash types.Hash) {
	// 如果已知的交易已经超过了最大上限，那么将旧数据清除一个
	for tm.knownTxs.Cardinality() >= maxKnownTxs {
		tm.knownTxs.Pop()
	}
	tm.knownTxs.Add(hash)
}

// AsyncSendTransactions 异步发送
func (tm *txManager) AsyncSendTransactions(txs models.Transactions) {
	select {
	// 将交易放进队列中，并且标记交易为已发送
	case tm.queuedTxs <- txs:
		for _, txList := range txs.Data() {
			for _, tx := range txList {
				tm.MarkTransaction(tx.Hash())
			}
		}
	default:
		if tm.isMetrics(3) {
			tm.log.Trace("mark transaction propagation", "count", txs.Len())
		}
	}
}

// SendTransactions 发送交易
func (tm *txManager) SendTransactions(txs models.Transactions) error {
	// 自己发送出去的，默认节点已经知道
	for _, txList := range txs.Data() {
		for _, tx := range txList {
			tm.knownTxs.Add(tx.Hash())
		}
	}
	if tm.isMetrics(3) {
		tm.log.Trace("sendTransactions", "txs", txs.Hashes())
	}

	toBytes, err := codec.Coder().Encode(txs)
	if err != nil {
		return err
	}
	if tm.isMetrics(3) {
		tm.log.Debug("sendTransactions", "data", hexutil.Encode(toBytes))
	}

	if err = tm.p2pService.Send(tm.id, &models.P2PMessage{
		Type: models.TransactionSend,
		Peer: "", // 发是自己的，接收时，是对方
		Data: toBytes,
	}); err != nil {
		tm.log.Error("txManager send p2p txs err", "err", err)
		return err
	}
	return err
}

func (tm *txManager) isMetrics(_metricsLevel uint64) bool {
	return tm.config.BroadcasterConfig().IsMetrics(_metricsLevel)
}

// PeersWithoutTx 返回peer集合
// peerId 数据来源的peerId
// isForce true不管来源peerId是否包含都需要进行广播
func (b *broadcaster) PeersWithoutTx(peerId *models.P2PID, hash types.Hash, isForce bool) []*peer {
	list := make([]*peer, 0)
	b.connectedPeers.Range(func(key, value interface{}) bool {
		p := value.(*peer)
		if !isForce {
			if peerId != nil && p.peerId == *peerId {
				// 来源ID等于当前的peerId，那么代表已经知道
				p.txManager.MarkTransaction(hash)
			} else {
				// 如果peer不含hash，代表需要进行p2p的节点
				if !p.txManager.knownTxs.Contains(hash) {
					list = append(list, p)
				}
			}
		} else {
			if !p.txManager.knownTxs.Contains(hash) {
				list = append(list, p)
			}
		}
		return true
	})
	return list
}

// BroadcastTxs 广播交易集合
// peerId 数据来源的peerId
// isForce true不管来源peerId是否包含都需要进行广播
func (b *broadcaster) BroadcastTxs(peerId *models.P2PID, txs []models.Transaction, isForce bool) {
	if txs == nil {
		return
	}
	var txSet = make(map[*peer]models.Transactions)

	// 给未知的peer发送tx
	for _, tx := range txs {
		peers := b.PeersWithoutTx(peerId, tx.Hash(), isForce)
		for _, peer := range peers {
			if _, ok := txSet[peer]; !ok {
				txSet[peer] = make([]models.TransactionSortedList, len(txs))
			}

			txSet[peer].Add(tx)
			if b.isMetrics(3) {
				b.log.Trace("broadcast txs", "txType", tx.TxType(), "hash", tx.Hash())
			}
		}
		if b.isMetrics(3) {
			b.log.Trace("broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
		}
	}
	for peer, txs := range txSet {
		peer.txManager.AsyncSendTransactions(txs)
	}
}
