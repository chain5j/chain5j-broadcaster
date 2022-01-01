// Package broadcaster
//
// @author: xwc1125
package broadcaster

import (
	"context"
	"github.com/chain5j/chain5j-pkg/event"
	"github.com/chain5j/chain5j-protocol/mock"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/logger"
	"github.com/chain5j/logger/zap"
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

func init() {
	zap.InitWithConfig(&logger.LogConfig{
		Console: logger.ConsoleLogConfig{
			Level:    4,
			Modules:  "*",
			ShowPath: false,
			Format:   "",
			UseColor: true,
			Console:  true,
		},
		File: logger.FileLogConfig{},
	})
}

func TestFactory_NewFactory(t *testing.T) {
	mockCtl := gomock.NewController(nil)
	// config
	mockConfig := mock.NewMockConfig(mockCtl)
	mockConfig.EXPECT().ChainConfig().Return(models.ChainConfig{
		GenesisHeight: 10,
		ChainID:       1,
		ChainName:     "chain5j",
		VersionName:   "v1.0.0",
		VersionCode:   1,
		TxSizeLimit:   128,
		Packer: &models.PackerConfig{
			WorkerType:           models.Timing,
			BlockMaxTxsCapacity:  10000,
			BlockMaxSize:         2048,
			BlockMaxIntervalTime: 1000,
			BlockGasLimit:        5000000,
			Period:               3000,
			EmptyBlocks:          0,
			Timeout:              1000,
			MatchTxsCapacity:     true,
		},
		StateApp: nil,
	}).AnyTimes()
	mockConfig.EXPECT().BlockchainConfig().Return(models.BlockchainLocalConfig{
		Metrics:      true,
		MetricsLevel: 2,
	}).AnyTimes()
	// p2p
	mockP2pService := mock.NewMockP2PService(mockCtl)
	mockP2pService.EXPECT().SubscribeNewPeer(gomock.Any()).Return(
		event.NewSubscription(func(quit <-chan struct{}) error {
			return nil
		}),
	).AnyTimes()
	mockP2pService.EXPECT().SubscribeDropPeer(gomock.Any()).Return(
		event.NewSubscription(func(quit <-chan struct{}) error {
			return nil
		}),
	).AnyTimes()
	mockP2pService.EXPECT().Id().Return(
		models.P2PID("JDq8kTCbdtuNqinHpEzR7caPUTXicVkT7tDCyGHRXWGU"),
	).AnyTimes()

	factory, err := NewBroadcaster(context.Background(),
		WithConfig(mockConfig),
		WithP2PService(mockP2pService))
	if err != nil {
		t.Fatal(err)
	}
	err = factory.Start()
	if err != nil {
		t.Fatal(err)
	}
	for {
		err = factory.Broadcast([]models.P2PID{}, 1, []byte("123456"))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
	}
}
