// Package broadcaster
//
// @author: xwc1125
package broadcaster

import (
	"fmt"
	"github.com/chain5j/chain5j-protocol/protocol"
)

type option func(f *broadcaster) error

func apply(f *broadcaster, opts ...option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(f); err != nil {
			return fmt.Errorf("option apply err:%v", err)
		}
	}
	return nil
}

func WithP2PService(p2pSrv protocol.P2PService) option {
	return func(f *broadcaster) error {
		f.p2pService = p2pSrv
		return nil
	}
}
func WithConfig(config protocol.Config) option {
	return func(f *broadcaster) error {
		f.config = config
		return nil
	}
}
