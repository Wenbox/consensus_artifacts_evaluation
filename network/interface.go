
// SPDX-License-Identifier: Apache-2.0

package network

import (
	"mytumbler-go/common"
)

type NetWork interface {
	Start()
	Stop()
	BroadcastMessage(msg *common.Message)
	SendMessage(id uint32, msg *common.Message)
}
