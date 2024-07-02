
// SPDX-License-Identifier: Apache-2.0

package common

type ConfigFile struct {
	Cfg   Config `json:"config"`
	Peers []Peer `json:"peers"`
}

type Config struct {
	ID           uint32 `json:"id"`
	N            uint32 `json:"n"`
	F            uint32 `json:"f"`
	PubKey       []byte `json:"pk"`
	PrivKey      []byte `json:"sk"`
	MasterPK     []byte `json:"master_pk"`
	ThresholdSK  []byte `json:"threshold_sk"`
	ThresholdPK  []byte `json:"threshold_pk"`
	Addr         string `json:"address"`
	RpcServer    string `json:"rpc_server"`
	ClientServer string `json:"client_server"`
	MaxBatchSize int    `json:"max_batch_size"`
	PayloadSize  int    `json:"payload_size"`
	MaxWaitTime  int    `json:"max_wait_time"`
	Coordinator  string `json:"coordinator"`
	Time         int    `json:"test_time"`
	Byzantine    bool   `json:"byzantine"`
	MaxDrift     int    `json:max_drift`
}

type Devp struct {
	PrivateIp string `json:"private"`
	PublicIp  string `json:"public"`
}

type CoorStart struct {
	Batch    int
	Payload  int
	Interval int
}

type NodeBack struct {
	StartID     uint32
	ReqNum      uint32
	SupermaTime uint64
	Addr        string
	NodeID      uint32
	Zero        uint32
}

type BlockInfo struct {
	StartID int32
	ReqNum  int32
}

type CoorStatistics struct {
	ConsensusLatency uint64
	ExecutionLatency uint64
	ConsensusNumber  uint64
	ExecutionNumber  uint64
	ID               uint32
	Zero             uint64
	LatencyMap       []uint64
}

type PayloadId struct {
	Id      uint32
	Payload []byte
}

type PayloadIds []PayloadId

func (p PayloadIds) Len() int {
	return len(p)
}
func (p PayloadIds) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p PayloadIds) Less(i, j int) bool {
	return p[i].Id < p[j].Id
}

func (m *Message) Less(other interface{}) bool {
	return m.Sequence < other.(*Message).Sequence
}

type Response struct {
}

type Peer struct {
	ID              uint32 `json:"id"`
	Addr            string `json:"addr"`
	PublicKey       []byte `json:"pk"`
	ThresholdPubKey []byte `json:"threshold_pk"`
}

type SuperMAKey struct {
	Sender    uint32
	Timestamp uint64
}

type SuperMAResult struct {
	Key  SuperMAKey
	Hash string
}

func SuperMAKeyCmp(k1 interface{}, k2 interface{}) int {
	if k1.(*SuperMAKey).Timestamp > k2.(*SuperMAKey).Timestamp {
		return 1
	}
	if k1.(*SuperMAKey).Timestamp < k2.(*SuperMAKey).Timestamp {
		return -1
	}
	if k1.(*SuperMAKey).Sender < k2.(*SuperMAKey).Sender {
		return -1
	}
	if k1.(*SuperMAKey).Sender > k2.(*SuperMAKey).Sender {
		return 1
	}
	return 0
}

func (key SuperMAKey) Less(other interface{}) bool {
	if key.Timestamp == other.(SuperMAKey).Timestamp {
		return key.Sender < other.(SuperMAKey).Sender
	}
	return key.Timestamp < other.(SuperMAKey).Timestamp
}
