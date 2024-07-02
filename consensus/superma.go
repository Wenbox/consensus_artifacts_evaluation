
// SPDX-License-Identifier: Apache-2.0

package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"mytumbler-go/common"
	"mytumbler-go/crypto"
	"mytumbler-go/logger"
	"mytumbler-go/network"
	"strconv"
	"strings"
)

type SuperMAState uint8

const (
	WAIT_BVAL SuperMAState = iota
	WAIT_PROM_OR_AUX
	WAIT_CONF
	WAIT_COIN
	CAN_EXIT
)

type SuperMA struct {
	network           network.NetWork
	cfg               *common.Config
	peers             map[uint32]common.Peer
	stop              bool
	state             SuperMAState
	sender            uint32
	decide            string
	alreadyDecided    bool
	timestamp         uint64
	round             uint32
	bv                map[uint32]map[string]struct{}
	havePromised      map[uint32]string
	haveBvaled        map[uint32]map[string]struct{}
	haveAuxed         map[uint32]string
	bvals             map[uint32]map[string]map[uint32][]byte
	auxs              map[uint32]map[string]map[uint32][]byte
	proms             map[uint32]map[string]map[uint32][]byte
	confs             map[uint32]map[string]map[uint32]struct{}
	confVals          map[uint32]map[string]struct{}
	coinDatas         map[uint32][]byte
	coinSigs          map[uint32]map[uint32][]byte
	coins             map[uint32]bool
	msgChan           chan *common.Message
	promChan          chan *common.Message
	promQcChan        chan *common.Message
	supermaFinishChan chan *common.SuperMAResult
	voteBvalChan      chan *common.Message
	logger            logger.Logger
}

func NewSuperMA(sender uint32, ts uint64, peers map[uint32]common.Peer, network network.NetWork,
	cfg *common.Config, supermaFinishCh chan *common.SuperMAResult,
	msgCh chan *common.Message, promChan chan *common.Message, promQcChan chan *common.Message,
	voteBvalCh chan *common.Message,
	logger logger.Logger) *SuperMA {
	ba := &SuperMA{
		network:           network,
		cfg:               cfg,
		peers:             peers,
		stop:              false,
		state:             WAIT_BVAL,
		sender:            sender,
		decide:            "",
		alreadyDecided:    false,
		timestamp:         ts,
		round:             0,
		bv:                make(map[uint32]map[string]struct{}),
		havePromised:      make(map[uint32]string),
		haveBvaled:        make(map[uint32]map[string]struct{}),
		haveAuxed:         make(map[uint32]string),
		bvals:             make(map[uint32]map[string]map[uint32][]byte),
		auxs:              make(map[uint32]map[string]map[uint32][]byte),
		proms:             make(map[uint32]map[string]map[uint32][]byte),
		confs:             make(map[uint32]map[string]map[uint32]struct{}),
		confVals:          make(map[uint32]map[string]struct{}),
		coinSigs:          make(map[uint32]map[uint32][]byte),
		coins:             make(map[uint32]bool),
		coinDatas:         make(map[uint32][]byte),
		msgChan:           msgCh,
		promChan:          promChan,
		promQcChan:        promQcChan,
		supermaFinishChan: supermaFinishCh,
		voteBvalChan:      voteBvalCh,
		logger:            logger,
	}
	return ba
}

func (ba *SuperMA) Run() {
	for {
		if ba.stop {
			return
		}
		select {
		case msg := <-ba.promQcChan:
			ba.handleMessage(msg)
		default:
			select {
			case msg := <-ba.promChan:
				ba.handleMessage(msg)
			case msg := <-ba.promQcChan:
				ba.handleMessage(msg)
			default:
				select {
				case msg := <-ba.promChan:
					ba.handleMessage(msg)
				case msg := <-ba.promQcChan:
					ba.handleMessage(msg)
				case msg := <-ba.msgChan:
					ba.handleMessage(msg)
				}
			}
		}
	}
}

func (ba *SuperMA) handleMessage(msg *common.Message) {
	if msg.Round < ba.round && msg.Type != common.Message_PROM {
		return
	}
	switch msg.Type {
	case common.Message_VAL:
		ba.onReceiveVal(msg)
	case common.Message_BVAL:
		ba.onReceiveBval(msg)
	case common.Message_PROM:
		ba.onReceiveProm(msg)
	case common.Message_AUX:
		ba.onReceiveAux(msg)
	case common.Message_CONF:
		ba.onReceiveConf(msg)
	case common.Message_COIN:
		ba.onReceiveCoin(msg)
	case common.Message_PROMQC:
		ba.onReceivePromQC(msg)
	case common.Message_VOTEZERO:
		ba.mayVoteZero()
	default:
		ba.logger.Error("invalid msg type in ba", errors.New("bug in ba msg dispatch"))
	}
}

func (ba *SuperMA) verifySignature(msg *common.Message, msg_type common.Message_MsgType) bool {
	tmp := &common.Message{
		Sequence:  msg.Sequence,
		Round:     msg.Round,
		Sender:    msg.Sender,
		Type:      msg_type,
		Timestamp: msg.Timestamp,
		Hash:      msg.Hash,
	}
	data, err := tmp.Marshal()
	if err != nil {
		ba.logger.Error("marshal msg in network recv failed", err)
		return false
	}
	if !crypto.Verify(ba.peers[msg.From].PublicKey, data, msg.Signature) {
		ba.logger.Warnf("invalid %v signature in network recv", msg.Type)
		return false
	}
	return true
}

func (ba *SuperMA) onReceiveVal(msg *common.Message) {
	if msg.From == ba.cfg.ID {
		return
	}
	if _, ok := ba.bvals[msg.Round][msg.Hash][msg.From]; ok {
		return
	}
	if ba.round > 0 || ba.state != WAIT_BVAL {
		return
	}
	if !ba.verifySignature(msg, common.Message_BVAL) {
		return
	}
	ba.addBval(msg.Round, msg.From, msg.Hash, msg.Signature)
}

func (ba *SuperMA) onReceiveBval(msg *common.Message) {
	if ba.state != WAIT_BVAL && ba.round == msg.Round {
		return
	}

	if _, ok := ba.bvals[msg.Round][msg.Hash][msg.From]; ok {
		return
	}

	if !ba.verifySignature(msg, common.Message_BVAL) {
		return
	}

	ba.addBval(msg.Round, msg.From, msg.Hash, msg.Signature)

	if ba.state != WAIT_BVAL {
		return
	}

	if ba.round == 0 && uint32(len(ba.bvals[0][msg.Hash])) == ba.cfg.F+1 {
		if ba.haveBvaled[0] != nil && len(ba.haveBvaled[0]) > 0 {
			return
		} else {
			_, ok1 := ba.havePromised[0]
			_, ok2 := ba.haveAuxed[0]
			if ok1 || ok2 {
				return
			}
		}
		vote := &common.Message{
			Round:     0,
			Sender:    ba.sender,
			Type:      common.Message_BVAL,
			Sequence:  0,
			Timestamp: ba.timestamp,
			Hash:      msg.Hash,
		}
		if msg.Hash == "0" {
			ba.addBval(0, ba.cfg.ID, msg.Hash, ba.Broadcast(vote))
		} else {
			ba.voteBvalChan <- vote
		}
	}

	// only amplify when round > 0
	if ba.round > 0 && uint32(len(ba.bvals[ba.round][msg.Hash])) >= ba.cfg.F+1 {
		if _, ok := ba.havePromised[ba.round]; !ok {
			if _, ok1 := ba.haveBvaled[ba.round][msg.Hash]; !ok1 {
				vote := &common.Message{
					Round:     ba.round,
					Sender:    ba.sender,
					Type:      common.Message_BVAL,
					Sequence:  0,
					Timestamp: ba.timestamp,
					Hash:      msg.Hash,
				}
				ba.addBval(ba.round, ba.cfg.ID, msg.Hash, ba.Broadcast(vote))
			}
		}
	}
}

func (ba *SuperMA) onReceiveProm(msg *common.Message) {
	if !ba.verifySignature(msg, common.Message_PROM) {
		return
	}

	if _, ok := ba.bv[msg.Round][msg.Hash]; !ok {
		qc := new(common.QC)
		err := qc.Unmarshal(msg.Payload)
		if err != nil {
			ba.logger.Error("unmarshal bval qc in prom failed", err)
			return
		}
		ba.onReceiveBvalQC(msg, qc)
	}
	ba.addProm(msg.Round, msg.From, msg.Hash, msg.Signature)
}

func (ba *SuperMA) onReceiveAux(msg *common.Message) {
	qc := new(common.QC)
	err := qc.Unmarshal(msg.Payload)
	if err != nil {
		ba.logger.Error("unmarshal bval qc in aux failed", err)
		return
	}
	ba.onReceiveBvalQC(msg, qc)
	ba.addAux(msg.Round, msg.From, qc.Hash, msg.Signature)
}

func (ba *SuperMA) onReceiveBvalQC(msg *common.Message, qc *common.QC) {
	if _, ok := ba.bv[msg.Round][qc.Hash]; ok {
		return
	}
	tmp := &common.Message{
		Round:     msg.Round,
		Sender:    msg.Sender,
		Type:      common.Message_BVAL,
		Sequence:  0,
		Timestamp: msg.Timestamp,
		Hash:      qc.Hash,
	}
	data, err := tmp.Marshal()
	if err != nil {
		ba.logger.Error("marshal msg in bval qc failed", err)
		return
	}
	for _, sig := range qc.Sigs {
		if _, ok := ba.bvals[msg.Round][msg.Hash][sig.Id]; ok {
			continue
		}
		if !crypto.Verify(ba.peers[sig.Id].PublicKey, data, sig.Sig) {
			ba.logger.Error("invalid bval signature in bval qc", err)
			return
		}
		ba.addBval(msg.Round, sig.Id, qc.Hash, sig.Sig)
	}
}

func (ba *SuperMA) onReceiveConf(msg *common.Message) {
	if ba.confs[msg.Round] == nil {
		ba.confs[msg.Round] = make(map[string]map[uint32]struct{})
	}
	if ba.confs[msg.Round][msg.Hash] == nil {
		ba.confs[msg.Round][msg.Hash] = make(map[uint32]struct{})
	}
	ba.confs[msg.Round][msg.Hash][msg.From] = struct{}{}
	if ba.round != msg.Round {
		return
	}
	sum := 0
	res := make(map[string]struct{})

	for k, v := range ba.confs[msg.Round] {
		vals := strings.Split(k, ",")
		flag := true
		for _, hash := range vals {
			if _, ok := ba.bv[msg.Round][hash]; !ok {
				flag = false
				break
			}
			res[hash] = struct{}{}
		}
		if flag {
			sum += len(v)
		}
	}
	if uint32(sum) >= ba.cfg.N-ba.cfg.F {
		ba.confVals[ba.round] = res
		if ba.state == WAIT_CONF {
			ba.onQuorumConf(msg.Timestamp, msg.Sender, msg.Round)
		}
	}
}

func (ba *SuperMA) onReceiveCoin(msg *common.Message) {
	if ba.coinSigs[msg.Round] == nil {
		ba.coinSigs[msg.Round] = make(map[uint32][]byte)
	}
	ba.coinSigs[msg.Round][msg.From] = msg.Payload
	if ba.state == WAIT_COIN && uint32(len(ba.coinSigs[msg.Round])) >= ba.cfg.N-ba.cfg.F {
		ba.onQuorumCoin()
	}
}

func (ba *SuperMA) onReceivePromQC(msg *common.Message) {
	qc := new(common.QC)
	err := qc.Unmarshal(msg.Payload)
	if err != nil {
		ba.logger.Error("unmarshal prom failed", err)
		return
	}

	tmp := &common.Message{
		Round:     msg.Round,
		Sender:    msg.Sender,
		Type:      common.Message_PROM,
		Sequence:  0,
		Timestamp: msg.Timestamp,
		Hash:      qc.Hash,
	}
	data, err := tmp.Marshal()
	if err != nil {
		ba.logger.Error("marshal msg in promqc failed", err)
		return
	}
	for _, sig := range qc.Sigs {
		if _, ok := ba.proms[msg.Round][qc.Hash][sig.Id]; ok {
			continue
		}
		if !crypto.Verify(ba.peers[sig.Id].PublicKey, data, sig.Sig) {
			ba.logger.Error("invalid prom signature in prom qc", err)
			return
		}
	}
	ba.stop = true
	ba.state = CAN_EXIT
	msg.From = ba.cfg.ID
	ba.network.BroadcastMessage(msg)
	// directly decide if receive n-f valid prom
	ba.Decide(msg.Hash)
}

func (ba *SuperMA) mayVoteZero() {
	if ba.round != 0 {
		return
	}
	if _, ok := ba.havePromised[0]; ok {
		return
	}
	if _, ok := ba.haveAuxed[0]; ok {
		return
	}
	if _, ok := ba.haveBvaled[0]["0"]; ok {
		return
	}

	vote := &common.Message{
		Round:     0,
		Sender:    ba.sender,
		Type:      common.Message_BVAL,
		Sequence:  0,
		Timestamp: ba.timestamp,
		Hash:      "0",
	}
	ba.addBval(0, ba.cfg.ID, "0", ba.Broadcast(vote))
}

func (ba *SuperMA) onQuorumBval(hash string) {
	ba.state = WAIT_PROM_OR_AUX
	payload := ba.getBvalQC(hash)
	msg := &common.Message{
		Sequence:  0,
		Round:     ba.round,
		Sender:    ba.sender,
		Timestamp: ba.timestamp,
		Hash:      hash,
	}
	flag := false
	if ba.haveBvaled[ba.round] == nil {
		flag = true
	} else {
		if len(ba.haveBvaled[ba.round]) == 1 {
			if _, ok := ba.haveBvaled[ba.round][hash]; ok {
				flag = true
			}
		}
	}
	if flag {
		msg.Type = common.Message_PROM
		ba.addProm(ba.round, ba.cfg.ID, hash, ba.BroadcastWithPayload(msg, payload))
	} else {
		msg.Type = common.Message_AUX
		ba.addAux(ba.round, ba.cfg.ID, hash, ba.BroadcastWithPayload(msg, payload))
	}
}

func (ba *SuperMA) onQuorumAux(vals []string) {
	ba.state = WAIT_CONF
	msg := &common.Message{
		Sequence:  0,
		Round:     ba.round,
		Sender:    ba.sender,
		Timestamp: ba.timestamp,
		Type:      common.Message_CONF,
		Hash:      strings.Join(vals, ","),
	}
	ba.Broadcast(msg)
	ba.onReceiveConf(msg)
}

func (ba *SuperMA) onQuorumConf(ts uint64, sender uint32, round uint32) {
	ba.state = WAIT_COIN
	data := ba.getCoinData(ts, sender, round)
	sigShare := crypto.BlsSign(data, ba.cfg.ThresholdSK)
	msg := &common.Message{
		Sequence:  0,
		Round:     ba.round,
		Sender:    ba.sender,
		Timestamp: ba.timestamp,
		Type:      common.Message_COIN,
		Payload:   sigShare,
	}
	ba.Broadcast(msg)
	ba.onReceiveCoin(msg)
}

func (ba *SuperMA) onQuorumCoin() {
	// normal path
	coinBytes := crypto.Recover(ba.coinSigs[ba.round])
	ba.coins[ba.round] = binary.LittleEndian.Uint32(coinBytes)%2 == 0

	var hash string
	if len(ba.confVals[ba.round]) == 1 {
		for v := range ba.confVals[ba.round] {
			hash = v
		}
		if ba.coins[ba.round] && hash != "0" {
			ba.Decide(hash)
		}
		if !ba.coins[ba.round] && hash == "0" {
			ba.Decide("0")
		}
	} else {
		if len(ba.confVals[ba.round]) > 2 {
			ba.logger.Warnf("vals in confVals: %v", ba.confVals[ba.round])
			return
		}
		for v := range ba.confVals[ba.round] {
			if (v != "0" && ba.coins[ba.round]) || (v == "0" && !ba.coins[ba.round]) {
				hash = v
				break
			}
		}
	}

	if ba.stop {
		return
	}

	ba.round++
	msg := &common.Message{
		Sequence:  0,
		Round:     ba.round,
		Sender:    ba.sender,
		Timestamp: ba.timestamp,
		Type:      common.Message_BVAL,
		Hash:      hash,
	}
	ba.state = WAIT_BVAL
	ba.addBval(ba.round, ba.cfg.ID, hash, ba.Broadcast(msg))
}

func (ba *SuperMA) getBvalQC(hash string) []byte {
	qc := &common.QC{
		Hash: hash,
		Sigs: make([]*common.Signature, ba.cfg.N-ba.cfg.F),
	}

	i := uint32(0)
	for id, sig := range ba.bvals[ba.round][hash] {
		qc.Sigs[i] = new(common.Signature)
		qc.Sigs[i].Id = id
		qc.Sigs[i].Sig = sig
		i++
		if i == ba.cfg.N-ba.cfg.F {
			break
		}
	}

	qcBytes, err := qc.Marshal()
	if err != nil {
		ba.logger.Error("marshal qc failed", err)
		return nil
	}
	return qcBytes
}

func (ba *SuperMA) getCoinData(ts uint64, sender uint32, round uint32) []byte {
	if _, ok := ba.coinDatas[round]; ok {
		return ba.coinDatas[round]
	}
	var buffer bytes.Buffer
	buffer.WriteString(strconv.FormatUint(ts, 10))
	buffer.WriteString("-")
	buffer.WriteString(strconv.FormatUint(uint64(sender), 10))
	buffer.WriteString("-")
	buffer.WriteString(strconv.FormatUint(uint64(round), 10))
	ba.coinDatas[round] = buffer.Bytes()
	return ba.coinDatas[round]
}

func (ba *SuperMA) Decide(hash string) {
	if !ba.alreadyDecided {
		ba.decide = hash
		res := &common.SuperMAResult{
			Key: common.SuperMAKey{
				Sender:    ba.sender,
				Timestamp: ba.timestamp,
			},
			Hash: hash,
		}
		ba.supermaFinishChan <- res
	}
	if ba.alreadyDecided && ((ba.decide != "0" && ba.coins[ba.round]) || (ba.decide == "0" && !ba.coins[ba.round])) {
		ba.state = CAN_EXIT
		ba.stop = true
		return
	}
	ba.alreadyDecided = true
}

func (ba *SuperMA) Broadcast(msg *common.Message) []byte {
	msgBytes, err := msg.Marshal()
	if err != nil {
		ba.logger.Error("marshal msg failed in superma", err)
		return nil
	}
	msg.Signature = crypto.Sign(ba.cfg.PrivKey, msgBytes)
	msg.From = ba.cfg.ID
	ba.network.BroadcastMessage(msg)
	return msg.Signature
}

func (ba *SuperMA) BroadcastWithPayload(msg *common.Message, payload []byte) []byte {
	msgBytes, err := msg.Marshal()
	if err != nil {
		ba.logger.Error("marshal msg failed in superma", err)
		return nil
	}
	msg.Signature = crypto.Sign(ba.cfg.PrivKey, msgBytes)
	msg.From = ba.cfg.ID
	msg.Payload = payload
	ba.network.BroadcastMessage(msg)
	return msg.Signature
}

func (ba *SuperMA) addBval(round uint32, from uint32, hash string, signature []byte) {
	if ba.bvals[round] == nil {
		ba.bvals[round] = make(map[string]map[uint32][]byte)
	}
	if ba.bvals[round][hash] == nil {
		ba.bvals[round][hash] = make(map[uint32][]byte)
	}
	ba.bvals[round][hash][from] = signature

	if from == ba.cfg.ID {
		if ba.haveBvaled[round] == nil {
			ba.haveBvaled[round] = make(map[string]struct{})
		}
		ba.haveBvaled[round][hash] = struct{}{}
	}

	if uint32(len(ba.bvals[round][hash])) >= ba.cfg.N-ba.cfg.F {
		if ba.bv[round] == nil {
			ba.bv[round] = make(map[string]struct{})
		}
		ba.bv[round][hash] = struct{}{}
		if ba.round == round && ba.state == WAIT_BVAL {
			ba.onQuorumBval(hash)
		}
	}
}

func (ba *SuperMA) addProm(round uint32, from uint32, hash string, signature []byte) {
	if ba.proms[round] == nil {
		ba.proms[round] = make(map[string]map[uint32][]byte)
	}
	if ba.proms[round][hash] == nil {
		ba.proms[round][hash] = make(map[uint32][]byte)
	}
	ba.proms[round][hash][from] = signature

	if ba.cfg.ID == from {
		ba.havePromised[round] = hash
	}

	if uint32(len(ba.proms[round][hash])) >= ba.cfg.N-ba.cfg.F {
		// fast path, broadcast n-f prom and exit
		ba.Decide(hash)
		ba.stop = true
		ba.state = CAN_EXIT

		qc := &common.QC{
			Hash: hash,
			Sigs: make([]*common.Signature, ba.cfg.N-ba.cfg.F),
		}
		i := uint32(0)
		for id, sig := range ba.proms[round][hash] {
			qc.Sigs[i] = new(common.Signature)
			qc.Sigs[i].Id = id
			qc.Sigs[i].Sig = sig
			i++
			if i == ba.cfg.N-ba.cfg.F {
				break
			}
		}
		qcBytes, err := qc.Marshal()
		if err != nil {
			ba.logger.Error("marshal prom qc failed", err)
			return
		}

		decideMsg := &common.Message{
			Round:     round,
			From:      ba.cfg.ID,
			Sender:    ba.sender,
			Timestamp: ba.timestamp,
			Type:      common.Message_PROMQC,
			Hash:      hash,
			Payload:   qcBytes,
		}
		ba.network.BroadcastMessage(decideMsg)
		return
	}

	if ba.round == round && ba.state == WAIT_PROM_OR_AUX && uint32(len(ba.auxs[round][hash])+len(ba.proms[round][hash])) >= ba.cfg.N-ba.cfg.F {
		ba.onQuorumAux([]string{hash})
	}
}

func (ba *SuperMA) addAux(round uint32, from uint32, hash string, signature []byte) {
	if ba.auxs[round] == nil {
		ba.auxs[round] = make(map[string]map[uint32][]byte)
	}
	if ba.auxs[round][hash] == nil {
		ba.auxs[round][hash] = make(map[uint32][]byte)
	}
	ba.auxs[round][hash][from] = signature

	if ba.cfg.ID == from {
		ba.haveAuxed[round] = hash
	}

	if ba.round != round {
		return
	}
	sum := 0
	res := make([]string, 0)
	for k, v := range ba.auxs[round] {
		sum += len(v)
		if _, ok := ba.bv[ba.round][k]; ok {
			res = append(res, k)
		}
	}
	for k, v := range ba.proms[round] {
		sum += len(v)
		if _, ok := ba.bv[ba.round][k]; ok {
			res = append(res, k)
		}
	}

	if ba.state == WAIT_PROM_OR_AUX && uint32(sum) >= ba.cfg.N-ba.cfg.F && len(res) > 0 {
		ba.onQuorumAux(res)
	}
}
