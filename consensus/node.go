
// SPDX-License-Identifier: Apache-2.0

package consensus

import (
	"bytes"
	"errors"
	"mytumbler-go/common"
	"mytumbler-go/crypto"
	"mytumbler-go/logger"
	"mytumbler-go/network"
	"sort"
	"sync/atomic"

	"mytumbler-go/utils"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/sortkeys"
	treemap "github.com/liyue201/gostl/ds/map"
)

type Node struct {
	cfg                *common.Config
	network            network.NetWork
	networkPayList     []network.NetWork
	peers              map[uint32]common.Peer
	lastProposed       uint64
	stop               bool
	canExecTimestamp   uint64
	lastExecuted       map[uint32]uint64
	lastDecided        map[uint32]uint64
	pendings           *treemap.Map
	passes             map[uint32]uint64
	instances          map[uint32]map[uint64]*SuperMA
	baMsgChans         map[uint32]map[uint64]chan *common.Message
	baPromChans        map[uint32]map[uint64]chan *common.Message
	baPromQcChans      map[uint32]map[uint64]chan *common.Message
	bvals              map[uint32]map[uint64]map[string]map[uint32]struct{}
	sequence           uint32
	nextSequences      map[uint32]uint32
	waitMsgs           map[uint32]*utils.PriorityQueue
	endorsed           *common.EndorsedEntries
	hashToPayloads     map[uint32]map[string]*common.Message
	blocks             []string
	timestampToHash    map[uint64]string
	hashToVals         map[uint32]map[string][]*common.Message
	executed           int
	dispatchChan       chan *common.Message
	msgChan            chan *common.Message
	supermaFinishChan  chan *common.SuperMAResult
	voteBvalChan       chan *common.Message
	proposeChan        chan []byte
	testMode           int
	startTime          []uint64
	consensusLatencies []uint64
	executionLatencies []uint64
	zeroNum            int
	startChan          chan struct{}
	startProposeChan   chan struct{}
	logger             logger.Logger
	mergeMsg           map[uint32]map[string]common.PayloadIds
	sliceNum           uint32
	bias               uint64
	delta              uint64
	currBatch          *common.Batch
	clientChan         chan *common.ClientReq
	tryProposeChan     chan struct{}
	connection         *rpc.Client
	reqNum             int
	timeflag           bool
	proposeflag        bool
	batchflag          bool
	interval           int
	startId            int32
	blockInfos         []*common.BlockInfo
	nextExecuteBlock   int
	ifPropose          int
	haveDispatchedZero map[uint32]map[uint64]bool
	forwardSkip        map[uint32]map[uint64]struct{}
	skipFlag           uint32
	runInstanceNum     uint32
	baFinished         map[uint32]map[uint64]struct{}
	baFinishedChan     chan *common.SuperMAKey
	zeroBaFinished     map[uint32]map[uint64]struct{}
	endorsedMap        map[uint32]map[uint64]map[uint32]struct{}
	last_pass          uint64
	next_pass          uint64
}

func NewNode(cfg *common.Config, peers map[uint32]common.Peer, logger logger.Logger, conn uint32, ifBussy int, testMode int) *Node {
	crypto.Init()

	node := &Node{
		cfg:                cfg,
		network:            nil,
		networkPayList:     nil,
		peers:              peers,
		lastProposed:       0,
		stop:               false,
		canExecTimestamp:   0,
		lastExecuted:       make(map[uint32]uint64),
		lastDecided:        make(map[uint32]uint64),
		pendings:           treemap.New(treemap.WithKeyComparator(common.SuperMAKeyCmp)),
		passes:             make(map[uint32]uint64),
		instances:          make(map[uint32]map[uint64]*SuperMA),
		baMsgChans:         make(map[uint32]map[uint64]chan *common.Message),
		baPromChans:        make(map[uint32]map[uint64]chan *common.Message),
		baPromQcChans:      make(map[uint32]map[uint64]chan *common.Message),
		bvals:              make(map[uint32]map[uint64]map[string]map[uint32]struct{}),
		sequence:           0,
		nextSequences:      make(map[uint32]uint32),
		waitMsgs:           make(map[uint32]*utils.PriorityQueue),
		endorsed:           new(common.EndorsedEntries),
		hashToPayloads:     make(map[uint32]map[string]*common.Message),
		blocks:             nil,
		startTime:          make([]uint64, 0),
		consensusLatencies: make([]uint64, 0),
		executionLatencies: make([]uint64, 0),
		timestampToHash:    make(map[uint64]string),
		hashToVals:         make(map[uint32]map[string][]*common.Message),
		executed:           0,
		dispatchChan:       make(chan *common.Message, 200000),
		msgChan:            make(chan *common.Message, 200000),
		supermaFinishChan:  make(chan *common.SuperMAResult, 200),
		voteBvalChan:       make(chan *common.Message, 200),
		proposeChan:        make(chan []byte, 10),
		startChan:          make(chan struct{}, 1),
		startProposeChan:   make(chan struct{}, 1),
		clientChan:         make(chan *common.ClientReq, 100),
		tryProposeChan:     make(chan struct{}, 10),
		logger:             logger,
		testMode:           testMode,
		currBatch:          new(common.Batch),
		blockInfos:         make([]*common.BlockInfo, 0),
		nextExecuteBlock:   0,
		reqNum:             0,
		timeflag:           true,
		proposeflag:        true,
		batchflag:          false,
		mergeMsg:           make(map[uint32]map[string]common.PayloadIds),
		sliceNum:           conn,
		bias:               1,
		delta:              0,
		zeroNum:            0,
		haveDispatchedZero: make(map[uint32]map[uint64]bool),
		ifPropose:          ifBussy,
		forwardSkip:        make(map[uint32]map[uint64]struct{}),
		skipFlag:           0,
		runInstanceNum:     0,
		baFinished:         make(map[uint32]map[uint64]struct{}),
		baFinishedChan:     make(chan *common.SuperMAKey, 200),
		zeroBaFinished:     make(map[uint32]map[uint64]struct{}),
		endorsedMap:        make(map[uint32]map[uint64]map[uint32]struct{}),
		last_pass:          0,
		next_pass:          0,
	}

	node.startId = 1

	for _, peer := range peers {
		node.passes[peer.ID] = 0
		node.nextSequences[peer.ID] = 0
		node.waitMsgs[peer.ID] = utils.NewPriorityQueue()
		node.endorsedMap[peer.ID] = make(map[uint64]map[uint32]struct{})
	}

	node.network = network.NewNoiseNetWork(node.cfg.ID, node.cfg.Addr, node.peers, node.msgChan, node.dispatchChan, node.logger, false, 0, cfg.Byzantine, cfg.F)
	for i := uint32(0); i < conn; i++ {
		node.networkPayList = append(node.networkPayList, network.NewNoiseNetWork(node.cfg.ID, node.cfg.Addr, node.peers, node.msgChan, node.dispatchChan, node.logger, true, i+1, cfg.Byzantine, cfg.F))
	}
	return node
}

func (n *Node) Run() {
	n.startRpcServer()
	n.network.Start()
	for _, networkPay := range n.networkPayList {
		networkPay.Start()
	}
	go n.dispatch()
	go n.proposeLoop()
	n.mainLoop()
}

func (n *Node) startRpcServer() {
	rpc.Register(n)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", n.cfg.RpcServer)
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}

func (n *Node) OnStart(msg *common.CoorStart, resp *common.Response) error {
	n.startChan <- struct{}{}
	return nil
}

func (n *Node) Request(req *common.ClientReq, resp *common.ClientResp) error {
	if req.StartId == 1 {
		n.interval = int(req.ReqNum)
		n.startChan <- struct{}{}
		if n.ifPropose == 1 {
			n.startProposeChan <- struct{}{}
		}
	}
	n.clientChan <- req
	return nil
}

func (n *Node) mainLoop() {
	<-n.startChan

	conn, err := rpc.DialHTTP("tcp", n.cfg.ClientServer)
	if err != nil {
		panic(err)
	}
	n.connection = conn

	timer := time.NewTimer(time.Second * time.Duration(n.cfg.Time))
	pass_ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case msg := <-n.msgChan:
			n.handleMessage(msg)
		case res := <-n.supermaFinishChan:
			n.onSuperMAFinish(res)
		case bvalMsg := <-n.voteBvalChan:
			n.onVoteBval(bvalMsg)
		case payload := <-n.proposeChan:
			n.propose(payload)
		case <-timer.C:
			n.StopClient()
			return
		case <-pass_ticker.C:
			if n.next_pass > n.last_pass {
				passMsg := &common.Message{
					Sequence:  n.sequence,
					Timestamp: n.next_pass,
					Type:      common.Message_PASS,
					From:      n.cfg.ID,
				}
				n.sequence++
				n.onReceivePass(passMsg)
				n.network.BroadcastMessage(passMsg)
				n.endorsed.Reset()
				n.last_pass = n.next_pass
			}
		}
	}
}

func (n *Node) proposeLoop() {
	if n.testMode == 1 {
		n.cfg.MaxBatchSize = 1
		n.timeflag = false
	} else {
		n.proposeflag = false
	}

	<-n.startProposeChan

	timer := time.NewTimer(time.Millisecond * time.Duration(n.cfg.MaxWaitTime))
	for {
		select {
		case <-timer.C:
			if n.timeflag {
				n.getBatch()
				n.proposeflag = false
				n.timeflag = false
			}
		case req := <-n.clientChan:
			n.currBatch.Reqs = append(n.currBatch.Reqs, req)
			n.reqNum += n.interval
			atomic.StoreUint32(&n.skipFlag, 1)
			n.batchflag = true
			if n.proposeflag {
				n.getBatch()
				n.proposeflag = false
			}
		case <-n.tryProposeChan:
			if n.batchflag {
				n.getBatch()
				n.proposeflag = false
			} else {
				n.proposeflag = true
			}
		}
	}
}

func (n *Node) getBatch() {
	if n.reqNum <= n.cfg.MaxBatchSize {
		payloadBytes, _ := proto.Marshal(n.currBatch)
		n.currBatch.Reset()
		currBlock := &common.BlockInfo{
			StartID: n.startId,
			ReqNum:  int32(n.reqNum),
		}
		n.startId += int32(n.reqNum)
		n.reqNum = 0
		n.blockInfos = append(n.blockInfos, currBlock)
		atomic.StoreUint32(&n.runInstanceNum, 1)
		n.proposeChan <- payloadBytes
		n.batchflag = false
		atomic.StoreUint32(&n.skipFlag, 0)
	} else {
		reqs := n.currBatch.Reqs[0 : n.cfg.MaxBatchSize/n.interval]
		n.currBatch.Reqs = n.currBatch.Reqs[n.cfg.MaxBatchSize/n.interval:]
		clientreqs := new(common.Batch)
		clientreqs.Reqs = reqs
		payloadBytes, _ := proto.Marshal(clientreqs)
		currBlock := &common.BlockInfo{
			StartID: n.startId,
			ReqNum:  int32(n.cfg.MaxBatchSize),
		}
		n.blockInfos = append(n.blockInfos, currBlock)
		n.startId += int32(n.cfg.MaxBatchSize)
		n.reqNum -= n.cfg.MaxBatchSize
		atomic.StoreUint32(&n.runInstanceNum, 1)
		n.proposeChan <- payloadBytes
		atomic.StoreUint32(&n.skipFlag, 1)
	}
}

func (n *Node) dispatch() {
	for {
		select {
		case msg := <-n.dispatchChan:
			if _, ok1 := n.baFinished[msg.Sender]; ok1 {
				if _, ok2 := n.baFinished[msg.Sender][msg.Timestamp]; ok2 {
					continue
				}
			}
			if !n.existInstance(msg.Timestamp, msg.Sender) {
				n.startInstance(msg)
			} else {
				if msg.Type == common.Message_AUX || msg.Type == common.Message_PROM {
					n.baPromChans[msg.Sender][msg.Timestamp] <- msg
				} else if msg.Type == common.Message_PROMQC {
					n.baPromQcChans[msg.Sender][msg.Timestamp] <- msg
				} else {
					n.baMsgChans[msg.Sender][msg.Timestamp] <- msg
				}
			}
		case ba := <-n.baFinishedChan:
			// garbage collection for finished ba instances
			if _, ok := n.baFinished[ba.Sender]; !ok {
				n.baFinished[ba.Sender] = make(map[uint64]struct{})
			}
			n.baFinished[ba.Sender][ba.Timestamp] = struct{}{}
			delete(n.instances[ba.Sender], ba.Timestamp)
			delete(n.baMsgChans[ba.Sender], ba.Timestamp)
			delete(n.baPromChans[ba.Sender], ba.Timestamp)
			delete(n.baPromQcChans[ba.Sender], ba.Timestamp)
		}
	}
}

func (n *Node) Stop() {
	conn, err := rpc.DialHTTP("tcp", n.cfg.Coordinator)
	if err != nil {
		panic(err)
	}
	totalConsensusLatency := uint64(0)
	totalExecutionLatency := uint64(0)
	for _, l := range n.consensusLatencies {
		totalConsensusLatency += l
	}
	for _, l := range n.executionLatencies {
		totalExecutionLatency += l
	}
	st := &common.CoorStatistics{
		Zero:            uint64(n.zeroNum),
		ConsensusNumber: uint64(len(n.consensusLatencies)),
		ExecutionNumber: uint64(len(n.executionLatencies)),
		ID:              n.cfg.ID,
	}
	if len(n.consensusLatencies) == 0 {
		st.ConsensusLatency = 0
	} else {
		st.ConsensusLatency = totalConsensusLatency / uint64(len(n.consensusLatencies))
	}
	if len(n.executionLatencies) == 0 {
		st.ExecutionLatency = 0
	} else {
		st.ExecutionLatency = totalExecutionLatency / uint64(len(n.executionLatencies))
	}
	var resp common.Response
	conn.Call("Coordinator.Finish", st, &resp)
}

func (n *Node) StopClient() {
	st := &common.NodeBack{
		Zero:   uint32(n.zeroNum),
		NodeID: n.cfg.ID,
		Addr:   n.cfg.Coordinator,
	}
	var resp common.Response
	n.connection.Call("Client.NodeFinish", st, &resp)
}

func (n *Node) getTimeStamp() uint64 {
	return uint64(time.Now().UnixNano() / 1000000)
}

func (n *Node) propose(payload []byte) {
	n.broadcastPayload(payload)

	ts := n.getTimeStamp()
	if ts < n.passes[n.cfg.ID] {
		if n.passes[n.cfg.ID] > n.lastProposed {
			ts = n.passes[n.cfg.ID] + 1
		} else {
			ts = n.lastProposed + 1
		}
	}
	if ts <= n.canExecTimestamp {
		ts = n.canExecTimestamp + 1
	}
	n.timestampToHash[ts] = n.blocks[len(n.blocks)-1]
	proposal := &common.Message{
		Round:     0,
		Sender:    n.cfg.ID,
		Type:      common.Message_BVAL,
		Sequence:  0,
		Timestamp: ts,
		Hash:      n.blocks[len(n.blocks)-1],
	}
	msgBytes, err := proto.Marshal(proposal)
	if err != nil {
		n.logger.Error("marshal val failed", err)
		return
	}
	proposal.Signature = crypto.Sign(n.cfg.PrivKey, msgBytes)

	proposal.From = n.cfg.ID
	proposal.Type = common.Message_VAL
	if n.hashToVals[n.cfg.ID] == nil {
		n.hashToVals[n.cfg.ID] = make(map[string][]*common.Message)
	}
	n.hashToVals[n.cfg.ID][proposal.Hash] = append(n.hashToVals[n.cfg.ID][proposal.Hash], proposal)
	n.network.BroadcastMessage(proposal)
	n.addBval(ts, n.cfg.ID, proposal.Hash, n.cfg.ID)
	n.dispatchChan <- proposal
	entry := &common.EndorsedEntry{
		Timestamp: proposal.Timestamp,
		Sender:    proposal.Sender,
	}
	n.endorsed.Entries = append(n.endorsed.Entries, entry)
	n.pendings.Insert(&common.SuperMAKey{
		Sender:    n.cfg.ID,
		Timestamp: ts,
	}, "")
	n.logger.Infof("propose %v", ts)
}

func (n *Node) broadcastPayload(payload []byte) {
	if len(n.startTime) > len(n.consensusLatencies) {
		return
	}
	hash := crypto.Hash(payload)
	sliceLength := len(payload) / int(n.sliceNum)
	for i := uint32(0); i < n.sliceNum; i++ {
		msgSlice := &common.Message{
			Sequence:        0,
			From:            n.cfg.ID,
			Round:           0,
			Sender:          n.cfg.ID,
			Timestamp:       0,
			Type:            common.Message_PAYLOAD,
			Hash:            hash,
			TotalPayloadNum: n.sliceNum,
			PayloadSlice:    i + 1,
		}
		if i < (n.sliceNum - 1) {
			msgSlice.Payload = payload[i*uint32(sliceLength) : (i+1)*uint32(sliceLength)]
		} else {
			msgSlice.Payload = payload[i*uint32(sliceLength):]
		}
		n.networkPayList[i].BroadcastMessage(msgSlice)
	}

	msg := &common.Message{
		Sequence:  0,
		From:      n.cfg.ID,
		Round:     0,
		Sender:    n.cfg.ID,
		Timestamp: 0,
		Type:      common.Message_PAYLOAD,
		Hash:      hash,
		Payload:   payload,
	}
	n.startTime = append(n.startTime, uint64(time.Now().UnixNano()/1000000))
	if n.hashToPayloads[n.cfg.ID] == nil {
		n.hashToPayloads[n.cfg.ID] = make(map[string]*common.Message)
	}
	n.hashToPayloads[n.cfg.ID][hash] = msg
	n.blocks = append(n.blocks, hash)
}

func (n *Node) handleMessage(msg *common.Message) {
	switch msg.Type {
	case common.Message_PASS:
		// handle pass in FIFO
		if n.nextSequences[msg.From] > msg.Sequence {
			n.logger.Errorln("invalid pass sequence number")
			return
		}
		if n.nextSequences[msg.From] < msg.Sequence {
			n.waitMsgs[msg.From].Push(msg)
			return
		}
		n.onReceivePass(msg)
		n.nextSequences[msg.From]++
		for {
			if n.waitMsgs[msg.From].Len() == 0 ||
				n.waitMsgs[msg.From].Top().(*common.Message).Sequence > n.nextSequences[msg.From] {
				return
			}
			if n.waitMsgs[msg.From].Top().(*common.Message).Sequence == n.nextSequences[msg.From] {
				n.onReceivePass(n.waitMsgs[msg.From].Pop().(*common.Message))
				n.nextSequences[msg.From]++
			}
		}
	case common.Message_PAYLOAD:
		n.onReceivePayload(msg)
	case common.Message_VAL:
		n.onReceiveVal(msg)
	case common.Message_SKIP:
		n.onReceiveSkip(msg)
	default:
		n.logger.Error("invalid msg type", errors.New("error in msg dispatch"))
	}
}

func (n *Node) onReceiveSkip(msg *common.Message) {
	// forward skip when first received
	if _, ok := n.forwardSkip[msg.From]; ok {
		if _, ok1 := n.forwardSkip[msg.From][msg.Timestamp]; ok1 {
			return
		}
	}
	if msg.From != n.cfg.ID {
		n.network.BroadcastMessage(msg)
	}
	if n.forwardSkip[msg.From] == nil {
		n.forwardSkip[msg.From] = make(map[uint64]struct{})
	}
	n.forwardSkip[msg.From][msg.Timestamp] = struct{}{}

	if n.lastDecided[msg.From] < msg.Timestamp {
		n.trySendPass(msg.Timestamp, msg.From)
	}
}

func (n *Node) onReceiveVal(msg *common.Message) {
	currTime := uint64(time.Now().UnixNano() / 1000000)
	if msg.Timestamp > currTime+uint64(n.cfg.MaxDrift) {
		go func() {
			duration := msg.Timestamp - currTime - uint64(n.cfg.MaxDrift)
			time.Sleep(time.Duration(duration) * time.Millisecond)
			n.msgChan <- msg
		}()
		return
	}
	n.dispatchChan <- msg
	if n.hashToVals[msg.Sender] == nil {
		n.hashToVals[msg.Sender] = make(map[string][]*common.Message)
	}
	n.hashToVals[msg.Sender][msg.Hash] = append(n.hashToVals[msg.Sender][msg.Hash], msg)
	n.addBval(msg.Timestamp, msg.Sender, msg.Hash, msg.Sender)

	if n.passes[n.cfg.ID] < msg.Timestamp {
		if _, ok := n.hashToPayloads[msg.Sender][msg.Hash]; ok {
			if _, ok := n.bvals[msg.Sender][msg.Timestamp][msg.Hash][n.cfg.ID]; !ok {
				// endorse if have not passed
				vote := &common.Message{
					Round:     0,
					Sender:    msg.Sender,
					Type:      common.Message_BVAL,
					Sequence:  0,
					Timestamp: msg.Timestamp,
					Hash:      msg.Hash,
				}
				voteBytes, err := proto.Marshal(vote)
				if err != nil {
					n.logger.Error("marshal vote failed", err)
					return
				}
				vote.Signature = crypto.Sign(n.cfg.PrivKey, voteBytes)
				vote.From = n.cfg.ID
				n.dispatchChan <- vote
				n.network.BroadcastMessage(vote)
				n.addBval(msg.Timestamp, msg.Sender, msg.Hash, n.cfg.ID)
				entry := &common.EndorsedEntry{
					Timestamp: msg.Timestamp,
					Sender:    msg.Sender,
				}
				n.endorsed.Entries = append(n.endorsed.Entries, entry)

				// skip if no pending requests
				if atomic.LoadUint32(&n.skipFlag) == 0 && atomic.LoadUint32(&n.runInstanceNum) == 0 {
					skip := &common.Message{
						Sender:    msg.Sender,
						Type:      common.Message_SKIP,
						Timestamp: msg.Timestamp,
						Hash:      msg.Hash,
					}
					skipBytes, err := proto.Marshal(skip)
					if err != nil {
						n.logger.Error("marshal vote failed", err)
						return
					}
					skip.Signature = crypto.Sign(n.cfg.PrivKey, skipBytes)
					skip.From = n.cfg.ID
					n.network.BroadcastMessage(skip)
					n.onReceiveSkip(skip)
				}
			}
		}
	}
}

func (n *Node) onReceivePayload(msgSlice *common.Message) {
	if int(n.sliceNum) == 1 {
		n.onReceiveCompletePayload(msgSlice)
		return
	}

	if n.mergeMsg[msgSlice.Sender] == nil {
		n.mergeMsg[msgSlice.Sender] = make(map[string]common.PayloadIds)
	}
	n.mergeMsg[msgSlice.Sender][msgSlice.Hash] = append(n.mergeMsg[msgSlice.Sender][msgSlice.Hash], common.PayloadId{Id: msgSlice.PayloadSlice, Payload: msgSlice.Payload})
	if len(n.mergeMsg[msgSlice.Sender][msgSlice.Hash]) == int(n.sliceNum) {
		sort.Sort(n.mergeMsg[msgSlice.Sender][msgSlice.Hash])
		var buffer bytes.Buffer
		for _, ps := range n.mergeMsg[msgSlice.Sender][msgSlice.Hash] {
			buffer.Write(ps.Payload)
		}

		msg := &common.Message{
			Sequence:  msgSlice.Sequence,
			From:      msgSlice.From,
			Round:     msgSlice.Round,
			Sender:    msgSlice.Sender,
			Timestamp: msgSlice.Timestamp,
			Type:      msgSlice.Type,
			Hash:      msgSlice.Hash,
			Payload:   buffer.Bytes(),
		}
		n.onReceiveCompletePayload(msg)
	}
}

func (n *Node) onReceiveCompletePayload(msg *common.Message) {
	if _, ok := n.hashToPayloads[msg.Sender][msg.Hash]; ok {
		return
	}
	if n.hashToPayloads[msg.Sender] == nil {
		n.hashToPayloads[msg.Sender] = make(map[string]*common.Message)
	}
	n.hashToPayloads[msg.Sender][msg.Hash] = msg
	if _, ok := n.hashToVals[msg.Sender][msg.Hash]; ok {
		ts := n.hashToVals[msg.Sender][msg.Hash][0].Timestamp
		for _, val := range n.hashToVals[msg.Sender][msg.Hash] {
			if val.Timestamp > ts {
				ts = val.Timestamp
			}
		}
		if n.passes[n.cfg.ID] < ts {
			if _, ok := n.bvals[msg.Sender][ts][msg.Hash][n.cfg.ID]; !ok {
				// endorse if have not passed
				vote := &common.Message{
					Round:     0,
					Sender:    msg.Sender,
					Type:      common.Message_BVAL,
					Sequence:  0,
					Timestamp: ts,
					Hash:      msg.Hash,
				}
				voteBytes, err := proto.Marshal(vote)
				if err != nil {
					panic(err)
				}
				vote.Signature = crypto.Sign(n.cfg.PrivKey, voteBytes)
				vote.From = n.cfg.ID
				n.dispatchChan <- vote
				n.network.BroadcastMessage(vote)
				n.addBval(ts, msg.Sender, msg.Hash, n.cfg.ID)
				entry := &common.EndorsedEntry{
					Timestamp: vote.Timestamp,
					Sender:    msg.Sender,
				}
				n.endorsed.Entries = append(n.endorsed.Entries, entry)

				// skip if no pending requests
				if atomic.LoadUint32(&n.skipFlag) == 0 && atomic.LoadUint32(&n.runInstanceNum) == 0 {
					skip := &common.Message{
						Sender:    msg.Sender,
						Type:      common.Message_SKIP,
						Timestamp: ts,
						Hash:      msg.Hash,
					}
					skipBytes, err := proto.Marshal(skip)
					if err != nil {
						n.logger.Error("marshal vote failed", err)
						return
					}
					skip.Signature = crypto.Sign(n.cfg.PrivKey, skipBytes)
					skip.From = n.cfg.ID
					n.network.BroadcastMessage(skip)
					n.onReceiveSkip(skip)
				}
			}
		}
	}
	n.tryExecute()
}

func (n *Node) onVoteBval(bvalMsg *common.Message) {
	if _, ok := n.bvals[bvalMsg.Sender][bvalMsg.Timestamp][bvalMsg.Hash][n.cfg.ID]; !ok {
		if n.passes[n.cfg.ID] < bvalMsg.Timestamp {
			msgBytes, err := bvalMsg.Marshal()
			if err != nil {
				n.logger.Error("marshal msg failed", err)
				return
			}
			bvalMsg.Signature = crypto.Sign(n.cfg.PrivKey, msgBytes)
			bvalMsg.From = n.cfg.ID

			n.dispatchChan <- bvalMsg
			n.network.BroadcastMessage(bvalMsg)
			n.addBval(bvalMsg.Timestamp, bvalMsg.Sender, bvalMsg.Hash, n.cfg.ID)

			entry := &common.EndorsedEntry{
				Timestamp: bvalMsg.Timestamp,
				Sender:    bvalMsg.Sender,
			}
			n.endorsed.Entries = append(n.endorsed.Entries, entry)
		}
	}
}

func (n *Node) onReceivePass(msg *common.Message) {
	if msg.Timestamp <= n.passes[msg.From] {
		n.logger.Warnf("invalid pass msg from %v", msg.From)
		return
	}
	old_pass := n.passes[msg.From]
	update_exec := false
	n.passes[msg.From] = msg.Timestamp
	if old_pass <= n.canExecTimestamp && msg.Timestamp > n.canExecTimestamp {
		passTime := make([]uint64, n.cfg.N)
		i := uint32(0)
		for _, t := range n.passes {
			passTime[i] = t
			i++
		}
		sortkeys.Uint64s(passTime)
		// update executable_ts as the 2f+1 th pass time
		if n.canExecTimestamp < passTime[n.cfg.F] {
			n.canExecTimestamp = passTime[n.cfg.F]
			update_exec = true
		}
	}

	var entries *common.EndorsedEntries
	if msg.From != n.cfg.ID {
		entries = new(common.EndorsedEntries)
		err := proto.Unmarshal(msg.Payload, entries)
		if err != nil {
			n.logger.Error("unmarshal pass message failed", err)
			return
		}
	} else {
		entries = n.endorsed
		endorseBytes, err := proto.Marshal(entries)
		msg.Payload = endorseBytes
		if err != nil {
			panic(err)
		}
	}
	for _, entry := range entries.Entries {
		key := &common.SuperMAKey{
			Sender:    entry.Sender,
			Timestamp: entry.Timestamp,
		}
		// insert others' endorsed instances into pending
		if entry.Timestamp > n.canExecTimestamp {
			if !n.pendings.Contains(key) {
				n.pendings.Insert(key, "")
			}
		}
		if n.pendings.Contains(key) {
			if n.endorsedMap[entry.Sender][entry.Timestamp] == nil {
				n.endorsedMap[entry.Sender][entry.Timestamp] = make(map[uint32]struct{})
			}
			n.endorsedMap[entry.Sender][entry.Timestamp][msg.From] = struct{}{}
		}
	}

	if update_exec {
		// abort pending instances passed by 2f+1 nodes
		iter := n.pendings.First()
		for i := n.pendings.Size(); i > 0; i-- {
			ts := iter.Key().(*common.SuperMAKey).Timestamp
			sender := iter.Key().(*common.SuperMAKey).Sender
			if _, ok := n.haveDispatchedZero[sender][ts]; ok {
				iter.Next()
				continue
			}
			if iter.Key().(*common.SuperMAKey).Timestamp > n.canExecTimestamp {
				break
			}
			if iter.Value().(string) != "" {
				iter.Next()
				continue
			}
			zero := &common.Message{
				Type:      common.Message_VOTEZERO,
				Timestamp: iter.Key().(*common.SuperMAKey).Timestamp,
				Sender:    iter.Key().(*common.SuperMAKey).Sender,
			}
			n.dispatchChan <- zero
			if n.haveDispatchedZero[sender] == nil {
				n.haveDispatchedZero[sender] = make(map[uint64]bool)
			}
			n.haveDispatchedZero[sender][ts] = true
			iter.Next()
		}
	}
	n.tryExecute()
}

func (n *Node) onSuperMAFinish(res *common.SuperMAResult) {
	if res.Hash == "0" {
		if _, ok := n.zeroBaFinished[res.Key.Sender][res.Key.Timestamp]; ok {
			return
		}
		if _, ok := n.zeroBaFinished[res.Key.Sender]; !ok {
			n.zeroBaFinished[res.Key.Sender] = make(map[uint64]struct{})
		}
		n.zeroBaFinished[res.Key.Sender][res.Key.Timestamp] = struct{}{}
	}

	n.baFinishedChan <- &res.Key

	if res.Hash == "0" && res.Key.Sender == n.cfg.ID {
		n.bias *= 2
		n.zeroNum++
	}

	if res.Hash != "0" {
		if _, ok := n.lastDecided[res.Key.Sender]; !ok || n.lastDecided[res.Key.Sender] < res.Key.Timestamp {
			n.trySendPass(res.Key.Timestamp, res.Key.Sender)
		}
	} else {
		n.logger.Warnf("superma %v-%v zero", res.Key.Timestamp, res.Key.Sender)
	}

	if res.Key.Sender == n.cfg.ID {
		atomic.StoreUint32(&n.runInstanceNum, 0)
		if res.Hash == "0" {
			hash := n.timestampToHash[res.Key.Timestamp]
			n.proposeChan <- n.hashToPayloads[n.cfg.ID][hash].Payload
		} else {
			latency := uint64(time.Now().UnixNano()/1000000) - n.startTime[len(n.consensusLatencies)]
			n.logger.Debugf("my ba %v-%v finished %v ms", res.Key.Timestamp, res.Key.Sender, latency)
			n.consensusLatencies = append(n.consensusLatencies, latency)
			n.tryProposeChan <- struct{}{}
		}
	} else {
		n.logger.Debugf("ba %v-%v finished", res.Key.Timestamp, res.Key.Sender)
	}

	n.pendings.Insert(&res.Key, res.Hash)
	n.tryExecute()
}

func (n *Node) trySendPass(ts uint64, sender uint32) {
	n.lastDecided[sender] = ts
	if len(n.lastDecided) >= int(2*n.cfg.F+1) {
		tmp := make([]uint64, len(n.lastDecided))
		i := 0
		for _, v := range n.lastDecided {
			tmp[i] = v
			i++
		}
		sortkeys.Uint64s(tmp)
		index := len(tmp) - int(2*n.cfg.F+1)
		if tmp[index] > n.next_pass {
			n.next_pass = tmp[index]
		}
	}
}

func (n *Node) tryExecute() {
	for {
		if n.pendings.Size() == 0 {
			return
		}
		key := n.pendings.First().Key().(*common.SuperMAKey)
		// 1. before executable_ts
		if key.Timestamp <= n.canExecTimestamp {
			// 2.1 ba have been finished
			if n.pendings.First().Value().(string) != "" {
				// 2.1.1 ba output 0, remove
				if n.pendings.First().Value().(string) == "0" {
					n.pendings.EraseIter(n.pendings.First())
					delete(n.endorsedMap[key.Sender], key.Timestamp)
					continue
				}
				// 2.1.2 ba output h and have received payload, execute
				if _, ok := n.hashToPayloads[key.Sender][n.pendings.First().Value().(string)]; ok {
					if key.Sender == n.cfg.ID {
						latency := uint64(time.Now().UnixNano()/1000000) - n.startTime[len(n.executionLatencies)]
						n.executionLatencies = append(n.executionLatencies, latency)
						n.logger.Infof("my execute %v-%v using %v ms", key.Timestamp, key.Sender, latency)

						st := &common.NodeBack{
							NodeID:      0,
							SupermaTime: n.consensusLatencies[len(n.consensusLatencies)-1],
							StartID:     uint32(n.blockInfos[n.nextExecuteBlock].StartID),
							ReqNum:      uint32(n.blockInfos[n.nextExecuteBlock].ReqNum),
						}
						var resp common.Response
						n.connection.Call("Client.NodeFinish", st, &resp)
						n.nextExecuteBlock++
					} else {
						n.logger.Infof("execute %v-%v", key.Timestamp, key.Sender)
					}
					n.lastExecuted[key.Sender] = key.Timestamp
					delete(n.hashToPayloads[key.Sender], n.pendings.First().Value().(string))
					n.pendings.EraseIter(n.pendings.First())
					delete(n.endorsedMap[key.Sender], key.Timestamp)
					continue
				}
			} else {
				// 2.2 n-f nodes passed who never endorsed, remove
				count := uint32(0)
				for id, time := range n.passes {
					if time >= key.Timestamp {
						if _, ok := n.endorsedMap[key.Sender][key.Timestamp][id]; !ok {
							count++
						}
					}
				}
				if count >= n.cfg.N-n.cfg.F {
					n.baFinishedChan <- key
					if key.Sender == n.cfg.ID {
						hash := n.timestampToHash[key.Timestamp]
						n.proposeChan <- n.hashToPayloads[n.cfg.ID][hash].Payload
					}
					n.pendings.EraseIter(n.pendings.First())
					delete(n.endorsedMap[key.Sender], key.Timestamp)
					if _, ok := n.zeroBaFinished[key.Sender][key.Timestamp]; ok {
						return
					}
					if _, ok := n.zeroBaFinished[key.Sender]; !ok {
						n.zeroBaFinished[key.Sender] = make(map[uint64]struct{})
					}
					n.zeroBaFinished[key.Sender][key.Timestamp] = struct{}{}
					continue
				}
				return
			}
		}
		return
	}
}

func (n *Node) existInstance(ts uint64, sender uint32) bool {
	if _, ok := n.instances[sender]; !ok {
		return false
	}
	_, ok := n.instances[sender][ts]
	return ok
}

func (n *Node) startInstance(msg *common.Message) {
	if n.instances[msg.Sender] == nil {
		n.baMsgChans[msg.Sender] = make(map[uint64]chan *common.Message)
		n.baPromChans[msg.Sender] = make(map[uint64]chan *common.Message)
		n.baPromQcChans[msg.Sender] = make(map[uint64]chan *common.Message)
		n.instances[msg.Sender] = make(map[uint64]*SuperMA)
	}
	n.baMsgChans[msg.Sender][msg.Timestamp] = make(chan *common.Message, 1000)
	n.baPromChans[msg.Sender][msg.Timestamp] = make(chan *common.Message, 1000)
	n.baPromQcChans[msg.Sender][msg.Timestamp] = make(chan *common.Message, 1000)

	if msg.Type == common.Message_AUX || msg.Type == common.Message_PROM {
		n.baPromChans[msg.Sender][msg.Timestamp] <- msg
	} else if msg.Type == common.Message_PROMQC {
		n.baPromQcChans[msg.Sender][msg.Timestamp] <- msg
	} else {
		n.baMsgChans[msg.Sender][msg.Timestamp] <- msg
	}

	n.instances[msg.Sender][msg.Timestamp] = NewSuperMA(msg.Sender, msg.Timestamp, n.peers,
		n.network, n.cfg, n.supermaFinishChan,
		n.baMsgChans[msg.Sender][msg.Timestamp],
		n.baPromChans[msg.Sender][msg.Timestamp],
		n.baPromQcChans[msg.Sender][msg.Timestamp],
		n.voteBvalChan, n.logger)
	go n.instances[msg.Sender][msg.Timestamp].Run()
}

func (n *Node) addBval(ts uint64, sender uint32, hash string, from uint32) {
	if n.bvals[sender] == nil {
		n.bvals[sender] = make(map[uint64]map[string]map[uint32]struct{})
	}
	if n.bvals[sender][ts] == nil {
		n.bvals[sender][ts] = make(map[string]map[uint32]struct{})
	}
	if n.bvals[sender][ts][hash] == nil {
		n.bvals[sender][ts][hash] = make(map[uint32]struct{})
	}
	n.bvals[sender][ts][hash][from] = struct{}{}
}
