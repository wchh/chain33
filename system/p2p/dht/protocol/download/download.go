package download

import (
	"context"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"

	//protobufCodec "github.com/multiformats/go-multicodec/protobuf"

	"github.com/33cn/chain33/common/log/log15"

	core "github.com/libp2p/go-libp2p-core"

	prototypes "github.com/33cn/chain33/system/p2p/dht/protocol/types"
	uuid "github.com/google/uuid"

	"github.com/33cn/chain33/queue"
	"github.com/33cn/chain33/types"
)

var (
	log = log15.New("module", "p2p.download")
)

func init() {
	prototypes.RegisterProtocol(protoTypeID, &downloadProtol{})
	prototypes.RegisterStreamHandler(protoTypeID, downloadBlockReq, &downloadHander{})
}

const (
	protoTypeID      = "DownloadProtocolType"
	downloadBlockReq = "/chain33/downloadBlockReq/1.0.0"
)

type (
	taskID  string
	taskNum int64
)
type job struct {
	taskID      string
	count       int32
	blockheight int64
}

//type Istream
type downloadProtol struct {
	*prototypes.BaseProtocol
	blockMsg chan *queue.Message
	jobs     map[string]int32
	locker   sync.Mutex
}

func (d *downloadProtol) InitProtocol(env *prototypes.P2PEnv) {
	d.P2PEnv = env
	//注册事件处理函数
	prototypes.RegisterEventHandler(types.EventFetchBlocks, d.handleEvent)

	d.blockMsg = make(chan *queue.Message)
	d.jobs = make(map[string]int32)
	go d.loopSendBlockMsg()

}

type downloadHander struct {
	*prototypes.BaseStreamHandler
}

//Handle 处理请求
func (d *downloadHander) Handle(stream core.Stream) {
	protocol := d.GetProtocol().(*downloadProtol)

	//解析处理
	var data types.MessageGetBlocksReq
	err := prototypes.ReadStream(&data, stream)
	if err != nil {
		log.Error("Handle", "err", err)
		return
	}
	recvData := data.Message
	protocol.processStreamReq(data.GetMessageData().GetId(), recvData, stream)

}

func (d *downloadProtol) getBlock(id string, message *types.P2PGetBlocks) (*types.MessageGetBlocksResp, error) {

	//允许下载的最大高度区间为256
	if message.GetEndHeight()-message.GetStartHeight() > 256 || message.GetEndHeight() < message.GetStartHeight() {
		return nil, errors.New("param error")

	}
	//开始下载指定高度
	reqblock := &types.ReqBlocks{Start: message.GetStartHeight(), End: message.GetEndHeight()}
	resp, err := d.QueryBlockChain(types.EventGetBlocks, reqblock)
	if err != nil {
		log.Error("sendToBlockChain", "Error", err.Error())
		return nil, err
	}

	blockDetails := resp.(*types.BlockDetails)
	var p2pInvData = make([]*types.InvData, 0)
	var invdata types.InvData
	for _, item := range blockDetails.Items {
		invdata.Reset()
		invdata.Ty = 2 //2 block,1 tx
		invdata.Value = &types.InvData_Block{Block: item.Block}
		p2pInvData = append(p2pInvData, &invdata)
	}

	peerID := d.GetHost().ID()
	pubkey, _ := d.GetHost().Peerstore().PubKey(peerID).Bytes()
	blocksResp := &types.MessageGetBlocksResp{MessageData: d.NewMessageCommon(id, peerID.Pretty(), pubkey, false),
		Message: &types.InvDatas{Items: p2pInvData}}

	return blocksResp, nil

}

func (d *downloadProtol) processStreamReq(id string, message *types.P2PGetBlocks, s core.Stream) {
	log.Debug("OnReq", "start", message.GetStartHeight(), "end", message.GetStartHeight(), "remoteId", s.Conn().RemotePeer().String(), "id", id)

	blockdata, err := d.getBlock(id, message)
	if err != nil {
		log.Error("processReq", "err", err, "pid", s.Conn().RemotePeer().String())
		return
	}
	err = prototypes.WriteStream(blockdata, s)
	if err != nil {
		log.Error("WriteStream", "err", err, "pid", s.Conn().RemotePeer().String())
		return
	}

	log.Debug("OnReq", "Send Block Height+++++++", blockdata.Message.GetItems()[0].GetBlock().GetHeight(), "send  to", s.Conn().RemotePeer().String())

}

//GetBlocks 接收来自chain33 blockchain模块发来的请求
func (d *downloadProtol) handleEvent(msg *queue.Message) {

	req := msg.GetData().(*types.ReqBlocks)
	if req.GetStart() > req.GetEnd() {
		log.Error("handleEvent", "download start", req.GetStart(), "download end", req.GetEnd())
		msg.Reply(d.GetQueueClient().NewMessage("blockchain", types.EventReply, types.Reply{Msg: []byte("start>end")}))
		return
	}
	pids := req.GetPid()
	if len(pids) == 0 { //根据指定的pidlist 获取对应的block header
		log.Debug("GetBlocks:pid is nil")
		msg.Reply(d.GetQueueClient().NewMessage("blockchain", types.EventReply, types.Reply{Msg: []byte("no pid")}))
		return
	}

	msg.Reply(d.GetQueueClient().NewMessage("blockchain", types.EventReply, types.Reply{IsOk: true, Msg: []byte("ok")}))
	var tID = uuid.New().String() + "+" + fmt.Sprintf("%d-%d", req.GetStart(), req.GetEnd())
	d.locker.Lock()
	d.jobs[tID] = int32(req.GetEnd() - req.Start + 1)
	d.locker.Unlock()
	log.Debug("handleEvent", "taskID", tID, "download start", req.GetStart(), "download end", req.GetEnd(), "pids", pids)

	//具体的下载逻辑
	var since = time.Now().UnixNano()

	//下发要下载的区块
	var taskChan = make(chan int64)
	go func(start, end int64) {
		for blockheight := start; blockheight <= end; blockheight++ {
			taskChan <- blockheight
		}
	}(req.GetStart(), req.GetEnd())

	//接收请求下载的区块
	ctx, cancelDownloadProc := context.WithCancel(d.Ctx)
	for _, pid := range pids { //以节点为单位，自由竞争下载区块
		go func(pid string) {
			d.processDownload(ctx, pid, tID, taskChan)
		}(pid)
	}

	d.waitTaskFinish(tID)
	cancelDownloadProc()

	log.Info("Download blocks Complete!", "TaskID++++++++++++++", tID, "blocknum", int32(req.GetEnd()-req.Start+1),
		"cost time", fmt.Sprintf("cost time:%d ms", (time.Now().UnixNano()-since)/1e6))

}

func (d *downloadProtol) waitTaskFinish(taskID string) {
	for {
		restJob := d.jobsNum(taskID)
		if restJob <= 0 {
			return
		}
		time.Sleep(time.Millisecond * 300)
	}
}

func (d *downloadProtol) loopSendBlockMsg() {
	client := d.GetQueueClient()
	for {
		select {
		case msg := <-d.blockMsg:
			client.SendTimeout(msg, false, 3*time.Second)

		case <-d.Ctx.Done():
			return
		}

	}
}

func (d *downloadProtol) jobsUpdate(job *job) int32 {
	d.locker.Lock()
	defer d.locker.Unlock()

	taskNum, ok := d.jobs[job.taskID]
	if ok {
		taskNum = taskNum + job.count
		if taskNum <= 0 {
			delete(d.jobs, job.taskID)
		}
		d.jobs[job.taskID] = taskNum
	}
	return taskNum
}

func (d *downloadProtol) jobsNum(taskID string) int32 {
	d.locker.Lock()
	defer d.locker.Unlock()
	taskNum, ok := d.jobs[taskID]
	if ok {
		return taskNum
	}
	return 0
}

//以节点pid 为中心，多节点自由竞争待下载的区块，如果此时有30个Pid被分配到下载任务中，则同时会会有30个Pid 下载不同的区块，
//如果下载失败，则把刚才下载失败的区块放回任务channel中，重新让其他节点进行任务竞争，直到下载成功。

func (d *downloadProtol) processDownload(ctx context.Context, pid, taskid string, taskChan chan int64) {

	id, err := peer.Decode(pid)
	if err != nil {
		//pid 格式不对，直接退出下载竞争序列
		return
	}
	for {
		select {
		case blockheight := <-taskChan:
			//各个节点竞争获取到blockheight
			childCtx, _ := context.WithTimeout(ctx, time.Second*30)
			err = d.downloadBlock(childCtx, blockheight, id)
			if err != nil { //下载失败，任务回收
				log.Error("downloadBlock", "err", err.Error())
				taskChan <- blockheight
				//用临时休眠的方式，暂停任务抢占,让其余节点抢占此区块高度的下载
				time.Sleep(time.Millisecond * 300)
				break
			}
			//下载成功
			var msg job
			msg.taskID = taskid
			msg.count = -1
			msg.blockheight = blockheight
			d.jobsUpdate(&msg)
			//进入下一轮竞争状态

		case <-ctx.Done():
			return
		}

	}
}

func (d *downloadProtol) downloadBlock(ctx context.Context, blockheight int64, pid peer.ID) error {
	select {
	case <-ctx.Done():
		return errors.New("download timeout")
	default:

		var retryCount uint
	ReDownload:
		retryCount++
		if retryCount > 3 {
			return errors.New("beyound max try count 3")
		}

		var since = time.Now().UnixNano()

		getblocks := &types.P2PGetBlocks{StartHeight: blockheight, EndHeight: blockheight,
			Version: 0}

		blockReq := &types.MessageGetBlocksReq{
			Message: getblocks}

		req := &prototypes.StreamRequest{
			PeerID: pid,
			Data:   blockReq,
			MsgID:  []core.ProtocolID{downloadBlockReq},
		}
		var resp types.MessageGetBlocksResp
		err := d.SendRecvPeer(req, &resp)
		if err != nil {
			log.Error("downloadBlock", "SendRecvPeer", err, "pid", pid, "retryCount", retryCount, "blockheight", blockheight)
			goto ReDownload
		}

		block := resp.GetMessage().GetItems()[0].GetBlock()
		remotePid := pid.Pretty()
		costTime := (time.Now().UnixNano() - since) / 1e6

		log.Debug("download+++++", "from", remotePid, "blockheight", block.GetHeight(),
			"blockSize (bytes)", block.Size(), "costTime ms", costTime)

		client := d.GetQueueClient()
		newmsg := client.NewMessage("blockchain", types.EventSyncBlock, &types.BlockPid{Pid: remotePid, Block: block}) //加入到输出通道)
		d.blockMsg <- newmsg
		return nil
	}

}
