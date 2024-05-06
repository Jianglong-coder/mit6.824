package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state 用于raft状态的持久化
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state        RaftState // 当前节点状态
	electionTime time.Time // 选举超时时间

	// Persistent state on all servers:
	currentTerm int // 当前节点见过的最大任期
	votedFor    int // 给谁投票了
	log         Log // 日志

	// Volatile state on all servers:
	commitIndex int // 提交的日志号
	lastApplied int // 已经应用到状态机上的日志号

	// Volatile state on leaders:
	nextIndex  []int // 所有follower下一条需要的日志号
	matchIndex []int // 所有follower匹配上的日志号 在leader想要commit命令时需要 可以直接对着matchIndex进行比较 判断是否可以commit

	applyCh   chan ApplyMsg // 与上层状态机交互 传递commond的管道
	applyCond *sync.Cond    // 唤醒让上层应用日志的条件变量

	snapshot          []byte // 快照数据
	lastsnapshotTerm  int    // 快照的包含的最后一条日志的任期
	lastsnapshotIndex int    // 快照的包含的最后一条日志的日志号
	//如果当前的节点(follower)接收了leader发送的快照时 trysnapshot会被置为true 然后唤醒applier(就是当前函数)协程 然后在这里给上层状态机发送快照数据
	trysnapshot bool // 是否刚刚接收了leader传来的快照
}

func (rf *Raft) RaftPersistSize() int {
	return rf.persister.RaftStateSize()
}

// 供上层状态机调用,获取当前peer的状态
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// raft节点初始化
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers         // 初始化peers数组
	rf.persister = persister // 做持久化用
	rf.me = me               // 当前节点在peers中的索引

	rf.state = Follower     // 初始化状态为follower
	rf.votedFor = -1        // 给谁投票了初始化为-1
	rf.resetElectionTimer() // 初始化选举超时时间

	rf.log = makeEmptyLog() // 初始化日志

	rf.nextIndex = make([]int, len(rf.peers))  //初始化所有follower下一条日志号
	rf.matchIndex = make([]int, len(rf.peers)) //初始化所有follower匹配日志号

	rf.applyCh = applyCh                //初始化applyCh与上层状态机交互(命令传递)的管道
	rf.applyCond = sync.NewCond(&rf.mu) //初始化applyCond条件变量 该变量控制applier协程的阻塞与唤醒

	rf.readPersist(persister.ReadRaftState()) //从持久化中恢复raft状态

	go rf.ticker()  //启动超时定时器协程
	go rf.applier() //启动向上层应用日志协程 没有新日志commit或快照应用时会阻塞

	return rf
}

// 供上层状态机调用,向当前peer尝试发送一条command
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//若peer不是leader,发送失败,直接返回
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogIndex() + 1
	//根据当前leader的任期和最后的一条日志号创建一条新的日志
	e := Entry{command, rf.currentTerm, index}

	//将command添加到日志中
	rf.log.appendone(e)
	rf.persist()

	//收到command后,立刻发送一轮追加日志,加快日志的同步
	rf.sendAppendsL(false)

	return index, rf.currentTerm, true
}

// 如果接收到的 RPC 请求或响应中,任期号T > currentTerm
// 则令 currentTerm = T,并切换为follower
func (rf *Raft) setNewTerm(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// 重置选举超时时间：350~700ms
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	t = t.Add(350 * time.Millisecond)
	ms := rand.Int63() % 350
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

// 心跳时间：100ms
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.check()
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 检查当前peer状态
func (rf *Raft) check() {
	rf.mu.Lock()
	if rf.state == Leader {
		//当前节点为leader,发送一轮心跳包
		rf.sendAppendsL(true)
	} else if time.Now().After(rf.electionTime) {
		//当前节点达到选举超时时间,转变状态为候选人并开始一轮选举
		rf.startElectionL()
	}
	rf.mu.Unlock()
}

type ApplyMsg struct {
	//让状态机应用命令是用的一组变量
	CommandValid bool        //标记该条消息是否是命令
	Command      interface{} //一条命令
	CommandIndex int         //命令在日志中的日志号
	CommandTerm  int         //命令的任期号
	//让状态机做快照时应用的变量
	// For 2D:
	SnapshotValid bool //标记该条消息是否是快照
	Snapshot      []byte
	SnapshotTerm  int //上次快照的任期
	SnapshotIndex int //上次快照的日志号
}

// 用于在leader append成功时检查是否能更新commitIndex的值
func (rf *Raft) updateCommitIndexL() {
	//如果不是leader 或 当前日志为空则退出
	if rf.state != Leader || len(rf.log.Entries) == 0 {
		return
	}

	start := rf.commitIndex + 1
	if start < rf.log.Index0 {
		start = rf.log.Index0
	}

	for n := start; n <= rf.lastLogIndex(); n++ {
		//每次commit都只能提交自己当前任期的日志,故不是当前任期则continue  如果任期有中断等下次再当上leader时 会提交之前的日志吗??????
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		//遍历其他raft peers,检查该日志是否在这些peer上
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
		}
		//如果发现该日志存在于大部分peers的日志中,则更新commitIndex,并唤醒applier线程
		if counter > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
		}
	}
}

// 等待唤醒,并进行快照或日志的apply 将commit的日志应用到状态机中
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// raft节点启动时 应用日志号初始化为0
	rf.lastApplied = 0
	//如果应用在状态机中的日志号小于当前日志条目中的第一条日志号
	if rf.lastApplied < rf.log.Index0 {
		rf.lastApplied = rf.log.Index0 - 1
	}
	//raft节点仍然存活
	for !rf.killed() {
		//当前节点刚刚接收了leader发来的快照
		if rf.trysnapshot {
			applyMsg := ApplyMsg{
				SnapshotValid: true,                 //标记词条消息为快照消息(通知上层状态机应用快照)
				Snapshot:      rf.snapshot,          //快照的数据
				SnapshotTerm:  rf.lastsnapshotTerm,  //快照最后任期号
				SnapshotIndex: rf.lastsnapshotIndex, //快照最后日志号
			}
			rf.trysnapshot = false //重置trysnapshot标记
			rf.mu.Unlock()
			rf.applyCh <- applyMsg //将快照消息放入applyCh管道中
			rf.mu.Lock()
			DPrintf("[rf:%v %v]: rf.applysnapshot success: lastsnapshotindex:%v, snapshot:%v", rf.me, rf.state, rf.lastsnapshotIndex, len(rf.snapshot))
		} else if len(rf.log.Entries) != 0 && rf.commitIndex > rf.lastApplied && rf.lastLogIndex() > rf.lastApplied { //已提交的日志号大于以应用的日志号 此时可以将新提交的日志应用于状态机
			rf.lastApplied++
			if rf.lastApplied >= rf.log.Index0 {
				DPrintf("[rf:%v %v]: try to apply lastapplied:%v", rf.me, rf.state, rf.lastApplied)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.at(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.currentTerm,
				}
				rf.mu.Unlock()
				//将applyMsg放入通道时先解锁,防止死锁阻塞
				//如果applyCh管道被其他goroutine锁定 并且mu互斥锁也被锁定 那么当前goroutine可能会因为尝试同时锁定两个资源(互斥锁和管道)导致阻塞
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				DPrintf("[rf:%v %v]: rf.apply success", rf.me, rf.state)
			}
		} else {
			//applier线程在此处等待被唤醒
			rf.applyCond.Wait()
		}
	}
}
