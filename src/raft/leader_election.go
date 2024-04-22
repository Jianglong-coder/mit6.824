package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者的任期
	CandidateId  int // 候选者的索引
	LastLogIndex int // 候选者的最后一条日志的索引
	LastLogTerm  int // 候选者的最后一条日志的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 回应给候选者的当前节点的任期
	VoteGranted bool // 是否投票
}

// 发起一次选举
func (rf *Raft) startElectionL() {
	rf.resetElectionTimer()

	//修改当前节点的状态
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me

	//向其他所有peer发送选举rpc
	rf.requestVotesL()
}

func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{
		// Your data here (2A, 2B).
		Term:         rf.currentTerm,    //候选者的任期
		CandidateId:  rf.me,             // 候选者的索引
		LastLogIndex: rf.lastLogIndex(), // 候选者的最后一条日志的索引
		LastLogTerm:  rf.lastLogTerm(),  // 候选者的最后一条日志的任期
	}
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.requestVote(serverId, args)
		}
	}
}

func (rf *Raft) requestVote(serverId int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//
	}
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 候选者任期 小于自己见过的最新任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 候选者任期 大于自己的见过的最新任期
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	//判断candidate的日志是否比peer更新
	//判断标准: 1.最后一条日志的term大小 2.最后日志的索引值大小
	upToDate := args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= args.LastLogIndex())

	//若当前peer没有给除了candidate以外的人投票，且candidate日志比peer新，同意投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 重置选举时间
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(151)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}
