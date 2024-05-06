package raft

//投票rpc的参数
type RequestVoteArgs struct {
	Term         int //候选者任期
	CandidateId  int //候选者ID
	LastLogIndex int //候选者最新日志号
	LastLogTerm  int //候选者最新日志任期
}

//投票rpc的返回值
type RequestVoteReply struct {
	Term        int  // follower任期
	VoteGranted bool // 是否投票
}

// 节点选举超时,开始一次新的选举
func (rf *Raft) startElectionL() {
	rf.resetElectionTimer() //重置选举超时计时器
	//修改当前peer的状态 变为candidate 给自己投票
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	//节点状态发生改变,持久化
	rf.persist()
	//向其他所有peer发送选举RPC
	rf.requestVotesL()
}

// candidate调用requestVote向其他所有peer群发RPC
func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex(), rf.lastLogTerm()}
	//voteConter：统计其他peer给当前peer的投票数
	voteCounter := 1
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.requestVote(serverId, args, &voteCounter)
		}
	}
}

// candidate对单个peer发送RPC的函数实现
func (rf *Raft) requestVote(serverId int, args *RequestVoteArgs, voteCounter *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)

	//candidate处理来自peer的reply
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//peer的任期比candidate高,转为follower并return
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term)
			return
		}
		//如果已不是candidate,则return
		if rf.state != Candidate {
			return
		}
		//peer给candidate投票
		if reply.VoteGranted {
			*voteCounter++
			//如果超过1/2的peer给当前candidate投票
			//且该选举未过期（rf.currentTerm == args.Term）
			if *voteCounter > len(rf.peers)/2 &&
				rf.currentTerm == args.Term {
				//candidate选举成功变为leader
				rf.state = Leader
				for i, _ := range rf.peers {
					//nextindex[]:初始值为领导人最后的日志条目的索引+1
					rf.nextIndex[i] = rf.lastLogIndex() + 1
					//matchindex[]:初始值为0
					rf.matchIndex[i] = 0
				}
				//立刻向其他所有peer发送心跳包,确立leader权威
				rf.sendAppendsL(true)
			}
		}
	}
}

// follower处理来自candidate的求票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//candidate任期大于当前节点任期 当前节点重置为follower 把VoteGranted
	// 如果出现多个候选者 在这里其他小号的候选者会被转为follower 所以就算有follower给小号候选者投票了也没关系 在这里都会被重置为follower
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	//若当前节点的任期比candidate大,拒绝投票并返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//判断candidate的日志是否比peer更新
	//判断标准:最后一条日志的term大小;最后日志的索引值大小
	upToDate := args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	//若当前节点没有给除了candidate以外的人投票,且candidate日志比peer新,同意投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//节点状态发生改变,持久化
		rf.persist()
		//重置选举超时时间
		rf.resetElectionTimer()
		//否则,拒绝投票
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
