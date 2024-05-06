package raft

type AppendEntriesArgs struct {
	Term         int     // leader见过的最大任期号 (应该就是leader的任期号 因为有大的任期号出现 leader身份会改变)
	LeaderId     int     // leader的id
	PrevLogIndex int     // 新的日志条目紧随之前的索引值
	PrevLogTerm  int     // PrevLogIndex对应的日志条目的任期号
	Entries      []Entry // 新的日志条目
	LeaderCommit int     // leader所提交的日志条目索引值
}

type AppendEntriesReply struct {
	Term     int  // 从节点见过的最高任期号
	Success  bool // appendentries是否成功
	Conflict bool // 日志是否有冲突
	XTerm    int  // 冲突的任期号
	XIndex   int  // 冲突的任期中的第一条日志号
	XLen     int  // 冲突的任期内的日志数量
}

// follower处理leader发送的Append RPC请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Conflict = false

	//当follower任期比leader高,则返回false
	if args.Term < rf.currentTerm {
		return
	}

	//重置选举超时时间
	rf.resetElectionTimer()

	//当leader的任期大于当前节点的任期 重置当前节点(可能是candidate也可能是follower)的任期和状态 重置为follower
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	//当前日志长度不为零的处理逻辑
	if len(rf.log.Entries) != 0 {
		//防止日志切片越界,对prelogindex进行处理,如果小于index0,则让其等于index0
		if args.PrevLogIndex < rf.log.Index0 {
			args.PrevLogIndex = rf.log.Index0
			args.PrevLogTerm = rf.log.at(rf.log.Index0).Term
		}
		//如果prelogindex大于当前日志的最后一条日志索引,则判定全部日志冲突
		if rf.lastLogIndex() < args.PrevLogIndex {
			reply.Conflict = true
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = rf.log.len()
			return
		}
		// 上一条日志的任期不相同 发生冲突了
		if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Conflict = true
			//取出当前节点(从节点)的上一条日志的任期号
			xTerm := rf.log.at(args.PrevLogIndex).Term
			//找到冲突任期内的第一条日志
			for xIndex := args.PrevLogIndex; xIndex > rf.log.Index0; xIndex-- {
				if rf.log.at(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex // 冲突任期号内的第一条日志的日志号
					break
				}
			}
			reply.XTerm = xTerm //冲突任期号
			reply.XLen = rf.log.len()
			return
		}

		//如果prelogindex日志没有发生冲突的处理逻辑
		for idx, entry := range args.Entries {
			if entry.Index < rf.log.Index0 {
				continue
			}
			//找到prelog之后第一条冲突的日志,截断该日志后的所有日志丢弃不用
			if len(rf.log.Entries) != 0 && entry.Index <= rf.lastLogIndex() && rf.log.at(entry.Index).Term != entry.Term {
				rf.log.cutend(entry.Index)
			}
			//追加上leader发送来的日志
			if entry.Index > rf.lastLogIndex() {
				rf.log.append(args.Entries[idx:]...)
				rf.persist() //日志条目发生变化 做持久化
				break
			}
		}
	} else { //当前节点日志长度为零的处理逻辑
		//如果leader发送的是heartbeat或args的第一条日志大于Index0说明日志冲突,直接返回
		if len(args.Entries) == 0 || args.Entries[0].Index > rf.log.Index0 {
			return
		}
		//否则找到args中对应Index0的第一条日志,然后追加leader发送的日志
		for idx, entry := range args.Entries {
			if entry.Index < rf.log.Index0 {
				continue
			}
			rf.log.append(args.Entries[idx:]...)
			rf.persist() //日志条目发生变化 做持久化
			break
		}
	}

	//更新follower的commitindex,并尝试唤醒applier线程
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Broadcast()
	}
	//appendentries成功
	reply.Success = true
}

// 给所有从节点发送AppendEntries RPC
func (rf *Raft) sendAppendsL(heartbeat bool) {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			if heartbeat || (len(rf.log.Entries) != 0 && rf.lastLogIndex() >= rf.nextIndex[peer]) {
				rf.sendAppendL(peer, heartbeat)
			}
		}
	}
}

// 给一个从节点发送AppendEntries RPC
func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	nextIndex := rf.nextIndex[peer] //获取对应从节点需要的日志号

	var prevLogIndex int // 前一条日志的日志号
	var prevLogTerm int  // 前一条日志的任期号
	var entries []Entry  // 存储要发送给从节点的日志条目

	if len(rf.log.Entries) == 0 { // 如果当前节点的日志条目数为0 说明做过快照 日志都被删除了 然后从快照初始化前一条日志号和前一天任期号
		prevLogIndex = rf.lastsnapshotIndex
		prevLogTerm = rf.lastsnapshotTerm
		entries = make([]Entry, 0)
	} else {
		lastLogIndex := rf.lastLogIndex() // 获取当前日志中最后一条日志的日志号
		if nextIndex <= rf.log.Index0 {   // 如果从节点需要的日志号小于leader日志中的第一条日志号
			nextIndex = rf.log.Index0           // 将从节点需要的日志号设置为Log第一条日志的日志号
			prevLogIndex = rf.lastsnapshotIndex // 读持久化
			prevLogTerm = rf.lastsnapshotTerm   // 读持久化
		} else if nextIndex > lastLogIndex+1 { // 如果从节点需要的日志号大于leader日志中的最后一条日志号
			nextIndex = lastLogIndex + 1
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log.at(prevLogIndex).Term
		} else { // 如果从节点需要的日志号在leader日志条目中
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log.at(prevLogIndex).Term
		}

		entries = make([]Entry, lastLogIndex-nextIndex+1) // 给entries(要发送给从节点的日志)分配空间
		copy(entries, rf.log.slice(nextIndex))            // 从leader的日志条目拷贝给要发送给从节点的entries 这里nextIndex==lastLogIndex + 1的时候 不会越界吗?????
	}

	args := &AppendEntriesArgs{ //创建发送给从节点的参数
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, &reply) //调用follower的rpc
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.pocessAppendReplyL(peer, args, &reply) //处理follower返回的AppendEntries的响应结果
		}
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader处理来自follower关于Append RPC的回复
func (rf *Raft) pocessAppendReplyL(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm { // 从节点返回的任期大于leader任期 说明当前集群中出现了更大的任期(代表出现了新的leader) 当前leader(旧)转为follower
		rf.setNewTerm(reply.Term)
		return
	} else if args.Term == rf.currentTerm { // 从节点返回的任期等于leader任期 代表当前节点还是leader
		if reply.Success { // AppendEntries成功  更新从节点对应的nextIndex(从节点需要的日志的号)和matchIndex(从节点匹配上的最大日志号)
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)    // 更新从节点对应的nextIndex(从节点需要的日志的号)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match) // matchIndex(从节点匹配上的最大日志号)
		} else if reply.Conflict { // AppendEntries失败 说明日志有冲突
			// 如果返回XTerm为-1,说明全部日志冲突
			if reply.XTerm == -1 {
				// 当XTerm为-1时XLen为-1 将对应从节点的nextIndex设置为-1
				rf.nextIndex[serverId] = reply.XLen
			} else {
				// 否则找到冲突任期中的最后一条日志的日志号
				lastLogInXTerm := rf.findLastLogInTermL(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 { //
					rf.nextIndex[serverId] = lastLogInXTerm // 回退到冲突任期内的最后一条日志的日志号 由leader自己查询得到的
				} else {
					rf.nextIndex[serverId] = reply.XIndex // 回退到冲突任期内的第一条日志的日志号(由follower的reply返回的)
				}
			}
			//如果待发送给follower的日志索引小于Index0(当前日志数组中第一条日志的日志号 说明前面已经被快照了 日志已被删除),则发送快照,并更新nextIndex[]
			if rf.nextIndex[serverId] < rf.log.Index0 {
				rf.sendSnapL(serverId)                 // 发送快照
				rf.nextIndex[serverId] = rf.log.Index0 // 将follower的nextIndex更新为当前日志数组中第一条日志的日志号
			}
		} else if rf.nextIndex[serverId] > 1 { //appendEntries失败 日志也没有冲突
			rf.nextIndex[serverId]--                    // 将follower的nextIndex回退到前一条日志
			if rf.nextIndex[serverId] < rf.log.Index0 { // 如果小于leader的第一条日志的日志号 发送快照 更新nextIndex
				rf.sendSnapL(serverId)                 // 发送快照
				rf.nextIndex[serverId] = rf.log.Index0 // 将follower的nextIndex更新为当前日志数组中第一条日志的日志号
			}
		}
		rf.updateCommitIndexL() //更新commitIndex
	}
}

// 找到冲突任期内的最后一条日志号
func (rf *Raft) findLastLogInTermL(x int) int {
	for i := rf.lastLogIndex(); i > rf.log.Index0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}
