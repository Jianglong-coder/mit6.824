package raft

// 给follower发送快照的rpc参数
type InstallSnapshotArgs struct {
	Term              int    // leader任期
	LeaderId          int    // leader的id
	LastIncludedIndex int    // 快照最后一条日志的日志号
	LastIncludedTerm  int    // 快照最后一条日志的任期
	Data              []byte // 快照数据
}

// 给follower发送快照的rpc响应
type InstallSnapshotReply struct {
	Term int
}

// rpc调用 follower接收来自leader发送过来的snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm //将当前节点任期返回给leader
	//如果leader任期比follower小或leader的snapshot比follower的snapshot的旧,直接return
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex < rf.lastsnapshotIndex {
		return
	}
	//否则无条件接收来自leader的snapshot
	//更新当前follower的状态 1.snapshot 2.lastsnapshotindex 3.lastsnapshotterm
	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)
	rf.lastsnapshotIndex = args.LastIncludedIndex
	rf.lastsnapshotTerm = args.LastIncludedTerm
	rf.trysnapshot = true
	DPrintf("[rf:%v %v]: InstallSnap, lastsnapindex:%v ", rf.me, rf.state, rf.lastsnapshotIndex)
	rf.persist()             // follower状态发生改变,做持久化
	rf.applyCond.Broadcast() // 唤醒applier协程 让状态机保存快照
}

// 给一个follower单独发送快照
func (rf *Raft) sendSnapL(peer int) {
	// 生成发送快照的rpc参数 包括leader任期 leader的序号 快照最后一条日志的索引和term 快照数据
	args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastsnapshotIndex, rf.lastsnapshotTerm, make([]byte, len(rf.snapshot))}
	copy(args.Data, rf.snapshot)
	// 起一个协程给follwer发送快照
	go func() {
		reply := InstallSnapshotReply{}
		ok := rf.sendSnapshot(peer, args, &reply) //调用发送快照的rpc
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.pocessSnapshotReplyL(peer, args, &reply) //处理rpc响应
		}
	}()
}

//给follower发送快照的rpc
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 处理来自follower接收快照后的rpc响应
func (rf *Raft) pocessSnapshotReplyL(serverId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 如果follower的任期比leader大, leader退位为follower
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
	}
}

// 供上层kvserver调用,主动进行日志的快照 index为上层状态机所拥有的全部日志信息包括index在内的日志号
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果上层状态机所拥有的状态(包括日志号index及以前所有信息)小于当前日志条目中的第一条 说明不需要做快照(因为状态机连当前日志条目中的最小的日志号都没包含 做不了日志的裁剪)
	if rf.log.Index0 > index {
		return
	}
	// 以下代码为可以做快照的逻辑 即可以做日志裁剪
	// 可以更新快照所包含的日志号和term
	rf.lastsnapshotIndex = index
	rf.lastsnapshotTerm = rf.log.at(index).Term
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	//如果该snapshot包含的最后一条日志的索引,小于当前日志的最后一条日志的索引,则进行日志裁剪
	if index < rf.log.lastLog().Index {
		DPrintf("[rf:%v]: receive snapshot index:%v, rf.lastlogindex:%v", rf.me, index, rf.log.lastLog().Index)
		rf.log.cutstart(index + 1) //日志裁剪

	} else { //如果上层状态机已经包含了日志的所有内容 将日志全部丢弃,创建新的空日志
		Entries := []Entry{}
		rf.log = mkLog(Entries, index+1)
	}
	//节点状态发生改变,持久化
	rf.persist()
}

// 供上层kvserver调用,判断是否可以安装快照 函数传进来的参数为要安装的快照的相关信息
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果快照最后一条日志小于commitindex或等于lastapplied说明可能已经有新的日志在apply或已经apply了,旧快照不能安装
	// 因为上面几个条件不满足的情况下应用快照 会导致一些已经应用在状态机中的命令被这个快照覆盖掉
	if lastIncludedIndex < rf.commitIndex || lastIncludedIndex == rf.lastApplied || len(snapshot) == 0 || lastIncludedIndex < rf.log.Index0 {
		return false
	}
	// 以下代码为可以安装快照的逻辑
	rf.lastsnapshotIndex = lastIncludedIndex
	rf.lastsnapshotTerm = lastIncludedTerm

	if len(rf.log.Entries) != 0 && lastIncludedIndex < rf.log.lastLog().Index {
		rf.log.cutstart(lastIncludedIndex + 1) //裁剪日志
	} else { //当前节点的Log中的日志已被快照全部包含 清空日志
		Entries := []Entry{}
		rf.log = mkLog(Entries, lastIncludedIndex+1)
	}
	//节点状态发生变化 做持久化
	rf.persist()
	return true
}
