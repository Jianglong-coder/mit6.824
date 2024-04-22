package raft

// AppendEntries RPC的参数
type AppendEntriesArgs struct {
	term         int   // 领导者的任期
	leaderId     int   // 领导者的id
	prevLogIndex int   // 领导者前一条日志的日志号
	prevLogTerm  int   // 领导者前一条日志的任期号
	entries      []Log // 发给从节点的日志 如果是心跳消息 日志为空
	leaderCommit int   // 领导者的提交日志号
}

// AppendEntries RPC的响应
type AppendEntriesReply struct {
	term    int  // 从节点的当前任期号 让领导者更新
	success bool // 从节点的prevLogIndex和PrevLogTerm匹配上 并把log保存
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	//遍历除自己意外的所有节点发送日志或者心跳包
	for peerIndex, _ := range rf.peers {
		if peerIndex != rf.me {
			if heartbeat { //是心跳包
				rf.sendAppendL(peerIndex, heartbeat)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peerIndex int, heartbeat bool) {
	// nextIndex := rf.nextIndex[peerIndex] // 对应服务器要接收的日志号

	// //var prevLogIndex int // 领导者 上一条日志号
	// //var prevLogTerm int  // 领导者 上一条日志任期号
	// var entries []Entry // 要发送给从节点的日志数组

	// if len(rf.log.Entries) == 0 {
	// 	entries = make([]Entry, 0) //数组置空
	// } else {
	// 	//lastLogIndex := rf.lastLogIndex()
	// }

	// args := &AppendEntriesArgs{}
}
