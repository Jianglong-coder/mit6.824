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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}
