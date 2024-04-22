package raft

//日志中的一条
type Entry struct {
	Command interface{} // 命令
	Term    int         // 该条日志的任期号
	Index   int         // 该条日志的日志号
}

type Log struct {
	Entries []Entry
	Index0  int // 作用未知
}

//创建一个初始容量为1的日志切片 初始日志Index未初始化默认为0(即第一条日志为无效日志)
func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 1),
		Index0:  0,
	}
	return log
}

//创建一个初始容量为0的日志切片 Index0由调用者决定

// func (l *Log) lastLog() *Entry {
// 	return
// }

func (rf *Raft) lastLogIndex() int {
	// if len(rf.log.Entries) == 0 {
	// 	return rf.lastsnapshotIndex
	// } else {
	// 	return rf.log.lastLog().Index
	// }
	return 0
}

func (rf *Raft) lastLogTerm() int {
	// if len(rf.log.Entries) == 0 {
	// 	return
	// }
	return 0
}
