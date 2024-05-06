package raft

type Log struct {
	Entries []Entry
	Index0  int // 日志数组中的第一条日志的起始日志号(注意:不是索引号) 它不一定是0 因为在做了快照(加入前三条日志存起来了)后 需要在日志数组中擦除最前面的三条日志
}

type Entry struct {
	Command interface{} // 命令
	Term    int         // 该条日志的任期号
	Index   int         // 该条日志的日志号
}

// 创建一个初始容量为1的日志切片,初始日志Index未初始化默认为0（即第一条日志为无效日志）
func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 1),
		Index0:  0,
	}
	return log
}

// 在日志裁剪后为空 就调用这个函数 创建一个初始容量为0,起始日志号为Index0的日志切片
func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (l *Log) appendone(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l *Log) at(idx int) *Entry {
	return &l.Entries[idx-l.Index0]
}

func (l *Log) slice(index int) []Entry {
	return l.Entries[index-l.Index0:]
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLog() *Entry {
	return l.at(l.Index0 + l.len() - 1)
}

// 返回最后一条日志的日志索引（不是切片下标索引！）
func (rf *Raft) lastLogIndex() int {
	//如果log中没有日志 就返回快照的所包含的最后一条日志号
	if len(rf.log.Entries) == 0 {
		return rf.lastsnapshotIndex
	} else {
		return rf.log.lastLog().Index
	}
}

// 返回最后一条日志的任期
func (rf *Raft) lastLogTerm() int {
	if len(rf.log.Entries) == 0 {
		return rf.lastsnapshotTerm
	} else {
		return rf.log.lastLog().Term
	}
}

func (l *Log) cutend(idx int) {
	l.Entries = l.Entries[0 : idx-l.Index0]
}

// 快照后 调用的日志裁剪函数 idx为不能裁剪的第一条日志号
func (l *Log) cutstart(idx int) {
	l.Entries = l.Entries[idx-l.Index0:] //裁剪日志
	l.Index0 = idx                       // 更新日志条目中的起始日志号
}
