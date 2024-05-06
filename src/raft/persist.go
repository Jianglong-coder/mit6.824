package raft

import (
	"bytes"

	"6.824/labgob"
)

// 持久化Raft状态
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	//先调用Encode方法 将当前raft需要保存的状态进行labgob编码 然后调用SaveStateAndSnapshot方法将编码后的数据持久化
	//保存的状态有: 1. 当前任期投票给谁了 2.当前见过的最大任期 3.日志条目 4.快照的最后一条日志的日志号和任期 5.快照数据
	if encoder.Encode(rf.votedFor) == nil &&
		encoder.Encode(rf.currentTerm) == nil &&
		encoder.Encode(rf.log) == nil &&
		encoder.Encode(rf.lastsnapshotIndex) == nil &&
		encoder.Encode(rf.lastsnapshotTerm) == nil {
		rf.persister.SaveStateAndSnapshot(writer.Bytes(), rf.snapshot)
	}
}

// 读取持久化数据以恢复Raft状态
func (rf *Raft) readPersist(data []byte) {
	//持久化数据为空
	if data == nil || len(data) < 1 {
		return
	}
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	var votedFor, currentTerm, lastsnapshotIndex, lastsnapshotTerm int
	var logs Log

	if decoder.Decode(&votedFor) == nil &&
		decoder.Decode(&currentTerm) == nil &&
		decoder.Decode(&logs) == nil &&
		decoder.Decode(&lastsnapshotIndex) == nil &&
		decoder.Decode(&lastsnapshotTerm) == nil {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = logs
		rf.lastsnapshotIndex = lastsnapshotIndex
		rf.lastsnapshotTerm = lastsnapshotTerm
		DPrintf("[rf:%v %v]:restart firstlogindex:%v, lastsnapshotindex:%v", rf.me, rf.state, rf.log.Index0, rf.lastsnapshotIndex)

		SnapshotData := rf.persister.ReadSnapshot() //从持久化中读取快照
		if len(SnapshotData) > 0 {
			rf.snapshot = SnapshotData
			//当commitindex小于lastsnapshotindex，马上更新commitindex，并尝试能否apply新日志
			if rf.commitIndex < lastsnapshotIndex {
				rf.commitIndex = lastsnapshotIndex
				rf.applyCond.Broadcast()
			}
		}
	}
}
