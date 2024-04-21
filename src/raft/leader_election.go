package raft

import (
	"math/rand"
	"time"
)

// 重置选举时间
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(151)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}
