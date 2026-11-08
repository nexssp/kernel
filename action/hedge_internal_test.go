package action

import "testing"

func TestHedgeDeadlineDoesNotLaunchAfterPrimaryCompletion(t *testing.T) {
	gate := &hedgeLaunchGate{}
	gate.markPrimaryDone()

	launched := false
	if launchHedgeIfPrimaryRunning(gate, func() { launched = true }) {
		t.Fatal("hedge launch gate reported that the primary was still running")
	}
	if launched {
		t.Fatal("hedge launched after primary completion")
	}
}

func TestHedgeCompletionCannotInterleaveWithDeadlineLaunch(t *testing.T) {
	gate := &hedgeLaunchGate{}
	launchEntered := make(chan struct{})
	releaseLaunch := make(chan struct{})
	launchFinished := make(chan struct{})
	launchResult := make(chan bool, 1)

	go func() {
		launchResult <- launchHedgeIfPrimaryRunning(gate, func() {
			close(launchEntered)
			<-releaseLaunch
			close(launchFinished)
		})
	}()
	<-launchEntered

	completionLockAcquired := make(chan bool, 1)
	completionDone := make(chan struct{})
	go func() {
		acquired := gate.mu.TryLock()
		completionLockAcquired <- acquired
		if acquired {
			gate.mu.Unlock()
		}
		gate.markPrimaryDone()
		close(completionDone)
	}()

	acquiredDuringLaunch := <-completionLockAcquired
	close(releaseLaunch)
	launched := <-launchResult
	<-launchFinished
	<-completionDone

	if acquiredDuringLaunch {
		t.Fatal("primary completion acquired the gate during the hedge launch decision")
	}
	if !launched {
		t.Fatal("hedge launch was rejected while the primary was still running")
	}
	if launchHedgeIfPrimaryRunning(gate, func() {
		t.Error("hedge launched after primary completion was recorded")
	}) {
		t.Fatal("hedge launch gate reported that the primary was still running")
	}
}
