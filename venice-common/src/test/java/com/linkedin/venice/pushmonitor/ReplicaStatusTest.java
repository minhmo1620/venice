package com.linkedin.venice.pushmonitor;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;


public class ReplicaStatusTest {
  private String instanceId = "testInstance";

  @Test
  public void testCreateReplicaStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    Assert.assertEquals(replicaStatus.getInstanceId(), instanceId);
    Assert.assertEquals(replicaStatus.getCurrentStatus(), STARTED);
    Assert.assertEquals(replicaStatus.getCurrentProgress(), 0);
  }

  private void testStatusesUpdate(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
      replicaStatus.setCurrentStatus(from);
      replicaStatus.updateStatus(status);
      Assert.assertEquals(replicaStatus.getCurrentStatus(), status);
    }
  }

  @Test
  public void testUpdateStatusFromSTARTED() {
    testStatusesUpdate(STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromPROGRESS() {
    testStatusesUpdate(PROGRESS, STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromERROR() {
    testStatusesUpdate(ERROR, STARTED);
  }

  @Test
  public void testUpdateStatusFromEndOfPushReceived() {
    testStatusesUpdate(END_OF_PUSH_RECEIVED, STARTED, ERROR, COMPLETED, START_OF_BUFFER_REPLAY_RECEIVED, TOPIC_SWITCH_RECEIVED);
  }

  @Test
  public void testUpdateStatusFromStartOfBufferReplayReceived() {
    testStatusesUpdate(START_OF_BUFFER_REPLAY_RECEIVED, STARTED, ERROR, PROGRESS, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromTopicSwitchReceived() {
    /**
     * For grandfathering, it's possible that END_OF_PUSH_RECEIVED status will come after a TOPIC_SWITCH status
     */
    testStatusesUpdate(TOPIC_SWITCH_RECEIVED, END_OF_PUSH_RECEIVED, STARTED, ERROR, PROGRESS, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromCOMPLETED() {
    testStatusesUpdate(COMPLETED, STARTED, ERROR, START_OF_INCREMENTAL_PUSH_RECEIVED, END_OF_INCREMENTAL_PUSH_RECEIVED, TOPIC_SWITCH_RECEIVED);
  }

  @Test
  public void testStatusHistory() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(STARTED);
    replicaStatus.updateStatus(PROGRESS);
    replicaStatus.updateStatus(COMPLETED);

    List<StatusSnapshot> history = replicaStatus.getStatusHistory();
    Assert.assertEquals(history.size(), 3);
    Assert.assertEquals(history.get(0).getStatus(), STARTED);
    Assert.assertEquals(history.get(1).getStatus(), PROGRESS);
    Assert.assertEquals(history.get(2).getStatus(), COMPLETED);
  }

  @Test
  public void testStatusHistoryTooLong() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH * 2; i++) {
      replicaStatus.updateStatus(STARTED);
    }
    Assert.assertEquals(replicaStatus.getStatusHistory().size(), replicaStatus.MAX_HISTORY_LENGTH);
  }

  @Test
  public void testStatusHistoryWithLotsOfProgressStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(STARTED);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH * 2; i++) {
      replicaStatus.updateStatus(PROGRESS);
    }
    Assert.assertEquals(replicaStatus.getCurrentStatus(), PROGRESS);
    Assert.assertEquals(replicaStatus.getStatusHistory().size(), 2,
        "PROGRESS should be added into history if the previous status is also PROGRESS.");
  }
}