package com.linkedin.venice.controller;

import static com.linkedin.venice.PushJobCheckpoints.DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
import static com.linkedin.venice.PushJobCheckpoints.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.controller.VeniceHelixAdmin.emitPushJobStatusMetrics;
import static com.linkedin.venice.controller.VeniceHelixAdmin.isPushJobFailedDueToUserError;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controller.stats.LogCompactionStats;
import com.linkedin.venice.controller.stats.PushJobStatusStats;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


public class TestPushJobStatusStats {
  private static final Set<PushJobCheckpoints> CUSTOM_USER_ERROR_CHECKPOINTS =
      new HashSet<>(Collections.singletonList(DVC_INGESTION_ERROR_OTHER));

  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testEmitPushJobStatusMetrics(
      boolean isIncrementalPush,
      boolean useUserProvidedUserErrorCheckpoints,
      boolean isRepush) {
    Set<PushJobCheckpoints> userErrorCheckpoints =
        useUserProvidedUserErrorCheckpoints ? CUSTOM_USER_ERROR_CHECKPOINTS : DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
    String storeName = "test-store";
    PushJobStatusRecordKey key = new PushJobStatusRecordKey(storeName, 1);
    PushJobDetails pushJobDetails = mock(PushJobDetails.class);
    Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
    pushJobConfigs.put(new Utf8("incremental.push"), String.valueOf(isIncrementalPush));
    when(pushJobDetails.getPushJobConfigs()).thenReturn(pushJobConfigs);

    String pushId = (isRepush ? Version.VENICE_RE_PUSH_PUSH_ID_PREFIX : "") + "test-push";
    when(pushJobDetails.getPushId()).thenReturn(pushId);

    when(pushJobDetails.getClusterName()).thenReturn(new Utf8("cluster1"));
    List<PushJobDetailsStatusTuple> statusTuples = new ArrayList<>();
    when(pushJobDetails.getOverallStatus()).thenReturn(statusTuples);

    Map<String, PushJobStatusStats> pushJobStatusStatsMap = new HashMap<>();
    PushJobStatusStats pushJobStatusStats = mock(PushJobStatusStats.class);
    pushJobStatusStatsMap.put("cluster1", pushJobStatusStats);

    Map<String, LogCompactionStats> logCompactionStatsMap = new HashMap<>();
    LogCompactionStats logCompactionStats = mock(LogCompactionStats.class);
    logCompactionStatsMap.put("cluster1", logCompactionStats);

    int numberSuccess = 0;
    int numberUserErrors = 0;
    int numberNonUserErrors = 0;

    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      boolean recordMetrics = false;
      if (isSucceeded(status) || isFailed(status)) {
        recordMetrics = true;
      }

      statusTuples.add(new PushJobDetailsStatusTuple(status.getValue(), 0L));

      for (PushJobCheckpoints checkpoint: PushJobCheckpoints.values()) {
        when(pushJobDetails.getPushJobLatestCheckpoint()).thenReturn(checkpoint.getValue());
        emitPushJobStatusMetrics(
            pushJobStatusStatsMap,
            logCompactionStatsMap,
            key,
            pushJobDetails,
            userErrorCheckpoints);
        boolean isUserError = userErrorCheckpoints.contains(checkpoint);

        if (isUserError) {
          if (recordMetrics) {
            if (isFailed(status)) {
              assertTrue(isPushJobFailedDueToUserError(status, pushJobDetails, userErrorCheckpoints));
              numberUserErrors++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberUserErrors)).recordIncrementalPushFailureDueToUserErrorSensor();
              } else {
                verify(pushJobStatusStats, times(numberUserErrors)).recordBatchPushFailureDueToUserErrorSensor();
              }
            } else {
              numberSuccess++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberSuccess)).recordIncrementalPushSuccessSensor();
              } else {
                verify(pushJobStatusStats, times(numberSuccess)).recordBatchPushSuccessSensor();
              }

              if (isRepush) {
                verify(logCompactionStats, times(numberSuccess)).setCompactionComplete(storeName);
              }
            }
          }
        } else {
          if (recordMetrics) {
            assertFalse(isPushJobFailedDueToUserError(status, pushJobDetails, userErrorCheckpoints));
            if (isFailed(status)) {
              numberNonUserErrors++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberNonUserErrors))
                    .recordIncrementalPushFailureNotDueToUserErrorSensor();
              } else {
                verify(pushJobStatusStats, times(numberNonUserErrors)).recordBatchPushFailureNotDueToUserErrorSensor();
              }
            } else {
              numberSuccess++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberSuccess)).recordIncrementalPushSuccessSensor();
              } else {
                verify(pushJobStatusStats, times(numberSuccess)).recordBatchPushSuccessSensor();
              }

              if (isRepush) {
                verify(logCompactionStats, times(numberSuccess)).setCompactionComplete(storeName);
              }
            }
          }
        }
      }
    }
  }
}
