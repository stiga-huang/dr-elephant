/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.deploy.history;

import com.linkedin.drelephant.spark.data.SparkJobProgressData;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.ApplicationEventListener;
import org.apache.spark.scheduler.ReplayListenerBus;
import org.apache.spark.storage.StorageStatusListener;
import org.apache.spark.storage.StorageStatusTrackingListener;
import org.apache.spark.ui.env.EnvironmentListener;
import org.apache.spark.ui.exec.ExecutorsListener;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.spark.ui.storage.StorageListener;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;

public class SparkDataCollectionTest {

    private static ReplayListenerBus replayBus;
    private static ApplicationEventListener applicationEventListener;
    private static JobProgressListener jobProgressListener;
    private static EnvironmentListener environmentListener;
    private static StorageStatusListener storageStatusListener;
    private static ExecutorsListener executorsListener;
    private static StorageListener storageListener;
    private static StorageStatusTrackingListener storageStatusTrackingListener;
    private static SparkDataCollection dataCollection;

    @BeforeClass
    public static void runBeforeClass() {
        replayBus = new ReplayListenerBus();
        applicationEventListener = new ApplicationEventListener();
        jobProgressListener = new JobProgressListener(new SparkConf());
        environmentListener = new EnvironmentListener();
        storageStatusListener = new StorageStatusListener();
        executorsListener = new ExecutorsListener(storageStatusListener);
        storageListener = new StorageListener(storageStatusListener);
        storageStatusTrackingListener = new StorageStatusTrackingListener();
        replayBus.addListener(storageStatusTrackingListener);

        dataCollection = new SparkDataCollection(applicationEventListener,
                jobProgressListener, storageStatusListener, environmentListener,
                executorsListener, storageListener, storageStatusTrackingListener);

        replayBus.addListener(applicationEventListener);
        replayBus.addListener(jobProgressListener);
        replayBus.addListener(environmentListener);
        replayBus.addListener(storageStatusListener);
        replayBus.addListener(executorsListener);
        replayBus.addListener(storageListener);
    }

    @Test
    public void testCollectJobProgressData() {
        InputStream in = new BufferedInputStream(SparkDataCollectionTest.class.getClassLoader().getResourceAsStream(
                "spark_event_logs/event_log_1"));
        replayBus.replay(in, in.toString(), false);

        SparkJobProgressData jobProgressData = dataCollection.getJobProgressData();
        assertNotNull("can't get job progress data", jobProgressData);
    }

}
