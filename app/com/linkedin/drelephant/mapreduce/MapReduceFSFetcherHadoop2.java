/*
 * Copyright 2016 Linkin Corp.
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

package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * This class implements the Fetcher for MapReduce Applications on Hadoop2
 * Instead of fetching data from job history server, it retrieves history logs and job configs from HDFS directly
 */
public class MapReduceFSFetcherHadoop2 implements ElephantFetcher<MapReduceApplicationData> {
    private static final Logger logger = Logger.getLogger(MapReduceFSFetcherHadoop2.class);
    private static final int MAX_SAMPLE_SIZE = 200;

    private static final String TIMESTAMP_DIR_FORMAT = "%04d" + File.separator + "%02d" + File.separator + "%02d";
    public static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;

    private FetcherConfigurationData _fetcherConfigurationData;
    private FileSystem _fs;
    private String _historyLocation;


    public MapReduceFSFetcherHadoop2(FetcherConfigurationData fetcherConfData) throws IOException {
        this._fetcherConfigurationData = fetcherConfData;

        Configuration conf = new Configuration();
        this._fs = FileSystem.get(conf);
        this._historyLocation = conf.get("mapreduce.jobhistory.done-dir");
        logger.info("history done dir: " + _historyLocation);
    }

    private String getHistoryDir(AnalyticJob job) {
        Calendar timestamp = Calendar.getInstance();
        timestamp.setTimeInMillis(job.getFinishTime());
        String datePart = String.format(TIMESTAMP_DIR_FORMAT,
                timestamp.get(Calendar.YEAR),
                // months are 0-based in Calendar, but people will expect January to
                // be month #1.
                timestamp.get(Calendar.MONTH) + 1,
                timestamp.get(Calendar.DAY_OF_MONTH));

        String appId = job.getAppId();
        int serialNumber = Integer.parseInt(appId.substring(appId.lastIndexOf('_') + 1));
        String serialPart = String.format("%09d", serialNumber).substring(0, SERIAL_NUMBER_DIRECTORY_DIGITS);

        return datePart + File.separator + serialPart;
    }

    @Override
    public MapReduceApplicationData fetchData(AnalyticJob job) throws Exception {
        String appId = job.getAppId();
        String jobId = Utils.getJobIdFromApplicationId(appId);
        String jobHistoryDirPath = _historyLocation + File.separator + getHistoryDir(job) + File.separator;
        String jobConfPath = null;
        String jobHistPath = null;

        RemoteIterator<LocatedFileStatus> it;
        try {
            it = _fs.listFiles(new Path(jobHistoryDirPath), false);
        } catch (FileNotFoundException e) {
            logger.info("Can't find logs of " + jobId);
            return null;
        }
        while (it.hasNext() && (jobConfPath == null || jobHistPath == null)) {
            String name = it.next().getPath().getName();
            if (name.contains(jobId)) {
                if (name.endsWith("_conf.xml")) {
                    jobConfPath = jobHistoryDirPath + name;
                } else if (name.endsWith(".jhist")) {
                    jobHistPath = jobHistoryDirPath + name;
                }
            }
        }
        if (jobConfPath == null) {
            throw new FileNotFoundException("Can't find config of " + jobId + " in " + jobHistoryDirPath);
        }
        if (jobHistPath == null) {
            throw new FileNotFoundException("Can't find history file of " + jobId + " in " + jobHistoryDirPath);
        }
        return fetchData(appId, jobId, jobConfPath, jobHistPath);
    }

    public MapReduceApplicationData fetchData(String appId, String jobId, String confFile, String histFile)
            throws Exception {

        JobHistoryParser parser = new JobHistoryParser(_fs, histFile);
        JobHistoryParser.JobInfo jobInfo = parser.parse();
        IOException parseException = parser.getParseException();
        if (parseException != null) {
            throw new RuntimeException("Could not parse history file " + histFile, parseException);
        }

        MapReduceApplicationData jobData = new MapReduceApplicationData();
        jobData.setAppId(appId).setJobId(jobId);

        // Fetch job config
        Configuration jobConf = new Configuration(false);
        jobConf.addResource(_fs.open(new Path(confFile)), confFile);
        Properties jobConfProperties = new Properties();
        for (Map.Entry<String, String> entry : jobConf) {
            jobConfProperties.put(entry.getKey(), entry.getValue());
        }
        jobData.setJobConf(jobConfProperties);

        String state = jobInfo.getJobStatus();
        if (state.equals("SUCCEEDED")) {

            jobData.setSucceeded(true);

            // Fetch job counter
            MapReduceCounterData jobCounter = getCounterData(jobInfo.getTotalCounters());

            // Fetch task data
            Map<TaskID, JobHistoryParser.TaskInfo> allTasks = jobInfo.getAllTasks();
            List<JobHistoryParser.TaskInfo> mapperInfoList = new ArrayList<JobHistoryParser.TaskInfo>();
            List<JobHistoryParser.TaskInfo> reducerInfoList = new ArrayList<JobHistoryParser.TaskInfo>();
            for (JobHistoryParser.TaskInfo taskInfo : allTasks.values()) {
                if (taskInfo.getTaskType() == TaskType.MAP) {
                    mapperInfoList.add(taskInfo);
                } else {
                    reducerInfoList.add(taskInfo);
                }
            }
            if (jobInfo.getTotalMaps() > MAX_SAMPLE_SIZE) {
                logger.debug(jobId + " total mappers: " + mapperInfoList.size());
            }
            if (jobInfo.getTotalReduces() > MAX_SAMPLE_SIZE) {
                logger.debug(jobId + " total reducers: " + reducerInfoList.size());
            }
            MapReduceTaskData[] mapperList = getTaskData(jobId, mapperInfoList);
            MapReduceTaskData[] reducerList = getTaskData(jobId, reducerInfoList);

            jobData.setCounters(jobCounter).setMapperData(mapperList).setReducerData(reducerList);

        } else if (state.equals("FAILED")){
            jobData.setSucceeded(false);
            jobData.setDiagnosticInfo(jobInfo.getErrorInfo());

        } else {
            // Should not reach here
            throw new RuntimeException("Job state not supported. Should be either SUCCEEDED or FAILED");
        }

        return jobData;
    }

    private MapReduceCounterData getCounterData(Counters counters) {
        MapReduceCounterData holder = new MapReduceCounterData();
        for (CounterGroup group : counters) {
            String groupName = group.getName();
            for (Counter counter : group) {
                holder.set(groupName, counter.getName(), counter.getValue());
            }
        }
        return holder;
    }

    private long[] getTaskExecTime(JobHistoryParser.TaskAttemptInfo attempInfo) {
        long startTime = attempInfo.getStartTime();
        long finishTime = attempInfo.getFinishTime();
        boolean isMapper = (attempInfo.getTaskType() == TaskType.MAP);

        long[] time;
        if (isMapper) {
            time = new long[] { finishTime - startTime, 0, 0 };
        } else {
            long shuffleFinishTime = attempInfo.getShuffleFinishTime();
            long mergeFinishTime = attempInfo.getSortFinishTime();
            time = new long[] { finishTime - startTime, shuffleFinishTime - startTime, mergeFinishTime - shuffleFinishTime};
        }
        return time;
    }

    private MapReduceTaskData[] getTaskData(String jobId, List<JobHistoryParser.TaskInfo> infoList) {
        if (infoList.size() > MAX_SAMPLE_SIZE) {
            logger.info(jobId + " needs sampling.");
            Collections.shuffle(infoList);
        }

        int sampleSize = Math.min(infoList.size(), MAX_SAMPLE_SIZE);

        MapReduceTaskData[] taskList = new MapReduceTaskData[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            JobHistoryParser.TaskInfo tInfo = infoList.get(i);
            if (!"SUCCEEDED".equals(tInfo.getTaskStatus())) {
                System.out.println("This is a failed task: " + tInfo.getTaskId().toString());
                continue;
            }

            String taskId = tInfo.getTaskId().toString();
            TaskAttemptID attemptId = tInfo.getSuccessfulAttemptId();
            taskList[i] = new MapReduceTaskData(taskId, attemptId.toString());

            MapReduceCounterData taskCounterData = getCounterData(tInfo.getCounters());
            long[] taskExecTime = getTaskExecTime(tInfo.getAllTaskAttempts().get(attemptId));

            taskList[i].setCounter(taskCounterData);
            taskList[i].setTime(taskExecTime);
        }
        return taskList;
    }
}
