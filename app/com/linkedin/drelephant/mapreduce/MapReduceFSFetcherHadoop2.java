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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class implements the Fetcher for MapReduce Applications on Hadoop2
 * Instead of fetching data from job history server, it retrieves history logs and job configs from
 * HDFS directly. Each job's data consists of a JSON event log file with extension ".jhist" and an
 * XML job configuration file.
 */
public class MapReduceFSFetcherHadoop2 implements ElephantFetcher<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(MapReduceFSFetcherHadoop2.class);
  private static final int MAX_SAMPLE_SIZE = 200;
  private static final String SAMPLING_ENABLED = "sampling_enabled";

  private static final String TIMESTAMP_DIR_FORMAT = "%04d" + File.separator + "%02d" + File.separator + "%02d";
  public static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;

  private FetcherConfigurationData _fetcherConfigurationData;
  private FileSystem _fs;
  private String _historyLocation;
  private String _intermediateHistoryLocation;

  public MapReduceFSFetcherHadoop2(FetcherConfigurationData fetcherConfData) throws IOException {
    this._fetcherConfigurationData = fetcherConfData;

    Configuration conf = new Configuration();
    this._fs = FileSystem.get(conf);
    this._historyLocation = conf.get("mapreduce.jobhistory.done-dir");
    this._intermediateHistoryLocation = conf.get("mapreduce.jobhistory.intermediate-done-dir");
    logger.info("Intermediate history dir: " + _intermediateHistoryLocation);
    logger.info("History done dir: " + _historyLocation);
  }

  /**
   * The location of a job history file is in format: {done_dir}/yyyy/mm/dd/{serialPart}.
   * yyyy/mm/dd is the year, month and date of the finish time.
   * serialPart is the first 6 digits of the serial number considering it as a 9 digits number.
   * PS: The serial number is the last part of an app id.
   * <p>
   * For example, if appId = application_1461566847127_84624, then serial number is 84624.
   * Consider it as a 9 digits number, serial number is 000084624. So the corresponding
   * serialPart is 000084. If this application finish at 2016-5-30, its history file will locate
   * at {done_dir}/2016/05/30/000084
   */
  private String getHistoryDir(AnalyticJob job) {
    // generate the date part
    Calendar timestamp = Calendar.getInstance();
    timestamp.setTimeInMillis(job.getFinishTime());
    String datePart = String.format(TIMESTAMP_DIR_FORMAT,
            timestamp.get(Calendar.YEAR),
            timestamp.get(Calendar.MONTH) + 1,
            timestamp.get(Calendar.DAY_OF_MONTH));

    // generate the serial part
    String appId = job.getAppId();
    int serialNumber = Integer.parseInt(appId.substring(appId.lastIndexOf('_') + 1));
    String serialPart = String.format("%09d", serialNumber)
            .substring(0, SERIAL_NUMBER_DIRECTORY_DIGITS);

    return _historyLocation + File.separator + datePart + File.separator + serialPart + File.separator;
  }

  private DataFiles getHistoryFiles(AnalyticJob job) throws IOException {
    String jobId = Utils.getJobIdFromApplicationId(job.getAppId());
    String jobConfPath = null;
    String jobHistPath = null;

    // Search files in done dir
    String jobHistoryDirPath = getHistoryDir(job);
    RemoteIterator<LocatedFileStatus> it;
    try {
      it = _fs.listFiles(new Path(jobHistoryDirPath), false);
    } catch (FileNotFoundException e) {
      logger.error("Can't find job info of " + jobId + ": directory " + jobHistoryDirPath + " not found");
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

    // If some files are missing, search in the intermediate-done-dir in case the HistoryServer has
    // not yet moved them into the done-dir.
    String intermediateDirPath = _intermediateHistoryLocation + File.separator + job.getUser() + File.separator;
    if (jobConfPath == null) {
      jobConfPath = intermediateDirPath + jobId + "_conf.xml";
      if (!_fs.exists(new Path(jobConfPath))) {
        throw new FileNotFoundException("Can't find config of " + jobId + " in neither "
                + jobHistoryDirPath + " nor " + intermediateDirPath);
      }
      logger.info("Found job config in intermediate dir: " + jobConfPath);
    }
    if (jobHistPath == null) {
      try {
        it = _fs.listFiles(new Path(intermediateDirPath), false);
        while (it.hasNext()) {
          String name = it.next().getPath().getName();
          if (name.contains(jobId) && name.endsWith(".jhist")) {
            jobHistPath = intermediateDirPath + name;
            logger.info("Found history file in intermediate dir: " + jobHistPath);
            break;
          }
        }
      } catch (FileNotFoundException e) {
        logger.error("Intermediate history directory " + intermediateDirPath + " not found");
      }
      if (jobHistPath == null) {
        throw new FileNotFoundException("Can't find history file of " + jobId + " in neither "
                + jobHistoryDirPath + " nor " + intermediateDirPath);
      }
    }

    return new DataFiles(jobConfPath, jobHistPath);
  }

  @Override
  public MapReduceApplicationData fetchData(AnalyticJob job) throws IOException {
    DataFiles files = getHistoryFiles(job);
    String confFile = files.getJobConfPath();
    String histFile = files.getJobHistPath();
    String appId = job.getAppId();

    String jobId = Utils.getJobIdFromApplicationId(appId);
    MapReduceApplicationData jobData = new MapReduceApplicationData();
    jobData.setAppId(appId).setJobId(jobId);

    try {
      JobHistoryParser parser = new JobHistoryParser(_fs, histFile);
      JobHistoryParser.JobInfo jobInfo = parser.parse();
      IOException parseException = parser.getParseException();
      if (parseException != null) {
        throw new RuntimeException("Could not parse history file " + histFile, parseException);
      }

      // Fetch job config
      Configuration jobConf = new Configuration(false);
      jobConf.addResource(_fs.open(new Path(confFile)), confFile);
      Properties jobConfProperties = new Properties();
      for (Map.Entry<String, String> entry : jobConf) {
        jobConfProperties.put(entry.getKey(), entry.getValue());
      }
      jobData.setJobConf(jobConfProperties);

      jobData.setSubmitTime(jobInfo.getSubmitTime());
      jobData.setStartTime(jobInfo.getLaunchTime());
      jobData.setFinishTime(jobInfo.getFinishTime());

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
      } else if (state.equals("FAILED")) {

        jobData.setSucceeded(false);
        jobData.setDiagnosticInfo(jobInfo.getErrorInfo());
      } else {
        // Should not reach here
        throw new RuntimeException("Job state not supported. Should be either SUCCEEDED or FAILED");
      }
    } catch(Exception e) {
      logger.error(String.format("Caught exception while fetching job info: %s", e));
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
      time = new long[]{finishTime - startTime, 0, 0, startTime, finishTime};
    } else {
      long shuffleFinishTime = attempInfo.getShuffleFinishTime();
      long mergeFinishTime = attempInfo.getSortFinishTime();
      time = new long[]{finishTime - startTime, shuffleFinishTime - startTime, mergeFinishTime - shuffleFinishTime, startTime, finishTime};
    }
    return time;
  }

  private MapReduceTaskData[] getTaskData(String jobId, List<JobHistoryParser.TaskInfo> infoList) {
    int sampleSize = infoList.size();

    // check if sampling is enabled
    if(Boolean.parseBoolean(_fetcherConfigurationData.getParamMap().get(SAMPLING_ENABLED))) {
      if (infoList.size() > MAX_SAMPLE_SIZE) {
        logger.info(jobId + " needs sampling.");
        Collections.shuffle(infoList);
      }
      sampleSize = Math.min(infoList.size(), MAX_SAMPLE_SIZE);
    }

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

  private class DataFiles {
    private String jobConfPath;
    private String jobHistPath;

    public DataFiles(String confPath, String histPath) {
      this.jobConfPath = confPath;
      this.jobHistPath = histPath;
    }

    public String getJobConfPath() {
      return jobConfPath;
    }

    public void setJobConfPath(String jobConfPath) {
      this.jobConfPath = jobConfPath;
    }

    public String getJobHistPath() {
      return jobHistPath;
    }

    public void setJobHistPath(String jobHistPath) {
      this.jobHistPath = jobHistPath;
    }
  }
}
