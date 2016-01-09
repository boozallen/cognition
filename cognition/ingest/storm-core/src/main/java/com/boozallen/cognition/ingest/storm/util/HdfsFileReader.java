/*
 * Licensed to Booz Allen Hamilton under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Booz Allen Hamilton licenses this file to you
 * under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.boozallen.cognition.ingest.storm.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This is a helper class for bolts that need to read a file from HDFS.
 *
 * @author hwu
 * @update hwu
 */
public class HdfsFileReader implements Serializable {
  private static final long serialVersionUID = -123488599758749321L;
  private static final String HADOOP_CONF_DIRECTORY = "hadoopconfdirectory";
  private static final String EMPTY_STRING = "";
  private Configuration hadoopConfig = new Configuration();
  private String hadoopConfDirectory;
  private FileSystem fileSystem;

  public HdfsFileReader(org.apache.commons.configuration.Configuration conf) {
    hadoopConfDirectory = conf.getString(HADOOP_CONF_DIRECTORY, EMPTY_STRING);

    Map<String, String> tempConfig = new HashMap<>();
    for (Iterator<?> itr = conf.getKeys(); itr.hasNext(); ) {
      String key = (String) itr.next();
      String value = conf.getString(key);
      tempConfig.put(key, value);
    }

    if (tempConfig.isEmpty()) {
      hadoopConfig.addResource(new Path(hadoopConfDirectory + File.separator + "core-site.xml"));
      hadoopConfig.addResource(new Path(hadoopConfDirectory + File.separator + "hdfs-site.xml"));
    } else {
      for (Map.Entry<String, String> entry : tempConfig.entrySet()) {
        hadoopConfig.set(entry.getKey(), entry.getValue());
      }
    }
  }

  public HdfsFileReader(Configuration conf) {
    hadoopConfig = conf;
  }

  public HdfsFileReader(Map<String, String> conf) {
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      hadoopConfig.set(entry.getKey(), entry.getValue());
    }
  }

  // connect to HDFS server
  private void connect2Hdfs() throws IOException {
    fileSystem = FileSystem.get(hadoopConfig);
  }

  public FSDataInputStream getFSDataInputStream(String hdfsPath) throws IllegalArgumentException, IOException {
    if (fileSystem == null) {
      this.connect2Hdfs();
    }

    return fileSystem.open(new Path("hdfs://" + hdfsPath));
  }

  public FSDataInputStream getFSDataInputStream(Path hdfsPath) throws IllegalArgumentException, IOException {
    if (fileSystem == null) {
      this.connect2Hdfs();
    }

    return fileSystem.open(hdfsPath);
  }

  public void getFileFromHdfs(String hdfsPath, File dst) throws IllegalArgumentException, IOException {
    if (fileSystem == null) {
      this.connect2Hdfs();
    }

    Path src = new Path(hdfsPath);
    fileSystem.copyToLocalFile(src, new Path(dst.getAbsolutePath()));
  }
}
