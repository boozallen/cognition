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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This is a helper class for bolts that need a local copy of a file from HDFS.
 *
 * @author michaelkorb
 */
public class HdfsFileLoader implements Serializable {
  private static final long serialVersionUID = -386488599758749528L;
  private static final String HADOOP_CONF_DIRECTORY = "hadoopconfdirectory";
  private static final String EMPTY_STRING = "";
  private Map<String, String> _hadoopConfig = new HashMap<>();
  private String _hadoopConfDirectory;

  public HdfsFileLoader(org.apache.commons.configuration.Configuration conf) {
    _hadoopConfDirectory = conf.getString(HADOOP_CONF_DIRECTORY, EMPTY_STRING);
    for (Iterator itr = conf.getKeys(); itr.hasNext(); ) {
      String key = (String) itr.next();
      String value = conf.getString(key);
      _hadoopConfig.put(key, value);
    }
  }

  private FileSystem getHadoopFileSystem() {
    Configuration conf = new Configuration();
    if (_hadoopConfig.isEmpty()) {
      conf.addResource(new Path(_hadoopConfDirectory + File.separator + "core-site.xml"));
      conf.addResource(new Path(_hadoopConfDirectory + File.separator + "hdfs-site.xml"));
    } else {
      for (Map.Entry<String, String> entry : _hadoopConfig.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    try {
      return FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void getFileFromHdfs(String hdfsPath, File dst) throws IllegalArgumentException, IOException {
    FileSystem fileSystem = this.getHadoopFileSystem();
    Path src = new Path(hdfsPath);
    fileSystem.copyToLocalFile(src, new Path(dst.getAbsolutePath()));
  }


}
