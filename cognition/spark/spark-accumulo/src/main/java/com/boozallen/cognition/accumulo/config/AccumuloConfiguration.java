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

package com.boozallen.cognition.accumulo.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * Wraps all configuration for accumulo and gives convenience methods
 * for accumulo. See accumulo documentation for full details.
 * 
 * @author mparker
 */
public class AccumuloConfiguration implements Serializable {
  private static final long serialVersionUID = 1L;
  private ClientConfiguration clientConfig;
  protected Job job;
  private Connector conn;
  private Instance zkInstance;
  private String accumuloUser;
  private String accumuloPassword; //Not secure

	/*public AccumuloConfiguration(){}*/

  public AccumuloConfiguration(AccumuloConfiguration config) {
    this.job = config.job;
    this.clientConfig = config.clientConfig;
  }

  public AccumuloConfiguration(String zkInstanceName, String zkHosts,
                               String accumuloUser, String accumuloPassword) throws AccumuloSecurityException, IOException {
    this(new ZooKeeperInstance(zkInstanceName,zkHosts),accumuloUser,accumuloPassword,false);
  }

  public AccumuloConfiguration(Instance instance, String accumuloUser, String accumuloPassword,
                               boolean isMock) throws AccumuloSecurityException, IOException {
    //NOTE: new Job(new Configuration()) does not work in scala shell due to the toString method's implementation
    //to get it to work in scala override the toString method and it will work
    
    //initialize fields, these are needed for lazy initialization of connector
    this.zkInstance = instance;
    this.accumuloUser = accumuloUser;
    this.accumuloPassword = accumuloPassword;
    
    this.job = new Job(new Configuration());
    AbstractInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
    AccumuloOutputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));

    AbstractInputFormat.setScanAuthorizations(job, new Authorizations());

    if (isMock) {
      AbstractInputFormat.setMockInstance(job, instance.getInstanceName());
      AccumuloOutputFormat.setMockInstance(job, instance.getInstanceName());
    } else {

      this.clientConfig = new ClientConfiguration();
      this.clientConfig.withInstance(instance.getInstanceName());
      this.clientConfig.withZkHosts(instance.getZooKeepers());

      AbstractInputFormat.setZooKeeperInstance(job, clientConfig);
      AccumuloOutputFormat.setZooKeeperInstance(job, this.clientConfig);
    }
  }

  public void setRanges(Collection<Range> ranges) {
    InputFormatBase.setRanges(job, ranges);
  }

  public void setAuthorizations(String auths) {
    if (auths == null || auths.isEmpty()) {
      AbstractInputFormat.setScanAuthorizations(job, Authorizations.EMPTY);
    } else {
      AbstractInputFormat.setScanAuthorizations(job, new Authorizations(auths));
    }
  }

  public void setTableName(String tableName) {
    InputFormatBase.setInputTableName(job, tableName);
  }

  public void addIterator(IteratorSetting is) {
    InputFormatBase.addIterator(job, is);
  }

  public Configuration getConfiguration() {
    return job.getConfiguration();
  }

  public Job getJob() {
    return job;
  }

  public void setOfflineScanner(String tableName) {
		/*		int index = 0;
		String clonedTableName = tableName + "_clone_" + index;
		while(conn.tableOperations().exists(clonedTableName)){
			clonedTableName = tableName + "_clone_" + index;
			index++;
		}
		try {
			conn.tableOperations().clone(tableName, clonedTableName, true, null, null);
			conn.tableOperations().offline(clonedTableName);
			setTableName(clonedTableName);*/
    InputFormatBase.setOfflineTableScan(job, true);
		/*		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException | TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

  }

  public void fetchColumns(Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
    InputFormatBase.fetchColumns(job, columnFamilyColumnQualifierPairs);
  }
  
  public Connector getConnector(){
    //lazy initialize
    try {
      conn = zkInstance.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
    } catch (AccumuloException | AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

}
