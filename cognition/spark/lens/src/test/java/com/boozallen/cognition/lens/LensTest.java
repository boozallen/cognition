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

package com.boozallen.cognition.lens;

import com.boozallen.cognition.accumulo.config.AccumuloConfiguration;
import com.boozallen.cognition.accumulo.config.CognitionConfiguration;
import com.boozallen.cognition.accumulo.structure.Source;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class LensTest {
  //Note: always give the instance a name, otherwise it won't be stored in the global instances and unexpected behavior can occur
  private static Instance instance = new MockInstance("test");
  private static final String user = "root";
  private static final String password = "";


  @BeforeClass
  public static void init() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
    Connector conn = instance.getConnector("root", new PasswordToken());
    conn.tableOperations().create("moreover");

    Mutation m = new Mutation("MOREOVER_0_1445332752000_4c414d8abfc5ae6ec38bb4dd200b535376db4724");
    m.put("data", "adultLanguage", new Value("false".getBytes()));
    m.put("data", "author.dateLastActive", new Value("".getBytes()));
    m.put("data", "author.description", new Value("".getBytes()));
    m.put("data", "author.email", new Value("".getBytes()));
    m.put("data", "author.homeUrl", new Value("".getBytes()));
    m.put("data", "author.name", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.followersCount", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.followingCount", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.kloutScore", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.statusesCount", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.totalViews", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.userId", new Value("".getBytes()));
    m.put("data", "author.publishingPlatform.userName", new Value("".getBytes()));

    m.put("data", "cognition.esjson", new Value("moreover json".getBytes()));
    m.put("data", "cognition.location", new Value("".getBytes()));
    m.put("data", "cognition.location.coordinates", new Value("[-105.64453,54.77535]".getBytes()));
    m.put("data", "cognition.location.country", new Value("".getBytes()));
    m.put("data", "cognition.location.fieldName", new Value("source.location.subregion".getBytes()));
    m.put("data", "commentsUrl", new Value("".getBytes()));
    m.put("data", "companies.company", new Value("json".getBytes()));
    m.put("data", "content", new Value(" test ".getBytes()));
    m.put("data", "copyright", new Value("".getBytes()));
    m.put("data", "dataFormat", new Value("text".getBytes()));
    m.put("data", "duplicateGroupId", new Value("23074043729".getBytes()));
    m.put("data", "embargoDate", new Value("".getBytes()));
    m.put("data", "geo.coordinates", new Value("".getBytes()));
    m.put("data", "harvestDate", new Value("2015-10-20T12:00:24Z".getBytes()));
    m.put("data", "id", new Value("23084260304".getBytes()));
    m.put("data", "language", new Value("English".getBytes()));
    m.put("data", "licenseEndDate", new Value("".getBytes()));
    m.put("data", "licenses.license.name", new Value("Publicly Available".getBytes()));
    m.put("data", "loginStatus", new Value("".getBytes()));
    m.put("data", "media", new Value("".getBytes()));
    m.put("data", "outboundUrls", new Value("".getBytes()));
    m.put("data", "publishedDate", new Value("2015-10-20T09:19:12Z".getBytes()));
    m.put("data", "publishingPlatform.inReplyToStatusId", new Value("".getBytes()));
    m.put("data", "publishingPlatform.inReplyToUserId", new Value("".getBytes()));
    m.put("data", "publishingPlatform.itemId", new Value("".getBytes()));
    m.put("data", "publishingPlatform.itemType", new Value("".getBytes()));
    m.put("data", "publishingPlatform.originalItemId", new Value("".getBytes()));
    m.put("data", "publishingPlatform.shareCount", new Value("".getBytes()));
    m.put("data", "publishingPlatform.statusId", new Value("".getBytes()));
    m.put("data", "publishingPlatform.totalViews", new Value("".getBytes()));
    m.put("data", "publishingPlatform.userMentions", new Value("".getBytes()));
    m.put("data", "sequenceId", new Value("518755760135".getBytes()));
    m.put("data", "source.category", new Value("National".getBytes()));
    m.put("data", "source.editorialRank", new Value("1".getBytes()));
    m.put("data", "source.feed.autoTopics", new Value("".getBytes()));
    m.put("data", "source.feed.copyright", new Value("".getBytes()));
    m.put("data", "source.feed.dataFormat", new Value("text".getBytes()));
    m.put("data", "source.feed.description", new Value("".getBytes()));
    m.put("data", "source.feed.editorialTopics.editorialTopic", new Value("[\"Standard\",\"News\",\"Geographic\",\"National\"]".getBytes()));
    m.put("data", "source.feed.generator", new Value("".getBytes()));
    m.put("data", "source.feed.genre", new Value("General".getBytes()));
    m.put("data", "source.feed.id", new Value("169587646".getBytes()));
    m.put("data", "source.feed.idFromPublisher", new Value("".getBytes()));
    m.put("data", "source.feed.imageUrl", new Value("".getBytes()));
    m.put("data", "source.feed.inWhiteList", new Value("true".getBytes()));
    m.put("data", "source.feed.language", new Value("Unassigned".getBytes()));
    m.put("data", "source.feed.mediaType", new Value("News".getBytes()));
    m.put("data", "source.feed.name", new Value("Reuters".getBytes()));
    m.put("data", "source.feed.publishingPlatform", new Value("".getBytes()));
    m.put("data", "source.feed.rank.autoRank", new Value("".getBytes()));
    m.put("data", "source.feed.rank.autoRankOrder", new Value("".getBytes()));
    m.put("data", "source.feed.rank.inboundLinkCount", new Value("117521".getBytes()));
    m.put("data", "source.feed.tags", new Value("".getBytes()));
    m.put("data", "source.homeUrl", new Value("http://www.reuters.com/".getBytes()));
    m.put("data", "source.location.country", new Value("United States".getBytes()));
    m.put("data", "source.location.countryCode", new Value("US".getBytes()));
    m.put("data", "source.location.region", new Value("Americas".getBytes()));
    m.put("data", "source.location.state", new Value("".getBytes()));
    m.put("data", "source.location.subregion", new Value("Northern America".getBytes()));
    m.put("data", "source.location.zipArea", new Value("".getBytes()));
    m.put("data", "source.location.zipCode", new Value("".getBytes()));
    m.put("data", "source.name", new Value("Reuters".getBytes()));
    m.put("data", "source.primaryLanguage", new Value("".getBytes()));
    m.put("data", "source.primaryMediaType", new Value("".getBytes()));
    m.put("data", "source.publisher", new Value("Thomson Reuters".getBytes()));
    m.put("data", "tags", new Value("".getBytes()));
    m.put("data", "title", new Value("REFILE-RPT-Beer mega merger faces jobs battle in SABMillers birthplace".getBytes()));
    m.put("data", "topics.topic", new Value("json".getBytes()));
    m.put("data", "url", new Value("http://ct.moreover.com/?a=23084260304&p=1pp&v=1&x=eQAcm4lV02nW9ilGQIa0sg".getBytes()));
    m.put("metadata", "cognition.dataType", new Value("moreover".getBytes()));
    m.put("metadata", "sha1_checksum", new Value("4c414d8abfc5ae6ec38bb4dd200b535376db4724".getBytes()));


    BatchWriter writer = conn.createBatchWriter("moreover", new BatchWriterConfig());
    writer.addMutation(m);
  }


  @Test
  public void test() throws AccumuloSecurityException, IOException, AccumuloException, TableExistsException, TableNotFoundException {

		/*Connector conn = instance.getConnector("root", new PasswordToken());
		Scanner scan = conn.createScanner("moreover", Authorizations.EMPTY);
		for(Map.Entry<Key, Value> entry : scan){
			System.out.println(entry);
		}*/

    SparkConf conf = new SparkConf();

    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.setAppName("test");
    conf.setMaster("local[2]");

    SparkContext sc = new SparkContext(conf);

    CognitionConfiguration pip = new CognitionConfiguration(new AccumuloConfiguration(instance, user, password, true));
    LensAPI lens = new LensAPI(sc, pip);
    Criteria criteria = new Criteria();
    criteria.addKeyword("test");
    criteria.setDates(Instant.parse("2015-10-20T09:19:12Z"), Instant.parse("2015-10-20T09:19:13Z"));
    SchemaAdapter s = new SchemaAdapter();
    s.loadJson("moreover-schema.json");
    criteria.setSource(Source.MOREOVER);
    criteria.setSchema(s);
    criteria.setAccumuloTable("moreover");
    String json = lens.query(criteria);
    assertEquals("[moreover json]", json);
  }

}
