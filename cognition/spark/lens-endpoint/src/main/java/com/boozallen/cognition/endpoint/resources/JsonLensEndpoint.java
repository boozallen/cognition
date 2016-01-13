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

package com.boozallen.cognition.endpoint.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource class used to implement service methods and associate the
 * appropriate URI template.
 * 
 */
@Path("/lensJson")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class JsonLensEndpoint extends LensEndpoint{

	final static Logger LOGGER = LoggerFactory.getLogger(JsonLensEndpoint.class);

	private final static String AND_DEFAULT_OPERATOR = "&";

	/**
	 * Executes a search query utilizing all sources/indexes.
	 * Required fields/parameters: startDate and endDate
	 * 
	 * @return the search results, in JSON format, that match the query
	 */
	@GET
	@Path("/_search")
    public String jsonQuery(@QueryParam("q") String q, String sourceIn){
		
		// initialize query properties
		String startDate = null;
		String endDate = null;
		String user = null;
		String language = null;
		String country = null;
		Integer limit = null;
		String source = null;
		List<String> keywords = new ArrayList<String>();
		
		q = convertDelim(q);
		String[] query = q.split(AND_DEFAULT_OPERATOR);
		List<String> paramStrings = new ArrayList<String>(Arrays.asList(query));
		
		for(String paramSplit : paramStrings){
			String[] param = paramSplit.split(":", 2);
			
			if(!param[0].isEmpty()){
				switch(param[0]){
					case "startDate":
						startDate = param[1].toLowerCase(); break;
					case "endDate":
						endDate = param[1].toLowerCase(); break;
					case "user":
						user = param[1].toLowerCase(); break;
					case "language":
						language = param[1].toLowerCase(); break;
					case "country":
						country = param[1].toLowerCase(); break;
					case "keywords":
						keywords.add(param[1].toLowerCase()); break;
					case "limit": 
						limit = Integer.parseInt(param[1]); break;
					case "source":
						source = param[1].toLowerCase(); break;
				}
			}
		}
		
		if(sourceIn != null && !sourceIn.isEmpty()){
			source = sourceIn;
		}
		
		return this.query(user, keywords, language, country, startDate, endDate, source, true, limit);
	}
	
	/**
	 * Executes a search query utilizing the {source} source/index.
	 * Required fields/parameters: Start and end date.
	 * 
	 * @return the search results, in JSON format, that match the query
	 */
	
	@GET
	@Path("/{source}/_search")
    public String querySource(@QueryParam("q") String q, @PathParam("source") String source){
		return jsonQuery(q,source);
	}
	
	// converts operator to default value before processing
	private String convertDelim(String queryString){
		String q = queryString;
		q = q.replaceAll("AND", AND_DEFAULT_OPERATOR);
		q = q.replaceAll(" AND ", AND_DEFAULT_OPERATOR);
		
		return q;
	}
	
}
