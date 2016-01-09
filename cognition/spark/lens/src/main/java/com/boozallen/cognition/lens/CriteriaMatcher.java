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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Class to help find the matches based on the given criteria
 * @author mparker
 *
 */
public class CriteriaMatcher implements Serializable {

  private static final long serialVersionUID = -7544744172598921447L;

  /**
   * Collects the options for the match from accumulo
   * @param criteria -- the criteria object
   * @param field -- the specified field
   * @param observed -- the accumulo row
   * @return the corresponding values to match on
   */
  public static Set<String> getObservedValues(Criteria criteria, Field field, Map<Tuple2<String, String>, String> observed) {
    Set<String> retVal = new HashSet<>();
    for (Column column : criteria.getSchema().getColumns(field, criteria.getSource())) {
      String value = observed.get(new Tuple2<String, String>(column.getColumnFamily().toString(),
          column.getColumnQualifier().toString()));
      if (value != null) {
        if (!criteria.isCaseSensitive()) {
          value = value.toLowerCase();
        }
        retVal.add(value);
      }
    }
    return retVal;
  }

  /**
   * Evaluate if the given value is in the set of accumulo values
   * @param value -- the specified value to filter on
   * @param observedValues -- the values from accumulo (could be multiple)
   * @return true if the criteria value matches accumulo, false otherwise
   */
  public static boolean checkField(String value, Set<String> observedValues) {
    if (observedValues.contains(value)) {
      return true;
    }
    return false;
  }

  /**
   * Evaluate if the row atches all of the criteria
   * @param criteria -- the criteria for filtering 
   * @param observedValues -- the values from accumulo
   * @return true if the values in accumulo match all of the criteria, false otherwise
   */
  public static boolean evaluate(Criteria criteria, Map<Tuple2<String, String>, String> observedValues) {
    for (Map.Entry<Field, String> crit : criteria.getStringMatches().entrySet()) {
      if (!checkField(crit.getValue(), getObservedValues(criteria, crit.getKey(), observedValues))) {
        return false;
      }//else continue down the list of criteria
    }

    //keyword part
    if (criteria.getKeywords().size() > 0) {
      Set<String> observed = getObservedValues(criteria, Field.KEYWORD, observedValues);
      if (observed.size() > 0) {
        observed = cleanValues(observed);
        for (String criteriaKeyword : criteria.getKeywords()) {
          for (String value : observed) {
            if (value.contains(criteriaKeyword)) {
              return true;
            }
          }
        }
      }
    } else {
      return true;
    }
    return false;

  }

  private static Set<String> cleanValues(Set<String> observed) {
    //wrap in spaces avoids costly tokenization
    Set<String> retVal = new HashSet<>();
    for (String x : observed) {
      retVal.add(" " + x.toLowerCase() + " ");
    }
    return retVal;
  }

}
