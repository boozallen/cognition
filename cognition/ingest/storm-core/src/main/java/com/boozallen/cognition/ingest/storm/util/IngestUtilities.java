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

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to normalize tuple fields.
 */
public final class IngestUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtilities.class);

  // ingest constants
  public static final String METADATA = "metadata";
  public static final String RECORD = "record";

  private static final int BYTE = 256;
  private static final String REGEX_MATCHES = "\\[]^+.|";

  private IngestUtilities() {
  }

  public static char getDelimiterByName(String delimiter) {
    if (delimiter == null || delimiter.equalsIgnoreCase("NULL")) {
      return '\0';
    } else if (delimiter.equalsIgnoreCase("SPACE")) {
      return ' ';
    } else if (delimiter.equalsIgnoreCase("TAB")) {
      return '\t';
    } else if (delimiter.equalsIgnoreCase("NEWLINE")) {
      return '\n';
    } else {
      return delimiter.charAt(0);
    }
  }

  public static String escapeDelimiter(char delimiter) {
    String delimString = Character.toString(delimiter);
    if (REGEX_MATCHES.contains(delimString)) {
      delimString = "\\" + delimString;
    }
    return delimString;
  }

  public static String removeUnprintableCharacters(String string) {
    return string.replaceAll("[\\u007f-\\u009d]", "");
  }

  public static long ipStringToLong(String ipString) {
    // e.g. convert "223.255.246.0" to 3758093824
    String[] ipParts = ipString.split("\\.");
    if (ipParts.length != 4) {
      LOGGER.error("Expected 4 ip parts, recieved " + Integer.toString(ipParts.length));
      return 0;
    }
    return (long) (Integer.parseInt(ipParts[0]) * Math.pow(BYTE, 3) + Integer.parseInt(ipParts[1])
        * Math.pow(BYTE, 2) + Integer.parseInt(ipParts[2]) * BYTE + Integer.parseInt(ipParts[3]));
  }

  public static String ipLongToString(Long ipLong) {
    // e.g. convert 3758093824 to "223.255.246.0"
    StringBuilder ipString = new StringBuilder();
    ipString.insert(0, unsignedByteToInt(ipLong.byteValue()));
    ipLong = ipLong >> 8;
    ipString.insert(0, ".").insert(0, unsignedByteToInt(ipLong.byteValue()));
    ipLong = ipLong >> 8;
    ipString.insert(0, ".").insert(0, unsignedByteToInt(ipLong.byteValue()));
    ipLong = ipLong >> 8;
    ipString.insert(0, ".").insert(0, unsignedByteToInt(ipLong.byteValue()));

    return ipString.toString();
  }

  public static Integer unsignedByteToInt(Byte b) {
    return b & 0xFF;
  }

  public static Integer unsignedShortToInt(Short b) {
    return b & 0xFFFF;
  }

  public static long unsignedIntToLong(Integer i) {
    return i & 0xffffffffL;
  }

  public static Level levelFromString(String level) {
    String ulevel = level.toUpperCase();
    if (ulevel.equals("ALL")) {
      return Level.ALL;
    } else if (ulevel.equals("DEBUG")) {
      return Level.DEBUG;
    } else if (ulevel.equals("ERROR")) {
      return Level.ERROR;
    } else if (ulevel.equals("FATAL")) {
      return Level.FATAL;
    } else if (ulevel.equals("INFO")) {
      return Level.INFO;
    } else if (ulevel.equals("OFF")) {
      return Level.OFF;
    } else if (ulevel.equals("TRACE")) {
      return Level.TRACE;
    } else if (ulevel.equals("WARN")) {
      return Level.WARN;
    }
    return Level.INFO;
  }

  /**
   * @param inputString
   * @return inputString without the enclosing quotation marks. If no enclosing quoation marks were found, the
   * original input is returned If inputString is "", returns null
   */
  public static String removeQuotes(String inputString) {
    if (inputString.length() < 2) {
      return inputString;
    }
    if (inputString.charAt(0) == '"' &&
        inputString.charAt(inputString.length() - 1) == '"') {
      if (inputString.equals("\"\"")) {
        return null;
      }
      return inputString.substring(1, inputString.length() - 1);
    }
    return inputString;
  }

}
