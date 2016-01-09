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

package com.boozallen.cognition.test.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Utility methods for getting test resources from class path. Test resources should be organized by class name. For
 * example, resource for TestClass should be placed as /TestClass/resource_file.txt
 *
 * @author bentse
 */
public class TestResourceUtils {
  static String getResourcePath(Class<?> testClass, String resource) {
    String canonicalName = testClass.getCanonicalName();
    String folder = canonicalName.replaceAll("\\.", "/");
    return String.format("/%s/%s", folder, resource);
  }

  public static URL getResource(Class<?> testClass, String resource) {
    return testClass.getResource(getResourcePath(testClass, resource));
  }

  public static InputStream getResourceAsStream(Class<?> testClass, String resource) {
    return testClass.getResourceAsStream(getResourcePath(testClass, resource));
  }

  public static String getResourceAsString(Class<?> testClass, String resource) throws IOException {
    return IOUtils.toString(getResourceAsStream(testClass, resource));
  }
}
