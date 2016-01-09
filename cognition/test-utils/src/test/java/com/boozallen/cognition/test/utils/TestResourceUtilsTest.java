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
import org.junit.Test;

import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;

import static com.boozallen.cognition.test.utils.TestResourceUtils.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class TestResourceUtilsTest {

  public static final String TEST_FILE_TEXT = "test file text";

  @Test
  public void testGetResourcePath() throws Exception {
    assertThat(getResourcePath(this.getClass(), "test-file.txt"),
        is("/com/boozallen/cognition/test/utils/TestResourceUtilsTest/test-file.txt"));
  }

  @Test
  public void testGetResource() throws Exception {
    URL resourceUrl = getResource(this.getClass(), "test-file.txt");
    try (InputStream stream = resourceUrl.openStream()) {
      StringWriter stringWriter = new StringWriter();
      IOUtils.copy(stream, stringWriter);
      assertThat(stringWriter.toString(), is(TEST_FILE_TEXT));
    }
  }

  @Test
  public void testGetResourceAsStream() throws Exception {
    try (InputStream stream = getResourceAsStream(this.getClass(), "test-file.txt")) {
      StringWriter stringWriter = new StringWriter();
      IOUtils.copy(stream, stringWriter);
      assertThat(stringWriter.toString(), is(TEST_FILE_TEXT));
    }
  }

  @Test
  public void testGetResourceAsString() throws Exception {
    assertThat(getResourceAsString(this.getClass(), "test-file.txt"), is(TEST_FILE_TEXT));
  }
}