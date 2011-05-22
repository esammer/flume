package com.cloudera.flume;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class IntegrationTestSupportTest {

  private IntegrationTestSupport testSupport;

  @Before
  public void setUp() {
    testSupport = new IntegrationTestSupport();
  }

  @Test
  public void testSetUpTearDown() throws IOException {
    testSupport.setUp();
    testSupport.tearDown();
  }

}
