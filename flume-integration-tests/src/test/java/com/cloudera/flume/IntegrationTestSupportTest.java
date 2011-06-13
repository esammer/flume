package com.cloudera.flume;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestSupportTest {

  private static final Logger logger = LoggerFactory
      .getLogger(IntegrationTestSupportTest.class);

  private File distDir;

  @Before
  public void setUp() {
    File distParentDir = new File("target/flume-distribution")
        .getAbsoluteFile();
    distDir = new File(distParentDir, distParentDir.list()[0]);

    logger.debug("Found distParentDir:{} distDir:{}", distParentDir, distDir);

  }

  private IntegrationTestSupport createTestSupport() {
    IntegrationTestSupport testSupport = new IntegrationTestSupport();
    testSupport.setDistributionDirectory(distDir);

    return testSupport;
  }

  @Test
  public void testSetUpTearDown() throws IOException {
    IntegrationTestSupport testSupport = createTestSupport();

    testSupport.setUp();

    Assert.assertTrue("Working directory doesn't exist or isn't a directory - "
        + testSupport.getWorkingDirectory(), testSupport.getWorkingDirectory()
        .isDirectory());

    for (String entry : testSupport.getWorkingDirectory().list()) {
      logger.debug("entry:{}", entry);
    }

    testSupport.tearDown();

    Assert
        .assertFalse(
            "Working directory still exists - "
                + testSupport.getWorkingDirectory(), testSupport
                .getWorkingDirectory().exists());

  }

  @Test
  public void testMultipleCopies() throws IOException {
    IntegrationTestSupport[] testSupporters = new IntegrationTestSupport[2];

    for (int i = 0; i < 2; i++) {
      testSupporters[i] = createTestSupport();
      testSupporters[i].setDistributionDirectory(distDir);
      testSupporters[i].setUp();

      Assert.assertTrue(
          "Working directory doesn't exist or isn't a directory - "
              + testSupporters[i].getWorkingDirectory(), testSupporters[i]
              .getWorkingDirectory().isDirectory());
    }

    Assert.assertNotSame(
        "Multiple test supporters have the same working directories",
        testSupporters[0].getWorkingDirectory(),
        testSupporters[1].getWorkingDirectory());

    for (int i = 0; i < 2; i++) {
      testSupporters[i].tearDown();

      Assert.assertFalse("Working directory still exists - "
          + testSupporters[i].getWorkingDirectory(), testSupporters[i]
          .getWorkingDirectory().exists());

    }
  }

}
