package com.cloudera.flume;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class IntegrationTestSupport {

  private static final Logger logger = LoggerFactory
      .getLogger(IntegrationTestSupport.class);
  private static final String tmpDirectory = "/tmp";

  private File distributionDirectory;
  private File workingDirectory;

  public IntegrationTestSupport() {
    String distProp = System.getProperty("flume.distribution.dir");

    Preconditions.checkNotNull(distProp,
        "The property flume.distribution.dir is not set");

    distributionDirectory = new File(distProp);
    workingDirectory = new File(tmpDirectory + File.separator + "flume-int-"
        + System.currentTimeMillis() + "-" + Thread.currentThread().getId());
  }

  public void setUp() throws IOException {
    logger.info("Establishing a clean Flume environment in {} from {}",
        workingDirectory, distributionDirectory);

    Preconditions.checkNotNull(distributionDirectory,
        "Flume distribution directory is not specified");
    Preconditions.checkState(distributionDirectory.isDirectory(),
        "Flume distribution directory " + distributionDirectory
            + " doesn't exist or isn't a directory");

    FileUtils.copyDirectory(distributionDirectory, workingDirectory);
  }

  public void tearDown() throws IOException {
    logger.info("Tearing down Flume integration test environment in {}",
        workingDirectory);

    FileUtils.deleteDirectory(workingDirectory);
  }

  public File getDistributionDirectory() {
    return distributionDirectory;
  }

  public void setDistributionDirectory(File distributionDirectory) {
    this.distributionDirectory = distributionDirectory;
  }

  public File getWorkingDirectory() {
    return workingDirectory;
  }

  public void setWorkingDirectory(File workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

}
