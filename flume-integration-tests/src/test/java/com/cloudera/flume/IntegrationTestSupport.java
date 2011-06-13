package com.cloudera.flume;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

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

  public void setUp() throws IOException {
    workingDirectory = new File(tmpDirectory + File.separator + "flume-int-"
        + System.currentTimeMillis() + "-" + Thread.currentThread().getId());

    logger.info("Establishing a clean Flume environment in {} from {}",
        workingDirectory, distributionDirectory);

    Preconditions.checkNotNull(distributionDirectory,
        "Flume distribution directory is not specified");
    Preconditions.checkState(distributionDirectory.isDirectory(),
        "Flume distribution directory " + distributionDirectory
            + " doesn't exist or isn't a directory");

    try {
      copy(distributionDirectory, workingDirectory);
    } catch (InterruptedException e) {
    }
  }

  public void tearDown() throws IOException {
    logger.info("Tearing down Flume integration test environment in {}",
        workingDirectory);

    if (workingDirectory != null) {
      FileUtils.deleteDirectory(workingDirectory);
    }
  }

  public static void copy(File source, File destination) throws IOException,
      InterruptedException {
    Process process = new ProcessBuilder("cp", "-rp", source.getPath(),
        destination.getPath()).start();

    BufferedReader inputReader = new BufferedReader(new InputStreamReader(
        process.getErrorStream()));

    StringBuilder errorText = new StringBuilder();
    String line = inputReader.readLine();

    while (line != null) {
      line = inputReader.readLine();
      errorText.append(line);
    }

    if (process.waitFor() != 0) {
      throw new IOException("Copy from " + source + " to " + destination
          + " failed - " + errorText.toString());
    }
  }

  @Override
  public String toString() {
    return String.format("{ workingDirectory:%s distributionDirectory:%s }",
        workingDirectory, distributionDirectory);
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
