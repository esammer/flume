package com.cloudera.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.Assert;
import org.junit.Test;

public class PathManagerTest {

  private static final Path testBaseDirectory = new Path(
      "/tmp/flume-test-pathmanager");

  private FileSystem fileSystem;

  public PathManagerTest() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
  }

  @Test
  public void testStateTransitions() throws IOException {
    PathManager pathManager = new PathManager(fileSystem, testBaseDirectory,
        "test1");

    Assert.assertNotNull(pathManager);

    OutputStream outputStream = null;

    Assert.assertFalse(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertFalse(fileSystem.exists(pathManager.getClosedPath()));

    try {
      outputStream = pathManager.open();
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(outputStream);
    Assert.assertTrue(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertFalse(fileSystem.exists(pathManager.getClosedPath()));

    boolean success = false;

    try {
      outputStream.close();
      success = pathManager.close();
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue(success);
    Assert.assertFalse(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertTrue(fileSystem.exists(pathManager.getClosedPath()));

    Assert.assertTrue(fileSystem.delete(testBaseDirectory, true));
  }

  @Test(expected = IllegalStateException.class)
  public void testTransitionNewToClose() throws IOException {
    PathManager pathManager = new PathManager(fileSystem, testBaseDirectory,
        "test1");

    Assert.assertNotNull(pathManager);

    Assert.assertFalse(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertFalse(fileSystem.exists(pathManager.getClosedPath()));

    boolean success = false;

    success = pathManager.close();

    // We shouldn't get here.
    Assert.assertFalse(success);
    Assert.fail("Did not encounter the expected exception.");
  }

  @Test
  public void testCompressedFile() throws IOException {
    String testContent = "I am a simple test message\n";

    PathManager pathManager = new PathManager(fileSystem, testBaseDirectory,
        "test3.gz");

    Assert.assertNotNull(pathManager);

    Assert.assertFalse(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertFalse(fileSystem.exists(pathManager.getClosedPath()));

    OutputStream outputStream = null;

    try {
      outputStream = pathManager.open();

      Assert.assertNotNull(outputStream);
      Assert.assertTrue(fileSystem.exists(pathManager.getOpenPath()));
      Assert.assertFalse(fileSystem.exists(pathManager.getClosedPath()));

      outputStream = new CompressionCodecFactory(new Configuration()).getCodec(
          new Path(pathManager.getFileName())).createOutputStream(outputStream);

      Assert.assertNotNull(outputStream);

      outputStream.write(testContent.getBytes());
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }

    boolean success = false;

    success = pathManager.close();

    Assert.assertTrue(success);
    Assert.assertFalse(fileSystem.exists(pathManager.getOpenPath()));
    Assert.assertTrue(fileSystem.exists(pathManager.getClosedPath()));

    InputStream inputStream = null;

    try {
      inputStream = new CompressionCodecFactory(new Configuration()).getCodec(
          pathManager.getClosedPath()).createInputStream(
          fileSystem.open(pathManager.getClosedPath()));

      byte[] buffer = new byte[512];
      int length = inputStream.read(buffer);

      Assert.assertTrue(length > 0);

      String content = new String(buffer, 0, length);

      Assert.assertEquals(testContent, content);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    Assert.assertTrue(fileSystem.delete(testBaseDirectory, true));
  }

}
