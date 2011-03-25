package com.cloudera.flume.handler.log4j;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.avro.AvroEventSource;

public class Log4jAvroAppenderTest {

  private static final Logger logger = Logger
      .getLogger(Log4jAvroAppenderTest.class);

  private static final int testServerPort = 12345;
  private static final int testEventCount = 100;

  private AvroEventSource eventSource;
  private Logger avroLogger;

  @Before
  public void setUp() throws IOException {
    eventSource = new AvroEventSource(testServerPort);
    avroLogger = Logger.getLogger("avrologger");

    Log4jAvroAppender avroAppender = new Log4jAvroAppender();

    avroAppender.setName("avro");
    avroAppender.setHostname("localhost");
    avroAppender.setPort(testServerPort);
    avroAppender.setReconnectAttempts(3);

    /*
     * Clear out all other appenders associated with this logger to ensure we're
     * only hitting the Avro appender. -esammer
     */
    avroLogger.removeAllAppenders();
    avroLogger.addAppender(avroAppender);
    avroLogger.setLevel(Level.ALL);

    eventSource.open();
  }

  @After
  public void tearDown() throws IOException {
    eventSource.close();
  }

  @Test
  public void testLog4jAvroAppender() throws InterruptedException {
    Assert.assertNotNull(avroLogger);

    int loggedCount = 0;
    int receivedCount = 0;

    for (int i = 0; i < testEventCount; i++) {
      avroLogger.info("test i:" + i);
      loggedCount++;
    }

    /*
     * We perform this in another thread so we can put a time SLA on it by using
     * Future#get(). Internally, the AvroEventSource uses a BlockingQueue.
     * -esammer
     */
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Callable<Event> callable = new Callable<Event>() {

      @Override
      public Event call() throws Exception {
        return eventSource.next();
      }
    };

    for (int i = 0; i < loggedCount; i++) {
      try {
        Future<Event> future = executor.submit(callable);

        /*
         * We must receive events in less than 1 second. This should be more
         * than enough as all events should be held in AvroEventSource's
         * BlockingQueue. -esammer
         */
        Event event = future.get(1, TimeUnit.SECONDS);

        Assert.assertNotNull(event);
        Assert.assertNotNull(event.getBody());
        Assert.assertEquals("test i:" + i, new String(event.getBody()));

        receivedCount++;
      } catch (ExecutionException e) {
        Assert.fail("Flume failed to handle an event: " + e.getMessage());
        break;
      } catch (TimeoutException e) {
        Assert
            .fail("Flume failed to handle an event within the given time SLA: "
                + e.getMessage());
        break;
      } catch (InterruptedException e) {
        Assert
            .fail("Flume source executor thread was interrupted. We count this as a failure.");
        Thread.currentThread().interrupt();
        break;
      }
    }

    executor.shutdown();

    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
      throw new IllegalStateException(
          "Executor is refusing to shutdown cleanly");
    }

    Assert.assertEquals(loggedCount, receivedCount);
  }

  @Test
  public void testConnectionRefused() {
    ((Log4jAvroAppender) avroLogger.getAppender("avro")).setPort(44000);

    boolean caughtException = false;

    try {
      avroLogger.info("message 1");
    } catch (Throwable t) {
      logger.debug("Logging to a non-existant server failed (as expected)", t);

      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }

  @Test
  public void testReconnect() throws IOException {
    avroLogger.info("message 1");

    Event event = eventSource.next();

    Assert.assertNotNull(event);
    Assert.assertEquals("message 1", new String(event.getBody()));

    eventSource.close();

    Callable<Void> logCallable = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        avroLogger.info("message 2");
        return null;
      }
    };

    ExecutorService logExecutor = Executors.newSingleThreadExecutor();

    boolean caughtException = false;

    try {
      logExecutor.submit(logCallable);

      Thread.sleep(1500);

      eventSource.open();

      logExecutor.shutdown();

      if (!logExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
            "Log executor is refusing to shutdown cleanly");
      }
    } catch (Throwable t) {
      logger.error(
          "Failed to reestablish a connection and log to an avroSource", t);

      caughtException = true;
    }

    Assert.assertFalse(caughtException);

    event = eventSource.next();

    Assert.assertNotNull(event);
    Assert.assertEquals("message 2", new String(event.getBody()));

    caughtException = false;

    try {
      avroLogger.info("message 3");
    } catch (Throwable t) {
      logger.debug("Logging to a closed server failed (not expected)", t);

      caughtException = true;
    }

    Assert.assertFalse(caughtException);

    event = eventSource.next();

    Assert.assertNotNull(event);
    Assert.assertEquals("message 3", new String(event.getBody()));
  }
}
