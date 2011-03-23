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

  private static final int testServerPort = 12345;
  private static final int testEventCount = 100;

  private AvroEventSource eventSource;
  private Logger logger;

  @Before
  public void setUp() throws IOException {
    eventSource = new AvroEventSource(testServerPort);
    logger = Logger.getLogger(Log4jAvroAppenderTest.class);

    Log4jAvroAppender avroAppender = new Log4jAvroAppender();

    avroAppender.setName("avro");
    avroAppender.setHostname("localhost");
    avroAppender.setPort(testServerPort);

    /*
     * Clear out all other appenders associated with this logger to ensure we're
     * only hitting the Avro appender. -esammer
     */
    logger.removeAllAppenders();
    logger.addAppender(avroAppender);
    logger.setLevel(Level.ALL);

    eventSource.open();
  }

  @After
  public void tearDown() throws IOException {
    eventSource.close();
  }

  @Test
  public void testLog4jAvroAppender() {
    Assert.assertNotNull(logger);

    int loggedCount = 0;
    int receivedCount = 0;

    for (int i = 0; i < testEventCount; i++) {
      logger.info("test i:" + i);
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

    Assert.assertEquals(loggedCount, receivedCount);
  }

}
