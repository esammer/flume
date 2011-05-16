package com.cloudera.flume.master2;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMaster {

  private static final Logger logger = LoggerFactory
      .getLogger(TestMaster.class);

  private Master createMaster() {
    Master master = new Master();

    return master;
  }

  @Test
  public void testRun() throws IOException, KeeperException,
      InterruptedException {
    Master master = createMaster();

    Assert.assertFalse(master.isElectedMaster());

    master.start();

    /*
     * We want to ensure that a single master eventually becomes the leader in a
     * reasonable amount of time.
     */
    for (int i = 0; i < 10; i++) {
      if (master.isElectedMaster()) {
        logger.info("Elected master in {} seconds", i);
        break;
      } else {
        logger.info("Not yet elected master...");
        Thread.sleep(1000);
      }
    }

    Assert.assertTrue("Not elected master within a reasonable time limit",
        master.isElectedMaster());

    master.stop();

    for (int i = 0; i < 10; i++) {
      if (!master.isElectedMaster()) {
        logger.info("Relinquished master status in {} seconds", i);
        break;
      } else {
        logger.info("Still holding master...");
        Thread.sleep(1000);
      }
    }

    Assert.assertFalse(
        "Did not relinquish master status within a reasonable time limit",
        master.isElectedMaster());
  }

  @Test
  public void testElection() throws InterruptedException {
    int masters = 10;
    final CountDownLatch latch = new CountDownLatch(masters);

    for (int i = 0; i < masters; i++) {
      final Master master = createMaster();

      new Thread() {

        @Override
        public void run() {
          try {
            master.start();
            Thread.sleep(3000);
          } catch (InterruptedException e) {
          } catch (IOException e) {
          }

          master.stop();

          latch.countDown();
        }
      }.start();
    }

    Assert.assertTrue(
        "Timed out waiting for masters to start, run, and stop cleanly",
        latch.await(10, TimeUnit.SECONDS));
  }

}
