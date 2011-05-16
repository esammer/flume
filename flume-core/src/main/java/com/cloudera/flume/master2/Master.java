package com.cloudera.flume.master2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.CommandManager;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.MasterAckManager;
import com.cloudera.flume.master.MasterAdminServer;
import com.cloudera.flume.master.flows.FlowConfigManager;
import com.cloudera.flume.master.logical.LogicalConfigurationManager;
import com.cloudera.zookeeper.support.election.LeaderElectionAware;
import com.cloudera.zookeeper.support.election.LeaderElectionSupport;
import com.cloudera.zookeeper.support.election.LeaderElectionSupport.EventType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Master implements LeaderElectionAware, Watcher {

  private static final Logger logger = LoggerFactory.getLogger(Master.class);

  private static final String masterElectionGroupName = "master";
  private static final String nodeGroupName = "nodes";

  private LeaderElectionSupport electionSupport;
  private volatile boolean electedMaster;
  private ZooKeeper zooKeeper;

  private ScheduledExecutorService nodeUpdaterService;

  private MasterAdminServer adminServer;
  private CommandManager commandManager;
  private ConfigurationManager configManager;
  private MasterAckManager ackManager;

  public Master() {
    electionSupport = new LeaderElectionSupport();
    electedMaster = false;

    nodeUpdaterService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("nodeUpdater-%s").build());

    adminServer = new MasterAdminServer(this, FlumeConfiguration.get());
    commandManager = new CommandManager();
    configManager = new LogicalConfigurationManager(
        new FlowConfigManager.FailoverFlowConfigManager(new ConfigManager(),
            null), new ConfigManager(), null);
    ackManager = new MasterAckManager();
  }

  public void start() throws IOException {
    try {
      zooKeeper = new ZooKeeper("localhost", 15000, this);
    } catch (IOException e) {
      logger.error("Unable to connect to ZooKeeper. Exception follows.", e);
    }

    electionSupport.setZooKeeper(zooKeeper);
    electionSupport.setRootNodeName("/flume2/" + masterElectionGroupName);

    electionSupport.addObserver(this);

    electionSupport.start();

    /*
     * Update the node list from ZooKeeper every 10 seconds. This should
     * probably be implemented using watches, but I don't want to deal with that
     * yet.
     */
    nodeUpdaterService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          updateNodes();
        } catch (KeeperException e) {
          logger.error(
              "Unable to update Flume node list. ZooKeeper error follows.", e);
        } catch (InterruptedException e) {
          logger.error("Unable to update Flume node list. Interrupted.");
          Thread.currentThread().interrupt();
        }
      }
    }, 0, 10, TimeUnit.SECONDS);
  }

  public void stop() {
    electionSupport.stop();

    nodeUpdaterService.shutdown();

    for (int i = 1; i <= 5; i++) {
      logger.info("Waiting for node update thread to stop. Attempt {} of 5", i);
      try {
        if (nodeUpdaterService.awaitTermination(1, TimeUnit.SECONDS)) {
          break;
        }
      } catch (InterruptedException e) {
        /*
         * If we're interrupted while trying to shut things down, we just break.
         */
        break;
      }
    }

    if (!nodeUpdaterService.isTerminated()) {
      logger
          .warn("Unable to stop the node update thread in the allotted time. Giving up.");
    }
  }

  private void updateNodes() throws KeeperException, InterruptedException {
    logger.debug("Updating Flume node list from ZK");

    List<String> nodes = zooKeeper
        .getChildren("/flume2/" + nodeGroupName, true);

    logger.debug("Found {} Flume nodes registered in zookeeper", nodes.size());

    for (String node : nodes) {
      String fqNode = "/flume2/" + nodeGroupName + "/" + node;
      Stat stat = new Stat();
      byte[] nodeData = zooKeeper.getData("/flume2/" + nodeGroupName + "/"
          + node, false, stat);

      logger.debug("Node:{} stat:{} data.length:{}", new Object[] { fqNode,
          stat, nodeData.length });
    }
  }

  @Override
  public String toString() {
    return "{ electedMaster:" + electedMaster + " }";
  }

  @Override
  public void onElectionEvent(EventType eventType) {
    switch (eventType) {
    case ELECTED_COMPLETE:
      becomeMaster();
      break;
    default:
      logger.info("Non-election event {}", eventType);
      if (isElectedMaster()) {
        relinquishMaster();
      }
    }
  }

  private void becomeMaster() {
    electedMaster = true;
    logger.info("Elected master");

    try {
      configManager.start();
      commandManager.start();
      adminServer.serve();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void relinquishMaster() {
    electedMaster = false;

    try {
      adminServer.stop();
      commandManager.stop();
      configManager.stop();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public boolean isElectedMaster() {
    return electedMaster;
  }

  public void setElectedMaster(boolean electedMaster) {
    this.electedMaster = electedMaster;
  }

  @Override
  public void process(WatchedEvent event) {
    // We don't yet handle watches on flume nodes, but we should.
  }

  public MasterAdminServer getAdminServer() {
    return adminServer;
  }

  public void setAdminServer(MasterAdminServer adminServer) {
    this.adminServer = adminServer;
  }

  public CommandManager getCommandManager() {
    return commandManager;
  }

  public void setCommandManager(CommandManager commandManager) {
    this.commandManager = commandManager;
  }

  public ConfigurationManager getConfigManager() {
    return configManager;
  }

  public void setConfigManager(ConfigurationManager configManager) {
    this.configManager = configManager;
  }

  public MasterAckManager getAckManager() {
    return ackManager;
  }

  public void setAckManager(MasterAckManager ackManager) {
    this.ackManager = ackManager;
  }

}
