package com.cloudera.flume.master2;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
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

public class Master implements LeaderElectionAware, Watcher {

  private static final Logger logger = LoggerFactory.getLogger(Master.class);

  private static final String masterElectionGroupName = "master";
  private static final String nodeGroupName = "nodes";

  private LeaderElectionSupport electionSupport;
  private volatile boolean electedMaster;
  private ZooKeeper zooKeeper;

  private MasterAdminServer adminServer;
  private CommandManager commandManager;
  private ConfigurationManager configManager;
  private MasterAckManager ackManager;
  private NodeStatusManager nodeStatusManager;

  public Master() {
    electionSupport = new LeaderElectionSupport();
    electedMaster = false;

    adminServer = new MasterAdminServer(this, FlumeConfiguration.get());
    commandManager = new CommandManager();
    configManager = new LogicalConfigurationManager(
        new FlowConfigManager.FailoverFlowConfigManager(new ConfigManager(),
            null), new ConfigManager(), null);
    ackManager = new MasterAckManager();
    nodeStatusManager = new NodeStatusManager();
  }

  public void start() throws IOException {
    try {
      zooKeeper = new ZooKeeper("localhost", 15000, this);
    } catch (IOException e) {
      logger.error("Unable to connect to ZooKeeper. Exception follows.", e);
    }

    nodeStatusManager.setZooKeeper(zooKeeper);
    nodeStatusManager.setNodeGroupName("/flume2/" + nodeGroupName);

    electionSupport.setZooKeeper(zooKeeper);
    electionSupport.setRootNodeName("/flume2/" + masterElectionGroupName);

    electionSupport.addObserver(this);

    nodeStatusManager.start();
    electionSupport.start();
  }

  public void stop() {
    logger.info("Master stopping");

    electionSupport.stop();
    nodeStatusManager.stop();
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

    // FIXME: This should cause master failure.
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
      configManager.stop();
      commandManager.stop();
      adminServer.stop();
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

  public NodeStatusManager getNodeStatusManager() {
    return nodeStatusManager;
  }

  public void setNodeStatusManager(NodeStatusManager nodeStatusManager) {
    this.nodeStatusManager = nodeStatusManager;
  }

}
