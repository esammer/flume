package com.cloudera.flume.master2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The component responsible for reading and understanding Flume node status
 * from ZooKeeper.
 * 
 */
public class NodeStatusManager {

  private static final Logger logger = LoggerFactory
      .getLogger(NodeStatusManager.class);

  private ZooKeeper zooKeeper;
  private String nodeGroupName;

  private Map<String, NodeStatus> nodeStatuses;
  private ScheduledExecutorService nodeUpdaterService;

  public NodeStatusManager() {
    nodeStatuses = new HashMap<String, NodeStatus>();

    nodeUpdaterService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("nodeUpdater-%s").build());
  }

  /**
   * Start the node status management service. An update thread will run
   * periodically and invoke the {@link #update()} method.
   */
  public synchronized void start() {
    // FIXME: Make this configurable, maybe even injected.
    nodeUpdaterService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          update();
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

  /**
   * Update Flume node status from ZooKeeper. This method is called regularly by
   * an update thread, although developers may invoke it directly to force an
   * update.
   * 
   * @throws KeeperException
   * @throws InterruptedException
   */
  public synchronized void update() throws KeeperException,
      InterruptedException {

    logger.debug("Updating node status");

    List<String> nodes = zooKeeper.getChildren(nodeGroupName, true);

    logger.debug("Found {} Flume nodes registered in zookeeper", nodes.size());

    Map<String, NodeStatus> newNodeStatus = new HashMap<String, NodeStatus>();

    for (String node : nodes) {
      String fqNode = nodeGroupName + "/" + node;
      Stat stat = new Stat();
      byte[] nodeData = zooKeeper.getData(nodeGroupName + "/" + node, false,
          stat);

      logger.debug("Node:{} stat:{} data.length:{}", new Object[] { fqNode,
          stat, nodeData.length });

      NodeStatus nodeStatus = new NodeStatus();

      nodeStatus.setVersion(stat.getVersion());
      nodeStatus.setStatusLastUpdated(stat.getMtime());
      nodeStatus.setJoinedAt(stat.getCtime());
      // FIXME: Extract hostname, physicalNodeName, state from data.

      newNodeStatus.put(nodeStatus.getPhysicalNodeName(), nodeStatus);
    }

    nodeStatuses = newNodeStatus;
  }

  /**
   * Stop the node status management service. After the service is stopped, no
   * guarantees are made as to the accuracy of the information returned by this
   * service.
   */
  public synchronized void stop() {
    logger.info("Node status manager stopping");

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

  @Override
  public String toString() {
    return "{ nodeStatuses:" + nodeStatuses + " zooKeeper:" + zooKeeper
        + " nodeGroupName:" + nodeGroupName + " }";
  }

  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  public void setZooKeeper(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  public String getNodeGroupName() {
    return nodeGroupName;
  }

  public void setNodeGroupName(String nodeGroupName) {
    this.nodeGroupName = nodeGroupName;
  }

  public Map<String, NodeStatus> getNodeStatuses() {
    return nodeStatuses;
  }

  public void setNodeStatuses(Map<String, NodeStatus> nodeStatuses) {
    this.nodeStatuses = nodeStatuses;
  }

  /**
   * The representation of a the status of a Flume node.
   */
  public static class NodeStatus {

    /**
     * This is the state that the master thinks the node is in. This is a super
     * set of the DriverState, which is the state that the node's driver thinks
     * it is in.
     */
    public static enum NodeState {
      HELLO, OPENING, ACTIVE, CLOSING, IDLE, ERROR, LOST, DECOMMISSIONED
    };

    private NodeState state;
    private String hostName;
    private String physicalNodeName;
    private long version;
    private long joinedAt;
    private long statusLastUpdated;

    public NodeState getState() {
      return state;
    }

    public void setState(NodeState state) {
      this.state = state;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public String getPhysicalNodeName() {
      return physicalNodeName;
    }

    public void setPhysicalNodeName(String physicalNodeName) {
      this.physicalNodeName = physicalNodeName;
    }

    public long getVersion() {
      return version;
    }

    public void setVersion(long version) {
      this.version = version;
    }

    public long getJoinedAt() {
      return joinedAt;
    }

    public void setJoinedAt(long joinedAt) {
      this.joinedAt = joinedAt;
    }

    public long getStatusLastUpdated() {
      return statusLastUpdated;
    }

    public void setStatusLastUpdated(long statusLastUpdated) {
      this.statusLastUpdated = statusLastUpdated;
    }

  }

}
