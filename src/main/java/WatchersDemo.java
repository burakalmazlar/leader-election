import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class WatchersDemo implements Watcher {

    private final static String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target_znode";


    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatchersDemo leaderElection = new WatchersDemo();

        leaderElection.connectToZookeeper();
        leaderElection.watchTargetZnode();
        leaderElection.run();

        leaderElection.close();

    }

    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) {
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);

        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data : " + new String(data) + " children : " + children);

    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None -> {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from zookeper event");
                        zooKeeper.notifyAll();
                    }
                }
            }
            case NodeCreated -> {
                System.out.println(TARGET_ZNODE + " was created");
            }
            case NodeDeleted -> {
                System.out.println(TARGET_ZNODE + " was deleted");
            }
            case NodeDataChanged -> {
                System.out.println(TARGET_ZNODE + " data changed");
            }
            case NodeChildrenChanged -> {
                System.out.println(TARGET_ZNODE + " children changed");
            }
        }

        try {
            watchTargetZnode();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
