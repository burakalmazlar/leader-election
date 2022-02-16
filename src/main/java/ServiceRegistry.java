import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String REGISTRY_ZNODE = "/service_registry";

    private final ZooKeeper zookeeper;
    private String currentZnode = null;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zookeper) {
        this.zookeeper = zookeper;
        createServiceRegistryZnode();
    }

    private void createServiceRegistryZnode() {
        try {
            if(zookeeper.exists(REGISTRY_ZNODE, false) == null) {
                String registryZnodePath = zookeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Registry Znode created " + registryZnodePath);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zookeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry " + currentZnode);
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if(this.allServiceAddresses == null){
            updateAddresses();
        }
        return this.allServiceAddresses;
    }

    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if(currentZnode != null && zookeeper.exists(currentZnode,false) != null) {
            zookeeper.delete(currentZnode, -1);
        }
    }

    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zookeeper.getChildren(REGISTRY_ZNODE, this);

        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String workerZnode : workerZnodes) {
            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat workerStat = zookeeper.exists(workerZnodeFullPath, false);
            if (workerStat == null) {
                continue;
            }
            byte [] addressBytes = zookeeper.getData(workerZnodeFullPath,false,workerStat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster address : " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
