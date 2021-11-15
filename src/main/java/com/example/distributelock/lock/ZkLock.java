package com.example.distributelock.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@Slf4j
public class ZkLock implements Watcher,AutoCloseable {

    private ZooKeeper zooKeeper;
    private String businessName;
    private String znode;

    public ZkLock(String connectionString, String businessName) throws IOException {
        Watcher watcher;
        this.zooKeeper = new ZooKeeper(connectionString, 30000, this);
        this.businessName = businessName;
    }

    public boolean getLock() throws KeeperException, InterruptedException {
        Stat existsNode = zooKeeper.exists("/" + businessName, false);
        if (existsNode == null) {
            // 路径、节点内容、zookeeperACL权限、节点类型（PERSISTENT-持久的、EPHEMERAL[ɪˈfemərəl]-临时）
            // 有序节点，路径后会自动添加
            zooKeeper.create("/" + businessName, businessName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 创建有序
        znode = zooKeeper.create("/" + businessName, businessName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        znode = znode.substring(znode.lastIndexOf("/") + 1);
        // 获取business下所有节点
        List<String> childrenNodes = zooKeeper.getChildren("/" + businessName, false);
        Collection.sort(childrenNodes); // 升序队列
        String firstNode = childrenNodes.get(0); // 获取最小的节点

        if (! firstNode.equals(znode)) {
            String lastNode = firstNode;
            for (String node : childrenNodes) {
                if (! znode.equals(node)) {
                    lastNode = node;
                } else {
                    zooKeeper.exists("/" + businessName + "/" + lastNode, true);
                }
            }
            // 释放
            synchronized (this) {
                wait();
            }
        }

        return true;
    }

    @Override
    public void close(WatchedEvent watchedEvent) throws Exception {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            synchronized (this) {
                notify();
            }
        }

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        zooKeeper.delete("/" + businessName + "/" + znode, -1);
        zooKeeper.close();
        log.info("我释放了锁");
    }
}
