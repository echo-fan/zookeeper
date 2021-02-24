package zk.demo1;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ZKOperate {
    @Test
    public void createNode() throws Exception {
        //定义重试机制，retry的第一个参数意思为尝试的时间（3000ms）  第二个参数为尝试次数3次
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(3000,3);
        //得到客户端
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("hadoop11:2181,hadoop12:2181,hadoop13:2181", retry);
        //开始服务端
        curatorFramework.start();

        //创建持久节点

        curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello/abc","helloword".getBytes());




        //停止服务端
        curatorFramework.close();

    }
    @Test
    public void createTempNode() throws Exception {

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                "hadoop11:2181,hadoop12:2181,hadoop13:2181", retry);
        curatorFramework.start();

        curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/linshi/abc","linshi".getBytes());


        Thread.sleep(8000);

        curatorFramework.close();


    }

    @Test
    public void updateNode() throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                "hadoop11:2181,hadoop12:2181,hadoop13:2181",
                new ExponentialBackoffRetry(3000,3));


        curatorFramework.start();

         curatorFramework.setData().forPath("/bb01", "bbbb".getBytes());


        curatorFramework.close();
    }


    @Test
    public void getNode() throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("" +
                        "hadoop11:2181,hadoop12:2181,hadoop13:2181",
                new ExponentialBackoffRetry(3000, 3));

        curatorFramework.start();

        byte[] bytes = curatorFramework.getData().forPath("/bb01");

        String s = new String(bytes);

        System.out.println(s);
        curatorFramework.close();
    }

    /*
    zk的watch机制
     */
    @Test
    public void wartchNode() throws Exception {

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("hadoop11:2181,hadoop12:2181,hadoop13:2181",
                new ExponentialBackoffRetry(3000, 3));
        curatorFramework.start();

        TreeCache treeCache = new TreeCache(curatorFramework, "/bb01");

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (null !=data){
                    TreeCacheEvent.Type type = event.getType();
                    switch (type){
                        case NODE_ADDED:
                            System.out.println("增加节点");
                            break;
                        case INITIALIZED:
                            System.out.println("节点初始化");
                            break;
                        case NODE_REMOVED:
                            System.out.println("节点删除");
                            break;
                        case NODE_UPDATED:
                            System.out.println("修改节点");
                            break;
                        default:
                            System.out.println("没有操作");
                    }

                }

            }
        });
    treeCache.start();
    Thread.sleep(500000);
    curatorFramework.close();
    }

}
