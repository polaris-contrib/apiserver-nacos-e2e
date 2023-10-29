package org.polarismesh.contrib.apiserver.nacosserver.e2e;

import com.alibaba.nacos.api.naming.NamingMaintainService;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.shaded.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class NamingE2E {


    public static final String SERVICE_NAME = "mock_service";

    public static final String SERVICE_WATCH_NAME = "mock_watch_service";

    public static final String MOCK_INSTANCE_IP_1 = "1.1.1.1";

    public static final String MOCK_INSTANCE_IP_2 = "2.2.2.2";

    public static final String MOCK_INSTANCE_IP_3 = "3.3.3.3";

    public static final int MOCK_INSTANCE_PORT = 8080;

    public static void testNamingFunction() throws Exception {
        NamingService client1 = Main.buildNamingClient();
        NamingService client2 = Main.buildNamingClient();

        Main.TestContext<NamingService> context = new Main.TestContext<NamingService>("", client1, client2);

        // 注册发现功能测试验证
        Main.log("------------- 开始进行 【注册发现】 相关 e2e 测试流程 -------------");

        try {
            Main.testRun("注册功能测试", context, NamingE2E::testRegister);
            Main.testRun("心跳功能测试", context, NamingE2E::testHeartbeat);
            Main.testRun("反注册功能测试", context, NamingE2E::testDeregister);
            Main.testRun("更新功能测试", context, NamingE2E::testUpdateInstance);
        } finally {
            Main.log("------------- 结束 【注册发现】 相关 e2e 测试流程 -------------");
            client1.shutDown();
            client2.shutDown();
        }

    }

    public static void testRegister(Main.TestContext<NamingService> context) throws Exception {
        // client1 注册，client1 & client2 同时查询实例
        NamingService client1 = context.client1;
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);

        // 睡眠等待 2s
        TimeUnit.SECONDS.sleep(10);

        NamingService client2 = context.client2;
        List<Instance> instances = client2.getAllInstances(SERVICE_NAME);
        Main.log("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
        Preconditions.checkState(Objects.nonNull(instances));
        Preconditions.checkState(instances.size() == 3);
    }

    public static void testHeartbeat(Main.TestContext<NamingService> context) throws Exception {
        // 直接查询客户端数据
        String serviceName = SERVICE_NAME + "_healthcheck";
        NamingService client3 = Main.buildNamingClient();
        NamingService client4 = Main.buildNamingClient();
        // 使用 client3 注册实例
        client3.registerInstance(serviceName, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        TimeUnit.SECONDS.sleep(10);
        try {
            List<Instance> instances = client4.getAllInstances(serviceName, true);
            Main.log("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, serviceName, instances);
            Preconditions.checkState(Objects.nonNull(instances));
            Preconditions.checkState(instances.size() == 1);
            for (Instance instance : instances) {
                Preconditions.checkState(instance.isHealthy());
            }
        } finally {
            client3.shutDown();
        }

        // 强行关闭
        // 等待 10s
        TimeUnit.SECONDS.sleep(10);

        // 再次获取一下实例列表，发现实例是非健康
        List<Instance> instances = client4.getAllInstances(serviceName, true);
        Main.log("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, serviceName, instances);
        Preconditions.checkState(Objects.nonNull(instances));
        // client 关闭后，实例直接被反注册掉，这里查询到的是没有这个实例了
        Preconditions.checkState(instances.size() == 1 || instances.isEmpty());
        for (Instance instance : instances) {
            Preconditions.checkState(!instance.isHealthy());
        }
    }

    public static void testUpdateInstance(Main.TestContext<NamingService> context) throws Exception {
        final String serviceName = SERVICE_NAME + "_update";
        NamingService client1 = context.client1;
        NamingService client2 = context.client2;
        NamingMaintainService maintainService = Main.buildMaintainClient();

        try {
            Map<String, String> metadata = new HashMap<>();

            Instance localInstance = new Instance();
            localInstance.setServiceName(serviceName);
            localInstance.setIp(MOCK_INSTANCE_IP_1);
            localInstance.setPort(MOCK_INSTANCE_PORT);
            localInstance.setEphemeral(true);
            localInstance.setHealthy(true);
            localInstance.setEnabled(true);
            localInstance.setMetadata(metadata);

            // 先触发服务订阅
            client2.getAllInstances(serviceName, true);

            // 注册一个实例
            client1.registerInstance(serviceName, localInstance);
            TimeUnit.SECONDS.sleep(10);
            List<Instance> instances = client2.getAllInstances(serviceName, true);
            Preconditions.checkState(instances.size() == 1);

            // 更新实例的 enable 属性
            localInstance.setEnabled(false);
            maintainService.updateInstance(serviceName, localInstance);
            TimeUnit.SECONDS.sleep(10);
            instances = client2.getAllInstances(serviceName, true);
            Preconditions.checkState(instances.isEmpty());

            // 恢复实例的 enable 属性，同时更新他的 metadata
            metadata.put("update_metadata_key", "update_metadata_value");
            localInstance.setEnabled(true);
            localInstance.setMetadata(metadata);
            maintainService.updateInstance(serviceName, localInstance);
            TimeUnit.SECONDS.sleep(10);
            instances = client2.getAllInstances(serviceName, true);
            Preconditions.checkState(instances.size() == 1);
            Instance remoteInstance = instances.get(0);
            cleanRemoteInstanceInfo(remoteInstance);
            Preconditions.checkState(Objects.equals(remoteInstance, localInstance), String.format("local : %s, remote : %s", localInstance, remoteInstance));
        } finally {
            maintainService.shutDown();
        }
    }

    public static void cleanRemoteInstanceInfo(Instance remoteInstance) {
        remoteInstance.setInstanceId(null);
        remoteInstance.setClusterName(null);
        if (Objects.nonNull(remoteInstance.getMetadata())) {
            remoteInstance.getMetadata().remove("campus");
            remoteInstance.getMetadata().remove("region");
            remoteInstance.getMetadata().remove("zone");
            remoteInstance.getMetadata().remove("internal-nacos-cluster");
            remoteInstance.getMetadata().remove("internal-nacos-service");
            remoteInstance.getMetadata().remove("internal-lastheartbeat");
        }
    }

    public static void testDeregister(Main.TestContext<NamingService> context) throws Exception {
        // client1 注册，client1 & client2 同时查询实例
        NamingService client1 = context.client1;
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);

        // 睡眠等待 10s
        TimeUnit.SECONDS.sleep(10);

        NamingService client2 = context.client2;
        List<Instance> instances = client2.getAllInstances(SERVICE_NAME);
        Main.log("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
        Preconditions.checkState(CollectionUtils.isEmpty(instances));
        Preconditions.checkState(instances.isEmpty());
    }

}
