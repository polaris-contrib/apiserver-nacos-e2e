package org.polarismesh.contrib.apiserver.nacosserver.e2e;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingMaintainService;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.google.common.base.Preconditions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    @FunctionalInterface
    public interface TestRunner {
        void accept(TestContext context) throws Exception;
    }

    private static final String SERVICE_NAME = "mock_service";

    private static final String SERVICE_WATCH_NAME = "mock_watch_service";

    private static final String MOCK_INSTANCE_IP_1 = "1.1.1.1";

    private static final String MOCK_INSTANCE_IP_2 = "2.2.2.2";

    private static final String MOCK_INSTANCE_IP_3 = "3.3.3.3";

    private static final int MOCK_INSTANCE_PORT = 8080;

    private static String SERVER_ADDR = "127.0.0.1:8848";

    private static String NAMESPACE = "";

    private static class TestContext<C> {
        final String label;
        final C client1;
        final C client2;

        private TestContext(String label, C client1, C client2) {
            this.label = label;
            this.client1 = client1;
            this.client2 = client2;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("eg. java -jar xxx.jar {serverAddr}");
        }
        SERVER_ADDR = args[0];
        if (args.length == 2) {
            NAMESPACE = args[1];
        }
        NamingService client1 = buildNamingClient();
        NamingService client2 = buildNamingClient();

        TestContext<NamingService> context = new TestContext<NamingService>("", client1, client2);

        // 注册发现功能测试验证
        testRun("注册功能测试", context, Main::testRegister);
        testRun("心跳功能测试", context, Main::testHeartbeat);
        testRun("反注册功能测试", context, Main::testDeregister);
        testRun("监听功能测试", context, Main::testSubscriber);
        testRun("更新功能测试", context, Main::testUpdateInstance);
    }

    private static void testRun(String label, TestContext context, TestRunner consumer) {
        long start = System.currentTimeMillis();
        System.out.printf("[NACOS][e2e][%s] start %s%n", label, new Date());
        try {
            consumer.accept(new TestContext(label, context.client1, context.client2));
        } catch (Throwable ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            System.out.printf("[NACOS][e2e][%s] has error:%s%n", label, sw);
            throw new RuntimeException(ex);
        } finally {
            System.out.printf("[NACOS][e2e][%s] end %s, cost %d ms%n", label, new Date(), System.currentTimeMillis() - start);
        }
    }

    private static void testRegister(TestContext<NamingService> context) throws Exception {
        // client1 注册，client1 & client2 同时查询实例
        NamingService client1 = context.client1;
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);

        // 睡眠等待 2s
        TimeUnit.SECONDS.sleep(2);

        NamingService client2 = context.client2;
        List<Instance> instances = client2.getAllInstances(SERVICE_NAME);
        System.out.printf("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
        Preconditions.checkState(Objects.nonNull(instances));
        Preconditions.checkState(instances.size() == 3);
    }

    private static void testHeartbeat(TestContext<NamingService> context) throws Exception {
        // 正常来说， Nacos 1.x 客户端内部维护的心跳 interval 为 5s，因此这里等待一段时间判断 server 能否正常的处理 nacos-client 的心跳请求
        TimeUnit.SECONDS.sleep(60);
        // 直接查询客户端数据

        NamingService client3 = buildNamingClient();
        List<Instance> instances = client3.getAllInstances(SERVICE_NAME, false);
        Preconditions.checkState(Objects.nonNull(instances));
        Preconditions.checkState(instances.size() == 3);
        for (Instance instance : instances) {
            Preconditions.checkState(instance.isHealthy());
        }
    }

    private static void testSubscriber(TestContext<NamingService> context) throws Exception {
        AtomicInteger status = new AtomicInteger(0);
        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch secondLatch = new CountDownLatch(1);
        NamingService client2 = context.client2;
        client2.subscribe(SERVICE_WATCH_NAME, event -> {
            NamingEvent namingEvent = (NamingEvent) event;
            List<Instance> instances;
            switch (status.get()) {
                case 0:
                    instances = namingEvent.getInstances();
                    System.out.printf("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
                    Preconditions.checkState(Objects.nonNull(instances));
                    Preconditions.checkState(instances.size() == 3);
                    firstLatch.countDown();
                    return;
                case 1:
                    instances = namingEvent.getInstances();
                    System.out.printf("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
                    Preconditions.checkState(CollectionUtils.isEmpty(instances));
                    Preconditions.checkState(instances.isEmpty());
                    secondLatch.countDown();
                    return;
                default:
            }
        });

        NamingService client1 = context.client1;
        client1.registerInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.registerInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);
        // 等待第一次 watch 监听结果
        firstLatch.await(10, TimeUnit.SECONDS);
        // 状态判断进入下一个阶段
        status.incrementAndGet();

        // 反注册用于测试 watch 的实例
        client1.deregisterInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_WATCH_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);
        secondLatch.await(10, TimeUnit.SECONDS);
    }

    private static void testUpdateInstance(TestContext<NamingService> context) throws Exception {
        final String serviceName = SERVICE_NAME + "_update";
        NamingService client1 = context.client1;
        NamingService client2 = context.client2;
        NamingMaintainService maintainService = buildMaintainClient();

        Map<String, String> metadata = new HashMap<>();

        Instance instance = new Instance();
        instance.setServiceName(serviceName);
        instance.setIp(MOCK_INSTANCE_IP_1);
        instance.setPort(MOCK_INSTANCE_PORT);
        instance.setEphemeral(true);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setMetadata(metadata);

        AtomicInteger receive = new AtomicInteger();
        AtomicReference<Instance> expectInstance = new AtomicReference<>(instance);

        client2.subscribe(serviceName, new EventListener() {
            @Override
            public void onEvent(Event event) {
                receive.incrementAndGet();
                NamingEvent namingEvent = (NamingEvent) event;
                if (receive.get() == 2) {
                    Preconditions.checkState(CollectionUtils.isEmpty(namingEvent.getInstances()));
                } else {
                    Preconditions.checkState(CollectionUtils.size(namingEvent.getInstances()) == 1);
                    Instance receiveInstance = namingEvent.getInstances().get(0);
                    Preconditions.checkState(Objects.equals(expectInstance.get(), receiveInstance));
                }
            }
        });

        // 注册一个实例
        client1.registerInstance(serviceName, instance);
        for (;;) {
            if (receive.get() == 1) {
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        // 更新实例的 enable 属性
        instance.setEnabled(false);
        expectInstance.set(instance);

        maintainService.updateInstance(serviceName, instance);
        for (;;) {
            if (receive.get() == 2) {
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }

        // 恢复实例的 enable 属性，同时更新他的 metadata
        metadata.put("update_metadata_key", "update_metadata_value");
        instance.setEnabled(true);
        instance.setMetadata(metadata);
        maintainService.updateInstance(serviceName, instance);
        for (;;) {
            if (receive.get() == 3) {
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    private static void testDeregister(TestContext<NamingService> context) throws Exception {
        // client1 注册，client1 & client2 同时查询实例
        NamingService client1 = context.client1;
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_1, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_2, MOCK_INSTANCE_PORT);
        client1.deregisterInstance(SERVICE_NAME, MOCK_INSTANCE_IP_3, MOCK_INSTANCE_PORT);

        // 睡眠等待 2s
        TimeUnit.SECONDS.sleep(2);

        NamingService client2 = context.client2;
        List<Instance> instances = client2.getAllInstances(SERVICE_NAME);
        System.out.printf("[NACOS][e2e][%s] service(%s) receive instances:%s%n", context.label, SERVICE_NAME, instances);
        Preconditions.checkState(CollectionUtils.isEmpty(instances));
        Preconditions.checkState(instances.isEmpty());
    }

    public static NamingService buildNamingClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        NamingService namingService =  NamingFactory.createNamingService(properties);
        return namingService;
    }

    public static NamingMaintainService buildMaintainClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        NamingMaintainService maintainService =  NacosFactory.createMaintainService(properties);
        return maintainService;
    }

    public static ConfigService buildConfigClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        ConfigService configService =  ConfigFactory.createConfigService(properties);
        return configService;
    }
}