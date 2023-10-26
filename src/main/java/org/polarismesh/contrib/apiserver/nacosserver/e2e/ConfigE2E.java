package org.polarismesh.contrib.apiserver.nacosserver.e2e;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.shaded.com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConfigE2E {

    public static final String GROUP = "mock_group";

    public static final String DATA_ID_1 = "mock_config_file_1";

    public static final String DATA_ID_2 = "mock_config_file_2";

    public static final String DATA_ID_3 = "mock_config_file_3";


    public static void testConfigFunction() throws Exception {
        ConfigService client1 = Main.buildConfigClient();
        ConfigService client2 = Main.buildConfigClient();

        Main.TestContext<ConfigService> context = new Main.TestContext<ConfigService>("", client1, client2);

        // 注册发现功能测试验证
        Main.log("------------- 开始进行 【配置中心】 相关 e2e 测试流程 -------------");
        try {
            Main.testRun("配置发布功能测试", context, ConfigE2E::testPublishConfig);
            Main.testRun("配置监听功能测试", context, ConfigE2E::testListenConfig);
            Main.testRun("配置删除功能测试", context, ConfigE2E::testDeleteConfig);
        } finally {
            Main.log("------------- 结束 【配置中心】 相关 e2e 测试流程 -------------");
            client1.shutDown();
            client2.shutDown();
        }
    }

    public static void testPublishConfig(Main.TestContext<ConfigService> context) throws Exception {
        ConfigService client1 = context.client1;
        ConfigService client2 = context.client2;

        String mockContent = "hello, polarismesh ";
        boolean ret = client1.publishConfig(DATA_ID_1, GROUP, mockContent);
        Preconditions.checkState(ret, "publish must be success");

        TimeUnit.SECONDS.sleep(5);

        String receiveRet = client2.getConfig(DATA_ID_1, GROUP, 5000);
        Preconditions.checkState(Objects.equals(receiveRet, mockContent),
                "content not equal, remote : %s, local : %s", receiveRet, mockContent);
    }

    public static void testListenConfig(Main.TestContext<ConfigService> context) throws Exception {
        ConfigService client1 = context.client1;
        ConfigService client2 = context.client2;

        CountDownLatch latch = new CountDownLatch(3);
        AtomicReference<Exception> errRef = new AtomicReference<>();
        AtomicInteger receiveCnt = new AtomicInteger(0);
        AtomicReference<String> expectConfigContent = new AtomicReference<>();

        client2.addListener(DATA_ID_2, GROUP, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String receiveRet) {
                try {
                    Main.log("[NACOS][e2e][%s] config(%s) receive content:%s%n", context.label, DATA_ID_2, receiveRet);
                    Preconditions.checkState(Objects.equals(receiveRet, expectConfigContent.get()),
                            "content not equal, remote : %s, local : %s", receiveRet, expectConfigContent.get());
                } catch (Exception ex) {
                    errRef.set(ex);
                } finally {
                    receiveCnt.incrementAndGet();
                    latch.countDown();
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        String baseContent = "hello, polarismesh";
        new Thread(() -> {
            try {
                for (int i = 0; i < 3; i++) {
                    String newContent = baseContent + ", publish times " + UUID.randomUUID();
                    expectConfigContent.set(newContent);
                    boolean ret = client1.publishConfig(DATA_ID_2, GROUP, expectConfigContent.get());
                    Preconditions.checkState(ret, "publish must be success");
                    TimeUnit.SECONDS.sleep(30);
                    Preconditions.checkState(receiveCnt.get() == (i + 1));
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).start();

        Preconditions.checkState(latch.await(100, TimeUnit.SECONDS));

        if (errRef.get() != null) {
            throw errRef.get();
        }
    }

    public static void testDeleteConfig(Main.TestContext<ConfigService> context) throws Exception {
        ConfigService client1 = context.client1;
        ConfigService client2 = context.client2;

        String content = UUID.randomUUID().toString();

        BlockingQueue<Object> notifier = new ArrayBlockingQueue<>(2);
        AtomicReference<Exception> errRef = new AtomicReference<>();
        client2.addListener(DATA_ID_3, Constants.DEFAULT_GROUP, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String receiveRet) {
                Main.log("[NACOS][e2e][%s] config(%s) receive content:%s%n", context.label, DATA_ID_2, receiveRet);
                try {
                    notifier.put(new Object());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // client1 先发布一个配置
        boolean ret = client1.publishConfig(DATA_ID_3, Constants.DEFAULT_GROUP, content);
        Preconditions.checkState(ret, "publish must be success");

        // 最多等待 30s listen 可以返回监听的信息数据
        Preconditions.checkState(Objects.nonNull(notifier.poll(30, TimeUnit.SECONDS)));
        Preconditions.checkState(Objects.isNull(errRef.get()), errRef.get());

        ret = client1.removeConfig(DATA_ID_3, Constants.DEFAULT_GROUP);
        Preconditions.checkState(ret, "remove must be success");

        // 删除配置后， nacos 客户端会推送一个 null 过来
        Preconditions.checkState(Objects.nonNull(notifier.poll(30, TimeUnit.SECONDS)));
        Preconditions.checkState(Objects.isNull(errRef.get()), errRef.get());
        String receiveRet = client2.getConfig(DATA_ID_3, Constants.DEFAULT_GROUP, 5000);
        Preconditions.checkState(Objects.isNull(receiveRet) || Objects.equals(receiveRet, ""));
    }

}
