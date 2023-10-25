package org.polarismesh.contrib.apiserver.nacosserver.e2e;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingMaintainService;
import com.alibaba.nacos.api.naming.NamingService;

import java.util.Date;
import java.util.Objects;
import java.util.Properties;

public class Main {

    @FunctionalInterface
    public interface TestRunner {
        void accept(TestContext context) throws Exception;
    }

    public static String SERVER_ADDR = "127.0.0.1:8848";

    public static String NAMESPACE = "";

    public static class TestContext<C> {
        final String label;
        final C client1;
        final C client2;

        public TestContext(String label, C client1, C client2) {
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

        ConfigE2E.testConfigFunction();
        NamingE2E.testNamingFunction();
    }

    public static void testRun(String label, TestContext context, TestRunner consumer) throws Exception {
        long start = System.currentTimeMillis();
        log("[NACOS][e2e][%s] start %s%n", label, new Date());
        try {
            consumer.accept(new TestContext(label, context.client1, context.client2));
        } finally {
            log("[NACOS][e2e][%s] end %s, cost %d ms%n", label, new Date(), System.currentTimeMillis() - start);
        }
    }

    public static NamingService buildNamingClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        NamingService namingService = NamingFactory.createNamingService(properties);
        return namingService;
    }

    public static NamingMaintainService buildMaintainClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        NamingMaintainService maintainService = NacosFactory.createMaintainService(properties);
        return maintainService;
    }

    public static ConfigService buildConfigClient() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, SERVER_ADDR);
        if (!Objects.equals(NAMESPACE, "")) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, NAMESPACE);
        }
        ConfigService configService = ConfigFactory.createConfigService(properties);
        return configService;
    }

    public static void log(String format, Object ... args) {
        System.out.printf("\n[" + new Date() + "]\n" + format, args);
    }
}