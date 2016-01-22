package org.robobario.application;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

public class WatcherConfiguration extends Configuration {

    public WatcherConfiguration() {
        DefaultServerFactory serverFactory = (DefaultServerFactory) super.getServerFactory();
        serverFactory.setJerseyRootPath("/service/*");
        serverFactory.setApplicationContextPath("/");
        serverFactory.setAdminContextPath("/admin");
        HttpConnectorFactory connectorFactory = (HttpConnectorFactory) serverFactory.getApplicationConnectors().get(0);
        connectorFactory.setPort(10103);
        connectorFactory = (HttpConnectorFactory) serverFactory.getAdminConnectors().get(0);
        connectorFactory.setPort(10104);
    }

}
