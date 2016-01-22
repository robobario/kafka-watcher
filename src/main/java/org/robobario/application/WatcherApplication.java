package org.robobario.application;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.robobario.resource.DspEventResource;
import org.robobario.service.KafkaGroupedEvents;
import org.robobario.service.KafkaWatcherService;

public class WatcherApplication extends Application<WatcherConfiguration> {

    public static void main(String[] args) throws Exception {
        new WatcherApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<WatcherConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/static/", "/", "index.html"));
    }

    @Override
    public void run(WatcherConfiguration watcherConfiguration, Environment environment) throws Exception {
        KafkaGroupedEvents groupedEvents = new KafkaGroupedEvents();
        Thread thread = new Thread(new KafkaWatcherService(groupedEvents));
        thread.start();
        environment.jersey().setUrlPattern("/api/*");
        environment.jersey().register(new DspEventResource(groupedEvents, environment.getObjectMapper()));
    }


    @Override
    public String getName() {
        return "kafka watcher";
    }
}
