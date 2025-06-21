package com.meituan.dorado.config.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ShutdownHook extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHook.class);
    private static final List<Disposable> configs = new LinkedList<>();

    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    public static synchronized void register(Disposable config) {
        configs.add(config);
    }

    private ShutdownHook() {
        super("ShutdownHook");
    }

    @Override
    public void run() {
        LOGGER.info("Destroying...");
        for (Disposable config : configs) {
            config.destroy();
        }
    }
}
