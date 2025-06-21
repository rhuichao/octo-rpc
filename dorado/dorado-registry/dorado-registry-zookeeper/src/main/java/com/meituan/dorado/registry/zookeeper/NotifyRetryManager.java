package com.meituan.dorado.registry.zookeeper;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.meituan.dorado.registry.zookeeper.util.NotifyMessage;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: liao
 * @Date: 2021/12/21 15:56
 */
public class NotifyRetryManager {

    private static final Logger logger = LoggerFactory.getLogger(NotifyRetryManager.class);

    /**
     * 定时任务和实时处理线程互斥锁
     */
    public static final ReentrantLock LOCK = new ReentrantLock();

    private static final Set<NotifyMessage> FAIL_NOTIFY_MSG_SET = new ConcurrentSet<>();

    private static final ScheduledThreadPoolExecutor RETRY_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("notify-retry-pool-%d").build());

    private static final NotifyRetryManager INSTANCE = new NotifyRetryManager();

    private volatile  boolean initFlag = false;

    public synchronized void init(){
        if(initFlag){
            logger.warn("NotifyRetryManager had init");
            return;
        }
        RETRY_EXECUTOR_SERVICE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                retry();
            }
        }, 3000,1000, TimeUnit.MILLISECONDS);
        initFlag = true;
        logger.warn("NotifyRetryManager init success");
    }


    private NotifyRetryManager(){
    }

    private static void retry(){
        LOCK.lock();
        try{
            Iterator<NotifyMessage> iterator = FAIL_NOTIFY_MSG_SET.iterator();
            while (iterator.hasNext()){
                NotifyMessage notifyMessage = iterator.next();
                try {
                    boolean success = true;
                    switch (notifyMessage.getType()) {
                        case CHILD_ADDED:
                            success = notifyMessage.getNodeChangeListener().childNodeAdded(notifyMessage.getChildPath(), notifyMessage.getChildNodePath());
                            break;
                        case CHILD_UPDATED:
                            success = notifyMessage.getNodeChangeListener().childNodeUpdated(notifyMessage.getChildPath(), notifyMessage.getChildNodePath());
                            break;
                        case CHILD_REMOVED:
                            success = notifyMessage.getNodeChangeListener().childNodeRemoved(notifyMessage.getChildPath(), notifyMessage.getChildNodePath());
                            break;
                        default:
                            logger.error("unsupported type:{}", notifyMessage.getType());
                            break;
                        }
                    if(success) {
                        iterator.remove();
                        logger.info("retry success,{}", notifyMessage);
                    }else{
                        logger.error("retry fail,{}", notifyMessage);
                        notifyMessage.setLastRetryTime(System.currentTimeMillis());
                        notifyMessage.setRetryTimes(notifyMessage.getRetryTimes() + 1);
                    }
                }catch (Exception e) {
                    logger.error("retry fail,{}", notifyMessage, e);
                    notifyMessage.setLastRetryTime(System.currentTimeMillis());
                    notifyMessage.setRetryTimes(notifyMessage.getRetryTimes() + 1);
                }
            }
        }catch (Exception e){
            logger.error("unexpected exception", e);
        }finally {
            LOCK.unlock();
        }

    }

    public static void addNotifyRetryMsg(NotifyMessage notifyMessage){
        LOCK.lock();
        try {
            FAIL_NOTIFY_MSG_SET.add(notifyMessage);
        }finally {
            LOCK.unlock();
        }
    }

    public static void removeNotifyRetryMsg(NotifyMessage notifyMessage){
        LOCK.lock();
        try {
            FAIL_NOTIFY_MSG_SET.remove(notifyMessage);
        }finally {
            LOCK.unlock();
        }
    }

    public static NotifyRetryManager getINSTANCE() {
        return INSTANCE;
    }
}
