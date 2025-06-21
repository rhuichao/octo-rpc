package com.meituan.dorado.registry.zookeeper.util;

import com.meituan.dorado.registry.zookeeper.curator.NodeChangeListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.Objects;

/**
 * @Author: liao
 * @Date: 2021/12/21 15:43
 */
public class NotifyMessage {

    /**
     * 类型
     */
    PathChildrenCacheEvent.Type type;

    /**
     * 全路径
     * /thrift/liao/mbase/mbase.liao.server/provider/192.168.20.198:10011
     */
    private String childPath;

    /**
     * 节点名称
     * 192.168.20.198:10011
     */
    private String childNodePath;

    /**
     * 通知时间
     */
    private long notifyTime = System.currentTimeMillis();

    /**
     * 最后重试时间
     */
    private long lastRetryTime;

    /**
     * 重试次数
     */
    private int retryTimes = 0;

    private NodeChangeListener nodeChangeListener;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NotifyMessage)) {
            return false;
        }
        NotifyMessage that = (NotifyMessage) o;
        return Objects.equals(getChildPath(), that.getChildPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChildPath());
    }

    public PathChildrenCacheEvent.Type getType() {
        return type;
    }

    public void setType(PathChildrenCacheEvent.Type type) {
        this.type = type;
    }

    public String getChildPath() {
        return childPath;
    }

    public void setChildPath(String childPath) {
        this.childPath = childPath;
    }

    public long getNotifyTime() {
        return notifyTime;
    }

    public void setNotifyTime(long notifyTime) {
        this.notifyTime = notifyTime;
    }

    public long getLastRetryTime() {
        return lastRetryTime;
    }

    public void setLastRetryTime(long lastRetryTime) {
        this.lastRetryTime = lastRetryTime;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public NodeChangeListener getNodeChangeListener() {
        return nodeChangeListener;
    }

    public void setNodeChangeListener(NodeChangeListener nodeChangeListener) {
        this.nodeChangeListener = nodeChangeListener;
    }

    public String getChildNodePath() {
        return childNodePath;
    }

    public void setChildNodePath(String childNodePath) {
        this.childNodePath = childNodePath;
    }

    public static NotifyMessage build(PathChildrenCacheEvent.Type type,String childPath, String childNodePath, NodeChangeListener listener){
        NotifyMessage notifyMessage = new NotifyMessage();
        notifyMessage.setType(type);
        notifyMessage.setChildPath(childPath);
        notifyMessage.setChildNodePath(childNodePath);
        notifyMessage.setNodeChangeListener(listener);
        return notifyMessage;
    }

    @Override
    public String toString() {
        return "NotifyMessage{" +
                "type=" + type +
                ", childPath='" + childPath + '\'' +
                ", childNodePath='" + childNodePath + '\'' +
                ", notifyTime=" + notifyTime +
                ", lastRetryTime=" + lastRetryTime +
                ", retryTimes=" + retryTimes +
                '}';
    }
}
