/*
 * Copyright 2018 Meituan Dianping. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meituan.dorado.registry.zookeeper;

import com.meituan.dorado.common.Constants;
import com.meituan.dorado.common.exception.DoradoException;
import com.meituan.dorado.common.exception.RegistryException;
import com.meituan.dorado.common.exception.ZkNodeExistsException;
import com.meituan.dorado.common.util.NetUtil;
import com.meituan.dorado.registry.DiscoveryService;
import com.meituan.dorado.registry.ProviderListener;
import com.meituan.dorado.registry.RegistryPolicy;
import com.meituan.dorado.registry.meta.Provider;
import com.meituan.dorado.registry.meta.SubscribeInfo;
import com.meituan.dorado.registry.zookeeper.curator.NodeChangeListener;
import com.meituan.dorado.registry.zookeeper.curator.StateChangeListener;
import com.meituan.dorado.registry.zookeeper.curator.ZookeeperClient;
import com.meituan.dorado.registry.zookeeper.curator.ZookeeperManager;
import com.meituan.dorado.registry.zookeeper.util.ZooKeeperNodeInfo;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ZookeeperDiscoveryService implements DiscoveryService {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperDiscoveryService.class);

    private ZookeeperClient zkClient;
    private String address;
    private RegistryPolicy registryPolicy;
    private final ConcurrentMap<SubscribeInfo, NodeChangeListener> listeners = new ConcurrentHashMap<>();

    public ZookeeperDiscoveryService(String address) {
        this.address = address;
        this.zkClient = ZookeeperManager.getZkClient(address);
        zkClient.addStateListener(new StateChangeListener() {
            @Override
            public void connStateChanged() {
                if (registryPolicy != null) {
                    registryPolicy.reSubscribe();
                }
            }
        });
    }

    /**
     * 服务订阅
     *
     * @param info
     * @param notifyListener
     */
    @Override
    public synchronized void subscribe(SubscribeInfo info, ProviderListener notifyListener) {
        try {
            NodeChangeListener listener = listeners.get(info);
            if (listener == null) {
                String path = generateNodePath(info);
                listener = buildNodeChangeListener(info, notifyListener);
                listeners.put(info, listener);
                zkClient.addChildNodeChangeListener(path, listener);
                List<Provider> providers = genProviderList(path, info);
                notifyListener.notify(providers);
                registerConsumer(info);
                logger.info("Subscribe on zookeeper[{}]: remoteAppkey={}, serviceName={}", address, info.getRemoteAppkey(), info.getServiceName());
            } else {
                logger.warn("{} has subscribed service", info);
            }
        } catch (Throwable e) {
            throw new RegistryException("Failed to subscribe service " + info.getServiceName(), e);
        }
    }

    @Override
    public synchronized void unsubscribe(SubscribeInfo info) {
        try {
            NodeChangeListener listener = listeners.remove(info);
            if (listener != null) {
                zkClient.removeChildNodeChangeListener(listener);
                logger.info("Unsubscribe on zookeeper[{}]: remoteAppkey={}, serviceName={}", address, info.getRemoteAppkey(), info.getServiceName());
            } else {
                logger.info("Have unsubscribe on zookeeper[{}]: remoteAppkey={}, serviceName={}", address, info.getRemoteAppkey(), info.getServiceName());
            }
            unregisterConsumer(info);
        } catch (Throwable e) {
            throw new RegistryException("Failed to unsubscribe service " + info.getServiceName(), e);
        }
    }

    @Override
    public void destroy() {
        ZookeeperManager.closeZkClient(address);
    }

    @Override
    public void setRegistryPolicy(RegistryPolicy registryPolicy) {
        this.registryPolicy = registryPolicy;
    }

    protected NodeChangeListener buildNodeChangeListener(final SubscribeInfo info, final ProviderListener notifyListener) {
        NodeChangeListener listener = new NodeChangeListener() {
            @Override
            public boolean childNodeAdded(String childPath, String childNodePath) {
                Provider provider = genProvider(childPath, info);
                if (provider != null) {
                    List<Provider> providerList = new ArrayList<>();
                    providerList.add(provider);
                    return notifyListener.added(providerList);
                }
                return true;
            }

            @Override
            public boolean childNodeUpdated(String childPath, String childNodePath) {
                Provider provider = genProvider(childPath, info);
                if (provider != null) {
                    List<Provider> providerList = new ArrayList<>();
                    providerList.add(provider);
                    return notifyListener.updated(providerList);
                } else {
                    // 如状态变更为不可用
                    if (NetUtil.isIpPortStr(childNodePath)) {
                        List<String> ipPorts = new ArrayList<>();
                        ipPorts.add(childNodePath);
                        return notifyListener.removed(ipPorts);
                    }
                }
                return true;
            }

            @Override
            public boolean childNodeRemoved(String childPath, String childNodePath) {
                if (NetUtil.isIpPortStr(childNodePath)) {
                    List<String> ipPorts = new ArrayList<>();
                    ipPorts.add(childNodePath);
                    return notifyListener.removed(ipPorts);
                }
                return true;
            }
        };
        return listener;
    }

    private Provider genProvider(String childPath, SubscribeInfo info) {
        String nodeData = null;
        try {
            nodeData = zkClient.getNodeData(childPath);
            Provider provider = ZooKeeperNodeInfo.genProvider(nodeData, info);
            if (provider != null) {
                return provider;
            }
        }catch (KeeperException e){
            if(e.code() == KeeperException.Code.NODEEXISTS){
                throw new ZkNodeExistsException("Get nodeData=" + nodeData + " from path=" + childPath + " to generate Provider failed.");
            }
            throw new DoradoException("Get nodeData=" + nodeData + " from path=" + childPath + " to generate Provider failed.");
        }
        catch (Exception e) {
            throw new DoradoException("Get nodeData=" + nodeData + " from path=" + childPath + " to generate Provider failed.");
        }
        return null;
    }

    private List<Provider> genProviderList(String parentPath, SubscribeInfo info) throws Exception {
        List<String> childNodes = zkClient.getChildren(parentPath);
        List<Provider> providers;
        if (childNodes == null || childNodes.isEmpty()) {
            providers = Collections.EMPTY_LIST;
        } else {
            providers = new ArrayList<>();
            for (String node : childNodes) {
                Provider provider = genProvider(parentPath + ZooKeeperNodeInfo.PATH_SEPARATOR + node, info);
                if (provider != null) {
                    providers.add(provider);
                }
            }
        }
        return providers;
    }

    private String generateNodePath(SubscribeInfo info) {
        StringBuilder pathBuilder = new StringBuilder(info.getRegistryGroup()).append(ZooKeeperNodeInfo.PATH_SEPARATOR)
                .append(info.getRemoteAppkey()).append(ZooKeeperNodeInfo.PATH_SEPARATOR).append(ZooKeeperNodeInfo.PROVIDER);
        return pathBuilder.toString();
    }

    private String generateConsumerNodePath(SubscribeInfo info) {
        StringBuilder pathBuilder = new StringBuilder(info.getRegistryGroup()).append(ZooKeeperNodeInfo.PATH_SEPARATOR)
                .append(info.getRemoteAppkey()).append(ZooKeeperNodeInfo.PATH_SEPARATOR).append(ZooKeeperNodeInfo.CONSUMER)
                .append(ZooKeeperNodeInfo.PATH_SEPARATOR).append(NetUtil.getLocalHost())
                .append(Constants.COLON).append(ZooKeeperNodeInfo.getPid());
        return pathBuilder.toString();
    }

    private synchronized void registerConsumer(SubscribeInfo info) {
        try {
            String path = generateConsumerNodePath(info);
            zkClient.createNode(path, ZooKeeperNodeInfo.genConsumerNodeData(info));
            logger.info("Register consumer on zookeeper[{}] path={}, info={}", address, path, info);
        } catch (Throwable e) {
            throw new RegistryException("Failed to register consumer: " + info.getLocalAppkey(), e);
        }
    }

    private synchronized void unregisterConsumer(SubscribeInfo info) {
        try {
            String path = generateConsumerNodePath(info);
            zkClient.delete(path);
            logger.info("Unregister consumer on zookeeper[{}] path={}, info{}", address, path, info);
        } catch (Throwable e) {
            throw new RegistryException("Failed to unregister consumer: " + info.getLocalAppkey(), e);
        }
    }

}
