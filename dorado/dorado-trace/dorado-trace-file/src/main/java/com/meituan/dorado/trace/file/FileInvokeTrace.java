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

package com.meituan.dorado.trace.file;

import com.meituan.dorado.common.Constants;
import com.meituan.dorado.rpc.meta.RpcInvocation;
import com.meituan.dorado.trace.AbstractInvokeTrace;
import com.meituan.dorado.trace.meta.TraceParam;
import com.meituan.dorado.transport.meta.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileInvokeTrace extends AbstractInvokeTrace {

    private static final Logger logger = LoggerFactory.getLogger(FileInvokeTrace.class);

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void init(String appkey) {
    }

    @Override
    public void clientSend(TraceParam traceParam, RpcInvocation invocation) {
        if (logger.isDebugEnabled()) {
            Request request = (Request)invocation.getAttachment(Constants.RPC_REQUEST);
            logger.debug("client send to {}:{}, seq:{}, reqSize:{}, respSize:{}, invokerTrace:{}",
                    traceParam.getRemoteIp(), traceParam.getRemotePort(), request!=null ? request.getSeq():"-1",
                    traceParam.getRequestSize(), traceParam.getResponseSize(),
                    traceParam.getTraceTimeline().genInvokerAllPhaseCost());
        }
    }

    @Override
    public void serverRecv(TraceParam traceParam, RpcInvocation invocation) {
    }

    @Override
    protected void serverSendInFilter(TraceParam traceParam, RpcInvocation invocation) {

    }

    @Override
    protected void serverSendInCodec(TraceParam traceParam, RpcInvocation invocation) {

    }

    @Override
    protected void syncClientRecv(TraceParam traceParam, RpcInvocation invocation) {
        if (logger.isDebugEnabled()) {
            Request request = (Request)invocation.getAttachment(Constants.RPC_REQUEST);
            logger.debug("client sync receive from {}:{}, seq:{}, reqSize:{}, respSize:{}, invokerTrace:{}",
                    traceParam.getRemoteIp(), traceParam.getRemotePort(), request!=null ? request.getSeq():"-1",
                    traceParam.getRequestSize(), traceParam.getResponseSize(),
                    traceParam.getTraceTimeline().genInvokerAllPhaseCost());
        }
    }

    @Override
    protected void asyncClientRecv(TraceParam traceParam, RpcInvocation invocation) {
        if (logger.isDebugEnabled()) {
            Request request = (Request)invocation.getAttachment(Constants.RPC_REQUEST);
            logger.debug("client async receive from {}:{}, seq:{}, reqSize:{}, respSize:{}, invokerTrace:{}",
                    traceParam.getRemoteIp(), traceParam.getRemotePort(), request!=null ? request.getSeq():"-1",
                    traceParam.getRequestSize(), traceParam.getResponseSize(),
                    traceParam.getTraceTimeline().genInvokerAllPhaseCost());
        }
    }

}
