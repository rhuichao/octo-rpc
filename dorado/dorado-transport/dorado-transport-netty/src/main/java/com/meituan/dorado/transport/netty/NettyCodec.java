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
package com.meituan.dorado.transport.netty;

import com.meituan.dorado.codec.Codec;
import com.meituan.dorado.common.exception.ProtocolException;
import com.meituan.dorado.common.exception.RequestTimeoutException;
import com.meituan.dorado.common.extension.ExtensionLoader;
import com.meituan.dorado.transport.LengthDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NettyCodec extends ByteToMessageCodec {

    private static final Logger logger = LoggerFactory.getLogger(NettyCodec.class);

    private static LengthDecoder lengthDecoder;

    private final Codec codec;
    private final Map<String, Object> attachments;

    static {
        try {
            lengthDecoder = ExtensionLoader.getExtension(LengthDecoder.class);
        } catch (Throwable e) {
            logger.error("No {} implement", LengthDecoder.class, e);
        }

    }

    public NettyCodec(Codec codec, Map<String, Object> attachments) {
        if (lengthDecoder == null) {
            throw new ProtocolException("No " + LengthDecoder.class + " implement, cannot do codec.");
        }
        if (attachments == null) {
            this.attachments = Collections.emptyMap();
        } else {
            this.attachments = attachments;
        }
        this.codec = codec;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        NettyChannel nettyChannel = ChannelManager.getOrAddChannel(ctx.channel());
        out.writeBytes(codec.encode(nettyChannel, msg, attachments));
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List out) throws Exception {
        String uuid = UUID.randomUUID().toString();
            int totalLength = lengthDecoder.decodeLength(in.nioBuffer());
            int readableBytes = in.readableBytes();
            if(logger.isDebugEnabled()) {
                logger.debug("uuid1:{},readableBytes:{},totalLength:{}", uuid, readableBytes, totalLength);
            }
            if (totalLength < 0) {
                logger.debug("Not getting enough bytes to get totalLength.");
                return;
            }
            if (readableBytes < totalLength) {
                logger.debug("Not getting enough bytes, need {} bytes but got {} bytes", totalLength,
                        readableBytes);
                return;
            }

            NettyChannel nettyChannel = ChannelManager.getOrAddChannel(ctx.channel());
            byte[] buffer = new byte[totalLength];
            in.readBytes(buffer);
            if(logger.isDebugEnabled()) {
                logger.debug("uuid2:{},getResByte:{}", uuid, Hex.encodeHexString(buffer, true));
            }

//
            try {
                out.add(codec.decode(nettyChannel, buffer, attachments));
            }catch (RequestTimeoutException e){
                logger.warn("Request has removed, cause Timeout happened earlier");
            }catch (Exception e){
                if(e.getCause() instanceof RequestTimeoutException){
                    logger.warn("Request has removed, cause Timeout happened earlier");
                    return;
                }
                logger.error("decode exception,", e);
            }

       // }
    }

    private final ByteToMessageDecoder decoder = new ByteToMessageDecoder() {
        @Override
        public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            NettyCodec.this.decode(ctx, in, out);
        }

        @Override
        protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            NettyCodec.this.decodeLast(ctx, in, out);
        }
    };

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug("channelRead begin");
        }
        try {
            decoder.channelRead(ctx, msg);
        }catch (Exception e){
            logger.error("read error",e);
            ctx.channel();
        }
        if(logger.isDebugEnabled()) {
            logger.debug("channelRead finish");
        }
    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug("channelReadComplete begin");
        }
        decoder.channelReadComplete(ctx);
        if(logger.isDebugEnabled()) {
            logger.debug("channelReadComplete finish");
        }
    }

}
