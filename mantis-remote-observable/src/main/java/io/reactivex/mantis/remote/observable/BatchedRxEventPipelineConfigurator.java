/*
 * Copyright 2019 Netflix, Inc.
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

package io.reactivex.mantis.remote.observable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchedRxEventPipelineConfigurator implements PipelineConfigurator<RemoteRxEvent, List<RemoteRxEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(BatchedRxEventPipelineConfigurator.class);
    private static final byte PROTOCOL_VERSION = 1;

    @SuppressWarnings("unchecked")
    static Map<String, String> fromBytesToMap(byte[] bytes) {
        Map<String, String> map = null;
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            in = new ObjectInputStream(bis);
            map = (Map<String, String>) in.readObject();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e1) {
            throw new RuntimeException(e1);
        } finally {
            try {
                if (bis != null) {bis.close();}
                if (in != null) {in.close();}
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return map;
    }

    static byte[] fromMapToBytes(Map<String, String> map) {
        ByteArrayOutputStream baos = null;
        ObjectOutput out = null;
        try {
            baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (out != null) {out.close();}
                if (baos != null) {baos.close();}
            } catch (IOException e1) {
                e1.printStackTrace();
                throw new RuntimeException(e1);
            }
        }
        return baos.toByteArray();
    }

    @Override
    public void configureNewPipeline(ChannelPipeline pipeline) {


        pipeline.addLast(new ChannelDuplexHandler() {

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg)
                    throws Exception {
                boolean handled = false;
                if (ByteBuf.class.isAssignableFrom(msg.getClass())) {


                    ByteBuf byteBuf = (ByteBuf) msg;
                    if (byteBuf.isReadable()) {
                        int protocolVersion = byteBuf.readByte();
                        if (protocolVersion != PROTOCOL_VERSION) {
                            throw new RuntimeException("Unsupported protocol version: " + protocolVersion);
                        }
                        int observableNameLength = byteBuf.readByte();
                        String observableName = null;
                        if (observableNameLength > 0) {
                            // read name
                            byte[] observableNameBytes = new byte[observableNameLength];
                            byteBuf.readBytes(observableNameBytes);
                            observableName = new String(observableNameBytes, Charset.forName("UTF-8"));
                        }

                        while (byteBuf.isReadable()) {
                            int lengthOfEvent = byteBuf.readInt();
                            int operation = byteBuf.readByte();
                            RemoteRxEvent.Type type = null;
                            Map<String, String> subscribeParams = null;
                            byte[] valueData = null;
                            if (operation == 1) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: next");
                                //	                        	}
                                type = RemoteRxEvent.Type.next;
                                valueData = new byte[lengthOfEvent - 1]; //subtract op code
                                byteBuf.readBytes(valueData);
                            } else if (operation == 2) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: error");
                                //	                        	}
                                type = RemoteRxEvent.Type.error;
                                valueData = new byte[lengthOfEvent - 1];
                                byteBuf.readBytes(valueData);
                            } else if (operation == 3) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: completed");
                                //	                        	}
                                type = RemoteRxEvent.Type.completed;
                            } else if (operation == 4) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: subscribed");
                                //	                        	}
                                type = RemoteRxEvent.Type.subscribed;
                                // read subscribe parameters
                                int subscribeParamsLength = byteBuf.readInt();
                                if (subscribeParamsLength > 0) {
                                    // read byte into map
                                    byte[] subscribeParamsBytes = new byte[subscribeParamsLength];
                                    byteBuf.readBytes(subscribeParamsBytes);
                                    subscribeParams = fromBytesToMap(subscribeParamsBytes);
                                }
                            } else if (operation == 5) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: unsubscribed");
                                //	                        	}
                                type = RemoteRxEvent.Type.unsubscribed;
                            } else if (operation == 6) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: heartbeat");
                                //	                        	}
                            } else if (operation == 7) {
                                //	                        	if(logger.isDebugEnabled()) {
                                //	                        		logger.debug("READ request for RemoteRxEvent: nonDataError");
                                //	                        	}
                                type = RemoteRxEvent.Type.nonDataError;
                                valueData = new byte[lengthOfEvent - 1];
                                byteBuf.readBytes(valueData);
                            } else {
                                throw new RuntimeException("operation: " + operation + " not support.");
                            }
                            // don't send heartbeats through pipeline
                            if (operation != 6) {
                                ctx.fireChannelRead(new RemoteRxEvent(observableName, type, valueData, subscribeParams));
                            }
                        }
                        handled = true;
                        byteBuf.release();
                    }
                }
                if (!handled) {
                    super.channelRead(ctx, msg);
                }

            }

            @Override
            public void write(ChannelHandlerContext ctx, Object msg,
                              ChannelPromise promise) throws Exception {
                if (msg instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<RemoteRxEvent> batch = (List<RemoteRxEvent>) msg;
                    ByteBuf buf = ctx.alloc().buffer();
                    writeHeader(buf, batch.get(0).getName());
                    for (RemoteRxEvent event : batch) {
                        writeBytesIntoBuf(event, buf);
                    }
                    super.write(ctx, buf, promise);
                    super.flush(ctx);
                } else {
                    super.write(ctx, msg, promise);
                }
            }
        });
    }

    private void writeHeader(ByteBuf buf, String name) {
        buf.writeByte(PROTOCOL_VERSION);
        String observableName = name;
        if (observableName != null && !observableName.isEmpty()) {
            // write length
            int nameLength = observableName.length();
            if (nameLength < 127) {
                buf.writeByte(nameLength);
                buf.writeBytes(observableName.getBytes());
            } else {
                throw new RuntimeException("observableName " + observableName +
                        " exceeds max limit of 127 characters");
            }
        } else {
            // no name provided, write 0 bytes for name length
            buf.writeByte(0);
        }
    }

    private void writeBytesIntoBuf(RemoteRxEvent event, ByteBuf buf) {

        if (event.getType() == RemoteRxEvent.Type.next) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: next");
            //			}
            byte[] data = event.getData();
            buf.writeInt(1 + data.length); // length, add additional byte for opcode
            buf.writeByte(1); // opcode
            buf.writeBytes(data);
        } else if (event.getType() == RemoteRxEvent.Type.error) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: error");
            //			}
            byte[] data = event.getData();
            buf.writeInt(1 + data.length); // length, add additional byte for opcode
            buf.writeByte(2); //opcode
            buf.writeBytes(data);
        } else if (event.getType() == RemoteRxEvent.Type.completed) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: completed");
            //			}
            buf.writeInt(1); // length
            buf.writeByte(3); // opcode
        } else if (event.getType() == RemoteRxEvent.Type.subscribed) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: subscribed");
            //			}
            Map<String, String> subscribeParameters = event.getSubscribeParameters();
            if (subscribeParameters != null && !subscribeParameters.isEmpty()) {
                byte[] subscribeBytes = fromMapToBytes(subscribeParameters);
                buf.writeInt(1 + 4 + subscribeBytes.length); // op code, subscribe bytes length, subscribe bytes
                buf.writeByte(4);
                buf.writeInt(subscribeBytes.length);
                buf.writeBytes(subscribeBytes);
            } else {
                buf.writeInt(2); //length
                buf.writeByte(4); //opcode
                buf.writeInt(0); // no data
            }
        } else if (event.getType() == RemoteRxEvent.Type.unsubscribed) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: unsubscribed");
            //			}
            buf.writeInt(1); // length
            buf.writeByte(5); //opcode
        } else if (event.getType() == RemoteRxEvent.Type.heartbeat) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: hearbeat");
            //			}
            buf.writeInt(1); // length
            buf.writeByte(6); //opcode
        } else if (event.getType() == RemoteRxEvent.Type.nonDataError) {
            //			if(logger.isDebugEnabled()) {
            //				logger.debug("WRITE request for RemoteRxEvent: nonDataError");
            //			}
            buf.writeInt(1); // length
            buf.writeByte(7); //opcode
            buf.writeBytes(event.getData());
        }
    }

}
