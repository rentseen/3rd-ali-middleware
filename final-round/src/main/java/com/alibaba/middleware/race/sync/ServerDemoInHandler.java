package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static String ipString=null;
    private static ByteBuf byteBuf=null;


    /**
     * 根据channel
     * 
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    public void comsumeCanalLog(){
        Channel channel = Server.getMap().get(ipString);
        StringBuilder sb=new StringBuilder();
        String line=null;
        //int count=0;
        /*while(true){
            //count++;
            sb.delete(0,sb.length());
            try {
                line=Server.logQueue.take();
                if(line==null){
                    break;
                }
                sb.append(line).append('\n');
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            byteBuf = Unpooled.wrappedBuffer(sb.toString().getBytes());
            channel.writeAndFlush(byteBuf);
            //System.out.println(count);
        }*/
        try {
            line=Server.logQueue.take();
            if(line==null){
                byteBuf.release();
                return;
            }
            sb.append(line).append('\n');
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        byteBuf = Unpooled.wrappedBuffer(String.valueOf(sb.toString()).getBytes());
        channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                //System.out.println(Thread.currentThread().getId());
                comsumeCanalLog();
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 保存channel
        ipString=getIPString(ctx);
        Server.getMap().put(ipString, ctx.channel());

        Server.logger.info("yelin: channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        Server.logger.info("yelin: Client said:" + resultStr);

        comsumeCanalLog();

        /*while (true) {
            // 向客户端发送消息
            String message = (String) getMessage();
            if (message != null) {
                Channel channel = Server.getMap().get("127.0.0.1");
                ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes());
                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Server发送消息成功！");
                    }
                });
            }
        }*/
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

   /* private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
        Thread.sleep(5000);

        return "message generated in ServerDemoInHandler";

    }*/
}
