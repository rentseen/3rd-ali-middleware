package com.alibaba.middleware.race.sync;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by wanshao on 2017/5/25.
 */

public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static boolean initFlag=true;
    //private static int count=0;

    public void store(String line){
        Client.pw.write(line);
        Client.pw.write('\n');
    }

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        //logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelRead");
        String line=null;
        //开始
        if(initFlag){
            initFlag=false;
            line = (String) msg;
            Client.logger.info(line);
            int base=0;
            int i=1;
            int len=line.length();
            for(;i<len;i++){
                if(line.charAt(i)=='|'){
                    Client.schema=line.substring(base,i);
                    break;
                }
            }
            i=i+1;
            base=i;
            for(;i<len;i++){
                if(line.charAt(i)=='|'){
                    Client.table=line.substring(base,i);
                    break;
                }
            }
            i=i+1;
            base=i;
            for(;i<len;i++){
                if(line.charAt(i)=='|'){
                    Client.startId=Long.parseLong(line.substring(base,i));
                    break;
                }
            }
            i=i+1;
            base=i;
            Client.endId=Long.parseLong(line.substring(base,len));
            return;
        }
        else{
            line = (String) msg;
            Client.logger.info(line);
            //结束
            if(line.charAt(0)=='-'){
                Client.pw.flush();
                Client.workerGroup.shutdownGracefully();
                return;
            }
            //count++;
            store(line);
        }
        //ctx.writeAndFlush("I have received your messages and wait for next messages");
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Client.logger.info("yelin: channelActive");
        String msg = "produce";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }
}
