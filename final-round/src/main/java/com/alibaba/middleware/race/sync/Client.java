package com.alibaba.middleware.race.sync;

import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

    private final static int port = Constants.SERVER_PORT;

    public static PrintWriter pw;
    private static String ip;
    public static String schema;
    public static String table;
    public static long startId;
    public static long endId;

    public static Map<String,Integer> indexOfName=new HashMap<>();
    public static Map<Long, List<String>> database =new HashMap<>();

    public static Logger logger = LoggerFactory.getLogger(Client.class);

    public static EventLoopGroup workerGroup;

    private EventLoopGroup loop = new NioEventLoopGroup();

    public Client(){
        try {
            pw=new PrintWriter(Constants.RESULT_HOME+"/"+Constants.RESULT_FILE_NAME);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        initProperties();

        logger.info("yelin: Welcome");
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    //ch.pipeline().addLast(new IdleStateHandler(5, 0, 0));
                    //ch.pipeline().addLast(new ClientIdleEventHandler());
                    //todo: maxlength 应该是字符的长度
                    ch.pipeline().addLast(new LineBasedFrameDecoder(1024));
                    ch.pipeline().addLast(new StringDecoder());
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });

            // Start the client.
            //ChannelFuture f = b.connect(host, port).sync();
            ChannelFuture f;
            while (true)
            {
                f = b.connect(host, port);
                f.awaitUninterruptibly();
                if (f.isSuccess())
                {
                    break;
                }
                else {
                    Thread.sleep(100);
                }
            }

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }

    }


}
