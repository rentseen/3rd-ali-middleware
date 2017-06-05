package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
/**
 * Created by rentseen on 17-5-26.
 */
public class ConsumerTester implements Runnable{
    private String id;
    private KeyValue properties = new DefaultKeyValue();
    PullConsumer consumer = new DefaultPullConsumer(properties);
    public ConsumerTester(String id){
        this.id=id;
        properties.put("STORE_PATH", "/home/rensten/test1");
    }
    public void run(){
        String topic1 = "TOPIC_1111111"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC_2111111"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE_1111111"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE_2111111"; //实际测试时，queue数目与消费线程数目相同
        String topic3 = "TOPIC_3111111"; //实际测试时大概会有100个Topic左右
        String topic4 = "TOPIC_4111111"; //实际测试时大概会有100个Topic左右
        String queue3 = "QUEUE_3111111"; //实际测试时，queue数目与消费线程数目相同
        String queue4 = "QUEUE_4111111"; //实际测试时，queue数目与消费线程数目相同
        ArrayList<String> l=new ArrayList<>();
        l.add(topic1);
        l.add(topic2);
        l.add(topic3);
        l.add(topic4);
        consumer.attachQueue(queue4, l);



        long startConsumer = System.currentTimeMillis();
        int count=0;
        while (true) {
            Message message = consumer.poll();
            //System.out.println(message.headers().toString());
            if (message == null) {
                //拉取为null则认为消息已经拉取完毕
                break;
            }
            else{
                //System.out.println(message.toString());
                if(id.equals("1")){
                    //System.out.println(message.headers().getString(MessageHeader.QUEUE));
                }
            }
            String topic = message.headers().getString(MessageHeader.TOPIC);
            String queue = message.headers().getString(MessageHeader.QUEUE);
            count++;
        }
        System.out.println(count);
        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;
    }
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Thread t1=new Thread(new ConsumerTester(""+0));
        //Thread t2=new Thread(new ConsumerTester(""+1));
        //Thread t3=new Thread(new ConsumerTester(""+2));
        t1.start();
        //t2.start();
        //t3.start();
        try {
            t1.join();
            //t2.join();
            //t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        long T1 = end - start;
        System.out.println(T1);
    }
}
