package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import org.junit.Assert;

/**
 * Created by yelinsheng on 17-5-25.
 */
public class ProducerTester implements Runnable{

    private String id;
    private KeyValue properties = new DefaultKeyValue();
    private Producer producer = new DefaultProducer(properties);

    public ProducerTester(String id){
        this.id=id;
        properties.put("STORE_PATH", "/home/rensten/test1");
    }

    public void run(){
        //构造测试数据
        String topic1 = "TOPIC_1111111"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC_2111111"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE_1111111"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE_2111111"; //实际测试时，queue数目与消费线程数目相同
        String topic3 = "TOPIC_3111111"; //实际测试时大概会有100个Topic左右
        String topic4 = "TOPIC_4111111"; //实际测试时大概会有100个Topic左右
        String queue3 = "QUEUE_3111111"; //实际测试时，queue数目与消费线程数目相同
        String queue4 = "QUEUE_4111111"; //实际测试时，queue数目与消费线程数目相同

        int i=0;
        while(i<2){
            Message m1=producer.createBytesMessageToTopic(topic1, ("TOPIC_" +i +"111").getBytes());
            Message m2=producer.createBytesMessageToTopic(topic2, ("TOPIC_" + i+"111111").getBytes());
            Message m3=producer.createBytesMessageToQueue(queue1, ("QUEUE_" + i+"111111").getBytes());
            Message m4=producer.createBytesMessageToQueue(queue2, ("QUEUE_" + i+"111111").getBytes());
            Message m5=producer.createBytesMessageToTopic(topic3, ("TOPIC_" + i+"111111").getBytes());
            Message m6=producer.createBytesMessageToTopic(topic4, ("TOPIC_" + i+"111111").getBytes());
            Message m7=producer.createBytesMessageToQueue(queue3, ("QUEUE_" + i+"111111").getBytes());
            Message m8=producer.createBytesMessageToQueue(queue4, "wek2f1c5t01083clfi1zejahsbjrs5vcdkji68dde2k236r2d4v65fzds5vv7s8uhxv2qyskk54az56ydxly6yp9axdtbk1dfrax1q1wz9k6ejzwys9pvr4ba9cgudwu2lfu6pp1v56tes9vgy6iu0fsal3g7c98x209ztg363baswtcidueqy9r5a7lxc2ig3wbgpddd6g0r89rke7tk4cy9edr2wtev16efcvcbefdallrqszpichy6taq9re3iyu8wcdcwdl7xkyx1xk5r9ce58c2t3qzjg3sc1rhfz8ud1zxpdbakebsex4ds4t7fheu0jd1edzb0gd6phuu4shd890r5g53sp3a7q0ev01dr0l56cek2s84lhdzw4eadayhdzct30fdg80ib34le9pe9uu0xp193bfpu0bcj5e4q4qvd9g99u67vrd6r48rxwcd2jukizluech3s9ybezzrs7c9w12exc8h1brcd9xg5uperc04h1i91t0uzkdh0iu7bvqluxxclt2c2zax7crdibd0d2gpx1a518wfbqg082f1fe4cl59i1gubc7lftjkw9i40lr1ccge7bdyp54sz0klrl1gcheu54zygei1i7vrddk0kvljdceglgdfhdereus2k1xw6kgxpfqdtyz9bx2gjx6rsly27khpchier6q5i8s6wel51wudf44xhy2fkwu2c8qqeq5ujeck64lcjsti2x0dz6est7yx5e8a5ycpe608d6phxa33s85iud6pxtck3xg8rpw4des88tadg2wj7xkc9gejcvffaqa1hpif7jthvq0iwc4jr9zjgvpqdge064g88edzge8c6gjf7pykpeif55czgvv9ad9cg9p01s5wxat7ij0lct8dvq8fzi3dctrcjcdhdalg8eww5klqcblr5ubgag6dh9asc4jcwscgsug6i6dpbfeza4u2h37sj4rrl80t3k3ql77r3vy1p3dt69dgg32ptwvr".getBytes());

            m1.putHeaders("MessageId","we1t4y8heh");
            m1.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m1.putProperties("h","haa");

            m2.putHeaders("MessageId","we2t4y8656heh");
            m2.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m2.putProperties("h","haa");

            m3.putHeaders("MessageId","we1t4y8656heh");
            m3.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m3.putProperties("h","haa");

            m4.putHeaders("MessageId","we2t4y8656heh");
            m4.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m4.putProperties("h","haa");

            m5.putHeaders("MessageId","we1t4y8656heh");
            m5.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m5.putProperties("h","haa");

            m6.putHeaders("MessageId","we2t4y8656heh");
            m6.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m6.putProperties("h","haa");

            m7.putHeaders("MessageId","we2t4y8656heh");
            m7.putProperties("PRO_OFFSET","PRODUCER3_13525");


            m8.putHeaders("MessageId","wek2f1c5t01083clfi1zejahsbjrs5vcdkji68dde2k236r2d4v65fzds5vv7s8uhxv2qyskk54az56ydxly6yp9axdtbk1dfrax1q1wz9k6ejzwys9pvr4ba9cgudwu2lfu6pp1v56tes9vgy6iu0fsal3g7c98x209ztg363baswtcidueqy9r5a7lxc2ig3wbgpddd6g0r89rke7tk4cy9edr2wtev16efcvcbefdallrqszpichy6taq9re3iyu8wcdcwdl7xkyx1xk5r9ce58c2t3qzjg3sc1rhfz8ud1zxpdbakebsex4ds4t7fheu0jd1edzb0gd6phuu4shd890r5g53sp3a7q0ev01dr0l56cek2s84lhdzw4eadayhdzct30fdg80ib34le9pe9uu0xp193bfpu0bcj5e4q4qvd9g99u67vrd6r48rxwcd2jukizluech3s9ybezzrs7c9w12exc8h1brcd9xg5uperc04h1i91t0uzkdh0iu7bvqluxxclt2c2zax7crdibd0d2gpx1a518wfbqg082f1fe4cl59i1gubc7lftjkw9i40lr1ccge7bdyp54sz0klrl1gcheu54zygei1i7vrddk0kvljdceglgdfhdereus2k1xw6kgxpfqdtyz9bx2gjx6rsly27khpchier6q5i8s6wel51wudf44xhy2fkwu2c8qqeq5ujeck64lcjsti2x0dz6est7yx5e8a5ycpe608d6phxa33s85iud6pxtck3xg8rpw4des88tadg2wj7xkc9gejcvffaqa1hpif7jthvq0iwc4jr9zjgvpqdge064g88edzge8c6gjf7pykpeif55czgvv9ad9cg9p01s5wxat7ij0lct8dvq8fzi3dctrcjcdhdalg8eww5klqcblr5ubgag6dh9asc4jcwscgsug6i6dpbfeza4u2h37sj4rrl80t3k3ql77r3vy1p3dt69dgg32ptwvr");
            m8.putProperties("PRO_OFFSET","PRODUCER3_13525");
            m8.putProperties("h","haa");

            producer.send(m1);
            producer.send(m2);
            producer.send(m3);
            producer.send(m4);
            producer.send(m5);
            producer.send(m6);
            producer.send(m7);
            producer.send(m8);
            i++;
        }
        producer.flush();
    }
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Thread t1=new Thread(new ProducerTester(""+0));
        Thread t2=new Thread(new ProducerTester(""+1));
        Thread t3=new Thread(new ProducerTester(""+2));
        t1.start();
        t2.start();
        t3.start();
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        long T1 = end - start;
        System.out.println(T1);
    }
}
