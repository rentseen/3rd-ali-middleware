package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;

public class DemoTester {


    public static void main(String[] args) {
        String topic = "TOPIC_17"; //实际测试时大概会有100个Topic左右
        String queue = "QUEUE_233"; //实际测试时，queue数目与消费线程数目相同
        {
            KeyValue properties = new DefaultKeyValue();
            properties.put("STORE_PATH", "/home/rensten/test1");
            Producer producer = new DefaultProducer(properties);



            int i=0;
            while(i<2){
                Message m1=producer.createBytesMessageToTopic(topic, "lexe1awdgiwz0".getBytes());
                Message m2=producer.createBytesMessageToQueue(queue, "x2b5c05fk2x".getBytes());
                Message m3=producer.createBytesMessageToQueue(queue, "wek2f1c5t01083clfi1zejahsbjrs5vcdkji68dde2k236r2d4v65fzds5vv7s8uhxv2qyskk54az56ydxly6yp9axdtbk1dfrax1q1wz9k6ejzwys9pvr4ba9cgudwu2lfu6pp1v56tes9vgy6iu0fsal3g7c98x209ztg363baswtcidueqy9r5a7lxc2ig3wbgpddd6g0r89rke7tk4cy9edr2wtev16efcvcbefdallrqszpichy6taq9re3iyu8wcdcwdl7xkyx1xk5r9ce58c2t3qzjg3sc1rhfz8ud1zxpdbakebsex4ds4t7fheu0jd1edzb0gd6phuu4shd890r5g53sp3a7q0ev01dr0l56cek2s84lhdzw4eadayhdzct30fdg80ib34le9pe9uu0xp193bfpu0bcj5e4q4qvd9g99u67vrd6r48rxwcd2jukizluech3s9ybezzrs7c9w12exc8h1brcd9xg5uperc04h1i91t0uzkdh0iu7bvqluxxclt2c2zax7crdibd0d2gpx1a518wfbqg082f1fe4cl59i1gubc7lftjkw9i40lr1ccge7bdyp54sz0klrl1gcheu54zygei1i7vrddk0kvljdceglgdfhdereus2k1xw6kgxpfqdtyz9bx2gjx6rsly27khpchier6q5i8s6wel51wudf44xhy2fkwu2c8qqeq5ujeck64lcjsti2x0dz6est7yx5e8a5ycpe608d6phxa33s85iud6pxtck3xg8rpw4des88tadg2wj7xkc9gejcvffaqa1hpif7jthvq0iwc4jr9zjgvpqdge064g88edzge8c6gjf7pykpeif55czgvv9ad9cg9p01s5wxat7ij0lct8dvq8fzi3dctrcjcdhdalg8eww5klqcblr5ubgag6dh9asc4jcwscgsug6i6dpbfeza4u2h37sj4rrl80t3k3ql77r3vy1p3dt69dgg32ptwvr".getBytes());

                m1.putHeaders("MessageId","lexe1awdgiwz0");
                m1.putProperties("PRO_OFFSET","PRODUCER1_12342");
                m1.putProperties("ellafi2","t0z8q1l");
                m1.putProperties("elk2hrj","t0zew6y");
                m1.putProperties("gci32g8","uvup30d");

                m2.putHeaders("MessageId","x2b5c05fk2x");
                m2.putProperties("PRO_OFFSET","PRODUCER3_1");
                m2.putProperties("hhh","haa");

                m3.putHeaders("MessageId","wek2f1c5t01083clfi1zejahsbjrs5vcdkji68dde2k236r2d4v65fzds5vv7s8uhxv2qyskk54az56ydxly6yp9axdtbk1dfrax1q1wz9k6ejzwys9pvr4ba9cgudwu2lfu6pp1v56tes9vgy6iu0fsal3g7c98x209ztg363baswtcidueqy9r5a7lxc2ig3wbgpddd6g0r89rke7tk4cy9edr2wtev16efcvcbefdallrqszpichy6taq9re3iyu8wcdcwdl7xkyx1xk5r9ce58c2t3qzjg3sc1rhfz8ud1zxpdbakebsex4ds4t7fheu0jd1edzb0gd6phuu4shd890r5g53sp3a7q0ev01dr0l56cek2s84lhdzw4eadayhdzct30fdg80ib34le9pe9uu0xp193bfpu0bcj5e4q4qvd9g99u67vrd6r48rxwcd2jukizluech3s9ybezzrs7c9w12exc8h1brcd9xg5uperc04h1i91t0uzkdh0iu7bvqluxxclt2c2zax7crdibd0d2gpx1a518wfbqg082f1fe4cl59i1gubc7lftjkw9i40lr1ccge7bdyp54sz0klrl1gcheu54zygei1i7vrddk0kvljdceglgdfhdereus2k1xw6kgxpfqdtyz9bx2gjx6rsly27khpchier6q5i8s6wel51wudf44xhy2fkwu2c8qqeq5ujeck64lcjsti2x0dz6est7yx5e8a5ycpe608d6phxa33s85iud6pxtck3xg8rpw4des88tadg2wj7xkc9gejcvffaqa1hpif7jthvq0iwc4jr9zjgvpqdge064g88edzge8c6gjf7pykpeif55czgvv9ad9cg9p01s5wxat7ij0lct8dvq8fzi3dctrcjcdhdalg8eww5klqcblr5ubgag6dh9asc4jcwscgsug6i6dpbfeza4u2h37sj4rrl80t3k3ql77r3vy1p3dt69dgg32ptwvr");
                m3.putProperties("PRO_OFFSET","PRODUCER3_13525");


                producer.send(m1);
                producer.send(m2);
                producer.send(m3);
                i++;
            }
            producer.flush();
        }

        {
            KeyValue properties = new DefaultKeyValue();
            PullConsumer consumer = new DefaultPullConsumer(properties);
            properties.put("STORE_PATH", "/home/rensten/test1");
            ArrayList<String> l=new ArrayList<>();
            l.add(topic);
            consumer.attachQueue(queue, l);

            while (true) {
                Message message = consumer.poll();
                //System.out.println(message.headers().toString());
                if (message == null) {
                    //拉取为null则认为消息已经拉取完毕
                    break;
                }
            }
        }



    }
}
