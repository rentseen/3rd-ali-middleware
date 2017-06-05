package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageReader messageRead = new MessageReader();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    private int index=0;
    private boolean flag=false;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageRead.setProterties(properties);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public synchronized Message poll() {
        /*if (buckets.size() == 0 || queue == null) {
            return null;
        }
        for (int i = 0; i < bucketList.size(); i++) {
            Message message = messageRead.pullMessage(queue, bucketList.get(i));
            if (message != null) {
                return message;
            }
        }
        return null;*/
        //由于每个topic会由多个消费者消费，所以消费者先轮流消费topic，每次每个topic读一个message，下次换下一个topic，这样能保证各个消费者的消费进度相似，从而提高文件系统里缓存的命中率。
        //由于queue只由绑定的消费者消费，所以消费者等消费完topic list之后再消费queue
        if (buckets.size() == 0 && queue == null ) {
            return null;
        }
        if(flag==false){
            int count=0;
            int size=bucketList.size();
            while(count<size){
                Message message = messageRead.pullMessage(queue, bucketList.get(index));
                if(message==null){
                    bucketList.remove(index);
                    count++;
                }
                else{
                    index=(index+1)%bucketList.size();
                    return message;
                }
            }
            flag=true;
        }
        Message message = messageRead.pullMessage(queue, queue);
        if(message==null){
            return null;
        }
        else{
            return message;
        }
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }
}
