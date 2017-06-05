package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private KeyValue properties = new DefaultKeyValue();
    private int fileNum=5;

    //add by yelinsheng
    // private static String baseDir="/home/admin/test/";

    //created by yelinsheng
    public void setProterties(KeyValue p){
        this.properties=p;
    }
    
    public static MessageStore getInstance() {
        return INSTANCE;
    }

    //add by yelinsheng
    private Map<String, List<PrintWriter>> messageWrite=new HashMap<>();

    /*//revised by yelinsheng
    public synchronized void putMessage(String bucket, Message message){
        //todo PrintWrte关闭的问题
        if (!messageWrite.containsKey(bucket)) {
            try {
                messageWrite.put(bucket, new PrintWriter(properties.getString("STORE_PATH")+"/"+bucket));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        PrintWriter pw = messageWrite.get(bucket);
        pw.println(message.toString());
        pw.println(message.headers().toString());
        pw.println(message.properties()==null?"":message.properties().toString());
    }*/

    //revised by yelinsheng
    //一个bucket包含多个文件，将消费者根据其id来哈希到不同的文件，这样可以降低多个消费者在写同一个bucket时由锁带来的时间开销
    //写使用PrintWriter，并设置其buffer大小
    public void putMessage(String bucket, Message message,int id){
        //todo PrintWrte关闭的问题
        synchronized (this){
            if (!messageWrite.containsKey(bucket)) {
                try {
                    List<PrintWriter> pwl=new ArrayList<>();
                    //pwl.add(new PrintWriter(new BufferedWriter(new FileWriter(properties.getString("STORE_PATH")+"/"+bucket+"1"),16384)));
                    //pwl.add(new PrintWriter(new BufferedWriter(new FileWriter(properties.getString("STORE_PATH")+"/"+bucket+"2"),16384)));
                    //pwl.add(new PrintWriter(new BufferedWriter(new FileWriter(properties.getString("STORE_PATH")+"/"+bucket+"3"),16384)));
                    for(int i=0;i<fileNum;i++){
                        pwl.add(new PrintWriter(new BufferedWriter(new FileWriter(properties.getString("STORE_PATH")+"/"+bucket+i),65536)));
                    }
                    messageWrite.put(bucket, pwl);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        List<PrintWriter> pwl = messageWrite.get(bucket);
        PrintWriter pw=pwl.get(id%fileNum);
        synchronized (pw){
            //System.out.println(message.toString());
            pw.write(message.toString());
            pw.write('\n');
        }
    }

    public synchronized void flushWrite(){
        for(List<PrintWriter> pwl: messageWrite.values()){
            for(PrintWriter pw:pwl){
                pw.flush();
            }
        }
    }
}
