package com.alibaba.middleware.race.sync;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Yelinsheng on 17-6-10.
 */
public class MessageProducer implements Runnable {
    private long startId;
    private long endId;
    Map<Long, ArrayList<ArrayList<Byte>>> database;
    Map<Integer,Integer> indexOfName;

    public MessageProducer(long start, long end,Map<Long, ArrayList<ArrayList<Byte>>> database,Map<Integer,Integer> indexOfName){
        this.startId=start;
        this.endId=end;
        this.database=database;
        this.indexOfName=indexOfName;
    }

    long parseLong(ArrayList<Byte> l){
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<l.size();i++){
            sb.append((char)l.get(i).byteValue());
        }
        if(sb.toString().equals("￥ﾾﾐ")){
            int a=0;
        }
        return Long.parseLong(sb.toString());
    }

    public void run(){
        Server.logger.info("producer start");
        int colNum=indexOfName.size();
        long id;
        Object[] key =  database.keySet().toArray();
        Arrays.sort(key);
        for(int k=0; k<key.length;k++){
            ArrayList<ArrayList<Byte>> l=database.get(key[k]);
            //todo 假设id在第一个位置
            id=(long)key[k];
            if(id<endId && id>startId){
                StringBuilder sb=new StringBuilder();
                int i=0;
                for(i=0;i<colNum-1;i++){
                    byte[] b=new byte[l.get(i).size()];
                    for(int j=0;j<b.length;j++){
                        b[j]=l.get(i).get(j);
                    }
                    try {
                        sb.append(new String(b,"utf-8")).append('\t');
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                byte[] b=new byte[l.get(i).size()];
                for(int j=0;j<b.length;j++){
                    b[j]=l.get(i).get(j);
                }
                try {
                    sb.append(new String(b,"utf-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                try {
                    Server.logQueue.put(sb.toString());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            Server.logQueue.put("-");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Server.logger.info("producer end");
    }
}
