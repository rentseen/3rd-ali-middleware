package com.alibaba.middleware.race.sync;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rentseen on 17-6-25.
 */
public class LogParser implements Runnable {

    private ArrayList<Byte> schema;
    private ArrayList<Byte> table;
    private long startId;
    private long endId;
    StringBuilder parseLongSb=new StringBuilder();
    ArrayList<Byte> name=new ArrayList<Byte>(4);
    ArrayList<Byte> value=new ArrayList<Byte>(4);
    ArrayList<Byte> idValue=new ArrayList<Byte>(4);

    public Map<Integer,Integer> indexOfName=new HashMap<>();
    public Map<Long, ArrayList<ArrayList<Byte>>> database =new HashMap<>();
    public List<Integer> nameLength=new ArrayList<>();
    public DataSource ds;

    public LogParser(String schema, String table, long start, long end){
        StringBuilder sb=new StringBuilder();
        sb.append(schema).append('|').append(table).append('|').append(start).append('|').append(end);
        try {
            Server.logQueue.put(sb.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.schema=new ArrayList<>(4);
        for(int i=0;i<schema.length();i++){
            this.schema.add((byte)schema.charAt(i));
        }
        this.table=new ArrayList<>(4);
        for(int i=0;i<table.length();i++){
            this.table.add((byte)table.charAt(i));
        }
        this.startId=start;
        this.endId=end;
        ds=new DataSource();
    }


    long parseLong(ArrayList<Byte> l){
        parseLongSb.delete(0,parseLongSb.length());
        for(int i=0;i<l.size();i++){
            parseLongSb.append((char)l.get(i).byteValue());
        }
        return Long.parseLong(parseLongSb.toString());
    }

    boolean canalLogParse(){
        //跳过log id，时间戳
        if(!ds.skip(41)){
            return false;
        }
        if(!ds.ifEqual(schema)){
            ds.nextLine();
            return true;
        }
        ds.skip(1);
        if(!ds.ifEqual(table)){
            ds.nextLine();
            return true;
        }
        ds.skip(1);
        byte type=ds.get();
        ds.skip(1);
        if(indexOfName.size()==0){
            //Server.logger.info("yelin: "+line);
            //初始化table信息
            ArrayList<ArrayList<Byte>> node=new ArrayList<ArrayList<Byte>>(4);

            int count=0;
            boolean ifId=true;
            byte c;
            while(true){
                c=ds.get();
                if(c=='\n'){
                    break;
                }
                name.clear();
                value=new ArrayList<>(2);
                while(true){
                    if(c==':'){
                        indexOfName.put(name.hashCode(),count);
                        nameLength.add(name.size());
                        break;
                    }
                    else {
                        name.add(c);
                    }
                    c=ds.get();
                }
                ds.skip(9);

                while (true){
                    c=ds.get();
                    if(c=='|'){
                        node.add(value);
                        break;
                    }
                    else {
                        value.add(c);
                    }
                }
                if(ifId){
                    database.put(parseLong(value),node);
                    ifId=false;
                }
                count=count+1;
            }
            return true;
        }

        if(type=='I'){
            ArrayList<ArrayList<Byte>> node=new ArrayList<ArrayList<Byte>>(4);
            boolean ifId=true;
            byte c;
            for(int step:nameLength){
                value=new ArrayList<>(2);
                ds.skip(step+10);
                while (true){
                    c=ds.get();
                    if(c=='|'){
                        node.add(value);
                        break;
                    }
                    else {
                        value.add(c);
                    }
                }
                if(ifId){
                    database.put(parseLong(value),node);
                    ifId=false;
                }
            }
            return ds.skip(1);
        } else if(type=='U'){
            ArrayList<ArrayList<Byte>> node=null;

            boolean ifId=true;
            byte c;
            int index;
            Long id=null;
            Long newId=null;
            //todo 默认第一个列是id，不知道是否会出bug

            while(true){
                c=ds.get();
                if(c=='\n'){
                    break;
                }
                name.clear();
                while(true){
                    if(c==':'){
                        index=indexOfName.get(name.hashCode());
                        break;
                    }
                    else {
                        name.add(c);
                    }
                    c=ds.get();
                }
                ds.skip(4);
                if(ifId){
                    idValue.clear();
                }
                while(true){
                    c=ds.get();
                    if(c=='|'){
                        if(ifId){
                            id=parseLong(idValue);
                            node=database.get(id);
                            //test
                            if(node==null){
                                int a=0;
                            }
                        }
                        break;
                    }
                    else {
                        if(ifId){
                            idValue.add(c);
                        }
                    }
                }

                //ds.skip(1);
                value=node.get(index);
                value.clear();
                while(true){
                    c=ds.get();
                    if(c=='|'){
                        break;
                    }
                    else {
                        value.add(c);
                    }
                }

                if(ifId){
                    newId=parseLong(value);
                    if(!id.equals(newId)){
                        database.remove(id);
                        database.put(newId,node);
                    }
                    ifId=false;
                }
            }
            return true;
        } else if(type=='D'){
            idValue.clear();
            byte c;
            Long id=null;
            boolean ifId=true;

            for(int step:nameLength){
                ds.skip(step+5);
                while (true){
                    c=ds.get();
                    if(c=='|'){
                        break;
                    }
                    if(ifId){
                        idValue.add(c);
                    }
                }
                if(ifId){
                    id=parseLong(idValue);
                    ArrayList<ArrayList<Byte>> node=database.get(id);
                    node.clear();
                    database.remove(id);
                    ifId=false;
                }
                ds.skip(5);
            }
            return ds.skip(1);
        }
        return true;
    }
    public void run(){
        Server.logger.info("parser start");
        //int count=0;
        while(canalLogParse()){
            /*count++;
            if(count%1000000==0){
                System.out.println("parse");
            }*/
        }

        /*try {
            Server.logQueue.put("-");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        Thread producer=new Thread(new MessageProducer(startId,endId,database,indexOfName));
        producer.start();
        Server.logger.info("parser end");
    }
}
