package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.*;
import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Created by rentseen on 17-5-26.
 */
public class MessageReader {

    // created by yelinshegn
    private KeyValue properties = new DefaultKeyValue();

    //created by yelinsheng
    public void setProterties(KeyValue p){
        this.properties=p;
    }
    
    private Map<String, List<BufferedReader>> messageRead=new HashMap<>();
    private Map<String,Integer> messageIndex=new HashMap<>();
    private int fileNum=5;

    /*public KeyValue readKeyValue(BufferedReader reader){
        String line="";
        try {
            line=reader.readLine();
            if(line==null){
                return null;
            }
        } catch (IOException e) {
            return null;
        }
        DefaultKeyValue kv=new DefaultKeyValue();
        int num=Integer.parseInt(line);
        for(int i=0;i<num;i++){
            String key="";
            String value="";
            try {
                key=reader.readLine();
                value=reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(key.charAt(0)=='1'){
                kv.getKvs().put(key.substring(1),Integer.parseInt(value));
            } else if(key.charAt(0)=='2'){
                kv.getKvs().put(key.substring(1),Long.parseLong(value));
            }else if(key.charAt(0)=='3'){
                kv.getKvs().put(key.substring(1),Double.parseDouble(value));
            }else if(key.charAt(0)=='4'){
                kv.getKvs().put(key.substring(1),value);
            }
        }
        return kv;
    }*/

    /*public Message getMessage(BufferedReader reader){
        String line;
        try {
            line=reader.readLine();
            if(line==null){
                return null;
            }
        } catch (IOException e) {
            return null;
        }
        DefaultBytesMessage message = new DefaultBytesMessage(line.getBytes());
        *//*
        message.setHeaders(readKeyValue(reader));
        message.setProperties(readKeyValue(reader));*//*
        try {
            line=reader.readLine();
            if(line==null){
                return null;
            }
        } catch (IOException e) {
            return null;
        }
        int pos = 0, end;
        boolean f = true;
        String key = "";
        DefaultKeyValue  h = new DefaultKeyValue();
        while ((end = line.indexOf('@', pos)) >= 0) {
            if(f){
                key = line.substring(pos, end);
                f = false;
            }
            else{
                String value = line.substring(pos+1, end);
                if(line.charAt(pos)=='1'){
                    h.getKvs().put(key, Integer.parseInt(value));
                }
                else if(line.charAt(pos)=='2'){
                    h.getKvs().put(key, Long.parseLong(value));
                }
                else if(line.charAt(pos)=='3'){
                    h.getKvs().put(key, Double.parseDouble(value));
                }
                else{
                    h.getKvs().put(key, value);
                }
                f= true;
            }
            pos = end + 1;
        }
        message.setHeaders(h);
        try {
            line=reader.readLine();
            if(line==null||line==""){
                return message;
            }
        } catch (IOException e) {
            return null;
        }
        pos = 0;
        f = true;
        key = "";
        DefaultKeyValue p = new DefaultKeyValue();
        while ((end = line.indexOf('@', pos)) >= 0) {
            if(f){
                key = line.substring(pos, end);
                f = false;
            }
            else{
                String value = line.substring(pos+1, end);
                if(line.charAt(pos)=='1'){
                    p.getKvs().put(key, Integer.parseInt(value));
                }
                else if(line.charAt(pos)=='2'){
                    p.getKvs().put(key, Long.parseLong(value));
                }
                else if(line.charAt(pos)=='3'){
                    p.getKvs().put(key, Double.parseDouble(value));
                }
                else{
                    p.getKvs().put(key, value);
                }
                f= true;
            }
            pos = end + 1;
        }
        message.setProperties(p);
        return message;
*//*
        String []tmp=line.split(",");
        byte[] body=tmp[1].getBytes();
        Message message = new DefaultBytesMessage(body);

        //todo value类型的问题
        StringBuilder l=new StringBuilder(tmp[0]);
        l.deleteCharAt(0);
        l.deleteCharAt(l.length()-1);
        String []pairs=new String(l).split(",");
        for(String pair:pairs){
            String []kv=pair.split("=");
            message.putHeaders(kv[0],kv[1]);
        }
        *//*
    }*/

    //由于每条message经过压缩，且有规律所寻，所以读取时可以直接操作String的index来解析message，从而降低字符串处理的时间复杂度。
    public Message getMessage(BufferedReader reader){
        String line;
        try {
            line=reader.readLine();
            if(line==null){
                return null;
            }
        } catch (IOException e) {
            return null;
        }
        //System.out.println(line);
        Message message;
        if(line.charAt(13)==';'){
            message=new DefaultBytesMessage(line.substring(0,13).getBytes());
            int pos=14;
            message.putHeaders("MessageId",line.substring(pos,pos+13));
            pos=pos+13;

            int base=pos;
            while(line.charAt(pos)!=',' && line.charAt(pos)!='.') pos++;
            if(line.charAt(pos)=='.'){
                message.putHeaders("Queue","QUEUE_"+line.substring(base,pos));
            }
            else{
                message.putHeaders("Topic","TOPIC_"+line.substring(base,pos));
            }

            pos=pos+1;
            base=pos;
            while(pos<line.length() && line.charAt(pos)!=',') pos++;
            message.putProperties("PRO_OFFSET","PRODUCER"+line.charAt(base)+"_"+line.substring(base+1,pos));

            if(pos<line.length()){
                int key_length=-1;
                pos=pos+1;
                base=pos;
                int end=line.length();

                while(pos<end){
                    pos=pos+1;
                    if(line.charAt(pos)==','){
                        key_length=pos-base;
                        break;
                    }
                }
                message.putProperties(line.substring(base,pos),line.substring(pos+1,pos+1+key_length));

                pos=pos+1+key_length;
                while(pos<end){
                    message.putProperties(line.substring(pos,pos+key_length),line.substring(pos+key_length,pos+2*key_length));
                    pos=pos+2*key_length;
                }
            }
        }
        /*else if(line.charAt(1003)==';'){
            message=new DefaultBytesMessage(line.substring(0,1003).getBytes());
            int pos=1006;
            while(line.charAt(pos)!=',') pos++;
            if(line.charAt(1004)=='q'){
                message.putHeaders("Queue","QUEUE_"+line.substring(1005,pos));
            }
            else{
                message.putHeaders("Topic","TOPIC_"+line.substring(1005,pos));
            }
            pos=pos+1;
            message.putHeaders("MessageId",line.substring(pos,pos+1003));
            pos=pos+1004;
            message.putProperties("PRO_OFFSET","PRODUCER"+line.substring(pos,pos+7));
            pos=pos+8;
            int base=pos;
            int mid=base;
            int end=line.length()-1;
            while(pos<end){
                pos=pos+1;
                if(line.charAt(pos)==':'){
                    mid=pos;
                }
                if(line.charAt(pos)==','){
                    message.putProperties(line.substring(base,mid),line.substring(mid+1,pos));
                    base=pos+1;
                }
            }
        }*/
        else{
            //System.out.println(line);
            int pos=0;
            while(line.charAt(pos)!=';') pos++;
            int body_length=pos;

            message=new DefaultBytesMessage(line.substring(0,body_length).getBytes());
            pos=body_length+1;
            message.putHeaders("MessageId",line.substring(pos,pos+body_length));
            pos=pos+body_length;

            int base=pos;
            while(line.charAt(pos)!=',' && line.charAt(pos)!='.') pos++;
            if(line.charAt(pos)=='.'){
                message.putHeaders("Queue","QUEUE_"+line.substring(base,pos));
            }
            else{
                message.putHeaders("Topic","TOPIC_"+line.substring(base,pos));
            }

            pos=pos+1;
            base=pos;
            while(pos<line.length() && line.charAt(pos)!=',') pos++;
            message.putProperties("PRO_OFFSET","PRODUCER"+line.charAt(base)+"_"+line.substring(base+1,pos));

            if(pos<line.length()){
                int key_length=-1;
                pos=pos+1;
                base=pos;
                int end=line.length();

                while(pos<end){
                    pos=pos+1;
                    if(line.charAt(pos)==','){
                        key_length=pos-base;
                        break;
                    }
                }
                message.putProperties(line.substring(base,pos),line.substring(pos+1,pos+1+key_length));

                pos=pos+1+key_length;
                while(pos<end){
                    message.putProperties(line.substring(pos,pos+key_length),line.substring(pos+key_length,pos+2*key_length));
                    pos=pos+2*key_length;
                }
            }

            /*message=new DefaultBytesMessage(line.substring(0,length).getBytes());
            pos=pos+2;
            int base=pos;
            while(line.charAt(pos)!=',') pos++;
            if(line.charAt(base-1)=='q'){
                message.putHeaders("Queue","QUEUE_"+line.substring(base,pos));
            }
            else{
                message.putHeaders("Topic","TOPIC_"+line.substring(base,pos));
            }

            pos=pos+1;
            message.putHeaders("MessageId",line.substring(pos,pos+length));
            pos=pos+length+1;

            base=pos;
            while(line.charAt(pos)!=',') pos++;
            message.putProperties("PRO_OFFSET","PRODUCER"+line.substring(base,pos));
            pos=pos+1;
            base=pos;
            int mid=base;
            int end=line.length()-1;
            while(pos<end){
                pos=pos+1;
                if(line.charAt(pos)==':'){
                    mid=pos;
                }
                if(line.charAt(pos)==','){
                    try {
                        message.putProperties(line.substring(base,mid),line.substring(mid+1,pos));
                    } catch (StringIndexOutOfBoundsException e){
                        System.out.println();
                    }
                    base=pos+1;
                }
            }*/
        }
        return message;
    }

    //一个bucket由多个文件保持，消费时轮流消费，提高文件系统里缓存的命中率
    //读用BufferedReader，并设置其buffer大小
    public Message pullMessage(String queue, String bucket) {
        List<BufferedReader> rdl = messageRead.get(bucket);
        if (rdl == null) {
            try {
                rdl=new ArrayList<>();
                //rdl.add(new BufferedReader(new FileReader(properties.getString("STORE_PATH")+"/"+bucket+"1"),131072));
                //rdl.add(new BufferedReader(new FileReader(properties.getString("STORE_PATH")+"/"+bucket+"2"),131072));
                //rdl.add(new BufferedReader(new FileReader(properties.getString("STORE_PATH")+"/"+bucket+"3"),131072));
                for(int i=0;i<fileNum;i++){
                    //rdl.add(new BufferedReader(new FileReader(properties.getString("STORE_PATH")+"/"+bucket+i)));
                    rdl.add(new BufferedReader(new FileReader(properties.getString("STORE_PATH")+"/"+bucket+i),131072));
                }
                messageRead.put(bucket,rdl);
                messageIndex.put(bucket,0);
            } catch (FileNotFoundException e) {
                return null;
            }
        }
        int count=0;
        int size=rdl.size();
        Message message=null;
        int index=messageIndex.get(bucket);
        while(count<size){
            BufferedReader reader=rdl.get(index);
            message=getMessage(reader);
            if(message!=null){
                messageIndex.put(bucket,(index+1)%rdl.size());
                break;
            }
            else{
                count++;
                rdl.remove(index);
                if(count<size){
                    index=index%rdl.size();
                }
            }

        }
        return message;
    }
}
