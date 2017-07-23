package com.alibaba.middleware.race.sync;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by rentseen on 17-6-26.
 */
public class DataSource {
    private int pos;
    private int len;
    private ByteBuffer buffer;
    public DataSource(){
        update();
    }
    public void update(){
        //Server.logger.info(""+Server.blockQueue.size());
        try {
            ByteBuffer bb=buffer;
            buffer=Server.blockQueue.take();
            pos=0;
            len=Server.blockSizeQueue.take();
            if(bb!=null){
                Server.blockBackQueue.put(bb);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public boolean skip(int step){
        if(pos+step<len){
            pos=pos+step;
            buffer.position(pos);

            return true;
        }
        else{
            step=step-(len-pos);
            update();
            if(len==0){
                System.out.println("fail");
                return false;
            }
            pos=pos+step;
            buffer.position(pos);
            return true;
        }
    }
    void nextLine(){
        while(true){
            pos++;
            byte tmp=buffer.get();
            if(pos==len){
                update();
            }
            if(tmp!='\n'){
                break;
            }
        }
    }

    boolean ifEqual(ArrayList<Byte> l){
        for(Byte b:l){
            pos++;
            byte tmp=buffer.get();
            if(pos==len){
                update();
            }
            if(b!=tmp){
                return false;
            }

        }
        return true;
    }
    public byte get(){
        if(pos<len){
            pos++;
            return buffer.get();
        }
        else{
            update();
            pos++;
            return buffer.get();
        }
    }
    /*public ArrayList<Byte> getArrayList(int step){
        ArrayList<Byte> result=new ArrayList<Byte>();
        if(pos+step<len){
            for(int i=0;i<step;i++){
                result.add(buffer.get());
            }
            pos=pos+step;
            return result;
        }
        else{
            int count=len-pos;
            for(int i=0;i<count;i++){
                result.add(buffer.get());
            }
            step=step-count;
            update();
            for(int i=0;i<step;i++){
                result.add(buffer.get());
            }
            pos=pos+step;
            return result;
        }
    }*/
}
