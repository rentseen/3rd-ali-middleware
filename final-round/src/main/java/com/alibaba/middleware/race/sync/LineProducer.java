/*
package com.alibaba.middleware.race.sync;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

*/
/**
 * Created by rentseen on 17-6-25.
 *//*

public class LineProducer implements Runnable {
    private List<FileReader> readerList=new ArrayList<>();
    private int readerIndex;
    char [] buffer=new char[300000000];//100Mb
    int pos;
    int len;

    public LineProducer(){
        FileReader fr=null;
        readerIndex=0;
        for(int i=1;i<=10;i++){
            try {
                fr=new FileReader(new File(Constants.DATA_HOME+"/"+i+".txt"));
                readerList.add(fr);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            pos=0;
            len=readerList.get(readerIndex).read(buffer,0,300000000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean updateBuffer(){
        //System.out.println("update");
        if(readerIndex>9){
            pos=0;
            len=0;
            return false;
        }
        pos=0;
        try {
            len = readerList.get(readerIndex).read(buffer,0,300000000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(len<300000000){
            ++readerIndex;
            Server.logger.info("reader index: "+readerIndex);
        }
        return true;
    }

    public StringBuilder readLine(){
        StringBuilder sb=new StringBuilder();
        while(true){
            if(pos<len){
                if(buffer[pos]!='\n'){
                    sb.append(buffer[pos]);
                    pos++;
                }
                else{
                    pos++;
                    return sb;
                }
            }
            else {
                if(!updateBuffer()){
                    return sb;
                }
            }
        }
    }
    public void run(){
        int count=0;
        while (true){
            StringBuilder sb=readLine();
            if(sb.length()!=0){
                try {
                    Server.lineQueue.put(sb);
                    count++;
                    if(count%1000000==0){
                        Server.logger.info("lineQueue size: "+Server.lineQueue.size());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    Server.lineQueue.put(sb);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
    }
}
*/
