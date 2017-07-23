package com.alibaba.middleware.race.sync;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rentseen on 17-6-26.
 */
public class BlockLoader implements Runnable{
    private List<FileChannel> readerList=new ArrayList<>();
    private int readerIndex;
    private final int bufferSize=10000000;//10Mb
    private int count=0;

    public BlockLoader(){
        FileInputStream fis=null;
        readerIndex=0;
        for(int i=1;i<=10;i++){
            try {
                fis=new FileInputStream(Constants.DATA_HOME+"/"+i+".txt");
                readerList.add(fis.getChannel());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    public void run(){
        //int count=0;
        int len=0;
        while (true){
            //test
            if(readerIndex>9){
                try {
                    Server.blockQueue.put(Server.blockBackQueue.take());
                    Server.blockSizeQueue.put(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }
            ByteBuffer buffer=null;
            if(count++<15){
                buffer=ByteBuffer.allocateDirect( bufferSize);
            }
            else {
                try {
                    buffer=Server.blockBackQueue.take();
                    buffer.clear();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                len = readerList.get(readerIndex).read(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(len<bufferSize){
                ++readerIndex;
                Server.logger.info("reader index: "+readerIndex);
            }
            try {
                buffer.position(0);
                Server.blockQueue.put(buffer);
                Server.blockSizeQueue.put(len);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("end");
    }
}
