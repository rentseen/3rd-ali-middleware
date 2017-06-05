package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties= new DefaultKeyValue();
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    //add by yelinsheng
    public String toString(){
        //return headers.toString()+","+new String(body);
        //根据测试message的规律充分压缩数据
        StringBuffer sb=new StringBuffer();
        sb.append(new String(body)).append(';');
        sb.append(headers.getString("MessageId"));
        if(headers.containsKey("Topic")){
            sb.append(headers.getString("Topic").substring(6)).append(',');
        }
        else{
            sb.append(headers.getString("Queue").substring(6)).append('.');
        }
        sb.append(properties.getString("PRO_OFFSET").charAt(8)).append(properties.getString("PRO_OFFSET").substring(10));

        boolean flag=false;
        for(String s:properties.keySet()){
            if(!s.equals("PRO_OFFSET") && flag==false){
                sb.append(',').append(s).append(',').append(properties.getString(s));
                flag=true;
            }
            else if(!s.equals("PRO_OFFSET") && flag==true){
                sb.append(s).append(properties.getString(s));
            }
        }
        return sb.toString();
        //return new String(body);
    }
    public void setHeaders(KeyValue keyValue){
    	this.headers = keyValue;
    }
    
    public void setProperties(KeyValue keyValue){
    	this.properties = keyValue;
    }

    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }
}
