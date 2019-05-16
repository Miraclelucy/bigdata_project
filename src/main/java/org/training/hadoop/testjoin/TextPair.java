package org.training.hadoop.testjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lushiqin on 20190401.
 */
public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    public void set(Text first,Text second) {
        this.first=first;
        this.second = second;
    }

    public TextPair(){
        set(new Text(),new Text());
    }

    public TextPair(String first,String second){
        set(new Text(first),new Text(second));
    }

    public TextPair(Text first,Text second){
        set(first,second);
    }


    @Override
    public void readFields(DataInput input) throws IOException {
        first.readFields(input);
        second.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        first.write(output);
        second.write(output);
    }

    public int hashCode(){
        return first.hashCode()*163/second.hashCode();
    }

    public boolean equals(Object j){
        if(j instanceof TextPair){
            TextPair tp=(TextPair)j;
            return first.equals(tp.first)&&second.equals(tp.second);
        }
        return  false;
    }

    @Override
    public int compareTo(TextPair tp) {
        if(!first.equals(tp.first)){
            return  first.compareTo(tp.first);
        }
        else{
            return  second.compareTo(tp.second);
        }
    }

}
