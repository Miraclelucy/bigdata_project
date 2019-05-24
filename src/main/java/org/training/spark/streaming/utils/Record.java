package org.training.spark.streaming.utils;

/**
 * Created by lushiqin on 2019-05-23.
 */
public class Record
        implements java.io.Serializable {

    private String word;

    public Record(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
