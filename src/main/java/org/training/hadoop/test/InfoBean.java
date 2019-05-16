package org.training.hadoop.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 *  @author lushiqin 20190327
 *  MR实现Join逻辑
 *
 * */
public class InfoBean  implements WritableComparable<InfoBean>{

    private int orderId;
    private String dateString;
    private String pId;
    private int amount;
    private String pName;
    private int categoryId ;
    private float price;

    private String flag ;//0订单   1商品

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId =  input.readInt();
        this.dateString = input.readUTF();
        this.pId = input.readUTF();
        this.amount = input.readInt();
        this.pName = input.readUTF();
        this.categoryId = input.readInt();
        this.price = input.readFloat();
        this.flag = input.readUTF();
    }
    /**
     * private int orderId;
     private String dateString;
     private int pId;
     private int amount;
     private String pName;
     private int categoryId ;
     private float price;
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(orderId);
        output.writeUTF(dateString);
        output.writeUTF(pId);
        output.writeInt(amount);
        output.writeUTF(pName);
        output.writeInt(categoryId);
        output.writeFloat(price);
        output.writeUTF(flag);
    }

    @Override
    public int compareTo(InfoBean o) {

        return this.price > o.price ? -1 : 1;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getFlag() {
        return flag;
    }
    public void setFlag(String flag) {
        this.flag = flag;
    }
    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }


    /**
     * Creates a new instance of InfoBean.
     *
     * @param orderId
     * @param dateString
     * @param pId
     * @param amount
     * @param pName
     * @param categoryId
     * @param price
     */

    public void set(int orderId, String dateString, String pId, int amount, String pName, int categoryId, float price, String flag) {
        this.orderId = orderId;
        this.dateString = dateString;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    /**
     * Creates a new instance of InfoBean.
     *
     */

    public InfoBean() {
    }

    @Override
    public String toString() {
        return orderId + "\t" + dateString + "\t" + amount + "\t" + pId
                + "\t" + pName + "\t" + categoryId + "\t" + price+"\t"+flag;
    }


}
