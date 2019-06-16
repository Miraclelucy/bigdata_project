package org.training.datamining.associationAnalysis;

/**
 * Created by lushiqin on 2019-05-28.
 */
public class Test {

    public static void main(String[] args) {
        testBreak();
        testContinue();
        testReturn();
    }

    static void testBreak() {
        for (int i = 0; i < 3; i++) {
            boolean flag=true;
            if (i % 2 == 0) {
                System.out.println("i=" + i);
            } else {
                flag=false;
                System.out.println("i=" + i+"执行了break语句,跳出当前循环!");
                System.out.println("i=" + i+"flag:"+flag);
                break;
            }
        }
    }

    static void testContinue() {
        for (int i = 0; i < 3; i++) {
            if (i % 2 == 0) {
                System.out.println("i=" + i+"没有执行continue语句输出i=" + i);
            } else {
                System.out.println("i=" + i+"执行了continue语句,跳出当前循环!");
                continue;
            }
        }
    }

    static void testReturn() {
        for (int i = 0; i < 3; i++) {
            System.out.println("i=" + i+"执行了return语句,直接跳出了当前的testReturn方法!");
            return;
        }
    }
}