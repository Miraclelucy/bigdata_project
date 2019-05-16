package org.training.hadoop.testjoin;

/**
 * Created by lushiqin on 20190401.
 * 使用场景：一个大表（表中的key内存仍然放不下），一个超大表
 * 在某些情况下，Semijoin抽取出来的小表的key集合在内存中仍然存放不下，这个时候可以使用BloomFilter以节省空间。
 * Bloom Filter最常见的作用是；判断某个元素是否在一个集合里面。它最重要的两个方法是add()和membershipTest()。
 */
public class BloomFilterJoin {

}
