package org.training.datamining.clustering;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by lushiqin on 2019-05-29.
 * K-Means聚类算法
 * 算法步骤
 1、设定数字k，从n个初始数据中随机的设置k个点为聚类中心点。
 2、针对n个点的每个数据点，遍历计算到k个聚类中心点的距离，最后按照离哪个中心点最近，就划分到那个类别中。
 3、对每个已经划分好类别的n个点，对同个类别的点求均值，作为此类别新的中心点。
 4、循环(2)，(3)直到最终中心点收敛。
 */
public class KMeansTool {
    // 输入数据文件地址
    private String filePath;
    // 分类类别个数
    private int classNum;
    // 类名称
    private ArrayList<String> classNames;
    // 聚类坐标点
    private ArrayList<Point> classPoints;
    // 所有的数据坐标点
    private ArrayList<Point> totalPoints;

    public KMeansTool(String filePath, int classNum) {
        this.filePath = filePath;
        this.classNum = classNum;
        readDataFile();
    }

    /**
     * 从文件中读取数据
     */
    private void readDataFile() {
        File file = new File(filePath);
        ArrayList<String[]> dataArray = new ArrayList<String[]>();

        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String str;
            String[] tempArray;
            while ((str = in.readLine()) != null) {
                tempArray = str.split(" ");
                dataArray.add(tempArray);
            }
            in.close();
        } catch (IOException e) {
            e.getStackTrace();
        }

        classPoints = new ArrayList<>();
        totalPoints = new ArrayList<>();
        classNames = new ArrayList<>();
        for (int i = 0, j = 1; i < dataArray.size(); i++) {
            if (j <= classNum) {
                classPoints.add(new Point(dataArray.get(i)[0],
                        dataArray.get(i)[1], j + ""));
                classNames.add(i + "");
                j++;
            }
            totalPoints
                    .add(new Point(dataArray.get(i)[0], dataArray.get(i)[1]));
        }
    }

    /**
     * K均值聚类算法实现
     */
    public void kMeansClustering() {
        double tempX = 0;
        double tempY = 0;
        int count = 0;
        double error = Integer.MAX_VALUE;
        Point temp;

        while (error > 0.01 * classNum) {
            for (Point p1 : totalPoints) {
                // 将所有的测试坐标点就近分类
                for (Point p2 : classPoints) {
                    p2.computerDistance(p1);
                }
                Collections.sort(classPoints);

                // 取出p1离类坐标点最近的那个点
                p1.setClassName(classPoints.get(0).getClassName());
            }

            error = 0;
            // 按照均值重新划分聚类中心点
            for (Point p1 : classPoints) {
                count = 0;
                tempX = 0;
                tempY = 0;
                for (Point p : totalPoints) {
                    if (p.getClassName().equals(p1.getClassName())) {
                        count++;
                        tempX += p.getX();
                        tempY += p.getY();
                    }
                }
                tempX /= count;
                tempY /= count;

                error += Math.abs((tempX - p1.getX()));//绝对值
                error += Math.abs((tempY - p1.getY()));//绝对值
                // 计算均值
                p1.setX(tempX);
                p1.setY(tempY);

            }

            for (int i = 0; i < classPoints.size(); i++) {
                temp = classPoints.get(i);
                System.out.println(MessageFormat.format("聚类中心点{0}，x={1},y={2}",
                        (i + 1), temp.getX(), temp.getY()));
            }
            System.out.println("----------");
        }

        System.out.println("结果值收敛");
        for (int i = 0; i < classPoints.size(); i++) {
            temp = classPoints.get(i);
            System.out.println(MessageFormat.format("聚类中心点{0}，x={1},y={2}",
                    (i + 1), temp.getX(), temp.getY()));
        }

    }

    public static void main(String[] args){
        String filePath = "/app/datamining/kmeans/input.txt";
        //聚类中心数量设定
        int classNum = 3;

        KMeansTool tool = new KMeansTool(filePath, classNum);
        tool.kMeansClustering();
    }

}