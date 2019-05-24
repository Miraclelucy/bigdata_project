package org.training.spark.streaming.utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * Created by lushiqin on 5/23/19.
 */
public class ConnectionPool
{

    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 5;i ++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://hadoop:3306/test?characterEncoding=utf8&useSSL=true",
                            "root",
                            "Cmge1234"
                    );
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }



}

