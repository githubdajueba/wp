package cn.hive;

import java.sql.*;

public class HiveJDBC {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException, SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://hadoop01:10000/default", "root", "");
        Statement stmt = con.createStatement();
        String dbName = "test";
        String tableName = dbName+".tb_test";
        System.out.println("insert into table "+ tableName + " values(1, 'dongcc')");
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database " + dbName);
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        stmt.execute("insert into table " + tableName + " values(1, 'dongcc')");
        // show tables
        String sql = "select * from "+tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
            System.out.println(res.getString(2));
        }
    }

}
