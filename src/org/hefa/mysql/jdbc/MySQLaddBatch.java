package org.hefa.mysql.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLaddBatch {
	public static String sql = "INSERT INTO Batch.user_ (id, name, age, sex) VALUES (?,?,?,?)";

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	public static Connection getConn() {
		Connection conn = null;
		try {
			String url =  "jdbc:mysql://192.168.1.100:3306/Batch?rewriteBatchedStatements=true";
			conn = DriverManager.getConnection(url, "root", "password");
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
		}
		return conn;
	}

	public void addBatch(boolean bool) {
		Connection conn = getConn();
		try {
			PreparedStatement prest = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			long a = System.currentTimeMillis();
			for (int x = 1; x <= 1000000; x++) {
				prest.setInt(1, x);
				prest.setString(2, "黄");
				prest.setString(3, "24");
				prest.setString(4, "男");
				prest.execute();
				if (x % 500 == 0) {
					conn.commit();
				}
			}
			long b = System.currentTimeMillis();
			long c = b - a;
			System.out.println("MySql非批量插入100万条记录，每500条提交一次事务，耗时：" + c / 1000 + "s");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			close(conn);
		}
	}

	public void addBatch() {
		Connection conn = getConn();
		try {
			PreparedStatement prest = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			long a = System.currentTimeMillis();
			for (int x = 1; x <= 1000000; x++) {
				prest.setInt(1, x + 1000000);
				prest.setString(2, "海");
				prest.setString(3, "24");
				prest.setString(4, "男");
				prest.addBatch();
				if (x % 500 == 0) {
					prest.executeBatch();
					conn.commit();
				}
			}
			long b = System.currentTimeMillis();
			long c = b - a;
			System.out.println("MySql批量插入100万条记录，每500条提交一次事务，耗时：" + c / 1000 + "s");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			close(conn);
		}
	}

	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		MySQLaddBatch mysql = new MySQLaddBatch();
		mysql.addBatch(false);
		mysql.addBatch();
	}
}
