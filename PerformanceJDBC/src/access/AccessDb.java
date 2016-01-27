package access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import utils.DriverUtils;

public class AccessDb extends DriverUtils {

	public static void insertDB1() {
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "insert into user (first_name,last_name) values (?,?) ";
			con = DriverUtils.getConnection();
			con.setAutoCommit(false);
			pst = con.prepareStatement(sql);

			for (int i = 0; i < 10000; i++) {
				pst.setString(1, "Hello");
				pst.setString(2, "JDBC");
				pst.execute();
			}
			con.commit();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			DriverUtils.closeConnection(con, rs, pst);
		}

	}

	public static void insertDB2() {
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "insert into user (first_name,last_name) values (?,?) ";
			con = DriverUtils.getConnection();
			con.setAutoCommit(false);
			pst = con.prepareStatement(sql);

			for (int i = 0; i < 10000; i++) {
				pst.setString(1, "Hello");
				pst.setString(2, "JDBC");
				pst.addBatch();
			}
			pst.executeBatch();
			con.commit();

		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			DriverUtils.closeConnection(con, rs, pst);
		}
	}

	public static void insertDB3() {
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "insert into user (first_name,last_name) values (?,?) ";
			con = DriverUtils.getConnection();
			con.setAutoCommit(false);
			pst = con.prepareStatement(sql);

			for (int i = 0; i < 10000; i++) {
				pst.setString(1, "Hello");
				pst.setString(2, "JDBC");
				pst.addBatch();

				if (i % 1000 == 0) {
					pst.executeBatch();
				}

			}
			pst.executeBatch();
			con.commit();

		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			DriverUtils.closeConnection(con, rs, pst);
		}
	}

	public static void insertDB4() {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			String sql = "select * from user";
			con = DriverUtils.getConnection();
			con.setAutoCommit(false);
			st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = st.executeQuery(sql);

			for (int i = 0; i < 10000; i++) {
				rs.moveToInsertRow();
				rs.updateString("first_name", "Hello");
				rs.updateString("last_name", "JDBC");
			}
			con.commit();

		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			DriverUtils.closeConnection(con, rs, st);
		}
	}

	public static void main(String args[]) {
		long startTime = System.currentTimeMillis();
		insertDB1();
		long endTime = System.currentTimeMillis();
		System.out.println("use Time (ms):" + (endTime - startTime));

		startTime = System.currentTimeMillis();
		insertDB2();
		endTime = System.currentTimeMillis();
		System.out.println("use Time (ms):" + (endTime - startTime));

		startTime = System.currentTimeMillis();
		insertDB3();
		endTime = System.currentTimeMillis();
		System.out.println("use Time (ms):" + (endTime - startTime));

		startTime = System.currentTimeMillis();
		insertDB4();
		endTime = System.currentTimeMillis();
		System.out.println("use Time (ms):" + (endTime - startTime));

	}
}
