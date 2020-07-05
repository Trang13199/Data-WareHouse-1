package ExtractData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDB {
	static Connection con;

	public static Connection getConnection(String database) throws ClassNotFoundException, SQLException {
		String url = "jdbc:mysql://localhost:3306/" + database;
		String user = "root";
		String password = "";

		Class.forName("com.mysql.jdbc.Driver");
		con = DriverManager.getConnection(url, user, password);
		return con;

	}
}
