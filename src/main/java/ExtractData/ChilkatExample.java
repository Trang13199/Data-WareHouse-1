package ELT;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;
import com.mysql.jdbc.PreparedStatement;

public class ChilkatExample {
	static Control c;
	private static PreparedStatement ps = null;
	private static String file_timestamp;
	private static String path;
	private static String local;
	private static String from = "datawarehouse0126@gmail.com";
	private static String to = "huyvo2581999@gmail.com";
	private static String passfrom = "datawarehouse2020";
//	private static String content = ";
	private static String subject = "Update log successfull: DATA WAREHOUSE SERVER  ";
	static String mess;

	static {
		try {
			System.loadLibrary("chilkat");// copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void getTrial() {
		CkGlobal glob = new CkGlobal();
		boolean success = glob.UnlockBundle("Anything for 30-day trial");
		if (success != true) {
			System.out.println(glob.lastErrorText());
			return;
		}
		int status = glob.get_UnlockStatus();
		if (status == 2) {
			System.out.println("Unlocked using purchased unlock code.");
		} else {
			System.out.println("Uncloked in trail mode.");
		}
		System.out.println(glob.lastErrorText());
	}

	public static boolean downloadFile(String host, int ports, String user, String pass, String path, String local)
			throws ClassNotFoundException, SQLException {
		ChilkatExample d = new ChilkatExample();
		d.getTrial();
		CkSsh ssh = new CkSsh();
		// Hostname may be an IP address or hostname:
//		String hostname = "www.some-ssh-server.com";
//		String hostname = "http://drive.ecepvn.org:5000/index.cgi?launchApp=SYNO.SDS.App.FileStation3.Instance&launchParam=openfile%3D%252FECEP%252Fsong.nguyen%252FDW_2020%252F&fbclid=IwAR1GjbMt_ZWTairglWCjOQQH6Q0NbyXgl0qP7LTBahWmR4HcJXNVoh5o5fw";
//		String hostname = "drive.ecepvn.org";
//		System.out.println(c.getHost());
		String hostname = host;

//		int port = 2227;
		int port = ports;

		// Connect to an SSH server:
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			mess = "Faild: Host and port invalid";
			return false;
		}

		// Wait a max of 5 seconds when reading responses..
		ssh.put_IdleTimeoutMs(5000);

		// Authenticate using login/password:
		success = ssh.AuthenticatePw(user, pass);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			mess = "Faild: User and pass invalid";
			return false;
		}

		// Once the SSH object is connected and authenticated, we use it
		// in our SCP object.
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return false;
		}

//		scp.put_SyncMustMatch(c.getTableName());// down tat ca cac file bat dau bang sinhvien
		String remotePath = path;
		String localPath = local; // thu muc muon down file ve
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);

		/*
		 * String remotePath =
		 * "/volume1/ECEP/song.nguyen/DW_2020/data/17130276_Sang_Nhom4.xlsx"; // String
		 * localPath = "/home/bob/test.txt"; String localPath =
		 * "E:\\DATA WAREHOUSE\\Error\\17130276_Sang_Nhom4.xlsx"; success =
		 * scp.DownloadFile(remotePath, localPath);
		 */
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return false;
		}

		System.out.println("SCP download file success.");

		// Disconnect
		ssh.Disconnect();
		return true;

	}

	public static void getLog(String dbLog, String dbcontrol, String table)
			throws ClassNotFoundException, SQLException {
		/// truy vấn câu lệnh
		String sql = "SELECT * FROM " + dbcontrol + "." + table + " where config_id=?";
		ps = (PreparedStatement) ConnectionDB.getConnection(dbcontrol).prepareStatement(sql);
		ps.setInt(1, 2);
		// nhập ResultSet
		ResultSet tmp = ps.executeQuery();
		// nhận ResultSet cho các record
		tmp.next();
		String target_table = tmp.getString("target_table");
		String local = tmp.getString("success_dir");
		String user = tmp.getString("userRemote");
		String pass = tmp.getString("passRemote");
		String path = tmp.getString("remotePath");
		String host = tmp.getString("host");
		int ports = tmp.getInt("port");
		int numberofline = tmp.getInt("numofdata");
		// bat dau load file ve local
		boolean download = new ChilkatExample().downloadFile(host, ports, user, pass, path, local);
		if (download) {
			// Thông báo thành công ra màn hình
			System.out.println("Dowload success file name: " + target_table + " " + host);
			// Cập nhật file_status là ERR và thời gian download là thời
			// gian hiện tại
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			file_timestamp = dtf.format(now);
			String sql1 = "INSERT INTO " + dbLog
					+ " (file_name, data_file_config_id,file_status,staging_load_count,file_timestamp) VALUES (?,?,?,?,?)";

			try {
				ps = (PreparedStatement) ConnectionDB.getConnection(dbcontrol).prepareStatement(sql1);
				ps.setString(1, target_table);
				ps.setInt(2, 1);
				ps.setString(3, "ER");
				ps.setInt(4, numberofline);
				ps.setString(5, file_timestamp);
				ps.executeUpdate();
				System.out.println("Success insert to log");
			} catch (ClassNotFoundException | SQLException e1) {
				e1.printStackTrace();
			}
			// gửi mail thông báo thành công
			SendMail s = new SendMail(from, to, passfrom,
					" Update log successfull from " + path + " to " + local + " at " + file_timestamp, subject);
			s.sendMail();

		} else {
			// thông báo ra màn hình
			System.out.println("DOWNLOAD KHONG THANH CONG");
			// Cập nhật file_status là FAILED và thời gian download là thời
			// gian hiện tại
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			file_timestamp = dtf.format(now);
			String sql1 = "INSERT INTO " + dbLog
					+ " (file_name, data_file_config_id,file_status,staging_load_count,file_timestamp) VALUES (?,?,?,?,?)";

			try {
				ps = (PreparedStatement) ConnectionDB.getConnection(dbcontrol).prepareStatement(sql1);
				ps.setString(1, target_table);
				ps.setInt(2, 1);
				ps.setString(3, "FAILED");
				ps.setInt(4, numberofline);
				ps.setString(5, file_timestamp);
				ps.executeUpdate();
				System.out.println("Success insert to log");
			} catch (ClassNotFoundException | SQLException e1) {
				e1.printStackTrace();
			}
//gửi mail về hệ thống thông báo lỗi
			SendMail s = new SendMail(from, to, passfrom, "Updated log faild: Error " + mess + ".",
					"Updated log Faild: DATA WAREHOUSE SERVER");
			s.sendMail();
		}
	}

}
