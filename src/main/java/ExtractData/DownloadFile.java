package ExtractData;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class DownloadFile {
	private static PreparedStatement ps = null;
	Connection con;
//	private static String file_timestamp;
	private static String from = "datawarehouse0126@gmail.com";
//	private static String to = "huyvo2581999@gmail.com";
	private static String to = "tranghoang13199@gmail.com";
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

	public static boolean downloadFile(String host, int ports, String user, String pass, String path, String local,
			String file_name, String file_type) throws ClassNotFoundException, SQLException {
		DownloadFile d = new DownloadFile();
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

		scp.put_SyncMustMatch(file_name + "*.*" + file_type);
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

	// ***** DOWNLOAD TAT CA CAC FILE CUA CAC NHOM VE LOCAL ********//

	public static void getLog() throws ClassNotFoundException, SQLException {
		Connection con;
		PreparedStatement pre;

		// ket noi toi controldb
		con = ConnectionDB.getConnection("controldb");
		String sql = "SELECT * FROM `table_config` WHERE config_id = 1";
		pre = con.prepareStatement(sql);
		System.out.println();
		ResultSet tmp = pre.executeQuery();
		// duyet record trong resultset
		while (tmp.next()) {
//				String target_table = tmp.getString("target_table");
			int id = tmp.getInt("config_id");
			String file_name = tmp.getString("file_name");
			String file_type = tmp.getString("file_type");
			String local = tmp.getString("source");
			String user = tmp.getString("userRemote");
			String pass = tmp.getString("passRemote");
			String path = tmp.getString("remotePath");
			String host = tmp.getString("host");
			int ports = tmp.getInt("port");
			String target_table = tmp.getString("target_table");

			// bat dau load file ve
			boolean download = new DownloadFile().downloadFile(host, ports, user, pass, path, local, file_name,
					file_type);
//		System.out.println("Dowload thanh cong");

			if (download) {
				// Thông báo thành công ra màn hình
				File file = new File(local + "\\" + target_table);
				if (!file.exists()) {
					System.out.println("File hk ton tai..........");
//					DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
//					LocalDateTime now = LocalDateTime.now();
//					file_timestamp = dtf.format(now);
					String sql1 = "INSERT INTO table_log (file_name,data_file_config_id,file_status,file_timestamp) VALUES (?,?,?,?)";
//cập nhật file_status là ERROR và thời gian là thời gian hiện tại
					try {
						ps = ConnectionDB.getConnection("controldb").prepareStatement(sql1);
						ps.setString(1, target_table);
						ps.setInt(2, id);
						ps.setString(3, "ERROR");
						ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
						ps.executeUpdate();
					} catch (ClassNotFoundException | SQLException e1) {
						e1.printStackTrace();
					}
					// gửi mail ve hệ thống thông báo lỗi
					SendMail s = new SendMail(from, to, passfrom, "Updated log faild: Error " + mess + ".",
							"Updated log Faild: DATA WAREHOUSE SERVER");
					s.sendMail();
				} else {
					System.out.println("Dowload success file name: " + target_table + " " + host);
					// Cập nhật status_file là ER và thời gian download là thời
					// gian hiện tại
					String sql1 = "INSERT INTO table_log (file_name,data_file_config_id,file_status,file_timestamp) VALUES (?,?,?,?)";

					try {
						ps = ConnectionDB.getConnection("controldb").prepareStatement(sql1);
						ps.setString(1, target_table);
						ps.setInt(2, id);
						ps.setString(3, "ER");
						ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
						ps.executeUpdate();
						System.out.println("Success insert to log");
					} catch (ClassNotFoundException | SQLException e1) {
						e1.printStackTrace();
					}
					// gửi mail thông báo thành công
					SendMail s = new SendMail(from, to, passfrom, " Update log successfull from " + path + " to "
							+ local + " at " + new Timestamp(System.currentTimeMillis()).toString().substring(0, 19),
							subject);
					s.sendMail();
				}
			} else {
				// In dòng thông báo file Không tồn tại
				System.out.println("File khong ton tai, idFile: " + target_table + " ,group: " + host);
//				// 6.2.3.1.3. Cập nhật status_file là ERROR Dowload và thời gian download llà
//				// thời gian hiện tại
				String sql1 = "INSERT INTO table_log (file_name,data_file_config_id,file_status,file_timestamp) VALUES (?,?,?,?)";

				try {
					ps = ConnectionDB.getConnection("controldb").prepareStatement(sql1);
					ps.setString(1, target_table);
					ps.setInt(2, id);
					ps.setString(3, "ERROR");
					ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
					ps.executeUpdate();
				} catch (ClassNotFoundException | SQLException e1) {
					e1.printStackTrace();
				}
				// gửi mail ve hệ thống thông báo lỗi
				SendMail s = new SendMail(from, to, passfrom, "Updated log faild: Error File No Exit",
						"Updated log Faild: DATA WAREHOUSE SERVER");
				s.sendMail();
			}
		}
	}

//	public static void main(String[] args) throws ClassNotFoundException, SQLException {
//		DownloadFile d = new DownloadFile();
//		d.getLog();
//	}

}
