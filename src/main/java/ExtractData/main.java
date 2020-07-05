package ExtractData;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;

public class main {
	public static void main(String[] args) throws InvalidFormatException,
			org.apache.poi.openxml4j.exceptions.InvalidFormatException, ClassNotFoundException, SQLException {
		DownloadFile d = new DownloadFile();
		Staging s = new Staging();
		while (true) {
			System.out.println("Choose a step:\n 1. GetConnection\n 2. Download file\n 3. Load Staging\n 0. Exist");
			Scanner sc = new Scanner(System.in);
			int value = sc.nextInt();
			if (value == 1) {
				Connection conn = new ConnectionDB().getConnection("staging");
				Connection con = new ConnectionDB().getConnection("controldb");

				if (conn != null) {
					System.out.println("Thanh cong");
				}
			} else if (value == 2) {
				// download
				System.out.println("DOWNLOAD FILE");
				Scanner sc2 = new Scanner(System.in);
				d.getLog();

			} else if (value == 3) {
				// staging
				System.out.println("LOAD STAGING");
				Scanner sc2 = new Scanner(System.in);
				s.staging();

			} else if (value == 0) {
				System.out.println("Good bye");
				break;
			} else {
				System.out.println("NOT FOUND");
			}
		}
	}

}
