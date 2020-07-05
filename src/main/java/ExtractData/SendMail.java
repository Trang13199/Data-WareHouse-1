package ExtractData;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendMail {
	String from;
	String to;
	String passfrom;
	String content;
	String subject;

	public SendMail(String from, String to, String passfrom, String content, String subject) {
		super();
		this.from = from;
		this.to = to;
		this.passfrom = passfrom;
		this.content = content;
		this.subject = subject;

	}

	public void sendMail() {

		// Get properties object
		Properties p = new Properties();
		p.put("mail.smtp.auth", "true");
		p.put("mail.smtp.starttls.enable", "true");
		p.put("mail.smtp.host", "smtp.gmail.com");
		p.put("mail.smtp.port", 587);

		// get Session
		Session s = Session.getInstance(p, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(from, passfrom);
			}
		});

		// compose message
		try {
			MimeMessage message = new MimeMessage(s);
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			message.setSubject(subject);
			message.setText(content);

			// send message
			Transport.send(message);

			System.out.println("Message sent successfully");
		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

//	public static void main(String[] args) {
//		SendMail s = new SendMail("datawarehouse0126@gmail.com", "huyvo2581999@gmail.com", "datawarehouse2020",
//				"Welcom to ABC", "Testing");
//		s.sendMail();
//	}
}
