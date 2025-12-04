package org.consumer;

import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

import java.util.Properties;

public class EmailSender {

    private static final String MAILHOG_HOST = "localhost";
    private static final int MAILHOG_PORT = 1025;
    private static final String FROM_EMAIL = "sender@example.com";

    // 1. Declare Session as static final so it is created only ONCE.
    private static final Session session;

    static {
        // This block runs once when the class is loaded
        Properties mailProps = new Properties();
        mailProps.put("mail.smtp.host", MAILHOG_HOST);
        mailProps.put("mail.smtp.port", MAILHOG_PORT);

        // Create the session and store it for reuse
        session = Session.getInstance(mailProps, null);
    }

    public static void sendEmail(String to, String subject, String body) throws Exception {
        // 2. Reuse the existing session (Thread-safe)
        MimeMessage message = new MimeMessage(session);

        message.setFrom(new InternetAddress(FROM_EMAIL));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
        message.setSubject(subject);
        message.setText(body);

        // 3. Send the message
        // Note: This still opens a new TCP connection for every email.
        Transport.send(message);
    }
}