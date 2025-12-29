require("dotenv").config();
const nodemailer = require("nodemailer");
const fs = require("fs");

const logFile = "smtp_result.txt";

function log(msg) {
  console.log(msg);
  fs.appendFileSync(logFile, msg + "\n");
}

(async () => {
  fs.writeFileSync(logFile, "Starting SMTP Check...\n");
  log(`Host: ${process.env.SMTP_HOST}`);
  log(`Port: ${process.env.SMTP_PORT}`);
  log(`User: ${process.env.SMTP_USER}`);

  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT),
    secure: false, // true for 465, false for 587
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });

  try {
    log("Verifying connection...");
    await transporter.verify();
    log("✅ Connection successful!");

    log("Sending test email...");
    const info = await transporter.sendMail({
      from: process.env.MAIL_FROM || process.env.SMTP_USER,
      to: process.env.SMTP_USER, // Send to self
      subject: "SMTP Test",
      text: "If you see this, SMTP is working!",
    });
    log(`✅ Email sent: ${info.messageId}`);
  } catch (err) {
    log(`❌ SMTP Error: ${err.message}`);
    log(JSON.stringify(err, null, 2));
  }
})();
