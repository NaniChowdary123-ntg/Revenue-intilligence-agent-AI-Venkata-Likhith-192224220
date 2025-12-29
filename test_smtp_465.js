require("dotenv").config();
const nodemailer = require("nodemailer");

(async () => {
  console.log("Testing SMTP on Port 465 (SSL)...");
  
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST || "smtp.gmail.com",
    port: 465,
    secure: true, // true for 465
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });

  try {
    console.log("Verifying connection...");
    await transporter.verify();
    console.log("✅ Connection successful on port 465!");
    process.exit(0);
  } catch (err) {
    console.error(`❌ SMTP Error on 465: ${err.message}`);
    process.exit(1);
  }
})();
