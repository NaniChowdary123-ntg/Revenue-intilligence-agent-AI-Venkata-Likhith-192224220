require("dotenv").config();
const nodemailer = require("nodemailer");

const smtpConfigs = [
  {
    name: "Gmail 587 (TLS)",
    host: "smtp.gmail.com",
    port: 587,
    secure: false,
  },
  {
    name: "Gmail 465 (SSL)",
    host: "smtp.gmail.com",
    port: 465,
    secure: true,
  },
  {
    name: "SendGrid 587 (TLS)",
    host: "smtp.sendgrid.net",
    port: 587,
    secure: false,
    auth: {
      user: "apikey",
      pass: process.env.SENDGRID_API_KEY || "your-sendgrid-key-here"
    }
  },
  {
    name: "Mailgun 587 (TLS)",
    host: "smtp.mailgun.org",
    port: 587,
    secure: false,
    auth: {
      user: process.env.MAILGUN_USER || "your-mailgun-user",
      pass: process.env.MAILGUN_PASS || "your-mailgun-password"
    }
  }
];

async function testSMTP(config) {
  console.log(`\n🧪 Testing ${config.name}...`);

  const transporterConfig = {
    host: config.host,
    port: config.port,
    secure: config.secure,
    auth: config.auth || {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  };

  const transporter = nodemailer.createTransport(transporterConfig);

  try {
    await transporter.verify();
    console.log(`✅ ${config.name}: Connection successful!`);
    return true;
  } catch (err) {
    console.log(`❌ ${config.name}: ${err.message}`);
    return false;
  }
}

async function main() {
  console.log("🔍 Testing multiple SMTP configurations...\n");

  for (const config of smtpConfigs) {
    await testSMTP(config);
  }

  console.log("\n💡 If all Gmail options fail, consider:");
  console.log("   1. Using SendGrid, Mailgun, or AWS SES");
  console.log("   2. Checking with your network administrator");
  console.log("   3. Using Gmail OAuth2 instead of app passwords");
  console.log("   4. Trying from a different network");
}

main().catch(console.error);