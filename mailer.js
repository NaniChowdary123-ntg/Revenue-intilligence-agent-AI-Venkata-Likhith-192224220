require('dotenv').config();
const nodemailer = require('nodemailer');
const sgMail = require('@sendgrid/mail');

const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_PASS = process.env.SMTP_PASS;
const SMTP_USER = process.env.SMTP_USER;

let transporter = null;
try {
  transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: String(process.env.SMTP_SECURE || '').trim() === '1' || SMTP_PORT === 465,
    auth: SMTP_USER && SMTP_PASS ? { user: SMTP_USER, pass: SMTP_PASS } : undefined,
  });
} catch (e) {
  // ignore
}

if (process.env.SENDGRID_API_KEY) {
  sgMail.setApiKey(process.env.SENDGRID_API_KEY);
}

async function sendViaSendGrid(mail) {
  if (!process.env.SENDGRID_API_KEY) throw new Error('SendGrid API key not configured');

  const msg = {
    to: mail.to,
    from: mail.from || process.env.MAIL_FROM || process.env.SMTP_USER,
    subject: mail.subject || '(no subject)',
    text: mail.text || undefined,
    html: mail.html || undefined,
  };

  // @sendgrid/mail returns a response array
  const res = await sgMail.send(msg);
  return { sendgrid: true, status: res[0].statusCode, body: res[0].body };
}

async function sendMail(mail) {
  // Try SMTP transporter first if available
  if (transporter) {
    try {
      return await transporter.sendMail(mail);
    } catch (err) {
      // If it's a network/socket error, fallback to SendGrid
      const isSocket = (err && (err.code === 'ESOCKET' || String(err.message || '').toLowerCase().includes('etimedout')));
      if (!isSocket) throw err;
      console.warn('SMTP send failed, falling back to SendGrid:', err.message || err);
    }
  }

  // Fallback to SendGrid HTTP API
  return await sendViaSendGrid(mail);
}

module.exports = {
  sendMail,
  transporter,
};
