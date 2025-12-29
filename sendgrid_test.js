require('dotenv').config();
const axios = require('axios');

async function sendEmail() {
  const apiKey = process.env.SENDGRID_API_KEY;
  if (!apiKey) {
    console.error('SENDGRID_API_KEY not set in .env');
    process.exit(1);
  }

  const payload = {
    personalizations: [
      { to: [{ email: process.env.SMTP_USER }], subject: 'SendGrid API Test' }
    ],
    from: { email: process.env.MAIL_FROM || process.env.SMTP_USER },
    content: [{ type: 'text/plain', value: 'If you see this, SendGrid API worked.' }]
  };

  try {
    const res = await axios.post('https://api.sendgrid.com/v3/mail/send', payload, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000
    });
    console.log('✅ SendGrid API request accepted, status:', res.status);
  } catch (err) {
    if (err.response) {
      console.error('❌ SendGrid API error:', err.response.status, err.response.data);
    } else {
      console.error('❌ Request failed:', err.message);
    }
    process.exit(1);
  }
}

sendEmail();