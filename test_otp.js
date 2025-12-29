const axios = require('axios');

(async () => {
  try {
    console.log('Testing OTP request to http://localhost:4000/api/auth/email-otp/request');
    
    const response = await axios.post('http://localhost:4000/api/auth/email-otp/request', {
      email: 'test@example.com'
    });
    
    console.log('✅ Success:', response.data);
  } catch (err) {
    console.error('❌ Error:', err.response?.data || err.message);
  }
})();
