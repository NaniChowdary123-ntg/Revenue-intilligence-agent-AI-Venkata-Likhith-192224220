const fs = require('fs');

const content = fs.readFileSync('server.js', 'utf8');
const lines = content.split('\n');

const endpoints = [];
let currentEndpoint = null;

for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  const match = line.match(/app\.(get|post|put|patch|delete)\s*\(\s*["']([^"']+)["']/);
  
  if (match) {
    const [, method, path] = match;
    endpoints.push({
      line: i + 1,
      method: method.toUpperCase(),
      path: path
    });
  }
}

console.log(JSON.stringify(endpoints, null, 2));
