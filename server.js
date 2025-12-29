// server.js
// ===================================
// BASIC SETUP
// ===================================
require("dotenv").config();
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const nodemailer = require("nodemailer");

// ✅ NEW (for attachments + exports + SSE support)
const multer = require("multer");
const path = require("path");
const fs = require("fs");

const app = express();



// CORS so your React app (Vite) can talk to this server
app.use(
  cors({
    origin: [
      process.env.CLIENT_ORIGIN || "http://localhost:5173",
      "http://localhost:5174" // Vite fallback port
    ],
    credentials: true,
  })
);


// Parse JSON bodies
app.use(express.json({ limit: "2mb" }));

// ===================================
// MYSQL CONNECTION POOL  ✅ FIXED for India timezone + DATE handling
// ===================================
const pool = mysql.createPool({
  host: process.env.DB_HOST || "127.0.0.1",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD ?? "", // ✅ important
  database: process.env.DB_NAME || "dental_clinic",
  waitForConnections: true,
  connectionLimit: 10,

  // ✅ CRITICAL: prevents DATE columns turning into JS Date objects (UTC shift)
  dateStrings: true, // or: dateStrings: ["DATE", "DATETIME", "TIMESTAMP"]

  // ✅ Keep driver aligned with India time for DATETIME/TIMESTAMP conversions
  timezone: "+05:30",
});

// ✅ Set MySQL session timezone (NOW(), CURDATE() behave as India)
(async () => {
  try {
    await pool.query("SET time_zone = '+05:30'");
    console.log("✅ MySQL session time_zone set to +05:30");
  } catch (e) {
    console.error("Failed to set MySQL time_zone:", e?.message || e);
  }
})();


if (typeof initAgents === "function") {
  initAgents(pool).catch((e) => console.error("Agent init failed:", e));
}

// ✅ Optional Node hooks (kept guarded)

try {
  ({ enqueueEvent, retryFailed: retryFailedFromEventQueue } = require("./agents/eventQueue"));
} catch (_) {}


try {
  ({
    runAppointmentAgentOnce,
    runInventoryAgentOnce,
    runRevenueAgentOnce,
    retryFailed: retryFailedFromAgentsIndex,
  } = require("./agents"));
} catch (_) {}

// ✅ Optional legacy Node PDF export hook (kept guarded)
let exportCasePdf = null;
try {
  ({ exportCasePdf } = require("./agents/caseTrackingAgent"));
} catch (_) {}

const JWT_SECRET = process.env.JWT_SECRET || "fallback-secret";

// ===================================
// APP TIMEZONE HELPERS (India)
// ===================================
const APP_TZ = process.env.APP_TZ || "Asia/Kolkata";

// Returns YYYY-MM-DD in Asia/Kolkata (prevents server UTC offset bugs)
function formatDateYYYYMMDD(d = new Date()) {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone: APP_TZ }).format(d); // YYYY-MM-DD
  } catch {
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
}

function toDateStr(x) {
  const s = String(x || "").trim();
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function toTimeStr(x) {
  const s = String(x || "").trim();
  if (!s) return "";
  return s.length === 5 ? `${s}:00` : s; // HH:MM -> HH:MM:SS
}

// ===================================
// MAILER (OTP + NOTIFICATIONS)
// ===================================
const smtpPort = Number(process.env.SMTP_PORT) || 587;
const smtpSecure =
  String(process.env.SMTP_SECURE || "").trim() === "1" ||
  String(process.env.SMTP_SECURE || "").trim().toLowerCase() === "true" ||
  smtpPort === 465;

const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: smtpPort,
  secure: smtpSecure,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

// Centralized mailer (falls back to SendGrid if SMTP fails)
const mailer = require('./mailer');

// Optional verify (won’t crash server)
(async () => {
  try {
    if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
      await transporter.verify();
      console.log("✅ SMTP transporter verified");
    } else {
      console.warn("⚠️ SMTP not fully configured (SMTP_HOST/SMTP_USER/SMTP_PASS). Emails may fail.");
    }
  } catch (e) {
    console.error("❌ SMTP verify failed:", e?.message || e);
  }
})();

// Generate 6-digit OTP like "483920"
function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

// ============ EMAIL TEMPLATES ============
function buildOtpEmailHtml(name, code) {
  const safeName = (name || "there").toString();
  const safeCode = String(code || "");
  return `
  <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
    <h2 style="margin:0 0 10px 0">Password reset</h2>
    <p style="margin:0 0 12px 0">Hi ${safeName},</p>
    <p style="margin:0 0 12px 0">Use this OTP to reset your password:</p>
    <div style="font-size:28px;font-weight:700;letter-spacing:3px;padding:12px 16px;border:1px solid #e5e7eb;border-radius:10px;display:inline-block">
      ${safeCode}
    </div>
    <p style="margin:12px 0 0 0;color:#6b7280">This code expires in 10 minutes.</p>
  </div>`;
}

function buildEmailVerificationHtml(name, code) {
  const safeName = (name || "there").toString();
  const safeCode = String(code || "");
  return `
  <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
    <h2 style="margin:0 0 10px 0">Email verification</h2>
    <p style="margin:0 0 12px 0">Hi ${safeName},</p>
    <p style="margin:0 0 12px 0">Use this code to verify your email:</p>
    <div style="font-size:28px;font-weight:700;letter-spacing:3px;padding:12px 16px;border:1px solid #e5e7eb;border-radius:10px;display:inline-block">
      ${safeCode}
    </div>
    <p style="margin:12px 0 0 0;color:#6b7280">This code expires in 10 minutes.</p>
  </div>`;
}

function buildPasswordChangedEmailHtml(name) {
  const safeName = (name || "there").toString();
  return `
  <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
    <h2 style="margin:0 0 10px 0">Password changed</h2>
    <p style="margin:0 0 12px 0">Hi ${safeName},</p>
    <p style="margin:0 0 12px 0">Your Dental Clinic AI account password was changed successfully.</p>
    <p style="margin:0;color:#6b7280">If this wasn’t you, contact your clinic/admin immediately.</p>
  </div>`;
}

// ===================================
// HELPERS
// ===================================
const signupOtpStore = new Map(); // email -> { code, expiresAt, verified? }

function generateUid(role) {
  const n = Math.floor(1000 + Math.random() * 9000);
  if (role === "Admin") return `AD-${n}`;
  if (role === "Doctor") return `DC-${n}`;
  if (role === "Assistant") return `AS-${n}`;
  return `PT-${n}`;
}

function normalizeRoleFromClient(role) {
  // Client may send: Admin/Doctor/Patient/Assistant OR ADMIN/DOCTOR/PATIENT/ASSISTANT
  if (!role) return "Patient";
  const r = String(role).trim();
  const u = r.toUpperCase();
  if (u === "ADMIN" || r === "Admin") return "Admin";
  if (u === "DOCTOR" || r === "Doctor") return "Doctor";
  if (u === "ASSISTANT" || r === "Assistant") return "Assistant";
  return "Patient";
}

function normalizeRoleToClient(dbRole) {
  if (dbRole === "Admin") return "ADMIN";
  if (dbRole === "Doctor") return "DOCTOR";
  if (dbRole === "Assistant") return "ASSISTANT";
  return "PATIENT";
}

function createTokenForUser(user) {
  const payload = {
    id: user.id,
    uid: user.uid,
    role: user.role,
  };
  return jwt.sign(payload, JWT_SECRET, { expiresIn: "8h" });
}

// Helper to avoid 500 when SQL schema is incomplete
async function safeQuery(label, sql, params = []) {
  try {
    const [rows] = await pool.query(sql, params);
    return rows;
  } catch (err) {
    console.error(`${label} SQL ERROR:`, err);
    return [];
  }
}

// ===================================
// ✅ ADDITIVE SCHEMA FIXES (won’t remove/change existing data)
// ===================================
async function ensureUsersResetSchema() {
  // Your forgot/reset uses users.reset_code + users.reset_expires
  const alterSafe = async (sql) => {
    try {
      await pool.query(sql);
    } catch (_) {}
  };
  await alterSafe(`ALTER TABLE users ADD COLUMN reset_code VARCHAR(12) NULL`);
  await alterSafe(`ALTER TABLE users ADD COLUMN reset_expires DATETIME NULL`);
}
ensureUsersResetSchema().catch((e) => console.error("ensureUsersResetSchema failed:", e?.message || e));

async function ensureAgentEventsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS agent_events (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        event_type VARCHAR(64) NOT NULL,
        payload_json LONGTEXT NULL,
        status ENUM('NEW','PENDING','PROCESSING','DONE','FAILED') NOT NULL DEFAULT 'NEW',
        available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        locked_by VARCHAR(64) NULL,
        locked_until DATETIME NULL,
        attempts INT NOT NULL DEFAULT 0,
        max_attempts INT NOT NULL DEFAULT 7,
        priority INT NOT NULL DEFAULT 100,
        last_error TEXT NULL,
        expires_at DATETIME NULL,
        next_retry_at DATETIME NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_status_available (status, available_at),
        KEY idx_status_retry (status, next_retry_at),
        KEY idx_event_type (event_type),
        KEY idx_locked_until (locked_until),
        KEY idx_created_at (created_at)
      ) ENGINE=InnoDB;
    `);

    // Backward-compatible ALTERs (ignore if already exists)
    const alterSafe = async (sql) => {
      try {
        await pool.query(sql);
      } catch (_) {}
    };

    await alterSafe(`ALTER TABLE agent_events MODIFY COLUMN status ENUM('NEW','PENDING','PROCESSING','DONE','FAILED') NOT NULL DEFAULT 'NEW'`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER status`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN locked_by VARCHAR(64) NULL AFTER available_at`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN locked_until DATETIME NULL AFTER locked_by`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN attempts INT NOT NULL DEFAULT 0 AFTER locked_until`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN max_attempts INT NOT NULL DEFAULT 7 AFTER attempts`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN priority INT NOT NULL DEFAULT 100 AFTER max_attempts`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN last_error TEXT NULL AFTER priority`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN expires_at DATETIME NULL AFTER last_error`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN next_retry_at DATETIME NULL AFTER expires_at`);
    await alterSafe(`ALTER TABLE agent_events ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`);

    await alterSafe(`ALTER TABLE agent_events ADD KEY idx_status_available (status, available_at)`);
    await alterSafe(`ALTER TABLE agent_events ADD KEY idx_status_retry (status, next_retry_at)`);
    await alterSafe(`ALTER TABLE agent_events ADD KEY idx_event_type (event_type)`);
    await alterSafe(`ALTER TABLE agent_events ADD KEY idx_locked_until (locked_until)`);

    console.log("✅ agent_events schema ready (Python worker queue)");
  } catch (e) {
    console.error("ensureAgentEventsSchema failed:", e?.message || e);
  }
}
ensureAgentEventsSchema();

// ===================================
// ✅ COMPAT HELPERS (works even if ./agents is missing)
// ===================================
async function enqueueEventDb(eventType, payload, createdByUserId = null) {
  const safePayload = {
    ...(payload || {}),
    __meta: {
      createdByUserId: createdByUserId || null,
      createdAt: new Date().toISOString(),
    },
  };

  await pool.query(
    `
    INSERT INTO agent_events (event_type, payload_json, status, available_at)
    VALUES (?, ?, 'PENDING', NOW())
    `,
    [String(eventType), JSON.stringify(safePayload)]
  );
}

async function enqueueEventCompat({ eventType, payload, createdByUserId }) {
  // ✅ First preference: Node eventQueue module (if exists)
  if (enqueueEvent) {
    try {
      await enqueueEvent(pool, eventType, payload, { createdByUserId: createdByUserId || null });
      return;
    } catch (e1) {
      try {
        await enqueueEvent(pool, { eventType, payload, createdByUserId: createdByUserId || null });
        return;
      } catch (e2) {
        console.error("enqueueEventCompat -> enqueueEvent failed, falling back to DB:", e2?.message || e2);
      }
    }
  }

  // ✅ Current architecture: DB outbox for Python worker
  try {
    await enqueueEventDb(eventType, payload, createdByUserId || null);
  } catch (e) {
    console.error("enqueueEventCompat -> enqueueEventDb failed:", e?.message || e);
  }
}

async function retryFailedCompat(limit = 100) {
  const fn = retryFailedFromAgentsIndex || retryFailedFromEventQueue;

  if (fn) {
    try {
      const r = await fn(pool, { limit });
      if (typeof r === "number") return r;
      if (r && typeof r.updated === "number") return r.updated;
      if (r && typeof r.retried === "number") return r.retried;
      return 0;
    } catch (e1) {
      try {
        const r = await fn(pool, limit);
        if (typeof r === "number") return r;
        return 0;
      } catch (e2) {
        console.error("retryFailedCompat failed:", e2?.message || e2);
      }
    }
    return 0;
  }

  // ✅ Fallback: FAILED -> PENDING (IMPORTANT: enum does NOT include 'NEW')
  try {
    const [r] = await pool.query(
      `UPDATE agent_events
       SET status='NEW',
           available_at=NOW(),
           locked_by=NULL,
           locked_until=NULL,
           last_error=NULL
       WHERE status='FAILED'
       ORDER BY id ASC
       LIMIT ?`,
      [Number(limit) || 100]
    );
    return r?.affectedRows || 0;
  } catch (e) {
    console.error("retryFailedCompat fallback failed:", e?.message || e);
    return 0;
  }
}

// ===================================
// ✅ NOTIFICATIONS SCHEMA (safe, additive)
// ===================================
async function ensureNotificationsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS notifications (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        user_id BIGINT NULL,
        user_role VARCHAR(16) NULL,
        channel ENUM('IN_APP','EMAIL','SMS','WHATSAPP','CALL') NOT NULL DEFAULT 'IN_APP',
        type VARCHAR(64) NULL,
        title VARCHAR(200) NULL,
        message TEXT NOT NULL,
        status ENUM('NEW','PENDING','SENT','FAILED','READ') NOT NULL DEFAULT 'NEW',
        scheduled_at DATETIME NULL,
        read_at DATETIME NULL,
        priority INT NOT NULL DEFAULT 100,
        related_entity_type VARCHAR(40) NULL,
        related_entity_id BIGINT NULL,
        template_key VARCHAR(64) NULL,
        template_vars_json LONGTEXT NULL,
        meta_json LONGTEXT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sent_at DATETIME NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_user (user_id),
        KEY idx_role (user_role),
        KEY idx_status (status),
        KEY idx_created_at (created_at),
        KEY idx_scheduled_at (scheduled_at)
      ) ENGINE=InnoDB;
    `);

    const alterSafe = async (sql) => {
      try {
        await pool.query(sql);
      } catch (_) {}
    };

    await alterSafe(`ALTER TABLE notifications MODIFY COLUMN status ENUM('NEW','PENDING','SENT','FAILED','READ') NOT NULL DEFAULT 'NEW'`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN user_role VARCHAR(16) NULL AFTER user_id`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN type VARCHAR(64) NULL AFTER channel`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN title VARCHAR(200) NULL AFTER type`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN scheduled_at DATETIME NULL AFTER status`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN read_at DATETIME NULL AFTER scheduled_at`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN priority INT NOT NULL DEFAULT 100 AFTER read_at`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN related_entity_type VARCHAR(40) NULL AFTER priority`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN related_entity_id BIGINT NULL AFTER related_entity_type`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN template_key VARCHAR(64) NULL AFTER related_entity_id`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN template_vars_json LONGTEXT NULL AFTER template_key`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN meta_json LONGTEXT NULL AFTER template_vars_json`);
    await alterSafe(`ALTER TABLE notifications ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`);

    await alterSafe(`ALTER TABLE notifications ADD KEY idx_role (user_role)`);
    await alterSafe(`ALTER TABLE notifications ADD KEY idx_scheduled_at (scheduled_at)`);

    console.log("✅ Notifications schema ready");
  } catch (e) {
    console.error("ensureNotificationsSchema failed:", e?.message || e);
  }
}
ensureNotificationsSchema();

// ===================================
// ✅ CASE ATTACHMENTS SCHEMA (safe, additive)
// ===================================
async function ensureCaseAttachmentsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS case_attachments (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        case_id BIGINT NOT NULL,
        file_name VARCHAR(255) NOT NULL,
        file_path TEXT NOT NULL,
        mime_type VARCHAR(120) NULL,
        uploaded_by_user_id BIGINT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_case (case_id),
        KEY idx_created_at (created_at)
      ) ENGINE=InnoDB;
    `);
  } catch (e) {
    console.error("ensureCaseAttachmentsSchema failed:", e?.message || e);
  }
}
ensureCaseAttachmentsSchema();

// ===================================
// ✅ CLINIC SETUP + ROLES/PERMISSIONS (safe, additive)
// ===================================
async function ensureClinicSetupSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS clinic_settings (
        id TINYINT NOT NULL,
        clinic_name VARCHAR(120) NULL,
        clinic_phone VARCHAR(40) NULL,
        clinic_email VARCHAR(190) NULL,
        clinic_address TEXT NULL,
        timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata',
        working_hours_json LONGTEXT NULL,
        treatment_catalog_json LONGTEXT NULL,
        note_templates_json LONGTEXT NULL,
        ai_preferences_json LONGTEXT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB;
    `);

    await pool.query(`INSERT IGNORE INTO clinic_settings (id, timezone) VALUES (1, 'Asia/Kolkata')`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS role_permissions (
        role ENUM('Admin','Doctor','Assistant','Patient') NOT NULL,
        permissions_json LONGTEXT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (role)
      ) ENGINE=InnoDB;
    `);

    // Seed defaults (idempotent)
    await pool.query(
      `INSERT IGNORE INTO role_permissions (role, permissions_json) VALUES
        ('Admin', '{"admin_all":true}'),
        ('Doctor', '{"doctor_portal":true,"cases":true,"appointments":true}'),
        ('Assistant', '{"assistant_portal":true,"inventory":true,"appointments":true}'),
        ('Patient', '{"patient_portal":true,"appointments":true,"billing":true}')`
    );

    await pool.query(`
      CREATE TABLE IF NOT EXISTS patient_profiles (
        user_id BIGINT UNSIGNED NOT NULL,
        medical_history TEXT NULL,
        allergies TEXT NULL,
        notes TEXT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id)
      ) ENGINE=InnoDB;
    `);

    console.log('✅ clinic_settings + role_permissions + patient_profiles ready');
  } catch (e) {
    console.error('ensureClinicSetupSchema failed:', e?.message || e);
  }
}
ensureClinicSetupSchema();

// ===================================
// ✅ Schema capability detection (to keep conflict logic robust across DB versions)
// ===================================
const schemaCaps = {
  appointments_has_scheduled_end_time: true,
  appointments_has_operatory_id: true,
};

async function detectSchemaCaps() {
  try {
    const [cols] = await pool.query(
      `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'appointments'
      `
    );
    const set = new Set((cols || []).map((r) => String(r.COLUMN_NAME || "").toLowerCase()));
    schemaCaps.appointments_has_scheduled_end_time = set.has("scheduled_end_time");
    schemaCaps.appointments_has_operatory_id = set.has("operatory_id");
  } catch (_) {
    // defaults remain true; conflict checks are wrapped anyway
  }
}
detectSchemaCaps();


// ===================================
// HEALTH CHECK
// ===================================
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", tz: APP_TZ, today: formatDateYYYYMMDD() });
});

// ===================================
// SIGN-UP EMAIL OTP (for Create Account)
// ===================================
app.post("/api/auth/email-otp/request", async (req, res) => {
  try {
    const { email } = req.body || {};
    console.log("👉 /api/auth/email-otp/request called with:", req.body);
    if (!email) {
      console.log("❌ Email missing");
      return res.status(400).json({ message: "Email is required" });
    }

    const normalizedEmail = String(email).trim().toLowerCase();

    const [existing] = await pool.query("SELECT id, full_name FROM users WHERE email = ?", [normalizedEmail]);
    console.log("🔍 Existing user check:", existing);
    if (existing.length > 0) {
      console.log("❌ Email already registered");
      return res.status(409).json({ message: "Email already registered. Please login instead." });
    }

    const otp = generateOtp();
    const expiresAt = Date.now() + 10 * 60 * 1000; // 10 minutes
    signupOtpStore.set(normalizedEmail, { code: otp, expiresAt });


    const html = buildEmailVerificationHtml(null, otp);

    console.log("📧 Sending email via mailer...");
    await mailer.sendMail({
      from: process.env.MAIL_FROM || '"Dental Clinic AI" <no-reply@clinic.ai>',
      to: normalizedEmail,
      subject: "Verify your Dental Clinic AI email",
      html: buildEmailVerificationHtml(null, otp),
      text: `Your Dental Clinic AI email verification code is ${otp}. It expires in 10 minutes.`,
    });

    return res.json({ message: "Verification code sent" });
  } catch (err) {
    console.error("❌ EMAIL OTP REQUEST ERROR:", err);
    return res.status(500).json({ message: "Server error: " + err.message });
  }
});

app.post("/api/auth/email-otp/verify", (req, res) => {
  try {
    const { email, otp } = req.body || {};
    if (!email || !otp) {
      return res.status(400).json({ message: "Email and OTP are required", valid: false });
    }

    const normalizedEmail = String(email).trim().toLowerCase();
    const entry = signupOtpStore.get(normalizedEmail);

    if (!entry) {
      return res.status(400).json({ message: "No verification code for this email", valid: false });
    }

    const now = Date.now();
    if (now > entry.expiresAt) {
      signupOtpStore.delete(normalizedEmail);
      return res.status(400).json({ message: "Code has expired", valid: false });
    }

    if (entry.code !== String(otp).trim()) {
      return res.status(400).json({ message: "Invalid code", valid: false });
    }

    signupOtpStore.set(normalizedEmail, { ...entry, verified: true });
    return res.json({ message: "Email verified", valid: true });
  } catch (err) {
    console.error("EMAIL OTP VERIFY ERROR:", err);
    return res.status(500).json({ message: "Server error", valid: false });
  }
});

// ===================================
// REGISTER / LOGIN / PASSWORD FLOWS
// ===================================
app.post("/api/auth/register", async (req, res) => {
  try {
    const { role, fullName, email, phone, dob, gender, address, password } = req.body || {};

    if (!role || !fullName || !email || !password) {
      return res.status(400).json({ message: "Missing required fields" });
    }

    const roleDb = normalizeRoleFromClient(role);
    if (!["Admin", "Doctor", "Patient"].includes(roleDb)) {
      return res.status(400).json({
        message: "Invalid role",
        allowed: ["ADMIN", "DOCTOR", "PATIENT"],
      });
    }

    const normalizedEmail = String(email).trim().toLowerCase();

    const [existing] = await pool.query("SELECT id FROM users WHERE email = ?", [normalizedEmail]);
    if (existing.length > 0) {
      return res.status(409).json({ message: "Email already registered" });
    }

    const passwordHash = await bcrypt.hash(String(password), 10);
    const uid = generateUid(roleDb);

    const [result] = await pool.query(
      `INSERT INTO users
        (uid, full_name, email, phone, dob, gender, address, role, password_hash)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        uid,
        String(fullName).trim(),
        normalizedEmail,
        phone ? String(phone).trim() : null,
        dob ? String(dob).trim() : null,
        gender ? String(gender).trim() : null,
        address ? String(address).trim() : null,
        roleDb,
        passwordHash,
      ]
    );

    const newUserId = result.insertId;

    const token = createTokenForUser({
      id: newUserId,
      uid,
      role: roleDb,
    });

    return res.status(201).json({
      message: "User created",
      uid,
      name: String(fullName).trim(),
      role: normalizeRoleToClient(roleDb),
      token,
    });
  } catch (err) {
    console.error("REGISTER ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, password, role } = req.body || {};

    if (!email || !password || !role) {
      return res.status(400).json({ message: "Missing credentials" });
    }

    const roleDb = normalizeRoleFromClient(role);
    const normalizedEmail = String(email).trim().toLowerCase();

    const [rows] = await pool.query("SELECT * FROM users WHERE email = ? AND role = ?", [normalizedEmail, roleDb]);

    if (rows.length === 0) {
      return res.status(401).json({ message: "Invalid email, password, or role" });
    }

    const user = rows[0];
    const isValid = await bcrypt.compare(password, user.password_hash);
    if (!isValid) {
      return res.status(401).json({ message: "Invalid email, password, or role" });
    }

    const token = createTokenForUser(user);

    return res.json({
      token,
      uid: user.uid,
      name: user.full_name,
      role: normalizeRoleToClient(user.role),
    });
  } catch (err) {
    console.error("LOGIN ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.post("/api/auth/forgot-password", async (req, res) => {
  try {
    const { email } = req.body || {};

    if (!email) {
      return res.status(400).json({ message: "Email is required" });
    }

    const normalizedEmail = String(email).trim().toLowerCase();

    const [rows] = await pool.query("SELECT id, full_name, email FROM users WHERE email = ?", [normalizedEmail]);

    // ✅ Don’t reveal existence (security). Same response either way.
    if (rows.length === 0) {
      return res.json({ message: "If that email exists, an OTP has been sent." });
    }

    const user = rows[0];
    const otp = generateOtp();
    const expires = new Date(Date.now() + 10 * 60 * 1000);

    await pool.query("UPDATE users SET reset_code = ?, reset_expires = ? WHERE id = ?", [otp, expires, user.id]);

    const html = buildOtpEmailHtml(user.full_name, otp);

    await mailer.sendMail({
      from: process.env.MAIL_FROM || '"Dental Clinic AI" <no-reply@clinic.ai>',
      to: user.email,
      subject: "Your Dental Clinic AI password reset code",
      html,
      text: `Your Dental Clinic AI password reset code is ${otp}. It expires in 10 minutes.`,
    });

    return res.json({ message: "If that email exists, an OTP has been sent." });
  } catch (err) {
    console.error("FORGOT PASSWORD ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.post("/api/auth/verify-otp", async (req, res) => {
  try {
    const { email, otp } = req.body || {};

    if (!email || !otp) {
      return res.status(400).json({ message: "Email and OTP are required" });
    }

    const normalizedEmail = String(email).trim().toLowerCase();

    const [rows] = await pool.query("SELECT id, reset_code, reset_expires FROM users WHERE email = ?", [normalizedEmail]);

    if (rows.length === 0) {
      return res.status(400).json({ message: "Invalid email or code" });
    }

    const user = rows[0];

    if (!user.reset_code || !user.reset_expires) {
      return res.status(400).json({ message: "No active reset code for this email" });
    }

    const now = Date.now();
    const expiresAt = new Date(user.reset_expires).getTime();

    if (String(user.reset_code) !== String(otp)) {
      return res.status(400).json({ message: "Invalid code" });
    }

    if (now > expiresAt) {
      return res.status(400).json({ message: "Code has expired" });
    }

    return res.json({ message: "Code verified" });
  } catch (err) {
    console.error("VERIFY OTP ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.post("/api/auth/reset-password", async (req, res) => {
  try {
    const { email, otp, newPassword } = req.body || {};

    if (!email || !otp || !newPassword) {
      return res.status(400).json({ message: "Missing required fields" });
    }

    const normalizedEmail = String(email).trim().toLowerCase();

    const [rows] = await pool.query(
      "SELECT id, full_name, reset_code, reset_expires FROM users WHERE email = ?",
      [normalizedEmail]
    );

    if (rows.length === 0) {
      return res.status(400).json({ message: "Invalid email or code" });
    }

    const user = rows[0];

    if (!user.reset_code || !user.reset_expires) {
      return res.status(400).json({ message: "No active reset code for this email" });
    }

    const now = Date.now();
    const expiresAt = new Date(user.reset_expires).getTime();

    if (String(user.reset_code) !== String(otp)) {
      return res.status(400).json({ message: "Invalid code" });
    }

    if (now > expiresAt) {
      return res.status(400).json({ message: "Code has expired" });
    }

    const passwordHash = await bcrypt.hash(newPassword, 10);

    await pool.query(
      "UPDATE users SET password_hash = ?, reset_code = NULL, reset_expires = NULL WHERE id = ?",
      [passwordHash, user.id]
    );

    try {
      const html = buildPasswordChangedEmailHtml(user.full_name);
      await mailer.sendMail({
        from: process.env.MAIL_FROM || '"Dental Clinic AI" <no-reply@clinic.ai>',
        to: normalizedEmail,
        subject: "Your Dental Clinic AI password was changed",
        html,
        text: "Your Dental Clinic AI account password was just changed. If this was not you, contact your clinic or administrator immediately.",
      });
    } catch (emailErr) {
      console.error("PASSWORD CHANGE EMAIL ERROR:", emailErr);
    }

    return res.json({ message: "Password reset successfully" });
  } catch (err) {
    console.error("RESET PASSWORD ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

// ===================================
// AUTH MIDDLEWARE  ✅ header token + optional query token (SSE safe)
// ===================================
// ===================================
// AUTH MIDDLEWARE  ✅ header token + optional query token (SSE safe)
// ===================================
const PUBLIC_PREFIXES = [
  "/api/health",
  "/debug/routes",

  // auth endpoints must be public
  "/api/auth/login",
  "/api/auth/register",
  "/api/auth/forgot-password",
  "/api/auth/verify-otp",
  "/api/auth/reset-password",
  "/api/auth/email-otp/request",
  "/api/auth/email-otp/verify",

  // static uploads should be public (or keep it protected if you want)
  "/uploads",
];

function isPublicPath(req) {
  const p = String(req.path || "");
  return PUBLIC_PREFIXES.some((x) => p === x || p.startsWith(x + "/"));
}

function authMiddleware(req, res, next) {
  // ✅ IMPORTANT: never require JWT for login/register/otp endpoints
  if (isPublicPath(req)) return next();

  const authHeader = req.headers.authorization || "";
  const tokenFromHeader = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  const tokenFromQuery = req.query?.token ? String(req.query.token) : ""; // SSE support
  const token = tokenFromHeader || tokenFromQuery;

  if (!token) {
    return res.status(401).json({ message: "Missing token", code: "NO_TOKEN" });
  }

  try {
    // clockTolerance helps small system clock skews (common on new laptops)
    const decoded = jwt.verify(token, JWT_SECRET, { clockTolerance: 60 });
    req.user = decoded;
    return next();
  } catch (err) {
    const msg = err?.message || "Invalid or expired token";

    // ✅ Make expired-token explicit so frontend can auto-logout/redirect
    if (err?.name === "TokenExpiredError") {
      console.error("AUTH ERROR:", msg);
      return res.status(401).json({
        message: "Token expired. Please login again.",
        code: "TOKEN_EXPIRED",
      });
    }

    console.error("AUTH ERROR:", msg);
    return res.status(401).json({ message: "Invalid token", code: "INVALID_TOKEN" });
  }
}


app.get("/api/auth/me", authMiddleware, (req, res) => {
  res.json({ user: req.user });
});

function requireRole(...allowed) {
  return (req, res, next) => {
    if (!req.user || !allowed.includes(req.user.role)) {
      return res.status(403).json({ message: "Forbidden" });
    }
    next();
  };
}

// ===================================
// ✅ NOTIFICATIONS API
// ===================================
app.get("/api/notifications", authMiddleware, async (req, res) => {
  try {
    const includeRead = String(req.query.includeRead || "1") === "1";
    const limit = Math.min(Math.max(parseInt(String(req.query.limit || "200"), 10) || 200, 1), 500);

    const userId = req.user.id;
    const userRole = req.user.role;

    const rows = await safeQuery(
      "NOTIFICATIONS LIST",
      `
      SELECT
        id, channel, type, title, message, status,
        DATE_FORMAT(scheduled_at, '%Y-%m-%d %H:%i:%s') AS scheduled_at,
        DATE_FORMAT(read_at, '%Y-%m-%d %H:%i:%s') AS read_at,
        DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at
      FROM notifications
      WHERE (
        user_id = ?
        OR (user_id IS NULL AND user_role = ?)
        OR (user_id IS NULL AND user_role IS NULL)
      )
      AND (${includeRead ? "1=1" : "status <> 'READ'"})
      ORDER BY id DESC
      LIMIT ?
      `,
      [userId, userRole, limit]
    );

    res.json({ items: rows });
  } catch (e) {
    console.error("GET /api/notifications error:", e);
    res.json({ items: [], error: true });
  }
});

app.post("/api/notifications/:id/read", authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const userRole = req.user.role;
    const id = Number(req.params.id);

    await pool.query(
      `
      UPDATE notifications
      SET status='READ', read_at=NOW()
      WHERE id = ?
        AND (
          user_id = ?
          OR (user_id IS NULL AND user_role = ?)
          OR (user_id IS NULL AND user_role IS NULL)
        )
      `,
      [id, userId, userRole]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("POST read notification error:", e);
    res.json({ ok: false, error: true });
  }
});

app.post("/api/notifications/read-all", authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const userRole = req.user.role;

    await pool.query(
      `
      UPDATE notifications
      SET status='READ', read_at=NOW()
      WHERE status <> 'READ'
        AND (
          user_id = ?
          OR (user_id IS NULL AND user_role = ?)
          OR (user_id IS NULL AND user_role IS NULL)
        )
      `,
      [userId, userRole]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("POST read-all notifications error:", e);
    res.json({ ok: false, error: true });
  }
});

// ===================================
// ✅ REALTIME SSE (NO websockets)
// ===================================
app.get("/api/events/stream", authMiddleware, (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const userId = req.user.id;
  const userRole = req.user.role;

  let lastId = 0;
  let alive = true;

  req.on("close", () => {
    alive = false;
  });

  res.write(`event: hello\ndata: ${JSON.stringify({ ok: true })}\n\n`);

  const interval = setInterval(async () => {
    if (!alive) return clearInterval(interval);

    const rows = await safeQuery(
      "SSE notifications",
      `
      SELECT id, type, title, message, status,
             DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at
      FROM notifications
      WHERE id > ?
        AND (
          user_id = ?
          OR (user_id IS NULL AND user_role = ?)
          OR (user_id IS NULL AND user_role IS NULL)
        )
      ORDER BY id ASC
      LIMIT 25
      `,
      [lastId, userId, userRole]
    );

    for (const n of rows) {
      lastId = n.id;
      res.write(`event: notification\n`);
      res.write(`data: ${JSON.stringify(n)}\n\n`);
    }

    res.write(`event: ping\ndata: ${Date.now()}\n\n`);
  }, 3000);
});

// ===================================
// ADMIN ROUTES
// ===================================
const ADMIN_BASE = "/api/admin";

// ===================================
// ✅ ADMIN: CLINIC SETUP (clinic profile, hours, templates, permissions)
// ===================================
app.get(
  `${ADMIN_BASE}/clinic-setup`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const rows = await safeQuery(
        "CLINIC SETTINGS",
        `SELECT
           id, clinic_name, clinic_phone, clinic_email, clinic_address, timezone,
           working_hours_json, treatment_catalog_json, note_templates_json, ai_preferences_json,
           updated_at
         FROM clinic_settings
         WHERE id = 1
         LIMIT 1`,
        []
      );

      const r = rows[0] || {};
      const parseJson = (x, fallback) => {
        try {
          return x ? JSON.parse(String(x)) : fallback;
        } catch {
          return fallback;
        }
      };

      // role permissions
      const rp = await safeQuery(
        "ROLE PERMISSIONS",
        `SELECT role, permissions_json FROM role_permissions ORDER BY role`,
        []
      );

      const permissions = {};
      for (const row of rp) {
        permissions[row.role] = parseJson(row.permissions_json, {});
      }

      return res.json({
        clinic: {
          clinicName: r.clinic_name || "",
          phone: r.clinic_phone || "",
          email: r.clinic_email || "",
          address: r.clinic_address || "",
          timezone: r.timezone || "Asia/Kolkata",
          workingHours: parseJson(r.working_hours_json, { start: "09:00", end: "18:00", stepMin: 15, days: [1,2,3,4,5,6] }),
          treatmentCatalog: parseJson(r.treatment_catalog_json, []),
          noteTemplates: parseJson(r.note_templates_json, []),
          aiPreferences: parseJson(r.ai_preferences_json, { enableAiSummaries: true, enableSmartScheduling: true }),
          updatedAt: r.updated_at || null,
        },
        permissions,
      });
    } catch (e) {
      console.error("GET clinic-setup error:", e);
      return res.json({
        clinic: {
          clinicName: "",
          phone: "",
          email: "",
          address: "",
          timezone: "Asia/Kolkata",
          workingHours: { start: "09:00", end: "18:00", stepMin: 15, days: [1,2,3,4,5,6] },
          treatmentCatalog: [],
          noteTemplates: [],
          aiPreferences: { enableAiSummaries: true, enableSmartScheduling: true },
          updatedAt: null,
        },
        permissions: {},
        error: true,
      });
    }
  }
);

app.put(
  `${ADMIN_BASE}/clinic-setup`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const {
        clinicName,
        phone,
        email,
        address,
        timezone,
        workingHours,
        treatmentCatalog,
        noteTemplates,
        aiPreferences,
        permissions,
      } = req.body || {};

      await pool.query(
        `UPDATE clinic_settings
         SET clinic_name = ?, clinic_phone = ?, clinic_email = ?, clinic_address = ?, timezone = ?,
             working_hours_json = ?, treatment_catalog_json = ?, note_templates_json = ?, ai_preferences_json = ?,
             updated_at = NOW()
         WHERE id = 1`,
        [
          clinicName ? String(clinicName).trim() : null,
          phone ? String(phone).trim() : null,
          email ? String(email).trim() : null,
          address ? String(address).trim() : null,
          timezone ? String(timezone).trim() : "Asia/Kolkata",
          workingHours ? JSON.stringify(workingHours) : null,
          treatmentCatalog ? JSON.stringify(treatmentCatalog) : null,
          noteTemplates ? JSON.stringify(noteTemplates) : null,
          aiPreferences ? JSON.stringify(aiPreferences) : null,
        ]
      );

      // Save role permissions (config only; middleware can enforce later)
      if (permissions && typeof permissions === "object") {
        for (const [role, perms] of Object.entries(permissions)) {
          if (!["Admin","Doctor","Assistant","Patient"].includes(String(role))) continue;
          await pool.query(
            `INSERT INTO role_permissions (role, permissions_json)
             VALUES (?, ?)
             ON DUPLICATE KEY UPDATE permissions_json = VALUES(permissions_json), updated_at = NOW()`,
            [String(role), JSON.stringify(perms || {})]
          );
        }
      }

      return res.json({ ok: true });
    } catch (e) {
      console.error("PUT clinic-setup error:", e);
      return res.status(500).json({ message: "Failed to save clinic setup" });
    }
  }
);

// ===================================
// ✅ ADMIN: USER MANAGEMENT (create staff + patients, list users)
// ===================================
function roleDbFromClient(role) {
  const upper = String(role || "").toUpperCase().trim();
  if (upper === "ADMIN") return "Admin";
  if (upper === "DOCTOR") return "Doctor";
  if (upper === "ASSISTANT") return "Assistant";
  return "Patient";
}

function roleClientFromDb(roleDb) {
  if (roleDb === "Admin") return "ADMIN";
  if (roleDb === "Doctor") return "DOCTOR";
  if (roleDb === "Assistant") return "ASSISTANT";
  return "PATIENT";
}

function randomTempPassword() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$";
  let out = "";
  for (let i = 0; i < 10; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function buildInviteEmailHtml(name, roleLabel, tempPassword) {
  const safeName = (name || "there").toString();
  const safeRole = (roleLabel || "user").toString();
  const safePw = (tempPassword || "").toString();
  return `
  <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto">
    <h2 style="margin:0 0 10px 0">Your ${safeRole} account is ready</h2>
    <p style="margin:0 0 10px 0">Hi ${safeName},</p>
    <p style="margin:0 0 10px 0">An admin created your Dental Clinic AI account.</p>
    <p style="margin:0 0 10px 0"><b>Temporary password:</b></p>
    <div style="font-size:20px;font-weight:700;letter-spacing:1px;padding:10px 14px;border:1px solid #e5e7eb;border-radius:10px;display:inline-block">
      ${safePw}
    </div>
    <p style="margin:12px 0 0 0;color:#6b7280">Login with this password and change it immediately using “Forgot password”.</p>
  </div>`;
}

app.get(
  `${ADMIN_BASE}/users`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const role = req.query.role ? String(req.query.role) : "";
      const roleDb = role ? roleDbFromClient(role) : null;

      const rows = await safeQuery(
        "ADMIN USERS LIST",
        `SELECT id, uid, full_name, email, phone, role, created_at
         FROM users
         WHERE (? IS NULL OR role = ?)
         ORDER BY created_at DESC
         LIMIT 500`,
        [roleDb, roleDb]
      );

      const items = rows.map((u) => ({
        id: u.id,
        uid: u.uid,
        fullName: u.full_name,
        email: u.email,
        phone: u.phone,
        role: roleClientFromDb(u.role),
        createdAt: u.created_at,
      }));

      return res.json({ items });
    } catch (e) {
      console.error("GET admin/users error:", e);
      return res.json({ items: [], error: true });
    }
  }
);

app.post(
  `${ADMIN_BASE}/users`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const {
        role,
        fullName,
        email,
        phone,
        dob,
        gender,
        address,
        medicalHistory,
        allergies,
        notes,
        sendInviteEmail,
        tempPassword,
      } = req.body || {};

      if (!role || !fullName) {
        return res.status(400).json({ message: "role and fullName are required" });
      }

      const roleDb = roleDbFromClient(role);
      if (!["Admin","Doctor","Assistant","Patient"].includes(roleDb)) {
        return res.status(400).json({ message: "Invalid role" });
      }

      const name = String(fullName).trim();
      const normalizedEmail = email ? String(email).trim().toLowerCase() : null;

      // Require email for staff roles (so they can login)
      if (["Admin","Doctor","Assistant"].includes(roleDb) && !normalizedEmail) {
        return res.status(400).json({ message: "Email is required for staff accounts" });
      }

      if (normalizedEmail) {
        const [existing] = await pool.query(`SELECT id FROM users WHERE email = ? LIMIT 1`, [normalizedEmail]);
        if (existing.length) {
          return res.status(409).json({ message: "Email already exists" });
        }
      }

      const uid = generateUid(roleDb);

      // Password behavior:
      // - Staff: always create a password (provided or generated) so login works
      // - Patient: optional password; if email provided, create password so patient can use portal
      let passwordHash = null;
      let usedTempPassword = null;

      if (["Admin","Doctor","Assistant"].includes(roleDb) || normalizedEmail) {
        const pw = tempPassword && String(tempPassword).trim() ? String(tempPassword).trim() : randomTempPassword();
        usedTempPassword = pw;
        passwordHash = await bcrypt.hash(pw, 10);
      }

      const [result] = await pool.query(
        `INSERT INTO users
          (uid, full_name, email, phone, dob, gender, address, role, password_hash)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          uid,
          name,
          normalizedEmail,
          phone ? String(phone).trim() : null,
          dob ? String(dob).trim() : null,
          gender ? String(gender).trim() : null,
          address ? String(address).trim() : null,
          roleDb,
          passwordHash,
        ]
      );

      const newUserId = result.insertId;

      // Patient profile extras
      if (roleDb === "Patient") {
        try {
          await pool.query(
            `INSERT INTO patient_profiles (user_id, medical_history, allergies, notes)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE medical_history = VALUES(medical_history), allergies = VALUES(allergies), notes = VALUES(notes)`,
            [
              newUserId,
              medicalHistory ? String(medicalHistory) : null,
              allergies ? String(allergies) : null,
              notes ? String(notes) : null,
            ]
          );
        } catch (e) {
          console.error("patient_profiles upsert failed:", e?.message || e);
        }
      }

      // Optional invite email
      const shouldSend =
        String(sendInviteEmail || "").toLowerCase() === "true" ||
        sendInviteEmail === 1 ||
        sendInviteEmail === true;

      if (shouldSend && normalizedEmail && usedTempPassword) {
        try {
          const roleLabel = roleDb === "Doctor" ? "Doctor" : roleDb === "Assistant" ? "Assistant" : roleDb;
          const html = buildInviteEmailHtml(name, roleLabel, usedTempPassword);
          await transporter.sendMail({
            from: process.env.MAIL_FROM || '"Dental Clinic AI" <no-reply@clinic.ai>',
            to: normalizedEmail,
            subject: "Your Dental Clinic AI account details",
            html,
            text: `Your ${roleLabel} account is ready. Temporary password: ${usedTempPassword}. Please change it using Forgot password after login.`,
          });
        } catch (e) {
          console.error("Invite email failed:", e?.message || e);
        }
      }

      return res.status(201).json({
        message: "User created",
        user: {
          id: newUserId,
          uid,
          fullName: name,
          email: normalizedEmail,
          phone: phone || null,
          role: roleClientFromDb(roleDb),
        },
      });
    } catch (e) {
      console.error("POST admin/users error:", e);
      return res.status(500).json({ message: "Failed to create user" });
    }
  }
);


// DASHBOARD SUMMARY (UNCHANGED)
app.get(
  `${ADMIN_BASE}/dashboard-summary`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const todayStr = formatDateYYYYMMDD(new Date());

    try {
      const rowsAppointments = await safeQuery(
        "ADMIN DASHBOARD appointments",
        `SELECT COUNT(*) AS count
         FROM appointments
         WHERE scheduled_date = ? AND status IN ('Confirmed','Checked in','Completed')`,
        [todayStr]
      );
      const todayAppointments = rowsAppointments[0]?.count ?? 0;

      const rowsLowStock = await safeQuery(
        "ADMIN DASHBOARD low stock",
        `SELECT COUNT(*) AS count
         FROM inventory_items
         WHERE stock <= reorder_threshold`,
        []
      );
      const lowStockItems = rowsLowStock[0]?.count ?? 0;

      const rowsRevenueToday = await safeQuery(
        "ADMIN DASHBOARD revenue today",
        `SELECT COALESCE(SUM(amount),0) AS total
         FROM invoices
         WHERE paid_date = ?
           AND status = 'Paid'`,
        [todayStr]
      );
      const todaysRevenue = Number(rowsRevenueToday[0]?.total || 0);

      const rowsCases = await safeQuery(
        "ADMIN DASHBOARD case pipeline",
        `SELECT stage, COUNT(*) AS count
         FROM cases
         GROUP BY stage`,
        []
      );
      const pipeline = {
        NEW: 0,
        IN_TREATMENT: 0,
        WAITING_ON_PATIENT: 0,
        READY_TO_CLOSE: 0,
        CLOSED: 0,
        BLOCKED: 0,
      };
      for (const r of rowsCases) {
        if (r.stage && Object.prototype.hasOwnProperty.call(pipeline, r.stage)) {
          pipeline[r.stage] = r.count;
        }
      }

      const activeCases =
        (pipeline.NEW || 0) +
        (pipeline.IN_TREATMENT || 0) +
        (pipeline.WAITING_ON_PATIENT || 0) +
        (pipeline.READY_TO_CLOSE || 0);

      const rowsNewPatients = await safeQuery(
        "ADMIN DASHBOARD new patients",
        `SELECT COUNT(*) AS count
         FROM users
         WHERE role = 'Patient'
           AND DATE(created_at) = ?`,
        [todayStr]
      );
      const newPatientsToday = rowsNewPatients[0]?.count ?? 0;

      const rowsReturningPatients = await safeQuery(
        "ADMIN DASHBOARD returning patients",
        `SELECT COUNT(DISTINCT a.patient_id) AS count
         FROM appointments a
         JOIN users u ON u.id = a.patient_id
         WHERE a.scheduled_date = ?
           AND u.role = 'Patient'
           AND u.created_at < ?`,
        [todayStr, todayStr]
      );
      const returningPatientsToday = rowsReturningPatients[0]?.count ?? 0;

      const rowsCancelled = await safeQuery(
        "ADMIN DASHBOARD cancellations",
        `SELECT COUNT(*) AS count
         FROM appointments
         WHERE scheduled_date = ?
           AND status = 'Cancelled'`,
        [todayStr]
      );
      const cancelledAppointmentsToday = rowsCancelled[0]?.count ?? 0;

      res.json({
        todayAppointments,
        todayAppointmentsDelta: 0,
        lowStockItems,
        todaysRevenue,
        todaysRevenueDeltaPercent: 0,
        activeCases,
        casePipeline: {
          new: pipeline.NEW,
          inTreatment: pipeline.IN_TREATMENT,
          awaitingFollowUp: pipeline.WAITING_ON_PATIENT,
        },
        patientSnapshot: {
          newPatientsToday,
          returningPatientsToday,
          cancelledAppointmentsToday,
        },
        asOf: todayStr,
      });
    } catch (err) {
      console.error("ADMIN DASHBOARD SUMMARY HANDLER ERROR:", err);
      res.json({
        todayAppointments: 0,
        todayAppointmentsDelta: 0,
        lowStockItems: 0,
        todaysRevenue: 0,
        todaysRevenueDeltaPercent: 0,
        activeCases: 0,
        casePipeline: { new: 0, inTreatment: 0, awaitingFollowUp: 0 },
        patientSnapshot: {
          newPatientsToday: 0,
          returningPatientsToday: 0,
          cancelledAppointmentsToday: 0,
        },
        asOf: todayStr,
        error: true,
      });
    }
  }
);

// ✅ Fallback suggest-slots logic (works without Node agents; safe, additive)
async function fallbackSuggestSlots({ doctorId, operatoryId, dateStr, durationMin }) {
  const stepMin = 15;
  const clinicStart = process.env.CLINIC_START || "09:00:00";
  const clinicEnd = process.env.CLINIC_END || "18:00:00";

  // Parse HH:MM:SS to minutes
  const toMin = (t) => {
    const [hh, mm] = String(t).split(":").map((x) => parseInt(x, 10));
    return (hh || 0) * 60 + (mm || 0);
  };
  const toTime = (m) => {
    const hh = String(Math.floor(m / 60)).padStart(2, "0");
    const mm = String(m % 60).padStart(2, "0");
    return `${hh}:${mm}:00`;
  };

  const startM = toMin(clinicStart);
  const endM = toMin(clinicEnd);
  const dur = Math.max(5, Number(durationMin) || 30);

  // Pull existing appointments for doctor (and operatory if provided)
  let rows = [];
  try {
    rows = await safeQuery(
      "SUGGEST fallback appointments",
      `
      SELECT scheduled_time, scheduled_end_time, predicted_duration_min, status, operatory_id
      FROM appointments
      WHERE scheduled_date = ?
        AND status NOT IN ('Cancelled')
        AND (
          doctor_id = ?
          ${schemaCaps.appointments_has_operatory_id && operatoryId ? " OR operatory_id = ?" : ""}
        )
      `,
      schemaCaps.appointments_has_operatory_id && operatoryId ? [dateStr, doctorId, operatoryId] : [dateStr, doctorId]
    );
  } catch (_) {
    rows = [];
  }

  const busy = [];
  for (const r of rows) {
    const st = toTimeStr(r.scheduled_time || "");
    if (!st) continue;

    let et = null;

    // Prefer scheduled_end_time when present
    if (schemaCaps.appointments_has_scheduled_end_time && r.scheduled_end_time) {
      et = toTimeStr(r.scheduled_end_time);
    }

    // Otherwise compute from predicted_duration_min
    if (!et) {
      const pd = Number(r.predicted_duration_min || 0) || 30;
      const m = toMin(st) + pd;
      et = toTime(m);
    }

    busy.push([toMin(st), toMin(et)]);
  }

  const isFree = (s, e) => {
    for (const [bs, be] of busy) {
      if (s < be && e > bs) return false; // overlap
    }
    return true;
  };

  const out = [];
  for (let m = startM; m + dur <= endM; m += stepMin) {
    const s = m;
    const e = m + dur;
    if (isFree(s, e)) out.push({ time: toTime(s).slice(0, 5), timeStr: toTime(s) });
    if (out.length >= 10) break;
  }

  return out;
}

// ✅ NEW: smart slot suggestions (does not break existing create appointment)
app.post(
  `${ADMIN_BASE}/appointments/suggest-slots`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const { doctorUid, date, type, operatoryId } = req.body || {};
      if (!doctorUid || !date) {
        return res.status(400).json({ message: "doctorUid and date required" });
      }

      const [drows] = await pool.query(`SELECT id FROM users WHERE uid = ? AND role = 'Doctor' LIMIT 1`, [doctorUid]);
      if (drows.length === 0) {
        return res.status(400).json({ message: "Doctor not found" });
      }

      let slots = [];

      // ✅ Guard require (won’t crash if Node agents removed)
      try {
        const appointmentAgent = require("./agents/appointmentAgent");
        const suggestSlots = appointmentAgent?.suggestSlots;
        if (typeof suggestSlots === "function") {
          slots = await suggestSlots(pool, {
            doctorId: drows[0].id,
            operatoryId: operatoryId || null,
            dateStr: toDateStr(date),
            type: type || "General",
          });
        }
      } catch (_) {
        slots = [];
      }

      // ✅ If Node agent not present / returned empty => fallback suggestions (server-side deterministic)
      if (!Array.isArray(slots) || slots.length === 0) {
        const durationMin = 30;
        const fb = await fallbackSuggestSlots({
          doctorId: drows[0].id,
          operatoryId: operatoryId || null,
          dateStr: toDateStr(date),
          durationMin,
        });
        slots = fb.map((x) => x.time);
      }

      return res.json({ slots });
    } catch (err) {
      console.error("SUGGEST SLOTS ERROR:", err);
      return res.json({ slots: [], error: true });
    }
  }
);

// APPOINTMENTS LIST (UNCHANGED)
app.get(
  `${ADMIN_BASE}/appointments`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const { date } = req.query;
    const dateFilter =
      typeof date === "string" && date.length ? toDateStr(date) : formatDateYYYYMMDD(new Date());

    try {
      const rows = await safeQuery(
        "ADMIN APPOINTMENTS LIST",
        `SELECT
           a.id,
           a.appointment_uid,
           DATE_FORMAT(a.scheduled_date, '%Y-%m-%d') AS scheduled_date,
           TIME_FORMAT(a.scheduled_time, '%h:%i %p') AS time_display,
           a.type,
           a.status,
           p.full_name AS patient_name,
           d.full_name AS doctor_name
         FROM appointments a
         LEFT JOIN users p ON p.id = a.patient_id
         LEFT JOIN users d ON d.id = a.doctor_id
         WHERE a.scheduled_date = ?
         ORDER BY a.scheduled_date, a.scheduled_time`,
        [dateFilter]
      );

      const items = rows.map((r) => ({
        id: r.appointment_uid || r.id,
        date: r.scheduled_date || null,
        time: r.time_display || null,
        patient: r.patient_name || "—",
        doctor: r.doctor_name || "—",
        type: r.type || "General",
        status: r.status || "Unknown",
      }));

      res.json({ items, date: dateFilter });
    } catch (err) {
      console.error("ADMIN APPOINTMENTS HANDLER ERROR:", err);
      res.json({ items: [], date: dateFilter, error: true });
    }
  }
);

// ADMIN: CREATE APPOINTMENT (existing behavior preserved) ✅ + NEW event emit for agents
app.post(
  `${ADMIN_BASE}/appointments`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const { patientUid, doctorUid, date, time, type, status, operatoryId } = req.body || {};

    if (!patientUid || !doctorUid || !date || !time) {
      return res.status(400).json({ message: "patientUid, doctorUid, date, and time are required" });
    }

    try {
      // Resolve patient by UID
      const [rowsPatient] = await pool.query(
        `SELECT id FROM users WHERE uid = ? AND role = 'Patient' LIMIT 1`,
        [patientUid]
      );
      if (rowsPatient.length === 0) {
        return res.status(400).json({ message: `Patient not found for UID ${patientUid}` });
      }
      const patientId = rowsPatient[0].id;

      // Resolve doctor by UID
      const [rowsDoctor] = await pool.query(
        `SELECT id FROM users WHERE uid = ? AND role = 'Doctor' LIMIT 1`,
        [doctorUid]
      );
      if (rowsDoctor.length === 0) {
        return res.status(400).json({ message: `Doctor not found for UID ${doctorUid}` });
      }
      const doctorId = rowsDoctor[0].id;

      const dateStr = toDateStr(date); // YYYY-MM-DD
      const timeStr = toTimeStr(time); // HH:MM:SS

      // ✅ Predict duration + suggestSlots + hasConflict (guard require)
      let predictDurationMinutes = async () => 30;
      let suggestSlots = async () => [];
      let hasConflictFn = null;

      try {
        const appointmentAgent = require("./agents/appointmentAgent");
        if (typeof appointmentAgent.predictDurationMinutes === "function") {
          predictDurationMinutes = appointmentAgent.predictDurationMinutes;
        }
        if (typeof appointmentAgent.suggestSlots === "function") {
          suggestSlots = appointmentAgent.suggestSlots;
        }
        if (typeof appointmentAgent.hasConflict === "function") {
          hasConflictFn = appointmentAgent.hasConflict;
        }
      } catch (_) {
        // Node agents missing => keep defaults/fallback DB overlap check below
      }

      const durationMin = await predictDurationMinutes(pool, { type: type || "General", doctorId });

      // ✅ Conflict check (uses Node agent if available; fallback is DB-safe)
      let conflict = false;

      if (hasConflictFn) {
        conflict = await hasConflictFn(pool, {
          doctorId,
          operatoryId: operatoryId || null,
          dateStr,
          startTime: timeStr,
          durationMin,
        });
      } else {
        try {
          // Compute end time
          const [[trow]] = await pool.query(
            `SELECT TIME_FORMAT(ADDTIME(?, SEC_TO_TIME(?*60)), '%H:%i:%s') AS end_time`,
            [timeStr, durationMin]
          );
          const endTime = trow.end_time;

          // Prefer scheduled_end_time overlap logic when column exists
          if (schemaCaps.appointments_has_scheduled_end_time) {
            const [rowsConflict] = await pool.query(
              `
              SELECT 1
              FROM appointments
              WHERE doctor_id = ?
                AND scheduled_date = ?
                AND status NOT IN ('Cancelled')
                AND TIMESTAMP(scheduled_date, scheduled_time) < TIMESTAMP(?, ?)
                AND TIMESTAMP(scheduled_date, scheduled_end_time) > TIMESTAMP(?, ?)
              LIMIT 1
              `,
              [doctorId, dateStr, dateStr, endTime, dateStr, timeStr]
            );
            if (rowsConflict.length) conflict = true;

            if (!conflict && schemaCaps.appointments_has_operatory_id && operatoryId) {
              const [rowsOpConflict] = await pool.query(
                `
                SELECT 1
                FROM appointments
                WHERE operatory_id = ?
                  AND scheduled_date = ?
                  AND status NOT IN ('Cancelled')
                  AND TIMESTAMP(scheduled_date, scheduled_time) < TIMESTAMP(?, ?)
                  AND TIMESTAMP(scheduled_date, scheduled_end_time) > TIMESTAMP(?, ?)
                LIMIT 1
                `,
                [operatoryId, dateStr, dateStr, endTime, dateStr, timeStr]
              );
              if (rowsOpConflict.length) conflict = true;
            }
          } else {
            // Fallback if scheduled_end_time doesn't exist: approximate with predicted_duration_min
            const [rows] = await pool.query(
              `
              SELECT scheduled_time, predicted_duration_min
              FROM appointments
              WHERE doctor_id = ?
                AND scheduled_date = ?
                AND status NOT IN ('Cancelled')
              `,
              [doctorId, dateStr]
            );

            const toMin = (t) => {
              const [hh, mm] = String(t || "00:00:00").split(":").map((x) => parseInt(x, 10));
              return (hh || 0) * 60 + (mm || 0);
            };
            const sNew = toMin(timeStr);
            const eNew = sNew + durationMin;

            for (const r of rows) {
              const s = toMin(toTimeStr(r.scheduled_time));
              const d = Number(r.predicted_duration_min || 0) || 30;
              const e = s + d;
              if (sNew < e && eNew > s) {
                conflict = true;
                break;
              }
            }
          }
        } catch (e) {
          // If conflict query fails for any schema mismatch, don't block booking (but log)
          console.error("Conflict fallback check error:", e?.message || e);
          conflict = false;
        }
      }

      if (conflict) {
        let slots = [];
        try {
          slots = await suggestSlots(pool, {
            doctorId,
            operatoryId: operatoryId || null,
            dateStr,
            type: type || "General",
          });
        } catch (_) {
          slots = [];
        }

        if (!Array.isArray(slots) || slots.length === 0) {
          const fb = await fallbackSuggestSlots({
            doctorId,
            operatoryId: operatoryId || null,
            dateStr,
            durationMin,
          });
          slots = fb.map((x) => x.time);
        }

        return res.status(409).json({
          message: "Time slot conflict: doctor already has an overlapping appointment.",
          conflict: true,
          suggestedSlots: slots,
        });
      }

      // generate BOTH UID and CODE
      const appointmentUid = `APT-${Date.now()}`;
      const appointmentCode = `AC-${Date.now()}`;

      // INSERT (unchanged columns)
      const [result] = await pool.query(
        `
        INSERT INTO appointments
          (appointment_uid, appointment_code, patient_id, doctor_id, scheduled_date, scheduled_time, type, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [appointmentUid, appointmentCode, patientId, doctorId, dateStr, timeStr, type || "General", status || "Confirmed"]
      );

      const appointmentDbId = result.insertId;

      // ✅ Fill predicted duration + end time (same as before)
      try {
        await pool.query(
          `UPDATE appointments
           SET predicted_duration_min = ?, scheduled_end_time = ADDTIME(scheduled_time, SEC_TO_TIME(?*60))
           WHERE id = ?`,
          [durationMin, durationMin, appointmentDbId]
        );
      } catch (_) {}

      // ✅ Emit event for Python agents (DB outbox)
      await enqueueEventCompat({
        eventType: "AppointmentCreated",
        payload: {
          appointmentId: appointmentDbId,
          appointmentUid,
          patientId,
          doctorId,
          date: dateStr,
          time: timeStr,
          type: type || "General",
          operatoryId: operatoryId || null,
        },
        createdByUserId: req.user?.id || null,
      });

      return res.status(201).json({
        appointment: {
          id: appointmentUid,
          code: appointmentCode,
          dbId: appointmentDbId,
          date: dateStr,
          time: timeStr,
          type: type || "General",
          status: status || "Confirmed",
          patientUid,
          doctorUid,
        },
      });
    } catch (err) {
      console.error("ADMIN CREATE APPOINTMENT ERROR:", err);
      return res.status(500).json({ message: "Failed to create appointment. See server logs." });
    }
  }
);

// ✅ NEW: mark appointment completed (triggers Inventory + Revenue + Case timeline via event queue)
app.patch(
  `${ADMIN_BASE}/appointments/:dbId/complete`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const dbId = Number(req.params.dbId);
      const [rows] = await pool.query(`SELECT * FROM appointments WHERE id = ? LIMIT 1`, [dbId]);
      if (rows.length === 0) return res.status(404).json({ message: "Appointment not found" });

      await pool.query(
        `UPDATE appointments
         SET status = 'Completed',
             actual_end_at = COALESCE(actual_end_at, NOW())
         WHERE id = ?`,
        [dbId]
      );

      await enqueueEventCompat({
        eventType: "AppointmentCompleted",
        payload: {
          appointmentId: dbId,
          patientId: rows[0].patient_id,
          doctorId: rows[0].doctor_id,
          type: rows[0].type || "General",
          linkedCaseId: rows[0].linked_case_id || null,
        },
        createdByUserId: req.user?.id || null,
      });

      return res.json({ ok: true });
    } catch (err) {
      console.error("COMPLETE APPOINTMENT ERROR:", err);
      return res.json({ ok: false, error: true });
    }
  }
);

// ✅ NEW: agent test endpoints (kept; if Node hooks missing, still supports Python via enqueue)
app.post(
  `${ADMIN_BASE}/agents/run`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const { agent } = req.body || {};
      if (!agent) return res.status(400).json({ message: "agent required" });

      // Legacy Node direct-run hooks (optional)
      if (agent === "appointment" && runAppointmentAgentOnce) return res.json(await runAppointmentAgentOnce(pool));
      if (agent === "inventory" && runInventoryAgentOnce) return res.json(await runInventoryAgentOnce(pool));
      if (agent === "revenue" && runRevenueAgentOnce) return res.json(await runRevenueAgentOnce(pool));

      // ✅ Python agents: enqueue a generic "AgentRunRequested" event
      await enqueueEventCompat({
        eventType: "AgentRunRequested",
        payload: { agent: String(agent).toLowerCase() },
        createdByUserId: req.user?.id || null,
      });

      return res.json({ ok: true, queued: true, agent });
    } catch (err) {
      console.error("AGENT RUN ERROR:", err);
      return res.json({ error: true });
    }
  }
);

app.post(
  `${ADMIN_BASE}/agents/retry-failed-events`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const n = await retryFailedCompat(100);
      return res.json({ retried: n });
    } catch (err) {
      console.error("RETRY FAILED EVENTS ERROR:", err);
      return res.json({ retried: 0, error: true });
    }
  }
);

// SIMPLE CASES LIST (UNCHANGED)
app.get(
  `${ADMIN_BASE}/cases`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const rows = await safeQuery(
        "ADMIN CASES SIMPLE",
        `SELECT
           c.case_uid,
           c.case_type,
           c.stage,
           p.full_name AS patient_name,
           d.full_name AS doctor_name
         FROM cases c
         LEFT JOIN users p ON p.id = c.patient_id
         LEFT JOIN users d ON d.id = c.doctor_id
         ORDER BY c.created_at DESC`,
        []
      );

      const items = rows.map((r) => ({
        id: r.case_uid,
        patient: r.patient_name || "—",
        doctor: r.doctor_name || "—",
        type: r.case_type || "General case",
        stage: r.stage || "NEW",
      }));

      res.json({ items });
    } catch (err) {
      console.error("ADMIN CASES HANDLER ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// ✅ NEW: export case PDF (kept; if Node hook missing -> 501)
app.get(
  `${ADMIN_BASE}/cases/:caseDbId/export/pdf`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      if (!exportCasePdf) return res.status(501).json({ message: "PDF export not enabled" });

      const caseId = Number(req.params.caseDbId);

      const outDir = path.join(__dirname, "exports");
      fs.mkdirSync(outDir, { recursive: true });

      const outPath = path.join(outDir, `case-${caseId}.pdf`);
      await exportCasePdf(pool, { caseId, outPath });

      return res.download(outPath);
    } catch (err) {
      console.error("CASE PDF EXPORT ERROR:", err);
      return res.status(500).json({ message: "Failed to export PDF" });
    }
  }
);

// ✅ NEW: CREATE INVENTORY ITEM (for AdminInventory "+ New item")
app.post(
  `${ADMIN_BASE}/inventory`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const { itemCode, name, category, stock, reorderThreshold, expiryDate } = req.body || {};

      if (!itemCode || !name) {
        return res.status(400).json({ message: "itemCode and name are required" });
      }

      const code = String(itemCode).trim();
      const nm = String(name).trim();
      const cat = String(category || "Uncategorized").trim();

      const stockNum = Number(stock);
      const rtNum = Number(reorderThreshold);

      if (!Number.isFinite(stockNum) || stockNum < 0) return res.status(400).json({ message: "stock must be 0 or greater" });
      if (!Number.isFinite(rtNum) || rtNum < 0) return res.status(400).json({ message: "reorderThreshold must be 0 or greater" });

      let status = "Healthy";
      if (stockNum <= rtNum) status = "Low";
      else if (stockNum <= Math.ceil(rtNum * 1.5)) status = "Reorder soon";

      const exp = expiryDate ? toDateStr(expiryDate) : null;

      await pool.query(
        `
        INSERT INTO inventory_items
          (item_code, name, category, stock, status, reorder_threshold, expiry_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        [code, nm, cat, Math.floor(stockNum), status, Math.floor(rtNum), exp]
      );

      return res.status(201).json({
        message: "Item created",
        item: {
          id: code,
          name: nm,
          category: cat,
          stock: Math.floor(stockNum),
          status,
          reorderThreshold: Math.floor(rtNum),
          expiryDate: exp,
        },
      });
    } catch (err) {
      console.error("ADMIN CREATE INVENTORY ERROR:", err);
      if (err?.code === "ER_DUP_ENTRY") return res.status(409).json({ message: "Item code already exists" });
      return res.status(500).json({ message: "Failed to create inventory item" });
    }
  }
);

// INVENTORY (UPDATED: include expiry_date)
app.get(
  `${ADMIN_BASE}/inventory`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const rows = await safeQuery(
        "ADMIN INVENTORY",
        `SELECT item_code, name, category, stock, status, reorder_threshold, expiry_date
         FROM inventory_items
         ORDER BY name ASC`,
        []
      );

      const items = rows.map((r) => ({
        id: r.item_code,
        name: r.name,
        category: r.category,
        stock: Number(r.stock || 0),
        status: r.status,
        reorderThreshold: r.reorder_threshold,
        expiryDate: r.expiry_date || null,
      }));

      res.json({ items });
    } catch (err) {
      console.error("ADMIN INVENTORY HANDLER ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// PATIENTS (UNCHANGED behavior; fixed dateStrings comparison safely)
app.get(
  `${ADMIN_BASE}/patients`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const rows = await safeQuery(
        "ADMIN PATIENTS",
        `SELECT
           u.id,
           u.uid,
           u.full_name,
           u.phone,
           MAX(a.scheduled_date) AS last_visit
         FROM users u
         LEFT JOIN appointments a ON a.patient_id = u.id
         WHERE u.role = 'Patient'
         GROUP BY u.id, u.uid, u.full_name, u.phone
         ORDER BY u.full_name ASC`,
        []
      );

      const todayStr = formatDateYYYYMMDD(new Date());
      const sixMonthsAgo = new Date();
      sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
      const sixMonthsAgoStr = formatDateYYYYMMDD(sixMonthsAgo);

      const items = rows.map((r) => {
        const lastVisitStr = r.last_visit ? toDateStr(r.last_visit) : null;
        const isActive = lastVisitStr && lastVisitStr >= sixMonthsAgoStr ? "Active" : "Inactive";

        return {
          id: r.uid,
          name: r.full_name,
          phone: r.phone,
          lastVisit: lastVisitStr || null,
          status: isActive,
        };
      });

      res.json({ items, asOf: todayStr });
    } catch (err) {
      console.error("ADMIN PATIENTS HANDLER ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// DOCTORS (UNCHANGED)
app.get(
  `${ADMIN_BASE}/doctors`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const rows = await safeQuery(
        "ADMIN DOCTORS",
        `SELECT
           u.id,
           u.uid,
           u.full_name,
           u.phone
         FROM users u
         WHERE u.role = 'Doctor'
         ORDER BY u.full_name ASC`,
        []
      );

      const items = rows.map((r) => ({
        id: r.uid,
        name: r.full_name,
        phone: r.phone || null,
      }));

      res.json({ items });
    } catch (err) {
      console.error("ADMIN DOCTORS HANDLER ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// REVENUE DASHBOARD (UNCHANGED)
app.get(
  `${ADMIN_BASE}/revenue-dashboard`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    try {
      const today = new Date();
      const year = today.getFullYear();
      const monthIndex = today.getMonth();
      const monthStart = new Date(year, monthIndex, 1);
      const nextMonthStart = new Date(year, monthIndex + 1, 1);
      const monthStartStr = formatDateYYYYMMDD(monthStart);
      const nextMonthStartStr = formatDateYYYYMMDD(nextMonthStart);

      const rowsCurrent = await safeQuery(
        "ADMIN REVENUE CURRENT",
        `SELECT
           COALESCE(SUM(CASE WHEN status = 'Paid' THEN amount ELSE 0 END), 0) AS total_paid,
           COALESCE(SUM(CASE WHEN status IN ('Pending','Overdue') THEN amount ELSE 0 END), 0) AS total_pending
         FROM invoices
         WHERE issue_date >= ? AND issue_date < ?`,
        [monthStartStr, nextMonthStartStr]
      );
      const thisMonthTotal = Number(rowsCurrent[0]?.total_paid || 0);
      const pendingOverdue = Number(rowsCurrent[0]?.total_pending || 0);

      const prevMonthStart = new Date(year, monthIndex - 1, 1);
      const currentMonthStart = monthStart;
      const prevMonthStartStr = formatDateYYYYMMDD(prevMonthStart);
      const currentMonthStartStr = formatDateYYYYMMDD(currentMonthStart);

      const rowsPrev = await safeQuery(
        "ADMIN REVENUE PREV",
        `SELECT
           COALESCE(SUM(CASE WHEN status = 'Paid' THEN amount ELSE 0 END), 0) AS total_paid
         FROM invoices
         WHERE issue_date >= ? AND issue_date < ?`,
        [prevMonthStartStr, currentMonthStartStr]
      );
      const prevMonthTotal = Number(rowsPrev[0]?.total_paid || 0);

      let growthPercent = null;
      if (prevMonthTotal > 0) {
        growthPercent = ((thisMonthTotal - prevMonthTotal) / prevMonthTotal) * 100;
      }

      const daysElapsed = today.getDate();
      const avgPerDay = daysElapsed > 0 ? thisMonthTotal / daysElapsed : thisMonthTotal;

      const sixMonthsAgo = new Date(year, monthIndex - 5, 1);
      const sixMonthsAgoStr = formatDateYYYYMMDD(sixMonthsAgo);

      const rowsSeries = await safeQuery(
        "ADMIN REVENUE SERIES",
        `SELECT
           DATE_FORMAT(issue_date, '%Y-%m') AS ym,
           COALESCE(SUM(CASE WHEN status = 'Paid' THEN amount ELSE 0 END), 0) AS total_paid
         FROM invoices
         WHERE issue_date >= ?
         GROUP BY ym
         ORDER BY ym ASC`,
        [sixMonthsAgoStr]
      );

      const last6Months = rowsSeries.map((r) => ({
        label: r.ym,
        value: Number(r.total_paid || 0),
      }));

      res.json({
        thisMonthTotal,
        pendingOverdue,
        avgPerDay,
        growthPercent,
        last6Months,
      });
    } catch (err) {
      console.error("ADMIN REVENUE DASHBOARD HANDLER ERROR:", err);
      res.json({
        thisMonthTotal: 0,
        pendingOverdue: 0,
        avgPerDay: 0,
        growthPercent: null,
        last6Months: [],
        error: true,
      });
    }
  }
);

// CASE TRACKING SUMMARY (UNCHANGED)
app.get(
  `${ADMIN_BASE}/cases/tracking-summary`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const todayStr = formatDateYYYYMMDD(new Date());

    try {
      const rowsTotal = await safeQuery("CASE TRACKING TOTAL", `SELECT COUNT(*) AS cnt FROM cases`, []);
      const totalCases = rowsTotal[0]?.cnt ?? 0;

      const rowsRisk = await safeQuery(
        "CASE TRACKING RISK",
        `SELECT
           SUM(CASE WHEN risk_score >= 80 THEN 1 ELSE 0 END) AS highRiskCount,
           SUM(
             CASE
               WHEN next_review_date IS NOT NULL
                AND next_review_date <= CURDATE()
                AND stage <> 'CLOSED'
               THEN 1 ELSE 0
             END
           ) AS needsFollowUpCount
         FROM cases`,
        []
      );
      const highRiskCount = rowsRisk[0]?.highRiskCount ?? 0;
      const needsFollowUpCount = rowsRisk[0]?.needsFollowUpCount ?? 0;

      const rowsStage = await safeQuery(
        "CASE TRACKING STAGES",
        `SELECT stage, COUNT(*) AS cnt
         FROM cases
         GROUP BY stage`,
        []
      );
      const byStage = {};
      for (const r of rowsStage) {
        if (!r.stage) continue;
        const key = String(r.stage).toUpperCase();
        byStage[key] = r.cnt;
      }

      res.json({
        totalCases,
        highRiskCount,
        needsFollowUpCount,
        byStage,
        updatedAt: todayStr,
      });
    } catch (err) {
      console.error("CASE TRACKING SUMMARY HANDLER ERROR:", err);
      res.json({
        totalCases: 0,
        highRiskCount: 0,
        needsFollowUpCount: 0,
        byStage: {},
        updatedAt: todayStr,
        error: true,
      });
    }
  }
);

// CASE TRACKING LIST (UNCHANGED)
app.get(
  `${ADMIN_BASE}/cases/tracking-list`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const limit =
      typeof req.query.limit === "string" && req.query.limit.trim()
        ? parseInt(req.query.limit, 10) || 50
        : 50;

    try {
      const rows = await safeQuery(
        "CASE TRACKING LIST",
        `SELECT
           c.id,
           c.case_uid AS caseId,
           c.case_type AS type,
           c.stage,
           c.priority,
           c.risk_score,
           c.next_action,
           c.next_review_date,
           c.updated_at,
           c.agent_summary,
           c.agent_recommendation,
           p.full_name AS patient_name,
           p.uid AS patient_uid,
           d.full_name AS doctor_name,
           d.uid AS doctor_uid
         FROM cases c
         LEFT JOIN users p ON p.id = c.patient_id
         LEFT JOIN users d ON d.id = c.doctor_id
         ORDER BY c.updated_at DESC
         LIMIT ?`,
        [limit]
      );

      const cases = rows.map((r) => ({
        id: r.id,
        caseId: r.caseId || `CASE-${r.id}`,
        type: r.type || "General case",
        stage: (r.stage && String(r.stage).toUpperCase()) || "NEW",
        priority: (r.priority && String(r.priority).toUpperCase()) || "MEDIUM",
        riskScore: Number(r.risk_score ?? 0),
        nextAction: r.next_action || null,
        nextReviewDate: r.next_review_date || null,
        lastUpdated: r.updated_at || new Date().toISOString(),
        agentSummary: r.agent_summary || null,
        agentRecommendation: r.agent_recommendation || null,
        patientName: r.patient_name || "Unknown patient",
        patientUid: r.patient_uid || null,
        doctorName: r.doctor_name || "Unassigned",
        doctorUid: r.doctor_uid || null,
        flagged: Number(r.risk_score ?? 0) >= 80,
      }));

      res.json({ cases });
    } catch (err) {
      console.error("CASE TRACKING LIST HANDLER ERROR:", err);
      res.json({ cases: [], error: true });
    }
  }
);

// UPDATE CASE STAGE (kept) ✅ + NEW emit CaseUpdated event for Python case agent
app.patch(
  `${ADMIN_BASE}/cases/:id`,
  authMiddleware,
  requireRole("Admin"),
  async (req, res) => {
    const caseId = Number(req.params.id);
    const { stage } = req.body || {};

    if (!stage) {
      return res.status(400).json({ message: "Stage is required" });
    }

    try {
      await pool.query("UPDATE cases SET stage = ?, updated_at = NOW() WHERE id = ?", [stage, caseId]);

      await enqueueEventCompat({
        eventType: "CaseUpdated",
        payload: { caseDbId: caseId, stage },
        createdByUserId: req.user?.id || null,
      });

      const [rows] = await pool.query("SELECT updated_at FROM cases WHERE id = ?", [caseId]);
      const row = rows[0];

      res.json({
        case: {
          id: caseId,
          stage,
          lastUpdated: row?.updated_at || new Date().toISOString(),
        },
      });
    } catch (err) {
      console.error("CASE UPDATE HANDLER ERROR:", err);
      res.json({
        case: {
          id: caseId,
          stage,
          lastUpdated: new Date().toISOString(),
        },
        error: true,
        message: "Failed to update case stage in database",
      });
    }
  }
);

// Example secured endpoint (UNCHANGED)
app.get(`${ADMIN_BASE}/secure-kpis`, authMiddleware, requireRole("Admin"), (req, res) => {
  res.json({
    message: "Some secure admin-only data",
    user: req.user,
  });
});

// ===================================
// DOCTOR ROUTES
// ===================================
const DOCTOR_BASE = "/api/doctor";

// Helper to convert DB stage → label
function mapStageDbToLabel(stageDb) {
  const s = String(stageDb || "").toUpperCase();
  if (s === "IN_TREATMENT") return "In treatment";
  if (s === "WAITING_ON_PATIENT") return "Waiting on patient";
  if (s === "CLOSED" || s === "COMPLETED") return "Completed";
  return "New";
}

// DOCTOR: dashboard summary (UNCHANGED)
app.get(
  `${DOCTOR_BASE}/dashboard-summary`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    const doctorId = req.user.id;
    const todayStr = formatDateYYYYMMDD(new Date());

    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const thirtyDaysAgoStr = formatDateYYYYMMDD(thirtyDaysAgo);

    try {
      const rowsToday = await safeQuery(
        "DOCTOR DASHBOARD TODAY APPOINTMENTS",
        `SELECT COUNT(*) AS cnt
         FROM appointments
         WHERE doctor_id = ?
           AND scheduled_date = ?
           AND (status IS NULL OR status <> 'Cancelled')`,
        [doctorId, todayStr]
      );
      const todayAppointments = rowsToday[0]?.cnt ?? 0;

      const rowsOpenCases = await safeQuery(
        "DOCTOR DASHBOARD OPEN CASES",
        `SELECT COUNT(*) AS cnt
         FROM cases
         WHERE doctor_id = ?
           AND (stage IS NULL OR stage <> 'CLOSED')`,
        [doctorId]
      );
      const openCases = rowsOpenCases[0]?.cnt ?? 0;

      const rowsNewPatients = await safeQuery(
        "DOCTOR DASHBOARD NEW PATIENTS 30D",
        `SELECT COUNT(DISTINCT a.patient_id) AS cnt
         FROM appointments a
         JOIN users u ON u.id = a.patient_id
         WHERE a.doctor_id = ?
           AND a.scheduled_date >= ?
           AND u.role = 'Patient'`,
        [doctorId, thirtyDaysAgoStr]
      );
      const newPatients30d = rowsNewPatients[0]?.cnt ?? 0;

      const rowsCompletion = await safeQuery(
        "DOCTOR DASHBOARD COMPLETION 30D",
        `SELECT
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed
         FROM appointments
         WHERE doctor_id = ?
           AND scheduled_date >= ?`,
        [doctorId, thirtyDaysAgoStr]
      );
      const total = rowsCompletion[0]?.total ?? 0;
      const completed = rowsCompletion[0]?.completed ?? 0;
      const completionRate = total > 0 ? Math.round((completed * 100) / total) : 0;

      res.json({
        todayAppointments,
        openCases,
        newPatients30d,
        completionRate,
        asOf: todayStr,
      });
    } catch (err) {
      console.error("DOCTOR DASHBOARD SUMMARY ERROR:", err);
      res.json({
        todayAppointments: 0,
        openCases: 0,
        newPatients30d: 0,
        completionRate: 0,
        asOf: todayStr,
        error: true,
      });
    }
  }
);

// DOCTOR: today’s appointments ✅ UPDATED (dbId + status mapping aligned)
app.get(
  `${DOCTOR_BASE}/appointments`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    const doctorId = req.user.id;
    const todayStr = formatDateYYYYMMDD(new Date());

    try {
      const rows = await safeQuery(
        "DOCTOR APPOINTMENTS",
        `SELECT
           a.id,
           a.appointment_uid,
           DATE_FORMAT(a.scheduled_date, '%Y-%m-%d') AS scheduled_date,
           TIME_FORMAT(a.scheduled_time, '%H:%i') AS time_display,
           a.type,
           a.status,
           p.full_name AS patient_name
         FROM appointments a
         LEFT JOIN users p ON p.id = a.patient_id
         WHERE a.doctor_id = ?
           AND a.scheduled_date = ?
         ORDER BY a.scheduled_time ASC`,
        [doctorId, todayStr]
      );

      const items = rows.map((r) => {
        const statusRaw = String(r.status || "").toUpperCase();

        let statusLabel = "Pending";
        if (statusRaw === "CONFIRMED") statusLabel = "Confirmed";
        else if (statusRaw === "CHECKED IN") statusLabel = "Checked in";
        else if (statusRaw === "IN PROGRESS") statusLabel = "In progress";
        else if (statusRaw === "COMPLETED") statusLabel = "Completed";
        else if (statusRaw === "CANCELLED") statusLabel = "Cancelled";

        return {
          dbId: Number(r.id),
          id: r.appointment_uid || String(r.id),
          date: r.scheduled_date || null,
          time: r.time_display || null,
          patient: r.patient_name || "—",
          reason: r.type || "General visit",
          room: "—",
          status: statusLabel,
        };
      });

      res.json({ items, date: todayStr });
    } catch (err) {
      console.error("DOCTOR APPOINTMENTS ERROR:", err);
      res.status(500).json({
        message: "Failed to load appointments",
        items: [],
        date: todayStr,
      });
    }
  }
);

// ✅ NEW: DOCTOR mark appointment completed
app.patch(
  `${DOCTOR_BASE}/appointments/:dbId/complete`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    try {
      const dbId = Number(req.params.dbId);
      const doctorId = req.user.id;

      const [rows] = await pool.query(
        `SELECT id, patient_id, doctor_id, type, linked_case_id
         FROM appointments
         WHERE id = ? AND doctor_id = ?
         LIMIT 1`,
        [dbId, doctorId]
      );

      if (rows.length === 0) {
        return res.status(404).json({ message: "Appointment not found" });
      }

      await pool.query(
        `UPDATE appointments
         SET status = 'Completed',
             actual_end_at = COALESCE(actual_end_at, NOW())
         WHERE id = ?`,
        [dbId]
      );

      await enqueueEventCompat({
        eventType: "AppointmentCompleted",
        payload: {
          appointmentId: dbId,
          patientId: rows[0].patient_id,
          doctorId: rows[0].doctor_id,
          type: rows[0].type || "General",
          linkedCaseId: rows[0].linked_case_id || null,
        },
        createdByUserId: req.user?.id || null,
      });

      return res.json({ ok: true });
    } catch (err) {
      console.error("DOCTOR COMPLETE APPOINTMENT ERROR:", err);
      return res.status(500).json({ ok: false, message: "Server error" });
    }
  }
);

// DOCTOR: patients (UNCHANGED)
app.get(
  `${DOCTOR_BASE}/patients`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    const doctorId = req.user.id;

    try {
      const rows = await safeQuery(
        "DOCTOR PATIENTS",
        `SELECT
           u.id,
           u.uid,
           u.full_name,
           MAX(a.scheduled_date) AS last_visit,
           COUNT(DISTINCT c.id) AS active_cases
         FROM users u
         JOIN appointments a ON a.patient_id = u.id
         LEFT JOIN cases c 
           ON c.patient_id = u.id
          AND c.doctor_id = ?
          AND c.stage <> 'CLOSED'
         WHERE u.role = 'Patient'
           AND a.doctor_id = ?
         GROUP BY u.id, u.uid, u.full_name
         ORDER BY u.full_name ASC`,
        [doctorId, doctorId]
      );

      const items = rows.map((r) => ({
        id: r.uid,
        name: r.full_name,
        lastVisit: r.last_visit ? toDateStr(r.last_visit) : null,
        activeCases: Number(r.active_cases || 0),
      }));

      res.json({ items });
    } catch (err) {
      console.error("DOCTOR PATIENTS ERROR:", err);
      res.status(500).json({
        message: "Failed to load patients",
        items: [],
      });
    }
  }
);

// DOCTOR: cases (UNCHANGED)
app.get(
  `${DOCTOR_BASE}/cases`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    const doctorId = req.user.id;

    try {
      const rows = await safeQuery(
        "DOCTOR CASES",
        `SELECT
           c.id,
           c.case_uid,
           c.case_type,
           c.stage,
           c.created_at,
           c.updated_at,
           p.full_name AS patient_name
         FROM cases c
         LEFT JOIN users p ON p.id = c.patient_id
         WHERE c.doctor_id = ?
         ORDER BY c.updated_at DESC`,
        [doctorId]
      );

      const cases = rows.map((r) => ({
        id: r.case_uid || `CASE-${r.id}`,
        patientName: r.patient_name || "Unknown patient",
        toothRegion: "Not specified",
        diagnosis: r.case_type || "General case",
        stage: mapStageDbToLabel(r.stage),
        createdAt: r.created_at || null,
        updatedAt: r.updated_at || null,
      }));

      res.json({ cases });
    } catch (err) {
      console.error("DOCTOR CASES ERROR:", err);
      res.status(500).json({
        message: "Failed to load cases",
        cases: [],
      });
    }
  }
);

// DOCTOR: create new case (kept) ✅ + NEW CaseUpdated event
app.post(
  `${DOCTOR_BASE}/cases`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    const doctorId = req.user.id;
    const { patientName, toothRegion, diagnosis, stage } = req.body || {};

    if (!patientName || !diagnosis) {
      return res.status(400).json({ message: "Patient name and diagnosis are required" });
    }

    try {
      const nameTrimmed = String(patientName).trim();
      let patientId = null;

      const [existing] = await pool.query(
        `SELECT id FROM users WHERE full_name = ? AND role = 'Patient' LIMIT 1`,
        [nameTrimmed]
      );

      if (existing.length > 0) {
        patientId = existing[0].id;
      } else {
        const newUid = generateUid("Patient");
        const [insertPatient] = await pool.query(
          `INSERT INTO users (uid, full_name, role, created_at)
           VALUES (?, ?, 'Patient', NOW())`,
          [newUid, nameTrimmed]
        );
        patientId = insertPatient.insertId;
      }

      const stageDb = String(stage || "NEW").toUpperCase();
      const caseUid = `CASE-${Date.now()}`;

      const caseType =
        diagnosis +
        (toothRegion && String(toothRegion).trim() ? ` – ${String(toothRegion).trim()}` : "");

      const [result] = await pool.query(
        `INSERT INTO cases
          (case_uid, patient_id, doctor_id, case_type, stage, priority, risk_score, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, 'MEDIUM', 0, NOW(), NOW())`,
        [caseUid, patientId, doctorId, caseType, stageDb]
      );

      const newId = result.insertId;

      await enqueueEventCompat({
        eventType: "CaseUpdated",
        payload: { caseDbId: newId, stage: stageDb, nextAction: "Case created" },
        createdByUserId: req.user?.id || null,
      });

      const [rows] = await pool.query(`SELECT created_at, updated_at FROM cases WHERE id = ?`, [newId]);
      const row = rows[0] || {};

      res.status(201).json({
        case: {
          id: caseUid,
          caseId: caseUid,
          patientName: nameTrimmed,
          toothRegion: toothRegion || "Not specified",
          diagnosis,
          stage: mapStageDbToLabel(stageDb),
          createdAt: row.created_at || new Date().toISOString(),
          updatedAt: row.updated_at || new Date().toISOString(),
        },
      });
    } catch (err) {
      console.error("DOCTOR CREATE CASE ERROR:", err);
      return res.status(500).json({ message: "Failed to create case. Please try again." });
    }
  }
);

// ✅ NEW: case attachments upload
const uploadDir = path.join(__dirname, "uploads");
fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

app.use("/uploads", express.static(uploadDir));

app.post(
  `${DOCTOR_BASE}/cases/:caseDbId/attachments`,
  authMiddleware,
  requireRole("Doctor"),
  upload.single("file"),
  async (req, res) => {
    try {
      const caseId = Number(req.params.caseDbId);
      if (!req.file) return res.status(400).json({ message: "file required" });

      const safeOriginal = String(req.file.originalname || "file").replace(/[/\\]/g, "_");
      const destPath = path.join(uploadDir, `${Date.now()}-${safeOriginal}`);
      fs.renameSync(req.file.path, destPath);

      await pool.query(
        `INSERT INTO case_attachments (case_id, file_name, file_path, mime_type, uploaded_by_user_id)
         VALUES (?, ?, ?, ?, ?)`,
        [caseId, safeOriginal, destPath, req.file.mimetype, req.user.id]
      );

      await enqueueEventCompat({
        eventType: "CaseUpdated",
        payload: { caseDbId: caseId, nextAction: "Attachment uploaded" },
        createdByUserId: req.user?.id || null,
      });

      return res.json({ ok: true });
    } catch (err) {
      console.error("CASE ATTACHMENT UPLOAD ERROR:", err);
      return res.status(500).json({ message: "Upload failed" });
    }
  }
);

// ========================================================================================
// DOCTOR: request AI summary for a case
//
// Doctors can trigger an AI-generated case summary via this endpoint.  The
// payload may include an optional array of visit database IDs to scope the
// summary; if omitted the agent will consider all visits for the case.  The
// request enqueues a CaseGenerateSummary event that will be handled by the
// Python CaseTrackingAgent and does not block the HTTP response.
app.post(
  `${DOCTOR_BASE}/cases/:caseDbId/summary`,
  authMiddleware,
  requireRole("Doctor"),
  async (req, res) => {
    try {
      const caseId = Number(req.params.caseDbId);
      if (!caseId) return res.status(400).json({ message: "Invalid case ID" });
      const { visitIds } = req.body || {};
      let visitList = [];
      if (Array.isArray(visitIds)) {
        visitList = visitIds
          .map((v) => {
            const n = Number(v);
            return Number.isFinite(n) && n > 0 ? n : null;
          })
          .filter((v) => v !== null);
      }
      await enqueueEventCompat({
        eventType: "CaseGenerateSummary",
        payload: { caseId, visitIds: visitList },
        createdByUserId: req.user?.id || null,
      });
      return res.json({ queued: true });
    } catch (err) {
      console.error("CASE SUMMARY REQUEST ERROR:", err);
      return res.status(500).json({ message: "Failed to request summary" });
    }
  }
);

// ===================================
// PATIENT ROUTES
// ===================================
const PATIENT_BASE = "/api/patient";

// PATIENT: dashboard (UNCHANGED)
app.get(
  `${PATIENT_BASE}/dashboard`,
  authMiddleware,
  requireRole("Patient"),
  async (req, res) => {
    const patientId = req.user.id;
    const todayStr = formatDateYYYYMMDD(new Date());

    try {
      const rowsAppt = await safeQuery(
        "PATIENT DASHBOARD APPTS",
        `SELECT
           a.id,
           a.appointment_uid,
           DATE_FORMAT(a.scheduled_date, '%Y-%m-%d') AS scheduled_date,
           TIME_FORMAT(a.scheduled_time, '%h:%i %p') AS time_display,
           a.type,
           a.status,
           d.full_name AS doctor_name
         FROM appointments a
         LEFT JOIN users d ON d.id = a.doctor_id
         WHERE a.patient_id = ?
           AND a.scheduled_date >= ?
         ORDER BY a.scheduled_date, a.scheduled_time
         LIMIT 5`,
        [patientId, todayStr]
      );

      const upcomingAppointments = rowsAppt.map((r) => {
        const statusRaw = String(r.status || "").toUpperCase();
        let statusLabel = "Pending";
        if (statusRaw === "CONFIRMED") statusLabel = "Confirmed";
        else if (statusRaw === "COMPLETED") statusLabel = "Completed";
        else if (statusRaw === "CANCELLED") statusLabel = "Cancelled";

        return {
          id: r.appointment_uid || r.id,
          date: r.scheduled_date || null,
          time: r.time_display || null,
          doctorName: r.doctor_name || "Clinic doctor",
          reason: r.type || "Dental visit",
          status: statusLabel,
          location: "Main clinic",
        };
      });

      const rowsCases = await safeQuery(
        "PATIENT DASHBOARD CASES",
        `SELECT
           case_uid,
           case_type,
           stage,
           updated_at,
           agent_summary
         FROM cases
         WHERE patient_id = ?
         ORDER BY updated_at DESC
         LIMIT 5`,
        [patientId]
      );

      const treatmentSummaries = rowsCases.map((r) => ({
        id: r.case_uid,
        title: r.case_type || "Dental case",
        lastUpdated: r.updated_at || null,
        stage: mapStageDbToLabel(r.stage),
        snippet:
          r.agent_summary ||
          "Summary not yet available. Your dentist may still be preparing this note.",
      }));

      const rowsInvoices = await safeQuery(
        "PATIENT DASHBOARD INVOICES",
        `SELECT
           id,
           issue_date,
           amount,
           status
         FROM invoices
         WHERE patient_id = ?
         ORDER BY issue_date DESC
         LIMIT 5`,
        [patientId]
      );

      const payments = rowsInvoices.map((r) => ({
        id: r.id,
        date: r.issue_date || null,
        description: `Invoice #${r.id}`,
        amount: Number(r.amount || 0),
        currency: "INR",
        status: r.status || "Pending",
      }));

      res.json({
        upcomingAppointments,
        treatmentSummaries,
        payments,
      });
    } catch (err) {
      console.error("PATIENT DASHBOARD ERROR:", err);
      res.json({
        upcomingAppointments: [],
        treatmentSummaries: [],
        payments: [],
        error: true,
      });
    }
  }
);

// PATIENT: appointments (UNCHANGED)
app.get(
  `${PATIENT_BASE}/appointments`,
  authMiddleware,
  requireRole("Patient"),
  async (req, res) => {
    const patientId = req.user.id;

    try {
      const rows = await safeQuery(
        "PATIENT APPOINTMENTS LIST",
        `SELECT
           a.id,
           a.appointment_uid,
           DATE_FORMAT(a.scheduled_date, '%Y-%m-%d') AS scheduled_date,
           TIME_FORMAT(a.scheduled_time, '%h:%i %p') AS time_display,
           a.type,
           a.status,
           d.full_name AS doctor_name
         FROM appointments a
         LEFT JOIN users d ON d.id = a.doctor_id
         WHERE a.patient_id = ?
         ORDER BY a.scheduled_date DESC, a.scheduled_time DESC`,
        [patientId]
      );

      const items = rows.map((r) => {
        const statusRaw = String(r.status || "").toUpperCase();
        let statusLabel = "Pending";
        if (statusRaw === "CONFIRMED") statusLabel = "Confirmed";
        else if (statusRaw === "COMPLETED") statusLabel = "Completed";
        else if (statusRaw === "CANCELLED") statusLabel = "Cancelled";

        return {
          id: r.appointment_uid || r.id,
          date: r.scheduled_date || null,
          time: r.time_display || null,
          doctor: r.doctor_name || "Clinic doctor",
          reason: r.type || "Dental visit",
          status: statusLabel,
          location: "Main clinic",
          notes: null,
        };
      });

      res.json({ items });
    } catch (err) {
      console.error("PATIENT APPOINTMENTS ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// PATIENT: treatments (UNCHANGED)
app.get(
  `${PATIENT_BASE}/treatments`,
  authMiddleware,
  requireRole("Patient"),
  async (req, res) => {
    const patientId = req.user.id;

    try {
      const rows = await safeQuery(
        "PATIENT TREATMENTS",
        `SELECT
           case_uid,
           case_type,
           stage,
           updated_at,
           agent_summary,
           agent_recommendation
         FROM cases
         WHERE patient_id = ?
         ORDER BY updated_at DESC`,
        [patientId]
      );

      const items = rows.map((r) => ({
        id: r.case_uid,
        title: r.case_type || "Dental case",
        lastUpdated: r.updated_at || null,
        stage: mapStageDbToLabel(r.stage),
        summary:
          r.agent_summary ||
          "Summary not yet generated. Your dentist may still be preparing this.",
        details: r.agent_recommendation || null,
      }));

      res.json({ items });
    } catch (err) {
      console.error("PATIENT TREATMENTS ERROR:", err);
      res.json({ items: [], error: true });
    }
  }
);

// PATIENT: billing (UNCHANGED)
app.get(
  `${PATIENT_BASE}/billing`,
  authMiddleware,
  requireRole("Patient"),
  async (req, res) => {
    const patientId = req.user.id;
    const todayStr = formatDateYYYYMMDD(new Date());

    try {
      const rows = await safeQuery(
        "PATIENT BILLING",
        `SELECT
           id,
           issue_date,
           amount,
           status,
           paid_date
         FROM invoices
         WHERE patient_id = ?
         ORDER BY issue_date DESC, id DESC`,
        [patientId]
      );

      let totalDue = 0;

      const invoices = rows.map((r) => {
        const amountNum = Number(r.amount || 0);
        const statusRaw = String(r.status || "").toUpperCase();

        let statusLabel = "Pending";
        let isPaid = false;

        if (statusRaw === "PAID") {
          statusLabel = "Paid";
          isPaid = true;
        } else if (statusRaw === "OVERDUE") {
          statusLabel = "Overdue";
        }

        if (!isPaid) totalDue += amountNum;

        return {
          id: r.id,
          invoiceNumber: `INV-${r.id}`,
          date: r.issue_date || null,
          amount: amountNum,
          currency: "INR",
          status: statusLabel,
          paidDate: r.paid_date || null,
        };
      });

      res.json({
        summary: {
          totalDue,
          currency: "INR",
          lastUpdated: todayStr,
          invoiceCount: invoices.length,
        },
        invoices,
      });
    } catch (err) {
      console.error("PATIENT BILLING ERROR:", err);
      res.json({
        summary: {
          totalDue: 0,
          currency: "INR",
          lastUpdated: todayStr,
          invoiceCount: 0,
        },
        invoices: [],
        error: true,
      });
    }
  }
);

// ===================================
// DEBUG ROUTE – see which routes exist (UNCHANGED)
// ===================================
app.get("/debug/routes", (req, res) => {
  const routes = [];

  app._router.stack.forEach((layer) => {
    if (layer.route && layer.route.path) {
      const methods = Object.keys(layer.route.methods)
        .filter((m) => layer.route.methods[m])
        .map((m) => m.toUpperCase());
      routes.push({ methods, path: layer.route.path });
    }
  });

  res.json({
    hasCaseSummary: routes.some((r) => r.path === "/api/admin/cases/tracking-summary" && r.methods.includes("GET")),
    routes,
  });
});

// ===================================
// START SERVER
// ===================================
const port = process.env.PORT || 4000;
app.listen(port, () => {
  console.log(`Auth + Admin + Doctor + Patient server running on http://localhost:${port}`);
});
