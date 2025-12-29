
SET SQL_MODE = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

CREATE DATABASE IF NOT EXISTS dental_clinic
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE dental_clinic;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid VARCHAR(64) NOT NULL,
  full_name VARCHAR(120) NOT NULL,
  email VARCHAR(190) NULL,
  phone VARCHAR(40) NULL,
  dob DATE NULL,
  gender VARCHAR(24) NULL,
  address VARCHAR(255) NULL,
  role ENUM('Admin','Doctor','Patient') NOT NULL,
  password_hash VARCHAR(255) NULL,

  -- Forgot-password OTP in this backend
  reset_code VARCHAR(16) NULL,
  reset_expires DATETIME NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_users_uid (uid),
  UNIQUE KEY uq_users_email (email),
  KEY idx_users_role (role),
  KEY idx_users_created_at (created_at)
) ENGINE=InnoDB;

-- =========================================================
-- OPERATORIES (AppointmentAgent)
-- =========================================================
CREATE TABLE IF NOT EXISTS operatories (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (id),
  UNIQUE KEY uq_operatories_name (name)
) ENGINE=InnoDB;

-- =========================================================
-- CASES (Admin/Doctor/Patient)
-- =========================================================
CREATE TABLE IF NOT EXISTS cases (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  case_uid VARCHAR(64) NOT NULL,

  patient_id BIGINT UNSIGNED NOT NULL,
  doctor_id BIGINT UNSIGNED NULL,

  case_type VARCHAR(255) NULL,
  stage VARCHAR(32) NOT NULL DEFAULT 'NEW',

  priority ENUM('LOW','MEDIUM','HIGH') NOT NULL DEFAULT 'MEDIUM',
  risk_score INT NOT NULL DEFAULT 0,

  next_action VARCHAR(255) NULL,
  next_review_date DATE NULL,

  -- compatibility with UI fields used in server.js
  agent_summary TEXT NULL,
  agent_recommendation TEXT NULL,

  -- CaseTrackingAgent adds/uses this
  approval_required TINYINT(1) NOT NULL DEFAULT 1,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_cases_case_uid (case_uid),
  KEY idx_cases_patient (patient_id),
  KEY idx_cases_doctor (doctor_id),
  KEY idx_cases_stage (stage),
  KEY idx_cases_updated_at (updated_at),

  CONSTRAINT fk_cases_patient
    FOREIGN KEY (patient_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_cases_doctor
    FOREIGN KEY (doctor_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- APPOINTMENTS (Admin/Doctor/Patient + AppointmentAgent)
-- =========================================================
CREATE TABLE IF NOT EXISTS appointments (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  appointment_uid VARCHAR(64) NOT NULL,
  appointment_code VARCHAR(64) NOT NULL,

  patient_id BIGINT UNSIGNED NOT NULL,
  doctor_id BIGINT UNSIGNED NOT NULL,

  scheduled_date DATE NOT NULL,
  scheduled_time TIME NOT NULL,

  -- used by conflict checks + predicted duration
  predicted_duration_min INT NULL,
  scheduled_end_time TIME NULL,

  type VARCHAR(120) NOT NULL DEFAULT 'General',
  status VARCHAR(32) NOT NULL DEFAULT 'Confirmed',

  -- smart scheduling / rooms
  operatory_id BIGINT UNSIGNED NULL,

  -- actual timestamps
  actual_checkin_at DATETIME NULL,
  actual_start_at DATETIME NULL,
  actual_end_at DATETIME NULL,

  -- link to case timeline
  linked_case_id BIGINT UNSIGNED NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_appointments_uid (appointment_uid),
  UNIQUE KEY uq_appointments_code (appointment_code),

  KEY idx_appt_date_time (scheduled_date, scheduled_time),
  KEY idx_appt_doctor_date (doctor_id, scheduled_date),
  KEY idx_appt_patient_date (patient_id, scheduled_date),
  KEY idx_appt_status (status),
  KEY idx_appt_operatory (operatory_id),
  KEY idx_appt_linked_case (linked_case_id),

  CONSTRAINT fk_appt_patient
    FOREIGN KEY (patient_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_appt_doctor
    FOREIGN KEY (doctor_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_appt_operatory
    FOREIGN KEY (operatory_id) REFERENCES operatories(id)
    ON DELETE SET NULL ON UPDATE CASCADE,

  CONSTRAINT fk_appt_case
    FOREIGN KEY (linked_case_id) REFERENCES cases(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- APPOINTMENT AUDIT LOGS (AppointmentAgent)
-- =========================================================
CREATE TABLE IF NOT EXISTS appointment_audit_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  appointment_id BIGINT UNSIGNED NOT NULL,
  actor_user_id BIGINT UNSIGNED NULL,
  action VARCHAR(64) NOT NULL,
  note TEXT NULL,
  meta_json LONGTEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_audit_appt (appointment_id),
  KEY idx_audit_actor (actor_user_id),

  CONSTRAINT fk_audit_appt
    FOREIGN KEY (appointment_id) REFERENCES appointments(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_audit_actor
    FOREIGN KEY (actor_user_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- INVENTORY ITEMS (Admin inventory + InventoryAgent)
-- =========================================================
CREATE TABLE IF NOT EXISTS vendors (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(120) NOT NULL,
  phone VARCHAR(40) NULL,
  email VARCHAR(120) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_vendor_name (name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  item_code VARCHAR(64) NOT NULL,
  name VARCHAR(150) NOT NULL,
  category VARCHAR(80) NOT NULL DEFAULT 'Uncategorized',

  stock INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'Healthy',
  reorder_threshold INT NULL DEFAULT 0,

  expiry_date DATE NULL,

  vendor_id BIGINT UNSIGNED NULL,
  unit_cost DECIMAL(10,2) NULL,
  target_stock INT NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_inventory_item_code (item_code),
  KEY idx_inventory_name (name),
  KEY idx_inventory_category (category),
  KEY idx_inventory_stock (stock),
  KEY idx_inventory_vendor (vendor_id),
  KEY idx_inventory_expiry (expiry_date),

  CONSTRAINT fk_inventory_vendor
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_usage_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  appointment_id BIGINT UNSIGNED NULL,
  item_code VARCHAR(64) NULL,
  qty_used INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_usage_appt (appointment_id),
  KEY idx_usage_item (item_code),

  CONSTRAINT fk_usage_appt
    FOREIGN KEY (appointment_id) REFERENCES appointments(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_alerts (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  item_code VARCHAR(64) NULL,
  alert_type ENUM('LOW_STOCK','EXPIRING_SOON','EXPIRED','ANOMALY') NOT NULL,
  message TEXT NOT NULL,
  status ENUM('OPEN','ACK','CLOSED') NOT NULL DEFAULT 'OPEN',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_item_type (item_code, alert_type),
  KEY idx_type_status (alert_type, status),
  KEY idx_alert_updated (updated_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS purchase_orders (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  vendor_id BIGINT UNSIGNED NOT NULL,
  status ENUM('DRAFT','SENT','RECEIVED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
  notes TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_vendor_status (vendor_id, status),

  CONSTRAINT fk_po_vendor
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS purchase_order_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  po_id BIGINT UNSIGNED NOT NULL,
  item_code VARCHAR(64) NOT NULL,
  qty INT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_po_item (po_id, item_code),
  KEY idx_po (po_id),

  CONSTRAINT fk_poi_po
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- BILLING (RevenueAgent + Patient/Admin dashboards)
-- =========================================================
CREATE TABLE IF NOT EXISTS invoices (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  patient_id BIGINT UNSIGNED NOT NULL,

  issue_date DATE NOT NULL,
  amount DECIMAL(10,2) NOT NULL DEFAULT 0,

  status ENUM('Pending','Paid','Overdue') NOT NULL DEFAULT 'Pending',
  paid_date DATE NULL,

  -- RevenueAgent adds/uses:
  appointment_id BIGINT UNSIGNED NULL,
  invoice_type ENUM('PROVISIONAL','FINAL') NOT NULL DEFAULT 'FINAL',

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_inv_patient (patient_id),
  KEY idx_inv_issue_date (issue_date),
  KEY idx_inv_status (status),
  KEY idx_inv_paid_date (paid_date),
  KEY idx_inv_appt (appointment_id),

  CONSTRAINT fk_inv_patient
    FOREIGN KEY (patient_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_inv_appt
    FOREIGN KEY (appointment_id) REFERENCES appointments(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS procedure_catalog (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  code VARCHAR(64) NOT NULL,
  name VARCHAR(120) NOT NULL,
  default_price DECIMAL(10,2) NOT NULL DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY uq_proc_code (code),
  KEY idx_proc_name (name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS invoice_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  invoice_id BIGINT UNSIGNED NOT NULL,
  code VARCHAR(64) NOT NULL,
  description VARCHAR(200) NULL,
  qty INT NOT NULL DEFAULT 1,
  unit_price DECIMAL(10,2) NOT NULL DEFAULT 0,
  amount DECIMAL(10,2) NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_inv_items_invoice (invoice_id),
  KEY idx_inv_items_code (code),

  CONSTRAINT fk_inv_items_invoice
    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS revenue_insights (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  as_of_date DATE NOT NULL,
  summary TEXT NULL,
  raw_json LONGTEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_rev_as_of (as_of_date)
) ENGINE=InnoDB;

-- =========================================================
-- CASE TRACKING (CaseTrackingAgent + Doctor upload routes)
-- =========================================================
CREATE TABLE IF NOT EXISTS case_timeline (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  case_id BIGINT UNSIGNED NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  title VARCHAR(200) NULL,
  body TEXT NULL,
  meta_json LONGTEXT NULL,
  created_by_user_id BIGINT UNSIGNED NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_case_time (case_id, created_at),
  KEY idx_case_event (case_id, event_type),

  CONSTRAINT fk_case_timeline_case
    FOREIGN KEY (case_id) REFERENCES cases(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_case_timeline_user
    FOREIGN KEY (created_by_user_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS case_summaries (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  case_id BIGINT UNSIGNED NOT NULL,
  summary TEXT NOT NULL,
  recommendation TEXT NULL,
  confidence INT NOT NULL DEFAULT 50,
  status ENUM('PENDING_REVIEW','APPROVED','REJECTED') NOT NULL DEFAULT 'PENDING_REVIEW',
  created_by_agent TINYINT(1) NOT NULL DEFAULT 1,
  approved_by_user_id BIGINT UNSIGNED NULL,
  approved_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_case_status (case_id, status),
  KEY idx_case_created (case_id, created_at),

  CONSTRAINT fk_case_summaries_case
    FOREIGN KEY (case_id) REFERENCES cases(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_case_summaries_approver
    FOREIGN KEY (approved_by_user_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS case_attachments (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  case_id BIGINT UNSIGNED NOT NULL,
  file_name VARCHAR(255) NOT NULL,
  file_path VARCHAR(500) NOT NULL,
  mime_type VARCHAR(80) NULL,
  uploaded_by_user_id BIGINT UNSIGNED NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_case_attach_case (case_id),
  KEY idx_case_attach_user (uploaded_by_user_id),

  CONSTRAINT fk_case_attach_case
    FOREIGN KEY (case_id) REFERENCES cases(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_case_attach_user
    FOREIGN KEY (uploaded_by_user_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS case_doctors (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  case_id BIGINT UNSIGNED NOT NULL,
  doctor_id BIGINT UNSIGNED NOT NULL,
  role ENUM('PRIMARY','CONSULT') NOT NULL DEFAULT 'CONSULT',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uniq_case_doc (case_id, doctor_id),
  KEY idx_case_doc_doc (doctor_id),

  CONSTRAINT fk_case_doctors_case
    FOREIGN KEY (case_id) REFERENCES cases(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_case_doctors_doctor
    FOREIGN KEY (doctor_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- OUTBOX / EVENT QUEUE + NOTIFICATIONS (agents/eventQueue.js)
-- =========================================================
CREATE TABLE IF NOT EXISTS agent_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_type VARCHAR(64) NOT NULL,
  payload_json LONGTEXT NULL,
  status ENUM('NEW','PROCESSING','DONE','FAILED','DEAD') NOT NULL DEFAULT 'NEW',
  priority INT NOT NULL DEFAULT 50,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_by VARCHAR(64) NULL,
  locked_until DATETIME NULL,
  correlation_id VARCHAR(64) NULL,
  created_by_user_id BIGINT UNSIGNED NULL,
  last_error TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_status_available (status, available_at, priority, id),
  KEY idx_locked_by (locked_by),
  KEY idx_correlation (correlation_id),
  KEY idx_event_type (event_type)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS notifications (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NULL,
  user_role VARCHAR(16) NULL,
  channel ENUM('IN_APP','EMAIL','SMS','WHATSAPP','CALL') NOT NULL DEFAULT 'IN_APP',
  type VARCHAR(64) NULL,
  title VARCHAR(200) NULL,
  message TEXT NOT NULL,
  status ENUM('PENDING','SENT','FAILED','READ') NOT NULL DEFAULT 'PENDING',
  scheduled_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at DATETIME NULL,
  read_at DATETIME NULL,
  meta_json LONGTEXT NULL,

  PRIMARY KEY (id),
  KEY idx_user (user_id),
  KEY idx_status (status),
  KEY idx_type (type),
  KEY idx_scheduled (scheduled_at),
  KEY idx_created_at (created_at),

  CONSTRAINT fk_notifications_user
    FOREIGN KEY (user_id) REFERENCES users(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- OPTIONAL SEED (safe to run)
-- =========================================================
INSERT IGNORE INTO operatories (id, name, is_active) VALUES
  (1, 'Room 1', 1),
  (2, 'Room 2', 1);

-- Example: basic catalog items (RevenueAgent uses UPPER_SNAKE code)
INSERT IGNORE INTO procedure_catalog (code, name, default_price) VALUES
  ('GENERAL', 'General consultation', 0.00),
  ('SCALING', 'Scaling', 0.00),
  ('FILLING', 'Filling', 0.00),
  ('ROOT_CANAL', 'Root canal', 0.00),
  ('EXTRACTION', 'Extraction', 0.00),
  ('IMPLANT', 'Implant', 0.00);



SET SQL_MODE = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
USE dental_clinic;

-- =========================================================
-- 0) Helper: add column if missing (works on MySQL/MariaDB)
-- =========================================================
DROP PROCEDURE IF EXISTS add_col_if_missing;
DELIMITER $$
CREATE PROCEDURE add_col_if_missing(
  IN p_table VARCHAR(64),
  IN p_col   VARCHAR(64),
  IN p_def   TEXT
)
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = p_table
      AND COLUMN_NAME = p_col
  ) THEN
    SET @sql = CONCAT('ALTER TABLE ', p_table, ' ADD COLUMN ', p_col, ' ', p_def);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;

-- =========================================================
-- 1) VISITS (new) - 1 visit per appointment (usually)
--    Stores procedure JSON safely for UI + agents
-- =========================================================
CREATE TABLE IF NOT EXISTS visits (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  visit_uid VARCHAR(64) NOT NULL,

  appointment_id BIGINT UNSIGNED NULL,
  linked_case_id BIGINT UNSIGNED NULL,

  patient_id BIGINT UNSIGNED NOT NULL,
  doctor_id BIGINT UNSIGNED NOT NULL,

  status ENUM('OPEN','CLOSED','CANCELLED') NOT NULL DEFAULT 'OPEN',

  -- clinical notes / summaries
  chief_complaint VARCHAR(255) NULL,
  clinical_notes TEXT NULL,
  diagnosis_text VARCHAR(255) NULL,

  -- Procedure JSON (UI-safe, agent-safe)
  -- Example: [{"code":"FILLING","tooth":"12","notes":"..."}, ...]
  procedures_json LONGTEXT NULL,

  -- Optional structured data
  vitals_json LONGTEXT NULL,
  findings_json LONGTEXT NULL,

  started_at DATETIME NULL,
  ended_at DATETIME NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_visits_uid (visit_uid),

  KEY idx_visits_appt (appointment_id),
  KEY idx_visits_case (linked_case_id),
  KEY idx_visits_patient (patient_id),
  KEY idx_visits_doctor (doctor_id),
  KEY idx_visits_status (status),
  KEY idx_visits_created (created_at),

  CONSTRAINT fk_visits_patient
    FOREIGN KEY (patient_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_visits_doctor
    FOREIGN KEY (doctor_id) REFERENCES users(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT fk_visits_appt
    FOREIGN KEY (appointment_id) REFERENCES appointments(id)
    ON DELETE SET NULL ON UPDATE CASCADE,

  CONSTRAINT fk_visits_case
    FOREIGN KEY (linked_case_id) REFERENCES cases(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

-- Optional: enforce “at most one visit per appointment”
-- (safe if you want strict, but keep disabled if your workflow allows multiple visits per appointment)
-- ALTER TABLE visits ADD UNIQUE KEY uq_visits_appointment (appointment_id);

-- =========================================================
-- 2) VISIT_PROCEDURES (new) - normalized procedure rows
--    Keeps procedure JSON + enables analytics + billing safety
-- =========================================================
CREATE TABLE IF NOT EXISTS visit_procedures (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  visit_id BIGINT UNSIGNED NOT NULL,

  -- Link to catalog by code (keeps compatibility with your existing procedure_catalog)
  procedure_code VARCHAR(64) NOT NULL,

  tooth VARCHAR(16) NULL,
  surface VARCHAR(32) NULL,
  qty INT NOT NULL DEFAULT 1,

  -- durations for scheduling analytics
  predicted_duration_min INT NULL,
  actual_duration_min INT NULL,

  -- pricing snapshot (optional)
  unit_price DECIMAL(10,2) NULL,
  amount DECIMAL(10,2) NULL,

  notes VARCHAR(255) NULL,
  meta_json LONGTEXT NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_vp_visit (visit_id),
  KEY idx_vp_code (procedure_code),

  CONSTRAINT fk_vp_visit
    FOREIGN KEY (visit_id) REFERENCES visits(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- 3) PROCEDURE_CONSUMABLES (new) - mapping procedure -> items
--    InventoryAgent uses this for AUTO deduction
-- =========================================================
CREATE TABLE IF NOT EXISTS procedure_consumables (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  procedure_type VARCHAR(120) NOT NULL,
  item_code VARCHAR(64) NOT NULL,
  qty INT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_proc_item (procedure_type, item_code),
  KEY idx_pc_proc (procedure_type),
  KEY idx_pc_item (item_code)
) ENGINE=InnoDB;

-- =========================================================
-- 4) VISIT_CONSUMABLES (new) - consumption per visit/procedure
--    Keeps audit trail + supports leakage/fraud detection
-- =========================================================
CREATE TABLE IF NOT EXISTS visit_consumables (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  visit_id BIGINT UNSIGNED NOT NULL,
  appointment_id BIGINT UNSIGNED NULL,
  doctor_id BIGINT UNSIGNED NULL,

  item_code VARCHAR(64) NOT NULL,
  qty_used INT NOT NULL DEFAULT 0,

  source ENUM('AUTO','MANUAL','ADJUSTMENT') NOT NULL DEFAULT 'AUTO',
  note VARCHAR(255) NULL,
  meta_json LONGTEXT NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_vc_visit (visit_id),
  KEY idx_vc_appt (appointment_id),
  KEY idx_vc_doctor (doctor_id),
  KEY idx_vc_item (item_code),
  KEY idx_vc_created (created_at),

  CONSTRAINT fk_vc_visit
    FOREIGN KEY (visit_id) REFERENCES visits(id)
    ON DELETE CASCADE ON UPDATE CASCADE,

  CONSTRAINT fk_vc_appt
    FOREIGN KEY (appointment_id) REFERENCES appointments(id)
    ON DELETE SET NULL ON UPDATE CASCADE,

  CONSTRAINT fk_vc_doctor
    FOREIGN KEY (doctor_id) REFERENCES users(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

-- =========================================================
-- 5) Non-breaking enhancements to existing tables
--    Adds visit_id to inventory_usage_logs (keeps UI intact)
-- =========================================================
CALL add_col_if_missing('inventory_usage_logs', 'visit_id', 'BIGINT UNSIGNED NULL AFTER appointment_id');
CALL add_col_if_missing('inventory_usage_logs', 'doctor_id', 'BIGINT UNSIGNED NULL AFTER visit_id');
CALL add_col_if_missing('inventory_usage_logs', 'source', "ENUM('AUTO','MANUAL','ADJUSTMENT') NOT NULL DEFAULT 'AUTO' AFTER qty_used");
CALL add_col_if_missing('inventory_usage_logs', 'meta_json', 'LONGTEXT NULL AFTER source');

-- indexes + FK (safe)
-- index
SET @ix1 := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='inventory_usage_logs' AND INDEX_NAME='idx_usage_visit'
);
SET @sql_ix1 := IF(@ix1=0, 'CREATE INDEX idx_usage_visit ON inventory_usage_logs(visit_id)', 'SELECT 1');
PREPARE s1 FROM @sql_ix1; EXECUTE s1; DEALLOCATE PREPARE s1;

-- FK for visit_id (only if column exists and fk not already present)
DROP PROCEDURE IF EXISTS add_fk_if_missing;
DELIMITER $$
CREATE PROCEDURE add_fk_if_missing(
  IN p_table VARCHAR(64),
  IN p_fkname VARCHAR(64),
  IN p_sql TEXT
)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA=DATABASE()
      AND TABLE_NAME=p_table
      AND CONSTRAINT_NAME=p_fkname
      AND CONSTRAINT_TYPE='FOREIGN KEY'
  ) THEN
    SET @sql = p_sql;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;

CALL add_fk_if_missing(
  'inventory_usage_logs',
  'fk_usage_visit',
  'ALTER TABLE inventory_usage_logs
     ADD CONSTRAINT fk_usage_visit
     FOREIGN KEY (visit_id) REFERENCES visits(id)
     ON DELETE SET NULL ON UPDATE CASCADE'
);

CALL add_fk_if_missing(
  'inventory_usage_logs',
  'fk_usage_doctor',
  'ALTER TABLE inventory_usage_logs
     ADD CONSTRAINT fk_usage_doctor
     FOREIGN KEY (doctor_id) REFERENCES users(id)
     ON DELETE SET NULL ON UPDATE CASCADE'
);

-- =========================================================
-- 6) Optional: add visit_id to invoice_items (non-breaking)
--    Helps detect "unbilled procedures" leakage accurately
-- =========================================================
CALL add_col_if_missing('invoice_items', 'visit_procedure_id', 'BIGINT UNSIGNED NULL AFTER invoice_id');

SET @ix2 := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='invoice_items' AND INDEX_NAME='idx_inv_items_vpid'
);
SET @sql_ix2 := IF(@ix2=0, 'CREATE INDEX idx_inv_items_vpid ON invoice_items(visit_procedure_id)', 'SELECT 1');
PREPARE s2 FROM @sql_ix2; EXECUTE s2; DEALLOCATE PREPARE s2;

CALL add_fk_if_missing(
  'invoice_items',
  'fk_invoice_items_vpid',
  'ALTER TABLE invoice_items
     ADD CONSTRAINT fk_invoice_items_vpid
     FOREIGN KEY (visit_procedure_id) REFERENCES visit_procedures(id)
     ON DELETE SET NULL ON UPDATE CASCADE'
);

-- =========================================================
-- 7) Cleanup helper procedures
-- =========================================================
DROP PROCEDURE IF EXISTS add_col_if_missing;
DROP PROCEDURE IF EXISTS add_fk_if_missing;
