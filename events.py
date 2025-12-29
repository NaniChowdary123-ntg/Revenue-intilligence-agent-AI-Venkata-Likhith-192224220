# dental_agents/events.py
# Central list of event names used by worker/Node outbox.
APPOINTMENT_CREATED = "AppointmentCreated"
APPOINTMENT_COMPLETED = "AppointmentCompleted"
CASE_UPDATED = "CaseUpdated"
CASE_GENERATE_SUMMARY = "CaseGenerateSummary"

APPOINTMENT_AUTO_SCHEDULE_REQUESTED = "AppointmentAutoScheduleRequested"
APPOINTMENT_MONITOR_TICK = "AppointmentMonitorTick"

INVENTORY_DAILY_TICK = "InventoryDailyTick"
REVENUE_DAILY_TICK = "RevenueDailyTick"
