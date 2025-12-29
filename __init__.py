# dental_agents/agents/__init__.py
from .appointment_agent import on_appointment_created, on_appointment_completed
from .inventory_agent import on_appointment_completed as inventory_on_appointment_completed, daily_inventory_checks
from .revenue_agent import on_appointment_created as revenue_on_appointment_created
from .revenue_agent import on_appointment_completed as revenue_on_appointment_completed
from .revenue_agent import daily_revenue_insights, ar_reminders_sweep
from .case_tracking_agent import on_case_updated, on_appointment_completed as case_on_appointment_completed
