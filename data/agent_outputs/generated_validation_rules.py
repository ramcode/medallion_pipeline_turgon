# Generated validation rules (implemented by the Silver stage)

def is_valid_ticket_id(value):
    return value.startswith("TKT-") and value[4:].isdigit()

def valid_non_negative(value):
    return value is not None and value >= 0

def valid_sla_hours(value):
    return value is not None and value > 0
