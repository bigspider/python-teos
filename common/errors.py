# Appointment errors [-1, -32]
APPOINTMENT_EMPTY_FIELD = -1
APPOINTMENT_WRONG_FIELD_TYPE = -2
APPOINTMENT_WRONG_FIELD_SIZE = -3
APPOINTMENT_WRONG_FIELD_FORMAT = -4
APPOINTMENT_FIELD_TOO_SMALL = -5
APPOINTMENT_FIELD_TOO_BIG = -6
APPOINTMENT_WRONG_FIELD = -7
APPOINTMENT_INVALID_SIGNATURE_OR_SUBSCRIPTION_ERROR = -8
APPOINTMENT_ALREADY_TRIGGERED = -9

# Registration errors [-33, -64]
REGISTRATION_MISSING_FIELD = -33
REGISTRATION_WRONG_FIELD_FORMAT = -34

# General errors [-65, -96]
INVALID_REQUEST_FORMAT = -65

# Custom RPC errors [255+]
RPC_TX_REORGED_AFTER_BROADCAST = -256
# UNHANDLED
UNKNOWN_JSON_RPC_EXCEPTION = -257
