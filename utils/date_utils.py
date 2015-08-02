import datetime

def get_current_utc_datetime():
    """
        Helper method that gives us the current utc datetime.
        This makes mocking the time during testing easier
    """
    return datetime.utcnow()

def addSeconds(seconds, dt):
    return dt + datetime.timedelta(seconds=seconds)