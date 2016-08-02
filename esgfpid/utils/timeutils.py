import datetime



def get_now_utc_as_formatted_string():
    now = get_now_utc()
    now_string = datetime.datetime.isoformat(now) # 2015-12-21T10:31:37.524825+00:00
    return now_string

def get_now_utc():
    ''' date in UTC, ISO format'''

    # Helper class for UTC time
    # Source: http://stackoverflow.com/questions/2331592/datetime-datetime-utcnow-why-no-tzinfo
    ZERO = datetime.timedelta(0)
    class UTC(datetime.tzinfo):
        """UTC"""
        def utcoffset(self, dt):
            return ZERO
        def tzname(self, dt):
            return "UTC"
        def dst(self, dt):
            return ZERO

    #now = datetime.datetime.now(timezone.utc) # Python 3.2
    now = datetime.datetime.now(UTC())
    return now
 