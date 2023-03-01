import time
import math
import datetime
from dateutil.relativedelta import relativedelta



def parse_time(s):
    if len(s) == 10:
        frmt = "%Y-%m-%d"
    else:
        frmt = "%Y-%m-%d %H:%M:%S"
    return datetime.datetime.strptime(s, frmt)


def datetime_modulo(date_t, delta_t):
    seconds = int((date_t - datetime.datetime.min).total_seconds())
    remainder = datetime.timedelta(
        seconds=seconds % delta_t.total_seconds(),
        microseconds=date_t.microsecond,
    )
    return remainder


def valid_year_start_dt(datetime_obj):
    ''' Checks that the datetime object has the correct
    month, day, and time at the beginning of the year. '''
    return datetime_obj.strftime("%m/%d %H:%M:%S") == "01/01 00:00:00"


def valid_month_start_dt(datetime_obj):
    ''' Checks that the datetime object has the correct
    day and time at the beginning of the month. '''
    return datetime_obj.strftime("%d %H:%M:%S") == "01 00:00:00"


def str_time_list(typ, start, end):
    if (typ == 'year'):
        diff = end.year - start.year
        valid_start_dt = lambda x: valid_year_start_dt(x)
        time_interval = relativedelta(years=+1)
        new_start = datetime.datetime(start.year, 1, 1) 
        new_end = datetime.datetime(end.year, 1, 1)
    elif (typ == 'month'):
        diff = end.month - start.month # can be negative
        valid_start_dt = lambda x: valid_month_start_dt(x)
        time_interval = relativedelta(months=+1)
        new_start = datetime.datetime(start.year, start.month, 1)
        new_end = datetime.datetime(end.year, end.month, 1)
    else:
        raise ValueError("unrecognized time_interval " + time_interval)

    time_list = list()
    if (diff == 0):
        time_list.append((new_start, new_end + time_interval))
    elif (diff == 1 or diff == -1):
        if (valid_start_dt(end)):
            time_list.append((new_start, new_end))
        else:
            raise ValueError("""Invalid start and end datetimes.""")
    else:
        if (valid_start_dt(new_start) and valid_start_dt(new_end)):
            temp_time = new_start
            while (temp_time < new_end):
                time_list.append((temp_time, temp_time + time_interval))
                temp_time += time_interval
        else:
            raise ValueError("""Invalid start and end datetimes.""")
    
    return time_list


def generate_time_list(start, end, time_interval):
    ''' Checks various conditions with the start and end time. Creates
        a list of time periods given a time interval. For example:
        time_interval = 30 -> [(00:00, 00:30), (00:30, 01:00), ...]
    '''

    if isinstance(time_interval, str):
        return str_time_list(time_interval, start, end)

    if (end < start):
        raise ValueError("Start time must be before end time.")

    time_list = list()
    if ((end - start) < time_interval):
        if (start.day != end.day and end.time().strftime("%H:%M:%S") != '00:00:00'):
            raise ValueError("""Make sure not to have time ranges less than the 
                              time interval that overlap multiple days.""")

        time_interval_hours = time_interval.total_seconds() / 3600.0
        start_hours = start.hour + start.minute / 60.0
        end_hours = end.hour + end.minute / 60.0

        start_div_interval = start_hours / time_interval_hours
        end_div_interval = end_hours / time_interval_hours

        start_floor = math.floor(start_hours / time_interval_hours) * time_interval_hours
        end_ceil = math.ceil(end_hours / time_interval_hours) * time_interval_hours

        start_floor_td = datetime.timedelta(seconds=(start_floor*3600))
        end_ceil_td = datetime.timedelta(seconds=(end_ceil*3600))

        new_start = datetime.datetime(start.year, start.month, start.day, 0, 0, 0) + start_floor_td 
        new_end = datetime.datetime(end.year, end.month, end.day, 0, 0, 0) + end_ceil_td

        if (start_div_interval.is_integer() or end_div_interval.is_integer()): # one datetime is on the time interval edge
            time_list.append((new_start, new_end))
        elif (int(end_hours / time_interval_hours) - int(start_hours / time_interval_hours) == 0): # both datetimes are between time interval
            time_list.append((new_start, new_end))
        else:
            raise ValueError("""Start and end time overlap time interval edge.""")
    else:
        if (datetime_modulo(start, time_interval) == datetime.timedelta(0) and
            datetime_modulo(end, time_interval) == datetime.timedelta(0)):
            temp_time = start
            while (temp_time < end):
                time_list.append((temp_time, temp_time + time_interval))
                temp_time += time_interval
        else:
            raise ValueError("""Start and end times must be divisable by the time 
                              interval when the time between them exceeds the time interval.""")
    return time_list


def check_timescale_cond(start_dt, end_dt, timescale):
    ''' Return true if the difference between the two
        datetimes equals the timescale. '''
 
    if (isinstance(timescale, datetime.timedelta)):
        return (start_dt + timescale == end_dt)

    elif (isinstance(timescale, str)):
        if (timescale == 'year'):
            return (start_dt + relativedelta(years=+1) == end_dt)
        elif (timescale == 'month'):
            return (start_dt + relativedelta(months=+1) == end_dt)
        else:
            raise ValueError("""Unknown timescale string value.""")

    else:
        raise ValueError("""Unknown timescale type.""")
