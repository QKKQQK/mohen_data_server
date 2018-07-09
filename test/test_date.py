from datetime import datetime, timedelta

record_date = datetime(2018, 7, 2, 15, 5, 35, 1000)

print(record_date)

print(record_date.year)
print(record_date.month)
print(record_date.day)
print(record_date.hour)
print(record_date.minute)
print(record_date.second)

print("Duration: ", str(timedelta(seconds=(366.387))))