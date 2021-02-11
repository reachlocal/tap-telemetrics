from datetime import datetime, timedelta

date_range = '20200101,20200112'

range_size = 5
range_parts = date_range.split(',')
date_format = "%Y%m%d"
start = datetime.strptime(range_parts[0], date_format)
end = datetime.strptime(range_parts[1], date_format)

output = []
while (end - start).days >= (range_size - 1):
    new_start = start + timedelta(days=range_size - 1)
    output.append({
        'start': start.replace(microsecond=0, second=0, minute=0, hour=0).isoformat(),
        'end': new_start.replace(microsecond=0, second=59, minute=59, hour=23).isoformat()
    })

    start = new_start + timedelta(days=1)

if (end - start).days > 0:
    output.append({
        'start': start.replace(microsecond=0, second=0, minute=0, hour=0).isoformat(),
        'end': end.replace(microsecond=0, second=59, minute=59, hour=23).isoformat()
    })

print(output)
