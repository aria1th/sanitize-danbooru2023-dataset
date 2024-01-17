command_string = "python sanity_check.py --start-idx {start} --end-idx {end} --threads 8 --retry 30 --proxy-auth {auth} --proxy-address http://{ip}:{port} --proxy --logging-file query_log_{index}.log --save-file difference_cache_{ranged}.jsonl --requests-cache requests_{index}.jsonl"

# executes the command string

start_from = 1
end_at = 7000000 #test

import os
import subprocess
ips = []
if not os.path.exists('ips.txt'):
    raise FileNotFoundError("ips.txt not found")
with open('ips.txt', 'r') as f:
    ips = f.read()
ips = ips.split()
ip_count = len(ips)
print(f"Total IPs: {ip_count}")

# run command with windows call command to show screen output
# split ranges to ips
per_ip_range = (end_at - start_from) // ip_count
print(f"Per IP Range: {per_ip_range}")
auth = input("Proxy Auth: ")
port = input("Proxy Port: ")

for i in range(ip_count):
    start = start_from + i * per_ip_range
    end = start_from + (i + 1) * per_ip_range
    if i == ip_count - 1:
        end = end_at
    print(f"IP {i}: {ips[i]}")
    print(f"Start: {start}, End: {end}")
    print(f"Command: {command_string.format(start=start, end=end, ip=ips[i], index=i, ranged=f'{start}_{end}', auth=auth, port=port)}")
    print()
    # run command with cmd call, asynchroneously
    # start /wait cmd /c {command_string}
    # this is for windows
    if os.name == 'nt':
        subprocess.Popen(f"start /wait cmd /c {command_string.format(start=start, end=end, ip=ips[i], index=i, ranged=f'{start}_{end}', auth=auth, port=port)}", shell=True)
    else:
        # with screen
        subprocess.Popen(f"screen -dmS {i} {command_string.format(start=start, end=end, ip=ips[i], index=i, ranged=f'{start}_{end}', auth=auth, port=port)}", shell=True)
