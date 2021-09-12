#!/usr/bin/env python3
from datetime import datetime
import sys

if len(sys.argv) > 1 and '-' in sys.argv[1]:
    try:
        target_time = datetime.fromisoformat(' '.join(sys.argv[1:]))
    except ValueError:
        print('Please use format YYYY-MM-DDTHH:MM[:SS]')
        sys.exit(1)
    if target_time > datetime.utcnow():
        print(f'Current UTC time is {datetime.utcnow():%Y-%m-%dt%H:%M}.')
        sys.exit(1)
    print(int(target_time.timestamp() / 60) - 22457216)
else:
    now = int(datetime.utcnow().timestamp() / 60) - 22457216
    from_ts = int(sys.argv[1]) + 1 if len(sys.argv) > 1 else now - 1
    for ts in range(from_ts, now):
        print(ts)
