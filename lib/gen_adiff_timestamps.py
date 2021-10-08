#!/usr/bin/env python3
from datetime import datetime, timezone
import sys

if len(sys.argv) > 1 and '-' in sys.argv[1]:
    if sys.argv[1][0] == '-':
        # Decode a timestamp. Super-hidden option!
        try:
            ts = 60 * (22457216 + int(sys.argv[1][1:]))
        except ValueError:
            print('Use either "-state" or a date for the first argument')
            sys.exit(1)
        print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M'))
    else:
        try:
            target_time = datetime.fromisoformat(' '.join(sys.argv[1:]) + '+00:00')
        except ValueError:
            print('Please use format YYYY-MM-DDTHH:MM[:SS]')
            sys.exit(1)
        if target_time > datetime.now(timezone.utc):
            print(f'Current UTC time is {datetime.utcnow():%Y-%m-%dt%H:%M}.')
            sys.exit(1)
        print(int(target_time.timestamp() / 60) - 22457216)
else:
    now = int(datetime.now(timezone.utc).timestamp() / 60) - 22457216
    from_ts = int(sys.argv[1]) + 1 if len(sys.argv) > 1 else now - 1
    for ts in range(from_ts, now):
        print(ts)
