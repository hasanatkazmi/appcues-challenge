appcues challenge
===============

To start web server:
```
>>python main.py
```
You can run stress tests using:
```
>>python stress-test.py
```

You can also change defaults inside stress-test.py to change unique keys and values POSTed per key to the server.
Works with Python 2.7 on *nix platforms.
All logs are sent to /tmp/appcues-log. Default log level is INFO. Change it to DEBUG though throughput will go down as file writes will go up.
Keys POSTed are case sensitive.

Everything is saved to local cache. Cache is 'offloaded' to the datebase every 8 secs or whenever cache size swells to 10KB. 10KB cache size is optimal. Increasing it will make very long SQL statements.