Introduction
------------

This is the deviantART fork of the **pystatsd** server.  We prefer to use runit instead of upstart.


Differences
------------
* Uses Twisted to handle the network portion
* Provides an XMLRPC interface for monitoring
* Publishes a lot more meta stats about internals
* Cleanly handles 'bad' metric types w/ discard
* Allows multiple metrics in a single message using newline character
* Failures go to stderror
* Has a '-d' debug option for dumping matching incoming stats to terminal
* Can notify NSCA on failures if a notify_nsca function is provided
* Allows incoming stats over TCP

* Graphite
    - https://github.com/graphite-project
* Statsd
    - code: https://github.com/etsy/statsd
    - blog post: http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/
* NSCA Notify
    - https://github.com/tkuhlman/scripts/blob/master/nagios/nsca-notify

statsd.py and statsdmonitor.py have been tested on Python 2.7.

Usage
-------------

* Get RUNIT going 

    aptitude install runit

See: http://alfredocambera.blogspot.com/2013/12/a-primer-on-runit-using-debian-wheezy.html

* The run script should be similar to:

```
#!/bin/bash
set -e
exec chpst -u statsd  /usr/sbin/statsd.py \
                                       -a \
                                       -q \
                                       -p <port>  \
                                       --flush-interval 10 \
                                       --graphite-port <graphite_port> \
                                       --graphite-host <graphite_host>  \
                                       --metadata-prefix statsd.my_prefix  \
                                       --sets-prefix my_prefix.set \
                                       --counters-prefix my_prefix \
                                       --timers-prefix my_prefix.timers \
                                       --gauge-prefix <my_prefix>
```

Troubleshooting
-------------

You can see the raw values received by pystatsd by packet sniffing:

    $ sudo ngrep -qd any . udp dst port 8125

You can see the raw values dispatched to carbon by packet sniffing:

    $ sudo ngrep -qd any stats tcp dst port 2003

Using statsdmonitor.py (the monitor port will +10 to the stats port)

    statsdmonitor.py localhost  8135

```
flush_duration: 0.410356044769
flush_errors: 0
last_publish.duration: 0.352137088776
last_publish.time: 1393294923.76
metrics.batch.count: 1454
metrics.batch.discarded: 0
metrics.discarded: 120
pending_stats: 10334
publish_error.socket: 0
publish_error.unknown: 0
render.counters.count: 0
render.counters.duration: 0
render.gauge.count: 28977
render.gauge.duration: 0.049978017807
render.set.count: 0
render.set.duration: 0
render.time: 0.0500249862671
render.timers.count: 0
render.timers.duration: 0
render.total: 28990
start_time: 1392235332.92
```

Authors
-------------

This library is under the BSD license and was forked from Steve Ivy at https://github.com/sivy/pystatsd.

The deviantART fork of this library was developed by Chase Pettet, Devendra Gera, and Chris Bolt at deviantART.

I have a presentation that included this fork about setting up Graphite.

http://chasemp.github.io/2014/02/27/python-monitoring-with-diamond-statsd-graphite/
