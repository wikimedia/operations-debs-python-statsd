#!/usr/bin/env python
import sys
sys.path.append("/usr/local/lib/python")
from twisted.web import xmlrpc, server
from twisted.internet import protocol, reactor
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.task import LoopingCall
import sys
import argparse
import time
import sys
import os
import re
import json
import socket
import time
import types
import logging
import inspect
import traceback

try:
    from da.common import nsca_notify
except ImportError:
    def nsca_notify(*args):
        pass


class Factory(protocol.Factory):
    """responds w/ tcp engine for incoming"""

    def __init__(self, stats_engine):
        self.stats_engine = stats_engine

    def buildProtocol(self, addr):
        return StatsTCPEngine(self.stats_engine)


class StatsTCPEngine(protocol.Protocol):
    """collect stats by tcp"""

    def __init__(self, stats_engine):
        self.stats_engine = stats_engine

    def dataReceived(self, data):
        self.stats_engine.process(data)


class StatsUDPEngine(DatagramProtocol):
    """collect stats by udp"""

    def __init__(self, stats_engine):
        self.stats_engine = stats_engine

    def datagramReceived(self, data, (host, port)):
        self.stats_engine.process(data)


class StatsHandler(xmlrpc.XMLRPC):
    """
    Deals w/ sanitizing, organizing, and flushing statsd style stats
    """

    def __init__(self, pct_threshold=90,
                       debug=False,
                       quiet=False,
                       d_metric=None,
                       flush_interval=10,
                       graphite_host='localhost',
                       graphite_port=2003,
                       metadata_prefix='statsd',
                       counters_prefix='stats',
                       sets_prefix='stats',
                       timers_prefix='stats.timers',
                       gauge_prefix='stats'):

        xmlrpc.XMLRPC.__init__(self)
        self.buf = 16284
        self.pct_threshold = pct_threshold
        self.graphite_host = graphite_host
        self.graphite_port = graphite_port
        self.metadata_prefix = metadata_prefix
        self.gauge_prefix = gauge_prefix
        self.counters_prefix = counters_prefix
        self.sets_prefix = sets_prefix
        self.timers_prefix = timers_prefix
        self.debug = debug
        self.quiet = quiet
        self.started = time.time()
        self.d_metric = d_metric
        self.flush_interval = flush_interval
        self.last_crit_error = 0

        self.gauge = {}
        self.set = {}
        self.counters = {}
        self.timers = {}
        self.flusher = 0
        self.stats = {}
        self.stats['start_time'] = self.started
        self.stats['flush_errors'] = 0
        self.stats['pending_stats'] = 0
        self.stats['publish_error.socket'] = 0
        self.stats['publish_error.unknown'] = 0
        self.stats['metrics.discarded'] = 0
        self.stats['metrics.batch.count'] = 0
        self.stats['metrics.batch.discarded'] = 0
        self.stats['last_publish.duration'] = 0
        #reflect on how to render counters
        self.methods = dict(inspect.getmembers(self, \
                            predicate=inspect.ismethod))

        try:
            self.destination = socket.gethostbyname(self.graphite_host)
        except socket.error:
            raise ValueError("Not a valid hostname or IP destination")

        if debug:
            print "__Starting Properties__"
            for prop, value in vars(self).iteritems():
                if value and prop != 'methods':
                    print prop, ": ", value

    def xmlrpc_istats(self):
        """Registers for xmlrpc as istats remotecall"""
        return json.dumps(self.stats)

    def _clean_key(self, k):
        return re.sub(
            r'[^a-zA-Z_\-0-9\.]',
            '',
            re.sub(
                r'\s+',
                '_',
                k.replace('/', '-').replace(' ', '_')
            )
        )

    def log(self, msg):
        if msg is None:
            return
        if not self.quiet:
            print msg

    def discard_metric(self, *args):
        self.stats['metrics.discarded'] += 1
        if self.debug:
            debug_msg = 'WARN discard: ' + '\n'.join(map(str, args))
            self.log(debug_msg)
        return

    def process(self, data):
        """
        Deal with incoming stats -- this should be as terse as possible
        """
        def sane_value(metric):
            """
            Making sure our data is numerical and sane
            """
            try:
                return int(metric)
            except ValueError:
                try:
                    return float(metric)
                except ValueError:
                    raise ValueError("%s is not sane numerical data" % (metric,))

        def operate(metric):
            """
            'bad' metrics are skipped
            metrics with all needed values are sent on
            Examples of good metrics:

            test.timer:3|ms
            test.timer:0.674413151865|ms
            test.set:0.674413151865|s
            test.counter.1_1:1.1|c
            test.counter.1_1:-1|c
            test.counter.samplerate:210|c|@0.5
            """
            self.stats['pending_stats'] += 1
            #split apart metric and properties
            bits = metric.split(':')
            #break property string into fields
            fields = bits[1].split('|')

            #We need at least a metric and a value
            if len(fields) < 2:
                raise ValueError("invalid fields: %s" % (fields))

            key = self._clean_key(bits[0])
            #Actions are further processing
            #info for each metric type
            action = fields[2] if len(fields) > 2 else None
            value = sane_value(fields[0])

            self.integrate(key, value, fields[1], action=action)

        try:
            if '\n' in data:
                self.stats['metrics.batch.count'] += 1
                metrics = filter(None, data.split('\n'))
                for metric in metrics:
                    operate(metric)
            else:
                operate(data)
        except Exception, e:
            self.discard_metric(data, str(e))
            return

    def integrate(self, key, value, pointer, action=None):
        """
        Integrate metric into existing data set
        being staged for flush
        """

        def new_gauge(key, value):
            if key not in self.gauge:
                self.gauge[key] = []
            self.gauge[key] = value

        def new_timer(key, value):
            if key not in self.timers or self.timers[key] is None:
                self.timers[key] = []
            self.timers[key].append(value)

        def new_set(key, value):
            if key not in self.set or self.set[key] is None:
                self.set[key] = 0
            self.set[key] += value

        def new_counter(key, value):
            #Try to extract sample rate
            if action:
                try:
                    pattern = '^@([\d\.]+)'
                    sample_rate = float(re.match(pattern, action).groups()[0])
                except ValueError, e:
                    self.discard_metric((key, value, action), str(e))
                    return
                except AttributeError, e:
                    self.discard_metric((key, value, action), str(e))
                    return
            else:
                sample_rate = 1

            if key not in self.counters or self.counters[key] is None:
                self.counters[key] = 0
            if sample_rate < 1:
                value = value * (1.0 / sample_rate)
            self.counters[key] += value

        def default(key, value):
            msg = "Received unknown metric %s" % (key)
            self.discard_metric(msg)
            return

        addmetric = {'g': new_gauge,
                     'ms': new_timer,
                     's': new_set,
                     'c': new_counter}.get(pointer, default)

        addmetric(key, value)

    def render_gauge(self, metric, value, ts):
        return "%s.%s %s %s\n" % (self.gauge_prefix, metric, value, ts)

    def render_counters(self, metric, value, ts):
        #http://python-history.blogspot.com/2009/03/problem-with-integer-division.html
        value = value / (self.flush_interval * 1.0)
        return '%s.%s %s %s\n' % (self.counters_prefix, metric, value, ts)

    def render_set(self, metric, value, ts):
        return '%s.%s %s %s\n' % (self.sets_prefix, metric, value, ts)

    def render_timers(self, metric, value, ts):
        # Sort all the received values. We need it to extract percentiles
        TIMER_MSG = '''%(prefix)s.%(key)s.lower %(min)s %(ts)s
        %(prefix)s.%(key)s.count %(count)s %(ts)s
        %(prefix)s.%(key)s.mean %(mean)s %(ts)s
        %(prefix)s.%(key)s.upper %(max)s %(ts)s
        %(prefix)s.%(key)s.upper_%(pct_threshold)s %(max_threshold)s %(ts)s
        '''

        value.sort()
        count = len(value)
        minimum = min(value)
        maximum = max(value)

        if count > 1:
            thresh_index = int((self.pct_threshold / 100.0) * count)
            max_threshold = value[thresh_index - 1]
            total = sum(value)
            mean = total / count
        else:
            mean = minimum
            max_threshold = maximum

        return TIMER_MSG % {
                            'prefix': self.timers_prefix,
                            'key': metric,
                            'mean': mean,
                            'max': maximum,
                            'min': minimum,
                            'count': count,
                            'max_threshold': max_threshold,
                            'pct_threshold': self.pct_threshold,
                            'ts': ts,
                             }

    def render_metrics(self):
        """
        collect rendered stats
        """

        render_stats = {}
        stat_string = ''

        self_stats = ['metrics.discarded',
                      'metrics.batch.discarded',
                      'metrics.batch.count']

        for stat in self_stats:
            render_stats[stat] = self.stats[stat]

        render_start = time.time()
        ts = int(time.time())

        #get self properties, if we have a matching 'render_<property>'
        #method then call the method for each key in property
        for prop in vars(self):
            render = 'render_' + prop
            metric_dict = getattr(self, prop)
            if render in self.methods:
                start = time.time()
                new = metric_dict.copy()
                metric_dict.clear()
                render_stats['render.' + str(prop) + '.count'] = len(new)
                for k, v in new.items():
                    if v is not None:
                        rendered_metric = self.methods[render](k, v, ts)

                        #d_metric comes in as '' from option parser
                        #this causes false positivies
                        #and it is quicker to fail the "is debug?" portion
                        #before inspecting d_metric for normal operation
                        if self.debug and self.d_metric:
                            if self.d_metric in rendered_metric:
                                self.log("match: %s" % (rendered_metric.strip('\n'),))

                        stat_string += rendered_metric

                total_time = time.time() - start
                if total_time < 0.001:
                    #indicates a time so small it will move decimal
                    #.0001 becomes 1.0-4 causing confusion
                    total_time = 0

                render_stats['render.' + str(prop) + '.duration'] = total_time

        render_stop = time.time()
        render_stats['render.time'] = render_stop - render_start
        render_stats['last_publish.duration'] = self.stats['last_publish.duration']
        for k, v in render_stats.items():
            stat_string += "%s.%s %s %s\n" % (self.metadata_prefix, k, v, ts)
            self.stats[k] = v
            if self.debug:
                self.log("%s: %s" % (str(k), str(v)))

        self.stats['render.total'] = len(filter(bool, stat_string.split('\n')))
        stat_string += "%s.numStats %s %d\n" % (self.metadata_prefix,
                                            self.stats['render.total'],
                                            time.time())
        return stat_string

    def flush(self):
        """
        try to flush out stats
        ideally a dead flush will not sink the whole ship
        it's best to catch stderr w/ runit
        """
        flush_start = time.time()
        try:
            stats = self.render_metrics()
            self._publish(stats)
            #reset per flush self stats
            self.stats['pending_stats'] = 0
            self.stats['metrics.batch.count'] = 0
        except Exception, e:
            self.stats['flush_errors'] += 1
            self.last_crit_error = time.time()
            print >> sys.stderr, "%s: %s" % (time.time(), str(e))
            print >> sys.stderr, "%s" % (traceback.format_exc())
        finally:
            self.stats['flush_duration'] = time.time() - flush_start

    def _publish(self, metrics):
        """Send stats to graphite"""
        publish_start = time.time()
        pub_status = 'unknown'
        try:
            graphite = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            graphite.settimeout(5)
            graphite.connect((self.destination, self.graphite_port))
            graphite.settimeout(30)
            graphite.sendall(metrics)
            pub_status = 'success'
        except socket.error, e:
            self.last_crit_error = time.time()
            self.stats['publish_error.socket'] += 1
            pub_status = 'fail'
            self.log("Error communicating with Graphite: %s" % str(e))
        finally:
            graphite.close()

        publish_duration = time.time() - publish_start
        self.stats['last_publish.duration'] = publish_duration
        self.stats['last_publish.time'] = publish_start
        self.log("\nPublish to %s: %s (%ds). Count %d @ %s" \
                % (self.graphite_host,
                           pub_status,
                           publish_duration,
                           self.stats['render.total'],
                           str(publish_start)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--alert', dest='alert', action='store_true', help='alert mode', default=False)
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='debug mode', default=False)
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true', help='quiet mode', default=False)
    parser.add_argument('-n', '--name', dest='name', help='hostname to run on ', default='')
    parser.add_argument('-m', '--debug_metric', dest='d_metric', help='metric to match for debug output', default='')
    parser.add_argument('-p', '--port', dest='port', help='port to run on (default: 8125)', type=int, default=8125)
    parser.add_argument('--graphite-port', dest='graphite_port', help='port to connect to graphite on (default: 2003)', type=int, default=2003)
    parser.add_argument('--graphite-host', dest='graphite_host', help='host to connect to graphite on (default: localhost)', type=str, default='localhost')
    parser.add_argument('--flush-interval', dest='flush_interval', help='how often to send data to graphite in seconds (default: 10)', type=int, default=10)
    parser.add_argument('--metadata-prefix', dest='metadata_prefix', help='prefix to append for data we flush about statsd internals (default: statsd)', type=str, default='statsd')
    parser.add_argument('--sets-prefix', dest='sets_prefix', help='prefix to append before sending set data to graphite (default: stats)', type=str, default='stats')
    parser.add_argument('--counters-prefix', dest='counters_prefix', help='prefix to append before sending counter data to graphite (default: stats)', type=str, default='stats')
    parser.add_argument('--timers-prefix', dest='timers_prefix', help='prefix to append before sending timing data to graphite (default: stats.timers)', type=str, default='stats.timers')
    parser.add_argument('--gauge-prefix', dest='gauge_prefix', help='prefix to append before sending gauge data to graphite (default: stats)', type=str, default='stats')
    parser.add_argument('-t', '--pct', dest='pct', help='stats pct threshold (default: 90)', type=int, default=90)
    options = parser.parse_args(sys.argv[1:])

    stats = StatsHandler(pct_threshold=options.pct,
                        debug=options.debug,
                        quiet=options.quiet,
                        flush_interval=options.flush_interval,
                        d_metric=options.d_metric,
                        graphite_host=options.graphite_host,
                        graphite_port=options.graphite_port,
                        metadata_prefix=options.metadata_prefix,
                        gauge_prefix=options.gauge_prefix,
                        counters_prefix=options.counters_prefix,
                        sets_prefix=options.sets_prefix,
                        timers_prefix=options.timers_prefix)

    def alert():

        errors = ['flush_errors',
                  'publish_error.socket',
                  'publish_error.unknown']

        for e in errors:
            if stats.stats[e]:
                status = 'WARN'
            stats.stats[e] = 0
        else:
            status = 'OK'

        try:
            info = "%s (%ds) @ %s" % (stats.stats['render.total'],
                                      stats.stats['last_publish.duration'],
                                      stats.stats['last_publish.time'])
        except KeyError, e:
            info = str(e)

        nsca_notify(status, options.metadata_prefix, info)

    if options.alert:
        alerttimer = LoopingCall(alert)
        alerttimer.start(60)
    flushtimer = LoopingCall(stats.flush)
    flushtimer.start(options.flush_interval)
    reactor.listenTCP(options.port + 10, server.Site(stats))
    reactor.listenTCP(options.port, Factory(stats_engine=stats))
    reactor.listenUDP(options.port, StatsUDPEngine(stats))
    reactor.run()

if __name__ == '__main__':
    main()

