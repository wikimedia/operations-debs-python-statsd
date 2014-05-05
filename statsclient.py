#!/usr/bin/env python

usage = """
     Takes two arguments: <statsd ip> <statsd port>
        """

import socket
import time
import json
import sys

class StatsdClient(object):

        def __init__(self, ip, port):
            self.ip = ip
            self.port = int(port)

        def _send(self, msg, transport='udp'):

            if transport == 'tcp':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
            s.connect((self.ip, self.port))
            try:
                s.sendall(msg)
                if transport == 'tcp':
                    return self.recv_basic(s)
                else:
                    return
            finally:
                s.close()

        def set(self, gauge, value):
            self._publish(gauge, value, 's')

        def timer(self, gauge, value):
            self._publish(gauge, value, 'ms')

        def gauge(self, gauge, value):
            self._publish(gauge, value, 'g')

        def increment(self, gauge, value, samplerate=1):
            self._publish(gauge, value, 'c', samplerate=samplerate)

        def decrement(self, gauge, value, samplerate=1):
            self._publish(gauge, '-' + str(value), 'c')

        def _publish(self, metric, value, type, samplerate=1):
            #valid: metric:value|type
            statstring = metric +  ':' + str(value) + '|' + type
            if samplerate != 1:
                statstring += '|@' + str(samplerate)
            return self._send(statstring)


def main():
    if len(sys.argv) < 3:
        print usage
        sys.exit(1)

    statengine = StatsdClient(sys.argv[1], sys.argv[2])

    import random, time
    rvalue = random.random()
    print time.time(), rvalue
    statengine.increment("test.counter.srpoint5", 1, samplerate=0.2)
    statengine.increment("test.fixed.counter.1", 1)
    statengine.timer("test.nonrandom", 1)
    statengine.timer("test.nonrandom", 2)
    statengine.timer("test.nonrandom", 3)
    statengine.gauge("test.random.gauge", rvalue)
    statengine.timer("test.random.timer", rvalue)
    statengine.set("test.random.set", rvalue)
    statengine.increment("test.random.counter", rvalue)
    statengine.increment("test.fixed.counter.2", 2)
    statengine.increment("test.fixed.counter.updown.1", 1)
    statengine.decrement("test.fixed.counter.updown.1", 1)
    statengine.increment("test.fixed.counter.updown.1_1", 1.1)
    statengine.decrement("test.fixed.counter.updown.1_1", 1)

if  __name__ == '__main__':
    main()
