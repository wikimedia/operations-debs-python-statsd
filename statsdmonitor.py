#!/usr/bin/env python

#Connects to statsd and pulls internal statistics

import sys
import socket
import time
import json
import sys
import xmlrpclib
import datetime


class StatsClient(object):

    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)

    def stats(self):
        proxy = xmlrpclib.Server('http://%s:%s/' % (self.ip, self.port))
        return json.loads(proxy.istats())


def main():
    usage = """
     Takes two arguments: <statsd ip> <statsd port>
        """
    if len(sys.argv) < 3:
        print usage
        sys.exit(1)

    statengine = StatsClient(sys.argv[1], sys.argv[2])
    stats = statengine.stats()
    for key in sorted(stats.iterkeys()):
        print "%s: %s" % (key, stats[key])
main()
