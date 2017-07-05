# -*- coding: utf-8 -*-
#
# This file is part of Glances.
#
# Copyright (C) 2017 Nicolargo <nicolas@nicolargo.com>
#
# Glances is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Glances is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Mongodb interface class."""

import sys
from time import time

import socket

from glances.logger import logger
from glances.exports.glances_export import GlancesExport
from pymongo import MongoClient, MongoReplicaSetClient


class Export(GlancesExport):

    """This class manages the Mongodb export module."""

    def __init__(self, config=None, args=None):
        """Init the Mongodb export IF."""
        super(Export, self).__init__(config=config, args=args)

        # Mandatories configuration keys (additional to host and port)
        self.db = None

        # Optionals configuration keys
        self.user = None
        self.password = None

        # Load the Cassandra configuration file section
        self.export_enable = self.load_conf('mongodb',
                                            mandatories=['replica_flag', 'db'],
                                            options=['host', 'port', 'replica_connection', 'replicaSet', 'user', 'password'])
        if not self.export_enable:
            sys.exit(2)

        # Init the Mongodb client
        self.client = self.init()

    def init(self):
        """Init the connection to the Mongodb server."""
        if not self.export_enable:
            return None

        if self.user is None:
            if self.replica_flag is None or not self.replica_flag or self.replica_flag.upper() == "FALSE":
                server_uri = 'mongodb://{}:{}/'.format(self.host, self.port)
            else:
                server_uri = 'mongodb:{}'.format(self.replica_connection)
        else:
            if self.replica_flag is None or not self.replica_flag or self.replica_flag.upper() == "FALSE":
                server_uri = 'mongodb://{}:{}@{}:{}/'.format(self.user,
                                                             self.password,
                                                             self.host,
                                                             self.port)
            else:
                server_uri = 'mongodb://{}:{}@{}/'.format(self.user,
                                                          self.password,
                                                          self.replica_connection)

        try:
            if self.replica_flag is None or not self.replica_flag or self.replica_flag.upper() == "FALSE":
                s = MongoClient(self.host, int(self.port))
            else:
                s = MongoReplicaSetClient(self.replica_connection, replicaSet=self.replicaSet)

        except Exception as e:
            logger.critical("Cannot connect to Mongodb server %s (%s)" % (server_uri, e))
            sys.exit(2)
        else:
            logger.info("Connected to the Mongodb server %s" % server_uri)

        try:
            s[self.db]
        except Exception as e:
            # Database did not exist
            # Create it...
            s.create(self.db)
        else:
            logger.info("There is already a %s database" % self.db)

        return s

    def database(self):
        """Return the Mongodb database object"""
        return self.client[self.db]

    def export(self, name, columns, points):
        """Write the points to the Mongodb server."""
        logger.debug("Export {} stats to Mongodb".format(name))

        # Create DB input
        data = dict(zip(columns, points))

        # Set the type to the current stat name
        data['type'] = name
        data['time'] = time()
        data['hostname'] = socket.gethostbyname()

        # Write input to the Mongodb database 'glances_stats' collection
        try:
            self.client[self.db].glances_stats.save(data)
        except Exception as e:
            logger.error("Cannot export {} stats to Mongodb ({})".format(name, e))
