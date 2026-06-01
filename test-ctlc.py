#!/usr/bin/env python3.6

# -*- coding: utf-8 -*-
import sys
import os
import argparse
import configparser


# When installed, the lib package lives under the FHS data dir while this
# entry point is in /opt/vasexperts/bin. Make the lib package importable from
# there. When run from a source checkout the repo root is already on sys.path,
# so this insert is a harmless no-op (the dir simply may not exist).
LIB_DIR = os.environ.get("TEST_CTLC_LIB", "/opt/vasexperts/var/lib/test-ctlc")
if LIB_DIR not in sys.path:
    sys.path.insert(0, LIB_DIR)

DEFAULT_CONFIG = "/opt/vasexperts/etc/test-ctlc/config.conf"

import lib.version as version
import lib.processor as processor
import lib.logger as logger
from lib.logger import logging









def parse_args():
    parser = argparse.ArgumentParser(prog="test-ctlc")
    parser.add_argument('-f', '--file')

    subparsers = parser.add_subparsers(dest='command')

    subparsers.add_parser('version', add_help=True, description="Print version")
    subparsers.add_parser('list', add_help=True, description="List available scenarios")


    info_parser = subparsers.add_parser('info', add_help=True, description="Info about scenarios")
    info_parser.add_argument('-n', '--name', metavar="NAME", action="append", help="show info about scenarios with the specified name, repeat option for many names")

    run_parser = subparsers.add_parser('run', add_help=True, description="Run scenarios")
    run_parser.add_argument('-f', '--file', metavar="FILE", nargs=1, default=None, help="file name")


    return parser.parse_args()



def main():
    args = parse_args()
    if args.command == 'version':
        print("{0}-{1}".format(version.version, version.release))
        ...
    elif args.command == 'list':
        ...
    elif args.command == 'info':
        ...
    elif args.command == 'run':

        try:
            config_path = args.file[0] if isinstance(args.file, list) else args.file
            if not config_path:
                config_path = DEFAULT_CONFIG
            config = configparser.ConfigParser()
            config.read(config_path)
            logger.initialize(config['main']['log_path'], config['main']['log_level'])
            processor.run(config)

        except Exception as e:
            print(f"Error{e}")
            return
    else:
        print("Please run \"python3 {0} --help\" for short description".format(sys.argv [0]))


#
#
if __name__ == "__main__":
    main()

