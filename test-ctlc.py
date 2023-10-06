#!/usr/bin/env python3.6

# -*- coding: utf-8 -*-
import lib.version as version
import lib.processor as processor
import lib.logger as logger
from lib.logger import logging


import sys
import argparse
import configparser









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
            config = configparser.ConfigParser()
            config.read(args.file)
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

