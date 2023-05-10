#!/usr/bin/python3
import boto3
import argparse
import dateutil
from datetime import datetime



client = boto3.client('logs')

def get_log_events(log_group,stream_names,start_timestamp=None,end_timestamp=None):
    """List the first 10000 log events from a CloudWatch group.

    :param log_group: Name of the CloudWatch log group.

    """
    if(start_timestamp and end_timestamp):
        resp = client.filter_log_events(logGroupName=log_group, limit=10000,logStreamNames=stream_names,startTime=start_timestamp,endTime=end_timestamp)
    elif(start_timestamp):
        print("Using start timestamp")
        resp = client.filter_log_events(logGroupName=log_group, limit=10000,logStreamNames=stream_names,startTime=start_timestamp)
    elif(end_timestamp):
        resp = client.filter_log_events(logGroupName=log_group, limit=10000,logStreamNames=stream_names,endTime=end_timestamp)
    else:
        resp = client.filter_log_events(logGroupName=log_group, limit=10000,logStreamNames=stream_names)
    results = resp['events'] 
    print(f"{log_group} -- {stream_names}")
    for r in results:
        print(r['message'])
    return results


def get_event_streams(group_name):
    stream_names = []
    streams = client.describe_log_streams(logGroupName=group_name)                    
    for stream_name in streams['logStreams']:
        stream_names.append(stream_name['logStreamName'])
    return stream_names

def get_event_groups():
    groups = []
    paginator = client.get_paginator('describe_log_groups')
    for page in paginator.paginate():
        for group in page['logGroups']:
            group_name = group['logGroupName']
            groups.append(group_name)
    return groups

       


parser = argparse.ArgumentParser(prog="logdumper", description='Cloudwatch log dumper')
parser.add_argument('-a','--all', action='store_true')
parser.add_argument('-g','--group')
parser.add_argument('-s','--stream')
parser.add_argument("-qg",'--querygroups', action='store_true')
parser.add_argument('-st','--startTime')
parser.add_argument('-et','--endTime')
args = parser.parse_args()
        
if args.all:
    groups = get_event_groups()
    for g in groups:
        streams = get_event_streams(g)
        for s in streams:
            get_log_events(g,s)

elif args.group and not args.stream:
    streams = get_event_streams(args.group)
    print("Streams Found:")
    for s in streams:
        print(f"\t{s}")

    if len(streams) == 0:
        print("\tNone :(")
elif args.querygroups:
    groups = get_event_groups()
    print("Groups Found:")
    for g in groups:
        print(f"\t{g}")

    if len(groups) == 0:
        print("\tNone :(")

elif args.group and args.stream:    
    start_timestamp = None
    end_timestamp = None
    if args.startTime:
        start_date = dateutil.parser.parse(args.startTime)
        print(start_date)
        
        start_timestamp = int(datetime.timestamp(start_date))*1000 #ms not s

    if args.endTime:
        end_date = dateutil.parser.parse(args.endTime)
        end_timestamp = int(datetime.timestamp(end_date))*1000 #ms not s
        

    results = get_log_events(args.group,[args.stream],start_timestamp=start_timestamp,end_timestamp=end_timestamp)
    print(start_timestamp)
else:
    parser.print_help()
    
