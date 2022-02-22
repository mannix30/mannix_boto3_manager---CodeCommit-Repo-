import logging
import sys
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s",
)
log = logging.getLogger()


# List Log Groups and Log Streams
def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client("logs", region_name=region_name)
    params = (
        {
            "logGroupNamePrefix": group_name,
        }
        if group_name
        else {}
    )
    res = cwlogs.describe_log_groups(**params)
    return res["logGroups"]


# List Log Group Streams
def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client("logs", region_name=region_name)
    params = (
        {
            "logGroupName": group_name,
        }
        if group_name
        else {}
    )
    if stream_name:
        params["logStreamNamePrefix"] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res["logStreams"]


# Filter Log Events
def filter_log_events(group_name, filter_pat, start=None, stop=None, region_name=None):
    cwlogs = boto3.client("logs", region_name=region_name)
    params = {
        "logGroupName": group_name,
        "filterPattern": filter_pat,
    }
    if start:
        params["startTime"] = start
    if stop:
        params["endTime"] = stop
    res = cwlogs.filter_log_events(**params)
    return res["events"]


def main(args):
    if hasattr(args, "func"):

        if args.func.__name__ == "list_log_groups":
            args.func(args.group, args.region)

        elif args.func.__name__ == "list_log_group_streams":
            args.func(args.group, args.stream, args.region)

        elif args.func.__name__ == "filter_log_events":
            args.func(args.group, args.filter_pat, args.start, args.stop, args.region)

        else:
            print("Invalid/Missing command")
            sys.exit(1)

    print("Done")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(title="Cloudwatch Log Actions")

    # ---Subparser for listing log groups
    sp_list_log_groups = sp.add_parser("list_log_groups", help="List log groups")
    sp_list_log_groups.add_argument("--group", help="Group Name")
    sp_list_log_groups.add_argument("--region", help="Region Name")
    sp_list_log_groups.set_defaults(func=list_log_groups)

    # ---Subparser for list log group streams---
    # _______________List log groups and log streams_______________
    sp_list_log_group_streams = sp.add_parser(
        "list_log_groups_streams", help="List log group streams"
    )
    sp_list_log_group_streams.add_argument("group", help="Group Name")
    sp_list_log_group_streams.add_argument("--stream", help="Stream Name")
    sp_list_log_group_streams.add_argument("--region", help="Region Name")
    sp_list_log_group_streams.set_defaults(func=list_log_group_streams)

    # _______________Filter log events_______________
    sp_filter_log_events = sp.add_parser(
        "filter_log_events", help="Output a filtered log events"
    )
    sp_filter_log_events.add_argument("group", help="Group Name")
    sp_filter_log_events.add_argument("filter_pat", help="Pattern for filtering logs")
    sp_filter_log_events.add_argument(
        "--start", help="Filtering logs beginning from this time"
    )
    sp_filter_log_events.add_argument(
        "--stop", help="Filtering logs ending at this time"
    )
    sp_filter_log_events.add_argument("region", help="Region Name")

    args_ = parser.parse_args()
    main(args_)
