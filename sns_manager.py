import logging
import sys
import boto3


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s",
)
log = logging.getLogger()


# Create SNS Topic
def create_sns_topic(topic_name):
    sns = boto3.client("sns")
    sns.create_topic(Name=topic_name)
    log.info(f"SNS topic created: {topic_name}")
    return True


# List all SNS Topics
def list_sns_topics(next_token=None):
    sns = boto3.client("sns")
    params = {"NextToken": next_token} if next_token else {}
    topics = sns.list_topics(**params)
    log.info(f" {topics} \n -----Listed all SNS topics-----")
    return topics.get("Topics", []), topics.get("NextToken", None)


# List all SNS subscriptions
def list_sns_subscriptions(next_token=None):
    sns = boto3.client("sns")
    params = {"NextToken": next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    log.info(subscriptions)
    print(subscriptions.get("Subscriptions", []))
    return (subscriptions.get("Subscriptions", []),)


# Subscribe to an SNS Topic
def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client("sns")
    params = {
        "TopicArn": topic_arn,
        "Protocol": "sms",
        "Endpoint": mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    log.info(f"Mobile number {mobile_number} now subscribed to {topic_arn}")
    return True


# Send an SNS Message
def send_sns_message(topic_arn, message):
    sns = boto3.client("sns")
    params = {
        "TopicArn": topic_arn,
        "Message": message,
    }
    res = sns.publish(**params)
    print(res)
    log.info(f"Message sent from {topic_arn}")
    return True


# Unsubscribe to an SNS Topic
def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client("sns")
    params = {
        "SubscriptionArn": subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    log.info(f"Unsubscribed: {subscription_arn}")
    return True


# Delete an SNS Topic(This will delete the topic and all it's subscriptions.)
def delete_sns_topic(topic_arn):
    sns = boto3.client("sns")
    sns.delete_topic(TopicArn=topic_arn)
    log.info(f"SNS Topic deleted: {topic_arn}")
    return True


def main(args_):
    if hasattr(args_, "func"):
        if args_.func.__name__ == "create_sns_topic":
            args_.func(args_.topic_name)
        elif args_.func.__name__ == "list_sns_topics":
            args_.func(args_.next_token)
        elif args_.func.__name__ == "list_sns_subscriptions":
            args_.func(args_.next_token)
        elif args_.func.__name__ == "subscribe_sns_topic":
            args_.func(args_.topic_arn, args_.mobile_number)
        elif args_.func.__name__ == "send_sns_message":
            args_.func(args_.topic_arn, args_.message)
        elif args_.func.__name__ == "unsubscribe_sns_topic":
            args_.func(args_.subscription_arn)
        elif args_.func.__name__ == "delete_sns_topic":
            args_.func(args_.topic_arn)
        else:
            logging.error("Invalid command!")
            sys.exit(1)
        logging.info("Done!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(title="Commands")

    # _______________Create topic subcommand_______________
    sp_create_topic = sp.add_parser(
        "create_sns_topic",
        help="Create an SNS Topic",
    )
    sp_create_topic.add_argument(
        "topic_name",
        help="Name of topic to be created",
    )
    sp_create_topic.set_defaults(func=create_sns_topic)

    # _______________List topics subcommand_______________
    sp_list_topic = sp.add_parser(
        "list_sns_topics",
        help="List all SNS topics",
    )
    sp_list_topic.add_argument(
        "next_token",
        help="Next token (default:None)",
        nargs="?",
        default=None,
    )
    sp_list_topic.set_defaults(func=list_sns_topics)

    # _______________List subscriptions subcommand_______________
    sp_list_subs = sp.add_parser(
        "list_sns_subscriptions",
        help="List all SNS Subscriptions",
    )
    sp_list_subs.add_argument(
        "next_token",
        help="Next token (default:None)",
        nargs="?",
        default=None,
    )
    sp_list_subs.set_defaults(func=list_sns_subscriptions)

    # _______________Subscribe topic subcommand_______________
    sp_subscribe_topic = sp.add_parser(
        "subscribe_sns_topic",
        help="Subscribe to SNS Topic",
    )
    sp_subscribe_topic.add_argument(
        "topic_arn",
        help="SNS Topic ARN",
    )
    sp_subscribe_topic.add_argument(
        "mobile_number",
        help="Mobile number to be subscribed",
    )
    sp_subscribe_topic.set_defaults(func=subscribe_sns_topic)

    # _______________Publish message to a topic subcommand________________
    sp_send_message = sp.add_parser(
        "send_sns_message",
        help="Publish a message topic",
    )
    sp_send_message.add_argument(
        "topic_arn",
        help="SNS Topic ARN",
    )
    sp_send_message.add_argument(
        "message",
        help="Message to be published",
    )
    sp_send_message.set_defaults(func=send_sns_message)

    # _______________Unsubscribe to a topic subcommand______________
    sp_unsubscribe = sp.add_parser(
        "unsubscribe_sns_topic",
        help="Unsubscribe to an SNS Topic",
    )
    sp_unsubscribe.add_argument(
        "subscription_arn",
        help="Subscription ARN",
    )
    sp_unsubscribe.set_defaults(func=unsubscribe_sns_topic)

    # _______________Delete a topic_______________
    sp_delete_topic = sp.add_parser(
        "delete_sns_topic",
        help="Delete an SNS Topic",
    )
    sp_delete_topic.add_argument(
        "topic_arn",
        help="SNS Topic ARN",
    )
    sp_delete_topic.set_defaults(func=delete_sns_topic)

    args_ = parser.parse_args()
    main(args_)
