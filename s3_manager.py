import logging
import uuid
import sys
from pathlib import Path, PosixPath


import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s",
)
log = logging.getLogger()


# Create Bucket
def create_bucket(name, region=None):
    region = region or "ap-southeast-1"
    client = boto3.client("s3", region_name=region)
    params = {
        "Bucket": name,
        "CreateBucketConfiguration": {
            "LocationConstraint": region,
        },
    }

    try:
        client.create_bucket(**params)
        log.info(f"Bucket {name} created")
        return True

    except ClientError as err:
        log.error(f"{err} - Params {params}")
        return False


# List Bucket
def list_buckets():
    s3 = boto3.resource("s3")

    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1

    print(f"Found {count} buckets!")


# Get a bucket
def get_bucket(name, create=False, region=None):
    client = boto3.resource("s3")
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
        log.info(str(bucket) + ":" + str(bucket.creation_date))
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f"Bucket {name} does not exist!")
            return


# Create a temporary text file in a bucket
def create_tempfile(file_name=None, content=None, size=300):
    filename = f"{file_name or uuid.uuid4().hex}.txt"
    with open(filename, "w") as f:
        f.write(f'{(content or "0") * size}')
    return filename


# Create bucket object
def create_bucket_object(bucket_name, file_path, key_prefix=None):
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object


#  Get Bucket Object(Download)
def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    bucket = get_bucket(bucket_name)
    params = {"key": object_key}
    if version_id:
        params["VersionId"] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f"{file_path}")
    return bucket_object, file_path


# Create Bucket object version(Enable bucket versioning)
def enable_bucket_versioning(bucket_name):
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status


# Delete Bucket Objects including all of its versions
def delete_bucket_objects(bucket_name, key_prefix=None):
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()
    targets = []  # This should be a max of 1000
    for obj in objects:
        targets.append(
            {
                "Key": obj.object_key,
                "VersionId": obj.version_id,
            }
        )
    bucket.delete_objects(
        Delete={
            "Objects": targets,
            "Quiet": True,
        }
    )

    return len(targets)


# Delete buckets
def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
        else:
            count = 0
            client = boto3.resource("s3")
            for bucket in client.buckets.iterator():
                try:
                    bucket.delete()
                    bucket.wait_until_not_exists()
                    count += 1
                except ClientError as err:
                    log.warning(f"Bucket {bucket.name}: {err}")
    return count


def main(args_):
    if hasattr(args_, "func"):
        # action =
        if args_.func.__name__ == "create_bucket":
            args_.func(args_.name, args_.region)
        elif args_.func.__name__ == "list_buckets":
            args_.func()
        elif args_.func.__name__ == "get_bucket":
            args_.func(args_.name, args_.create, args_.region)
        elif args_.func.__name__ == "create_tempfile":
            args_.func(args_.file_name, args_.content, args_.size)
        elif args_.func.__name__ == "create_bucket_object":
            args_.func(args_.bucket_name, args_.file_path, args_.key_prefix)
        elif args_.func.__name__ == "get_bucket_object":
            args_.func(
                args_.bucket_name, args_.object_key,
                args_.dest, args_.version_id
            )
        elif args_.func.__name__ == "enable_bucket_versioning":
            args_.func(args_.bucket_name)
        elif args_.func.__name__ == "delete_bucket_objects":
            args_.func(args_.bucket_name, args_.key_prefix)
        elif args_.func.__name__ == "delete_buckets":
            args_.func(args_.name)
        else:
            log.error("Invalid/Missing command.")
            sys.exit(1)
        print("Done")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(title="Commands")

    # _______________Create bucket subcommand______________
    sp_create_bucket = sp.add_parser(
        "create_bucket",
        help="Create an S3 bucket",
    )
    sp_create_bucket.add_argument(
        "name", help="Specify name of the S3 bucket."
    )
    sp_create_bucket.add_argument(
        "region",
        help="Region: None (default)",
        nargs="?",
        default=None,
    )
    sp_create_bucket.set_defaults(func=create_bucket)

    # _______________List buckets subcommand________________
    sp_list_buckets = sp.add_parser(
        "list_buckets",
        help="List all S3 bucket",
    )
    sp_list_buckets.set_defaults(func=list_buckets)

    # _______________Get bucket subcommand_______________
    sp_get_bucket = sp.add_parser(
        "get_bucket",
        help="Download a bucket.",
    )
    sp_get_bucket.add_argument(
        "name",
        help="Specify the name of the bucket",
    )
    sp_get_bucket.add_argument(
        "create", help="Boolean: False (default)", nargs="?", default=False
    )
    sp_get_bucket.add_argument(
        "region", help="Region: None (default)", nargs="?", default=None
    )
    sp_get_bucket.set_defaults(func=get_bucket)

    # _______________Create temporary file subcommand_______________
    sp_create_temp = sp.add_parser(
        "create_tempfile",
        help="Create a temporary text file",
    )
    sp_create_temp.add_argument(
        "file_name", help="Filename: None (default)", nargs="?", default=None
    )
    sp_create_temp.add_argument(
        "content", help="Content: None (default)", nargs="?", default=None
    )
    sp_create_temp.add_argument(
        "size", help="File size: 300 (default", nargs="?", default=None
    )
    sp_create_temp.set_defaults(func=create_tempfile)

    # _______________Create bucket object subcommand_______________
    sp_create_bucket_object = sp.add_parser(
        "create_bucket_object",
        help="Create a bucket object",
    )
    sp_create_bucket_object.add_argument(
        "bucket_name",
        help="Bucket name for the created object",
    )
    sp_create_bucket_object.add_argument(
        "fila_path",
        help="Path of the file to be uploaded to the bucket",
    )
    sp_create_bucket_object.add_argument(
        "key_prefix", help="Key prefix: None (default)",
        nargs="?", default=None
    )
    sp_create_bucket_object.set_defaults(func=create_bucket_object)

    # _______________Get bucket object subcommand_______________
    sp_get_bucket_object = sp.add_parser(
        "get_bucket_object",
        help="Download a bucket object.",
    )
    sp_get_bucket_object.add_argument(
        "bucket_name",
        help="The target bucket",
    )
    sp_get_bucket_object.add_argument(
        "object_key",
        help="The bucket object to get",
    )
    sp_get_bucket_object.add_argument(
        "dest", help="Path for saving object", nargs="?", default=None
    )
    sp_get_bucket_object.add_argument(
        "version_id", help="Version ID: None (default)",
        nargs="?", default=None
    )
    sp_get_bucket_object.set_defaults(func=get_bucket_object)

    # _______________Enable bucket object versioning subcommand_______________
    sp_enable_bucket_versioning = sp.add_parser(
        "enable_bucket_versioning",
        help="Enable bucket versioning for the given bucket_name",
    )
    sp_enable_bucket_versioning.add_argument(
        "bucket_name",
        help="Bucket name",
    )
    sp_enable_bucket_versioning.set_defaults(func=enable_bucket_versioning)

    # _______________Delete bucket objects subcommand_______________
    sp_delete_bucket_objects = sp.add_parser(
        "delete_bucket_objects",
        help="Delete all bucket objects with all its versions",
    )
    sp_delete_bucket_objects.add_argument(
        "bucket_name",
        help="Name of the bucket",
    )
    sp_delete_bucket_objects.add_argument(
        "key_prefix", help="Key Prefix: None (default)",
        nargs="?", default=None
    )
    sp_delete_bucket_objects.set_defaults(func=delete_bucket_objects)

    sp_delete_buckets = sp.add_parser(
        "delete_buckets",
        help="Delete an S3 Bucket.",
    )
    sp_delete_buckets.add_argument(
        "name", help="Bucket name: None (default)", nargs="?", default=None
    )
    sp_delete_buckets.set_defaults(func=delete_buckets)

    # Execute subcommand function
    args_ = parser.parse_args()
    main(args_)
