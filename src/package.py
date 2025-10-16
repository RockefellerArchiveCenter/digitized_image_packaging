import json
import logging
import os
import tarfile
import traceback
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

import bagit
import boto3
import botocore
import shortuuid
from asnake.aspace import ASpace
from asnake.utils import find_closest_value
from aws_assume_role_lib import assume_role
from dateutil import parser, relativedelta

from .clients import AquilaClient

logging.basicConfig(
    level=int(os.environ.get('LOGGING_LEVEL', logging.INFO)),
    format='%(filename)s::%(funcName)s::%(lineno)s %(message)s')
logging.getLogger("bagit").setLevel(logging.ERROR)


class Packager(object):

    def __init__(self, region, role_arn, ssm_parameter_path, refid,
                 rights_ids, tmp_dir, source_bucket, destination_bucket,
                 pdf_destination_bucket, embargoed_destination_bucket,
                 embargoed_pdf_destination_bucket, sns_topic):
        self.region = region
        self.role_arn = role_arn
        self.refid = refid
        self.rights_ids = [r.strip() for r in rights_ids.split(',')]
        self.tmp_dir = tmp_dir
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.embargoed_destination_bucket = embargoed_destination_bucket
        self.pdf_destination_bucket = pdf_destination_bucket
        self.embargoed_pdf_destination_bucket = embargoed_pdf_destination_bucket
        self.sns_topic = sns_topic
        self.ssm_parameter_path = ssm_parameter_path
        self.service_name = 'digitized_image_packaging'
        if not Path(self.tmp_dir).is_dir():
            Path(self.tmp_dir).mkdir(parents=True)
        logging.debug(self.__dict__)

    def run(self):
        """Main method, which calls all other methods."""
        logging.debug(
            f'Packaging started for package {self.refid}.')
        try:
            bag_dir = Path(self.tmp_dir, self.refid)
            bag_identifier = str(uuid4())
            config = self.get_config(self.ssm_parameter_path)
            aquila_client = AquilaClient(config.get('AQUILA_BASEURL'))
            self.as_client = ASpace(
                baseurl=config.get('AS_BASEURL'),
                username=config.get('AS_USERNAME'),
                password=config.get('AS_PASSWORD')
            ).client
            self.as_repo = config.get('AS_REPO')
            as_uri = self.uri_from_refid(bag_dir.name)
            as_data = self.get_as_data(as_uri)
            rights_data = aquila_client.get_rights_data(
                self.rights_ids, as_data)
            self.is_embargoed = self.has_embargo(rights_data)
            self.move_to_tmp()
            self.deliver_pdf(as_uri)
            self.create_bag(bag_dir, self.rights_ids, as_data)
            if self.is_embargoed:
                compressed_path = self.compress_embargoed_bag(bag_dir)
            else:
                bag_json = self.get_bag_json(
                    bag_identifier, as_data['display_string'], rights_data)
                compressed_path = self.compress_bag(
                    bag_identifier, bag_dir, bag_json)
            self.deliver_package(compressed_path)
            self.cleanup_successful_job()
            self.deliver_success_notification()
            logging.info(
                f'Package {self.refid} successfully packaged.')
        except Exception as e:
            logging.exception(e)
            self.cleanup_failed_job(bag_dir)
            self.deliver_failure_notification(e)

    def get_client_with_role(self, resource, role_arn):
        """Gets Boto3 client which authenticates with a specific IAM role.

        Args:
            resource (str): AWS resource client should be associated with.
            role_arn (str): ARN for assumed role session.

        Returns:
            client (boto3.Client): client with assumed role session.
        """
        session = boto3.Session()
        assumed_role_session = assume_role(session, role_arn)
        return assumed_role_session.client(resource)

    def get_as_data(self, as_uri):
        """Fetches data from ArchivesSpace.

        Args:
            as_uri (str): URI for archival object in ArchivesSpace.

        Returns:
            as_data (dict): formatted data from ArchivesSpace.
        """
        ao = self.as_client.get(as_uri).json()
        start_date, end_date = self.get_date_range(
            find_closest_value(ao, 'dates', self.as_client))
        formatted_start_date, formatted_end_date = self.format_aspace_date(
            start_date, end_date)
        return {
            "start_date": formatted_start_date,
            "end_date": formatted_end_date,
            "display_string": ao['display_string'],
            "uri": as_uri
        }

    def get_download_path(self, current_path):
        if 'master_edited' in current_path:
            new_path = current_path.replace('master_edited', 'service')
        else:
            new_path = current_path.replace('master/', '')
        return new_path

    def move_to_tmp(self):
        """Copies files from source bucket into temporary directory."""
        client = self.get_client_with_role('s3', self.role_arn)
        paginator = client.get_paginator('list_objects_v2')
        for prefix in [f'{self.refid}/master', f'{self.refid}/master_edited']:
            pages = paginator.paginate(
                Bucket=self.source_bucket,
                Prefix=prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        new_path = self.get_download_path(obj['Key'])
                        destination_path = Path(self.tmp_dir, new_path)
                        destination_path.parent.mkdir(
                            parents=True, exist_ok=True)
                        client.download_file(
                            self.source_bucket, obj['Key'], str(destination_path))

    def uri_from_refid(self, refid):
        """Uses the find_by_id endpoint in AS to return the URI of an archival object.

        Args:
            refid (str): refid for an archival object in ArchivesSpace.

        Returns:
            as_uri (str): URI for archival object matching refid.
        """
        find_by_refid_url = f"repositories/{
            self.as_repo}/find_by_id/archival_objects?ref_id[]={refid}"
        resp = self.as_client.get(find_by_refid_url)
        resp.raise_for_status()
        results = resp.json()
        if len(results.get("archival_objects")) == 1:
            return results['archival_objects'][0]['ref']
        else:
            raise Exception("{} results found for search {}. Expected one result.".format(
                len(results.get("archival_objects")), find_by_refid_url))

    def get_active_rights_acts(self, acts):
        """Evaluates rights statement act end dates to determine if it is still active.

        Args:
            acts (list): Acts from rights statements.

        Returns:
            acts (list): Acts which are currently active.
        """
        current_date = datetime.now()
        for idx, act in reversed(list(enumerate(acts))):
            if act.get('end_date'):
                statement_end = datetime.strptime(act['end_date'], "%Y-%m-%d")
                if (current_date > statement_end):
                    acts.pop(idx)
        return acts

    def has_embargo(self, rights_statements):
        """Determines if a package is embargoed from online access.

        Args:
            rights_statements (list of dicts): rights data from Aquila.

        Returns:
            embargo (bool): if the package is embargoed from online access.
        """
        for rights_statement in rights_statements:
            for granted in self.get_active_rights_acts(
                    rights_statement['rights_granted']):
                if granted['act'] in [
                        'publish', 'disseminate'] and granted['grant_restriction'] != 'allow':
                    return True
        return False

    def get_date_range(self, dates_array):
        """Gets maximum and minimum dates from an AS date array.

        Args:
            dates_array (list of dicts): ArchivesSpace date list

        Returns:
            start_date (str): earliest date in date list.
            end_date (str): latest date in date list
        """
        start_dates = []
        end_dates = []
        for date in dates_array:
            start_dates.append(date['begin'])
            if date['date_type'] == 'single':
                end_dates.append(date['begin'])
            else:
                end_dates.append(date['end'])
        return sorted(start_dates)[0], sorted(end_dates)[-1]

    def format_aspace_date(self, start_date, end_date):
        """Formats ASpace dates so that they can be parsed by Aquila.
        Assumes beginning of month or year if a start date, and end of month or
        year if an end date.

        Args:
            start_date (str): unformatted start date
            end_date (str): unformatted end date

        Returns:
            formatted_start_date (str): start date in format YYYY-MM-DD
            formatted_start_date (str): end date in format YYYY-MM-DD
        """
        parsed_start = parser.isoparse(start_date)
        parsed_end = parser.isoparse(end_date)
        formatted_start = parsed_start.strftime('%Y-%m-%d')
        if len(end_date) == 4:
            formatted_end = (
                parsed_end + relativedelta.relativedelta(
                    month=12, day=31)).strftime('%Y-%m-%d')
        elif len(end_date) == 7:
            formatted_end = (
                parsed_end + relativedelta.relativedelta(
                    day=31)).strftime('%Y-%m-%d')
        else:
            formatted_end = end_date
        return formatted_start, formatted_end

    def create_bag(self, bag_dir, rights_ids, as_data):
        """Creates a BagIt bag from a directory.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
            rights_ids (list): List of rights IDs to apply to the package.
            as_data (dict): Data from ArchivesSpace.
        """
        metadata = {
            'ArchivesSpace-URI': as_data['uri'],
            'Start-Date': as_data['start_date'],
            'End-Date': as_data['end_date'],
            'Origin': 'digitization',
            'Rights-ID': rights_ids,
            'Title': as_data['display_string'],
            'BagIt-Profile-Identifier': 'zorya_bagit_profile.json'}
        bagit.make_bag(bag_dir, metadata)
        logging.debug(
            f'Bag created from {bag_dir} with Rights IDs {rights_ids}.')

    def get_bag_json(self, identifier, title, rights_data):
        return {
            "identifier": identifier,
            "title": title,
            "origin": 'digitization',
            "rights_statements": rights_data
        }

    def compress_embargoed_bag(self, bag_dir):
        """Creates a compressed archive file from a bag.

        This archive file contains the binary files as a Bagit bag.

         Args:
            bag_dir (pathlib.Path): directory containing local files.

        Returns:
            compressed_path (pathlib.Path): path of compressed archive.
        """
        compressed_path = Path(f"{bag_dir}.tar.gz")
        with tarfile.open(str(compressed_path), "w:gz") as tar:
            tar.add(bag_dir, arcname=self.refid)
        rmtree(bag_dir)
        logging.debug(f'Compressed bag {compressed_path} created.')
        return compressed_path

    def compress_bag(self, bag_identifier, bag_dir, bag_json):
        """Creates a compressed archive file from a bag.

        This archive file contains JSON bag data, as well as another
        archive containing the binary files as a Bagit bag.

        Args:
            bag_identifier (str): newly-minted UUID for the bag.
            bag_dir (pathlib.Path): directory containing local files.
            bag_json (dict): data about the bag to include in a JSON file.

        Returns:
            compressed_path (pathlib.Path): path of compressed archive.
        """
        root_dir = Path(self.tmp_dir, bag_identifier)
        outer_compressed_path = Path(self.tmp_dir, f"{bag_identifier}.tar.gz")
        inner_compressed_path = root_dir / f"{bag_identifier}.tar.gz"
        root_dir.mkdir()
        with tarfile.open(str(inner_compressed_path), "w:gz") as tar:
            tar.add(bag_dir, arcname=bag_identifier)
        with open(Path(root_dir, f"{bag_identifier}.json"), "w") as json_file:
            json.dump(
                bag_json,
                json_file,
                indent=4,
                sort_keys=True,
                default=str)
        with tarfile.open(str(outer_compressed_path), "w:gz") as tar:
            tar.add(root_dir, arcname=bag_identifier)
        rmtree(bag_dir)
        rmtree(root_dir)
        logging.debug(f'Compressed bag {outer_compressed_path} created.')
        return outer_compressed_path

    def upload_file(self, bucket, source_file_path,
                    destination_path, content_type, increment_if_exists):
        """Uploads file to an S3 bucket.

        Args:
            bucket (string): AWS S3 bucket to upload file to.
            source_file_path (pathlib.Path): local file to upload.
            destination_path (string): target key in S3 bucket.
            content_type (string): content type for file to upload.
            increment_if_exists (boolean): check to see if file exist and add incrementing iterator
        """
        client = self.get_client_with_role('s3', self.role_arn)
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True)

        if increment_if_exists:
            try:
                client.head_object(
                    Bucket=bucket,
                    Key=destination_path)
                split_path = destination_path.split('.')
                extension = '.'.join(split_path[1:])
                split_destination = split_path[0].split('_')
                if len(split_destination) == 2:
                    current_iterator = int(split_destination[1])
                    updated_destination = f'{
                        split_destination[0]}_{
                        current_iterator + 1}'
                else:
                    updated_destination = f'{split_destination[0]}_1'
                self.upload_file(
                    bucket,
                    source_file_path,
                    f'{updated_destination}.{extension}',
                    content_type,
                    increment_if_exists)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    client.upload_file(
                        source_file_path,
                        bucket,
                        destination_path,
                        ExtraArgs={'ContentType': content_type},
                        Config=transfer_config)
                else:
                    raise Exception(e)
        else:
            client.upload_file(
                source_file_path,
                bucket,
                destination_path,
                ExtraArgs={'ContentType': content_type},
                Config=transfer_config)
        logging.debug(
            f'Source file {source_file_path} uploaded to {bucket} as {destination_path}')

    def deliver_package(self, package_path):
        """Delivers packaged files to destination.

        Args:
            package_path (pathlib.Path): path of compressed archive to upload.
        """
        destination = self.embargoed_destination_bucket if self.is_embargoed else self.destination_bucket
        self.upload_file(
            destination,
            package_path,
            package_path.name,
            'application/gzip',
            self.is_embargoed)
        package_path.unlink()
        logging.debug(f'Packaged delivered to {destination}.')

    def deliver_pdf(self, as_uri):
        """Delivers PDF file to destination.

        Args:
            as_uri (str): URI for archival object in ArchivesSpace
        """
        client = self.get_client_with_role('s3', self.role_arn)
        pdf_path = Path(self.tmp_dir, f'{self.refid}.pdf')
        client.download_file(
            self.source_bucket,
            f'{self.refid}/service_edited/{self.refid}.pdf',
            str(pdf_path))
        destination = self.embargoed_pdf_destination_bucket if self.is_embargoed else self.pdf_destination_bucket
        target_path = f'{
            self.refid}.pdf' if self.is_embargoed else f'pdfs/{shortuuid.uuid(as_uri)}'
        self.upload_file(
            destination,
            pdf_path,
            target_path,
            'application/pdf',
            self.is_embargoed)
        pdf_path.unlink()
        logging.debug(f'PDF delivered to {destination}.')

    def cleanup_successful_job(self):
        """Remove artifacts from successful job."""
        client = self.get_client_with_role('s3', self.role_arn)
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.source_bucket,
            Prefix=self.refid)

        objects_to_delete = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                response = client.delete_objects(
                    Bucket=self.source_bucket,
                    Delete={'Objects': batch, 'Quiet': True})
                if 'Errors' in response:
                    errors = "\n".join([e["Key"] for e in response["errors"]])
                    raise Exception(f'Error deleting objects: {errors}')
        logging.debug('Cleanup from successful job completed.')

    def cleanup_failed_job(self, bag_dir):
        """Remove artifacts from failed job.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
        """
        if bag_dir.is_dir():
            rmtree(bag_dir)
        Path(f"{bag_dir}.tar.gz").unlink(missing_ok=True)
        logging.debug('Cleanup from failed job completed.')

    def deliver_success_notification(self):
        """Sends notifications after successful run."""
        client = self.get_client_with_role('sns', self.role_arn)
        client.publish(
            TopicArn=self.sns_topic,
            Message=f'Package {self.refid} successfully packaged.',
            MessageAttributes={
                'refid': {
                    'DataType': 'String',
                    'StringValue': self.refid,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'SUCCESS',
                }
            })
        logging.debug('Success notification delivered.')

    def deliver_failure_notification(self, exception):
        """"Sends notifications when run fails.

        Args:
            exception (Exception): the exception that was thrown.
        """
        client = self.get_client_with_role('sns', self.role_arn)
        tb = ''.join(traceback.format_exception(exception)[:-1])
        client.publish(
            TopicArn=self.sns_topic,
            Message=f'Package {self.refid} failed packaging.',
            MessageAttributes={
                'refid': {
                    'DataType': 'String',
                    'StringValue': self.refid,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'FAILURE',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': str(exception),
                },
                'traceback': {
                    'DataType': 'String',
                    'StringValue': tb,
                }
            })
        logging.debug('Failure notification delivered.')

    def get_config(self, ssm_parameter_path):
        """Fetch config values from Parameter Store.

        Args:
            ssm_parameter_path (str): Path to parameters

        Returns:
            configuration (dict): all parameters found at the supplied path.
                The following keys are expected to be present:
                    - AWS_ACCESS_KEY_ID
                    - AWS_SECRET_ACCESS_KEY
                    - AS_BASEURL
                    - AS_REPO
                    - AS_USERNAME
                    - AS_PASSWORD
        """
        client = self.get_client_with_role('ssm', self.role_arn)
        configuration = {}
        param_details = client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_name = param_path_array[-1]
            configuration[section_name] = param.get('Value')

        return configuration


if __name__ == '__main__':
    refid = os.environ.get('REFID')
    rights_ids = os.environ.get('RIGHTS_IDS')
    region = os.environ.get('AWS_REGION')
    role_arn = os.environ.get('AWS_ROLE_ARN')
    tmp_dir = os.environ.get('TMP_DIR')
    source_bucket = os.environ.get('AWS_SOURCE_BUCKET')
    destination_bucket = os.environ.get('AWS_DESTINATION_BUCKET')
    pdf_destination_bucket = os.environ.get('AWS_PDF_DESTINATION_BUCKET')
    embargoed_destination_bucket = os.environ.get(
        'AWS_EMBARGOED_DESTINATION_BUCKET')
    embargoed_pdf_destination_bucket = os.environ.get(
        'AWS_EMBARGOED_PDF_DESTINATION_BUCKET')
    sns_topic = os.environ.get('AWS_SNS_TOPIC')
    ssm_parameter_path = f"/{os.environ.get('ENV')
                             }/{os.environ.get('APP_CONFIG_PATH')}"

    Packager(
        region,
        role_arn,
        ssm_parameter_path,
        refid,
        rights_ids,
        tmp_dir,
        source_bucket,
        destination_bucket,
        pdf_destination_bucket,
        embargoed_destination_bucket,
        embargoed_pdf_destination_bucket,
        sns_topic).run()
