import logging
import os
import tarfile
import traceback
from pathlib import Path
from shutil import copytree, rmtree

import bagit
import boto3
from asnake.aspace import ASpace
from asnake.utils import find_closest_value
from aws_assume_role_lib import assume_role
from dateutil import parser, relativedelta

logging.basicConfig(
    level=int(os.environ.get('LOGGING_LEVEL', logging.INFO)),
    format='%(filename)s::%(funcName)s::%(lineno)s %(message)s')
logging.getLogger("bagit").setLevel(logging.ERROR)


class Packager(object):

    def __init__(self, region, role_arn, ssm_parameter_path, refid,
                 rights_ids, tmp_dir, source_dir, destination_bucket, sns_topic):
        self.region = region
        self.role_arn = role_arn
        self.refid = refid
        self.rights_ids = [r.strip() for r in rights_ids.split(',')]
        self.tmp_dir = tmp_dir
        self.source_dir = source_dir
        self.destination_bucket = destination_bucket
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
            config = self.get_config(self.ssm_parameter_path)
            self.as_client = ASpace(
                baseurl=config.get('AS_BASEURL'),
                username=config.get('AS_USERNAME'),
                password=config.get('AS_PASSWORD')
            ).client
            self.as_repo = config.get('AS_REPO')
            self.move_to_tmp(bag_dir)
            self.create_bag(bag_dir, self.rights_ids)
            compressed_path = self.compress_bag(bag_dir)
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
        """Gets Boto3 client which authenticates with a specific IAM role."""
        session = boto3.Session()
        assumed_role_session = assume_role(session, role_arn)
        return assumed_role_session.client(resource)

    def move_to_tmp(self, dest_dir):
        """Moves files from source directory into temporary directory

        Returns:
            dest_dir (Pathlib.Path instances): destination directory of files.
        """
        source_dir = Path(self.source_dir, self.refid)
        copytree(source_dir, dest_dir)

    def uri_from_refid(self, refid):
        """Uses the find_by_id endpoint in AS to return the URI of an archival object."""
        find_by_refid_url = f"repositories/{self.as_repo}/find_by_id/archival_objects?ref_id[]={refid}"
        resp = self.as_client.get(find_by_refid_url)
        resp.raise_for_status()
        results = resp.json()
        if len(results.get("archival_objects")) == 1:
            return results['archival_objects'][0]['ref']
        else:
            raise Exception("{} results found for search {}. Expected one result.".format(
                len(results.get("archival_objects")), find_by_refid_url))

    def get_date_range(self, dates_array):
        """Gets maximum and minimum dates from an AS date array.

        Args:
            dates (list of dicts): ArchivesSpace date list

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

    def create_bag(self, bag_dir, rights_ids):
        """Creates a BagIt bag from a directory.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
            rights_ids (list): List of rights IDs to apply to the package.
        """
        obj_uri = self.uri_from_refid(bag_dir.name)
        start_date, end_date = self.get_date_range(
            find_closest_value(obj_uri, 'dates', self.as_client))
        formatted_start_date, formatted_end_date = self.format_aspace_date(
            start_date, end_date)
        metadata = {
            'ArchivesSpace-URI': obj_uri,
            'Start-Date': formatted_start_date,
            'End-Date': formatted_end_date,
            'Origin': 'digitization',
            'Rights-ID': rights_ids,
            'BagIt-Profile-Identifier': 'zorya_bagit_profile.json'}
        bagit.make_bag(bag_dir, metadata)
        logging.debug(
            f'Bag created from {bag_dir} with Rights IDs {rights_ids}.')

    def compress_bag(self, bag_dir):
        """Creates a compressed archive file from a bag.

        Args:
            bag_dir (pathlib.Path): directory containing local files.

        Returns:
            compressed_path (pathlib.Path): path of compressed archive.
        """
        compressed_path = Path(f"{bag_dir}.tar.gz")
        with tarfile.open(str(compressed_path), "w:gz") as tar:
            tar.add(bag_dir, arcname=Path(bag_dir).name)
        rmtree(bag_dir)
        logging.debug(f'Compressed bag {compressed_path} created.')
        return compressed_path

    def deliver_package(self, package_path):
        """Delivers packaged files to destination.

        Args:
            package_path (pathlib.Path): path of compressed archive to upload.
        """
        client = self.get_client_with_role('s3', self.role_arn)
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True)
        client.upload_file(
            package_path,
            self.destination_bucket,
            package_path.name,
            ExtraArgs={'ContentType': 'application/gzip'},
            Config=transfer_config)
        package_path.unlink()
        logging.debug('Packaged delivered.')

    def cleanup_successful_job(self):
        """Remove artifacts from successful job."""
        rmtree(Path(self.source_dir, self.refid))
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
                    'StringValue': f'{str(exception)}\n\n<pre>{tb}</pre>',
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
    source_dir = os.environ.get('SOURCE_DIR')
    destination_bucket = os.environ.get('AWS_DESTINATION_BUCKET')
    sns_topic = os.environ.get('AWS_SNS_TOPIC')
    ssm_parameter_path = f"/{os.environ.get('ENV')}/{os.environ.get('APP_CONFIG_PATH')}"

    Packager(
        region,
        role_arn,
        ssm_parameter_path,
        refid,
        rights_ids,
        tmp_dir,
        source_dir,
        destination_bucket,
        sns_topic).run()
