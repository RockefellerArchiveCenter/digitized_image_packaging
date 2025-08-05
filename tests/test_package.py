import json
from pathlib import Path
from shutil import copyfile, copytree, rmtree
from unittest.mock import ANY, DEFAULT, MagicMock, patch

import bagit
import boto3
import pytest
import shortuuid
from asnake.aspace import ASpace
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.package import Packager

from .helpers import MockResponse

ARGS = ['us-east-1',
        'digitized-image-packaging-role-arn',
        '/dev/digitized_image_packaging',
        'b90862f3baceaae3b7418c78f9d50d52',
        '1,2',
        'tmp',
        'source_bucket',
        'destination_bucket',
        'pdf_bucket',
        'embargoed_destination_bucket',
        'embargoed_pdf_bucket',
        'topic']
packager = Packager(*ARGS)


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Fixture to create and tear down tmp dir before and after a test is run"""
    dir_list = [ARGS[5], ARGS[6]]
    for dir in dir_list:
        tmp_dir = Path(dir)
        if not tmp_dir.is_dir():
            tmp_dir.mkdir()

    mock_response = MagicMock()
    mock_response.return_value.text = "v3.0.2"

    with patch.multiple('asnake.client.web_client.ASnakeClient', get=mock_response, authorize=DEFAULT):

        yield  # this is where the testing happens

    for dir in dir_list:
        rmtree(dir)


@mock_aws
@patch('src.package.Packager.get_client_with_role')
def test_get_config(mock_role):
    """Asserts configuration is fetched as expected."""
    packager = Packager(*ARGS)
    ssm = boto3.client('ssm', region_name='us-east-1')
    mock_role.return_value = ssm
    path = "/dev/digitized-image-packaging"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString")
    config = packager.get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}


@patch('src.package.Packager.get_config')
@patch('src.clients.AquilaClient.__init__')
@patch('src.package.Packager.uri_from_refid')
@patch('src.package.Packager.get_as_data')
@patch('src.clients.AquilaClient.get_rights_data')
@patch('src.package.Packager.has_embargo')
@patch('src.package.Packager.move_to_tmp')
@patch('src.package.Packager.create_bag')
@patch('src.package.Packager.get_bag_json')
@patch('src.package.Packager.compress_bag')
@patch('src.package.Packager.compress_embargoed_bag')
@patch('src.package.Packager.deliver_package')
@patch('src.package.Packager.deliver_pdf')
@patch('src.package.Packager.cleanup_successful_job')
@patch('src.package.Packager.deliver_success_notification')
def test_run(mock_notification, mock_cleanup, mock_pdf, mock_deliver, mock_compress_embargoed, mock_compress, mock_bag_json, mock_create,
             mock_move, mock_has_embargo, mock_rights_data, mock_as_data, mock_as_uri, mock_aquila, mock_config):
    """Asserts run method calls other methods."""
    packager = Packager(*ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    aquila_baseurl = 'https://aquila.rockarch.org/api/'
    mock_aquila.return_value = None
    config = {'AQUILA_BASEURL': aquila_baseurl}
    mock_config.return_value = config
    as_uri = '/repositories/2/archival_objects/1'
    mock_as_uri.return_value = as_uri
    rights_data = []
    mock_rights_data.return_value = rights_data
    mock_has_embargo.return_value = False
    mock_bag_json.return_value = {}
    compressed_name = "foo.tar.gz"
    mock_compress.return_value = compressed_name
    as_data = {'display_string': 'foo'}
    mock_as_data.return_value = as_data

    packager.run()

    mock_cleanup.assert_called_once_with()
    mock_notification.assert_called_once_with()
    mock_pdf.assert_called_once_with(as_uri)
    mock_deliver.assert_called_once_with(compressed_name)
    mock_compress.assert_called_once_with(ANY, bag_dir, {})
    mock_compress_embargoed.assert_not_called()
    mock_bag_json.assert_called_once_with(ANY, "foo", [])
    mock_create.assert_called_once_with(bag_dir, packager.rights_ids, as_data)
    mock_move.assert_called_once_with()
    mock_has_embargo.assert_called_once_with(rights_data)
    mock_rights_data.assert_called_once_with(packager.rights_ids, as_data)
    mock_as_data.assert_called_once_with(as_uri)
    mock_as_uri.assert_called_once_with(packager.refid)
    mock_aquila.assert_called_once_with(aquila_baseurl)
    mock_config.assert_called_once_with(packager.ssm_parameter_path)


@patch('src.package.Packager.get_config')
@patch('src.package.Packager.uri_from_refid')
@patch('src.package.Packager.cleanup_failed_job')
@patch('src.package.Packager.deliver_failure_notification')
def test_run_with_exception(
        mock_notification, mock_cleanup, mock_as_uri, mock_config):
    """Asserts exception is handled correctly."""
    packager = Packager(*ARGS)
    exception = Exception("No matching refid found.")
    mock_as_uri.side_effect = exception
    packager.run()
    mock_cleanup.assert_called_once_with(
        Path(packager.tmp_dir, packager.refid))
    mock_notification.assert_called_once_with(exception)
    mock_config.assert_called_once_with(packager.ssm_parameter_path)


@patch('src.package.Packager.get_date_range')
@patch('src.package.Packager.format_aspace_date')
@patch('src.package.find_closest_value')
@patch('asnake.client.ASnakeClient.get')
def test_get_as_data(mock_get, mock_find_closest, mock_dates, mock_range):
    """Asserts data is fetched from AS as expected."""
    as_data = {"display_string": "foobar"}
    mock_get.return_value = MockResponse(as_data, 200)
    packager = Packager(*ARGS)
    packager.as_client = ASpace().client
    as_uri = "/repositories/2/archival_objects/1234"
    as_dates = ('1999-01-01', '2000-12-31')
    mock_dates.return_value = as_dates
    mock_range.return_value = as_dates

    data = packager.get_as_data(as_uri)

    assert data == {
        'display_string': 'foobar',
        'start_date': as_dates[0],
        'end_date': as_dates[1],
        'uri': as_uri
    }


@patch('src.package.Packager.get_active_rights_acts')
def test_has_embargo(mock_acts):
    """Asserts embargoed status is corrrectly determined."""
    packager = Packager(*ARGS)
    for fixture, expected in [
            ('no_acts.json', False),
            ('disallow_disseminate.json', True),
            ('disallow_publish.json', True),
            ('disseminate_conditional.json', True),
            ('disseminate_allow.json', False)]:
        with open(Path('tests', 'fixtures', 'rights', fixture), 'r') as df:
            active_acts = json.load(df)
            mock_acts.return_value = active_acts

            output = packager.has_embargo(
                [
                    {'rights_granted': []},
                    {'rights_granted': []}
                ]
            )

            assert output == expected
            mock_acts.reset_mock()


def test_get_active_rights_acts():
    """Asserts active rights statements are correctly parsed."""
    packager = Packager(*ARGS)
    with open(Path('tests', 'fixtures', 'rights', 'active_acts_input.json'), 'r') as df:
        acts = json.load(df)
        parsed = packager.get_active_rights_acts(acts)
        with open(Path('tests', 'fixtures', 'rights', 'active_acts_output.json'), 'r') as pf:
            expected = json.load(pf)
            assert parsed == expected


@mock_aws
def test_move_to_tmp():
    """Asserts packages are moved to temp directory as expected."""
    packager = Packager(*ARGS)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    client = boto3.client('s3')
    client.create_bucket(Bucket=packager.source_bucket)
    fixture_path = Path('tests', 'fixtures', packager.refid)
    for dirpath, _, files in fixture_path.walk():
        for f in files:
            source = dirpath / f
            destination = Path(
                packager.refid,
                source.relative_to(fixture_path))
            client.upload_file(
                str(source),
                packager.source_bucket,
                str(destination))

    packager.move_to_tmp()

    assert tmp_path.is_dir()
    assert (tmp_path / 'service').is_dir()
    assert len(list(tmp_path.glob('*.tif'))) == 2
    assert len(list((tmp_path / 'service').glob('*.tif'))) == 2


def test_create_bag():
    """Asserts bag is created as expected."""
    as_data = {
        'start_date': '1999-01-01',
        'end_date': '2000-12-31',
        'display_string': 'foobar',
        'uri': '/repositories/2/archival_objects/1234'
    }
    packager = Packager(*ARGS)

    fixture_path = Path('tests', 'fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)

    packager.create_bag(tmp_path, packager.rights_ids, as_data)
    bag = bagit.Bag(str(tmp_path))
    assert bag.is_valid()
    for key in ['ArchivesSpace-URI', 'Start-Date',
                'End-Date', 'Origin', 'Rights-ID', 'BagIt-Profile-Identifier']:
        assert key in bag.info
    assert bag.info['Origin'] == 'digitization'
    assert bag.info['ArchivesSpace-URI'] == '/repositories/2/archival_objects/1234'
    assert bag.info['Start-Date'] == '1999-01-01'
    assert bag.info['End-Date'] == '2000-12-31'
    assert bag.info['Rights-ID'] == ARGS[4].split(',')
    assert bag.info['Title'] == 'foobar'
    assert bag.info['BagIt-Profile-Identifier'] == 'zorya_bagit_profile.json'


@patch('asnake.client.web_client.ASnakeClient.get')
def test_uri_from_refid(mock_get):
    """Asserts refids are translated to URIs as expected."""
    packager = Packager(*ARGS)
    mock_get.return_value.text = "v3.0.2"
    packager.as_client = ASpace().client
    packager.as_repo = '2'
    refid = '12345'
    as_url = f'repositories/2/find_by_id/archival_objects?ref_id[]={refid}'

    with open(Path('tests', 'fixtures', 'refid_single.json'), 'r') as df:
        resp = json.load(df)
        mock_get.return_value.json.return_value = resp
        returned = packager.uri_from_refid(refid)
        assert returned == '/repositories/2/archival_objects/929951'
        mock_get.assert_called_with(as_url)

    for fixture_path in ['refid_multiple.json', 'refid_none.json']:
        with open(Path('tests', 'fixtures', fixture_path), 'r') as df:
            resp = json.load(df)
            with pytest.raises(Exception):
                mock_get.return_value.json.return_value = resp
                packager.uri_from_refid(refid)


def test_get_date_range():
    """Asserts date ranges are parsed as expected."""
    packager = Packager(*ARGS)
    for fixture_path, expected in [
            ('single.json', ('1950', '1950')),
            ('single_range.json', ('1950', '1969')),
            ('multiple_range.json', ('1950', '1989')),
            ('multiple_mixed.json', ('1950', '1969')),
            ('multiple_mixed_after_end.json', ('1950', '1980')),
            ('multiple_mixed_before_start.json', ('1940', '1969'))]:
        with open(Path('tests', 'fixtures', 'get_date_range', fixture_path), 'r') as df:
            date_data = json.load(df)
            returned = packager.get_date_range(date_data)
            assert returned[0] == expected[0]
            assert returned[1] == expected[1]


def test_format_aspace_date():
    """Asserts dates are formatted as expected."""
    packager = Packager(*ARGS)
    for input, expected in [
            (['1950', '1969'], ('1950-01-01', '1969-12-31')),
            (['1950-03', '1969-04'], ('1950-03-01', '1969-04-30')),
            (['1950-02-03', '1969-04-05'], ('1950-02-03', '1969-04-05')),
            (['1950', '1950'], ('1950-01-01', '1950-12-31'))]:
        returned = packager.format_aspace_date(*input)
        assert returned[0] == expected[0]
        assert returned[1] == expected[1]


def test_get_bag_json():
    """Asserts bag data is structured correctly."""
    identifier = '123456789'
    title = 'foo'
    rights_data = []
    packager = Packager(*ARGS)

    output = packager.get_bag_json(identifier, title, rights_data)

    assert output == {
        "identifier": identifier,
        "title": title,
        "origin": 'digitization',
        "rights_statements": rights_data
    }


def test_compress_bag():
    """Asserts compressed files are correctly created and original directory is removed."""
    packager = Packager(*ARGS)
    fixture_path = Path('tests', 'fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    bagit.make_bag(tmp_path)
    bag_identifier = "123456789"

    compressed = packager.compress_bag(bag_identifier, tmp_path, {})
    assert compressed.is_file()
    assert not tmp_path.exists()


def test_compress_embargoed_bag():
    packager = Packager(*ARGS)
    fixture_path = Path('tests', 'fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    bagit.make_bag(tmp_path)

    compressed = packager.compress_embargoed_bag(tmp_path)
    assert compressed.is_file()
    assert not tmp_path.exists()


@mock_aws
def test_upload_file_already_exists():
    """Asserts files are not overwritten when expected."""
    packager = Packager(*ARGS)
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket)
    s3.put_object(
        Body=b'test content',
        Bucket=packager.destination_bucket,
        Key=f'{packager.refid}.tar.gz')

    compressed_file = f"{packager.refid}.tar.gz"
    fixture_path = Path('tests', 'fixtures', compressed_file)
    tmp_path = Path(packager.tmp_dir, compressed_file)
    copyfile(fixture_path, tmp_path)

    for expected_incrementor in range(1, 3):
        packager.upload_file(
            packager.destination_bucket,
            tmp_path,
            f'{packager.refid}.tar.gz',
            'application/gzip',
            True)

        assert s3.get_object(
            Bucket=packager.destination_bucket,
            Key=f'{packager.refid}_{expected_incrementor}.tar.gz')

    for expected_incrementor in range(1, 3):
        s3.delete_object(
            Bucket=packager.destination_bucket,
            Key=f'{packager.refid}_{expected_incrementor}.tar.gz')

    packager.upload_file(
        packager.destination_bucket,
        tmp_path,
        f'{packager.refid}.tar.gz',
        'application/gzip',
        False)

    assert s3.get_object(
        Bucket=packager.destination_bucket,
        Key=f'{packager.refid}.tar.gz')

    for expected_incrementor in range(1, 2):
        with pytest.raises(Exception):
            s3.head_object(
                Bucket=packager.destination_bucket,
                Key=f'{packager.refid}_{expected_incrementor}_{expected_incrementor}.tar.gz')


def test_deliver_package():
    """Asserts unembargoed compressed package is delivered and local copy is removed."""
    packager = Packager(*ARGS)
    compressed_file = f"{packager.refid}.tar.gz"
    fixture_path = Path('tests', 'fixtures', compressed_file)
    tmp_path = Path(packager.tmp_dir, compressed_file)

    for is_embargoed, destination_bucket in [
            (False, packager.destination_bucket),
            (True, packager.embargoed_destination_bucket)]:
        with mock_aws():
            copyfile(fixture_path, tmp_path)
            packager.is_embargoed = is_embargoed
            s3 = boto3.client('s3', region_name='us-east-1')
            s3.create_bucket(Bucket=destination_bucket)
            packager.deliver_package(tmp_path)
            assert s3.get_object(
                Bucket=destination_bucket,
                Key=compressed_file)
            assert not tmp_path.exists()


@mock_aws
def test_deliver_pdf():
    """Asserts compressed package is delivered and local copy is removed."""
    packager = Packager(*ARGS)
    as_uri = "/repositories/2/archival_objects/1234"
    fixture_path = Path('tests', 'fixtures', packager.refid)
    client = boto3.client('s3')
    client.create_bucket(Bucket=packager.source_bucket)
    fixture_path = Path('tests', 'fixtures', packager.refid)
    for dirpath, _, files in fixture_path.walk():
        for f in files:
            source = dirpath / f
            destination = Path(
                packager.refid,
                source.relative_to(fixture_path))
            client.upload_file(
                str(source),
                packager.source_bucket,
                str(destination))

    for is_embargoed, destination_bucket, destination_key in [
            (False, packager.pdf_destination_bucket,
             f'pdfs/{shortuuid.uuid(as_uri)}'),
            (True, packager.embargoed_pdf_destination_bucket, f'{packager.refid}.pdf')]:
        with mock_aws():
            packager.is_embargoed = is_embargoed
            s3 = boto3.client('s3', region_name='us-east-1')
            s3.create_bucket(Bucket=destination_bucket)

            packager.deliver_pdf(as_uri)
            assert s3.get_object(
                Bucket=destination_bucket,
                Key=destination_key)


@mock_aws
def test_cleanup_successful_job():
    """Asserts successful job is cleaned up as expected."""
    packager = Packager(*ARGS)
    fixture_path = Path(
        'tests',
        'fixtures',
        'b90862f3baceaae3b7418c78f9d50d52')
    client = boto3.client('s3')
    client.create_bucket(Bucket=packager.source_bucket)
    fixture_path = Path('tests', 'fixtures', packager.refid)
    for dirpath, _, files in fixture_path.walk():
        for f in files:
            source = dirpath / f
            destination = Path(
                packager.refid,
                source.relative_to(fixture_path))
            client.upload_file(
                str(source),
                packager.source_bucket,
                str(destination))

    packager.cleanup_successful_job()

    object_count = client.list_objects_v2(
        Bucket=packager.source_bucket,
        Prefix=packager.refid)['KeyCount']

    assert object_count == 0


def test_cleanup_failed_job():
    """Asserts failed job is cleaned up as expected."""
    packager = Packager(*ARGS)
    fixture_path = Path(
        'tests',
        'fixtures',
        'b90862f3baceaae3b7418c78f9d50d52')
    compressed_fixture_path = Path('tests', 'fixtures',
                                   'b90862f3baceaae3b7418c78f9d50d52.tar.gz')
    tmp_path = Path(packager.tmp_dir, packager.refid)
    compressed_tmp_path = Path(packager.tmp_dir,
                               'b90862f3baceaae3b7418c78f9d50d52.tar.gz')
    copytree(fixture_path, tmp_path)
    copyfile(compressed_fixture_path, compressed_tmp_path)

    packager.cleanup_failed_job(tmp_path)

    assert not tmp_path.is_dir()
    assert not compressed_tmp_path.is_file()


@mock_aws
@patch('src.package.Packager.get_client_with_role')
def test_deliver_success_notification(mock_role):
    """Assert success notifications are delivered as expected."""
    packager = Packager(*ARGS)
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    packager.sns_topic = topic_arn

    packager.deliver_success_notification()

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid


@mock_aws
@patch('src.package.Packager.get_client_with_role')
@patch('traceback.format_exception')
def test_deliver_failure_notification(mock_traceback, mock_role):
    """Asserts failure notifications are delivered as expected."""
    packager = Packager(*ARGS)
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    packager.sns_topic = topic_arn
    exception_message = "foo"
    exception = Exception(exception_message)
    mock_traceback.return_value = ['baz', 'buzz']

    packager.deliver_failure_notification(exception)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'FAILURE'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid
    assert exception_message in message_body['MessageAttributes']['message']['Value']
    assert message_body['MessageAttributes']['traceback']['Value'] == 'baz'
