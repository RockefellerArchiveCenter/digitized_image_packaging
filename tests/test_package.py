import json
from pathlib import Path
from shutil import copyfile, copytree, rmtree
from unittest.mock import DEFAULT, MagicMock, patch

import bagit
import boto3
import pytest
from asnake.aspace import ASpace
from moto import mock_s3, mock_sns, mock_sqs, mock_ssm, mock_sts
from moto.core import DEFAULT_ACCOUNT_ID

from src.package import Packager

AUDIO_ARGS = ['us-east-1', 'digitized-av-packaging-role-arn', '/dev/digitized_av_packaging',
              'b90862f3baceaae3b7418c78f9d50d52', "1,2", "tmp",
              "source", "destination", "destination_video_mezz", "destination_video_access",
              "destination_audio_access", "destination_poster", "topic"]
VIDEO_ARGS = ['us-east-1', 'digitized-av-packaging-role-arn', '/dev/digitized_av_packaging',
              '20f8da26e268418ead4aa2365f816a08', "1,2", "tmp",
              "source", "destination", "destination_video_mezz", "destination_video_access",
              "destination_audio_access", "destination_poster", "topic"]


@pytest.fixture
def audio_packager():
    packager = Packager(*AUDIO_ARGS)
    packager.format = 'audio'
    return packager


@pytest.fixture
def video_packager():
    packager = Packager(*VIDEO_ARGS)
    packager.format = 'video'
    return packager


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Fixture to create and tear down tmp dir before and after a test is run"""
    dir_list = [AUDIO_ARGS[5], AUDIO_ARGS[6]]
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


@mock_ssm
@mock_sts
@patch('src.package.Packager.get_client_with_role')
def test_get_config(mock_role):
    packager = Packager(*VIDEO_ARGS)
    ssm = boto3.client('ssm', region_name='us-east-1')
    mock_role.return_value = ssm
    path = "/dev/digitized-av-packaging"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString",
        )
    config = packager.get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}


@patch('src.package.Packager.get_config')
@patch('src.package.Packager.move_to_tmp')
@patch('src.package.Packager.parse_format')
@patch('src.package.Packager.create_poster')
@patch('src.package.Packager.deliver_derivatives')
@patch('src.package.Packager.create_bag')
@patch('src.package.Packager.compress_bag')
@patch('src.package.Packager.deliver_package')
@patch('src.package.Packager.cleanup_successful_job')
@patch('src.package.Packager.deliver_success_notification')
def test_run(mock_notification, mock_cleanup, mock_deliver, mock_compress, mock_create,
             mock_deliver_derivatives, mock_poster, mock_parse, mock_move, mock_config):
    """Asserts run method calls other methods."""
    packager = Packager(*AUDIO_ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    compressed_name = "foo.tar.gz"
    mock_compress.return_value = compressed_name
    file_list = []
    packager.run()
    mock_cleanup.assert_called_once_with()
    mock_notification.assert_called_once_with()
    mock_deliver.assert_called_once_with(compressed_name)
    mock_compress.assert_called_once_with(bag_dir)
    mock_create.assert_called_once_with(bag_dir, packager.rights_ids)
    mock_deliver_derivatives.assert_called_once_with()
    mock_poster.assert_called_once_with(bag_dir)
    mock_parse.assert_called_once_with(file_list)
    mock_move.assert_called_once_with(bag_dir)
    mock_config.assert_called_once_with(packager.ssm_parameter_path)


@patch('src.package.Packager.get_config')
@patch('src.package.Packager.move_to_tmp')
@patch('src.package.Packager.cleanup_failed_job')
@patch('src.package.Packager.deliver_failure_notification')
def test_run_with_exception(
        mock_notification, mock_cleanup, mock_move, mock_config):
    packager = Packager(*AUDIO_ARGS)
    exception = Exception("Error moving.")
    mock_move.side_effect = exception
    packager.run()
    mock_cleanup.assert_called_once_with(
        Path(packager.tmp_dir, packager.refid))
    mock_notification.assert_called_once_with(exception)
    mock_config.assert_called_once_with(packager.ssm_parameter_path)


def test_parse_format():
    """Asserts format is correctly parsed from files."""
    packager = Packager(*AUDIO_ARGS)
    video_files = [
        Path(f'{packager.refid}.mkv'),
        Path(f'{packager.refid}.mov'),
        Path(f'{packager.refid}.mp4')]
    audio_files = [
        Path(f'{packager.refid}.wav'),
        Path(f'{packager.refid}.mp3')]
    for expected, file_list in [
            ('audio', audio_files), ('video', video_files)]:
        assert expected == packager.parse_format(file_list)

    unrecognized_files = [
        Path(f'{packager.refid}.tif'),
        Path(f'{packager.refid}.jpg')]
    with pytest.raises(Exception):
        packager.parse_format(unrecognized_files)


def test_create_poster(video_packager):
    """Asserts poster image is created as expected."""
    fixture_path = Path('tests', 'fixtures', video_packager.refid)
    tmp_path = Path(video_packager.tmp_dir, video_packager.refid)
    copytree(fixture_path, tmp_path)

    video_packager.create_poster(tmp_path)
    assert Path(tmp_path, "poster.png").is_file()


def test_derivative_map_audio(audio_packager):
    """Asserts information for audio derivatives is correctly produced."""
    bag_dir = Path(audio_packager.tmp_dir, audio_packager.refid)
    map = audio_packager.derivative_map()
    assert len(map) == 1
    assert map == [
        (bag_dir / f"{audio_packager.refid}.mp3",
         audio_packager.destination_bucket_audio_access,
         'audio/mpeg')]


def test_derivative_map_video(video_packager):
    """Asserts information for video derivatives is correctly produced."""
    bag_dir = Path(video_packager.tmp_dir, video_packager.refid)
    map = video_packager.derivative_map()
    assert len(map) == 3
    assert map == [
        (bag_dir / f"{video_packager.refid}.mov",
            video_packager.destination_bucket_video_mezzanine, "video/quicktime"),
        (bag_dir / f"{video_packager.refid}.mp4",
            video_packager.destination_bucket_video_access, "video/mp4"),
        (bag_dir / "poster.png",
            video_packager.destination_bucket_poster, "image/x-png")]


@mock_s3
@mock_sts
def test_deliver_derivatives():
    """Assert derivatives are delivered to correct buckets and deleted locally."""
    packager = Packager(*VIDEO_ARGS)
    packager.format = 'video'
    fixture_path = Path('tests', 'fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    poster = tmp_path / "poster.png"
    poster.touch()

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket_video_access)
    s3.create_bucket(Bucket=packager.destination_bucket_video_mezzanine)
    s3.create_bucket(Bucket=packager.destination_bucket_poster)

    packager.deliver_derivatives()

    assert s3.get_object(
        Bucket=packager.destination_bucket_video_access,
        Key=f"{packager.refid}.mp4")
    assert s3.get_object(
        Bucket=packager.destination_bucket_video_mezzanine,
        Key=f"{packager.refid}.mov")
    assert s3.get_object(
        Bucket=packager.destination_bucket_poster,
        Key=f"{packager.refid}.png")
    assert Path(tmp_path, f"{packager.refid}.mkv").is_file()
    assert not Path(tmp_path, f"{packager.refid}.mov").is_file()
    assert not Path(tmp_path, f"{packager.refid}.mp4").is_file()
    assert not Path(tmp_path, "poster.png").is_file()


@mock_s3
@mock_sts
def test_deliver_derivatives_multiple_masters():
    """
    Assert derivatives are delivered to correct buckets and deleted
    locally for bags with multiple master files.
    """
    packager = Packager(*VIDEO_ARGS)
    packager.refid = '20f8da26e268418ead4aa2365f816a09'
    packager.format = 'video'
    fixture_path = Path('tests', 'fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    poster = tmp_path / "poster.png"
    poster.touch()

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket_video_access)
    s3.create_bucket(Bucket=packager.destination_bucket_video_mezzanine)
    s3.create_bucket(Bucket=packager.destination_bucket_poster)

    packager.deliver_derivatives()

    assert s3.get_object(
        Bucket=packager.destination_bucket_video_access,
        Key=f"{packager.refid}.mp4")
    assert s3.get_object(
        Bucket=packager.destination_bucket_video_mezzanine,
        Key=f"{packager.refid}.mov")
    assert s3.get_object(
        Bucket=packager.destination_bucket_poster,
        Key=f"{packager.refid}.png")
    assert Path(tmp_path, f"{packager.refid}_01.mkv").is_file()
    assert Path(tmp_path, f"{packager.refid}_02.mkv").is_file()
    assert not Path(tmp_path, f"{packager.refid}.mov").is_file()
    assert not Path(tmp_path, f"{packager.refid}.mp4").is_file()
    assert not Path(tmp_path, "poster.png").is_file()


@patch('src.package.Packager.get_date_range')
@patch('src.package.Packager.format_aspace_date')
@patch('src.package.Packager.uri_from_refid')
def test_create_bag(mock_uri, mock_dates,
                    mock_range, audio_packager):
    """Asserts bag is created as expected."""
    audio_packager.as_client = ASpace().client
    as_uri = "/repositories/2/archival_objects/1234"
    as_dates = ('1999-01-01', '2000-12-31')
    mock_uri.return_value = as_uri
    mock_dates.return_value = as_dates
    mock_range.return_value = as_dates

    fixture_path = Path('tests', 'fixtures', audio_packager.refid)
    tmp_path = Path(audio_packager.tmp_dir, audio_packager.refid)
    copytree(fixture_path, tmp_path)

    audio_packager.create_bag(tmp_path, audio_packager.rights_ids)
    bag = bagit.Bag(str(tmp_path))
    assert bag.is_valid()
    for key in ['ArchivesSpace-URI', 'Start-Date',
                'End-Date', 'Origin', 'Rights-ID', 'BagIt-Profile-Identifier']:
        assert key in bag.info
    assert bag.info['Origin'] == 'av_digitization'
    assert bag.info['ArchivesSpace-URI'] == as_uri
    assert bag.info['Start-Date'] == as_dates[0]
    assert bag.info['End-Date'] == as_dates[1]
    assert bag.info['Rights-ID'] == AUDIO_ARGS[4].split(',')
    assert bag.info['BagIt-Profile-Identifier'] == 'zorya_bagit_profile.json'


@patch('asnake.client.web_client.ASnakeClient.get')
def test_uri_from_refid(mock_get, audio_packager):
    """Asserts refids are translated to URIs as expected."""
    mock_get.return_value.text = "v3.0.2"
    audio_packager.as_client = ASpace().client
    audio_packager.as_repo = '2'
    refid = '12345'
    as_url = f'repositories/2/find_by_id/archival_objects?ref_id[]={refid}'

    with open(Path('tests', 'fixtures', 'refid_single.json'), 'r') as df:
        resp = json.load(df)
        mock_get.return_value.json.return_value = resp
        returned = audio_packager.uri_from_refid(refid)
        assert returned == '/repositories/2/archival_objects/929951'
        mock_get.assert_called_with(as_url)

    for fixture_path in ['refid_multiple.json', 'refid_none.json']:
        with open(Path('tests', 'fixtures', fixture_path), 'r') as df:
            resp = json.load(df)
            with pytest.raises(Exception):
                mock_get.return_value.json.return_value = resp
                audio_packager.uri_from_refid(refid)


def test_get_date_range(audio_packager):
    """Asserts date ranges are parsed as expected."""
    for fixture_path, expected in [
            ('single.json', ('1950', '1950')),
            ('single_range.json', ('1950', '1969')),
            ('multiple_range.json', ('1950', '1989')),
            ('multiple_mixed.json', ('1950', '1969')),
            ('multiple_mixed_after_end.json', ('1950', '1980')),
            ('multiple_mixed_before_start.json', ('1940', '1969'))]:
        with open(Path('tests', 'fixtures', 'get_date_range', fixture_path), 'r') as df:
            date_data = json.load(df)
            returned = audio_packager.get_date_range(date_data)
            assert returned[0] == expected[0]
            assert returned[1] == expected[1]


def test_format_aspace_date(audio_packager):
    """Asserts dates are formatted as expected."""
    for input, expected in [
            (['1950', '1969'], ('1950-01-01', '1969-12-31')),
            (['1950-03', '1969-04'], ('1950-03-01', '1969-04-30')),
            (['1950-02-03', '1969-04-05'], ('1950-02-03', '1969-04-05')),
            (['1950', '1950'], ('1950-01-01', '1950-12-31'))]:
        returned = audio_packager.format_aspace_date(*input)
        assert returned[0] == expected[0]
        assert returned[1] == expected[1]


def test_compress_bag(audio_packager):
    """Asserts compressed files are correctly created and original directory is removed."""
    fixture_path = Path('tests', 'fixtures', audio_packager.refid)
    tmp_path = Path(audio_packager.tmp_dir, audio_packager.refid)
    copytree(fixture_path, tmp_path)
    bagit.make_bag(tmp_path)

    compressed = audio_packager.compress_bag(tmp_path)
    assert compressed.is_file()
    assert not tmp_path.exists()


@mock_s3
@mock_sts
def test_deliver_package():
    """Asserts compressed package is delivered and local copy is removed."""
    packager = Packager(*AUDIO_ARGS)
    compressed_file = f"{packager.refid}.tar.gz"
    fixture_path = Path('tests', 'fixtures', compressed_file)
    tmp_path = Path(packager.tmp_dir, compressed_file)
    copyfile(fixture_path, tmp_path)
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket)

    packager.deliver_package(tmp_path)
    assert s3.get_object(
        Bucket=packager.destination_bucket,
        Key=compressed_file)
    assert not tmp_path.exists()


def test_cleanup_successful_job(audio_packager):
    """Asserts successful job is cleaned up as expected."""
    fixture_path = Path(
        'tests',
        'fixtures',
        'b90862f3baceaae3b7418c78f9d50d52')
    src_path = Path(audio_packager.source_dir, audio_packager.refid)
    copytree(fixture_path, src_path)

    audio_packager.cleanup_successful_job()

    source_objects = list(src_path.glob('*'))

    assert len(source_objects) == 0


def test_cleanup_failed_job(audio_packager):
    """Asserts failed job is cleaned up as expected."""
    fixture_path = Path(
        'tests',
        'fixtures',
        'b90862f3baceaae3b7418c78f9d50d52')
    compressed_fixture_path = Path('tests', 'fixtures',
                                   'b90862f3baceaae3b7418c78f9d50d52.tar.gz')
    tmp_path = Path(audio_packager.tmp_dir, audio_packager.refid)
    compressed_tmp_path = Path(audio_packager.tmp_dir,
                               'b90862f3baceaae3b7418c78f9d50d52.tar.gz')
    copytree(fixture_path, tmp_path)
    copyfile(compressed_fixture_path, compressed_tmp_path)

    audio_packager.cleanup_failed_job(tmp_path)

    assert not tmp_path.is_dir()
    assert not compressed_tmp_path.is_file()


@mock_sns
@mock_sqs
@mock_sts
@patch('src.package.Packager.get_client_with_role')
def test_deliver_success_notification(mock_role):
    """Assert success notifications are delivered as expected."""
    packager = Packager(*AUDIO_ARGS)
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
    packager.format = 'audio'

    packager.deliver_success_notification()

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['format']['Value'] == packager.format
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid


@mock_sns
@mock_sqs
@mock_sts
@patch('src.package.Packager.get_client_with_role')
def test_deliver_failure_notification(mock_role):
    """Asserts failure notifications are delivered as expected."""
    packager = Packager(*AUDIO_ARGS)
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
    packager.format = 'audio'
    exception_message = "foo"
    exception = Exception(exception_message)

    packager.deliver_failure_notification(exception)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['format']['Value'] == packager.format
    assert message_body['MessageAttributes']['outcome']['Value'] == 'FAILURE'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid
    assert exception_message in message_body['MessageAttributes']['message']['Value']
