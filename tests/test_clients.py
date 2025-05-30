from unittest.mock import patch

from src.clients import AquilaClient

from .helpers import MockResponse


def test_aquila_client_init():
    client = AquilaClient("foo")
    assert client.baseurl == "foo"


@patch('requests.Session.post')
def test_aquila_client_get_rights_data(mock_post):
    mock_post.return_value = MockResponse(
        {'rights_statements':
            [
                {'rights_granted': []},
                {'rights_granted': []}
            ]
         },
        200)
    aquila_baseurl = "https://aquila.rockarch.org/api"
    rights_ids = ['1', '2']
    start_date = '1999-01-01'
    end_date = '2000-12-31'
    as_data = {
        'start_date': start_date,
        'end_date': end_date,
    }
    client = AquilaClient(aquila_baseurl)

    output = client.get_rights_data(rights_ids, as_data)

    assert output == [
        {'rights_granted': []},
        {'rights_granted': []}
    ]
    mock_post.assert_called_once_with(
        f'{aquila_baseurl}/rights-assemble/',
        json={
            'identifiers': rights_ids,
            'start_date': start_date,
            'end_date': end_date}
    )
