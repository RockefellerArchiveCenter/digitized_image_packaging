from requests import Session


class AquilaClient(object):

    def __init__(self, baseurl):
        self.session = Session()
        self.baseurl = baseurl

    def get_rights_data(self, rights_ids, as_data):
        """Fetches structured rights statements from Aquila.

        Args:
            rights_ids (list): Identifiers for rights statements in Aquila.
            as_data (dict): Data from ArchivesSpace.

        Returns:
            rights_data (list): structured rights data from Aquila.
        """
        data = {
            'identifiers': rights_ids,
            'start_date': as_data['start_date'],
            'end_date': as_data['end_date']
        }
        resp = self.session.post(
            f'{self.baseurl.rstrip("/")}/rights-assemble/',
            json=data)
        resp.raise_for_status()
        return resp.json()['rights_statements']
