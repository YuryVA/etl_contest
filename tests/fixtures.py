import time
import pytest   # noqa
import docker   # noqa
from .helpers import (get_session_id,
                      get_unused_port,
                      ping_container,
                      load_assets_to_source_db,
                      load_struct_to_destination_db,
                      load_assets_to_destination_db)


BASE_DOCKER_IMAGE = 'percona/percona-server:5.7.32'


class Container(object):
    def __init__(self):
        self.container = None
        self.session_id = get_session_id()
        self.docker_client = docker.client.from_env()
        self.unused_port = get_unused_port()

        self.credentials = {'host': 'localhost',
                            'port': self.unused_port,
                            'database': 'sandbox',
                            'user': 'etl',
                            'password': 'etl_contest',
                            'autocommit': True}

        self.env = {'MYSQL_DATABASE': 'sandbox',
                    'MYSQL_USER': 'etl',
                    'MYSQL_PASSWORD': 'etl_contest',
                    'MYSQL_ROOT_PASSWORD': 'root_etl_contest'}

        self.ports = {3306: self.unused_port}

    def __enter__(self):

        print(f'Run {self.session_id} on port {self.unused_port}')
        self.docker_client.images.pull(BASE_DOCKER_IMAGE)

        self.container = self.docker_client.containers.run(
            image=BASE_DOCKER_IMAGE,
            ports=self.ports,
            environment=self.env,
            name=self.session_id,
            detach=True)

        time.sleep(2)
        ping_container(self.credentials, self.session_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.stop()
        time.sleep(2)
        self.docker_client.close()


@pytest.fixture
def mysql_source_image():
    with Container() as c:
        load_assets_to_source_db(c.credentials)
        yield c.credentials


@pytest.fixture
def mysql_destination_image():
    with Container() as c:
        load_struct_to_destination_db(c.credentials)
        yield c.credentials


@pytest.fixture
def mysql_destination_image_2():
    with Container() as c:
        load_struct_to_destination_db(c.credentials)
        load_assets_to_destination_db(c.credentials)
        yield c.credentials
