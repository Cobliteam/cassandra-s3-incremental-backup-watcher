import socket
import time
import uuid

from cassandra_s3_incremental_backup_watcher.main import Runner


def wait_for_port(host, port, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.create_connection((host, port), 1)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except socket.error:
            continue
        else:
            return

    raise socket.timeout('Waiting for {}:{}'.format(host, port))


class TestRunner(Runner):
    def __init__(self, data_dirs, s3_resource, s3_bucket, delete=False,
                 dry_run=True):
        super(TestRunner, self).__init__(None)

        self.delete = delete
        self.dry_run = dry_run
        self.data_dirs = data_dirs
        self.s3_bucket = s3_bucket
        self._s3_resource = s3_resource

    def parse_node_info(self, args):
        self.datacenter = 'datacenter1'
        self.host_id = str(uuid.uuid4())

    def parse_s3_options(self, args):
        self.s3_path = '{}/{}/{}'.format('test', self.datacenter, self.host_id)
        self.s3_settings = {}

    def parse_filters(self, args):
        self.keyspace_filter = lambda ks: True
        self.table_filter = lambda tbl: True

    def parse_misc(self, args):
        pass
