import json
import logging
import openstack
import os
import subprocess
import sys
import time

from datetime import datetime


class Skipper(object):
    TYPE_NONE = 'none'
    TYPE_OSP = 'osp'

    @staticmethod
    def detect(host_type, data):
        from .hosts import BaseHost
        if host_type == BaseHost.TYPE_OSP and 'osp_source_environment' in data:
            return Skipper.TYPE_OSP
        else:
            return Skipper.TYPE_NONE

    @staticmethod
    def factory(skipper_type, data):
        if skipper_type == Skipper.TYPE_OSP:
            return OSPSkipper(data)
        return Skipper()

    @staticmethod
    def get_command():
        return sys.executable, os.path.realpath(__file__)

    def prepare_exports(self):
        logging.info('No preparation needed for this migration source.')

    def transfer_exports(self):
        logging.info('No need to skip virt-v2v for volume transfer.')

    def close_exports(self):
        logging.info('No cleanup needed for this migration source.')


class OSPSkipper(Skipper):
    def prepare_exports(self):
        self._test_ssh_connection()
        self._shutdown_source_vm()
        self.root_volume_id, self.data_volume_ids = \
                self._get_root_and_data_volumes()
        self.root_volume_copy, self.root_snapshot = \
                self._detach_data_volumes_from_source()
        self._attach_volumes_to_converter()
        self._export_volumes_from_converter()
        self.exported = True

    def transfer_exports(self):
        logging.info('TODO: copy NBD exports from here.')
        out = subprocess.check_output(['qemu-img', 'info', 'nbd://localhost:10809'],  stderr=subprocess.STDOUT)
        logging.info('NBD EXPORT: %s', out)
        if self.export_process:
            time.sleep(10)

    def close_exports(self):
        if self.exported:
            self._test_ssh_connection()
            self._converter_close_exports()
            self._detach_volumes_from_converter()
            self._attach_data_volumes_to_source()
            self.exported = False

    def __init__(self, data):
        osp_env = data['osp_source_environment']
        osp_arg_list = ["auth_url", "username", "password",
                        "project_name", "project_domain_name",
                        "user_domain_name", "verify"]
        osp_args = {arg: osp_env[arg] for arg in osp_arg_list}
        self.source_converter = osp_env['conversion_vm_id']
        self.source_instance = osp_env['vm_id']
        self.conn = openstack.connect(**osp_args)
        self.key = data['ssh_key_file']

    def _source_vm(self):
        """
        Changes to the VM returned by get_server_by_id are not necessarily
        reflected in existing objects, so just get a new one every time.
        """
        return self.conn.get_server_by_id(self.source_instance)

    def _converter(self):
        return self.conn.get_server_by_id(self.source_converter)

    def _test_ssh_connection(self):
        ip = self._converter().accessIPv4
        out = subprocess.check_output(['ssh',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
            'cloud-user@'+ip,
            'echo conn'])
        if out.strip() == 'conn':
            return True
        return False

    def _shutdown_source_vm(self):
        server = self.conn.compute.get_server(self._source_vm().id)
        if server.status != 'SHUTOFF':
            self.conn.compute.stop_server(server=server)
            logging.info('Waiting 300s for source VM to stop...')
            self.conn.compute.wait_for_server(server, 'SHUTOFF', wait=300)

    def _detach_data_volumes_from_source(self):
        """
        Detach data volumes from source VM, and pretend to "detach" the boot
        volume by creating a new volume from a snapshot of the VM.
        """
        sourcevm = self._source_vm()

        # Detach non-root volumes
        for volume in self.data_volume_ids:
            v = self.conn.get_volume_by_id(volume)
            logging.info('Detaching %s from %s', volume, sourcevm.id)
            self.conn.detach_volume(server=sourcevm, volume=v, wait=True)

        # Create a snapshot of the root volume
        logging.info('Creating root device snapshot')
        root_snapshot = self.conn.create_volume_snapshot(force=True, wait=True,
                name="rhosp-migration-{}".format(self.root_volume_id),
                volume_id=self.root_volume_id)

        # Create a new volume from the snapshot
        logging.info('Creating new volume from root snapshot')
        root_volume_copy = self.conn.create_volume(wait=True,
                snapshot_id=root_snapshot.id,
                size=root_snapshot.size)
        return root_volume_copy, root_snapshot

    def _get_root_and_data_volumes(self):
        root_volume_id = None
        data_volume_ids = []
        for volume in self._source_vm().volumes:
            logging.info('Inspecting volume: %s', volume['id'])
            v = self.conn.volume.get_volume(volume['id'])
            for attachment in v.attachments:
                if attachment['server_id'] == self._source_vm().id:
                    if attachment['device'] == '/dev/vda':
                        logging.info('Assuming this volume is the root disk')
                        root_volume_id = attachment['volume_id']
                    else:
                        logging.info('Assuming this is a data volume')
                        data_volume_ids.append(attachment['volume_id'])
                else:
                    logging.info('Attachment is not part of current VM?')
        return root_volume_id, data_volume_ids

    def _attach_volumes_to_converter(self):
        """ Attach all the source volumes to the conversion host """
        conversion_volume_ids = [self.root_volume_copy.id]+self.data_volume_ids
        for volume_id in conversion_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s to conversion host...', volume_id)
            self.conn.attach_volume(server=self._converter(), volume=volume)

    def _export_volumes_from_converter(self):
        """ SSH to source conversion host and start an NBD export """
        logging.info('Exporting volumes from source conversion host...')
        ip = self._converter().accessIPv4
        root = self.conn.get_volume_by_id(self.root_volume_copy.id)
        port_offset = 0
        name = root.attachments[0].device
        nbdports = ['-L', '{0}:localhost:{0}'.format(10809+port_offset)]
        nbdkits = 'sudo nbdkit --exit-with-parent -p {} file {} & \n '.format(10809+port_offset, name)
        logging.info('Exporting device %s from source conversion host.', name)
        for volume_id in self.data_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            name = volume.attachments[0].device
            logging.info('Exporting device %s...', name)
            port_offset += 1
            nbdports.extend(['-L', '{0}:localhost:{0}'.format(10809+port_offset)])
            nbdkits += 'sudo nbdkit --exit-with-parent -p {} file {} & \n '.format(10809+port_offset, name)

        cmdarg = ['ssh',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10']
        cmdarg.extend(nbdports)
        cmdarg.append('cloud-user@'+ip)
        cmdarg.append(nbdkits)
        logging.info('NBD EXPORT: command is %s', str(cmdarg))
        self.export_process = subprocess.Popen(cmdarg, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(5)
        logging.info('All devices exported.')

    def _converter_close_exports(self):
        """ SSH to source conversion host and close the NBD export """
        logging.info('Stopping export from source conversion host...')
        #self.export_process.terminate()
        out, err = self.export_process.communicate()
        logging.info('NBD EXPORT OUT: %s', out)
        logging.info('NBD EXPORT ERR: %s', err)
        logging.info('NBD EXPORT PID: %s', str(self.export_process.pid))
        logging.info('NBD EXPORT VAL: %s', str(self.export_process.wait()))

    def _detach_volumes_from_converter(self):
        """ Detach volumes from conversion host """
        for volume in self._converter().volumes:
            logging.info('Inspecting volume %s', volume["id"])
            v = self.conn.volume.get_volume(volume["id"])
            for attachment in v.attachments:
                if attachment["server_id"] == self._converter().id:
                    if attachment["device"] == "/dev/vda":
                        logging.info('This is the root volume for this VM')
                    else:
                        logging.info('This volume is a data disk for this VM')
                        self.conn.detach_volume(server=self._converter(),
                                volume=v, wait=True)
                        logging.info('Detached.')
                else:
                    logging.info('Attachment is not part of current VM?')

    def _attach_data_volumes_to_source(self):
        """ Clean up the copy of the root volume and re-attach data volumes """

        # Delete the copy of the source root disk
        logging.info('Removing copy of root volume')
        self.conn.delete_volume(name_or_id=self.root_volume_copy.id, wait=True)
        logging.info('Deleting root device snapshot')
        self.conn.delete_volume_snapshot(name_or_id=self.root_snapshot.id,
                wait=True)

        # Attach data volumes back to source VM
        logging.info('Re-attaching volumes to source VM...')
        for volume_id in self.data_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s back to source VM...', volume_id)
            self.conn.attach_volume(volume=volume, wait=True,
                    server=self._source_vm())

state_log = './mrlog.log'
msg_log = './msglog.log'
def log_message(message):
    tstr = datetime.now().isoformat()
    d = {"message": message, "timestamp": tstr, "type": "message"}
    with open(state_log, 'a') as f, open(msg_log, 'a') as g:
        f.write(json.dumps(d)+'\n')
        g.write(json.dumps(message)+'\n')
        f.flush()
        g.flush()
    print(message)

def main():
    global state_log
    global msg_log
    with open('/root/shout.log', 'w') as f:
        f.write('running\n')
        f.write(sys.argv[0]+'\n')
        f.write(sys.argv[1]+'\n')
        f.write(sys.argv[2]+'\n')
    print("args: {}".format(sys.argv))
    print("pid: {}".format(os.getpid()))
    state_log = sys.argv[1]
    msg_log = sys.argv[3]
    log_message('Starting up.')
    logging.basicConfig(
        level=logging.INFO,
        filename='demo.log')
    data = json.loads(sys.argv[2])
    osp = OSPSkipper(data)
    log_message('Testing SSH connection...')
    logging.info('SSH OK: %s', str(osp._test_ssh_connection()))
    time.sleep(2)
    log_message('Copying disk 1/10 to target...')
    time.sleep(5)
    #log_message('Copying disk 2/10 to target...')
    #time.sleep(5)
    #log_message('Copying disk 3/10 to target...')
    #time.sleep(5)
    #log_message('Copying disk 4/10 to target...')
    #time.sleep(5)
    #log_message('Copying disk 5/10 to target...')
    #time.sleep(5)
    #log_message('Copying disk 6/10 to target...')
    #time.sleep(5)
    osp.prepare_exports()
    try:
        osp.transfer_exports()
    except Exception:
        pass
    osp.close_exports()

if __name__ == '__main__':
    main()
