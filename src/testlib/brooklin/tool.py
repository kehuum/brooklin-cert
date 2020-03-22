import json
import subprocess

from abc import ABC, abstractmethod
from testlib.core.utils import typename
from testlib.brooklin.datastream import Datastream

BASE_TOOL_COMMAND = 'brooklin-tool datastream'

GET_COMMAND = 'get'
LIST_COMMAND = 'list'
CREATE_COMMAND = 'create'
DELETE_COMMAND = 'delete'
UPDATE_COMMAND = 'update'
STOP_COMMAND = 'stop'
PAUSE_COMMAND = 'pause'
RESUME_COMMAND = 'resume'
RESTART_COMMAND = 'restart'


class BrooklinToolCommandBuilder(object):
    """brooklin-tool command generator"""

    @staticmethod
    def build_datastream_get_command(args):
        return f'{BASE_TOOL_COMMAND} {GET_COMMAND} ' \
               f'-n {args.name} ' \
               f'-f {args.fabric} ' \
               f'-t {args.tag} ' \
               f'--json'

    @staticmethod
    def build_datastream_list_command(args):
        return f'{BASE_TOOL_COMMAND} {LIST_COMMAND} ' \
               f'-f {args.fabric} ' \
               f'-t {args.tag}'

    @staticmethod
    def build_datastream_create_command(args):
        create_command = f'{BASE_TOOL_COMMAND} {CREATE_COMMAND} mirrormaker ' \
                         f'-f {args.fabric} ' \
                         f'--tag {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--whitelist "{args.whitelist}" ' \
                         f'--source-cluster-dns {args.src_cluster_dns} ' \
                         f'--destination-cluster-dns {args.dest_cluster_dns} ' \
                         f'--jira {args.jira} ' \
                         f'--num-tasks {args.numtasks} ' \
                         f'--cert {args.cert} '

        join_csv_values = BrooklinToolCommandBuilder.join_csv_values

        # The remaining are optional, so must check if they exist
        if args.users:
            create_command += f'--users {join_csv_values(args.users)} '
        if args.groups:
            create_command += f'--groups {join_csv_values(args.groups)} '
        if args.applications:
            create_command += f'--applications {join_csv_values(args.applications)} '
        if args.offsetreset:
            create_command += f'--offset-reset {args.offsetreset} '
        if args.groupid:
            create_command += f'--group-id {args.groupid} '
        if args.partitionmanaged:
            create_command += f'--partitionManaged '
        if args.passthrough:
            create_command += f'--passthrough '
        if args.identity:
            create_command += f'--identity '
        if args.topiccreate:
            create_command += f'--createTopic '
        if args.metadata:
            for metadata in args.metadata:
                create_command += f'--metadata {metadata} '
        return create_command

    @staticmethod
    def build_datastream_delete_command(args):
        delete_command = f'{BASE_TOOL_COMMAND} {DELETE_COMMAND} ' \
                         f'-f {args.fabric} ' \
                         f'--tags {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--jira {args.jira} ' \
                         f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.force:
            delete_command += f'--force'

        return delete_command

    @staticmethod
    def build_datastream_update_command(args):
        update_command = f'{BASE_TOOL_COMMAND} {UPDATE_COMMAND} ' \
                         f'-f {args.fabric} ' \
                         f'--tags {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--jira {args.jira} ' \
                         f'--cert {args.cert} '

        for metadata in args.metadata:
            update_command += f'--metadata {metadata} '

        # The remaining are optional, so must check if they exist
        if args.force:
            update_command += f'--force '
        if args.newwhitelist:
            update_command += f'--new-whitelist "{args.newwhitelist}" '
        if args.restart:
            update_command += f'--restart '
        if args.wait:
            update_command += f'--wait {args.wait}'

        return update_command

    @staticmethod
    def build_datastream_stop_command(args):
        stop_command = f'{BASE_TOOL_COMMAND} {STOP_COMMAND} ' \
                       f'-f {args.fabric} ' \
                       f'--tags {args.tag} ' \
                       f'-n {args.name} ' \
                       f'--cert {args.cert} ' \
                       f'--message "{args.message}" '

        # The remaining are optional, so must check if they exist
        if args.force:
            stop_command += f'--force'

        return stop_command

    @staticmethod
    def build_datastream_pause_command(args):
        pause_command = f'{BASE_TOOL_COMMAND} {PAUSE_COMMAND} ' \
                        f'-f {args.fabric} ' \
                        f'--tags {args.tag} ' \
                        f'-n {args.name} ' \
                        f'--message "{args.message}" '

        # The remaining are optional, so must check if they exist
        if args.topic:
            pause_command += f'--topic {args.topic} '
        if args.partitions:
            pause_command += f'--partitions {BrooklinToolCommandBuilder.join_csv_values(args.partitions)} '
        if args.force:
            pause_command += f'--force'

        return pause_command

    @staticmethod
    def build_datastream_resume_command(args):
        resume_command = f'{BASE_TOOL_COMMAND} {RESUME_COMMAND} ' \
                         f'-f {args.fabric} ' \
                         f'--tags {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.topic:
            resume_command += f'--topic {args.topic} '
        if args.partitions:
            resume_command += f'--partitions {BrooklinToolCommandBuilder.join_csv_values(args.partitions)}'

        return resume_command

    @staticmethod
    def build_datastream_restart_command(args):
        restart_command = f'{BASE_TOOL_COMMAND} {RESTART_COMMAND} ' \
                          f'-f {args.fabric} ' \
                          f'--tags {args.tag} ' \
                          f'-n {args.name} ' \
                          f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.wait:
            restart_command += f'--wait {args.wait}'

        return restart_command

    @staticmethod
    def join_csv_values(values_list):
        return ','.join(values_list)


class DatastreamCommandError(Exception):
    """Parent exception for all errors encountered when executing datastream commands (e.g. create, delete)"""

    def __init__(self, message, cause=None):
        self._message = message
        self._cause = cause

    @property
    def message(self):
        return self._message

    @property
    def cause(self):
        return self._cause

    def __str__(self):
        s = f"Message: {self.message}"
        if self.cause:
            s += f"\nCause: {typename(self.cause)}: {self.cause}"
        return s


class DatastreamCommandFailedError(DatastreamCommandError):
    """Exception raised when a datastream command (e.g. create, delete) fails"""
    pass


class ShellCommandTimeoutError(DatastreamCommandError):
    """Exception raised when a shell command (typically brooklin-tool) takes too long to execute"""

    def __init__(self, command, cause=None):
        super().__init__(f'Time out elapsed waiting for command to complete: {command}', cause)


class ShellCommandFailedError(DatastreamCommandError):
    """Exception raised when a shell command (typically brooklin-tool) completes with nonzero exit status"""

    def __init__(self, command, returncode, stderr):
        super().__init__(f'Command failed with exit code {returncode}: {command} \n'
                         f'Output:\n'
                         f'{stderr.read().decode("utf-8")}')


class NoSuchDatastreamError(DatastreamCommandError):
    """Exception raised when a datastream is not found"""

    def __init__(self, datastream, cause=None):
        super().__init__(f'Datastream not found: {datastream}', cause)


class DatastreamCommand(object):
    """Base class of all datastream commands

    It serves two main purposes:

        - It gives all datastream commands access to the parsed arguments
        of the datastream operation requested by the user.

        - It provides two basic APIs for all datastream commands, execute()
        and validate(). Each command may choose to implement either one of
        these APIs or both.

    """

    def __init__(self, args):
        self.args = args

    def execute(self):
        """Execute the datastream command"""
        pass

    def validate(self):
        """Validate the side-effects of executing the datastream command"""
        pass

    @staticmethod
    def run_command(command, timeout=120):
        """A utility for executing shell commands"""

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            returncode = process.wait(timeout)
        except subprocess.TimeoutExpired as err:
            raise ShellCommandTimeoutError(command, err)
        else:
            if returncode != 0:
                raise ShellCommandFailedError(command, returncode, process.stderr)
            return process.stdout.read().decode('utf-8')


class SimpleDatastreamCommand(DatastreamCommand, ABC):
    """Abstract base class of all datastream commands that map directly to brooklin-tool commands

    Extenders provide this base with the brooklin-tool commands they want to execute by
    overriding the command(self) @property. This base then takes care of executing these
    commands on the shell, returning their outputs, and raising errors in case of failures.

     """

    def execute(self):
        print(f"Running command: {self.command}")
        if not self.args.dryrun:
            return DatastreamCommand.run_command(self.command)

    @property
    @abstractmethod
    def command(self):
        return ''


class GetDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream get"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_get_command(self.args)

    def execute(self) -> Datastream:
        output = super().execute()
        try:
            return Datastream(json.loads(output))
        except json.JSONDecodeError as err:
            raise NoSuchDatastreamError(self.args.name, err)


class ListDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream list"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_list_command(self.args)


class CreateDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream create"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_create_command(self.args)

    def validate(self):
        """Validate datastream exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready:
            raise DatastreamCommandFailedError(f"Datastream {self.args.name} created but its status is: "
                                               f"{datastream.status}")


class DeleteDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream delete"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_delete_command(self.args)

    def validate(self):
        """Validate datastream does not exist"""
        get_command = GetDatastream(self.args)
        try:
            datastream = get_command.execute()
        except NoSuchDatastreamError:
            pass
        else:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not deleted successfully \n'
                                               f'Found: \n'
                                               f'{datastream}')


class UpdateDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream update"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_update_command(self.args)

    def validate(self):
        """Validate datastream exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready:
            raise DatastreamCommandFailedError(f"Datastream {self.args.name} updated but its status is: "
                                               f"{datastream.status}")
        if self.args.newwhitelist and datastream.whitelist != self.args.newwhitelist:
            raise DatastreamCommandFailedError(f"Datastream {self.args.name} whitelist update failed. "
                                               f"Current whitelist is: {datastream.whitelist}")


class StopDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream stop"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_stop_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is STOPPED"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_stopped:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not stopped successfully; '
                                               f'its status is: {datastream.status}')


class PauseDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream pause"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_pause_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is PAUSED"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_paused:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not paused successfully; '
                                               f'its status is: {datastream.status}')


class ResumeDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream resume"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_resume_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not resumed successfully; '
                                               f'its status is: {datastream.status}')


class RestartDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream restart"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_restart_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not restarted successfully; '
                                               f'its status is: {datastream.status}')
