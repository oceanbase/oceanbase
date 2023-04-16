# coding: utf-8
# OceanBase Deploy.
# Copyright (C) 2021 OceanBase
#
# This file is part of OceanBase Deploy.
#
# OceanBase Deploy is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# OceanBase Deploy is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with OceanBase Deploy.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import absolute_import, division, print_function

from stdio import IO

import os
import sys
import textwrap
from uuid import uuid1 as uuid
from optparse import OptionParser, BadOptionError, Option, IndentedHelpFormatter

from core import TestBench
from stdio import IO
from tool import DirectoryUtil
from error import LockError

ROOT_IO = IO(1)


class OptionHelpFormatter(IndentedHelpFormatter):

    def format_option(self, option):
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:                       # start help on same line as opts
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            help_lines = help_text.split('\n')
            if len(help_lines) == 1:
                help_lines = textwrap.wrap(help_text, self.help_width)
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                           for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)


class AllowUndefinedOptionParser(OptionParser):
    IS_TTY = sys.stdin.isatty()

    def __init__(self,
                 usage=None,
                 option_list=None,
                 option_class=Option,
                 version=None,
                 conflict_handler="error",
                 description=None,
                 formatter=None,
                 add_help_option=True,
                 prog=None,
                 epilog=None,
                 allow_undefine=True,
                 undefine_warn=True
                 ):
        OptionParser.__init__(
            self, usage, option_list, option_class, version, conflict_handler,
            description, formatter, add_help_option, prog, epilog
        )
        self.allow_undefine = allow_undefine
        self.undefine_warn = undefine_warn

    def warn(self, msg, file=None):
        if self.IS_TTY:
            print("%s %s" % (IO.WARNING_PREV, msg))
        else:
            print('warn: %s' % msg)

    def _process_long_opt(self, rargs, values):
        try:
            value = rargs[0]
            OptionParser._process_long_opt(self, rargs, values)
        except BadOptionError as e:
            if self.allow_undefine:
                key = e.opt_str
                value = value[len(key)+1:]
                setattr(values, key.strip('-').replace('-', '_'),
                        value if value != '' else True)
                self.undefine_warn and self.warn(e)
            else:
                raise e

    def _process_short_opts(self, rargs, values):
        try:
            value = rargs[0]
            OptionParser._process_short_opts(self, rargs, values)
        except BadOptionError as e:
            if self.allow_undefine:
                key = e.opt_str
                value = value[len(key)+1:]
                setattr(values, key.strip('-').replace('-', '_'),
                        value if value != '' else True)
                self.undefine_warn and self.warn(e)
            else:
                raise e


class BaseCommand(object):

    def __init__(self, name, summary):
        self.name = name
        self.summary = summary
        self.args = []
        self.cmds = []
        self.opts = {}
        self.prev_cmd = ''
        self.is_init = False
        self.hidden = False
        self.parser = AllowUndefinedOptionParser(add_help_option=False)
        self.parser.add_option('-h', '--help', action='callback',
                               callback=self._show_help, help='Show help and exit.')
        self.parser.add_option('-v', '--verbose', action='callback',
                               callback=self._set_verbose, help='Activate verbose output.')

    def _set_verbose(self, *args, **kwargs):
        ROOT_IO.set_verbose_level(0xfffffff)

    def init(self, cmd, args):
        if self.is_init is False:
            self.prev_cmd = cmd
            self.args = args
            self.is_init = True
            self.parser.prog = self.prev_cmd
            option_list = self.parser.option_list[2:]
            option_list.append(self.parser.option_list[0])
            option_list.append(self.parser.option_list[1])
            self.parser.option_list = option_list
        return self

    def parse_command(self):
        self.opts, self.cmds = self.parser.parse_args(self.args)
        return self.opts

    def do_command(self):
        raise NotImplementedError

    def _show_help(self, *args, **kwargs):
        ROOT_IO.print(self._mk_usage())
        self.parser.exit(1)

    def _mk_usage(self):
        return self.parser.format_help(OptionHelpFormatter())


class TestBenchCommand(BaseCommand):

    HOME_PATH = os.path.join(os.getenv('HOME'), '.testbench')

    def init_home(self):
        if os.path.exists(self.HOME_PATH):
            return
        for part in ['results', 'log', 'repository']:
            part_dir = os.path.join(self.HOME_PATH, part)
            DirectoryUtil.mkdir(part_dir)

    def parse_command(self):
        super(TestBenchCommand, self).parse_command()

    def do_command(self):
        self.parse_command()
        self.init_home()
        trace_id = uuid()
        ret = False
        try:
            log_dir = os.path.join(self.HOME_PATH, 'log')
            DirectoryUtil.mkdir(log_dir)
            log_path = os.path.join(log_dir, 'testbench')
            ROOT_IO.init_trace_logger(log_path, 'testbench', trace_id)
            tb = TestBench(self.HOME_PATH, self.opts, ROOT_IO)
            ROOT_IO.track_limit += 1
            ROOT_IO.verbose('cmd: %s' % self.cmds)
            ROOT_IO.verbose('opts: %s' % self.opts)
            self._do_command(tb)
        except NotImplementedError:
            ROOT_IO.exception(
                'command \'%s\' is not implemented' % self.prev_cmd)
        except LockError:
            ROOT_IO.exception('Another app is currently holding the obd lock.')
        except SystemExit:
            pass
        except KeyboardInterrupt:
            ROOT_IO.exception('Keyboard Interrupt')
        except:
            e = sys.exc_info()[1]
            ROOT_IO.exception('Running Error: %s' % e)
        ROOT_IO.print('Trace ID: %s' % trace_id)
        return ret

    def _do_command(self, tb):
        raise NotImplementedError

    def _do_step(self, description, callback):
        ROOT_IO.start_loading(description)
        if callback():
            ROOT_IO.stop_loading('succeed')
        else:
            ROOT_IO.stop_loading('failed')
            raise RuntimeError


class MajorCommand(BaseCommand):

    def __init__(self, name, summary):
        super(MajorCommand, self).__init__(name, summary)
        self.commands = {}

    def _mk_usage(self):
        if self.commands:
            usage = ['%s <command> [options]\n\nAvailable commands:\n' %
                     self.prev_cmd]
            commands = [x for x in self.commands.values()]
            commands.sort(key=lambda x: x.name)
            for command in commands:
                usage.append("%-14s %s\n" % (command.name, command.summary))
            self.parser.set_usage('\n'.join(usage))
        return super(MajorCommand, self)._mk_usage()

    def do_command(self):
        if not self.is_init:
            ROOT_IO.error('%s command not init' % self.prev_cmd)
            raise SystemExit('command not init')
        if len(self.args) < 1:
            ROOT_IO.print(
                'You need to give some commands.\n\nTry `testbench --help` for more information.')
            self._show_help()
            return False
        base, args = self.args[0], self.args[1:]
        if base not in self.commands:
            self.parse_command()
            self._show_help()
            return False
        cmd = '%s %s' % (self.prev_cmd, base)
        ROOT_IO.track_limit += 1
        return self.commands[base].init(cmd, args).do_command()

    def register_command(self, command):
        self.commands[command.name] = command


class ClusterMajorCommand(MajorCommand):

    def __init__(self):
        super(ClusterMajorCommand, self).__init__(
            'cluster', 'Manage a local cluster, and only one cluster can be deployed.')
        self.register_command(ClusterDeployCommand())
        self.register_command(ClusterDestroyCommand())
        self.register_command(ClusterDisplayCommand())


class ClusterDeployCommand(TestBenchCommand):

    def __init__(self):
        super(ClusterDeployCommand, self).__init__(
            'deploy', 'Deploy a cluster with the given configuration file.')
        self.parser.add_option(
            '-c', '--config', type='string', help='Path to the configuration file.')
        self.parser.add_option('-m', '--monitor', action='store_true',
                               help='Monitor system status and alert abnormal behavior.')

    def _check(self):
        config = getattr(self.opts, 'config', '')
        if not config:
            ROOT_IO.error(
                'Fail to deploy a cluster without configuration file.')
            return False
        if not os.path.exists(config):
            ROOT_IO.error(
                'Configuration file {} does not exist.'.format(config))
            return False
        return True

    def _do_command(self, tb):
        self._do_step('Checking options.', self._check)
        self._do_step('Deploying the local cluster.', tb.deploy_cluster)
        self._do_step('Starting the local cluster.', tb.start_cluster)
        self._do_step('Bootstraping the local cluster.', tb.bootstrap)


class ClusterDestroyCommand(TestBenchCommand):

    def __init__(self):
        super(ClusterDestroyCommand, self).__init__(
            'destroy', 'Stop all servers and clear the workspace for each server.')

    def _do_command(self, tb):
        self._do_step('Destroying local cluster.', tb.destroy_cluster)


class ClusterDisplayCommand(TestBenchCommand):

    def __init__(self):
        super(ClusterDisplayCommand, self).__init__(
            'display', 'Display the status of each server.')

    def _do_command(self, tb):
        self._do_step('Displaying the local cluster.', tb.display_cluster)


class MainCommand(MajorCommand):

    def __init__(self):
        super(MainCommand, self).__init__('testbench', '')
        self.register_command(ClusterMajorCommand())


if __name__ == "__main__":
    ROOT_IO.track_limit += 2
    if MainCommand().init('testbench', sys.argv[1:]).do_command():
        ROOT_IO.exit(0)
    ROOT_IO.exit(1)
