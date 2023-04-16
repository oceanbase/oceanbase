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

import os
import signal
import sys
import fcntl
import traceback
import inspect2
import six
import logging
from logging import handlers

from enum import Enum
from halo import Halo, cursor
from colorama import Fore
from prettytable import PrettyTable
from progressbar import AdaptiveETA, Bar, SimpleProgress, ETA, FileTransferSpeed, Percentage, ProgressBar
from types import MethodType
from inspect2 import Parameter

from log import Logger


if sys.version_info.major == 3:
    raw_input = input
    def input(msg): return int(raw_input(msg))


class BufferIO(object):

    def __init__(self):
        self._buffer = []

    def write(self, s):
        self._buffer.append(s)

    def read(self):
        s = ''.join(self._buffer)
        self._buffer = []
        return s


class SysStdin(object):

    NONBLOCK = False
    STATS = None
    FD = None

    @classmethod
    def fileno(cls):
        if cls.FD is None:
            cls.FD = sys.stdin.fileno()
        return cls.FD

    @classmethod
    def stats(cls):
        if cls.STATS is None:
            cls.STATS = fcntl.fcntl(cls.fileno(), fcntl.F_GETFL)
        return cls.STATS

    @classmethod
    def nonblock(cls):
        if cls.NONBLOCK is False:
            fcntl.fcntl(cls.fileno(), fcntl.F_SETFL,
                        cls.stats() | os.O_NONBLOCK)
            cls.NONBLOCK = True

    @classmethod
    def block(cls):
        if cls.NONBLOCK:
            fcntl.fcntl(cls.fileno(), fcntl.F_SETFL, cls.stats())
            cls.NONBLOCK = True

    @classmethod
    def readline(cls, blocked=False):
        if blocked:
            cls.block()
        else:
            cls.nonblock()
        return cls._readline()

    @classmethod
    def read(cls, blocked=False):
        return ''.join(cls.readlines(blocked=blocked))

    @classmethod
    def readlines(cls, blocked=False):
        if blocked:
            cls.block()
        else:
            cls.nonblock()
        return cls._readlines()

    @classmethod
    def _readline(cls):
        if cls.NONBLOCK:
            try:
                for line in sys.stdin:
                    return line
            except IOError:
                return ''
            finally:
                cls.block()
        else:
            return sys.stdin.readline()

    @classmethod
    def _readlines(cls):
        if cls.NONBLOCK:
            lines = []
            try:
                for line in sys.stdin:
                    lines.append(line)
            except IOError:
                pass
            finally:
                cls.block()
            return lines
        else:
            return sys.stdin.readlines()


class FormtatText(object):

    @staticmethod
    def format(text, color):
        return color + text + Fore.RESET

    @staticmethod
    def info(text):
        return FormtatText.format(text, Fore.BLUE)

    @staticmethod
    def success(text):
        return FormtatText.format(text, Fore.GREEN)

    @staticmethod
    def warning(text):
        return FormtatText.format(text, Fore.YELLOW)

    @staticmethod
    def error(text):
        return FormtatText.format(text, Fore.RED)


class LogSymbols(Enum):

    INFO = FormtatText.info('!')
    SUCCESS = FormtatText.success('ok')
    WARNING = FormtatText.warning('!!')
    ERROR = FormtatText.error('x')


class IOTable(PrettyTable):

    @property
    def align(self):
        """Controls alignment of fields
        Arguments:

        align - alignment, one of "l", "c", or "r" """
        return self._align

    @align.setter
    def align(self, val):
        if not self._field_names:
            self._align = {}
        elif isinstance(val, dict):
            val_map = val
            for field in self._field_names:
                if field in val_map:
                    val = val_map[field]
                    self._validate_align(val)
                else:
                    val = 'l'
                self._align[field] = val
        else:
            if val:
                self._validate_align(val)
            else:
                val = 'l'
            for field in self._field_names:
                self._align[field] = val


class IOHalo(Halo):

    def __init__(self, text='', color='cyan', text_color=None, spinner='line', animation=None, placement='right', interval=-1, enabled=True, stream=sys.stdout):
        super(IOHalo, self).__init__(text=text, color=color, text_color=text_color, spinner=spinner,
                                     animation=animation, placement=placement, interval=interval, enabled=enabled, stream=stream)

    def start(self, text=None):
        if getattr(self._stream, 'isatty', lambda: False)():
            return super(IOHalo, self).start(text=text)
        else:
            text and self._stream.write(text)

    def stop_and_persist(self, symbol=' ', text=None):
        if getattr(self._stream, 'isatty', lambda: False)():
            return super(IOHalo, self).stop_and_persist(symbol=symbol, text=text)
        else:
            self._stream.write(' %s\n' % symbol)

    def succeed(self, text=None):
        return self.stop_and_persist(symbol=LogSymbols.SUCCESS.value, text=text)

    def fail(self, text=None):
        return self.stop_and_persist(symbol=LogSymbols.ERROR.value, text=text)

    def warn(self, text=None):
        return self.stop_and_persist(symbol=LogSymbols.WARNING.value, text=text)

    def info(self, text=None):
        return self.stop_and_persist(symbol=LogSymbols.INFO.value, text=text)


class IOProgressBar(ProgressBar):

    @staticmethod
    def _get_widgets(widget_type, text):
        if widget_type == 'download':
            return ['%s: ' % text, Percentage(), ' ', Bar(marker='#', left='[', right=']'), ' ', ETA(), ' ', FileTransferSpeed()]
        elif widget_type == 'timer':
            return ['%s: ' % text, Percentage(), ' ', Bar(marker='#', left='[', right=']'), ' ', AdaptiveETA()]
        elif widget_type == 'simple_progress':
            return ['%s: (' % text, SimpleProgress(sep='/'), ') ', Bar(marker='#', left='[', right=']')]
        else:
            return ['%s: ' % text, Percentage(), ' ', Bar(marker='#', left='[', right=']')]

    def __init__(self, maxval=None, text='', term_width=None, poll=1, left_justify=True, stream=None, widget_type='download'):
        super(IOProgressBar, self).__init__(maxval=maxval, widgets=self._get_widgets(
            widget_type, text), term_width=term_width, poll=poll, left_justify=left_justify, fd=stream)

    def start(self):
        self._hide_cursor()
        return super(IOProgressBar, self).start()

    def update(self, value=None):
        return super(IOProgressBar, self).update(value=value)

    def finish(self):
        if self.finished:
            return
        self._show_cursor()
        return super(IOProgressBar, self).finish()

    def interrupt(self):
        if self.finished:
            return
        self._show_cursor()
        self.finished = True
        self.fd.write('\n')
        if self.signal_set:
            signal.signal(signal.SIGWINCH, signal.SIG_DFL)

    def _need_update(self):
        return (self.currval == self.maxval or self.currval == 0 or getattr(self.fd, 'isatty', lambda: False)()) \
            and super(IOProgressBar, self)._need_update()

    def _check_stream(self):
        if self.fd.closed:
            return False
        try:
            check_stream_writable = self.fd.writable
        except AttributeError:
            pass
        else:
            return check_stream_writable()
        return True

    def _hide_cursor(self):
        """Disable the user's blinking cursor
        """
        if self._check_stream() and self.fd.isatty():
            cursor.hide(stream=self.fd)

    def _show_cursor(self):
        """Re-enable the user's blinking cursor
        """
        if self._check_stream() and self.fd.isatty():
            cursor.show(stream=self.fd)


class MsgLevel(object):

    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    VERBOSE = DEBUG
    NOTSET = 0


class IO(object):

    WIDTH = 64
    VERBOSE_LEVEL = 0
    WARNING_PREV = FormtatText.warning('[WARN]')
    ERROR_PREV = FormtatText.error('[ERROR]')
    IS_TTY = sys.stdin.isatty()
    INPUT = SysStdin

    def __init__(self,
                 level,
                 msg_lv=MsgLevel.DEBUG,
                 use_cache=False,
                 track_limit=0,
                 root_io=None,
                 stream=sys.stdout
                 ):
        self.level = level
        self.msg_lv = msg_lv
        self.log_path = None
        self.trace_id = None
        self.log_name = 'default'
        self.log_path = None
        self._trace_logger = None
        self._log_cache = [] if use_cache else None
        self._root_io = root_io
        self.track_limit = track_limit
        self._verbose_prefix = '-' * self.level
        self.sub_ios = {}
        self.sync_obj = None
        self._out_obj = None if self._root_io else stream
        self._cur_out_obj = self._out_obj
        self._before_critical = None

    def init_trace_logger(self, log_path, log_name=None, trace_id=None):
        if self._trace_logger is None:
            self.log_path = log_path
            if trace_id:
                self.trace_id = trace_id
            if log_name:
                self.log_name = log_name

    def __getstate__(self):
        state = {}
        for key in self.__dict__:
            state[key] = self.__dict__[key]
        for key in ['_trace_logger', 'sync_obj', '_out_obj', '_cur_out_obj', '_before_critical']:
            state[key] = None
        return state

    @property
    def trace_logger(self):
        if self.log_path and self._trace_logger is None:
            self._trace_logger = Logger(self.log_name)
            handler = handlers.TimedRotatingFileHandler(
                self.log_path, when='midnight', interval=1, backupCount=30)
            if self.trace_id:
                handler.setFormatter(logging.Formatter(
                    "[%%(asctime)s.%%(msecs)03d] [%s] [%%(levelname)s] %%(message)s" % self.trace_id, "%Y-%m-%d %H:%M:%S"))
            else:
                handler.setFormatter(logging.Formatter(
                    "[%%(asctime)s.%%(msecs)03d] [%%(levelname)s] %%(message)s", "%Y-%m-%d %H:%M:%S"))
            self._trace_logger.addHandler(handler)
        return self._trace_logger

    @property
    def log_cache(self):
        if self._root_io:
            self._root_io.log_cache
        return self._log_cache

    def before_close(self):
        if self._before_critical:
            try:
                self._before_critical(self)
            except:
                pass

    def _close(self):
        self.before_close()
        self._flush_log()

    def __del__(self):
        self._close()

    def exit(self, code):
        self._close()
        sys.exit(code)

    def set_cache(self, status):
        if status:
            self._cache_on()

    def _cache_on(self):
        if self._root_io:
            return False
        if self.log_cache is None:
            self._log_cache = []
        return True

    def _cache_off(self):
        if self._root_io:
            return False
        if self.log_cache is not None:
            self._flush_log()
            self._log_cache = None
        return True

    def get_cur_out_obj(self):
        if self._root_io:
            return self._root_io.get_cur_out_obj()
        return self._cur_out_obj

    def _start_buffer_io(self):
        if self._root_io:
            return False
        if self._cur_out_obj != self._out_obj:
            return False
        self._cur_out_obj = BufferIO()
        return True

    def _stop_buffer_io(self):
        if self._root_io:
            return False
        if self._cur_out_obj == self._out_obj:
            return False
        text = self._cur_out_obj.read()
        self._cur_out_obj = self._out_obj
        if text:
            self.print(text)
        return True

    @staticmethod
    def set_verbose_level(level):
        IO.VERBOSE_LEVEL = level

    def _start_sync_obj(self, sync_clz, before_critical, *arg, **kwargs):
        if self._root_io:
            return self._root_io._start_sync_obj(sync_clz, before_critical, *arg, **kwargs)
        if self.sync_obj:
            return None
        if not self._start_buffer_io():
            return None
        kwargs['stream'] = self._out_obj
        try:
            self.sync_obj = sync_clz(*arg, **kwargs)
            self._before_critical = before_critical
        except Exception as e:
            self._stop_buffer_io()
            raise e
        return self.sync_obj

    def _clear_sync_ctx(self):
        self._stop_buffer_io()
        self.sync_obj = None
        self._before_critical = None

    def _stop_sync_obj(self, sync_clz, stop_type, *arg, **kwargs):
        if self._root_io:
            ret = self._root_io._stop_sync_obj(
                sync_clz, stop_type, *arg, **kwargs)
            self._clear_sync_ctx()
        else:
            if not isinstance(self.sync_obj, sync_clz):
                return False
            try:
                ret = getattr(self.sync_obj, stop_type)(*arg, **kwargs)
            except Exception as e:
                raise e
            finally:
                self._clear_sync_ctx()
        return ret

    def start_loading(self, text, *arg, **kwargs):
        if self.sync_obj:
            return False
        self.sync_obj = self._start_sync_obj(
            IOHalo, lambda x: x.stop_loading('fail'), *arg, **kwargs)
        if self.sync_obj:
            self.log(MsgLevel.INFO, text)
            return self.sync_obj.start(text)

    def stop_loading(self, stop_type, *arg, **kwargs):
        if not isinstance(self.sync_obj, IOHalo):
            return False
        if getattr(self.sync_obj, stop_type, False):
            return self._stop_sync_obj(IOHalo, stop_type, *arg, **kwargs)
        else:
            return self._stop_sync_obj(IOHalo, 'stop')

    def update_loading_text(self, text):
        if not isinstance(self.sync_obj, IOHalo):
            return False
        self.log(MsgLevel.VERBOSE, text)
        self.sync_obj.text = text
        return self.sync_obj

    def start_progressbar(self, text, maxval, widget_type='download'):
        if self.sync_obj:
            return False
        self.sync_obj = self._start_sync_obj(IOProgressBar, lambda x: x.finish_progressbar(
        ), text=text, maxval=maxval, widget_type=widget_type)
        if self.sync_obj:
            self.log(MsgLevel.INFO, text)
            return self.sync_obj.start()

    def update_progressbar(self, value):
        if not isinstance(self.sync_obj, IOProgressBar):
            return False
        return self.sync_obj.update(value)

    def finish_progressbar(self):
        if not isinstance(self.sync_obj, IOProgressBar):
            return False
        return self._stop_sync_obj(IOProgressBar, 'finish')

    def interrupt_progressbar(self):
        if not isinstance(self.sync_obj, IOProgressBar):
            return False
        return self._stop_sync_obj(IOProgressBar, 'interrupt')

    def sub_io(self, pid=None, msg_lv=None):
        if not pid:
            pid = os.getpid()
        if msg_lv is None:
            msg_lv = self.msg_lv
        key = "%s-%s" % (pid, msg_lv)
        if key not in self.sub_ios:
            sub_io = self.__class__(
                self.level + 1,
                msg_lv=msg_lv,
                track_limit=self.track_limit,
                root_io=self._root_io if self._root_io else self
            )
            sub_io.log_name = self.log_name
            sub_io.log_path = self.log_path
            sub_io.trace_id = self.trace_id
            sub_io._trace_logger = self.trace_logger
            self.sub_ios[key] = sub_io
        return self.sub_ios[key]

    def print_list(self, ary, field_names=None, exp=lambda x: x if isinstance(x, (list, tuple)) else [x], show_index=False, start=0, **kwargs):
        if not ary:
            title = kwargs.get("title", "")
            empty_msg = kwargs.get("empty_msg", "{} is empty.".format(title))
            if empty_msg:
                self.print(empty_msg)
            return
        show_index = field_names is not None and show_index
        if show_index:
            show_index.insert(0, 'idx')
        table = IOTable(field_names, **kwargs)
        for row in ary:
            row = exp(row)
            if show_index:
                row.insert(start)
                start += 1
            table.add_row(row)
        self.print(table)

    def read(self, msg='', blocked=False):
        if msg:
            self._print(MsgLevel.INFO, msg)
        return self.INPUT.read(blocked)

    def confirm(self, msg):
        msg = '%s [y/n]: ' % msg
        self.print(msg, end='')
        if self.IS_TTY:
            while True:
                try:
                    ans = raw_input()
                    if ans == 'y':
                        return True
                    if ans == 'n':
                        return False
                except Exception as e:
                    if not e:
                        return False
        else:
            return False

    def _format(self, msg, *args):
        if args:
            msg = msg % args
        return msg

    def _print(self, msg_lv, msg, *args, **kwargs):
        if msg_lv < self.msg_lv:
            return
        kwargs['file'] = self.get_cur_out_obj()
        kwargs['file'] and print(self._format(msg, *args), **kwargs)
        del kwargs['file']
        self.log(msg_lv, msg, *args, **kwargs)

    def log(self, levelno, msg, *args, **kwargs):
        self._cache_log(levelno, msg, *args, **kwargs)

    def _cache_log(self, levelno, msg, *args, **kwargs):
        if self.trace_logger:
            log_cache = self.log_cache
            lines = str(msg).split('\n')
            for line in lines:
                if log_cache is None:
                    self._log(levelno, line, *args, **kwargs)
                else:
                    log_cache.append((levelno, line, args, kwargs))

    def _flush_log(self):
        if not self._root_io and self.trace_logger and self._log_cache:
            for levelno, line, args, kwargs in self._log_cache:
                self.trace_logger.log(levelno, line, *args, **kwargs)
            self._log_cache = []

    def _log(self, levelno, msg, *args, **kwargs):
        if self.trace_logger:
            self.trace_logger.log(levelno, msg, *args, **kwargs)

    def print(self, msg, *args, **kwargs):
        self._print(MsgLevel.INFO, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self._print(MsgLevel.WARN, '%s %s' %
                    (self.WARNING_PREV, msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._print(MsgLevel.ERROR, '%s %s' %
                    (self.ERROR_PREV, msg), *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        if self._root_io:
            return self._root_io.critical(msg, *args, **kwargs)
        self._print(MsgLevel.CRITICAL, '%s %s' %
                    (self.ERROR_PREV, msg), *args, **kwargs)
        self.exit(kwargs['code'] if 'code' in kwargs else 255)

    def verbose(self, msg, *args, **kwargs):
        if self.level > self.VERBOSE_LEVEL:
            self.log(MsgLevel.VERBOSE, '%s %s' %
                     (self._verbose_prefix, msg), *args, **kwargs)
            return
        self._print(MsgLevel.VERBOSE, '%s %s' %
                    (self._verbose_prefix, msg), *args, **kwargs)

    if sys.version_info.major == 2:
        def exception(self, msg, *args, **kwargs):
            import linecache
            exception_msg = []
            ei = sys.exc_info()
            exception_msg.append('Traceback (most recent call last):')
            stack = traceback.extract_stack()[self.track_limit:-2]
            tb = ei[2]
            while tb is not None:
                f = tb.tb_frame
                lineno = tb.tb_lineno
                co = f.f_code
                filename = co.co_filename
                name = co.co_name
                linecache.checkcache(filename)
                line = linecache.getline(filename, lineno, f.f_globals)
                tb = tb.tb_next
                stack.append((filename, lineno, name, line))
            for line in stack:
                exception_msg.append('  File "%s", line %d, in %s' % line[:3])
                if line[3]:
                    exception_msg.append('    ' + line[3].strip())
            lines = []
            for line in traceback.format_exception_only(ei[0], ei[1]):
                lines.append(line)
            if lines:
                exception_msg.append(''.join(lines))
            if self.level <= self.VERBOSE_LEVEL:
                def print_stack(m): return self._print(MsgLevel.ERROR, m)
            else:
                def print_stack(m): return self.log(MsgLevel.ERROR, m)
            msg and self.error(msg)
            print_stack('\n'.join(exception_msg))
    else:
        def exception(self, msg, *args, **kwargs):
            ei = sys.exc_info()
            traceback_e = traceback.TracebackException(
                type(ei[1]), ei[1], ei[2], limit=None)
            pre_stach = traceback.extract_stack()[self.track_limit:-2]
            pre_stach.reverse()
            for summary in pre_stach:
                traceback_e.stack.insert(0, summary)
            lines = []
            for line in traceback_e.format(chain=True):
                lines.append(line)
            if self.level <= self.VERBOSE_LEVEL:
                def print_stack(m): return self._print(MsgLevel.ERROR, m)
            else:
                def print_stack(m): return self.log(MsgLevel.ERROR, m)
            msg and self.error(msg)
            print_stack(''.join(lines))


class _Empty(object):
    pass


EMPTY = _Empty()
del _Empty


class FakeReturn(object):

    def __call__(self, *args, **kwargs):
        return None

    def __len__(self):
        return 0


FAKE_RETURN = FakeReturn()


class StdIO(object):

    def __init__(self, io=None):
        self.io = io
        self._attrs = {}
        self._warn_func = getattr(self.io, "warn", print)

    def __getattr__(self, item):
        if item.startswith('__'):
            return super(StdIO, self).__getattribute__(item)
        if self.io is None:
            return FAKE_RETURN
        if item not in self._attrs:
            attr = getattr(self.io, item, EMPTY)
            if attr is not EMPTY:
                self._attrs[item] = attr
            else:
                self._warn_func(FormtatText.warning(
                    "WARNING: {} has no attribute '{}'".format(self.io, item)))
                self._attrs[item] = FAKE_RETURN
        return self._attrs[item]


FAKE_IO = StdIO()


def get_stdio(io_obj):
    if io_obj is None:
        return FAKE_IO
    elif isinstance(io_obj, StdIO):
        return io_obj
    else:
        return StdIO(io_obj)


def safe_stdio_decorator(default_stdio=None):

    def decorated(func):
        is_bond_method = False
        _type = None
        if isinstance(func, (staticmethod, classmethod)):
            is_bond_method = True
            _type = type(func)
            func = func.__func__
        all_parameters = inspect2.signature(func).parameters
        if "stdio" in all_parameters:
            default_stdio_in_params = all_parameters["stdio"].default
            if not isinstance(default_stdio_in_params, Parameter.empty):
                _default_stdio = default_stdio_in_params or default_stdio

            def func_wrapper(*args, **kwargs):
                _params_keys = list(all_parameters.keys())
                _index = _params_keys.index("stdio")
                if "stdio" not in kwargs and len(args) > _index:
                    stdio = get_stdio(args[_index])
                    tmp_args = list(args)
                    tmp_args[_index] = stdio
                    args = tuple(tmp_args)
                else:
                    stdio = get_stdio(kwargs.get("stdio", _default_stdio))
                    kwargs["stdio"] = stdio
                return func(*args, **kwargs)
            return _type(func_wrapper) if is_bond_method else func_wrapper
        else:
            return _type(func) if is_bond_method else func
    return decorated


class SafeStdioMeta(type):

    @staticmethod
    def _init_wrapper_func(func):
        def wrapper(*args, **kwargs):
            setattr(args[0], "_wrapper_func", {})
            func(*args, **kwargs)
            if "stdio" in args[0].__dict__:
                args[0].__dict__["stdio"] = get_stdio(
                    args[0].__dict__["stdio"])

        if func.__name__ != wrapper.__name__:
            return wrapper
        else:
            return func

    def __new__(mcs, name, bases, attrs):

        for key, attr in attrs.items():
            if key.startswith("__") and key.endswith("__"):
                continue
            if isinstance(attr, (staticmethod, classmethod)):
                attrs[key] = safe_stdio_decorator()(attr)
        cls = type.__new__(mcs, name, bases, attrs)
        cls.__init__ = mcs._init_wrapper_func(cls.__init__)
        return cls


class _StayTheSame(object):
    pass


STAY_THE_SAME = _StayTheSame()


class SafeStdio(six.with_metaclass(SafeStdioMeta)):
    _wrapper_func = {}

    def __getattribute__(self, item):
        _wrapper_func = super(
            SafeStdio, self).__getattribute__("_wrapper_func")
        if item not in _wrapper_func:
            attr = super(SafeStdio, self).__getattribute__(item)
            if (not item.startswith("__") or not item.endswith("__")) and isinstance(attr, MethodType):
                if "stdio" in inspect2.signature(attr).parameters:
                    _wrapper_func[item] = safe_stdio_decorator(
                        default_stdio=getattr(self, "stdio", None))(attr)
                    return _wrapper_func[item]
            _wrapper_func[item] = STAY_THE_SAME
            return attr
        if _wrapper_func[item] is STAY_THE_SAME:
            return super(SafeStdio, self).__getattribute__(item)
        return _wrapper_func[item]

    def __setattr__(self, key, value):
        if key in self._wrapper_func:
            del self._wrapper_func[key]
        return super(SafeStdio, self).__setattr__(key, value)
