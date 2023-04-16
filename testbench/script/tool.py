
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
import bz2
import sys
import stat
import gzip
import fcntl
import signal
import shutil
import re
import json
import hashlib
from io import BytesIO

from ruamel.yaml import YAML

from stdio import SafeStdio
_open = open
if sys.version_info.major == 2:
    from collections import OrderedDict
    from backports import lzma
    from io import open as _open

    def encoding_open(path, _type, encoding=None, *args, **kwrags):
        if encoding:
            kwrags['encoding'] = encoding
            return _open(path, _type, *args, **kwrags)
        else:
            return open(path, _type, *args, **kwrags)

    class TimeoutError(OSError):

        def __init__(self, *args, **kwargs):
            super(TimeoutError, self).__init__(*args, **kwargs)

else:
    import lzma
    encoding_open = open

    class OrderedDict(dict):
        pass


__all__ = ("timeout", "DynamicLoading", "ConfigUtil", "DirectoryUtil",
           "FileUtil", "YamlLoader", "OrderedDict", "COMMAND_ENV")

_WINDOWS = os.name == 'nt'


class Timeout(object):

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def _is_timeout(self):
        return self.seconds and self.seconds > 0

    def __enter__(self):
        if self._is_timeout():
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        if self._is_timeout():
            signal.alarm(0)


timeout = Timeout


class Timeout:

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def _is_timeout(self):
        return self.seconds and self.seconds > 0

    def __enter__(self):
        if self._is_timeout():
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        if self._is_timeout():
            signal.alarm(0)


timeout = Timeout


class DynamicLoading(object):

    class Module(object):

        def __init__(self, module):
            self.module = module
            self.count = 0

    LIBS_PATH = {}
    MODULES = {}

    @staticmethod
    def add_lib_path(lib):
        if lib not in DynamicLoading.LIBS_PATH:
            DynamicLoading.LIBS_PATH[lib] = 0
        if DynamicLoading.LIBS_PATH[lib] == 0:
            sys.path.insert(0, lib)
        DynamicLoading.LIBS_PATH[lib] += 1

    @staticmethod
    def add_libs_path(libs):
        for lib in libs:
            DynamicLoading.add_lib_path(lib)

    @staticmethod
    def remove_lib_path(lib):
        if lib not in DynamicLoading.LIBS_PATH:
            return
        if DynamicLoading.LIBS_PATH[lib] < 1:
            return
        try:
            DynamicLoading.LIBS_PATH[lib] -= 1
            if DynamicLoading.LIBS_PATH[lib] == 0:
                idx = sys.path.index(lib)
                del sys.path[idx]
        except:
            pass

    @staticmethod
    def remove_libs_path(libs):
        for lib in libs:
            DynamicLoading.remove_lib_path(lib)

    @staticmethod
    def import_module(name, stdio=None):
        if name not in DynamicLoading.MODULES:
            try:
                stdio and getattr(stdio, 'verbose', print)('import %s' % name)
                module = __import__(name)
                DynamicLoading.MODULES[name] = DynamicLoading.Module(module)
            except:
                stdio and getattr(stdio, 'exception', print)(
                    'import %s failed' % name)
                stdio and getattr(stdio, 'verbose', print)(
                    'sys.path: %s' % sys.path)
                return None
        DynamicLoading.MODULES[name].count += 1
        stdio and getattr(stdio, 'verbose', print)(
            'add %s ref count to %s' % (name, DynamicLoading.MODULES[name].count))
        return DynamicLoading.MODULES[name].module

    @staticmethod
    def export_module(name, stdio=None):
        if name not in DynamicLoading.MODULES:
            return
        if DynamicLoading.MODULES[name].count < 1:
            return
        try:
            DynamicLoading.MODULES[name].count -= 1
            stdio and getattr(stdio, 'verbose', print)(
                'sub %s ref count to %s' % (name, DynamicLoading.MODULES[name].count))
            if DynamicLoading.MODULES[name].count == 0:
                stdio and getattr(stdio, 'verbose', print)('export %s' % name)
                del sys.modules[name]
                del DynamicLoading.MODULES[name]
        except:
            stdio and getattr(stdio, 'exception', print)(
                'export %s failed' % name)


class ConfigUtil(object):

    @staticmethod
    def get_value_from_dict(conf, key, default=None, transform_func=None):
        try:
            # 不要使用 conf.get(key, default)来替换，这里还有类型转换的需求
            value = conf[key]
            return transform_func(value) if value is not None and transform_func else value
        except:
            return default

    @staticmethod
    def get_list_from_dict(conf, key, transform_func=None):
        try:
            return_list = conf[key]
            if transform_func:
                return [transform_func(value) for value in return_list]
            else:
                return return_list
        except:
            return []


class DirectoryUtil(object):

    @staticmethod
    def list_dir(path, stdio=None):
        files = []
        if os.path.isdir(path):
            for fn in os.listdir(path):
                fp = os.path.join(path, fn)
                if os.path.isdir(fp):
                    files += DirectoryUtil.list_dir(fp)
                else:
                    files.append(fp)
        return files

    @staticmethod
    def copy(src, dst, stdio=None):
        if not os.path.isdir(src):
            stdio and getattr(stdio, 'error', print)(
                "cannot copy tree '%s': not a directory" % src)
            return False
        try:
            names = os.listdir(src)
        except:
            stdio and getattr(stdio, 'exception', print)(
                "error listing files in '%s':" % (src))
            return False

        if DirectoryUtil.mkdir(dst, stdio):
            return False

        ret = True
        links = []
        for n in names:
            src_name = os.path.join(src, n)
            dst_name = os.path.join(dst, n)
            if os.path.islink(src_name):
                link_dest = os.readlink(src_name)
                links.append((link_dest, dst_name))

            elif os.path.isdir(src_name):
                ret = DirectoryUtil.copy(src_name, dst_name, stdio) and ret
            else:
                FileUtil.copy(src_name, dst_name, stdio)
        for link_dest, dst_name in links:
            FileUtil.symlink(link_dest, dst_name, stdio)
        return ret

    @staticmethod
    def mkdir(path, mode=0o755, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('mkdir %s' % path)
        try:
            os.makedirs(path, mode=mode)
            return True
        except OSError as e:
            if e.errno == 17:
                return True
            elif e.errno == 20:
                stdio and getattr(stdio, 'error', print)(
                    '%s is not a directory', path)
            else:
                stdio and getattr(stdio, 'error', print)(
                    'failed to create directory %s', path)
            stdio and getattr(stdio, 'exception', print)('')
        except:
            stdio and getattr(stdio, 'exception', print)('')
            stdio and getattr(stdio, 'error', print)(
                'failed to create directory %s', path)
        return False

    @staticmethod
    def rm(path, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('rm %s' % path)
        try:
            if os.path.exists(path):
                if os.path.islink(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path)
            return True
        except Exception as e:
            stdio and getattr(stdio, 'exception', print)('')
            stdio and getattr(stdio, 'error', print)(
                'failed to remove %s', path)
        return False


class FileUtil(object):

    COPY_BUFSIZE = 1024 * 1024 if _WINDOWS else 64 * 1024

    @staticmethod
    def checksum(target_path, stdio=None):
        from client import LocalClient
        if not os.path.isfile(target_path):
            info = 'No such file: ' + target_path
            if stdio:
                getattr(stdio, 'error', print)(info)
                return False
            else:
                raise IOError(info)
        ret = LocalClient.execute_command(
            'md5sum {}'.format(target_path), stdio=stdio)
        if ret:
            return ret.stdout.strip().split(' ')[0].encode('utf-8')
        else:
            m = hashlib.md5()
            with open(target_path, 'rb') as f:
                m.update(f.read())
            return m.hexdigest().encode(sys.getdefaultencoding())

    @staticmethod
    def copy_fileobj(fsrc, fdst):
        fsrc_read = fsrc.read
        fdst_write = fdst.write
        while True:
            buf = fsrc_read(FileUtil.COPY_BUFSIZE)
            if not buf:
                break
            fdst_write(buf)

    @staticmethod
    def copy(src, dst, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('copy %s %s' % (src, dst))
        if os.path.exists(src) and os.path.exists(dst) and os.path.samefile(src, dst):
            info = "`%s` and `%s` are the same file" % (src, dst)
            if stdio:
                getattr(stdio, 'error', print)(info)
                return False
            else:
                raise IOError(info)

        for fn in [src, dst]:
            try:
                st = os.stat(fn)
            except OSError:
                pass
            else:
                if stat.S_ISFIFO(st.st_mode):
                    info = "`%s` is a named pipe" % fn
                    if stdio:
                        getattr(stdio, 'error', print)(info)
                        return False
                    else:
                        raise IOError(info)

        try:
            if os.path.islink(src):
                FileUtil.symlink(os.readlink(src), dst)
                return True
            with FileUtil.open(src, 'rb') as fsrc, FileUtil.open(dst, 'wb') as fdst:
                FileUtil.copy_fileobj(fsrc, fdst)
                os.chmod(dst, os.stat(src).st_mode)
                return True
        except Exception as e:
            if int(getattr(e, 'errno', -1)) == 26:
                from client import LocalClient
                if LocalClient.execute_command('/usr/bin/cp -f %s %s' % (src, dst), stdio=stdio):
                    return True
            elif stdio:
                getattr(stdio, 'exception', print)('copy error: %s' % e)
            else:
                raise e
        return False

    @staticmethod
    def symlink(src, dst, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('link %s %s' % (src, dst))
        try:
            if DirectoryUtil.rm(dst, stdio):
                os.symlink(src, dst)
                return True
        except Exception as e:
            if stdio:
                getattr(stdio, 'exception', print)('link error: %s' % e)
            else:
                raise e
        return False

    @staticmethod
    def open(path, _type='r', encoding=None, stdio=None):
        stdio and getattr(stdio, 'verbose', print)(
            'open %s for %s' % (path, _type))
        if os.path.exists(path):
            if os.path.isfile(path):
                return encoding_open(path, _type, encoding=encoding)
            info = '%s is not file' % path
            if stdio:
                getattr(stdio, 'error', print)(info)
                return None
            else:
                raise IOError(info)
        dir_path, file_name = os.path.split(path)
        if not dir_path or DirectoryUtil.mkdir(dir_path, stdio=stdio):
            return encoding_open(path, _type, encoding=encoding)
        info = '%s is not file' % path
        if stdio:
            getattr(stdio, 'error', print)(info)
            return None
        else:
            raise IOError(info)

    @staticmethod
    def unzip(source, ztype=None, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('unzip %s' % source)
        if not ztype:
            ztype = source.split('.')[-1]
        try:
            if ztype == 'bz2':
                s_fn = bz2.BZ2File(source, 'r')
            elif ztype == 'xz':
                s_fn = lzma.LZMAFile(source, 'r')
            elif ztype == 'gz':
                s_fn = gzip.GzipFile(source, 'r')
            else:
                s_fn = open(source, 'r')
            return s_fn
        except:
            stdio and getattr(stdio, 'exception', print)(
                'failed to unzip %s' % source)
        return None

    @staticmethod
    def rm(path, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('rm %s' % path)
        if not os.path.exists(path):
            return True
        try:
            os.remove(path)
            return True
        except:
            stdio and getattr(stdio, 'exception', print)(
                'failed to remove %s' % path)
        return False

    @staticmethod
    def move(src, dst, stdio=None):
        return shutil.move(src, dst)

    @staticmethod
    def share_lock_obj(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)(
            'try to get share lock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return obj

    @classmethod
    def share_lock(cls, path, _type='w', stdio=None):
        return cls.share_lock_obj(cls.open(path, _type=_type, stdio=stdio))

    @staticmethod
    def exclusive_lock_obj(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)(
            'try to get exclusive lock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return obj

    @classmethod
    def exclusive_lock(cls, path, _type='w', stdio=None):
        return cls.exclusive_lock_obj(cls.open(path, _type=_type, stdio=stdio))

    @staticmethod
    def unlock(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('unlock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_UN)
        return obj

    @staticmethod
    def isexecutable(obj):
        return (os.path.isfile(obj) or os.path.islink(obj)) and os.access(obj, os.X_OK)


class YamlLoader(YAML):

    def __init__(self, stdio=None, typ=None, pure=False, output=None, plug_ins=None):
        super(YamlLoader, self).__init__(
            typ=typ, pure=pure, output=output, plug_ins=plug_ins)
        self.stdio = stdio
        if not self.Representer.yaml_multi_representers and self.Representer.yaml_representers:
            self.Representer.yaml_multi_representers = self.Representer.yaml_representers

    def load(self, stream):
        try:
            return super(YamlLoader, self).load(stream)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('Parsing error:\n%s' % e)
            raise e

    def loads(self, yaml_content):
        try:
            stream = BytesIO()
            yaml_content = str(yaml_content).encode()
            stream.write(yaml_content)
            stream.seek(0)
            return self.load(stream)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('Parsing error:\n%s' % e)
            raise e

    def dump(self, data, stream=None, transform=None):
        try:
            return super(YamlLoader, self).dump(data, stream=stream, transform=transform)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('dump error:\n%s' % e)
            raise e

    def dumps(self, data, transform=None):
        try:
            stream = BytesIO()
            self.dump(data, stream=stream, transform=transform)
            stream.seek(0)
            content = stream.read()
            if sys.version_info.major == 2:
                return content
            return content.decode()
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('dumps error:\n%s' % e)
            raise e


_KEYCRE = re.compile(r"\$(\w+)")


def var_replace(string, var, pattern=_KEYCRE):
    if not var:
        return string
    done = []

    while string:
        m = pattern.search(string)
        if not m:
            done.append(string)
            break

        varname = m.group(1).lower()
        replacement = var.get(varname, m.group())

        start, end = m.span()
        done.append(string[:start])
        done.append(str(replacement))
        string = string[end:]

    return ''.join(done)


class CommandEnv(SafeStdio):

    def __init__(self):
        self.source_path = None
        self._env = os.environ.copy()
        self._cmd_env = {}

    def load(self, source_path, stdio=None):
        if self.source_path:
            stdio.error("Source path of env already set.")
            return False
        self.source_path = source_path
        try:
            if os.path.exists(source_path):
                with FileUtil.open(source_path, 'r') as f:
                    self._cmd_env = json.load(f)
        except:
            stdio.exception(
                "Failed to load environments from {}".format(source_path))
            return False
        return True

    def save(self, stdio=None):
        if self.source_path is None:
            stdio.error("Command environments need to load at first.")
            return False
        stdio.verbose("save environment variables {}".format(self._cmd_env))
        try:
            with FileUtil.open(self.source_path, 'w', stdio=stdio) as f:
                json.dump(self._cmd_env, f)
        except:
            stdio.exception('Failed to save environment variables')
            return False
        return True

    def get(self, key, default=""):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def set(self, key, value, save=False, stdio=None):
        stdio.verbose(
            "set environment variable {} value {}".format(key, value))
        self._cmd_env[key] = str(value)
        if save:
            return self.save(stdio=stdio)
        return True

    def delete(self, key, save=False, stdio=None):
        stdio.verbose("delete environment variable {}".format(key))
        if key in self._cmd_env:
            del self._cmd_env[key]
        if save:
            return self.save(stdio=stdio)
        return True

    def clear(self, save=True, stdio=None):
        self._cmd_env = {}
        if save:
            return self.save(stdio=stdio)
        return True

    def __getitem__(self, item):
        value = self._cmd_env.get(item)
        if value is None:
            value = self._env.get(item)
        if value is None:
            raise KeyError(item)
        return value

    def __contains__(self, item):
        if item in self._cmd_env:
            return True
        elif item in self._env:
            return True
        else:
            return False

    def copy(self):
        result = dict(self._env)
        result.update(self._cmd_env)
        return result

    def show_env(self):
        return self._cmd_env


COMMAND_ENV = CommandEnv()
