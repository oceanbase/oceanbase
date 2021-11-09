#!/usr/bin/env python2
# -*- mode: python;coding: utf-8 -*-

# dooba ---
#
# Filename: dooba
# Description: `dooba' is a easy tools monitoring oceanbase cluster for
#               oceanbase admins. It's based on python curses library, and is a
#               powerful tool for watching oceanbase cluster status with
#               straightfoward vision.


# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License
# as published by the Free Software Foundation; either version 3, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this program; see the file COPYING. If not, see
# <http://www.gnu.org/licenses/>. 
#

# Code:

from collections import deque
from datetime import datetime, timedelta, date
from errno import *
from getopt import getopt, GetoptError
from locale import setlocale, LC_ALL
from os import environ, read, setsid
from pprint import pformat
from random import shuffle
from subprocess import Popen, PIPE, STDOUT, call
from telnetlib import Telnet
from threading import Thread
from time import sleep, strftime
from urllib2 import urlopen, URLError
import BaseHTTPServer
import atexit
import bz2
import curses
import curses.textpad
import itertools
import json
import math
import os
import select
import signal
import socket
import struct
import sys
import tempfile
import textwrap
import types
import traceback
import time
import pdb

f_data_log = ''
def seconds(td):
    return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / (10**6);


def check_datalog(datalog, tenantid=1):
    global f_data_log

    d = os.path.split(datalog)[0]
    l = os.path.split(datalog)[1]

    now = datetime.now()
    l = l + "." + str(tenantid) + "_" + now.strftime("%Y%m%d-%H%M%S")

    if not os.path.isdir(d):
        return False
    try:
        f = open(d+"/"+l, "w")
        f.write("----")
        f.close()
    except:
        f.close()
        return False

    f_data_log = d+"/"+l

    return True

class Global:
    MAX_LINES = 900
    WIDGET_HEIGHT = 7
    DEFAULT_USER = ''
    DEFAULT_PASS = ''




class Options(object):
    host = '127.0.0.1'
    port = 2881
    user = root
    password = ""
    database = "oceanbase"
    supermode = False
    interval = 1
    dataid = 0
    using_ip_port = False
    env = 'unknown'
    machine_interval = 5
    degradation = False
    show_win_file = None
    daemon = False
    http = False
    daemon_action = 'start'
    http_port = 33244
    debug = False
    datalog = ''

    def __str__(self):
        result = {}
        for k in dir(self):
            v = getattr(self, k)
            if k.find('__') == 0:
                continue
            result[k] = v
        return pformat(result)

# auxiliary functions
class ColumnFactory(object):
    def __init__(self, svr, ip):
        DEBUG(self.__init__, "ColumnFactory.__init__(svr='%s',ip='%s') " % (svr, ip), "")
        self.__svr = svr
        self.__ip = ip

    def count(self, name, sname, obname, duration=True, enable=False):
        DEBUG(self.count, "ColumnFactory.count(name='%s',sname='%s',obname='%s' " % (name, sname, obname), "" )
        if type(obname) == str:
            pass
        elif type(obname) == list:
            obname = [name for name in obname]
        else:
            raise Exception("unsupport type %s" % type(obname))

        return self.count0(name, sname, obname, duration, enable)

    def count0(self, name, sname, obname,duration=True, enable=False):

        svr = self.__svr
        ip = self.__ip
        def calc_func(stat,ip=ip):
            def try_get(d, k, b):
                try:
                    if d.has_key(k):
                        return d['%s' % k]
                    else:
                        return d['%s' % b]
                except Exception as e:
                    raise e
            try:

                if type(obname) == str:
                    return try_get(stat, svr, oceanbase.get_current_tenant())[ip][obname]
                elif type(obname) == list:
                    return sum([try_get(stat, svr, oceanbase.get_current_tenant())[ip][name] for name in obname])
                else:
                    raise Exception("ColumnFactory.count0 unsupport type %s" % type(obname))
            except KeyError as e:
                DEBUG(calc_func, "ColumnFactory.count0( stat='%s', ip='%s') exception :" % (stat, ip) ,  e)
                pass

        return Column(name, calc_func, 7, duration=duration, enable=enable, sname=sname)

    def time(self, name, sname, obnamet, obnamec=None, duration=False, enable=False):
        if obnamec is None:
            obnamec = obnamet
        return self.time0(name, sname, obnamet, obnamec, duration=duration,enable=enable)

    def time0(self, name, sname, obnamet, obnamec=None, duration=False, enable=False):
        DEBUG(self.time0, "ColumnFactory.time0(svr='%s',ip='%s',name='%s',sname='%s',obnamet='%s', obnamec='%s')" % (self.__svr, self.__ip, name, sname, obnamet, obnamec) , "")

        svr = self.__svr
        ip = self.__ip


        def calc_func(stat,ip=ip):
            def try_get(d, k, b):
                if d.has_key(k):
                    return d['%s' % k]
                else:
                    return d['%s' % b]
            try:

                if type(obnamec) == str:
                    total_count = try_get(stat, svr, oceanbase.get_current_tenant())[ip][obnamec]
                elif type(obnamec) == list:
                    total_count = sum([try_get(stat, svr, oceanbase.get_current_tenant())[ip][name] for name in obnamec])

                return try_get(stat, svr, oceanbase.get_current_tenant())[ip][obnamet] / 1000 / float(total_count or 1)
            except Exception as e:
                DEBUG(calc_func, "ColumnFactory.time0.calc_func(stat='%s',ip='%s') exception:" % (stat, ip), e)
                pass

        return Column(name, calc_func, 7, duration=duration, enable=enable, sname=sname)

    def cache(self, name, sname, obname):
        svr = self.__svr
        ip = self.__ip
        return Column(name, lambda stat,ip=ip: stat[svr][ip][obname+"_cache_hit"]  / float(stat[svr][ip][obname+"_cache_hit"] + stat[svr][ip][obname+"_cache_miss"] or 1),
                      7, enable=False, sname=sname)


def mem_str(mem_int, bit=False):
    mem_int = int(mem_int)
    if mem_int < 1024:
        return str(mem_int) + (bit and 'b' or '')
    mem_int = float(mem_int)
    mem_int /= 1024
    if mem_int < 1024:
        return "%.1fK" % mem_int + (bit and 'b' or '')
    mem_int /= 1024
    if mem_int < 1024:
        return "%.1fM" % mem_int + (bit and 'b' or '')
    mem_int /= 1024
    if mem_int < 1024:
        return "%.2fG" % mem_int + (bit and 'b' or '')
    mem_int /= 1024
    if mem_int < 1024:
        return "%.2fT" % mem_int + (bit and 'b' or '')
    return "UNKNOW"

def count_str(count_int, kilo=True):
    return str(count_int)

def percent_str(percent_int):
    return str(round(float(percent_int) * 100,1)) + "%"

class Cowsay(object):
    '''Copyright 2011 Jesse Chan-Norris <jcn@pith.org>
       https://github.com/jcn/cowsay-py/blob/master/cowsay.py'''
    def __init__(self, str, length=40):
        self.__result = self.build_bubble(str, length) + self.build_cow()

    def __str__(self):
        return self.__result

    def build_cow(self):
        return """
         \   ^__^
          \  (oo)\_______
             (__)\       )\/\\
                 ||----w |
                 ||     ||
                """

    def build_bubble(self, str, length=40):
        bubble = []
        lines = self.normalize_text(str, length)
        bordersize = len(lines[0])
        bubble.append("  " + "_" * bordersize)

        for index, line in enumerate(lines):
            border = self.get_border(lines, index)
            bubble.append("%s %s %s" % (border[0], line, border[1]))
            bubble.append("  " + "-" * bordersize)

        return "\n".join(bubble)

    def normalize_text(self, str, length):
        lines  = textwrap.wrap(str, length)
        maxlen = len(max(lines, key=len))
        return [ line.ljust(maxlen) for line in lines ]

    def get_border(self, lines, index):
        if len(lines) < 2:
            return [ "<", ">" ]
        elif index == 0:
            return [ "/", "\\" ]
        elif index == len(lines) - 1:
            return [ "\\", "/" ]
        else:
            return [ "|", "|" ]


class Daemon:
        """
        A generic daemon class.

        Usage: subclass the Daemon class and override the run() method
        """
        def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                self.stdin = stdin
                self.stdout = stdout
                self.stderr = stderr
                self.pidfile = pidfile

        def daemonize(self):
                """
                do the UNIX double-fork magic, see Stevens' "Advanced
                Programming in the UNIX Environment" for details (ISBN 0201563177)
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
                """
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit first parent
                                sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # decouple from parent environment
                os.chdir("/")
                os.setsid()
                os.umask(0)

                # do second fork
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit from second parent
                                sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # redirect standard file descriptors
                sys.stdout.flush()
                sys.stderr.flush()
                si = file(self.stdin, 'r')
                so = file(self.stdout, 'a+')
                se = file(self.stderr, 'a+', 0)
                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

                # write pidfile
                atexit.register(self.delpid)
                pid = str(os.getpid())
                file(self.pidfile,'w+').write("%s\n" % pid)

        def delpid(self):
                os.remove(self.pidfile)

        def start(self):
                """
                Start the daemon
                """
                # Check for a pidfile to see if the daemon already runs
                try:
                        pf = file(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None

                if pid:
                        message = "pidfile %s already exist. Daemon already running?\n"
                        sys.stderr.write(message % self.pidfile)
                        sys.exit(1)

                # Start the daemon
                self.daemonize()
                self.run()

        def stop(self):
                """
                Stop the daemon
                """
                # Get the pid from the pidfile
                try:
                        pf = file(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None

                if not pid:
                        message = "pidfile %s does not exist. Daemon not running?\n"
                        sys.stderr.write(message % self.pidfile)
                        return # not an error in a restart

                # Try killing the daemon process
                try:
                        while 1:
                                os.kill(pid, signal.SIGTERM)
                                sleep(0.1)
                except OSError, err:
                        err = str(err)
                        if err.find("No such process") > 0:
                                if os.path.exists(self.pidfile):
                                        os.remove(self.pidfile)
                        else:
                                print str(err)
                                sys.exit(1)

        def restart(self):
                """
                Restart the daemon
                """
                self.stop()
                self.start()

        def run(self):
                """
                You should override this method when you subclass Daemon. It will be called after the process has been
                daemonized by start() or restart().
                """


class Blowfish:

    """Blowfish encryption Scheme

    This class implements the encryption and decryption
    functionality of the Blowfish cipher.

    Public functions:

        def __init__ (self, key)
            Creates an instance of blowfish using 'key'
            as the encryption key. Key is a string of
            length ranging from 8 to 56 bytes (64 to 448
            bits). Once the instance of the object is
            created, the key is no longer necessary.

        def encrypt (self, data):
            Encrypt an 8 byte (64-bit) block of text
            where 'data' is an 8 byte string. Returns an
            8-byte encrypted string.

        def decrypt (self, data):
            Decrypt an 8 byte (64-bit) encrypted block
            of text, where 'data' is the 8 byte encrypted
            string. Returns an 8-byte string of plaintext.

        def cipher (self, xl, xr, direction):
            Encrypts a 64-bit block of data where xl is
            the upper 32-bits and xr is the lower 32-bits.
            'direction' is the direction to apply the
            cipher, either ENCRYPT or DECRYPT constants.
            returns a tuple of either encrypted or decrypted
            data of the left half and right half of the
            64-bit block.

        def initCTR(self):
            Initializes CTR engine for encryption or decryption.

        def encryptCTR(self, data):
            Encrypts an arbitrary string and returns the
            encrypted string. The method can be called successively
            for multiple string blocks.

        def decryptCTR(self, data):
            Decrypts a string encrypted with encryptCTR() and
            returns the decrypted string.

    Private members:

        def __round_func (self, xl)
            Performs an obscuring function on the 32-bit
            block of data 'xl', which is the left half of
            the 64-bit block of data. Returns the 32-bit
            result as a long integer.

    """

    # Cipher directions
    ENCRYPT = 0
    DECRYPT = 1

    # For the __round_func
    modulus = long (2) ** 32

    def __init__ (self, key):

        if not key or len (key) < 8 or len (key) > 56:
            raise RuntimeError, "Attempted to initialize Blowfish cipher with key of invalid length: %s" % len (key)

        self.p_boxes = [
            0x243F6A88, 0x85A308D3, 0x13198A2E, 0x03707344,
            0xA4093822, 0x299F31D0, 0x082EFA98, 0xEC4E6C89,
            0x452821E6, 0x38D01377, 0xBE5466CF, 0x34E90C6C,
            0xC0AC29B7, 0xC97C50DD, 0x3F84D5B5, 0xB5470917,
            0x9216D5D9, 0x8979FB1B
        ]

        self.s_boxes = [
            [
                0xD1310BA6, 0x98DFB5AC, 0x2FFD72DB, 0xD01ADFB7,
                0xB8E1AFED, 0x6A267E96, 0xBA7C9045, 0xF12C7F99,
                0x24A19947, 0xB3916CF7, 0x0801F2E2, 0x858EFC16,
                0x636920D8, 0x71574E69, 0xA458FEA3, 0xF4933D7E,
                0x0D95748F, 0x728EB658, 0x718BCD58, 0x82154AEE,
                0x7B54A41D, 0xC25A59B5, 0x9C30D539, 0x2AF26013,
                0xC5D1B023, 0x286085F0, 0xCA417918, 0xB8DB38EF,
                0x8E79DCB0, 0x603A180E, 0x6C9E0E8B, 0xB01E8A3E,
                0xD71577C1, 0xBD314B27, 0x78AF2FDA, 0x55605C60,
                0xE65525F3, 0xAA55AB94, 0x57489862, 0x63E81440,
                0x55CA396A, 0x2AAB10B6, 0xB4CC5C34, 0x1141E8CE,
                0xA15486AF, 0x7C72E993, 0xB3EE1411, 0x636FBC2A,
                0x2BA9C55D, 0x741831F6, 0xCE5C3E16, 0x9B87931E,
                0xAFD6BA33, 0x6C24CF5C, 0x7A325381, 0x28958677,
                0x3B8F4898, 0x6B4BB9AF, 0xC4BFE81B, 0x66282193,
                0x61D809CC, 0xFB21A991, 0x487CAC60, 0x5DEC8032,
                0xEF845D5D, 0xE98575B1, 0xDC262302, 0xEB651B88,
                0x23893E81, 0xD396ACC5, 0x0F6D6FF3, 0x83F44239,
                0x2E0B4482, 0xA4842004, 0x69C8F04A, 0x9E1F9B5E,
                0x21C66842, 0xF6E96C9A, 0x670C9C61, 0xABD388F0,
                0x6A51A0D2, 0xD8542F68, 0x960FA728, 0xAB5133A3,
                0x6EEF0B6C, 0x137A3BE4, 0xBA3BF050, 0x7EFB2A98,
                0xA1F1651D, 0x39AF0176, 0x66CA593E, 0x82430E88,
                0x8CEE8619, 0x456F9FB4, 0x7D84A5C3, 0x3B8B5EBE,
                0xE06F75D8, 0x85C12073, 0x401A449F, 0x56C16AA6,
                0x4ED3AA62, 0x363F7706, 0x1BFEDF72, 0x429B023D,
                0x37D0D724, 0xD00A1248, 0xDB0FEAD3, 0x49F1C09B,
                0x075372C9, 0x80991B7B, 0x25D479D8, 0xF6E8DEF7,
                0xE3FE501A, 0xB6794C3B, 0x976CE0BD, 0x04C006BA,
                0xC1A94FB6, 0x409F60C4, 0x5E5C9EC2, 0x196A2463,
                0x68FB6FAF, 0x3E6C53B5, 0x1339B2EB, 0x3B52EC6F,
                0x6DFC511F, 0x9B30952C, 0xCC814544, 0xAF5EBD09,
                0xBEE3D004, 0xDE334AFD, 0x660F2807, 0x192E4BB3,
                0xC0CBA857, 0x45C8740F, 0xD20B5F39, 0xB9D3FBDB,
                0x5579C0BD, 0x1A60320A, 0xD6A100C6, 0x402C7279,
                0x679F25FE, 0xFB1FA3CC, 0x8EA5E9F8, 0xDB3222F8,
                0x3C7516DF, 0xFD616B15, 0x2F501EC8, 0xAD0552AB,
                0x323DB5FA, 0xFD238760, 0x53317B48, 0x3E00DF82,
                0x9E5C57BB, 0xCA6F8CA0, 0x1A87562E, 0xDF1769DB,
                0xD542A8F6, 0x287EFFC3, 0xAC6732C6, 0x8C4F5573,
                0x695B27B0, 0xBBCA58C8, 0xE1FFA35D, 0xB8F011A0,
                0x10FA3D98, 0xFD2183B8, 0x4AFCB56C, 0x2DD1D35B,
                0x9A53E479, 0xB6F84565, 0xD28E49BC, 0x4BFB9790,
                0xE1DDF2DA, 0xA4CB7E33, 0x62FB1341, 0xCEE4C6E8,
                0xEF20CADA, 0x36774C01, 0xD07E9EFE, 0x2BF11FB4,
                0x95DBDA4D, 0xAE909198, 0xEAAD8E71, 0x6B93D5A0,
                0xD08ED1D0, 0xAFC725E0, 0x8E3C5B2F, 0x8E7594B7,
                0x8FF6E2FB, 0xF2122B64, 0x8888B812, 0x900DF01C,
                0x4FAD5EA0, 0x688FC31C, 0xD1CFF191, 0xB3A8C1AD,
                0x2F2F2218, 0xBE0E1777, 0xEA752DFE, 0x8B021FA1,
                0xE5A0CC0F, 0xB56F74E8, 0x18ACF3D6, 0xCE89E299,
                0xB4A84FE0, 0xFD13E0B7, 0x7CC43B81, 0xD2ADA8D9,
                0x165FA266, 0x80957705, 0x93CC7314, 0x211A1477,
                0xE6AD2065, 0x77B5FA86, 0xC75442F5, 0xFB9D35CF,
                0xEBCDAF0C, 0x7B3E89A0, 0xD6411BD3, 0xAE1E7E49,
                0x00250E2D, 0x2071B35E, 0x226800BB, 0x57B8E0AF,
                0x2464369B, 0xF009B91E, 0x5563911D, 0x59DFA6AA,
                0x78C14389, 0xD95A537F, 0x207D5BA2, 0x02E5B9C5,
                0x83260376, 0x6295CFA9, 0x11C81968, 0x4E734A41,
                0xB3472DCA, 0x7B14A94A, 0x1B510052, 0x9A532915,
                0xD60F573F, 0xBC9BC6E4, 0x2B60A476, 0x81E67400,
                0x08BA6FB5, 0x571BE91F, 0xF296EC6B, 0x2A0DD915,
                0xB6636521, 0xE7B9F9B6, 0xFF34052E, 0xC5855664,
                0x53B02D5D, 0xA99F8FA1, 0x08BA4799, 0x6E85076A
            ],
            [
                0x4B7A70E9, 0xB5B32944, 0xDB75092E, 0xC4192623,
                0xAD6EA6B0, 0x49A7DF7D, 0x9CEE60B8, 0x8FEDB266,
                0xECAA8C71, 0x699A17FF, 0x5664526C, 0xC2B19EE1,
                0x193602A5, 0x75094C29, 0xA0591340, 0xE4183A3E,
                0x3F54989A, 0x5B429D65, 0x6B8FE4D6, 0x99F73FD6,
                0xA1D29C07, 0xEFE830F5, 0x4D2D38E6, 0xF0255DC1,
                0x4CDD2086, 0x8470EB26, 0x6382E9C6, 0x021ECC5E,
                0x09686B3F, 0x3EBAEFC9, 0x3C971814, 0x6B6A70A1,
                0x687F3584, 0x52A0E286, 0xB79C5305, 0xAA500737,
                0x3E07841C, 0x7FDEAE5C, 0x8E7D44EC, 0x5716F2B8,
                0xB03ADA37, 0xF0500C0D, 0xF01C1F04, 0x0200B3FF,
                0xAE0CF51A, 0x3CB574B2, 0x25837A58, 0xDC0921BD,
                0xD19113F9, 0x7CA92FF6, 0x94324773, 0x22F54701,
                0x3AE5E581, 0x37C2DADC, 0xC8B57634, 0x9AF3DDA7,
                0xA9446146, 0x0FD0030E, 0xECC8C73E, 0xA4751E41,
                0xE238CD99, 0x3BEA0E2F, 0x3280BBA1, 0x183EB331,
                0x4E548B38, 0x4F6DB908, 0x6F420D03, 0xF60A04BF,
                0x2CB81290, 0x24977C79, 0x5679B072, 0xBCAF89AF,
                0xDE9A771F, 0xD9930810, 0xB38BAE12, 0xDCCF3F2E,
                0x5512721F, 0x2E6B7124, 0x501ADDE6, 0x9F84CD87,
                0x7A584718, 0x7408DA17, 0xBC9F9ABC, 0xE94B7D8C,
                0xEC7AEC3A, 0xDB851DFA, 0x63094366, 0xC464C3D2,
                0xEF1C1847, 0x3215D908, 0xDD433B37, 0x24C2BA16,
                0x12A14D43, 0x2A65C451, 0x50940002, 0x133AE4DD,
                0x71DFF89E, 0x10314E55, 0x81AC77D6, 0x5F11199B,
                0x043556F1, 0xD7A3C76B, 0x3C11183B, 0x5924A509,
                0xF28FE6ED, 0x97F1FBFA, 0x9EBABF2C, 0x1E153C6E,
                0x86E34570, 0xEAE96FB1, 0x860E5E0A, 0x5A3E2AB3,
                0x771FE71C, 0x4E3D06FA, 0x2965DCB9, 0x99E71D0F,
                0x803E89D6, 0x5266C825, 0x2E4CC978, 0x9C10B36A,
                0xC6150EBA, 0x94E2EA78, 0xA5FC3C53, 0x1E0A2DF4,
                0xF2F74EA7, 0x361D2B3D, 0x1939260F, 0x19C27960,
                0x5223A708, 0xF71312B6, 0xEBADFE6E, 0xEAC31F66,
                0xE3BC4595, 0xA67BC883, 0xB17F37D1, 0x018CFF28,
                0xC332DDEF, 0xBE6C5AA5, 0x65582185, 0x68AB9802,
                0xEECEA50F, 0xDB2F953B, 0x2AEF7DAD, 0x5B6E2F84,
                0x1521B628, 0x29076170, 0xECDD4775, 0x619F1510,
                0x13CCA830, 0xEB61BD96, 0x0334FE1E, 0xAA0363CF,
                0xB5735C90, 0x4C70A239, 0xD59E9E0B, 0xCBAADE14,
                0xEECC86BC, 0x60622CA7, 0x9CAB5CAB, 0xB2F3846E,
                0x648B1EAF, 0x19BDF0CA, 0xA02369B9, 0x655ABB50,
                0x40685A32, 0x3C2AB4B3, 0x319EE9D5, 0xC021B8F7,
                0x9B540B19, 0x875FA099, 0x95F7997E, 0x623D7DA8,
                0xF837889A, 0x97E32D77, 0x11ED935F, 0x16681281,
                0x0E358829, 0xC7E61FD6, 0x96DEDFA1, 0x7858BA99,
                0x57F584A5, 0x1B227263, 0x9B83C3FF, 0x1AC24696,
                0xCDB30AEB, 0x532E3054, 0x8FD948E4, 0x6DBC3128,
                0x58EBF2EF, 0x34C6FFEA, 0xFE28ED61, 0xEE7C3C73,
                0x5D4A14D9, 0xE864B7E3, 0x42105D14, 0x203E13E0,
                0x45EEE2B6, 0xA3AAABEA, 0xDB6C4F15, 0xFACB4FD0,
                0xC742F442, 0xEF6ABBB5, 0x654F3B1D, 0x41CD2105,
                0xD81E799E, 0x86854DC7, 0xE44B476A, 0x3D816250,
                0xCF62A1F2, 0x5B8D2646, 0xFC8883A0, 0xC1C7B6A3,
                0x7F1524C3, 0x69CB7492, 0x47848A0B, 0x5692B285,
                0x095BBF00, 0xAD19489D, 0x1462B174, 0x23820E00,
                0x58428D2A, 0x0C55F5EA, 0x1DADF43E, 0x233F7061,
                0x3372F092, 0x8D937E41, 0xD65FECF1, 0x6C223BDB,
                0x7CDE3759, 0xCBEE7460, 0x4085F2A7, 0xCE77326E,
                0xA6078084, 0x19F8509E, 0xE8EFD855, 0x61D99735,
                0xA969A7AA, 0xC50C06C2, 0x5A04ABFC, 0x800BCADC,
                0x9E447A2E, 0xC3453484, 0xFDD56705, 0x0E1E9EC9,
                0xDB73DBD3, 0x105588CD, 0x675FDA79, 0xE3674340,
                0xC5C43465, 0x713E38D8, 0x3D28F89E, 0xF16DFF20,
                0x153E21E7, 0x8FB03D4A, 0xE6E39F2B, 0xDB83ADF7
            ],
            [
                0xE93D5A68, 0x948140F7, 0xF64C261C, 0x94692934,
                0x411520F7, 0x7602D4F7, 0xBCF46B2E, 0xD4A20068,
                0xD4082471, 0x3320F46A, 0x43B7D4B7, 0x500061AF,
                0x1E39F62E, 0x97244546, 0x14214F74, 0xBF8B8840,
                0x4D95FC1D, 0x96B591AF, 0x70F4DDD3, 0x66A02F45,
                0xBFBC09EC, 0x03BD9785, 0x7FAC6DD0, 0x31CB8504,
                0x96EB27B3, 0x55FD3941, 0xDA2547E6, 0xABCA0A9A,
                0x28507825, 0x530429F4, 0x0A2C86DA, 0xE9B66DFB,
                0x68DC1462, 0xD7486900, 0x680EC0A4, 0x27A18DEE,
                0x4F3FFEA2, 0xE887AD8C, 0xB58CE006, 0x7AF4D6B6,
                0xAACE1E7C, 0xD3375FEC, 0xCE78A399, 0x406B2A42,
                0x20FE9E35, 0xD9F385B9, 0xEE39D7AB, 0x3B124E8B,
                0x1DC9FAF7, 0x4B6D1856, 0x26A36631, 0xEAE397B2,
                0x3A6EFA74, 0xDD5B4332, 0x6841E7F7, 0xCA7820FB,
                0xFB0AF54E, 0xD8FEB397, 0x454056AC, 0xBA489527,
                0x55533A3A, 0x20838D87, 0xFE6BA9B7, 0xD096954B,
                0x55A867BC, 0xA1159A58, 0xCCA92963, 0x99E1DB33,
                0xA62A4A56, 0x3F3125F9, 0x5EF47E1C, 0x9029317C,
                0xFDF8E802, 0x04272F70, 0x80BB155C, 0x05282CE3,
                0x95C11548, 0xE4C66D22, 0x48C1133F, 0xC70F86DC,
                0x07F9C9EE, 0x41041F0F, 0x404779A4, 0x5D886E17,
                0x325F51EB, 0xD59BC0D1, 0xF2BCC18F, 0x41113564,
                0x257B7834, 0x602A9C60, 0xDFF8E8A3, 0x1F636C1B,
                0x0E12B4C2, 0x02E1329E, 0xAF664FD1, 0xCAD18115,
                0x6B2395E0, 0x333E92E1, 0x3B240B62, 0xEEBEB922,
                0x85B2A20E, 0xE6BA0D99, 0xDE720C8C, 0x2DA2F728,
                0xD0127845, 0x95B794FD, 0x647D0862, 0xE7CCF5F0,
                0x5449A36F, 0x877D48FA, 0xC39DFD27, 0xF33E8D1E,
                0x0A476341, 0x992EFF74, 0x3A6F6EAB, 0xF4F8FD37,
                0xA812DC60, 0xA1EBDDF8, 0x991BE14C, 0xDB6E6B0D,
                0xC67B5510, 0x6D672C37, 0x2765D43B, 0xDCD0E804,
                0xF1290DC7, 0xCC00FFA3, 0xB5390F92, 0x690FED0B,
                0x667B9FFB, 0xCEDB7D9C, 0xA091CF0B, 0xD9155EA3,
                0xBB132F88, 0x515BAD24, 0x7B9479BF, 0x763BD6EB,
                0x37392EB3, 0xCC115979, 0x8026E297, 0xF42E312D,
                0x6842ADA7, 0xC66A2B3B, 0x12754CCC, 0x782EF11C,
                0x6A124237, 0xB79251E7, 0x06A1BBE6, 0x4BFB6350,
                0x1A6B1018, 0x11CAEDFA, 0x3D25BDD8, 0xE2E1C3C9,
                0x44421659, 0x0A121386, 0xD90CEC6E, 0xD5ABEA2A,
                0x64AF674E, 0xDA86A85F, 0xBEBFE988, 0x64E4C3FE,
                0x9DBC8057, 0xF0F7C086, 0x60787BF8, 0x6003604D,
                0xD1FD8346, 0xF6381FB0, 0x7745AE04, 0xD736FCCC,
                0x83426B33, 0xF01EAB71, 0xB0804187, 0x3C005E5F,
                0x77A057BE, 0xBDE8AE24, 0x55464299, 0xBF582E61,
                0x4E58F48F, 0xF2DDFDA2, 0xF474EF38, 0x8789BDC2,
                0x5366F9C3, 0xC8B38E74, 0xB475F255, 0x46FCD9B9,
                0x7AEB2661, 0x8B1DDF84, 0x846A0E79, 0x915F95E2,
                0x466E598E, 0x20B45770, 0x8CD55591, 0xC902DE4C,
                0xB90BACE1, 0xBB8205D0, 0x11A86248, 0x7574A99E,
                0xB77F19B6, 0xE0A9DC09, 0x662D09A1, 0xC4324633,
                0xE85A1F02, 0x09F0BE8C, 0x4A99A025, 0x1D6EFE10,
                0x1AB93D1D, 0x0BA5A4DF, 0xA186F20F, 0x2868F169,
                0xDCB7DA83, 0x573906FE, 0xA1E2CE9B, 0x4FCD7F52,
                0x50115E01, 0xA70683FA, 0xA002B5C4, 0x0DE6D027,
                0x9AF88C27, 0x773F8641, 0xC3604C06, 0x61A806B5,
                0xF0177A28, 0xC0F586E0, 0x006058AA, 0x30DC7D62,
                0x11E69ED7, 0x2338EA63, 0x53C2DD94, 0xC2C21634,
                0xBBCBEE56, 0x90BCB6DE, 0xEBFC7DA1, 0xCE591D76,
                0x6F05E409, 0x4B7C0188, 0x39720A3D, 0x7C927C24,
                0x86E3725F, 0x724D9DB9, 0x1AC15BB4, 0xD39EB8FC,
                0xED545578, 0x08FCA5B5, 0xD83D7CD3, 0x4DAD0FC4,
                0x1E50EF5E, 0xB161E6F8, 0xA28514D9, 0x6C51133C,
                0x6FD5C7E7, 0x56E14EC4, 0x362ABFCE, 0xDDC6C837,
                0xD79A3234, 0x92638212, 0x670EFA8E, 0x406000E0
            ],
            [
                0x3A39CE37, 0xD3FAF5CF, 0xABC27737, 0x5AC52D1B,
                0x5CB0679E, 0x4FA33742, 0xD3822740, 0x99BC9BBE,
                0xD5118E9D, 0xBF0F7315, 0xD62D1C7E, 0xC700C47B,
                0xB78C1B6B, 0x21A19045, 0xB26EB1BE, 0x6A366EB4,
                0x5748AB2F, 0xBC946E79, 0xC6A376D2, 0x6549C2C8,
                0x530FF8EE, 0x468DDE7D, 0xD5730A1D, 0x4CD04DC6,
                0x2939BBDB, 0xA9BA4650, 0xAC9526E8, 0xBE5EE304,
                0xA1FAD5F0, 0x6A2D519A, 0x63EF8CE2, 0x9A86EE22,
                0xC089C2B8, 0x43242EF6, 0xA51E03AA, 0x9CF2D0A4,
                0x83C061BA, 0x9BE96A4D, 0x8FE51550, 0xBA645BD6,
                0x2826A2F9, 0xA73A3AE1, 0x4BA99586, 0xEF5562E9,
                0xC72FEFD3, 0xF752F7DA, 0x3F046F69, 0x77FA0A59,
                0x80E4A915, 0x87B08601, 0x9B09E6AD, 0x3B3EE593,
                0xE990FD5A, 0x9E34D797, 0x2CF0B7D9, 0x022B8B51,
                0x96D5AC3A, 0x017DA67D, 0xD1CF3ED6, 0x7C7D2D28,
                0x1F9F25CF, 0xADF2B89B, 0x5AD6B472, 0x5A88F54C,
                0xE029AC71, 0xE019A5E6, 0x47B0ACFD, 0xED93FA9B,
                0xE8D3C48D, 0x283B57CC, 0xF8D56629, 0x79132E28,
                0x785F0191, 0xED756055, 0xF7960E44, 0xE3D35E8C,
                0x15056DD4, 0x88F46DBA, 0x03A16125, 0x0564F0BD,
                0xC3EB9E15, 0x3C9057A2, 0x97271AEC, 0xA93A072A,
                0x1B3F6D9B, 0x1E6321F5, 0xF59C66FB, 0x26DCF319,
                0x7533D928, 0xB155FDF5, 0x03563482, 0x8ABA3CBB,
                0x28517711, 0xC20AD9F8, 0xABCC5167, 0xCCAD925F,
                0x4DE81751, 0x3830DC8E, 0x379D5862, 0x9320F991,
                0xEA7A90C2, 0xFB3E7BCE, 0x5121CE64, 0x774FBE32,
                0xA8B6E37E, 0xC3293D46, 0x48DE5369, 0x6413E680,
                0xA2AE0810, 0xDD6DB224, 0x69852DFD, 0x09072166,
                0xB39A460A, 0x6445C0DD, 0x586CDECF, 0x1C20C8AE,
                0x5BBEF7DD, 0x1B588D40, 0xCCD2017F, 0x6BB4E3BB,
                0xDDA26A7E, 0x3A59FF45, 0x3E350A44, 0xBCB4CDD5,
                0x72EACEA8, 0xFA6484BB, 0x8D6612AE, 0xBF3C6F47,
                0xD29BE463, 0x542F5D9E, 0xAEC2771B, 0xF64E6370,
                0x740E0D8D, 0xE75B1357, 0xF8721671, 0xAF537D5D,
                0x4040CB08, 0x4EB4E2CC, 0x34D2466A, 0x0115AF84,
                0xE1B00428, 0x95983A1D, 0x06B89FB4, 0xCE6EA048,
                0x6F3F3B82, 0x3520AB82, 0x011A1D4B, 0x277227F8,
                0x611560B1, 0xE7933FDC, 0xBB3A792B, 0x344525BD,
                0xA08839E1, 0x51CE794B, 0x2F32C9B7, 0xA01FBAC9,
                0xE01CC87E, 0xBCC7D1F6, 0xCF0111C3, 0xA1E8AAC7,
                0x1A908749, 0xD44FBD9A, 0xD0DADECB, 0xD50ADA38,
                0x0339C32A, 0xC6913667, 0x8DF9317C, 0xE0B12B4F,
                0xF79E59B7, 0x43F5BB3A, 0xF2D519FF, 0x27D9459C,
                0xBF97222C, 0x15E6FC2A, 0x0F91FC71, 0x9B941525,
                0xFAE59361, 0xCEB69CEB, 0xC2A86459, 0x12BAA8D1,
                0xB6C1075E, 0xE3056A0C, 0x10D25065, 0xCB03A442,
                0xE0EC6E0E, 0x1698DB3B, 0x4C98A0BE, 0x3278E964,
                0x9F1F9532, 0xE0D392DF, 0xD3A0342B, 0x8971F21E,
                0x1B0A7441, 0x4BA3348C, 0xC5BE7120, 0xC37632D8,
                0xDF359F8D, 0x9B992F2E, 0xE60B6F47, 0x0FE3F11D,
                0xE54CDA54, 0x1EDAD891, 0xCE6279CF, 0xCD3E7E6F,
                0x1618B166, 0xFD2C1D05, 0x848FD2C5, 0xF6FB2299,
                0xF523F357, 0xA6327623, 0x93A83531, 0x56CCCD02,
                0xACF08162, 0x5A75EBB5, 0x6E163697, 0x88D273CC,
                0xDE966292, 0x81B949D0, 0x4C50901B, 0x71C65614,
                0xE6C6C7BD, 0x327A140A, 0x45E1D006, 0xC3F27B9A,
                0xC9AA53FD, 0x62A80F00, 0xBB25BFE2, 0x35BDD2F6,
                0x71126905, 0xB2040222, 0xB6CBCF7C, 0xCD769C2B,
                0x53113EC0, 0x1640E3D3, 0x38ABBD60, 0x2547ADF0,
                0xBA38209C, 0xF746CE76, 0x77AFA1C5, 0x20756060,
                0x85CBFE4E, 0x8AE88DD8, 0x7AAAF9B0, 0x4CF9AA7E,
                0x1948C25C, 0x02FB8A8C, 0x01C36AE4, 0xD6EBE1F9,
                0x90D4F869, 0xA65CDEA0, 0x3F09252D, 0xC208E69F,
                0xB74E6132, 0xCE77E25B, 0x578FDFE3, 0x3AC372E6
            ]
        ]

        # Cycle through the p-boxes and round-robin XOR the
        # key with the p-boxes
        key_len = len (key)
        index = 0
        for i in range (len (self.p_boxes)):
            val = (ord (key[index % key_len]) << 24) + \
                  (ord (key[(index + 1) % key_len]) << 16) + \
                  (ord (key[(index + 2) % key_len]) << 8) + \
                   ord (key[(index + 3) % key_len])
            self.p_boxes[i] = self.p_boxes[i] ^ val
            index = index + 4

        # For the chaining process
        l, r = 0, 0

        # Begin chain replacing the p-boxes
        for i in range (0, len (self.p_boxes), 2):
            l, r = self.cipher (l, r, self.ENCRYPT)
            self.p_boxes[i] = l
            self.p_boxes[i + 1] = r

        # Chain replace the s-boxes
        for i in range (len (self.s_boxes)):
            for j in range (0, len (self.s_boxes[i]), 2):
                l, r = self.cipher (l, r, self.ENCRYPT)
                self.s_boxes[i][j] = l
                self.s_boxes[i][j + 1] = r

        self.initCTR()


    def cipher (self, xl, xr, direction):
        """Encryption primitive"""
        if direction == self.ENCRYPT:
            for i in range (16):
                xl = xl ^ self.p_boxes[i]
                xr = self.__round_func (xl) ^ xr
                xl, xr = xr, xl
            xl, xr = xr, xl
            xr = xr ^ self.p_boxes[16]
            xl = xl ^ self.p_boxes[17]
        else:
            for i in range (17, 1, -1):
                xl = xl ^ self.p_boxes[i]
                xr = self.__round_func (xl) ^ xr
                xl, xr = xr, xl
            xl, xr = xr, xl
            xr = xr ^ self.p_boxes[1]
            xl = xl ^ self.p_boxes[0]
        return xl, xr


    def __round_func (self, xl):
        a = (xl & 0xFF000000) >> 24
        b = (xl & 0x00FF0000) >> 16
        c = (xl & 0x0000FF00) >> 8
        d = xl & 0x000000FF

        # Perform all ops as longs then and out the last 32-bits to
        # obtain the integer
        f = (long (self.s_boxes[0][a]) + long (self.s_boxes[1][b])) % self.modulus
        f = f ^ long (self.s_boxes[2][c])
        f = f + long (self.s_boxes[3][d])
        f = (f % self.modulus) & 0xFFFFFFFF

        return f


    def encrypt (self, data):
        if not len (data) == 8:
            raise RuntimeError, "Attempted to encrypt data of invalid block length: %s" % len(data)

        # Use big endianess since that's what everyone else uses
        xl = ord (data[3]) | (ord (data[2]) << 8) | (ord (data[1]) << 16) | (ord (data[0]) << 24)
        xr = ord (data[7]) | (ord (data[6]) << 8) | (ord (data[5]) << 16) | (ord (data[4]) << 24)

        cl, cr = self.cipher (xl, xr, self.ENCRYPT)
        chars = ''.join ([
            chr ((cl >> 24) & 0xFF), chr ((cl >> 16) & 0xFF), chr ((cl >> 8) & 0xFF), chr (cl & 0xFF),
            chr ((cr >> 24) & 0xFF), chr ((cr >> 16) & 0xFF), chr ((cr >> 8) & 0xFF), chr (cr & 0xFF)
        ])
        return chars


    def decrypt (self, data):
        if not len (data) == 8:
            raise RuntimeError, "Attempted to encrypt data of invalid block length: %s" % len(data)

        # Use big endianess since that's what everyone else uses
        cl = ord (data[3]) | (ord (data[2]) << 8) | (ord (data[1]) << 16) | (ord (data[0]) << 24)
        cr = ord (data[7]) | (ord (data[6]) << 8) | (ord (data[5]) << 16) | (ord (data[4]) << 24)

        xl, xr = self.cipher (cl, cr, self.DECRYPT)
        chars = ''.join ([
            chr ((xl >> 24) & 0xFF), chr ((xl >> 16) & 0xFF), chr ((xl >> 8) & 0xFF), chr (xl & 0xFF),
            chr ((xr >> 24) & 0xFF), chr ((xr >> 16) & 0xFF), chr ((xr >> 8) & 0xFF), chr (xr & 0xFF)
        ])
        return chars


    # ==== CBC Mode ====
    def initCBC(self, iv=0):
        """Initializes CBC mode of the cypher"""
        assert struct.calcsize("Q") == self.block_size()
        self.cbc_iv = struct.pack("Q", iv)


    def encryptCBC(self, data):
        """
        Encrypts a buffer of data using CBC mode. Multiple successive buffers
        (belonging to the same logical stream of buffers) can be encrypted
        with this method one after the other without any intermediate work.
        Each buffer must be a multiple of 8-octets (64-bits) in length.
        """
        if type(data) != types.StringType:
            raise RuntimeError, "Can only work on 8-bit strings"
        if (len(data) % 8) != 0:
            raise RuntimeError, "Can only work with data in 64-bit multiples in CBC mode"

        xor = lambda t: ord(t[0]) ^ ord(t[1])
        result = ''
        block_size = self.block_size()
        for i in range(0, len(data), block_size):
            p_block = data[i:i+block_size]
            pair = zip(p_block, self.cbc_iv)
            j_block = ''.join(map(chr, map(xor, pair)))
            c_block = self.encrypt(j_block)
            result += c_block
            self.cbc_iv = c_block
        return result


    def decryptCBC(self, data):
        if type(data) != types.StringType:
            raise RuntimeError, "Can only work on 8-bit strings"
        if (len(data) % 8) != 0:
            raise RuntimeError, "Can only work with data in 64-bit multiples in CBC mode"

        xor = lambda t: ord(t[0]) ^ ord(t[1])
        result = ''
        block_size = self.block_size()
        for i in range(0, len(data), block_size):
            c_block = data[i:i+block_size]
            j_block = self.decrypt(c_block)
            pair = zip(j_block, self.cbc_iv)
            p_block = ''.join(map(chr, map(xor, pair)))
            result += p_block
            self.cbc_iv = c_block
        return result


    # ==== CTR Mode ====
    def initCTR(self, iv=0):
        """Initializes CTR mode of the cypher"""
        assert struct.calcsize("Q") == self.block_size()
        self.ctr_iv = iv
        self._calcCTRBUF()


    def _calcCTRBUF(self):
        """Calculates one block of CTR keystream"""
        self.ctr_cks = self.encrypt(struct.pack("Q", self.ctr_iv)) # keystream block
        self.ctr_iv += 1
        self.ctr_pos = 0


    def _nextCTRByte(self):
        """Returns one byte of CTR keystream"""
        b = ord(self.ctr_cks[self.ctr_pos])
        self.ctr_pos += 1
        if self.ctr_pos >= len(self.ctr_cks):
            self._calcCTRBUF()
        return b


    def encryptCTR(self, data):
        """
        Encrypts a buffer of data with CTR mode. Multiple successive buffers
        (belonging to the same logical stream of buffers) can be encrypted
        with this method one after the other without any intermediate work.
        """
        if type(data) != types.StringType:
            raise RuntimeException, "Can only work on 8-bit strings"
        result = []
        for ch in data:
            result.append(chr(ord(ch) ^ self._nextCTRByte()))
        return "".join(result)


    def decryptCTR(self, data):
        return self.encryptCTR(data)


    def block_size(self):
        return 8


    def key_length(self):
        return 56


    def key_bits(self):
        return 56 * 8

    @staticmethod
    def testVectors():
        import binascii
        # for more vectors see http://www.schneier.com/code/vectors.txt
        vectors = (
            ('0000000000000000',        '0000000000000000',        '4EF997456198DD78'),
            ('FFFFFFFFFFFFFFFF',        'FFFFFFFFFFFFFFFF',        '51866FD5B85ECB8A'),
            ('3000000000000000',        '1000000000000001',        '7D856F9A613063F2'),
            ('1111111111111111',        '1111111111111111',        '2466DD878B963C9D'),
            ('49E95D6D4CA229BF',        '02FE55778117F12A',        'CF9C5D7A4986ADB5'),
            ('E0FEE0FEF1FEF1FE',        '0123456789ABCDEF',        'C39E072D9FAC631D'),
            ('07A7137045DA2A16',        '3BDD119049372802',        '2EEDDA93FFD39C79'),
        )
        ok = True
        for v in vectors:
            c = Blowfish(binascii.a2b_hex(v[0]))
            e = binascii.b2a_hex(c.encrypt(binascii.a2b_hex(v[1]))).upper()
            if e != v[2]:
                print "VECTOR TEST FAIL: expecting %s, got %s" % (repr(v), e)
                ok = False
        return ok


class oceanbase(object):
    instant_key_list = [
    # mergeserver
    'ms_memory_limit', 'ms_memory_total', 'ms_memory_parser',
    'ms_memory_transformer', 'ms_memory_ps_plan', 'ms_memory_rpc_request',
    'ms_memory_sql_array', 'ms_memory_expression', 'ms_memory_row_store',
    'ms_memory_session', 'ps_count',
    # chunkserver
    'serving_version', 'old_ver_tablets_num', 'old_ver_merged_tablets_num',
    'new_ver_tablets_num', 'new_ver_tablets_num', 'memory_used_default',
    'memory_used_network', 'memory_used_thread_buffer', 'memory_used_tablet',
    'memory_used_bi_cache', 'memory_used_block_cache',
    'memory_used_bi_cache_unserving', 'memory_used_block_cache_unserving',
    'memory_used_join_cache', 'memory_used_sstable_row_cache',
    'memory_used_merge_buffer', 'memory_used_merge_split_buffer',
    # updateserver
    'memory_total', 'memory_limit', 'memtable_total', 'memtable_used',
    'total_rows', 'active_memtable_limit', 'active_memtable_total',
    'active_memtable_used', 'active_total_rows', 'frozen_memtable_limit',
    'frozen_memtable_total', 'frozen_memtable_used', 'frozen_total_rows',
    'low_prio_queued_count', 'normal_prio_queued_count', 'high_prio_queued_count',
    'hotspot_queued_count',
    # machine stat
    'load1', 'load5', 'load15', 'MemTotal', 'MemFree',
    # mock
    'timestamp',
    'tenant','memstore limit','total memstore used','active sessions','cpu usage'
    ]

    delta_key_list = [
    # sql
    'active memstore used',
    'active sessions',
    'block cache hit',
    'block cache miss',
    'block index cache hit',
    'block index cache miss',
    'bloom filter cache hit',
    'bloom filter cache miss'
    'bloom filter filts',
    'bloom filter passes',
    'io read bytes',
    'io read count',
    'io write bytes',
    'io write count',
    'location cache hit',
    'location cache miss',
    'row cache hit',
    'row cache miss',
    'rpc deliver fail',
    'rpc net delay',
    'rpc net frame delay',
    'rpc packet in',
    'sql delete count',
    'sql delete time',
    'sql distributed count',
    'sql insert count',
    'sql insert time',
    'sql local count',
    'sql remote count',
    'sql replace count',
    'sql replace time',
    'sql select count',
    'sql select time',
    'sql update count',
    'sql update time',
    'trans commit count',
    'trans commit time',
    'trans rollback count',
    'time_delta'
    ]

    sum_key_list = [
    'tenant',
    'total memstore used',
    'memstore limit',
    'major freeze trigger',
    'timestamp'
    ]

    app_info = {'username':None, 'password':None}

    def __init__(self, dataid=None):
        self.__q = {}
        self.__stop = True
        self.__machine_stat = {}
        self.__host = Options.host
        self.__port = Options.port
        self.update_dataid(dataid)
        self.__cur_cluster_svrs = None
        self.__cur_tenant_id = 1
        self.tenant = []

    def update_dataid(self, dataid):
        if dataid:
            self.app_info = ObConfigHelper().get_app_info(dataid)

    def dosql(self, sql, host=None, port=None, database=None):
        if host is None:
            host = self.__host
        if port is None:
            port = self.__port
        if host is None:
            host = Options.host
        if port is None:
            port = Options.port
        if database is None:
            database = Options.database
        username = Options.user or self.app_info['username'] or Global.DEFAULT_USER
        password = Options.password or self.app_info['password'] or Global.DEFAULT_PASS

        if password == "":
            mysql = "mysql --connect_timeout=10 -c -s -N -A -h%s -P%d -u%s  %s" % (host, port, username, database)
        else:
            mysql = "mysql --connect_timeout=10 -c -s -N -A -h%s -P%d -u%s -p'%s'  %s" % (host, port, username, password, database)

        cmd = "%s -e \"%s\"" % (mysql, sql)
        DEBUG(self.dosql, "oceanbase.dosql : ", cmd)

        p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        output = p.communicate()[0]
        err = p.wait()
        if err:
            raise Exception('popen Fail', cmd)
        return output

    def show_sql_result(self, sql, host=None, port=None):
        if host is None:
            host = self.__host
        if port is None:
            port = self.__port
        if host is None:
            host = Options.host
        if port is None:
            port = Options.port
        if database is None:
            database = Options.database
        username = Options.user or self.app_info['username'] or Global.DEFAULT_USER
        password = Options.password or self.app_info['password'] or Global.DEFAULT_PASS

        if password == "":
            mysql = "mysql --connect_timeout=10 -c -s -N -A -h%s -P%d -u%s  %s" % (host, port, username, database)
        else:
            mysql = "mysql --connect_timeout=10 -c -s -N -A -h%s -P%d -u%s -p'%s'  %s" % (host, port, username, password, database)

        cmd = "%s -e \"%s\" | less" % (mysql, sql)
        curses.endwin()
        call("clear")

        DEBUG(self.dosql, "oceanbase.show_sql_result : ", cmd)

        os.system(cmd)
        curses.doupdate()

    def mysql(self, host=None, port=None):
        if host is None:
            host = self.__host
        if port is None:
            port = self.__port
        if host is None:
            host = Options.host
        if port is None:
            port = Options.port
        if database is None:
            database = Options.database
        username = Options.user or self.app_info['username'] or Global.DEFAULT_USER
        password = Options.password or self.app_info['password'] or Global.DEFAULT_PASS
        cmd = "mysql --connect_timeout=10 -c -A -h%s -P%d -u%s -p'%s' %s" % (host, port, username, password, database)
        environ['MYSQL_PS1'] = "(\u@\h) [%s]> " % self.app
        call('clear')
        try:
            call(cmd, shell=True)
        except KeyboardInterrupt:
            pass
        call('clear')

    def ssh(self, host):
        cmd = "ssh -o StrictHostKeyChecking=no %s" % host
        call('clear')
        try:
            call(cmd, shell=True)
        except KeyboardInterrupt:
            pass
        call('clear')

    def test_alive(self, fatal=True, do_false=None, do_true=None, host=None, port=None):
        if host is None:
            host = Options.host
        if port is None:
            port = Options.port
        try:
            oceanbase.dosql('select 1', host=host, port=port)
            if do_true is not None:
                do_true("Check oceanbase alive successfully! [%s:%d]" % (host, port))
        except Exception:
            if do_false is not None:
                do_false("Can't connect oceanbase, plz check options!\n"
                         "Options: [IP:%s] [PORT:%d] [USER:%s] [PASS:%s]"
                         % (host, port, Options.user, Options.password))
            if fatal:
                exit(1)
            else:
                return False
        return True

    def __check_schema(self):

        sql = """ SELECT unit.svr_ip, unit.svr_port FROM __all_resource_pool pool JOIN __all_unit unit ON (pool.resource_pool_id = unit.resource_pool_id ) WHERE pool.tenant_id = %s ; """ % (str(self.get_current_tenant()))
        res = self.dosql(sql)

        s_join = ""

        for one in res.split("\n")[:-1]:
            a = one.split("\t")
            if s_join == "":
                s_join = """ %s (svr_ip = '%s' and svr_port = %s ) """ % ( s_join, a[0], a[1])
            else:
                s_join = """ %s or (svr_ip = '%s' and svr_port = %s ) """ % ( s_join, a[0], a[1])

        stat_ids = "stat_id in (10000,10004,10005,10006,30007,30008,30009,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,40011,40012,40013,50000,50001,50002,50003,50004,50005,50006,50007,50008,50009,50010,50011,60000,60002,60003,60005,130000,130001,130002,130004) "

        sql2 = """ select current_time(), stat.con_id, stat.svr_ip, stat.svr_port, stat.name, stat.value from gv\$sysstat stat where stat.class IN (1,4,8,16,32)   and con_id = %s and (%s) """  % (  str(self.get_current_tenant()) , stat_ids)

        DEBUG(self.__check_schema, "oceanbase.check schema sql : ", sql2)

        return sql2

    def __get_cpu_usage(self):
    	#sql = """SELECT t1.tenant_id,t2.zone, t1.svr_ip,t1.svr_port , round(cpu_quota_used/t4.max_cpu, 3) cpu_usage FROM __all_tenant_resource_usage t1 JOIN  __all_server t2 ON (t1.svr_ip=t2.svr_ip and t1.svr_port=t2.svr_port) JOIN __all_resource_pool t3 ON (t1.tenant_id=t3.tenant_id) JOIN __all_unit_config t4 ON (t3.unit_config_id=t4.unit_config_id) WHERE report_time > date_sub(now(), INTERVAL 30 SECOND) AND t1.tenant_id IN (%s)  ORDER BY t1.tenant_id, t2.zone, t1.svr_ip, t1.svr_port; """    % (str(self.get_current_tenant()))
        sql = """SELECT t1.con_id tenant_id,t2.zone, t1.svr_ip,t1.svr_port , round(t1.value/(100*t4.max_cpu), 3) cpu_usage FROM gv\$sysstat t1 JOIN  __all_server t2 ON (t1.svr_ip=t2.svr_ip and t1.svr_port=t2.svr_port) JOIN __all_resource_pool t3 ON (t1.con_id=t3.tenant_id) JOIN __all_unit_config t4 ON (t3.unit_config_id=t4.unit_config_id) WHERE  t1.con_id IN (%s) and t1.stat_id=140006  ORDER BY t1.con_id, t2.zone, t1.svr_ip, t1.svr_port; """    % (str(self.get_current_tenant()))
    	res = self.dosql(sql)

        DEBUG(self.__get_cpu_usage, "oceanbase.get cpu usage res : ", res)
    	r = dict()

    	for one in res.split("\n")[:-1]:
            a = one.split("\t")
            tnt = a[0]
            #ip = a[2]
            #port = a[3]
            ip = '%s:%s' % (a[2],a[3])
            name = "cpu usage"
            value = float(a[4])

            if tnt not in r:
                r[tnt] = dict()
            if ip not in r[tnt]:
                r[tnt][ip] = dict()
            r[tnt][ip][name] = value

        DEBUG(self.__get_cpu_usage, "oceanbase.get cpu usage r: ", r)

        return r;


    def __get_all_stat(self):
        sql = self.__check_schema()
        res = self.dosql(sql)
        r = self.__get_cpu_usage()

        time = str(datetime.now())
        now = datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')
        if not self.__using_server_time():
            time = str(datetime.now())
        for one in res.split("\n")[:-1]:
            a = one.split("\t")
            if self.__using_server_time():
                time = a[0]
            now = datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')
            tnt = a[1]
            #ip = a[2]
            #port = a[3]
            ip = '%s:%s' % (a[2],a[3])
            name = a[4]
            value = int(a[5])

            if tnt not in r:
                r[tnt] = dict()
            if ip not in r[tnt]:
                r[tnt][ip] = dict()
            r[tnt][ip][name] = value

        r['time_delta'] = timedelta()
        r['timestamp'] = now
        r['tenant'] = int(a[1])

        DEBUG(self.__get_all_stat, "oceanbase.__get_all_stat returned: ", r)
        DEBUG(self.__get_all_stat, "oceanbase.__get_all_stat finished with r['tenant']=%d" % r['tenant'], "")

        return r

    def __get_stat(self, stat):
        r = ''
        for k in stat.keys():

            if type(stat[k]) == dict:
                r = r + self.__get_stat(stat[k]) + ","
            else:
                r = r + "'" +  k + "':" + str(stat[k]) + ","

        return r.rstrip(',')

    def __print_stat(self, stat):
        now = datetime.now()

        s = now.strftime("%Y%m%d%H%M%S")

        if f_data_log != '':
            s = s + ',' + self.__get_stat(stat)
            f = open(f_data_log, "a")
            f.write(s+"\n")
            f.close()


    def sub_stat(self, now, prev):

        r = self.__sub_stat(now, prev)
        return r

    def __sub_stat(self, now, prev):
        r = {}
        for k in now.keys():

            if k in self.instant_key_list:
                r[k] = now[k]
            elif k == 'time_delta':
                r[k] = now['timestamp'] - prev['timestamp']
            else:
                if type(now[k]) == dict:
                    r.update({k: self.__sub_stat(now[k], prev[k])})
                elif type(now[k]) == int:
                    r[k] = now[k] - prev[k]
                elif type(now[k]) == float:
                	r[k] = round(now[k] - prev[k],2)
                else:
                    DEBUG(self.__sub_stat, "unknown type of now['%s'] : " % k, type(now[k]))
                    pass

        self.__print_stat(r)

        return r

    def __init_stat(self, stat):
        r = stat.copy()

        for k in r.keys():
            if k == 'tenant':
                r[k] = stat[k]
            elif k == 'time_delta':
                r[k] = timedelta()
            else:
                if type(r[k]) == dict:
                    r[k] = self.__init_stat(r[k])
                elif type(r[k]) == type(datetime.min):
                    r[k] = datetime.strptime('2016-11-11 00:00:00', "%Y-%m-%d %H:%M:%S")
                else:
                    if k in ( 'tenant'):
                        stat_sum[k] = 1
                    elif type(r[k]) == float:
                        r[k] = 0.0
                    else:
                        r[k] = 0

        return r;

    def __op_stat(self, stat_sum, stat_new, op_type):
        r = stat_sum.copy()

        for k in stat_sum.keys():
            #if k == 'time_delta':
                #DEBUG(self.__op_stat, "oceanbase.__op_stat 'time_delta' : ",  stat_sum[k])

            if type(stat_sum[k]) == dict:
                r[k] = self.__op_stat(stat_sum[k], stat_new[k], op_type)
            elif k == 'timestamp':
                r[k] = datetime.strptime('2016-11-11 00:00:00', "%Y-%m-%d %H:%M:%S")
            elif k in self.delta_key_list:
                if op_type == 'add':
                    r[k] = stat_sum[k] + stat_new[k]
                elif op_type == 'sub':
                    r[k] = stat_sum[k] - stat_new[k]
            else:
                r[k] = stat_new[k]

            #if k == 'time_delta':
                #DEBUG(self.__op_stat, "oceanbase.__op_stat 'time_delta' : ",  stat_sum[k])


        return r;



    def __update_oceanbase_stat_runner(self):
        DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner : ",  "")

        q = self.__q
        prev = self.__get_all_stat()


        while not self.__stop:


            sleep(Options.interval)
            if self.__stop:
                break
            try:
                cur = self.__get_all_stat()
            except Exception:
                continue
            try:
                stat_new = self.__sub_stat(cur, prev)
                DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner stat_new :", stat_new)


                if len(q) == 0:
                    stat_sum = self.__init_stat(prev)
                    stat_sum['timestamp'] = stat_new['timestamp']
                    #stat_avg = stat_sum.copy()

                    #q.appendleft(stat_avg)
                    q.appendleft(stat_sum)


                stat_sum = q.popleft()
                #q.popleft()

                DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner stat_sum :", stat_sum)


                q.appendleft(stat_new)

                stat_sum = self.__op_stat(stat_sum, stat_new, 'add')

                DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner stat_sum :", stat_sum)

                #DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner stat_avg :", stat_avg)


                #stat_avg = self.__avg_stat(stat_sum, len(q) )

                #DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner stat_avg :", stat_avg)

                if (len(q) > Global.MAX_LINES):
                    r = q.pop()
                    stat_sum = self.__op_stat(stat_sum, r, 'sub')
                    #stat_avg = self.__avg_stat(stat_sum, len(q))

                #q.appendleft(stat_avg)
                q.appendleft(stat_sum)


            except Exception, e:
                DEBUG(self.__update_oceanbase_stat_runner, "oceanbase.__update_oceanbase_stat_runner tenant changed from %d to %d , exception" % (prev['tenant'], cur['tenant']), e)

                q.clear()
                prev = cur
                pass

            prev = cur


    def __do_ssh(self, host, cmd):
        BUFFER_SIZE = 1 << 16
        ssh = Popen(['ssh', '-o', 'PreferredAuthentications=publickey', '-o', 'StrictHostKeyChecking=no', host, cmd],
                    stdout=PIPE, stderr=PIPE, preexec_fn=setsid, shell=False)
        output = ssh.communicate()[0]
        err = ssh.wait()
        if err:
            raise Exception('popen Fail', "%s: %s" % (host, cmd))
        return output

    def __get_machine_stat(self, ip):
        cmd = ("cat <(date +%s%N) <(cat /proc/stat | head -1 | cut -d' ' -f 3-9) "
               "/proc/loadavg  <(cat /proc/meminfo) <(echo END_MEM) "
               "<(cat /proc/net/dev | sed -n 3~1p) <(echo END_NET) "
               "<(cat /proc/diskstats | grep '  8  ') <(echo END_DISK)")
        try:
            output = self.__do_ssh(ip, cmd)
        except Exception:
            return {}

        lines = output.split('\n')

        res = {}
        res['time_delta'] = float(lines[0]) / 1000   # ms
        lines = lines[1:]

        cpuinfo = lines[0]
        columns = cpuinfo.split(" ")
        res['user'] = int(columns[0])
        res['nice'] = int(columns[1])
        res['sys'] = int(columns[2])
        res['idle'] = int(columns[3])
        res['iowait'] = int(columns[4])
        res['irq'] = int(columns[5])
        res['softirq'] = int(columns[6])
        res['total'] = 0
        for c in columns:
            res['total'] += int(c)
        lines = lines[1:]

        loadavg = lines[0]
        columns = loadavg.split(" ")
        res['load1'] = columns[0]
        res['load5'] = columns[1]
        res['load15'] = columns[2]
        lines = lines[1:]

        # mem
        for line in lines:
            if 'END_MEM' == line:
                break
            kv = line.split()
            k = kv[0][:-1]
            v = kv[1]
            res[k] = int(v) * 1024
        idx = lines.index('END_MEM')
        lines = lines[idx + 1:]

        # net
        res['net'] = {}
        for line in lines:
            if 'END_NET' == line:
                break
            colon = line.find(':')
            assert colon > 0, line
            name = line[:colon].strip()
            res['net'][name] = {}
            fields = line[colon+1:].strip().split()
            res['net'][name]['bytes_recv'] = int(fields[0])
            res['net'][name]['packets_recv'] = int(fields[1])
            res['net'][name]['errin'] = int(fields[2])
            res['net'][name]['dropin'] = int(fields[3])
            res['net'][name]['bytes_sent'] = int(fields[8])
            res['net'][name]['packets_sent'] = int(fields[9])
            res['net'][name]['errout'] = int(fields[10])
            res['net'][name]['dropout'] = int(fields[11])
        idx = lines.index('END_NET')
        lines = lines[idx + 1:]

        res['disk'] = {}
        SECTOR_SIZE = 512
        for line in lines:
            # http://www.mjmwired.net/kernel/Documentation/iostats.txt
            if 'END_DISK' == line:
                break
            _, _, name, reads, _, rbytes, rtime, writes, _, wbytes, wtime = line.split()[:11]
            res['disk'][name] = {}
            res['disk'][name]['rbytes'] = int(rbytes) * SECTOR_SIZE
            res['disk'][name]['wbytes'] = int(wbytes) * SECTOR_SIZE
            res['disk'][name]['reads'] = int(reads)
            res['disk'][name]['writes'] = int(writes)
            res['disk'][name]['rtime'] = int(rtime)
            res['disk'][name]['wtime'] = int(wtime)
        return res
        idx = lines.index('END_DISK')
        lines = lines[idx + 1:]

    def __update_server_stat_runner(self):
        prev = {}
        for ip in self.ip_list:
            prev[ip] = self.__get_machine_stat(ip)
        while not self.__stop:
            sleep(Options.machine_interval)
            shuffle(self.ip_list)
            for ip in self.ip_list:
                if self.__stop:
                    break
                cur = self.__get_machine_stat(ip)
                try:
                    result = self.__sub_stat(cur, prev[ip])
                    self.__machine_stat.update({ip: result})
                except KeyError:
                    pass
                prev[ip] = cur

    def __update_version(self):
        self.version = "Unknown"
        res = self.dosql("show variables like 'version_comment'")
        for one in res.split("\n")[:-1]:
            a = one.split("\t")
            self.version = a[1]
        self.app = oceanbase.dosql("select value from __all_sys_config_stat"
                                     " where name='appname' limit 1;")[:-1]

    def __using_server_time(self):
        return False

    def start(self):
        self.__stop = False
        self.__q = deque([])
        self.__th = Thread(target=self.__update_oceanbase_stat_runner, args=())
        self.__th.daemon = True
        self.__th.start()
        if Options.env == 'online':
            self.__update_ip_list()
            self.__machine_stat = {}
            self.__svr_th = Thread(target=self.__update_server_stat_runner, args=())
            self.__svr_th.daemon = True
            self.__svr_th.start()

    def dump_queue(self):
        print self.__q

    def now(self):
        return self.__q[1]

    def stat_count(self):
        return len(self.__q)


    def latest(self, num=1):
        return list(self.__q)[0:num]

    def machine_stat(self):
        return self.__machine_stat

    def update_tenant_info(self):
        DEBUG(self.update_tenant_info, "oceanbase.update_tenant_info" , "")
        class Tenant:
            def __init__(self, tid, name, zone_list, selected):

                self.tenant_id = tid
                self.tenant_name = name
                self.zone_list = zone_list
                self.selected = selected
                self.svr_list = {"observer":[]}
        res = self.dosql("select tenant_id, tenant_name, zone_list from __all_tenant")
        self.tenant = []
        flg = True
        for line in res.split("\n")[0:-1]:
            tnt = line.split("\t")
            self.tenant.append(Tenant(tnt[0], tnt[1], tnt[2], flg))
            flg = False


        svrs = self.dosql("select server.svr_ip, server.svr_port, server.id, server.zone, server.inner_port, server.with_rootserver, server.status, pool.tenant_id, unit.migrate_from_svr_ip, unit.migrate_from_svr_port, server.zone from __all_server server join __all_unit unit on (server.svr_ip=unit.svr_ip and server.svr_port=unit.svr_port) join __all_resource_pool pool on (unit.resource_pool_id=pool.resource_pool_id) ")
        for line in svrs.rstrip("\n").split("\n"):
            svr = line.split("\t")
            for tnt in self.tenant:
                if tnt.tenant_id == svr[7]:
                    tnt.svr_list["observer"].append({"zone":svr[10], "ip":svr[0], "port":svr[1], "role":svr[5]})
                    if svr[8] != '':
                    	tnt.svr_list["observer"].append({"zone":svr[10], "ip":svr[8], "port":svr[9], "role":svr[5]})

        #self.__update_version()
        #self.__update_ip_list()
        self.__update_cur_cluster_info()
        self.__update_sample()

#   def update_cluster_info(self):
#       class Cluster:
#           def __init__(self, id, vip, port, role):
#               self.id = id
#               self.vip = vip
#               try:
#                   self.port = int(port)
#               except ValueError:
#                   self.port = 0
#               self.role = role
#               self.selected = False
#               self.svr_list = {'chunkserver':[], 'mergeserver':[], 'rootserver':[], 'updateserver':[]}

#       res = self.dosql('SELECT cluster_id,cluster_vip,cluster_port,cluster_role FROM __all_cluster')
#       self.cluster = []
#       for line in res.split("\n")[:-1]:
#           clu = line.split("\t")
#           self.cluster.append(Cluster(int(clu[0]), clu[1], clu[2], int(clu[3])))

#       svrs = oceanbase.dosql("select svr_type,cluster_id,svr_ip,svr_port,svr_role from __all_server")
#       for line in svrs.rstrip("\n").split("\n"):
#           svr = line.split("\t")
#           for clu in self.cluster:
#               if clu.id == int(svr[1]):
#                   clu.svr_list[svr[0]].append({'ip': svr[2], 'port': int(svr[3]), 'role': int(svr[4])})

#       if Options.dataid:
#           self.update_lms()

#       self.__update_version()
#       self.__update_ip_list()
#       self.__update_cur_cluster_info()
#       self.__update_sample()

    def check_lms(self, say):
        if Options.dataid is not None:
            lms_list = self.app_info['lms_list']
            if not lms_list or len(lms_list) <= 0:
                say('Get lms list fail, plz check'
                    + ' [ dataid = %s, lms_list = %s ]' % (Options.dataid, lms_list))
            else:
                for lms in lms_list:
                    say('checking lms [%s:%s]' % lms)
                    if oceanbase.test_alive(host=lms[0], port=lms[1], fatal=False,
                                            do_false=say, do_true=say):
                        self.__host = lms[0]
                        self.__port = lms[1]
                        #oceanbase.update_cluster_info()
                        oceanbase.update_tenant_info()
                        return True
        return False

    def __update_sample(self):
        self.sample = self.__get_all_stat()


        DEBUG(self.__update_sample, "stat:" , self.sample)

    def __update_ip_list(self):
        ip_list = []
        ip_map = {}
        for clu in self.tenant:
            svr_list = clu.svr_list
            for name in ("observer"):
                ip_list += [svr["ip"] for svr in svr_list[name]]
                for ip in [svr["ip"] for svr in svr_list[name]]:
                    if ip in ip_map:
                        ip_map[ip].append(name)
                    else:
                        ip_map[ip] = [name]
        self.ip_list = list(set(ip_list))
        self.ip_map = ip_map

#   def __update_ip_list(self):
#       ip_list = []
#       ip_map = {}
#       for clu in self.cluster:
#           svr_list = clu.svr_list
#           for name in ('chunkserver', 'mergeserver', 'updateserver', 'rootserver'):
#               ip_list += [svr['ip'] for svr in svr_list[name]]
#               for ip in [svr['ip'] for svr in svr_list[name]]:
#                   if ip in ip_map:
#                       ip_map[ip].append(name)
#                   else:
#                       ip_map[ip] = [name]
#       self.ip_list = list(set(ip_list))
#       self.ip_map = ip_map

    def __update_cur_cluster_info(self):
        for tnt in self.tenant:

            if int(self.__cur_tenant_id)== int(tnt.tenant_id) :
                self.__cur_cluster_svrs = tnt.svr_list

                DEBUG(self.__update_cur_cluster_info, "oceanbase.__update_cur_cluster_info return tenant_id : %s, cluster_svrs : " % tnt.tenant_id , self.__cur_cluster_svrs)
                return self.__cur_cluster_svrs


    def update_lms(self):
        svrs = oceanbase.dosql("select svr_ip, svr_port from __all_server where with_rootserver = 1 limit 1")
        for line in svrs.rstrip("\n").split("\n"):
            svr = line.split("\t")
            ip = svr[0]
            try:
                port = int(svr[1])
                self.__host = svr[0]
                self.__port = svr[1]
            except ValueError:
                return


    def set_current_tenant(self, tid):
        DEBUG(self.set_current_tenant, "oceanbase.set_current_tenant(tid='%s')" % (tid), "")
        self.__cur_tenant_id = tid

    def get_current_tenant(self):
        return self.__cur_tenant_id

    def get_tenant_svr(self, tid=None):
        for tnt in self.tenant:
            if True == tnt.selected:
                return tnt.svr_list

    def find_svr_list(self):
        if not self.__cur_cluster_svrs:
            self.__update_cur_cluster_info()
        return self.__cur_cluster_svrs

    def stop(self):
        self.__stop = True

    def switch_tenant(self, tid):
        for tnt in self.tenant:
            if tid == tnt.tenant_id:
                tnt.selected = True
                self.__q.clear()
            else:
                tnt.selected = False
                DEBUG(self.switch_tenant, "selected false!", tnt)

        DEBUG(self.switch_tenant, "oceanbase.switch_tenant : ", [" ".join([tnt.tenant_id, str(tnt.selected)]) for tnt in self.tenant])
        self.__update_cur_cluster_info()
        self.__update_sample()

class Page(object):
    def __init__(self, parent, layout, y, x, height, width):

        self.__parent = parent
        self.__layout = layout
        self.__widgets = []
        self.__win = parent.derwin(height, width, y, x)
        self.__y = y
        self.__x = x
        self.__height = height
        self.__width = width
        self.border()
        self.__win.nodelay(1)
        self.__win.timeout(0)
        self.__win.keypad(1)
        self.move(y, x)
        self.resize(height, width)
        self.__cur_select = 0
        self.__shown_widget_num = 0
        self.cur_tenant_id = 1
        self.__cur_tenant_id = 1

    def get_current_tenant(self):
        return self.__cur_tenant_id

    def set_current_tenant(self, tid):
        DEBUG(self.set_current_tenant, "Page.set_current_tenant(tid='%s' )" % tid, "")
        self.__cur_tenant_id = tid

    def add_widget(self, widget):
        DEBUG(self.add_widget, "Page.add_widget(widget='%s') " % widget, "")
        if 0 == len(self.__widgets):
            widget.select(True)
        self.__widgets.append(widget)
        self.__rearrange()

    def update_widgets(self):
        pass

    def __rearrange(self):
        undeleted_widgets = filter(lambda w: False == w.delete(), self.__widgets)
        self.__layout.rearrange(0, 0, self.__height, self.__width, undeleted_widgets)

    def rearrange(self):
        self.__rearrange()

    def update(self):
        self.update_widgets()

    def clear_widgets(self):
        self.__widgets = []

    def resize(self, height, width):
        self.__height = height
        self.__width = width
        self.__win.resize(height, width)
        self.__rearrange()

    def move(self, y, x):
        self.__x = x
        self.__y = y
        self.__win.mvderwin(y, x)
        self.__rearrange()

    def __reset_widgets(self):
        if len(self.__widgets) <= 0:
            return
        shown_widgets = self.shown_widgets()
        map(lambda w: w.select(False), self.__widgets)
        map(lambda w: w.delete(False), self.__widgets)
        self.__cur_select = 0
        self.__widgets[self.__cur_select].select(True)
        self.__rearrange()

    def __delete_current_widget(self):
        shown_widgets = self.shown_widgets()
        if len(shown_widgets) <= 0:
            return
        shown_widgets[self.__cur_select].select(False)
        shown_widgets[self.__cur_select].delete(True)
        self.__cur_select -= 1
        self.select_next()
        self.__rearrange()

    def border(self):
        pass

    def process_key(self, ch):

        if ch == ord('d'):
            self.__delete_current_widget()
        elif ch == ord('R'):
            self.__reset_widgets()
        elif ch == ord('m'):
            curses.endwin()
            oceanbase.mysql()
            curses.doupdate()
        elif ch == ord('j'):
            curses.endwin()
            w = self.selected_widget()
            oceanbase.ssh(w.host())
            curses.doupdate()

    def redraw(self):
        self.erase()
        self.__layout.redraw(self.shown_widgets())

    def getch(self):
        return self.__win.getch()

    def erase(self):
        self.__win.erase()

    def win(self):
        return self.__win

    def title(self):
        return 'Untitled'

    def select_next(self):
        shown_widgets = self.shown_widgets()
        if len(shown_widgets) <= 0:
            return
        shown_widgets[self.__cur_select].select(False)
        self.__cur_select = (self.__cur_select + 1) % len(shown_widgets)
        shown_widgets[self.__cur_select].select(True)

    def parent(self):
        return self.__parent

    def shown_widgets(self):
        '''Actually shown widgets that is all widgets except for
        1. deleted widgets and,
        2. couldn\'t display widgets as no space for them.
        '''
        return filter(lambda w: w.show() and False == w.delete(), self.__widgets)

    def valid_widgets(self):
        '''All widgets excpet for the deleted widgets.'''
        return filter(lambda w: False == w.delete(), self.__widgets)

    def all_widgets(self):
        return self.__widgets

    def selected_widget(self):
        return self.shown_widgets()[self.__cur_select]

    def select_columns(self):
        if len(self.all_widgets()) > 0:
            all_widgets = [hc_widget.column_widget() for hc_widget in self.all_widgets()]
            columns = all_widgets[0].valid_columns()
            columns_ret = ColumnCheckBox('Select Columns', columns, self.__parent).run()
            for widget in all_widgets:
                for idx in range(0, len(columns)):
                    widget.valid_columns()[idx].enable(columns[idx].enable(enable=True))
                widget.update()
            [hc_widget.resize() for hc_widget in self.all_widgets()]
            self.rearrange()


class Layout(object):
    def __init__(self):
        pass

    def redraw(self, widgets):
        for widget in widgets:
            try:
                if widget.show():
                    widget.redraw()
            except Exception:
                pass

    def __calc_widget_height(self, height, width, widgets):
        max_min_height = max([ widget.min_height() for widget in widgets ])
        for widget in widgets:
            widget.min_height(max_min_height)

        wwidths = [ widget.min_width() for widget in widgets ]
        cur_width = 0
        nline = 1
        for wwidth in wwidths:
            if cur_width + wwidth <= width:
                cur_width += wwidth
            else:
                cur_width = wwidth
                nline += 1
        widget_height = height / nline
        return max(widget_height, max_min_height)

    def rearrange(self, y, x, height, width, widgets):
        if height <= 0 or width <= 0 or len(widgets) <= 0:
            return 0
        widget_height = self.__calc_widget_height(height, width, widgets)

        for widget in widgets:
            widget.show(False)
        cur_y = 0
        cur_x = 0
        for index,widget in enumerate(widgets):
            if cur_x + widget.min_width() > width:
                cur_y += widget_height
                cur_x = 0
            try:
                widget.move(0, 0)
                widget.resize(widget_height, widget.min_width())
                widget.move(cur_y + y, cur_x + x)
                widget.show(True)
            except curses.error:
                return index
            cur_x += widget.min_width()
        return len(widgets)


class Widget(object):
    def __init__(self, min_height, min_width, parent, use_win=False):
        DEBUG(self.__init__, "Widget.__init__(%d, %d, %s)" % (min_height, min_width, parent), "")
        if use_win:
            self.__win = parent
        else:
            self.__win = parent.derwin(min_height, min_width, 0, 0)
        self.__min_height = min_height
        self.__min_width = min_width
        self.__height, self.__width = self.__win.getmaxyx()
        self.__y, self.__x = self.__win.getmaxyx()
        self.__select = False
        self.__show = False
        self.__deleted = False

    def resize(self, height, width):
        self.__height = height
        self.__width = width
        self.__win.resize(height, width)

    def move(self, y, x):
        self.__win.mvderwin(y, x)
        self.__y = y
        self.__x = x

    def mvwin(self, y, x):
        self.__win.mvwin(y, x)
        self.__y = y
        self.__x = x

    def min_height(self, height=None):
        if height:
            self.__min_height = height
        return self.__min_height

    def min_width(self, width=None):
        if width:
            self.__min_width = width
        return self.__min_width

    def geometry(self):
        return self.__y, self.__x, self.__height, self.__width

    def height(self):
        return self.__height

    def width(self):
        return self.__width

    def redraw(self):
        pass

    def refresh(self):
        self.__win.refresh()

    def erase(self):
        self.__win.erase()

    def win(self):
        return self.__win

    def select(self, select = None):
        if select is not None:
            self.__select = select
        return self.__select

    def win(self):
        return self.__win

    def show(self, show=None):
        if show is not None:
            self.__show = show
        return self.__show

    def update(self):
        pass

    def delete(self, delete=None):
        if delete is not None:
            self.__deleted = delete
        return self.__deleted


class Column(object):
    def __init__(self, name, filter, width, duration=False, enable=True, sname=None):
        DEBUG(self.__init__, "Column.__init__(name='%s', filter='%s', width=%d) " % (name, filter, width), "")
        self.__name = name
        self.__filter = filter
        self.__width = width
        self.__duration = duration
        self.__enable = enable
        self.__sname = sname or name
        self.__valid = True


    def __str__(self):
        return self.name() + " (" + self.sname() + ")"

    def __repr__(self):
        return self.name()

    def __eq__(self, obj):
        return isinstance(obj, Column) and self.name() == obj.name()

    def name(self):
        return self.__name

    def sname(self):
        return self.__sname

    def header(self):
        return self.__sname.center(self.__width).upper()

    def value(self, stat):
        def seconds(td):
            return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / (10**6);

        d = self.__filter(stat)


        DEBUG(self.__init__, "Column.__init__ , %s : " % self.__name , d )

        if type(d) == type(0.0):
            div = (seconds(stat['time_delta']) or 1.0) if self.__duration else 1.0

            DEBUG(self.__init__, "Column.__init__ , div : " , div )

            v = d / div
            if (v > 100):
                v = int(v)
                return mem_str(v).center(self.__width)[:self.__width]
            elif self.__sname == "PCT.":
                return percent_str(v).center(self.__width)[:self.__width]
            return ("%.2f" % v).center(self.__width)[:self.__width]
        elif type(d) == type(0):
            div = (seconds(stat['time_delta']) or 1.0) if self.__duration else 1.0

            DEBUG(self.__init__, "Column.__init__ , div : " , div )

            v = int(d / div)
            if (self.__sname == 'ni' or self.__sname == 'no'):
                return mem_str(v).center(self.__width)[:self.__width]
            return mem_str(v).center(self.__width)[:self.__width]
        elif d == "00:00:00":
            v = str(stat['time_delta'])

            return v[0:v.find('.')].center(self.__width)[:self.__width]
        elif type(d) == type(''):
            return d.center(self.__width)[:self.__width]

    def enable(self, enable=None):
        if enable is not None:
            self.__enable = enable
        return self.__enable

    def valid(self, valid=None):
        if valid is not None:
            self.__valid = valid
        return self.__valid

class MachineStatWidget(Widget):
    def __init__(self, name, parent, border=True):
        self.__name = name
        self.__border = border
        width = 48
        Widget.__init__(self, 25, width, parent)

    def host(self):
        return self.__name.split(':')[0]

    def type_str(self):
        return self.__name.split(':')[1]

    def redraw(self):
        if self.__border:
            if self.select():
                self.win().attron(curses.color_pair(1) | curses.A_BOLD)
                self.win().box()
                self.win().attroff(curses.color_pair(1) | curses.A_BOLD)
            else:
                self.win().box()
            self.win().addstr(0, 2, " " + self.__name + " ", curses.color_pair(3))

        try:
            stat = oceanbase.machine_stat()[self.host()]
        except KeyError:
            return

        if self.__border:
            # cpu info
            self.win().addstr(1, 2, "%-7s%4s%%" % ("CPU", "100"), curses.color_pair(7) | curses.A_BOLD)
            # self.win().attron(curses.color_pair(8))
            self.win().addstr(1, 17, "%-7s%4.1f%%" % ("nice:", float(stat['nice']) / stat['total'] * 100))
            self.win().addstr(2, 2, "%-7s%4.1f%%" % ("user:", float(stat['user']) / stat['total'] * 100))
            self.win().addstr(2, 17, "%-7s%4.1f%%" % ("iowait:", float(stat['iowait']) / stat['total'] * 100))
            self.win().addstr(3, 2, "%-7s%4.1f%%" % ("sys:", float(stat['sys']) / stat['total'] * 100))
            self.win().addstr(3, 17, "%-7s%4.1f%%" % ("irq:", float(stat['irq']) / stat['total'] * 100))
            self.win().addstr(4, 2, "%-7s%4.1f%%" % ("idle:", float(stat['idle']) / stat['total'] * 100))
            self.win().addstr(4, 17, "%-7s%4.1f%%" % ("sirq:", float(stat['softirq']) / stat['total'] * 100))
            # self.win().attroff(curses.color_pair(8))

            # load
            self.win().addstr(1, 32, "%-7s%6s" % ("Load", "x-core"), curses.color_pair(7) | curses.A_BOLD)
            # self.win().attron(curses.color_pair(8))
            self.win().addstr(2, 32, "%-7s%6s" % ("1 min:", stat['load1']))
            self.win().addstr(3, 32, "%-7s%6s" % ("5 min:", stat['load5']))
            self.win().addstr(4, 32, "%-7s%6s" % ("15 min:", stat['load15']))
            # self.win().attroff(curses.color_pair(8))

            # mem info
            self.win().addstr(6, 2, "%-6s%7s" % ("Mem", mem_str(stat['MemTotal'])), curses.color_pair(7) | curses.A_BOLD)
            self.win().addstr(7, 2, "%-6s%7s" % ("used:", mem_str(stat['MemTotal'] - stat['MemFree'])))
            self.win().addstr(8, 2, "%-6s%7s" % ("free:", mem_str(stat['MemFree'])))

            # net info
            self.win().addstr(6, 17, "%-9s%9s%9s" % ("Network", 'Rx/s', 'Tx/s'), curses.color_pair(7) | curses.A_BOLD)
            for idx,item in enumerate(stat['net'].items()):
                if idx > 5:
                    break
                self.win().addstr(7 + idx, 17, "%-9s%9s%9s" %
                                  (item[0],
                                   mem_str(item[1]['bytes_recv'] * pow(10, 6) / float(stat['time_delta']), bit=True),
                                   mem_str(item[1]['bytes_sent'] * pow(10, 6) / float(stat['time_delta']), bit=True)))

            # disk io info
            self.win().addstr(14, 2, "%-5s%6s%9s" % ("Disk I/O", 'In/s', 'Out/s'), curses.color_pair(7) | curses.A_BOLD)
            idx = 15
            for item in stat['disk'].items():
                if idx >= self.height() - 1:
                    break
                if item[1]['rbytes'] <= 0 and item[1]['wbytes'] <= 0:
                    continue
                self.win().addstr(idx, 2, "%-5s%9s%9s" %
                                  (item[0],
                                   mem_str(item[1]['rbytes'] * pow(10, 6) / float(stat['time_delta'])),
                                   mem_str(item[1]['wbytes'] * pow(10, 6) / float(stat['time_delta']))))
                idx += 1


class ColumnWidget(Widget):
    def __init__(self, name, columns, parent, border=True, colorindex=0):
        DEBUG(self.__init__, "ColumnWidget.__init__(name='%s', columns='%s', parent='%s')" % (name, columns, parent), "")

        self.__check_column_valid(columns)
        self.__name = name
        self.__columns = columns
        self.__border = border
        self.__colorindex = colorindex
        enabled_columns = filter(lambda c: c.enable(), self.valid_columns())
        width = len(" ".join([c.header() for c in enabled_columns])) + 2 # 2 padding
        width = max(width, 18)
        if border:
            width += 2
        Widget.__init__(self, Global.WIDGET_HEIGHT, width, parent)

    def __check_column_valid(self, columns):
        for c in columns:
            try:
                c.value(oceanbase.sample)
            except KeyError:
                c.valid(False)

    def valid_columns(self):
        return filter(lambda c: c.valid(), self.__columns)

    def update(self):
        enabled_columns = filter(lambda c: c.enable(), self.valid_columns())
        width = len(" ".join([c.header() for c in enabled_columns])) + 2 # 2 border + 2 padding
        if self.__border:
            width += 2
        self.min_width(width)

    def redraw(self):
        def stat_count():
            return oceanbase.stat_count()
        def latest(num):
            return oceanbase.latest(num)

        enabled_columns = filter(lambda c: c.enable(), self.valid_columns())

        DEBUG(self.redraw, "ColumnWidget.redraw(stat_count=%d, enabled_columns='%s') " % (oceanbase.stat_count(), enabled_columns), "")

        if self.__border:
            if self.select():
                self.win().attron(curses.color_pair(1) | curses.A_BOLD)
                self.win().box()
                self.win().attroff(curses.color_pair(1) | curses.A_BOLD)
            else:
                self.win().box()
            if self.__name:
                self.win().addstr(0, 2, " " + self.__name + " ", curses.color_pair(3))
            print_lines = min(stat_count(), self.height() - 3)
            self.win().addstr(1, 1, " " + " ".join([ c.header() for c in enabled_columns ]) + " ", curses.color_pair(2))
            border_offset = 1
        else:
            print_lines = min(stat_count(), self.height() - 1)
            self.win().addstr(0, 0, " " + " ".join([ c.header() for c in enabled_columns ]) + " ", curses.color_pair(2))
            border_offset = 0
        latest = latest(print_lines)

        DEBUG(self.redraw, "ColumnWidget.redraw  latest stat[%s/%s]:" % (print_lines, stat_count()) , latest)



        li = []
        for i in range(0, len(latest)):
            item = []
            for c in enabled_columns:

                v = c.value(latest[i])

                DEBUG(self.redraw, "ColumnWidget.redraw : item value:  %s:%s" % (c, v), "")

                item.append(str(v))


            li.append(" " + " ".join(item))


            for ii in range(0, len(li)):
                if ii==0:
                    self.win().addstr(ii + 1 + border_offset, border_offset, li[ii], curses.color_pair(10))
                else:
                    self.win().addstr(ii + 1 + border_offset, border_offset, li[ii], curses.color_pair(self.__colorindex))

        #DEBUG(self.redraw, "ColumnWidget.redraw latest output :", li[3])

class HeaderColumnWidget(Widget):
    def __init__(self, name, columns, parent, padding=0, get_header=None, colorindex=0):
        DEBUG(self.__init__, "HeaderColumnWidget.__init__(name='%s', columns='%s', parent='%s')" % (name, columns, parent), "")
        self.__name = name
        self.__padding = padding
        self.__get_header = get_header
        Widget.__init__(self, Global.WIDGET_HEIGHT, 0, parent)

        self.__column_widget = ColumnWidget(None, columns, self.win(), False, colorindex)
        self.min_width(self.__column_widget.min_width() + 2)
        self.resize(self.min_height(), self.min_width())


    def redraw(self):
        DEBUG(self.redraw, "HeaderColumnWidget.redraw() ", "")
        if self.select():
            self.win().attron(curses.color_pair(1))
            self.win().box()
            self.win().attroff(curses.color_pair(1))
        else:
            self.win().box()

        nline = 1

        if self.__get_header and oceanbase.stat_count() > 0:
            header = self.__get_header(oceanbase.now())
            string = ""
            hitems = header.items()
            hitems.sort()
            DEBUG(self.redraw, "HeaderColumnWidget.redraw() header.items() ", hitems)
            for item in hitems :
                item_str = ": ".join([item[0], str(item[1])])
                if (len(string) + len(item_str) + 2 > self.min_width() - 2):
                    string = string[2:]
                    self.win().addstr(nline, 1, string.center(self.min_width() - 2))
                    string = ""
                    nline += 1
                string += "  " + item_str
            if string:
                string = string[2:]
                self.win().addstr(nline, 1, string.center(self.min_width() - 2))
                self.__set_padding(nline)
            else:                         # fix ob 0.4.1 wired bug
                self.__set_padding(0)
        self.__column_widget.redraw()
        if self.__name:
            self.win().addstr(0, 2, " " + self.__name + " ", curses.color_pair(3))
        time = strftime(" %Y-%m-%d %T ")
        self.win().addstr(0, self.width() - len(time) - 2, time, curses.color_pair(3))

    def resize(self, height=None, width=None):
        DEBUG(self.resize, "height, width", "|".join([str(height), str(width)]))
        if height is not None and width is not None:
            Widget.resize(self, height, width)
            maxy, maxx = self.win().getmaxyx()
            self.__column_widget.resize(maxy - self.__padding - 2, maxx - 2)
        if height is None:
            height = self.__column_widget.min_height() + self.__padding + 2
            self.resize(height, width)
        if width is None:
            width = self.__column_widget.min_width() + 2
            self.min_width(width)
            self.resize(height, width)

    def move(self, y, x):
        self.win().mvderwin(y, x)
        self.__column_widget.win().mvderwin(self.__padding + 1, 1)

    def __set_padding(self, padding):
        self.min_height(padding + 2 + 2)
        maxy, maxx = self.win().getmaxyx()
        self.__padding = padding
        self.__column_widget.move(0, 0)
        self.__column_widget.resize(maxy - self.__padding - 2, maxx - 2)
        self.__column_widget.move(self.__padding + 1, 1)

    def update(self):
        self.__column_widget.update()

    def host(self):
        return self.__name.split(":")[0]

    def sql_port(self):
        return self.__name.split(":")[1]

    def column_widget(self):
        return self.__column_widget


class StatusWidget(Widget):
    def __init__(self, parent):
        DEBUG(self.__init__, "StatusWidget.__init__(parent='%s') " % parent, "")
        self.__parent = parent
        maxy, maxx = parent.getmaxyx()
        Widget.__init__(self, 2, maxx, parent)

        self.resize(2, maxx)
        self.move(maxy - 2, 0)
        self.win().bkgd(curses.color_pair(1))

    def redraw(self):
        now = strftime("%Y-%m-%d %T")
        maxy, maxx = self.win().getmaxyx()
#       tps = self.__get_tps()
#       qps = self.__get_qps()
        svr_list = oceanbase.find_svr_list()["observer"]
        statstr = "HOST: %s:%d " % (Options.host, Options.port)
        statstr += "RUMC: %d" % (len(svr_list))
        try:
            self.win().addstr(1, maxx - len(now) - 2, now)
            self.win().addstr(1, 2, statstr)
            self.win().hline(0, 0, curses.ACS_HLINE, 1024)
        except curses.error:
            pass



    def __get_tps(self):
        if oceanbase.stat_count() > 1:
            last = oceanbase.now()
            insert = sum([item["SQL_INSERT_COUNT"]
                            for item in last[str(self.cur_tenant_id)].values()])
            replace = sum([item["SQL_REPLACE_COUNT"]
                            for item in last[str(self.cur_tenant_id)].values()])
            delete = sum([item["SQL_DELETE_COUNT"]
                            for item in last[str(self.cur_tenant_id)].values()])
            update = sum([item["SQL_UPDATE_COUNT"]
                            for item in last[str(self.cur_tenant_id)].values()])
            tps = float(insert + replace + delete + update) / seconds(last['time_delta'])
            return int(tps)
        else:
            return 0

    def __get_qps(self):
        if oceanbase.stat_count() > 1:
            last = oceanbase.now()
            total = float(sum([item["SQL_SELECT_COUNT"]
                               for item in last[oceanbase.__cur_tenant_id].values()]))
            qps = total / seconds(last['time_delta'])
            return int(qps)
        else:
            return 0


class HeaderWidget(Widget):
    def __init__(self, parent):
        DEBUG(self.__init__, "HeaderWidget.__init__(parent='%s') " % parent, "")
        maxy, maxx = parent.getmaxyx()
        Widget.__init__(self, 2, maxx, parent)
        self.__page = None
        self.win().bkgd(curses.color_pair(1))

    def redraw(self):
        DEBUG(self.redraw, "HeaderWidget.redraw ", "")
        self.erase()
        if self.__page is None:
            try:
                self.win().addstr(0, 2, '1:Help 2:Gallery 3:SQL(ObServer) 4:History(30 minutes) 5:Bianque 6:Machine Stat 7-0:Blank', curses.A_BOLD)
                self.win().hline(1, 0, curses.ACS_HLINE, 1024, curses.A_BOLD)
            except curses.error:
                pass
        else:
            maxy, maxx = self.win().getmaxyx()
            shown_num = len(self.__page.shown_widgets())
            valid_num = len(self.__page.valid_widgets())
            all_num = len(self.__page.all_widgets())
            shown_str = 'Shown: %d / Valid: %d / Total: %d' % (shown_num, valid_num, all_num)
            try:
                self.win().addstr(0, 2, self.__page.title(), curses.A_BOLD)
                self.win().addstr(0, maxx - len(shown_str) - 4, shown_str, curses.A_BOLD)
                self.win().hline(1, 0, curses.ACS_HLINE, 1024, curses.A_BOLD)
            except curses.error:
                pass

    def switch_page(self, page):
        DEBUG(self.switch_page, "HeaderWidget.switch_page(page='%s' " % (page) , "")
        self.__page = page


class MessageBox(object):
    def __init__(self, parent, msg):
        self.msg = msg
        self.parent = parent
        maxy, maxx = parent.getmaxyx()
        width = min(maxx - 20, len(msg)) + 10
        height = 5
        y = (maxy - height) / 2
        x = (maxx - width) / 2
        self.win = parent.derwin(height, width, y, x)
        self.win.erase()
        self.win.attron(curses.color_pair(1))
        self.win.box()
        self.win.attroff(curses.color_pair(1))
        self.win.addstr(2, 4, self.msg, curses.color_pair(0))

    def run(self, anykey=False):
        res = True
        while (1):
            c = self.win.getch()
            if anykey:
                break
            elif c == ord('y'):
                res = True
                break
            elif c == ord('n'):
                res = False
                break
            elif c == ord('q'):
                res = False
                break
        self.win.erase()
        self.win.refresh()
        return res

class PopPad(Widget):
    def __init__(self, name, items, parent, **args):
        self.__offset_x = 0
        self.__offset_y = 0
        self.__name = name

        maxy, maxx = parent.getmaxyx()
        width = maxx - 100
        height = 10
        Widget.__init__(self, height, width, parent)
        self.resize(height, width)
        y = (maxy - height - self.__offset_y) / 2
        x = (maxx - width - self.__offset_x) / 2
        self.mvwin(self.__offset_y + y, self.__offset_x + x)
        self.move(self.__offset_y + y, self.__offset_x + x)

    def __do_key(self):
        ch = self.win().getch()
        stopFlg = False
        if ch in (ord("q"), ord("Q")):
            stopFlg = True

    def __redraw(self):
        self.win().erase()
        self.win().attron(curses.color_pair(1))
        self.win().box()
        self.win().attroff(curses.color_pair(1))
        self.win().addstr(0, 2, ' %s ' % self.__name, curses.color_pair(3))

    def run(self):
        self.__redraw()
        while(True):
            if self.__do_key():
                self.win().erase()
                self.win().refresh()
                return
            self.__redraw()
            self.win().refresh()

class SelectionBox(Widget):
    __padding_left = 5
    __padding_top = 0
    __offset_y = 0
    __offset_x = 0
    __chr_list = 'abcdefghimorstuvwxyzABCDEFGHIMORSTUVWXYZ0123456789'
    __hot_key = False

    def __init__(self, name, items, parent, **args):
        self.__index = 0
        self.__name = name
        self.items = items
        self.parent = parent
        self.__start_idx = 0
        self.__stop_idx = 0
        s = list(self.__chr_list)
        shuffle(s)
        self.__chr_list = ''.join(s)
        self.__offset_y = args.get('offset_y', self.__offset_y)
        self.__offset_x = args.get('offset_x', self.__offset_x)
        self.__hot_key = args.get('hot_key', self.__hot_key)

        maxy, maxx = parent.getmaxyx()
        width = max(len(name) + 6, max([len(it) + 5 for it in items]))
        height = min(len(items) + 2, maxy - self.__offset_y)
        Widget.__init__(self, height, width, parent)
        self.resize(height, width)
        y = (maxy - height - self.__offset_y) / 2
        x = (maxx - width - self.__offset_x) / 2
        self.mvwin(self.__offset_y + y, self.__offset_x + x)
        self.move(self.__offset_y + y, self.__offset_x + x)
        self.win().nodelay(1)
        self.win().timeout(0)
        self.win().keypad(1)
        self.__stop_idx = self.__start_idx + self.height() - 2

    def __do_key(self):
        ch = self.win().getch()
        stop = False
        if self.__fm is not None and ch != -1:
            self.__fm()
            self.__fm = None
        elif ch in [ord('s'), ord('S')]:
            self.__index = -2
            stop = True
        if ch in [ ord('j'), ord('\t'), ord('n'), ord('J'), ord('N'), curses.KEY_DOWN ]:
            if self.__index == len(self.items) - 1:
                self.__start_idx = 0
                self.__stop_idx = self.__start_idx + self.height() - 2
                self.__index = 0
            elif self.__index == self.__stop_idx - 1:
                self.__start_idx = self.__start_idx + 1
                self.__stop_idx =  self.__stop_idx + 1
                self.__index = self.__index + 1
            else:
                self.__index = self.__index + 1
        elif ch in [ ord('k'), ord('K'), ord('p'), ord('P'), curses.KEY_UP ]:
            if self.__index == 0:
                self.__start_idx = max(0, len(self.items) - self.height() + 2)
                self.__stop_idx = self.__start_idx + self.height() - 2
                self.__index = len(self.items) - 1
            elif self.__index == self.__start_idx:
                self.__start_idx = self.__start_idx - 1
                self.__stop_idx =  self.__stop_idx - 1
                self.__index = self.__index - 1
            else:
                self.__index = self.__index - 1
        elif ch in [ ord('\n'), ord(' ') ]:
            stop = True
        elif ch in [ ord('q'), ord('Q') ]:
            self.__index = -1
            stop = True
        elif not self.__hot_key:
            pass
        elif ch in [ ord(c) for c in self.__chr_list ]:
            self.__index = [ ord(c) for c in self.__chr_list ].index(ch) + self.__start_idx
            stop = True
        return stop

    def __redraw(self):
        self.win().erase()
        self.win().attron(curses.color_pair(1))
        self.win().box()
        self.win().attroff(curses.color_pair(1))
        self.win().addstr(0, 2, ' %s ' % self.__name, curses.color_pair(3))
        for idx, item in enumerate(self.items):
            if idx < self.__start_idx or idx >= self.__stop_idx:
                continue

            if not self.__hot_key:
                line = item.center(self.width() - 2)
            elif idx - self.__start_idx < len(self.__chr_list):
                pref = ' %s)' % self.__chr_list[idx - self.__start_idx]
                line = pref + item.center(self.width() - 5)
            else:
                pref = '   '
                line = pref + item.center(self.width() - 5)
            flag = 0
            if idx == self.__index:
                flag = curses.A_BOLD | curses.color_pair(5)
            self.win().addstr(idx - self.__start_idx + self.__padding_top + 1, 1, line, flag)

    def run(self, first_movement=None):
        self.__fm = first_movement
        res = True
        while (1):
            if self.__do_key():
                self.win().erase()
                self.win().refresh()
                return self.__index
            self.__redraw()
            self.win().refresh()

class ColumnCheckBox(Widget):
    __padding_left = 5
    __padding_top = 0
    __margin_top = 4
    __margin_bottom = 4
    __offset_y = 0
    __offset_x = 0

    def __init__(self, name, items, parent, **args):
        self.__index = 0
        self.__name = name
        self.items = items
        self.parent = parent
        self.__start_idx = 0
        self.__stop_idx = 0

        self.__margin_top = args.get('marging_top', self.__margin_top)
        self.__margin_bottom = args.get('marging_bottom', self.__margin_bottom)
        self.__offset_y = args.get('offset_y', self.__offset_y)
        self.__offset_x = args.get('offset_x', self.__offset_x)
        maxy, maxx = parent.getmaxyx()
        width = max(len(name) + 6, max([len(str(it)) + 9 for it in items]))
        height = min(len(items) + 2, maxy - self.__offset_y - self.__margin_top - self.__margin_bottom)
        Widget.__init__(self, height, width, parent)
        self.resize(height, width)
        y = (maxy - height - self.__offset_y) / 2
        x = (maxx - width - self.__offset_x) / 2
        self.mvwin(self.__offset_y + y, self.__offset_x + x)
        self.move(self.__offset_y + y, self.__offset_x + x)
        self.win().nodelay(1)
        self.win().timeout(0)
        self.win().keypad(1)
        self.__stop_idx = self.__start_idx + self.height() - 2

    def __do_key(self):
        ch = self.win().getch()
        stop = False
        if self.__fm is not None and ch != -1:
            self.__fm()
            self.__fm = None
        if ch in [ ord('j'), ord('\t'), ord('n'), ord('J'), ord('N'), curses.KEY_DOWN ]:
            if self.__index == len(self.items) - 1:
                self.__start_idx = 0
                self.__stop_idx = self.__start_idx + self.height() - 2
                self.__index = 0
            elif self.__index == self.__stop_idx - 1:
                self.__start_idx = self.__start_idx + 1
                self.__stop_idx =  self.__stop_idx + 1
                self.__index = self.__index + 1
            else:
                self.__index = self.__index + 1
        elif ch in [ ord('k'), ord('K'), ord('p'), ord('P'), curses.KEY_UP ]:
            if self.__index == 0:
                self.__start_idx = max(0, len(self.items) - self.height() + 2)
                self.__stop_idx = self.__start_idx + self.height() - 2
                self.__index = len(self.items) - 1
            elif self.__index == self.__start_idx:
                self.__start_idx = self.__start_idx - 1
                self.__stop_idx =  self.__stop_idx - 1
                self.__index = self.__index - 1
            else:
                self.__index = self.__index - 1
        elif ch in [ ord(' ') ]:
            self.items[self.__index].enable(not self.items[self.__index].enable())
        elif ch in [ ord('\n') ]:
            stop = True
        elif ch in [ ord('q'), ord('Q') ]:
            self.__index = -1
            stop = True
        return stop

    def __redraw(self):
        self.win().erase()
        self.win().attron(curses.color_pair(1))
        self.win().box()
        self.win().attroff(curses.color_pair(1))
        self.win().addstr(0, 2, ' %s ' % self.__name, curses.color_pair(3))
        for idx, item in enumerate(self.items):
            if idx < self.__start_idx or idx >= self.__stop_idx:
                continue
            pref = ' [%s]' % (item.enable() and 'X' or ' ')
            line = pref + str(item).center(self.width() - 6)
            flag = 0
            if idx == self.__index:
                flag = curses.A_BOLD | curses.color_pair(5)
            self.win().addstr(idx - self.__start_idx + self.__padding_top + 1, 1, line, flag)

    def run(self, first_movement=None):
        self.__fm = first_movement
        res = True
        while (1):
            if self.__do_key():
                self.win().erase()
                self.win().refresh()
                return self.__index
            self.__redraw()
            self.win().refresh()
        if self.__index == -1:
            return []
        else:
            return self.items


class InputBox(Widget):
    def __init__(self, parent, prompt="Password", password=False, width=30):
        self.__index = 0
        self.__name = "name"
        self.__offset_x = 0
        self.__offset_y = 0
        self.__password = password
        height = 3
        win = curses.newwin(0, 0, 0, 0)
        Widget.__init__(self, height, width, win, True)
        self.resize(height, width)
        maxy, maxx = parent.getmaxyx()
        y = (maxy - height - self.__offset_y) / 2
        x = (maxx - width - self.__offset_x) / 2
        self.mvwin(y, x)
        win.box()
        win.refresh()
        self.__prompt = prompt + ": "
        win.addstr(1, 1, self.__prompt)
        win.move(1, 1 + len(self.__prompt))
        self.__textbox = curses.textpad.Textbox(win)
        self.__result = ""

    def validator(self, ch):
        y, x = self.win().getyx()
        if ch == 127:
            new_x = max(x - 1, len(self.__prompt) + 1)
            self.win().delch(y, new_x)
            self.win().insch(' ')
            self.win().move(y, new_x)
            self.__result = self.__result[:-1]
            return 0
        elif ch == 7 or ch == 10:                             # submit
            return 7
        elif x == self.width() - 2:
            return 0
        elif ch < 256:
            self.__result += chr(ch)
            if self.__password:
                return ord('*')
            else:
                return ch

    def run(self):
        curses.noecho()
        curses.curs_set(1)
        self.__textbox.edit(self.validator)
        curses.curs_set(0)
        return self.__result


class GalleryPage(Page):
    def __add_widgets(self):
        DEBUG(self.__add_widgets, "GalleryPage.__add_widgets() tenant_id : ", self.cur_tenant_id)

        time_widget = ColumnWidget("TIME-TENANT", [
            Column("time", lambda stat:stat["timestamp"].strftime("%H:%M:%S"), 8, False),
            Column("tenant", lambda stat:str(stat["tenant"]),  6, False ),
            ], self.win(), True, 3)

        sql_count_widget = ColumnWidget("SQL COUNT", [
            Column("sel.", lambda stat:sum([item["sql select count"] for item in stat[str(self.cur_tenant_id)].values()]), 6, True),
            Column("ins.", lambda stat:sum([ item["sql insert count"] for item in stat[str(self.cur_tenant_id)].values() ]), 6, True),
            Column("upd.", lambda stat:sum([ item["sql update count"] for item in stat[str(self.cur_tenant_id)].values() ]), 6, True),
            Column("del.", lambda stat:sum([ item["sql delete count"] for item in stat[str(self.cur_tenant_id)].values() ]), 6, True),
            Column("rep.", lambda stat:sum([ item["sql replace count"] for item in stat[str(self.cur_tenant_id)].values() ]), 5, True),
            Column("cmt.", lambda stat:sum([ item["trans commit count"] for item in stat[str(self.cur_tenant_id)].values() ]), 6, True),
            Column("rol.", lambda stat:sum([ item["trans rollback count"] for item in stat[str(self.cur_tenant_id)].values() ]), 4, True),
            ], self.win(), True, 6)

        sql_rt_widget = ColumnWidget("SQL RT", [
            Column("sel.", lambda stat:
                (sum([ item["sql select time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["sql select count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                6, False),
            Column("ins.", lambda stat:
                (sum([ item["sql insert time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["sql insert count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                6, False),
            Column("upd.", lambda stat:
                (sum([ item["sql update time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["sql update count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                6, False),
            Column("del.", lambda stat:
                (sum([ item["sql delete time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["sql delete count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                6, False),
            Column("rep.", lambda stat:
                (sum([ item["sql replace time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["sql replace count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                5, False),
            Column("cmt.", lambda stat:
                (sum([ item["trans commit time"] for item in stat[str(self.cur_tenant_id)].values() ])
                 / float(sum([ item["trans commit count"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000),
                6, False),
            ], self.win(), True, 8)

        net_rt_widget = ColumnWidget("RPC", [
            Column("fail", lambda stat:sum([item["rpc deliver fail"] for item in stat[str(self.cur_tenant_id)].values()]),
                4, True),
            Column("net", lambda stat:sum([ item["rpc net delay"] for item in stat[str(self.cur_tenant_id)].values() ]) / float(sum([ item["rpc packet in"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000,
                4, False),
            Column("frame", lambda stat:sum([ item["rpc net frame delay"] for item in stat[str(self.cur_tenant_id)].values() ]) / float(sum([ item["rpc packet in"] for item in stat[str(self.cur_tenant_id)].values() ]) or 1) / 1000,
                4, False),
            ], self.win(), True, 0)

        memory_widget = ColumnWidget("MEMORY(T)", [
            Column("⊿active", lambda stat:
                    sum([item["active memstore used"] for item in stat[str(self.cur_tenant_id)].values()]),
                7, True),
            Column("TOTAL", lambda stat:
                    sum([item["total memstore used"] for item in stat[str(self.cur_tenant_id)].values()]),
                7, False),
            Column("PCT.", lambda stat:
                    sum([item["total memstore used"] for item in stat[str(self.cur_tenant_id)].values()])
                    / float(sum([item["memstore limit"] for item in stat[str(self.cur_tenant_id)].values()]) or 1),
                6, False)
            ], self.win(), True, 8)

        iops_widget = ColumnWidget("IOPS", [
            Column("SES.", lambda stat:
                   sum([item["active sessions"] for item in stat[str(self.cur_tenant_id)].values() ]),
                6, True),
            Column("ior", lambda stat:
                   sum([item["io read count"] for item in stat[str(self.cur_tenant_id)].values() ]),
                6, True),
            Column("ior-sz", lambda stat:
                   sum([item["io read bytes"] for item in stat[str(self.cur_tenant_id)].values() ]),
                7, True),
            Column("iow", lambda stat:
                   sum([item["io write count"] for item in stat[str(self.cur_tenant_id)].values() ]),
                5, True),
            Column("iow-sz", lambda stat:
                   sum([item["io write bytes"] for item in stat[str(self.cur_tenant_id)].values() ]),
                6, True),
            ], self.win(), True, 6)

        self.add_widget(time_widget)
        self.add_widget(sql_count_widget)
        self.add_widget(sql_rt_widget)
        self.add_widget(net_rt_widget)
        self.add_widget(memory_widget)
        self.add_widget(iops_widget)

    def __init__(self, y, x, h, w, parent):

        Page.__init__(self, parent, Layout(), y, x, h, w)

        try:
            self.__add_widgets()
        except curses.error:
            pass

    def title(self):
        return "Gallery"

    def process_key(self, ch):
        if ch == ord('j'):
            pass
        else:
            Page.process_key(self, ch)

class BianquePage(Page):
    def update_widgets(self):
        pass

    def title(self):
        return "Bianque"

    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)
        try:
            self.update_widgets()
        except curses.error:
            pass

class HistoryPage(Page):
    def update_widgets(self):
        pass

    def title(self):
        return "History"

    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)
        try:
            self.update_widgets()
        except curses.error:
            pass

#DEV = file("/Users/mq/Downloads/dooba.log", "w")
DEV = file("/dev/null","w")
def DEBUG(*args):
    global DEV
    if Options.debug :
        print >> DEV, "line %s :%s in [%s] %s %s" % (sys._getframe().f_lineno, time.asctime(), args[0].func_name, args[1], args[2])

class SQLPage(Page):
    def update_widgets(self):
        DEBUG(self.update_widgets, "SQLPage.update_widgets() ", "")

        def observer_info(stat, ip):
            def try_add(name, key, wrapper):
                try:
                    if key in oceanbase.delta_key_list:
                        result[name] = wrapper(int(stat[str(oceanbase.get_current_tenant())][ip][key] / seconds(stat['time_delta'])))
                    else:
                        result[name] = wrapper(stat[str(oceanbase.get_current_tenant())][ip][key])

                    DEBUG(try_add,"SQLPage.try_add(name='%s', key='%s', wrapper='%s', value='%s'" % (name, key, wrapper, result[name]), "")
                except Exception as e:
                    DEBUG(try_add, "SQLPage.try_add(name='%s', key='%s', wrapper='%s' exception :" % (name, key, wrapper) , e)


            def try_add2(name, key1, key2, wrapper):
                try:
                    result[name] = wrapper(float(stat[str(oceanbase.get_current_tenant())][ip][key1] )/ float((stat[str(oceanbase.get_current_tenant())][ip][key1] + stat[str(oceanbase.get_current_tenant())][ip][key2]) or 1))
                    DEBUG(try_add2, "wrapper %s" % (name) , (stat[str(oceanbase.get_current_tenant())][ip][key1] + stat[str(oceanbase.get_current_tenant())][ip][key2]))
                    DEBUG(try_add2, "wrapper %s" % (name) , stat[str(oceanbase.get_current_tenant())][ip][key1])
                    DEBUG(try_add2, "%s result " % (name) , result[name])
                except Exception as e:
                    DEBUG(try_add2, "SQLPage.try_add2(name='%s', key1='%s', key2='%s' ) exception" % (name ,key1 ,key2), e)
                    pass

            result = dict()
            try_add("Active Sess", "active sessions", count_str)
            try_add("IO-R Cnt", "io read count", mem_str)
            try_add("IO-R Size", "io read bytes", mem_str)
            try_add("IO-W Cnt", "io write count", mem_str)
            try_add("IO-W Size", "io write bytes", mem_str)
            try_add("CPU", "cpu usage", percent_str)
            try_add2("Cache-Row Hit", "row cache hit", "row cache miss", percent_str)
            try_add2("Cache-Loc Hit", "location cache hit", "location cache miss", percent_str)
            try_add2("Cache-Blk Hit", "block cache hit", "block cache miss", percent_str)
            try_add2("Cache-BI Hit", "block index cache hit", "block index cache miss", percent_str)
            #try_add2("B-F-cache hit", "bloom filter cache hit", "bloom filter cache miss", percent_str)


            return result

        svr = "observer"
        self.clear_widgets()
        DEBUG(self.update_widgets, "SQLPage.update_widgets svr_list :", oceanbase.get_tenant_svr())

        i = 0
        ci = 0
        for ms in oceanbase.get_tenant_svr()[svr]:
            i = i+1
            if i % 2 == 0 :
                ci = 6
            else:
                ci = 8

            zone = ms['zone']
            #ip = ms['ip']
            #port = ms['port']
            ip = '%s:%s' % (ms['ip'], ms['port'])
            DEBUG(observer_info, "oceanbase.get_current_tenant()", oceanbase.get_current_tenant())
            f = ColumnFactory(svr, ip)
            DEBUG(self.update_widgets, "column factory:", f)
            #widget = HeaderColumnWidget( '%s:%s:%d' % (zone,ip,int(port)), [
            widget = HeaderColumnWidget( '%s:%s' % (zone,ip), [
                #default(SQL)
                f.count("Sql Select Count", "ssc", "sql select count", duration=True, enable=True),
                f.time("Sql Select Time", "ssrt", "sql select time","sql select count" , duration=False,enable=True),
                f.count("Sql Insert Count", "sic", "sql insert count", duration=True, enable=True),
                f.time("Sql Insert Time", "sirt", "sql insert time","sql insert count", duration=False, enable=True),
                f.count("Sql Update Count", "suc", "sql update count", duration=True, enable=True),
                f.time("Sql Update Time", "surt", "sql update time", "sql update count", duration=False, enable=True),
                f.count("Sql Delete Count", "sdc", "sql delete count", duration=True, enable=True),
                f.time("Sql Delete Time", "sdrt", "sql delete time", "sql delete count", duration=False, enable=True),
                f.count("Sql Replace Count", "src", "sql replace count", duration=True, enable=True),
                f.time("Sql Replace Time", "srrt", "sql replace time","sql replace count", duration=False, enable=True),
                f.count("Trans Commit Count", "tcc", "trans commit count", duration=True, enable=True),
                f.time("Trans Commit Time", "tcrt", "trans commit time","trans commit count", duration=False, enable=True),
                f.count("Sql Local Count", "slc", "sql local count", duration=True, enable=True),
                f.count("Sql Remote Count", "src", "sql remote count", duration=True, enable=True),
                #f.count("Sql Distributed Count", "sdc", "sql distributed count", duration=True,enable=True),
                #f.count("Inner SQL Connection Execute Count", "iscec", "inner sql connection execute count"),
                #f.time("Inner SQL Connection Execute Time", "iscet", "inner sql connection execute time"),
                #inner packet
                #f.count("RPC Packet In", "rpci", "rpc packet in"),
                #f.count("RPC Packet In Bytes", "rpci(B)", "rpc packet in bytes"),
                #f.count("RPC Packet Out", "rpco", "rpc packet out"),
                #f.count("RPC Packet Out Bytes", "rpco(B)", "rpc packet out bytes"),
                #f.count("RPC Deliver Fail", "rpc fail", "rpc deliver fail"),
                #f.time("RPC Net Delay", "rpc delay", "rpc net delay"),
                #f.count("RPC Net Frame Delay", "rpc frame delay", "rpc net frame delay"),
                #outer packet
                #f.count("MySQL Packet In", "mpci", "mysql packet in"),
                #f.count("MySQL Packet In Bytes", "mpci(B)", "mysql packet in bytes"),
                #f.count("MySQL Packet Out", "mpco", "mysql packet out"),
                #f.count("MySQL Packet Out Bytes", "mpco(B)", "mysql packet out bytes"),
                #f.count("MySQL Deliver Fail", "mdf", "mysql deliver fail"),
                #request queue
                #f.count("Request Enqueue Count", "enqueue", "request enqueue count"),
                #f.count("Request Dequeue Count", "dequeue", "request dequeue count"),
                #f.time("Request Queue Time", "QT", "request queue time"),
                #transmition
                #f.time("Trans Commit Log Time", "tclt", "trans commit log time"),
                #f.count("Trans Commit Log Sync Count", "tclsc(sync)", "trans commit log sync count"),
                #f.count("Trans Commit LOg Submit Count", "tclsc(submit)", "trans commit log submit count"),
                #f.count("Trans System Trans Count", "tstc", "trans system trans count"),
                #f.count("Trans User Trans Count", "tutc", "trans user trans count"),
                #f.count("Trans Start Count", "tsc", "trans start count"),
                #f.count("Trans Total Used Time", "ttut", "trans total used time"),
                #f.count("Trans Rollback Count", "trc", "trans rollback count"),
                #f.time("Trans Rollback Time", "trt", "trans rollback time"),
                #f.count("Trans Timeout Count", "ttc", "trans timeout count"),
                #f.count("Trans Single Partition Count", "tspc", "trans single partition count"),
                #cache
                #FIXME
                #f.count("Row Cache Hit", "rch", "row cache hit"),
                #f.count("Row Cache Miss", "rcm", "row cache miss"),
                #f.count("Block Index Cache Hit", "bich", "block index cache hit"),
                #f.count("Block Index Cache Miss", "bicm", "block index cache miss"),
                #f.count("Bloom Filter Cache Hit", "bfch", "bloom filter cache hit"),
                #f.count("Bloom Filter Cache Miss", "bfcm", "bloom filter cache miss"),
                #f.count("Bloom Filter Filts", "bff", "bloom filter filts"),
                #f.count("Bloom Filter Passes", "bfp", "bloom filter passes"),
                #f.count("Block Cache Hit", "bch", "block cache hit"),
                #f.count("Block Cache Miss", "bcm", "block cache miss"),
                #f.count("Location Cache Hit", "lch", "location cache hit"),
                #f.count("Location Cache Miss", "lcm", "location cache miss"),
                #resources
                #f.count("Active Sessions", "as", "active sessions"),
                #f.count("IO Read Count", "iorc", "io read count"),
                #f.count("IO Read Delay", "iord", "io read delay"),
                #f.count("IO Read Bytes", "ior(B)", "io read bytes"),
                #f.count("IO Write Count", "iowc", "io write count"),
                #f.count("IO Write Delay", "iowd", "io write delay"),
                #f.count("IO Write Bytes", "iow(B)", "io write bytes"),
                #f.count("Memstore Scan Count", "msc", "memstore scan count"),
                #f.count("Memstore Scan Succ Count", "mssc", "memstore scan succ count"),
                #f.count("Memstore Scan Fail Count", "msfc", "memstore scan fail count"),
                #f.count("Memstore Get Count", "mgc", "memstore get count"),
                #f.count("Memstore Get Succ Count", "mgsc", "memstore get succ count"),
                #f.count("Memstore Get Fail Count", "mgfc", "memstore get fail count"),
                #f.count("Memstore Apply Count", "mac", "memstore apply count"),
                #f.count("Memstore Apply Succ Count", "masc", "memstore apply succ count"),
                #f.count("Memstore Apply Fail Count", "mafc", "memstore apply fail count"),
                #f.count("Memstore Row Count", "mrc", "memstore row count"),
                #f.time("Memstore Get Time", "mgt", "memstore get time"),
                #f.time("Memstore Scan Time", "mst", "memstore scan time"),
                #f.time("Memstore Apply Time", "mat", "memstore apply time"),
                #f.count("Memstore Read Lock Succ Count", "mrlsc", "memstore read lock succ count"),
                #f.count("Memstore Read Lock Fail Count", "mrlfc", "memstore read lock fail count"),
                #f.count("Memstore Write Lock Succ Count", "mwlsc", "memstore write lock succ count"),
                #f.count("Memstore Write Lock Fail Count", "mwlfc", "memstore write lock fail count"),
                #f.time("Memstore Wait Write Lock Time", "mwwlt", "memstore wait write lock time"),
                #f.time("Memstore Wait Read Lock Time", "mwrlt", "memstore wait read lock time"),
                #f.count("IO Read Micro Index Count", "iormic", "io read micro index count"),
                #f.count("IO Read Micro Index Bytes", "iormib", "io read micro index bytes"),
                #f.count("IO Prefetch Micro Block Count", "iopmbc", "io prefetch micro block count"),
                #f.count("IO Prefetch Micro Block Bytes", "iopmbb", "io prefetch micro block bytes"),
                #f.count("IO Read Uncompress Micro Block Count", "iorumbc", "io read uncompress micro block count"),
                #f.count("IO Read Uncompress Micro Block Bytes", "iorumbb", "io read uncompress micro block bytes"),
                #f.count("Active Memstore Used", "amu", "active memstore used"),
                #f.count("Total Memstore Used", "tmu", "total memstore used"),
                #f.count("Major Freeze Trigger", "mft", "major freeze trigger"),
                #f.count("Memstore Limit", "ml", "memstore limit"),
                #f.count("Min Memory Size", "mms(min)", "min memory size"),
                #f.count("Max Memory Size", "mms(max)", "max memory size"),
                #f.count("Memory Usage", "mu", "memory usage"),
                #f.count("Min CPUS", "mc(min)", "min cpus"),
                #f.count("Max CPUS", "mc(max)", "max cpus"),
                #meta
                #f.count("Refresh Schema Count", "rsc", "refresh schema count"),
                #f.time("Refresh Schema Time", "rst", "refresh schema time"),
                #f.count("Partition Table Operator Get Count", "ptogc", "partition table operator get count"),
                #f.time("Partition Table Operator Get Time", "ptogt", "partition table operator get time"),
                #clog
                #f.count("Submitted To Sliding Window Log Count", "sswlc", "submitted to sliding window log count"),
                #f.count("Submitted To Sliding Window Log Size", "sswls", "submitted to sliding window log size"),
                #f.count("Index Log Flushed Count", "ilfc", "index log flushed count"),
                #f.count("Index Log Flushed Clog Size", "ilfcs", "index log flushed clog size"),
                #f.count("Clog Flushed Count", "cfc", "clog flushed count"),
                #f.count("Clog Flushed Size", "cfs", "clog flushed size"),
                #f.count("Clog Read Count", "crc(read)", "clog read count"),
                #f.count("Clog Read Size", "crs", "clog read size"),
                #f.count("Clog Disk Read Size", "cdrs", "clog disk read size"),
                #f.count("Clog Disk Read Count", "cdrc", "clog disk read count"),
                #f.time("Clog Disk Read Time", "cdrt", "clog disk read time"),
                #f.count("Clog Fetch Log Size", "cfls", "clog fetch log size"),
                #f.count("Clog Fetch Log Count", "cflc", "clog fetch log size"),
                #f.count("Clog Fetch Log By Location Size", "cflbls", "clog fetch log by location size"),
                #f.count("Clog Fetch Log By Location Count", "cflblc", "clog fetch log by location count"),
                #f.count("Clog Read Request Succ Size", "crrss", "clog read request succ size"),
                #f.count("Clog Read Request Succ Count", "crrsc", "clog read request succ count"),
                #f.count("Clog Read Request Fail Count", "crrfc", "clog read request fail count"),
                #f.time("Clog Confirm Delay Time", "ccdt", "clog confirm delay time"),
                #f.count("Clog Flush Task Generate Count", "cftgc", "clog flush task generate count"),
                #f.count("Clog Flush Task Release Count", "cftrc", "clog flush task release count"),
                #f.count("Clog RPC Delay Time", "crdt", "clog rpc delay time"),
                #f.count("Clog RPC Count", "crc(rpc)", "clog rpc count"),
                #f.count("Clog Non KV Cache Hit Count", "cnkchc", "clog non kv cache hit count"),
                #f.time("Clog RPC Request Handle Time", "crrht", "clog rpc request handle time"),
                #f.count("Clog RPC Request Count", "crrc", "clog rpc request count"),
                #f.count("Clog Cache Hit Count", "cchc", "clog cache hit count")
                ], self.win(), get_header=lambda stat,ip=ip: observer_info(stat,ip) , colorindex=ci)
            self.add_widget(widget)

    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)
        try:
            self.update_widgets()
        except curses.error:
            pass
    def select_columns(self):
        if len(self.all_widgets()) > 0:
            all_widgets = [hc_widget.column_widget() for hc_widget in self.all_widgets()]
            columns = all_widgets[0].valid_columns()
            i = ColumnCheckBox("Select Columns", columns, self.parent()).run()
            for w in all_widgets:
                w.valid_columns()[i].enable(enable=(not w.valid_columns()[i].enable()))
                w.update()
#           for widget in all_widgets:
#               for idx in range(0, len(columns)):
#                   widget.valid_columns()[idx].enable(columns[idx].enable(enable=True))
#               widget.update()
#           [hc_widget.resize() for hc_widget in self.all_widgets()]
#           self.rearrange()

    def process_key(self, ch):
        w = self.selected_widget()
        if ch == ord('m'):
            curses.endwin()
            oceanbase.mysql(host=w.host(), port=int(w.sql_port()))
            curses.doupdate()
        elif ch == ord('o'):
            pass
        elif ch == ord('O'):
            like_str = InputBox(self.win(), prompt="LIKE STR").run()
            oceanbase.show_sql_result("select name,value from __all_sys_config_stat where svr_ip='%s' and svr_type='mergeserver' and name like '%s' order by name" % (w.host(), like_str))
        elif Options.env == 'online' and ch == ord('l'):
            cmd = "ssh -t %s 'less oceanbase/log/mergeserver.log'" % w.host()
            curses.endwin()
            os.system(cmd)
            curses.doupdate()
        elif ch == ord("i"):
            self.select_columns()
        else:
            Page.process_key(self, ch)

    def title(self):
        return "SQLPage"

class EventPage(Page):
    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)

    def title(self):
        return "EventPage"

    def update_widgets(self):
        Page.update_widgets(self)

class MachineStatPage(Page):
    def update_widgets(self):
        pass

    def title(self):
        return "Machine Stat"

    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)

class HelpPage(Page):
    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)
        self.win().bkgd(curses.color_pair(4))

    def redraw(self):
        nline = [0]                       # hack mutating outer variables for python2
        def addline(x, line, attr=0):
            self.win().addstr(nline[0], x, line, attr)
            nline[0] += 1
        def addkeys(keys):
            for key_item in keys:
                if 0 == len(key_item):
                    string = ""
                else:
                    string = "    %-14s: %s" % key_item
                addline(4, string)
            nline[0] += 1
        def addgroup(group_name, keys):
            addline(4, group_name, curses.color_pair(4) | curses.A_BOLD)
            addline(2, "----------------------------------------------", curses.color_pair(4) | curses.A_BOLD)
            addkeys(keys)
        try:
            Page.redraw(self)
            ob_keys = [('c', 'Switch between tenants'),
                       ('w', 'write a screenshot file to current window')]
            addgroup("Global Keys  -  oceanbase", ob_keys)
            widget_keys = [('Tab','Select next widget'),
                           ('m', 'Connect to oceanbase lms for this cluster using mysql'),
                           ('j', 'ssh to selected host')]
            addgroup("Global Keys  -  Widget", widget_keys)
            pages_keys = [('1 F1', 'Help page'), ('2 F2', 'Gallery page'), ('3 F3', 'Observer page'), ('4 F4', 'History page'),
                          ('d', 'Delete selected widget'), ('R', 'Restore deleted widgets'),
                          ('=', 'Filter Columns for current page (ms,cs,ups page only)')]
            addgroup("Global Keys  -  Page", pages_keys)
            test_keys = [('p', 'Messsage box tooltips')]
            addgroup("Global Keys  -  Test", test_keys)
            select_keys = [('DOWN TAB J P', 'Next item'), ('UP K N', 'Previous item'),
                           ('SPC ENTER', 'Select current item'),
                           ('Q q', 'Quit selection box')]
            addgroup("Global Keys  -  Selection Box", select_keys)
            system_keys = [('q', 'quit dooba')]
            addgroup("Global Keys  -  System", system_keys)
            support = [
                ('Author', 'Yudi Shi (fufeng.syd)'),
                ('Mail', 'fufeng.syd@alipay.com'),
                (),
                ('project page', ''),
                ('bug report', ''),
                ('feature req', '')
                ]
            addgroup("Support", support)
        except curses.error:
            pass

    def title(self):
        return 'Help'


class BlankPage(Page):
    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)

    def title(self):
        return 'Blank Page'

class FalloutPage(Page):
    def __init__(self, y, x, h, w, parent):
        Page.__init__(self, parent, Layout(), y, x, h, w)
        self.c = 0

    def redraw(self):
        if self.c < 9:
            _____="""FFFFFFFFFFFFFFFFFFFFFF                lllllll lllllll                                           tttt\nF::::::::::::::::::::F                l:::::l l:::::l                                        ttt:::t\nF::::::::::::::::::::F                l:::::l l:::::l                                        t:::::t\nFF::::::FFFFFFFFF::::F                l:::::l l:::::l                                        t:::::t\n  F:::::F       FFFFFFaaaaaaaaaaaaa    l::::l  l::::l    ooooooooooo   uuuuuu    uuuuuuttttttt:::::ttttttt\n  F:::::F             a::::::::::::a   l::::l  l::::l  oo:::::::::::oo u::::u    u::::ut:::::::::::::::::t\n  F::::::FFFFFFFFFF   aaaaaaaaa:::::a  l::::l  l::::l o:::::::::::::::ou::::u    u::::ut:::::::::::::::::t\n  F:::::::::::::::F            a::::a  l::::l  l::::l o:::::ooooo:::::ou::::u    u::::utttttt:::::::tttttt\n  F:::::::::::::::F     aaaaaaa:::::a  l::::l  l::::l o::::o     o::::ou::::u    u::::u      t:::::t\n  F::::::FFFFFFFFFF   aa::::::::::::a  l::::l  l::::l o::::o     o::::ou::::u    u::::u      t:::::t\n  F:::::F            a::::aaaa::::::a  l::::l  l::::l o::::o     o::::ou::::u    u::::u      t:::::t\n  F:::::F            a::::aaaa::::::a  l::::l  l::::l o::::o     o::::ou::::u    u::::u      t:::::t\n  F:::::F           a::::a    a:::::a  l::::l  l::::l o::::o     o::::ou:::::uuuu:::::u      t:::::t    tttttt\nFF:::::::FF         a::::a    a:::::a l::::::ll::::::lo:::::ooooo:::::ou:::::::::::::::uu    t::::::tttt:::::t\nF::::::::FF         a:::::aaaa::::::a l::::::ll::::::lo:::::::::::::::o u:::::::::::::::u    tt::::::::::::::t\nF::::::::FF          a::::::::::aa:::al::::::ll::::::l oo:::::::::::oo   uu::::::::uu:::u      tt:::::::::::tt\nF::::::::FF          a::::::::::aa:::al::::::ll::::::l oo:::::::::::oo   uu::::::::uu:::u      tt:::::::::::tt\nFFFFFFFFFFF           aaaaaaaaaa  aaaallllllllllllllll   ooooooooooo       uuuuuuuu  uuuu        ttttttttttt\n\n\n                                             00000000000000000\n                                  00000000000000000000000000000000000000\n                      0000000000000000000000000000000000000000000000000000000000000\n               0000000000000000000000000000000000000000000000000000000000000000000000000000000\n          000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n        00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n     00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n    0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n     00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n           0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n                0000000000000000000000000000000000000000000000000000000000000000000000000000000\n                                 0000000000000000000000000000000000000000\n                                    000000000000000000000000000000000\n                                           0000000000000000000\n                                                 0000000\n                                                   000\n                                                   000\n                                                   000\n                                                  00000\n                    000000                      000000000                    00000\n                 000000000000                000000000000000              00000000000\n            000000000000000000000000000000000000000000000000000000000000000000000000000000000\n00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\n__      ____ _ _ __  __      ____ _ _ __   _ __   _____   _____ _ __    ___| |__   __ _ _ __   __ _  ___  ___\n\ \ /\ / / _` | '__| \ \ /\ / / _` | '__| | '_ \ / _ \ \ / / _ \ '__|  / __| '_ \ / _` | '_ \ / _` |/ _ \/ __|\n \ V  V / (_| | | _   \ V  V / (_| | |    | | | |  __/\ V /  __/ |    | (__| | | | (_| | | | | (_| |  __/\__ \\\n  \_/\_/ \__,_|_|( )   \_/\_/ \__,_|_|    |_| |_|\___| \_/ \___|_|     \___|_| |_|\__,_|_| |_|\__, |\___||___/\n                 |/                                                                           |___/\n
            """
            self.win().addstr(2, 0, _____, curses.color_pair(0))
        else:
            return

    def title(self):
        return "Fallout"


class Dooba(object):
    def build_oceanbase(self):
        return '''
  ___                       ____
 / _ \  ___ ___  __ _ _ __ | __ )  __ _ ___  ___
| | | |/ __/ _ \/ _` | \'_ \|  _ \ / _` / __|/ _ \\
| |_| | (_|  __/ (_| | | | | |_) | (_| \__ \  __/
 \___/ \___\___|\__,_|_| |_|____/ \__,_|___/\___|'''

    def build_dooba(self):
        return '''
     _             _
  __| | ___   ___ | |__   __ _
 / _` |/ _ \ / _ \| \'_ \ / _` |
| (_| | (_) | (_) | |_) | (_| |
 \__,_|\___/ \___/|_.__/ \__,_|'''

    def __init_curses(self, win):
        self.stdscr = win
        self.stdscr.keypad(1)
        self.stdscr.nodelay(1)
        self.stdscr.timeout(0)
        self.maxy, self.maxx = self.stdscr.getmaxyx()
        curses.curs_set(0)
        curses.noecho()
        self.__term = curses.termname()
        self.__init_colors()

    def __init_colors(self):
        if curses.has_colors():
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_RED, -1)   # header widget and status widget
            curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_GREEN) # column header
            curses.init_pair(3, curses.COLOR_MAGENTA, -1) # widget title
            curses.init_pair(4, curses.COLOR_YELLOW, -1) # help page color
            curses.init_pair(5, curses.COLOR_RED, curses.COLOR_CYAN)
            curses.init_pair(6, curses.COLOR_GREEN, -1) # column header
            curses.init_pair(7, curses.COLOR_WHITE, -1) # machine stat header
            curses.init_pair(8, curses.COLOR_BLUE, -1)
            curses.init_pair(9, curses.COLOR_MAGENTA, -1)
            curses.init_pair(10, curses.COLOR_WHITE, curses.COLOR_CYAN)


    def __resize_term(self):
        self.maxy, self.maxx = self.stdscr.getmaxyx()
        try:
            self.help_w.move(0, 0)
            self.help_w.mvwin(0, 0)
            self.help_w.resize(2, self.maxx)
            self.stat_w.move(self.maxy - 2, 0)
            self.stat_w.resize(2, self.maxx)
            for page in self.__all_page:
                page.resize(self.maxy - 4, self.maxx)
        except curses.error:
            pass
        self.stdscr.erase()

    def __do_key(self):
        ch = self.__all_page[self.__page_index].getch()
        if ch == ord('q'):
            curses.endwin()
            return True
        elif ch >= ord('0') and ch <= ord('9'):
            self.__page_index = ch - ord('0')
            self.help_w.switch_page(self.__all_page[self.__page_index])
        elif ch >= curses.KEY_F1 and ch <= curses.KEY_F9:
            self.__page_index = ch - curses.KEY_F1
            self.help_w.switch_page(self.__all_page[self.__page_index])
        elif ch == ord('\t'):
            self.__all_page[self.__page_index].select_next()
        elif ch == ord('w'):
            pagename = self.__all_page[self.__page_index].title()
            appname = oceanbase.app
            filename = "%s_%s.dooba.win.bz2" % (appname, pagename)
            tmpf = tempfile.TemporaryFile()
            self.stdscr.putwin(tmpf);
            tmpf.seek(0)
            f = bz2.BZ2File(filename, "w")
            f.write(tmpf.read())
            f.close()
            tmpf.close()
            MessageBox(self.stdscr, "[ INFO ] save screen to %s done!" % filename).run(anykey=True)
        #what is this ?
        elif ch == ord('p'):
            MessageBox(self.stdscr, "[TEST]What's up? Nigger! (- -)#").run()
        elif ch == ord('z'):
            items = [('check1  12344', False), ('check2 1', True), ('check3 124328432431', False)]
            CheckBox('Test check box', items, self.stdscr).run()
#       elif ch == ord('i'):
#           result = InputBox(self.stdscr).run()
#           MessageBox(self.stdscr, "[TEST] %s" % result).run(anykey=True)
        elif ch == ord('C'):
            pass

        elif ch == ord('c'):
            self.__select_cluster()
        elif ch == ord("t"):
            PopPad("Abbreviation", [], self.stdscr).run()
        self.__all_page[self.__page_index].process_key(ch)

    def __run(self):
        while (1):
            self.stdscr.erase()

            self.help_w.redraw()

            DEBUG(self.__run, "page __all_page[%s] redraw" % self.__page_index, self.__all_page[self.__page_index])
            self.__all_page[self.__page_index].redraw()
            self.stat_w.redraw()

            if (curses.is_term_resized(self.maxy, self.maxx)):
                self.__resize_term()
            if self.__do_key():
                break

            self.stdscr.refresh()
            sleep(0.05)

    def __select_cluster(self):
        DEBUG(self.__select_cluster,"oceanbase.tenant: %s" % (len(oceanbase.tenant)),"")
        if len(oceanbase.tenant) <= 1:
            idx = 0
        else:
            DEBUG(self.__select_cluster, "tnt.selected", [tnt.selected for tnt in oceanbase.tenant])
            idx = SelectionBox("Select Tenant", [ "[%s] %s " % ("*" if tnt.selected else " ", tnt.tenant_name) for tnt in oceanbase.tenant], self.stdscr).run()
            if True == oceanbase.tenant[idx].selected:
                pass
            else:
                tid = oceanbase.tenant[idx].tenant_id
                oceanbase.set_current_tenant(tid)

                if f_data_log != '':
                    if not check_datalog(Options.datalog, tid):
                        print "fail to write datalog [%s]" % (Options.datalog)

                oceanbase.switch_tenant(tid)
                DEBUG(self.__select_cluster, "global oceanbase", oceanbase)
                DEBUG(self.__select_cluster, "oceanbase.tenant", oceanbase.tenant[idx])
                DEBUG(self.__select_cluster, "oceanbase.tenant[idx]", oceanbase.tenant[idx].selected)
                DEBUG(self.__select_cluster, "oceanbase.get_current_tenant()", oceanbase.get_current_tenant())
                for page in self.__all_page:
                    page.cur_tenant_id = tid
                    page.update()
                DEBUG(self.__select_cluster, "page.cur_tenant_id", [" ".join([str(page.title()), page.cur_tenant_id]) for page in self.__all_page])

    def __show_logo(self):
        self.stdscr.hline(7, 0, curses.ACS_HLINE, 1024, curses.A_BOLD | curses.color_pair(1))
        self.stdscr.refresh()
        oceanbase_width = max([len(line) for line in self.build_oceanbase().split('\n')])
        oceanbase_height = len(self.build_oceanbase().split('\n'))
        ob_win = curses.newwin(oceanbase_height + 1, oceanbase_width + 1, 0, 10)
        ob_win.addstr(self.build_oceanbase(), curses.A_BOLD | curses.color_pair(1))
        ob_win.refresh()

        dooba_width = max([len(line) for line in self.build_dooba().split('\n')])
        dooba_height = len(self.build_dooba().split('\n'))
        dooba_win = curses.newwin(dooba_height + 1, dooba_width + 1, 0, self.maxx - dooba_width - 10)
        dooba_win.addstr(self.build_dooba(), curses.A_BOLD | curses.color_pair(6))
        dooba_win.refresh()

    def __cowsay(self, saying):
        cowsay = str(Cowsay(saying))
        cowsay_width = max([len(line) for line in cowsay.split('\n')])
        cowsay_height = len(cowsay.split('\n'))
        try:
            cowsay_win = curses.newwin(cowsay_height + 1, cowsay_width + 1,
                                   self.maxy - cowsay_height - 2, self.maxx - cowsay_width - 10)
            cowsay_win.addstr(cowsay, curses.color_pair(3))
            cowsay_win.refresh()
            self.__cowsay_win = cowsay_win
        except curses.error:
            pass




    def __update_lms(self):
        return oceanbase.check_lms(self.__cowsay)



    def __run_show_win_file(self):
        self.stdscr.erase()
        bzfh = bz2.BZ2File(Options.show_win_file)
        fh = tempfile.TemporaryFile()
        fh.write(bzfh.read())
        fh.seek(0)
        bzfh.close()
        self.stdscr = curses.getwin(fh)
        self.stdscr.nodelay(0)
        self.stdscr.notimeout(1)
        fh.close()
        ch = self.stdscr.getch()
        curses.endwin()
        exit(0)

    def __init__(self, win):
        DEBUG(self.__init__, "Dooba.__init__(win='%s') " % win, "")
        self.__all_page = []
        self.__page_index = 2

        self.__init_curses(win)
        minx = 80
        if self.maxx < minx or self.maxy < 20:
            MessageBox(self.stdscr,
                       "[ ERROR ] %dx%d is tooooo smalllll! %dx20 is at least..." % (self.maxx, self.maxy, minx)).run(anykey=True)
            return
        self.__select_cluster()

        oceanbase.set_current_tenant('1')
        oceanbase.update_tenant_info()
        oceanbase.start()
        self.__show_logo()


        self.stat_w = StatusWidget(self.stdscr)
        self.help_w = HeaderWidget(self.stdscr)

        self.__all_page.append(FalloutPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))        # 0
        self.__all_page.append(HelpPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))         # 1
        self.__all_page.append(GalleryPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))      # 2
        self.__all_page.append(SQLPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))          # 3
        self.__all_page.append(HistoryPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))      # 4
        self.__all_page.append(BianquePage(2, 0, self.maxy - 4, self.maxx, self.stdscr))      # 5
        self.__all_page.append(MachineStatPage(2, 0, self.maxy-4, self.maxx, self.stdscr))    # 6
        self.__all_page.append(BlankPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))        # 7
        self.__all_page.append(BlankPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))        # 8
        self.__all_page.append(BlankPage(2, 0, self.maxy - 4, self.maxx, self.stdscr))        # 9
        self.__run()


class DoobaMain(object):
    '''NAME
        dooba - A curses powerful tool for oceanbase admin, more than a monitor

SYNOPSIS
        dooba [OPTIONS]

Options

    ·   --host=HOST, -h HOST

        Connect to oceanbase on the given host.

    ·   --port=PORT, -P PORT

        The TCP/IP port to use for connecting to oceanbase server.

    ·   --user=USER, -u USER

        The user to use for connecting to oceanbase server.

    ·   --password=PASSWORD, -p PASSWORD

        The password to use for connecting to oceanbase server.
    '''

    def __usage(self):
        print('Usage: dooba [-h|--host=HOST] [-P|--port=PORT]')
        print('Usage: dooba [--dataid=DATAID]')
        print('Usage: dooba --show dooba_win_file')


    def __set_env(self):
        setlocale(LC_ALL, "en_US.UTF-8")
        environ['TERM'] = 'xterm'



    def __parse_options(self):
        try:
            opts, args = getopt(sys.argv[1:], '?dh:i:I:p:P:su:D',
                                ['debug', 'help', 'host=', 'interval=', 'port=',
                                 'password=', 'supermode', 'user=', 'dataid=',
                                 'online', 'offline', 'machine-interval=', 'degradation',
                                 'show=', 'daemon', 'http', 'start', 'stop', 'restart',
                                 'http-port=', 'database','datalog='])
        except GetoptError as err:
            print str(err) # will print something like "option -a not recognized"
            self.__usage()
            exit(2)
        for o, v in opts:
            if o in ('-?', '--help'):
                print self.__doc__
                exit(1)
            if o in ('-d', '--debug'):
                Options.debug = True
            elif o in ('-h', '--host'):
                Options.host = v
                Options.using_ip_port = True
            elif o in ('-P', '--port'):
                Options.port = int(v)
                Options.using_ip_port = True
            elif o in ('-u', '--user'):
                Options.user = v
            elif o in ('-p', '--password'):
                Options.password = v
            elif o in ('-s', '--supermode'):
                Options.supermode = True
            elif o in ('-i', '--interval'):
                Options.interval = float(v)
            elif o in ('-I', '--machine-interval'):
                Options.machine_interval = float(v)
            elif o in ('--dataid'):
                Options.dataid = v
            elif o in ('--online'):
                Options.env = 'online'
            elif o in ('--offline'):
                Options.env = 'offline'
            elif o in ('-D', '--degradation'):
                Options.degradation = True
            elif o in ('--show'):
                Options.show_win_file = v
            elif o in ('--http'):
                Options.http = True
            elif o in ('--start'):
                Options.daemon_action = 'start'
            elif o in ('--stop'):
                Options.daemon_action = 'stop'
            elif o in ('--restart'):
                Options.daemon_action = 'restart'
            elif o in ('--daemon'):
                Options.daemon = True
            elif o in ('--http-port'):
                Options.http_port = int(v)
            elif o in ('--database'):
                Options.database = v
            elif o in ('--datalog'):
                if not check_datalog(v) :
                    assert False, 'invalid datalog filename [%s]' % (v)

                Options.datalog = v
            else:
                assert False, 'unhandled option [%s]' % ( o )
        return args

    def __ignore_signal(self):
        def signal_handler(signal, frame):
            pass
        signal.signal(signal.SIGINT, signal_handler)

    def __myprint(self, info):
        print info
        print
        self.__usage()

    def __init__(self):
        global oceanbase
        self.__set_env()

        if "print-config" in self.__parse_options():
            print "".join(ObConfigHelper().get_config(Options.dataid))
            ObConfigHelper().get_dataid_list()
            exit(0)

        if not Options.show_win_file:
            oceanbase = oceanbase(Options.dataid)
            if Options.using_ip_port:
                oceanbase.test_alive(do_false=self.__myprint)

        if Options.http:
            pid_file = '/tmp/dooba.%d.pid' % Options.http_port
            if Options.daemon:
                if Options.daemon_action == 'start':
                    HTTPServerDaemon(pid_file).start()
                elif Options.daemon_action == 'stop':
                    HTTPServerDaemon(pid_file).stop()
                elif Options.daemon_action == 'restart':
                    HTTPServerDaemon(pid_file).restart()
            else:
                try:
                    HTTPServerDaemon(pid_file).run()
                except KeyboardInterrupt:
                    pass
        else:
            try:
                curses.wrapper(Dooba)
            except KeyboardInterrupt:
                pass


class HTTPServerDaemon(Daemon):
    def run(self):
        #oceanbase.update_cluster_info()
        oceanbase.update_tenant_info()
        oceanbase.start()
        server = BaseHTTPServer.HTTPServer(('', Options.http_port), WebRequestHandler)
        server.serve_forever()


class JSONDateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, (timedelta)):
            return 'NULL'
        else:
            return json.JSONEncoder.default(self, obj)


class WebRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        sample = oceanbase.now()
        self.wfile.write('HTTP/1.1 200 OK\nContent-Type: text/html\n\n')
        self.wfile.write(json.dumps(sample, cls=JSONDateTimeEncoder))


if __name__ == "__main__":
    DoobaMain()
#
# dooba ends here
