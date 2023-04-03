#!/usr/bin/python

import os
import sys
import time

test_list = (
             'memtable/',
             'replayengine/',
             'transaction/',
             './',
             '../clog/',
            )

quick_list = {
                  'memtable/' : (
#                                 'test_memtable_compact_writer',
#                                 'test_memtable_mutator',
                                 'test_rowkey_codec',
                                ),
                  'transaction/' : (
#                                    'test_ob_trans_ctx_mgr',
                                    'test_ob_mask_set',
                                    'test_ob_clog_adapter',
                                    'test_ob_location_adapter',
                                    'test_ob_trans_msg',
                                    'test_ob_trans_log',
                                    'test_ob_trans_define',
                                    'test_ob_trans_submit_log_cb',
                                    'test_ob_trans_rpc',
                                    'test_ob_trans_version_mgr',
                                   ),
                  './' : (
                          'test_row_fuse',
                          'test_single_row_merge',
                          'test_multiple_get_merge',
                          'test_multiple_scan_merge',
                          # 'test_base_storage',
                          'test_save_storage_info',
                          'test_range_iterator',
                         ),
                  '../clog/' : (
                                # 'test_fetch_log_engine',
                                'test_log_allocator',
                                'test_log_callback_engine',
                                'test_log_checksum_V2',
                                'test_log_common',
                                'test_log_define',
                                'test_log_ext_ring_buffer',
                                'test_log_index',
                                # 'test_log_reconfirm',
                                'test_log_replay_engine_wrapper',
                                'test_log_state_driver_runnable',
                                'test_log_task',
                                # 'test_new_log_state_mgr',
                                'test_ob_index_entry',
                                # 'test_ob_index_iterator',
                                'test_ob_log_block',
                                'test_ob_log_cache',
                                # 'test_ob_log_direct_reader',
                                'test_ob_log_entry',
                                'test_ob_log_entry_header',
                                # 'test_ob_log_file_pool',
                                'test_ob_log_file_trailer',
                                # 'test_ob_log_iterator',
                                # 'test_ob_raw_entry_iterator',
                                # 'test_spmg_fixed_cache',
                               ),
                 }


log_dir = os.path.dirname(os.getcwd())
log_file = "%s/run_all_test.log" % log_dir


def retrieve_ut_executables(test_list):
  '''aa'''
  exec_list = []
  for f in test_list:
    found = False
    for l in open(f + 'Makefile.am'):
      if found:
        s = l.strip()
        b_flag = False
        if s[-1] == '\\':
          s = s[0:-1]
        else:
          b_flag = True
        exec_list += [(f, name) for name in s.split()]
        if b_flag:
          break
      elif l.startswith('bin_PROGRAMS'):
        s = l.strip()
        s = s[s.index('=')+1:].strip()
        if s[-1] == '\\':
          s = s[0:-1]
          found = True
        exec_list += [(f, name) for name in s.split()]
  return exec_list

if __name__ == '__main__':
  exit_code = 0
  quick = False
  if len(sys.argv) == 2 and sys.argv[1] == 'quick':
    quick = True
  pos = 45
  for ut in retrieve_ut_executables(test_list):
    fn = ut[0] + ut[1]
    print fn,
    i = 0
    while i < pos - len(fn):
      i += 1
      print '',
    if quick and ((not quick_list.has_key(ut[0])) or (ut[1] not in quick_list[ut[0]])):
      et = 0
      print "\033[33mSKIPPED\033[0m [%fs]" % (et)
    else:
      olddir = os.getcwd()
      os.chdir(ut[0])
      st = time.time()
      err = os.system("./" + ut[1] + " >> %s 2>&1" % log_file)
      et = time.time() - st
      os.chdir(olddir)
      if err != 0:
        print "\033[31mFAILED \033[0m [%fs]" % (et)
        exit_code = 1
      else:
        print "\033[32mSUCCESS\033[0m [%fs]" % (et)
      time.sleep(0.05)
  sys.exit(exit_code)
