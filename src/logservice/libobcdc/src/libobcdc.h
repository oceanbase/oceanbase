/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * OBCDC header file
 * This file defines interface of OBCDC
 */

#ifndef  OCEANBASE_LIBOBCDC_LIBOBCDC_
#define  OCEANBASE_LIBOBCDC_LIBOBCDC_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <fnmatch.h> // FNM_CASEFOLD
#include <stdint.h>
#include <map>
#ifndef OB_USE_DRCMSG
#include "oblogmsg/LogRecord.h"
typedef oceanbase::logmessage::ILogRecord ICDCRecord;
#else
#include <drcmsg/BR.h>
typedef IBinlogRecord ICDCRecord;
#endif

namespace oceanbase
{
namespace libobcdc
{
struct ObCDCError
{
  enum ErrLevel
  {
    ERR_WARN = 0,
    ERR_ABORT,
  } level_;             ///< error level
  int errno_;           ///< error number
  const char *errmsg_;  ///< error message
};

struct CDCTaskStat
{
  CDCTaskStat();
  ~CDCTaskStat();
  void reset();
  int64_t fetcher_part_trans_count_;
  int64_t ddl_in_process_part_trans_count_;
  int64_t seq_queue_part_trans_count_;
  int64_t seq_ready_trans_count_;
  int64_t seqed_trans_count_;
  int64_t storage_task_count_;
  int64_t reader_task_count_;
  int64_t dml_parser_redo_count_;
  int64_t formatter_br_count_;
  int64_t formatter_redo_count_;
  int64_t formatter_lob_stmt_count_;
  int64_t lob_merger_task_count_;
  int64_t sorter_task_count_;
  int64_t committer_dml_task_count_;
  int64_t committer_ddl_task_count_;
  int64_t br_queue_dml_count_;
  int64_t br_queue_ddl_count_;
  int64_t rc_part_trans_count_;
  int64_t rc_br_count_;
  int64_t out_ddl_br_count_;
  int64_t out_dml_br_count_;
};

typedef void (* ERROR_CALLBACK) (const ObCDCError &err);

class IObCDCInstance
{
public:
  virtual ~IObCDCInstance() {};
public:
  /*
   * init libobcdc
   * @param config_file       config file name
   * @param start_timestamp   start timestamp (by second)
   * @param err_cb            error callback function pointer
   */
  virtual int init(const char *config_file,
      const uint64_t start_timestamp_sec,
      ERROR_CALLBACK err_cb = NULL) = 0;

  /*
   * init libobcdc
   * @param configs         config by map
   * @param start_timestamp start timestamp (by second)
   * @param err_cb          error callback function pointer
   */
  virtual int init(const std::map<std::string, std::string> &configs,
      const uint64_t start_timestamp_sec,
      ERROR_CALLBACK err_cb = NULL) = 0;

  /*
   * init libobcdc
   * @param configs         config by map
   * @param start_timestamp start timestamp by microsecond
   * @param err_cb          error callback function pointer
   */
  virtual int init_with_start_tstamp_usec(const std::map<std::string, std::string> &configs,
      const uint64_t start_timestamp_usec,
      ERROR_CALLBACK err_cb = NULL) = 0;

  virtual void destroy() = 0;

  /*
   * fetch next binlog record from OB cluster
   * @param record           binlog record, memory allocated by oblog, support release_record(corresponding times) after mutli next_record
   * @param OB_SUCCESS       success
   * @param OB_TIMEOUT       timeout
   * @param other errorcode  fail
   */
  virtual int next_record(ICDCRecord **record, const int64_t timeout_us) = 0;

  /*
   * fetch next binlog record from OB cluster
   * @param [out] record        binlog record, memory allocated by oblog, support release_record(corresponding tiems) after mutli next_record
   * @param [out] major_version major version of ICDCRecord
   * @param [out] tenant_id     tenant id of ICDCRecord
   *
   * @param OB_SUCCESS          success
   * @param OB_TIMEOUT          timeout
   * @param other error code    fail
   */
  virtual int next_record(ICDCRecord **record,
      int32_t &major_version,
      uint64_t &tenant_id,
      const int64_t timeout_us) = 0;

  /*
   * release recorcd for EACH ICDCRecord
   * @param record
   */
  virtual void release_record(ICDCRecord *record) = 0;

  /*
   * Launch libobcdc
   * @retval OB_SUCCESS on success
   * @retval ! OB_SUCCESS on fail
   */
  virtual int launch() = 0;

  /*
   * Stop libobcdc
   */
  virtual void stop() = 0;

  /// get all serving tenant id list after oblog inited
  ///
  /// @param [out]            tenant_ids tenant ids that oblog serving
  ///
  /// @retval OB_SUCCESS      success
  /// @retval other value     fail
  virtual int get_tenant_ids(std::vector<uint64_t> &tenant_ids) = 0;

  /*
  * get obcdc memory usage stat
  * @param [out] expected_usage       expected memory usage(aka memory_limit in config, cdc may use memory more than this value)
  * @param [out] hard_mem_limit       hard mem limit(cdc will use memory at most with this value, if enable_hard_mem_limit is 0, hard_mem_limit won't be effective)
  * @param [out] memory_hold          memory hold by obcdc (memory hold by cdc)
  * @param [out] memory_used          memory used by obcdc (memory used by cdc)
  * @param [out] redo_dispatch_limit  limit for redo can be dispatch and handled
  * @param [out] redo_dispatched      redo size in handling(increase while redo read from local_storage, decrease while redo is recycled(after user release_record))
  *
  * @note
  * memory_hold - mmeory_used is memory cached by cdc(not in use)
  */
  virtual void get_mem_stat(
    int64_t &expected_usage,
    int64_t &hard_mem_limit,
    int64_t &memory_hold,
    int64_t &memory_used,
    int64_t &redo_dispatch_limit,
    int64_t &redo_dispatched) const = 0;

  /*
  * get task queue stat in obcdc
  */
  virtual void get_task_stat(CDCTaskStat &task_stat) const = 0;
};

class ObCDCFactory
{
public:
  ObCDCFactory();
  ~ObCDCFactory();
public:
  IObCDCInstance *construct_obcdc();
  void deconstruct(IObCDCInstance *log);
};
}
}

#endif // OCEANBASE_LIBOBCDC_LIBOBCDC_
