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
 */

#ifndef  OCEANBASE_LIBOBLOG_LIBOBLOG_
#define  OCEANBASE_LIBOBLOG_LIBOBLOG_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <fnmatch.h> // FNM_CASEFOLD
#include <stdint.h>
#include <map>
#include <LogRecord.h>

using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace liboblog
{
struct ObLogError
{
  enum ErrLevel
  {
    ERR_WARN = 0,
    ERR_ABORT,
  } level_;             ///< error level
  int errno_;           ///< error number
  const char *errmsg_;  ///< error message
};

typedef void (* ERROR_CALLBACK) (const ObLogError &err);

class IObLog
{
public:
  virtual ~IObLog() {};
public:
  /*
   * init liboblog
   * @param config_file       config file name
   * @param start_timestamp   start timestamp (by second)
   * @param err_cb            error callback function pointer
   */
  virtual int init(const char *config_file,
      const uint64_t start_timestamp,
      ERROR_CALLBACK err_cb = NULL) = 0;

  /*
   * init liboblog
   * @param configs         config by map
   * @param start_timestamp start timestamp (by secon)
   * @param err_cb          error callback function pointer
   */
  virtual int init(const std::map<std::string, std::string>& configs,
      const uint64_t start_timestamp,
      ERROR_CALLBACK err_cb = NULL) = 0;

  /*
   * init liboblog
   * @param configs         config by map
   * @param start_timestamp start timestamp by microsecond
   * @param err_cb          error callback function pointer
   */
  virtual int init_with_start_tstamp_usec(const std::map<std::string, std::string>& configs,
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
  virtual int next_record(ILogRecord **record, const int64_t timeout_us) = 0;

  /*
   * fetch next binlog record from OB cluster
   * @param [out] record        binlog record, memory allocated by oblog, support release_record(corresponding tiems) after mutli next_record
   * @param [out] major_version major version of ILogRecord
   * @param [out] tenant_id     tenant id of ILogRecord
   *
   * @param OB_SUCCESS          success
   * @param OB_TIMEOUT          timeout
   * @param other error code    fail
   */
  virtual int next_record(ILogRecord **record,
      int32_t &major_version,
      uint64_t &tenant_id,
      const int64_t timeout_us) = 0;

  /*
   * release recorcd for EACH ILogRecord
   * @param record
   */
  virtual void release_record(ILogRecord *record) = 0;

  /*
   * Launch liboblog
   * @retval OB_SUCCESS on success
   * @retval ! OB_SUCCESS on fail
   */
  virtual int launch() = 0;

  /*
   * Stop liboblog
   */
  virtual void stop() = 0;

  /// Match the TableGroup being served
  /// Currently, TableGroup refers to a specific Database in the format "Tenant.Database".
  ///
  /// @param [in] pattern        target pattern string
  /// @param [out] is_matched    match result
  /// @param [in] fnmatch_flags  fnmatch flags
  ///
  /// @retval OB_SUCCESS         success
  /// @retval other value        fail
  virtual int table_group_match(const char *pattern, bool &is_matched, int fnmatch_flags = FNM_CASEFOLD) = 0;

  /// get all serving tenant TableGroup list
  ///
  /// @param [out]             table_groups tablegroup list
  ///
  /// @retval OB_SUCCESS       success
  /// @retval other value      fail
  virtual int get_table_groups(std::vector<std::string> &table_groups) = 0;

  /// get all serving tenant id list after oblog inited
  ///
  /// @param [out]            tenant_ids tenant ids that oblog serving
  ///
  /// @retval OB_SUCCESS      success
  /// @retval other value     fail
  virtual int get_tenant_ids(std::vector<uint64_t> &tenant_ids) = 0;
};

class ObLogFactory
{
public:
  ObLogFactory();
  ~ObLogFactory();
public:
  IObLog *construct_oblog();
  void deconstruct(IObLog *log);
};
}
}

#endif // OCEANBASE_LIBOBLOG_LIBOBLOG_
