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
