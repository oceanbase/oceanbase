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

#ifndef OCEANBASE_LS_PALF_STAT_INFO_H_
#define OCEANBASE_LS_PALF_STAT_INFO_H_

#include "lib/container/ob_se_array.h"              // ObSEArray
#include "share/ob_ls_id.h"                         // ObLSID
#include "share/ob_define.h"                        // MAX_IP_ADDR_LENGTH
#include "common/ob_role.h"                         // ObRole
#include "logservice/palf/lsn.h"                    // LSN

namespace oceanbase
{
namespace logservice
{
// Record in __all_virtual_log_stat table
struct LogStatRecord
{
  LogStatRecord() { reset(); }

  common::ObAddr server_;
  ObRole role_;
  palf::LSN begin_lsn_;
  palf::LSN end_lsn_;

  void reset()
  {
    server_.reset();
    role_ = common::INVALID_ROLE;
    begin_lsn_.reset();
    end_lsn_.reset();
  }

  void reset(const common::ObAddr &server,
      const ObRole &role,
      palf::LSN &begin_lsn,
      palf::LSN &end_lsn)
  {
    server_ = server;
    role_ = role;;
    begin_lsn_ = begin_lsn;
    end_lsn_ = end_lsn;
  }

  inline bool is_valid() const
  {
    return server_.is_valid() && begin_lsn_.is_valid() && end_lsn_.is_valid();
  }

  int64_t to_string(char *buffer, int64_t length) const;
};

class ObLSLogInfo
{
public:
  static const int64_t DEFAULT_RECORDS_NUM = 16;
  typedef common::ObSEArray<LogStatRecord, DEFAULT_RECORDS_NUM> LogStatRecordArray;
  ObLSLogInfo() { reset(); }
  virtual ~ObLSLogInfo() { reset(); }
  void reset();

  int init(const uint64_t tenant_id, const share::ObLSID &ls_id);
  inline bool is_valid() const { return ls_id_.is_valid_with_tenant(tenant_id_); }

  int add(const LogStatRecord &log_stat_record);
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline share::ObLSID get_ls_id() const { return ls_id_; }
  inline const LogStatRecordArray &get_log_stat_array() { return log_stat_records_; }
  inline int64_t get_log_stat_cnt() { return log_stat_records_.count(); }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(log_stat_records));

private:
  uint64_t tenant_id_;                      // tenant_id
  share::ObLSID ls_id_;                     // identifier for log stream
  LogStatRecordArray log_stat_records_;

  DISALLOW_COPY_AND_ASSIGN(ObLSLogInfo);
};

} // namespace logservice
} // namespace oceanbase

#endif
