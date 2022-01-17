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

#ifndef OCEANBASE_LIBOBLOG_LOG_ENTRY_WRAPPER_H__
#define OCEANBASE_LIBOBLOG_LOG_ENTRY_WRAPPER_H__

#include "clog/ob_log_entry_header.h"             // ObLogEntryHeader
#include "clog/ob_log_entry.h"                    // ObLogEntry
#include "storage/ob_storage_log_type.h"          // ObStorageType

namespace oceanbase
{
namespace liboblog
{
// 1. For non-PG, ObLogEntry prevails
// 2. For PG, the log format is as follows
//               --------------------------------------------------------------------------------------------
//  ObLogEntry:  | ObLogEntryHeader | offset | submit_timestamp | log_type | trans_id_inc | trans data | ....
//               --------------------------------------------------------------------------------------------
// 1. 4Byte offset: records the offset of the next transaction log
// 2. 8Byte submit_timestamp: records the commit timestamp of this transaction log
// 3. 8Byte log_type: records the transaction log type
// 4. 8Byte trans_id_inc: records the ObTransId inc number, ensuring that the same transaction is committed to the same replay engine queue
//
// So, for PG aggregation logs:
// 1. submit_timestamp cannot use the commit timestamp of the ObLogEntryHeader, it needs to use the 8Byte-submit_timestamp of the corresponding transaction log
// 2. buf and buf_len should be parsed and calculated, not using the ObLogEntryHeader's information
// 3. Aggregate log parsing needs to be done continuously until the end of the buffer

struct ObLogAggreTransLog
{
  int32_t next_log_offset_;
  int64_t submit_timestamp_;
  storage::ObStorageLogType log_type_;
  int64_t trans_id_inc_;
  const char *buf_;
  int64_t buf_len_;

  ObLogAggreTransLog() { reset(); }

  void reset()
  {
    next_log_offset_ = 0;
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
    log_type_ = storage::OB_LOG_UNKNOWN;
    trans_id_inc_ = 0;
    buf_ = NULL;
    buf_len_ = 0;
  }

  void reset(const int64_t next_log_offset,
     const int64_t submit_timestamp,
     const storage::ObStorageLogType log_type,
     const int64_t trans_id_inc,
     const char *buf,
     const int64_t buf_len)
  {
    next_log_offset_ = next_log_offset;
    submit_timestamp_ = submit_timestamp;
    log_type_ = log_type;
    trans_id_inc_ = trans_id_inc;
    buf_ = buf;
    buf_len_ = buf_len;
  }

  TO_STRING_KV(K_(next_log_offset),
      K_(submit_timestamp),
      K_(log_type),
      K_(trans_id_inc),
      KP_(buf),
      K_(buf_len));
};

class ObLogEntryWrapper
{
public:
  ObLogEntryWrapper(const bool is_pg,
      const clog::ObLogEntry &log_entry,
      ObLogAggreTransLog &aggre_trans_log);
  ~ObLogEntryWrapper();

public:
  const clog::ObLogEntryHeader &get_header() const { return log_entry_.get_header(); }

  bool is_pg_aggre_log() const { return is_pg_; }

  // 1. Non-PG, returns the commit timestamp of the ObLogEntryHeader
  // 2. PG, returns the commit timestamp of the parsed log
  int64_t get_submit_timestamp() const;
  // 1. Non-PG, based on ObLogEntry
  // 2. PG, returns the corresponding buf and buf_len of the aggregated log
  const char *get_buf() const;
  int64_t get_buf_len() const;
  bool is_batch_committed() const;
  int32_t get_log_offset() const { return aggre_trans_log_.next_log_offset_; }

  TO_STRING_KV(K_(is_pg),
      K_(log_entry),
      K_(aggre_trans_log));

private:
  bool is_pg_;
  const clog::ObLogEntry &log_entry_;
  ObLogAggreTransLog &aggre_trans_log_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogEntryWrapper);
};

} // liboblog
} // oceanbase

#endif
