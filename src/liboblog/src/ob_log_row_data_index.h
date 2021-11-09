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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_ROW_INDEX_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_ROW_INDEX_H_

#include "ob_log_binlog_record.h"

namespace oceanbase
{
namespace liboblog
{
// Row data index, in-memory index of one INSERT/UPDATE/DELETE statement
class ObLogRowDataIndex
{
public:
  ObLogRowDataIndex();
  virtual ~ObLogRowDataIndex();
  void reset();

public:
  int init(const uint64_t tenant_id,
      const char *participant_key,
      const uint64_t log_id,
      const int32_t log_offset,
      const uint64_t row_no,
      const bool is_rollback,
      const int32_t row_sql_no);
  bool is_valid() const;

  ObLogBR *get_binlog_record() { return br_; }
  const ObLogBR *get_binlog_record() const { return br_; }
  void set_binlog_record(ObLogBR *br) { br_ = br; }

  uint64_t get_tenant_id() const { return tenant_id_; }

  inline void set_host(void *host) { host_ = host; }
  inline void *get_host() { return host_; }

  int get_storage_key(std::string &key);
  uint64_t get_log_id() const { return log_id_; }
  int32_t get_log_offset() const {return log_offset_; }
  uint64_t get_row_no() const { return row_no_; }
  bool is_rollback() const { return is_rollback_; }
  int32_t get_row_sql_no() const { return row_sql_no_; }
  bool before(const ObLogRowDataIndex &row_index, const bool is_single_row);
  bool equal(const ObLogRowDataIndex &row_index, const bool is_single_row);

  int64_t get_br_commit_seq() const { return br_commit_seq_; }
  void *get_trans_ctx_host() { return trans_ctx_host_; }
  void set_br_commit_seq(const int64_t br_commit_seq, void *trans_ctx_host)
  {
    br_commit_seq_ = br_commit_seq;
    trans_ctx_host_ = trans_ctx_host;
  }

  void set_next(ObLogRowDataIndex *next) {next_ = next;};
  ObLogRowDataIndex *get_next() {return next_;};

  // The data needs to be freed from memory after it has been persisted
  // 1. free the Unserilized ILogRecord
  // 2. Free the serialised value
  int free_br_data();

  int construct_serilized_br_data(ObLogBR *&br);

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  bool log_before_(const ObLogRowDataIndex &row_index) { return log_id_ < row_index.log_id_; }
  bool log_equal_(const ObLogRowDataIndex &row_index) { return log_id_ == row_index.log_id_; }

private:
  ObLogBR       *br_;
  void          *host_;               // PartTransTask

  uint64_t      tenant_id_;

  // StorageKey: participant_key+log_id+log_offset+row_no
  const char    *participant_key_str_;
  uint64_t      log_id_;
  int32_t       log_offset_;
  // DML, DDL statement record row_no (numbered from 0 within the subdivision, not global)
  // 0 for other types of records
  uint64_t      row_no_;

  bool          is_rollback_;
  int32_t       row_sql_no_;

  int64_t       br_commit_seq_;          // Streaming commit model - seq
  void          *trans_ctx_host_;

  ObLogRowDataIndex *next_;
};

}
}

#endif
