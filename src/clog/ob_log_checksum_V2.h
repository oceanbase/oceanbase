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

#ifndef OCEANBASE_CLOG_OB_LOG_CHECKSUM_V2_
#define OCEANBASE_CLOG_OB_LOG_CHECKSUM_V2_

#include "common/ob_partition_key.h"

namespace oceanbase {
namespace clog {
class ObILogChecksum {
public:
  ObILogChecksum()
  {}
  virtual ~ObILogChecksum()
  {}

public:
  // called by leader/follower when handle_index_log_()
  virtual int acquire_accum_checksum(const int64_t data_checksum, int64_t& accum_checksum) = 0;
  // called when slide out from sw
  virtual int verify_accum_checksum(const int64_t data_checksum, const int64_t accum_checksum) = 0;
  // called when set_next_index_log_id
  virtual void set_accum_checksum(const int64_t accum_checksum) = 0;
  virtual void set_accum_checksum(const uint64_t log_id, const int64_t accum_checksum) = 0;
  virtual void set_verify_checksum(const uint64_t log_id, const int64_t verify_checksum) = 0;
  virtual int64_t get_verify_checksum() const = 0;
};

class ObLogChecksum : public ObILogChecksum {
public:
  ObLogChecksum();
  virtual ~ObLogChecksum()
  {}

public:
  int init(uint64_t log_id, const int64_t accum_checksum, const common::ObPartitionKey& partition_key);
  virtual int acquire_accum_checksum(const int64_t data_checksum, int64_t& accum_checksum);
  virtual int verify_accum_checksum(const int64_t data_checksum, const int64_t accum_checksum);
  virtual void set_accum_checksum(const int64_t accum_checksum);
  virtual void set_accum_checksum(const uint64_t log_id, const int64_t accum_checksum);
  virtual void set_verify_checksum(const uint64_t log_id, const int64_t verify_checksum);
  virtual int64_t get_verify_checksum() const;

private:
  bool is_inited_;
  common::ObPartitionKey partition_key_;
  int64_t accum_checksum_;
  int64_t verify_checksum_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogChecksum);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CHECKSUM_V2_
