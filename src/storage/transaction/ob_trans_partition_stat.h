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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_PARTITION_STAT_
#define OCEANBASE_TRANSACTION_OB_TRANS_PARTITION_STAT_

#include "ob_trans_define.h"
#include "common/ob_range.h"

namespace oceanbase {
namespace transaction {
class ObTransPartitionStat {
public:
  ObTransPartitionStat()
  {
    reset();
  }
  virtual ~ObTransPartitionStat()
  {}
  void reset();
  int init(const common::ObAddr& addr, const common::ObPartitionKey& partition, const int64_t ctx_type,
      const bool is_master, const bool is_frozen, const bool is_stopped, const int64_t active_read_only_count,
      const int64_t active_read_write_count, const int64_t total_ctx_count, const int64_t mgr_addr,
      const uint64_t with_dependency_trx_count, const uint64_t without_dependency_trx_count,
      const uint64_t end_trans_by_prev_count, const uint64_t end_trans_by_checkpoint_count,
      const uint64_t end_trans_by_self_count);

  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int64_t get_ctx_type() const
  {
    return ctx_type_;
  }
  bool is_master() const
  {
    return is_master_;
  }
  bool is_frozen() const
  {
    return is_frozen_;
  }
  bool is_stopped() const
  {
    return is_stopped_;
  }
  int64_t get_read_only_count() const
  {
    return read_only_count_;
  }
  int64_t get_active_read_write_count() const
  {
    return active_read_write_count_;
  }
  const common::ObVersion& get_active_memstore_version() const
  {
    return active_memstore_version_;
  }
  int64_t get_total_ctx_count() const
  {
    return total_ctx_count_;
  }
  int64_t get_mgr_addr() const
  {
    return mgr_addr_;
  }
  uint64_t get_with_dependency_trx_count()
  {
    return with_dependency_trx_count_;
  }
  uint64_t get_without_dependency_trx_count()
  {
    return without_dependency_trx_count_;
  }
  uint64_t get_end_trans_by_prev_count()
  {
    return end_trans_by_prev_count_;
  }
  uint64_t get_end_trans_by_checkpoint_count()
  {
    return end_trans_by_checkpoint_count_;
  }
  uint64_t get_end_trans_by_self_count()
  {
    return end_trans_by_self_count_;
  }

  TO_STRING_KV(K_(addr), K_(partition), K_(ctx_type), K_(is_master), K_(is_frozen), K_(is_stopped), K_(read_only_count),
      K_(active_read_write_count), K_(active_memstore_version), K_(total_ctx_count), K_(mgr_addr),
      K_(with_dependency_trx_count), K_(without_dependency_trx_count), K_(end_trans_by_prev_count),
      K_(end_trans_by_checkpoint_count), K_(end_trans_by_self_count));

private:
  common::ObAddr addr_;
  common::ObPartitionKey partition_;
  int64_t ctx_type_;
  bool is_master_;
  bool is_frozen_;
  bool is_stopped_;
  int64_t read_only_count_;
  int64_t active_read_write_count_;
  common::ObVersion active_memstore_version_;
  int64_t total_ctx_count_;
  int64_t mgr_addr_;
  uint64_t with_dependency_trx_count_;
  uint64_t without_dependency_trx_count_;
  uint64_t end_trans_by_prev_count_;
  uint64_t end_trans_by_checkpoint_count_;
  uint64_t end_trans_by_self_count_;
};

}  // namespace transaction
}  // namespace oceanbase
#endif  // OCEANABAE_TRANSACTION_OB_TRANS_PARTITION_STAT_
