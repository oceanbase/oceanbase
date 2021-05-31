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

#include "ob_trans_partition_stat.h"
#include "ob_trans_ctx.h"

namespace oceanbase {
using namespace common;

namespace transaction {
void ObTransPartitionStat::reset()
{
  addr_.reset();
  partition_.reset();
  ctx_type_ = ObTransCtxType::UNKNOWN;
  is_master_ = false;
  is_frozen_ = false;
  is_stopped_ = false;
  read_only_count_ = 0;
  active_read_write_count_ = 0;
  active_memstore_version_.reset();
  total_ctx_count_ = 0;
  mgr_addr_ = 0;
}

// don't valid input arguments
int ObTransPartitionStat::init(const common::ObAddr& addr, const common::ObPartitionKey& partition,
    const int64_t ctx_type, const bool is_master, const bool is_frozen, const bool is_stopped,
    const int64_t read_only_count, const int64_t active_read_write_count, const int64_t total_ctx_count,
    const int64_t mgr_addr, const uint64_t with_dependency_trx_count, const uint64_t without_dependency_trx_count,
    const uint64_t end_trans_by_prev_count, const uint64_t end_trans_by_checkpoint_count,
    const uint64_t end_trans_by_self_count)
{
  int ret = OB_SUCCESS;

  addr_ = addr;
  partition_ = partition;
  ctx_type_ = ctx_type;
  is_master_ = is_master;
  is_frozen_ = is_frozen;
  is_stopped_ = is_stopped;
  read_only_count_ = read_only_count;
  active_read_write_count_ = active_read_write_count;
  active_memstore_version_ = ObVersion(2);
  total_ctx_count_ = total_ctx_count;
  mgr_addr_ = mgr_addr;
  with_dependency_trx_count_ = with_dependency_trx_count;
  without_dependency_trx_count_ = without_dependency_trx_count;
  end_trans_by_prev_count_ = end_trans_by_prev_count;
  end_trans_by_checkpoint_count_ = end_trans_by_checkpoint_count;
  end_trans_by_self_count_ = end_trans_by_self_count;

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
