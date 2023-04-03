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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
#define OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_simple_iterator.h"
#include "share/ob_ls_id.h"
#include "storage/memtable/ob_memtable_context.h"
#include "ob_trans_ctx.h"
#include "ob_trans_stat.h"

#include "storage/tx/ob_tx_ls_log_writer.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "ob_trans_hashmap.h"
#include "ob_trans_ctx_mgr_v4.h"

namespace oceanbase
{

namespace transaction
{

typedef common::ObSimpleIterator<ObDuplicatePartitionStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16> ObDuplicatePartitionStatIterator;

struct ObELRStatSummary {
  ObELRStatSummary() { reset(); }

  void reset()
  {
    with_dependency_trx_count_ = 0;
    without_dependency_trx_count_ = 0;
    end_trans_by_prev_count_ = 0;
    end_trans_by_checkpoint_count_ = 0;
    end_trans_by_self_count_ = 0;
  }

  TO_STRING_KV(K_(with_dependency_trx_count), K_(without_dependency_trx_count),
      K_(end_trans_by_prev_count), K_(end_trans_by_checkpoint_count), K_(end_trans_by_self_count));

  uint64_t with_dependency_trx_count_;
  uint64_t without_dependency_trx_count_;
  uint64_t end_trans_by_prev_count_;
  uint64_t end_trans_by_checkpoint_count_;
  uint64_t end_trans_by_self_count_;
};

}
}
#endif // OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
