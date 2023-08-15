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

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;

struct ObTableLoadTransCtx
{
public:
  ObTableLoadTransCtx(ObTableLoadTableCtx *ctx, const table::ObTableLoadTransId &trans_id);
  OB_INLINE table::ObTableLoadTransStatusType get_trans_status() const
  {
    obsys::ObRLockGuard guard(rwlock_);
    return trans_status_;
  }
  OB_INLINE void get_trans_status(table::ObTableLoadTransStatusType &trans_status,
                                  int &error_code) const
  {
    obsys::ObRLockGuard guard(rwlock_);
    trans_status = trans_status_;
    error_code = error_code_;
  }
  int advance_trans_status(table::ObTableLoadTransStatusType trans_status);
  int set_trans_status_error(int error_code);
  int set_trans_status_abort();
  int check_trans_status(table::ObTableLoadTransStatusType trans_status) const;
  TO_STRING_KV(K_(trans_id), K_(trans_status), K_(error_code));
public:
  ObTableLoadTableCtx * const ctx_;
  const table::ObTableLoadTransId trans_id_;
  mutable obsys::ObRWLock rwlock_;
  common::ObArenaAllocator allocator_;
  table::ObTableLoadTransStatusType trans_status_;
  int error_code_;
};

}  // namespace observer
}  // namespace oceanbase
