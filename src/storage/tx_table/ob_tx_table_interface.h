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

#ifndef OCEANBASE_STORAGE_OB_TRANS_TABLE_INTERFACE_
#define OCEANBASE_STORAGE_OB_TRANS_TABLE_INTERFACE_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
class ObTxTable;
class ObTxTableGuard
{
public:
  ObTxTableGuard() : tx_table_(nullptr), epoch_(-1) {}
  ~ObTxTableGuard() { reset(); }

  ObTxTableGuard(const ObTxTableGuard &guard) : ObTxTableGuard() { *this = guard; }

  ObTxTableGuard &operator=(const ObTxTableGuard &guard)
  {
    reset();
    tx_table_ = guard.tx_table_;
    epoch_ = guard.epoch_;
    return *this;
  }

  void reset()
  {
    if (OB_NOT_NULL(tx_table_)) {
      tx_table_ = nullptr;
      epoch_ = -1;
    }
  }

  int init(ObTxTable *tx_table);
  bool is_valid() const { return nullptr != tx_table_ ? true : false; }

  ObTxTable *get_tx_table() const { return tx_table_; }
  int64_t epoch() const { return epoch_; }

  TO_STRING_KV(KP_(tx_table), K_(epoch));

private:
  ObTxTable *tx_table_;
  int64_t epoch_;
};


}  // namespace storage
}  // namespace oceanbase

#endif
