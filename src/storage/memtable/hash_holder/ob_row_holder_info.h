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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_INFO_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_INFO_H_

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "share/scn.h"
#include "storage/tx/ob_tx_seq.h" // ObTxSEQ
#include "storage/tx/ob_trans_define.h" // ObTransID

namespace oceanbase
{
namespace memtable
{

struct RowHolderInfo {// standard layout
  RowHolderInfo() : tx_id_(), seq_(), scn_() {}
  ~RowHolderInfo() { new (this) RowHolderInfo(); }
  RowHolderInfo(const transaction::ObTransID &tx_id,
                const transaction::ObTxSEQ &seq,
                const share::SCN &scn)
  : tx_id_(tx_id), seq_(seq), scn_(scn) {}
  RowHolderInfo(const RowHolderInfo &) = default;
  RowHolderInfo &operator=(const RowHolderInfo &) = default;
  bool is_valid() const { return tx_id_.is_valid(); }
  TO_STRING_KV(K_(tx_id), K_(seq), K_(scn));
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ seq_;
  share::SCN scn_;
};

}
}
#endif