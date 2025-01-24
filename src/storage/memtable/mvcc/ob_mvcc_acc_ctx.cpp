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

#include "ob_mvcc_acc_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
namespace oceanbase
{
using namespace transaction;
namespace memtable
{
int ObMvccAccessCtx::get_write_seq(ObTxSEQ &seq) const
{
  int ret = OB_SUCCESS;
  // for update uk or pk, set branch part to 0, in orer to let tx-callback fall into single list
  if (tx_scn_.support_branch() && write_flag_.is_update_uk()) {
    const int branch = tx_scn_.get_branch();
    if (branch == 0) {
      seq = tx_scn_;
    } else if (OB_UNLIKELY(ObTxDesc::is_alloced_branch_id(branch))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "external branch not support concurrent update uk / pk", K(ret), KPC(this));
    } else {
      seq = ObTxSEQ(tx_scn_.get_seq(), 0);
    }
  } else {
    seq = tx_scn_;
  }
  return ret;
}
} // memtable
} // oceanbase
