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

#include "storage/concurrency_control/ob_trans_stat_row.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace concurrency_control
{

void build_trans_stat_datum(const storage::ObTableIterParam *param,
                            const blocksstable::ObDatumRow &row,
                            const ObTransStatRow trans_stat_row)
{
  const int64_t MAX_SIZE_FOR_TRANS_STAT_DATUM = 100;
  // trans stat datum index for vectorized execution
  TRANS_LOG(DEBUG, "memtable try to generate trans_info",
            K(trans_stat_row), K(param), K(param->op_), K(row.trans_info_),
            K(lbt()));
  char *trans_stat_ptr = row.trans_info_;
  if (OB_NOT_NULL(param->op_)
      && OB_NOT_NULL(trans_stat_ptr)) {
    trans_stat_ptr[0] = '\0';
    concurrency_control::build_trans_stat_(trans_stat_row,
                                           MAX_SIZE_FOR_TRANS_STAT_DATUM,
                                           trans_stat_ptr);
    TRANS_LOG(DEBUG, "memtable generate trans_info",
        K(ObString(strlen(trans_stat_ptr), trans_stat_ptr)),
        K(param->op_->is_vectorized()), K(trans_stat_row), K(param),
        K(param->op_->eval_ctx_));
  }
}

void build_trans_stat_(const ObTransStatRow trans_stat_row,
                       const int64_t trans_stat_len,
                       char *trans_stat_ptr)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(trans_stat_ptr,
                              trans_stat_len,
                              pos,
                              "[%ld, %ld, %ld, %ld]",
                              trans_stat_row.trans_version_.get_val_for_tx(),
                              trans_stat_row.scn_.get_val_for_tx(),
                              trans_stat_row.trans_id_.get_id(),
                              trans_stat_row.seq_no_.cast_to_int()))) {
    TRANS_LOG(WARN, "failed to printf", K(ret), K(pos), K(trans_stat_len), K(trans_stat_row));
    trans_stat_ptr[0] = '\0';
  } else {
    if (pos > trans_stat_len) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected length for datum", K(pos));
      trans_stat_ptr[0] = '\0';
    }
  }
}


} // namespace concurrency_control
} // namespace oceanbase
