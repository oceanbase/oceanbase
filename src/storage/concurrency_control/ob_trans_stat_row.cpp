/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/concurrency_control/ob_trans_stat_row.h"

namespace oceanbase
{
namespace concurrency_control
{

void build_trans_stat_datum(const storage::ObTableIterParam *param,
                            const blocksstable::ObDatumRow &row,
                            const ObTransStatRow &trans_stat_row)
{
  // trans stat datum index for vectorized execution
  TRANS_LOG(DEBUG, "memtable try to generate trans_info",
            K(trans_stat_row), K(param), K(param->op_), K(row.trans_info_),
            K(lbt()));
  char *trans_stat_ptr = row.trans_info_;
  if (param->need_trans_info()
      && OB_NOT_NULL(trans_stat_ptr)) {
    trans_stat_ptr[0] = '\0';
    concurrency_control::build_trans_stat_(trans_stat_row,
                                           ObTransStatRow::MAX_TRANS_STRING_SIZE,
                                           trans_stat_ptr);
    TRANS_LOG(DEBUG, "memtable generate trans_info",
        K(ObString(strlen(trans_stat_ptr), trans_stat_ptr)),
        K(trans_stat_row), K(param));
  }
}

void build_trans_stat_(const ObTransStatRow &trans_stat_row,
                       const int64_t trans_stat_len,
                       char *trans_stat_ptr)
{
  // Diagnostic text is allowed to truncate. databuff_printf preserves the
  // fitting prefix and NUL-terminates nonempty buffers even on size overflow.
  (void)databuff_printf(trans_stat_ptr,
                       trans_stat_len,
                       "[%ld, %ld, %ld, (%d,%ld), %ld, %p, %p]",
                       trans_stat_row.trans_version_.get_val_for_tx(),
                       trans_stat_row.scn_.get_val_for_tx(),
                       trans_stat_row.trans_id_.get_id(),
                       trans_stat_row.seq_no_.get_branch(),
                       trans_stat_row.seq_no_.get_seq(),
                       trans_stat_row.snapshot_.get_val_for_tx(),
                       trans_stat_row.mvcc_row_,
                       trans_stat_row.first_trans_node_);
}


} // namespace concurrency_control
} // namespace oceanbase
