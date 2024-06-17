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

#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{

namespace transaction
{

IMPL_ON_DEMAND_PRINT_FUNC(ObTxSubState)
{
  int ret = OB_SUCCESS;
  int tmp_pos = 0;

  // if (flag_.is_valid()) {
    ON_DEMAND_START_PRINT(SubState);

    TX_KV_PRINT_WITH_ERR(flag_.info_log_submitted_ > 0, info_log_submitted,
                         flag_.info_log_submitted_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.gts_waiting_ > 0, gts_waiting, flag_.gts_waiting_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.state_log_submitting_ > 0, state_log_submitting,
                         flag_.state_log_submitting_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.state_log_submitted_ > 0, state_log_submitted,
                         flag_.state_log_submitted_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.prepare_notify_ > 0, prepare_notify, flag_.prepare_notify_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.force_abort_ > 0, force_abort, flag_.force_abort_, " ");
    TX_KV_PRINT_WITH_ERR(flag_.transfer_blocking_ > 0, transfer_blocking, flag_.transfer_blocking_, " ");

    ON_DEMAND_END_PRINT(SubState);
  // }
  return ret;
}

IMPL_ON_DEMAND_PRINT_FUNC(ObTxExecInfo)
{
  int ret = OB_SUCCESS;
  int tmp_pos = 0;

  ON_DEMAND_START_PRINT(ExecInfo);

  TX_KV_PRINT_WITH_ERR(true, downstream_state, to_str_tx_state(state_), ", ");
  TX_KV_PRINT_WITH_ERR(true, upstream, upstream_, ", ");
  TX_KV_PRINT_WITH_ERR(true, participants,participants_, ", ");
  TX_KV_PRINT_WITH_ERR(true, redo_log_no, redo_lsns_.count(), ", ");
  TX_KV_PRINT_WITH_ERR(true, scheduler, scheduler_, ", ");
  TX_KV_PRINT_WITH_ERR(true, prepare_version, prepare_version_, ", ");
  TX_KV_PRINT_WITH_ERR(true, trans_type, trans_type_, ", ");
  TX_KV_PRINT_WITH_ERR(true, next_log_entry_no, next_log_entry_no_, ", ");
  TX_KV_PRINT_WITH_ERR(true, max_applied_log_ts, max_applied_log_ts_, ", ");
  TX_KV_PRINT_WITH_ERR(true, max_appling_log_ts, max_applying_log_ts_, ", ");
  TX_KV_PRINT_WITH_ERR(true, max_applying_part_log_no, max_applying_part_log_no_, ", ");
  TX_KV_PRINT_WITH_ERR(true, max_submitted_seq_no, max_submitted_seq_no_, ", ");
  TX_KV_PRINT_WITH_ERR(true, checksum, checksum_, ", ");
  TX_KV_PRINT_WITH_ERR(true, checksum_scn, checksum_scn_, ", ");
  TX_KV_PRINT_WITH_ERR(true, need_checksum, need_checksum_, ", ");
  TX_KV_PRINT_WITH_ERR(true, data_complete, data_complete_, ", ");
  TX_KV_PRINT_WITH_ERR(true, is_dup_tx, is_dup_tx_, ", ");
  TX_KV_PRINT_WITH_ERR(true, exec_epoch, exec_epoch_, ", ");


  TX_KV_PRINT_WITH_ERR(!incremental_participants_.empty(),incremental_participants, incremental_participants_, ", ");
  TX_KV_PRINT_WITH_ERR(!intermediate_participants_.empty(), intermediate_participants, intermediate_participants_, ", ");
  TX_KV_PRINT_WITH_ERR(prev_record_lsn_.is_valid(), prev_record_lsn, prev_record_lsn_, ", ");
  TX_KV_PRINT_WITH_ERR(!redo_lsns_.empty(), redo_lsns, redo_lsns_, ", ");
  TX_KV_PRINT_WITH_ERR(!multi_data_source_.empty(), multi_data_source, multi_data_source_, ", ");
  TX_KV_PRINT_WITH_ERR(max_durable_lsn_.is_valid(),max_durable_lsn , max_durable_lsn_, ", ");
  TX_KV_PRINT_WITH_ERR(!prepare_log_info_arr_.empty(),prepare_log_info_arr , prepare_log_info_arr_, ", ");
  TX_KV_PRINT_WITH_ERR(!xid_.empty(), xid , xid_, ", ");
  TX_KV_PRINT_WITH_ERR(is_sub2pc_, is_sub2pc , is_sub2pc_, ", ");
  TX_KV_PRINT_WITH_ERR(is_transfer_blocking_, is_transfer_blocking , is_transfer_blocking_, ", ");
  TX_KV_PRINT_WITH_ERR(!commit_parts_.empty(), commit_parts , commit_parts_, ", ");
  TX_KV_PRINT_WITH_ERR(!transfer_parts_.empty(), transfer_parts, transfer_parts_, ", ");
  TX_KV_PRINT_WITH_ERR(is_empty_ctx_created_by_transfer_, is_empty_ctx_created_by_transfer, is_empty_ctx_created_by_transfer_, ", ");
  TX_KV_PRINT_WITH_ERR(serial_final_scn_.is_valid(), serial_final_scn, serial_final_scn_, ", ");
  TX_KV_PRINT_WITH_ERR(serial_final_seq_no_.is_valid(), serial_final_seq_no,serial_final_seq_no_, ", ");

  TX_KV_PRINT_WITH_ERR(!dli_batch_set_.empty(), dli_batch_count, dli_batch_set_.size(), ", ");
  TX_KV_PRINT_WITH_ERR(!dli_batch_set_.empty(), dli_batch_set, dli_batch_set_, " ");

  ON_DEMAND_END_PRINT(ExecInfo);

  return ret;
}

IMPL_ON_DEMAND_PRINT_FUNC(ObPartTransCtx)
{
  int ret = OB_SUCCESS;
  int tmp_pos = 0;

  ON_DEMAND_START_PRINT(TxCtxExtra);

  TX_KV_PRINT_WITH_ERR(!busy_cbs_.is_empty(), busy_cbs_cnt, busy_cbs_.get_size(), ", ");
  TX_KV_PRINT_WITH_ERR(!busy_cbs_.is_empty(), oldest_busy_cb, busy_cbs_.get_first(), ", ");
  TX_KV_PRINT_WITH_ERR(final_log_cb_.is_valid() && !final_log_cb_.is_callbacked(), final_log_cb,
                       final_log_cb_, ", ");

  TX_PRINT_FUNC_WITH_ERR(sub_state_.is_valid(), sub_state_.on_demand_print_, ", ");


  TX_KV_PRINT_WITH_ERR(!coord_prepare_info_arr_.empty(), coord_prepare_info_arr_,
                       coord_prepare_info_arr_, ", ");

  TX_KV_PRINT_WITH_ERR(get_retain_cause() != RetainCause::UNKOWN, retain_cause, retain_cause_, ", ");

  TX_KV_PRINT_WITH_ERR(!state_info_array_.empty(), state_info_array, state_info_array_, ", ");

  // TX_KV_PRINT_WITH_ERR(OB_NOT_NULL(block_frozen_memtable_), block_frozen_memtable,
  //                      block_frozen_memtable_, ", ");
  //
  TX_PRINT_FUNC_WITH_ERR(true,
                       exec_info_.on_demand_print_, " ");
  ON_DEMAND_END_PRINT(TxCtxExtra);

  return ret;
}

} // namespace transaction

} // namespace oceanbase
