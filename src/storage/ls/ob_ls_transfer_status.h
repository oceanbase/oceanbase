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

#ifndef OCEABASE_STORAGE_OB_LS_TRANSFER_STATUS
#define OCEABASE_STORAGE_OB_LS_TRANSFER_STATUS

#include "lib/lock/ob_spin_lock.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{

class ObLSTransferStatus
{
public:
  ObLSTransferStatus() { reset(); }
  ~ObLSTransferStatus() { reset(); }
  int init(ObLS *ls);
  void reset();
  int online();
  int offline();
  bool is_finished();
  void reset_prepare_op();
  void reset_move_tx_op();
  int update_status(const transaction::ObTransID tx_id,
                    const int64_t task_id,
                    const share::SCN op_scn,
                    const transaction::NotifyType op_type,
                    const transaction::ObTxDataSourceType mds_type);
  transaction::ObTransID get_tx_id() {  return transfer_tx_id_; }
  bool get_transfer_prepare_enable() { return transfer_prepare_op_; }
  int get_transfer_prepare_status(bool &enable, share::SCN &scn);
  TO_STRING_KV(K_(ls), K_(transfer_tx_id), K_(transfer_task_id),
      K_(transfer_prepare_op), K_(transfer_prepare_scn),
      K_(move_tx_op), K_(move_tx_scn));
private:
  int update_status_inner_(const transaction::ObTransID tx_id,
                           const int64_t task_id,
                           const share::SCN op_scn,
                           const transaction::NotifyType op_type,
                           const transaction::ObTxDataSourceType mds_type);
  int replay_status_inner_(const transaction::ObTransID tx_id,
                           const int64_t task_id,
                           const share::SCN op_scn,
                           const transaction::NotifyType op_type,
                           const transaction::ObTxDataSourceType mds_type);
  int enable_upper_trans_calculation_(const share::SCN op_scn);
  int disable_upper_trans_calculation_();
private:
  bool is_inited_;
  ObLS *ls_;
  common::ObSpinLock lock_;
  transaction::ObTransID transfer_tx_id_;
  int64_t transfer_task_id_;
  bool transfer_prepare_op_;
  share::SCN transfer_prepare_scn_;
  bool move_tx_op_;
  share::SCN move_tx_scn_;
};

} // end storage
} // end oceanbase

#endif
