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

#ifndef OB_ALL_VIRTUAL_TX_LOCK_STAT_H
#define OB_ALL_VIRTUAL_TX_LOCK_STAT_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{

namespace transaction
{
class ObTransService;
class ObTxLockStat;
}

//  bool is_inited_;
//  common::ObAddr addr_;
//  uint64_t tenant_id_;
//  share::ObLSID ls_id_;
//  ObMemtableKeyInfo memtable_key_;
//  uint32_t session_id_;
//  uint64_t proxy_session_id_;
//  ObTransID tx_id_;
//  int64_t ctx_create_time_;
//  int64_t expired_time_;

namespace observer
{
class ObGVTxLockStat : public common::ObVirtualTableScannerIterator,
                       public omt::ObMultiTenantOperator
{
public:
  ObGVTxLockStat();
  ~ObGVTxLockStat();
public:
  int inner_get_next_row(common::ObNewRow *&row) override;
  void reset() override;
private:
  bool is_need_process(uint64_t tenant_id) override;
  int process_curr_tenant(common::ObNewRow *&row) override;
  void release_last_tenant() override;

  int prepare_start_to_read_();
  int get_next_tx_lock_stat_(transaction::ObTxLockStat &tx_lock_stat);
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  static const int64_t OB_MEMTABLE_KEY_BUFFER_SIZE = 128;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char tx_id_buffer_[OB_MIN_BUFFER_SIZE];
  char proxy_session_id_buffer_[OB_MIN_BUFFER_SIZE];
  char memtable_key_buffer_[OB_MEMTABLE_KEY_BUFFER_SIZE];
  int output_row_(const transaction::ObTxLockStat& tx_lock_stat, ObNewRow *&row);
private:
  share::ObLSID ls_id_;
  transaction::ObTransService *txs_;
  transaction::ObLSIDIterator ls_id_iter_;
  transaction::ObTxLockStatIterator tx_lock_stat_iter_;
};
}//observer
}//oceanbase

#endif /* OB_ALL_VIRTUAL_TX_LOCK_STAT_H */
