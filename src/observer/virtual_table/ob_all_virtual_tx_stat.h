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

#ifndef OB_ALL_VIRTUAL_TX_STAT_H_
#define OB_ALL_VIRTUAL_TX_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_simple_iterator.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_tx_ls_log_writer.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "common/ob_clock_generator.h"
#include "storage/tx/ob_tx_stat.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
}
namespace transaction
{
class ObTransService;
class ObTransID;
class ObStartTransParam;
class ObTxStat;
}
namespace observer
{
class ObGVTxStat: public common::ObVirtualTableScannerIterator
{
public:
  explicit ObGVTxStat() { reset(); }
  virtual ~ObGVTxStat() { destroy(); }
public:
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  virtual void destroy();
private:
  int prepare_start_to_read_();
  int fill_tenant_ids_();
  int get_next_tx_info_(transaction::ObTxStat &tx_stat);
  bool is_valid_timestamp_(const int64_t timestamp) const;
private:
  enum
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    TX_TYPE,
    TX_ID,
    SESSION_ID,
    SCHEDULER_ADDR,
    IS_DECIDED,
    LS_ID,
    PARTICIPANTS,
    TX_CTX_CREATE_TIME,
    TX_EXPIRED_TIME,
    REF_CNT,
    LAST_OP_SN,
    PENDING_WRITE,
    STATE,
    PART_TX_ACTION,
    TX_CTX_ADDR,
    MEM_CTX_ID,
    PENDING_LOG_SIZE,
    FLUSHED_LOG_SIZE,
    ROLE_STATE,
    IS_EXITING,
    COORD,
    LAST_REQUEST_TS,
    GTRID,
    BQUAL,
    FORMAT_ID,
    START_SCN,
    END_SCN,
    REC_SCN,
    TRANSFER_BLOCKING,
    BUSY_CBS_CNT,
    REPLAY_COMPLETE,
    SERIAL_LOG_FINAL_SCN,
    CALLBACK_LIST_STATS,
  };

  static const int64_t OB_MAX_BUFFER_SIZE = 1024;
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  static const int64_t CTX_ADDR_BUFFER_SIZE = 20;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char participants_buffer_[OB_MAX_BUFFER_SIZE];
  char scheduler_buffer_[common::MAX_IP_PORT_LENGTH + 8];
  char ctx_addr_buffer_[20];
private:
  bool init_;
  transaction::ObTxStatIterator tx_stat_iter_;
  common::ObArray<uint64_t> all_tenants_;
  transaction::ObXATransID xid_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTxStat);
};

}
}
#endif /* OB_ALL_VIRTUAL_TRANS_STAT_H */
