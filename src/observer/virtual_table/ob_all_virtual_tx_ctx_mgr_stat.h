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

#ifndef OB_ALL_VIRTUAL_TX_CTX_MGR_STAT
#define OB_ALL_VIRTUAL_TX_CTX_MGR_STAT

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_simple_iterator.h"
#include "storage/tx/ob_tx_ls_log_writer.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"

namespace oceanbase
{
namespace transaction
{
class ObTransService;
class ObLSTxCtxMgrStat;
}
namespace observer
{
class ObGVTxCtxMgrStat: public common::ObVirtualTableScannerIterator
{
public:
  explicit ObGVTxCtxMgrStat(transaction::ObTransService *trans_service)
      : trans_service_(trans_service) { reset(); }
  virtual ~ObGVTxCtxMgrStat() { destroy(); }
public:
  int inner_get_next_row(common::ObNewRow *&row);
  void reset();
  void destroy();
private:
  int prepare_start_to_read_();
private:
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char memstore_version_buffer_[common::MAX_VERSION_LENGTH];
private:
  transaction::ObTransService *trans_service_;
  transaction::ObTxCtxMgrStatIterator tx_ctx_mgr_stat_iter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTxCtxMgrStat);
};

}
}
#endif /* OB_ALL_VIRTUAL_TX_CTX_MGR_STAT */
