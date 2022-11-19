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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_util.h"
#include "ob_lob_manager.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{

using namespace common;
using namespace transaction;
namespace storage
{

int ObInsertLobColumnHelper::start_trans(const share::ObLSID &ls_id,
                                         const bool is_for_read,
                                         const int64_t timeout_ts,
                                         ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  tx_param.access_mode_ = is_for_read ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW; 
  tx_param.cluster_id_ = ObServerConfig::get_instance().cluster_id;
  tx_param.isolation_ = transaction::ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = std::max(0l, timeout_ts - ObTimeUtility::current_time());

  ObTransService *txs = MTL(ObTransService*);
  if (OB_FAIL(txs->acquire_tx(tx_desc))) {
    LOG_WARN("fail to acquire tx", K(ret));
  } else if (OB_FAIL(txs->start_tx(*tx_desc, tx_param))) {
    LOG_WARN("fail to start tx", K(ret));
  }
  return ret;
}

int ObInsertLobColumnHelper::end_trans(transaction::ObTxDesc *tx_desc,
                                       const bool is_rollback,
                                       const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  transaction::ObTxExecResult trans_result;
  ObTransService *txs = MTL(ObTransService*);

  if (OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx_desc is null", K(ret));
  } else {
    if (is_rollback) {
      if (OB_SUCCESS != (tmp_ret = txs->rollback_tx(*tx_desc))) {
        ret = tmp_ret;
        LOG_WARN("fail to rollback tx", K(ret), KPC(tx_desc));
      }
    } else if (OB_SUCCESS != (tmp_ret = txs->commit_tx(*tx_desc, timeout_ts))) {
      ret = tmp_ret;
      LOG_WARN("fail commit trans", K(ret), KPC(tx_desc), K(timeout_ts));
    }
    if (OB_SUCCESS != (tmp_ret = txs->release_tx(*tx_desc))) {
      ret = tmp_ret;
      LOG_WARN("release tx failed", K(ret), KPC(tx_desc));
    }
  }
  return ret;
}

int ObInsertLobColumnHelper::insert_lob_column(ObIAllocator &allocator,
                                               const share::ObLSID ls_id,
                                               const common::ObTabletID tablet_id,
                                               const ObColDesc &column,
                                               blocksstable::ObStorageDatum &datum,
                                               const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObTxDesc *tx_desc = nullptr;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObTransService *txs = MTL(transaction::ObTransService*);
  ObTxReadSnapshot snapshot;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager handle.", K(ret));
  } else if (!column.col_type_.is_lob_v2()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column));
  } else if (OB_FAIL(start_trans(ls_id, false/*is_for_read*/, timeout_ts, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret), K(column));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else {
    ObString data = datum.get_string();
    // init lob access param
    ObLobAccessParam lob_param;
    lob_param.tx_desc_ = tx_desc;
    lob_param.snapshot_ = snapshot;
    lob_param.sql_mode_ = SMO_DEFAULT;
    lob_param.ls_id_ = ls_id;
    lob_param.tablet_id_ = tablet_id;
    lob_param.coll_type_ = column.col_type_.get_collation_type();
    lob_param.allocator_ = &allocator;
    lob_param.lob_common_ = nullptr;
    lob_param.timeout_ = timeout_ts;
    lob_param.scan_backward_ = false;
    lob_param.offset_ = 0;
    lob_param.len_ = ObCharset::strlen_char(lob_param.coll_type_, data.ptr(), data.length());

    if (OB_FAIL(lob_mngr->append(lob_param, data))) {
      LOG_WARN("lob append failed.", K(ret));
    } else {
      datum.set_lob_data(*lob_param.lob_common_, lob_param.lob_common_->get_handle_size(lob_param.byte_size_));
      LOG_DEBUG("henry:write lob column", K(lob_param), KPC(lob_param.lob_common_),
          K(lob_param.lob_common_->get_handle_size(lob_param.byte_size_)),
          K(column.col_type_.get_collation_type()));
    }

    if (OB_SUCCESS != (tmp_ret = end_trans(tx_desc, OB_SUCCESS != ret, timeout_ts))) {
      ret = tmp_ret; 
      LOG_WARN("fail to end trans", K(ret), KPC(tx_desc), K(lob_param));
    }
  }
  return ret;
}


}
}
