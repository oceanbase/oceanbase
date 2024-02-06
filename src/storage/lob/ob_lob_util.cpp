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

int ObLobAccessParam::set_lob_locator(common::ObLobLocatorV2 *lob_locator)
{
  int ret = OB_SUCCESS;
  ObString disk_locator;
  if (OB_ISNULL(lob_locator)) {
    // do nothing
  } else if (!lob_locator->has_lob_header()) {
    // do nothing
  } else if (!lob_locator->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob locator is invalid", K(ret), KPC(lob_locator));
  } else if (!(lob_locator->is_lob_disk_locator() || lob_locator->is_persist_lob() || lob_locator->is_full_temp_lob())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob locator type is invalid", K(ret), KPC(lob_locator));
  } else if (OB_FAIL(lob_locator->get_disk_locator(disk_locator))) {
    LOG_WARN("failed to get lob common from lob locator", K(ret), KPC(lob_locator));
  } else {
    lob_common_ = reinterpret_cast<ObLobCommon*>(disk_locator.ptr());
    handle_size_ = disk_locator.length();
    lob_locator_ = lob_locator;
  }
  return ret;
}

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
    } else {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
      if (OB_SUCCESS != (tmp_ret = txs->commit_tx(*tx_desc, timeout_ts))) {
        ret = tmp_ret;
        LOG_WARN("fail commit trans", K(ret), KPC(tx_desc), K(timeout_ts));
      }
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
                                               const int64_t timeout_ts,
                                               const bool has_lob_header,
                                               const uint64_t src_tenant_id)
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
  } else if (!column.col_type_.is_lob_storage()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column));
  } else {
    ObString data = datum.get_string();
    // datum with null ptr and zero len should treat as no lob header
    bool set_has_lob_header = has_lob_header && data.length() > 0;
    ObLobLocatorV2 src(data, set_has_lob_header);
    if (src.has_inrow_data() && lob_mngr->can_write_inrow(data.length())) {
      // fast path for inrow data
      if (OB_FAIL(src.get_inrow_data(data))) {
        LOG_WARN("fail to get inrow data", K(ret), K(src));
      } else {
        void *buf = allocator.alloc(data.length() + sizeof(ObLobCommon));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buffer failed", K(ret), K(data.length()));
        } else {
          ObLobCommon *lob_comm = new(buf)ObLobCommon();
          MEMCPY(lob_comm->buffer_, data.ptr(), data.length());
          datum.set_lob_data(*lob_comm, data.length() + sizeof(ObLobCommon));
        }
      }
    } else {
      if (OB_FAIL(start_trans(ls_id, false/*is_for_read*/, timeout_ts, tx_desc))) {
        LOG_WARN("fail to get tx_desc", K(ret), K(column));
      } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, snapshot))) {
        LOG_WARN("fail to get snapshot", K(ret));
      } else {
        // 4.0 text tc compatiable
        ObLobAccessParam lob_param;
        lob_param.src_tenant_id_ = src_tenant_id;
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
        if (!src.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid src lob locator.", K(ret));
        } else if (OB_FAIL(lob_mngr->append(lob_param, src))) {
          LOG_WARN("lob append failed.", K(ret));
        } else {
          datum.set_lob_data(*lob_param.lob_common_, lob_param.handle_size_);
        }
      }
      if (OB_SUCCESS != (tmp_ret = end_trans(tx_desc, OB_SUCCESS != ret, timeout_ts))) {
        ret = tmp_ret;
        LOG_WARN("fail to end trans", K(ret), KPC(tx_desc));
      }
    }
  }
  return ret;
}

int ObInsertLobColumnHelper::insert_lob_column(ObIAllocator &allocator,
                                               const share::ObLSID ls_id,
                                               const common::ObTabletID tablet_id,
                                               const ObColDesc &column,
                                               ObObj &obj,
                                               const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datum;
  datum.from_obj(obj);
  if (OB_SUCC(insert_lob_column(allocator, ls_id, tablet_id, column, datum, timeout_ts, obj.has_lob_header(), MTL_ID()))) {
    obj.set_lob_value(obj.get_type(), datum.get_string().ptr(), datum.get_string().length());
  }
  return ret;
}

}
}
