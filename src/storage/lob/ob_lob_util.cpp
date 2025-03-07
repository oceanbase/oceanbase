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
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

using namespace common;
using namespace transaction;
namespace storage
{

ObCollationType ObLobCharsetUtil::get_collation_type(ObObjType type, ObCollationType ori_coll_type)
{
  ObCollationType coll_type = ori_coll_type;
  if (ob_is_json(type)) {
    coll_type = CS_TYPE_BINARY;
  }
  return coll_type;
}

void ObLobCharsetUtil::transform_query_result_charset(
    const common::ObCollationType& coll_type,
    const char* data,
    uint32_t len,
    uint32_t &byte_len,
    uint32_t &byte_st)
{
  byte_st = ObCharset::charpos(coll_type, data, len, byte_st);
  byte_len = ObCharset::charpos(coll_type, data + byte_st, len - byte_st, byte_len);
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
                                               const ObObjType &obj_type,
                                               const ObCollationType &cs_type,
                                               const ObLobStorageParam &lob_storage_param,
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
  } else {
    ObString data = datum.get_string();
    // datum with null ptr and zero len should treat as no lob header
    bool set_has_lob_header = has_lob_header && data.length() > 0;
    ObLobLocatorV2 src(data, set_has_lob_header);
    int64_t byte_len = 0;
    if (OB_FAIL(src.get_lob_data_byte_len(byte_len))) {
      LOG_WARN("fail to get lob data byte len", K(ret), K(src));
    } else if (src.has_inrow_data() && lob_mngr->can_write_inrow(byte_len, lob_storage_param.inrow_threshold_)) {
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
        LOG_WARN("fail to get tx_desc", K(ret));
      } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, snapshot))) {
        LOG_WARN("fail to get snapshot", K(ret));
      } else {
        // 4.0 text tc compatiable
        ObLobAccessParam lob_param;
        lob_param.src_tenant_id_ = src_tenant_id;
        lob_param.tx_desc_ = tx_desc;
        if (OB_FAIL(lob_param.snapshot_.assign(snapshot))) {
          LOG_WARN("assign snapshot fail", K(ret));
        } else {
          lob_param.sql_mode_ = SMO_DEFAULT;
          lob_param.ls_id_ = ls_id;
          lob_param.tablet_id_ = tablet_id;
          lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(obj_type, cs_type);
          lob_param.allocator_ = &allocator;
          lob_param.lob_common_ = nullptr;
          lob_param.timeout_ = timeout_ts;
          lob_param.scan_backward_ = false;
          lob_param.offset_ = 0;
          lob_param.inrow_threshold_ = lob_storage_param.inrow_threshold_;
          lob_param.is_index_table_ = lob_storage_param.is_index_table_;
          lob_param.main_table_rowkey_col_ = !lob_storage_param.is_index_table_ && lob_storage_param.is_rowkey_col_;
          LOG_DEBUG("lob storage param", K(lob_storage_param), K(cs_type));
        }
        if (OB_FAIL(ret)) {
        } else if (!src.is_valid()) {
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
                                               const ObObjType &obj_type,
                                               const ObCollationType &cs_type,
                                               const ObLobStorageParam &lob_storage_param,
                                               ObObj &obj,
                                               const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datum;
  datum.from_obj(obj);
  if (OB_SUCC(insert_lob_column(allocator, ls_id, tablet_id, obj_type, cs_type, lob_storage_param, datum, timeout_ts, obj.has_lob_header(), MTL_ID()))) {
    obj.set_lob_value(obj.get_type(), datum.get_string().ptr(), datum.get_string().length());
  }
  return ret;
}

int ObInsertLobColumnHelper::insert_lob_column(ObIAllocator &allocator,
                                               ObIAllocator &lob_allocator,
                                               transaction::ObTxDesc *tx_desc,
                                               share::ObTabletCacheInterval &lob_id_geneator,
                                               const share::ObLSID ls_id,
                                               const common::ObTabletID tablet_id,
                                               const common::ObTabletID lob_meta_tablet_id,
                                               const ObObjType &obj_type,
                                               const ObCollationType collation_type,
                                               const ObLobStorageParam &lob_storage_param,
                                               blocksstable::ObStorageDatum &datum,
                                               const int64_t timeout_ts,
                                               const bool has_lob_header,
                                               const uint64_t src_tenant_id,
                                               ObLobMetaWriteIter &iter)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager handle.", K(ret));
  } else {
    ObString data = datum.get_string();
    // datum with null ptr and zero len should treat as no lob header
    bool set_has_lob_header = has_lob_header && data.length() > 0;
    ObLobLocatorV2 src(data, set_has_lob_header);
    int64_t byte_len = 0;
    if (OB_FAIL(src.get_lob_data_byte_len(byte_len))) {
      LOG_WARN("fail to get lob data byte len", K(ret), K(src));
    } else if (src.has_inrow_data() && lob_mngr->can_write_inrow(byte_len, lob_storage_param.inrow_threshold_)) {
      // do fast inrow
      if (src.is_inrow_disk_lob_locator()) {
        // if is disk inrow lob, no need alloc new memory, just reset lob common header
        char *buf = src.ptr_;
        ObLobCommon *lob_comm = new (buf) ObLobCommon();
        if (lob_comm->buffer_ != buf + sizeof(ObLobCommon)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob common buffer ptr is invalid", K(ret), KPC(lob_comm));
        } else {
          datum.set_lob_data(*lob_comm, src.size_);
          iter.set_end();
        }
      } else if (OB_FAIL(src.get_inrow_data(data))) {
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
          iter.set_end();
        }
      }
    } else {
      ObTransService *txs = MTL(transaction::ObTransService*);
      ObLobAccessParam lob_param;
      lob_param.tx_desc_ = tx_desc;
      // lob_param.snapshot_ = snapshot;
      lob_param.sql_mode_ = SMO_DEFAULT;
      lob_param.ls_id_ = ls_id;
      lob_param.tablet_id_ = tablet_id;
      lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(obj_type, collation_type);
      lob_param.allocator_ = &allocator;
      lob_param.lob_common_ = nullptr;
      lob_param.timeout_ = timeout_ts;
      lob_param.scan_backward_ = false;
      lob_param.offset_ = 0;
      lob_param.lob_meta_tablet_id_ = lob_meta_tablet_id;
      lob_param.lob_id_geneator_ = &lob_id_geneator;
      lob_param.inrow_threshold_ = lob_storage_param.inrow_threshold_;
      lob_param.is_index_table_ = lob_storage_param.is_index_table_;
      lob_param.main_table_rowkey_col_ = !lob_storage_param.is_index_table_ && lob_storage_param.is_rowkey_col_;
      lob_param.src_tenant_id_ = src_tenant_id;
      lob_param.set_tmp_allocator(&lob_allocator);
      if (!src.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid src lob locator.", K(ret));
      } else if (OB_FAIL(lob_mngr->append(lob_param, src, iter))) {
        LOG_WARN("lob append failed.", K(ret));
      } else {
        datum.set_lob_data(*lob_param.lob_common_, lob_param.handle_size_);
      }
    }
  }
  return ret;
}

int ObInsertLobColumnHelper::delete_lob_column(ObIAllocator &allocator,
                                              const share::ObLSID ls_id,
                                              const common::ObTabletID tablet_id,
                                              const ObCollationType& collation_type,
                                              blocksstable::ObStorageDatum &datum,
                                              const int64_t timeout_ts,
                                              const bool has_lob_header)
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
  } else {
    ObString data = datum.get_string();
    // datum with null ptr and zero len should treat as no lob header
    bool set_has_lob_header = has_lob_header && data.length() > 0;
    ObLobLocatorV2 lob(data, set_has_lob_header);
    int64_t byte_len = 0;
    if (lob.has_inrow_data()) {
      // delete inrow lob no need to use the lob manager
    } else {
      if (OB_FAIL(start_trans(ls_id, false/*is_for_read*/, timeout_ts, tx_desc))) {
        LOG_WARN("fail to get tx_desc", K(ret));
      } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, snapshot))) {
        LOG_WARN("fail to get snapshot", K(ret));
      } else {
        // 4.0 text tc compatiable
        ObLobAccessParam lob_param;
        lob_param.tx_desc_ = tx_desc;
        lob_param.tablet_id_ = tablet_id;
        if (!lob.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid src lob locator.", K(ret));
        } else if (OB_FAIL(lob_mngr->build_lob_param(lob_param, allocator, collation_type, 0, UINT64_MAX, timeout_ts, lob))) {
          LOG_WARN("fail to build lob param.", K(ret));
        } else if (OB_FAIL(lob_mngr->erase(lob_param))) {
          LOG_WARN("lob meta row delete failed.", K(ret));
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

OB_DEF_SERIALIZE_SIZE(ObLobChunkIndex)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(seq_id_);
  OB_UNIS_ADD_LEN(offset_);
  OB_UNIS_ADD_LEN(pos_);
  OB_UNIS_ADD_LEN(byte_len_);
  OB_UNIS_ADD_LEN(flag_);
  OB_UNIS_ADD_LEN(data_idx_);
  OB_UNIS_ADD_LEN(old_data_idx_);
  return len;
}

OB_DEF_SERIALIZE(ObLobChunkIndex)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(seq_id_);
  OB_UNIS_ENCODE(offset_);
  OB_UNIS_ENCODE(pos_);
  OB_UNIS_ENCODE(byte_len_);
  OB_UNIS_ENCODE(flag_);
  OB_UNIS_ENCODE(data_idx_);
  OB_UNIS_ENCODE(old_data_idx_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLobChunkIndex)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(seq_id_);
  OB_UNIS_DECODE(offset_);
  OB_UNIS_DECODE(pos_);
  OB_UNIS_DECODE(byte_len_);
  OB_UNIS_DECODE(flag_);
  OB_UNIS_DECODE(data_idx_);
  OB_UNIS_DECODE(old_data_idx_);
  return ret;
}

class ObLobChunkIndexComparator
{
public:
  bool operator()(const ObLobChunkIndex &a, const ObLobChunkIndex &b) const
  {
    return a.offset_ < b.offset_;
  }
};

OB_DEF_SERIALIZE_SIZE(ObLobChunkData)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(data_);
  return len;
}

OB_DEF_SERIALIZE(ObLobChunkData)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(data_)
  return ret;
}

OB_DEF_DESERIALIZE(ObLobChunkData)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(data_)
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLobPartialData)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(chunk_size_);
  OB_UNIS_ADD_LEN(data_length_);
  OB_UNIS_ADD_LEN(locator_);
  OB_UNIS_ADD_LEN(index_.count());
  for (int i = 0; i < index_.count(); ++i) {
    len += index_[i].get_serialize_size();
  }
  OB_UNIS_ADD_LEN(data_.count());
  for (int i = 0; i < data_.count(); ++i) {
    len += data_[i].get_serialize_size();
  }
  OB_UNIS_ADD_LEN(old_data_.count());
  for (int i = 0; i < old_data_.count(); ++i) {
    len += old_data_[i].get_serialize_size();
  }
  return len;
}

OB_DEF_SERIALIZE(ObLobPartialData)
{
  int ret = OB_SUCCESS;
  int32_t index_count = index_.count();
  int32_t data_count = data_.count();
  int32_t old_data_count = old_data_.count();

  OB_UNIS_ENCODE(chunk_size_);
  OB_UNIS_ENCODE(data_length_);
  OB_UNIS_ENCODE(locator_);
  OB_UNIS_ENCODE(index_count);
  for (int i = 0; OB_SUCC(ret) && i < index_count; ++i) {
    if (OB_FAIL(index_[i].serialize(buf, buf_len, pos))) {
      LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
    }
  }
  OB_UNIS_ENCODE(data_count);
  for (int i = 0; OB_SUCC(ret) && i < data_count; ++i) {
    if (OB_FAIL(data_[i].serialize(buf, buf_len, pos))) {
      LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len), K(i));
    }
  }
  OB_UNIS_ENCODE(old_data_count);
  for (int i = 0; OB_SUCC(ret) && i < old_data_count; ++i) {
    if (OB_FAIL(old_data_[i].serialize(buf, buf_len, pos))) {
      LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len), K(i));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLobPartialData)
{
  int ret = OB_SUCCESS;
  int32_t index_count = 0;
  int32_t data_count = 0;
  int32_t old_data_count = 0;

  OB_UNIS_DECODE(chunk_size_);
  OB_UNIS_DECODE(data_length_);
  OB_UNIS_DECODE(locator_);
  OB_UNIS_DECODE(index_count);
  for (int32_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
    ObLobChunkIndex idx;
    int32_t data_idx = 0;
    if (OB_FAIL(idx.deserialize(buf, data_len, pos))) {
      LOG_ERROR("deserialize chunk idx failed", K(ret), K(pos), K(data_len), K(i));
    } else if (OB_FAIL(push_chunk_index(idx))) {
      LOG_ERROR("deserialize push_back failed", K(ret), K(pos), K(data_len), K(i));
    }
  }
  OB_UNIS_DECODE(data_count);
  for (int32_t i = 0; OB_SUCC(ret) && i < data_count; ++i) {
    ObLobChunkData data;
    if (OB_FAIL(data.deserialize(buf, data_len, pos))) {
      LOG_ERROR("deserialize failed", K(ret), K(pos), K(data_len), K(i));
    } else if (OB_FAIL(data_.push_back(data))) {
      LOG_ERROR("deserialize failed", K(ret), K(pos), K(data_len), K(i));
    }
  }
  OB_UNIS_DECODE(old_data_count);
  for (int32_t i = 0; OB_SUCC(ret) && i < old_data_count; ++i) {
    ObLobChunkData data;
    if (OB_FAIL(data.deserialize(buf, data_len, pos))) {
      LOG_ERROR("deserialize failed", K(ret), K(pos), K(data_len), K(i));
    } else if (OB_FAIL(old_data_.push_back(data))) {
      LOG_ERROR("deserialize failed", K(ret), K(pos), K(data_len), K(i));
    }
  }
  return ret;
}

int ObLobPartialData::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(search_map_.create(10, "LobPartial"))) {
    LOG_WARN("map create fail", K(ret));
  }
  return ret;
}

int ObLobPartialData::push_chunk_index(const ObLobChunkIndex &chunk_index)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_.push_back(chunk_index))) {
    LOG_ERROR("push_back failed", K(ret));
  } else if (OB_FAIL(search_map_.set_refactored(chunk_index.offset_/chunk_size_, index_.count() - 1))) {
    LOG_ERROR("set_refactored failed", K(ret), K(index_.count()), K(chunk_index));
  }
  return ret;
}

int ObLobPartialData::get_ori_data_length(int64_t &len) const
{
  ObLobLocatorV2 locator(locator_);
  return locator.get_lob_data_byte_len(len);
}

int ObLobPartialData::sort_index()
{
  int ret = OB_SUCCESS;
  lib::ob_sort(index_.begin(), index_.end(), ObLobChunkIndexComparator());
  search_map_.reuse();
  for (int i = 0; i < index_.count(); ++i) {
    const ObLobChunkIndex &chunk_index = index_[i];
    if (OB_FAIL(search_map_.set_refactored(chunk_index.offset_/chunk_size_, i))) {
      LOG_ERROR("set_refactored failed", K(ret), K(index_.count()), K(chunk_index));
    }
  }
  return ret;
}

bool ObLobPartialData::is_full_mode()
{
  return data_.count() == 1 && data_[0].data_.length() == data_length_;
}

int64_t ObLobPartialData::get_modified_chunk_cnt() const
{
  int64_t chunk_cnt = 0;
  for (int i = 0; i < index_.count(); ++i) {
    const ObLobChunkIndex &chunk_index = index_[i];
    if (chunk_index.is_modified_) {
      chunk_cnt++;
    } else if (chunk_index.is_add_) {
      // new add chunk contain all new append data, it may be more than one chunk
      chunk_cnt += (chunk_index.byte_len_ + chunk_size_ - 1) / chunk_size_;
    }
  }
  return chunk_cnt;
}

}
}
