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

#include "lib/oblog/ob_log.h"
#include "ob_lob_manager.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "lib/objectpool/ob_server_object_pool.h"

namespace oceanbase
{
namespace storage
{

int ObLobManager::mtl_new(ObLobManager *&m) {
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  m = OB_NEW(ObLobManager, oceanbase::ObModIds::OMT_TENANT, tenant_id);
  if (OB_ISNULL(m)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObLobManager::mtl_destroy(ObLobManager *&m)
{
  if (OB_UNLIKELY(nullptr == m)) {
    LOG_WARN("meta mem mgr is nullptr", KP(m));
  } else {
    OB_DELETE(ObLobManager, oceanbase::ObModIds::OMT_TENANT, m);
    m = nullptr;
  }
}

int ObLobManager::mtl_init(ObLobManager* &m)
{
  return m->init();
}

int ObLobManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  lib::ObMemAttr mem_attr(tenant_id, "LobAllocator", ObCtxIds::LOB_CTX_ID);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobManager init twice.", K(ret));
  } else if (allocator_.init(common::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr)) {
    LOG_WARN("init allocator failed.", K(ret));
  } else if (OB_FAIL(lob_ctxs_.create(DEFAULT_LOB_META_BUCKET_CNT, &allocator_))) {
    LOG_WARN("Init lob meta maps falied.", K(ret));
  } else {
    lob_ctx_.lob_meta_mngr_ = &meta_manager_;
    lob_ctx_.lob_piece_mngr_ = &piece_manager_;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObLobManager::start()
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int ObLobManager::stop()
{
  STORAGE_LOG(INFO, "[LOB]stop");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    auto meta_iter = lob_ctxs_.begin();
    while (meta_iter != lob_ctxs_.end()) {
      if (OB_NOT_NULL(meta_iter->second.lob_meta_mngr_)) {
        meta_iter->second.lob_meta_mngr_->~ObLobMetaManager();
        allocator_.free(meta_iter->second.lob_meta_mngr_);
        meta_iter->second.lob_meta_mngr_ = nullptr;
      }
      if (OB_NOT_NULL(meta_iter->second.lob_piece_mngr_)) {
        meta_iter->second.lob_piece_mngr_->~ObLobPieceManager();
        allocator_.free(meta_iter->second.lob_piece_mngr_);
        meta_iter->second.lob_piece_mngr_ = nullptr;
      }
      ++meta_iter;
    }
    // TODO
    // 1. 触发LobOperator中内存数据的异步flush
    // 2. 清理临时LOB
  }
  return ret;
}

void ObLobManager::wait()
{
  STORAGE_LOG(INFO, "[LOB]wait");
  // TODO
  // 1. 等待LobOperator中内存数据的异步flush完成
}

void ObLobManager::destroy()
{
  STORAGE_LOG(INFO, "[LOB]destroy");
  // TODO
  // 1. LobOperator.destroy()
  lob_ctxs_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator, ObString &data, ObString &out)
{
  int ret = OB_SUCCESS;
  void* buf = allocator.alloc(data.length() + sizeof(ObLobCommon));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for lob data", K(ret), K(data));
  } else {
    ObLobCommon *lob_data = new(buf)ObLobCommon();
    MEMCPY(lob_data->buffer_, data.ptr(), data.length());
    out.assign_ptr(reinterpret_cast<char*>(buf), data.length() + sizeof(ObLobCommon));
  }
  return ret;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator,
    const ObIArray<share::schema::ObColDesc> &column_ids,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (column_ids.at(i).col_type_.is_lob_v2()) {
      if (datum_row.storage_datums_[i].is_null() || datum_row.storage_datums_[i].is_nop_value()) {
      } else {
        ObString data = datum_row.storage_datums_[i].get_string();
        ObString out;
        if (OB_FAIL(ObLobManager::fill_lob_header(allocator, data, out))) {
          LOG_WARN("failed to fill lob header for column.", K(i), K(column_ids), K(data));
        } else {
          datum_row.storage_datums_[i].set_string(out);
        }
      }
    }
  }
  return ret;
}

void ObLobManager::transform_query_result_charset(
    const common::ObCollationType& coll_type,
    const char* data,
    uint32_t len,
    uint32_t &byte_len,
    uint32_t &byte_st)
{
  byte_st = ObCharset::charpos(coll_type, data, len, byte_st);
  byte_len = ObCharset::charpos(coll_type, data + byte_st, len - byte_st, byte_len);
}

int ObLobManager::get_real_data(
    ObLobAccessParam& param,
    const ObLobQueryResult& result,
    ObString& data)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx;
  if (result.meta_result_.info_.piece_id_ != ObLobMetaUtil::LOB_META_INLINE_PIECE_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid piece id.", K(ret), K(result));
  } else if (result.meta_result_.info_.piece_id_ == ObLobMetaUtil::LOB_META_INLINE_PIECE_ID) {
    // read data from lob_meta.lob_data
    uint32_t byte_len = result.meta_result_.len_;
    uint32_t byte_st = result.meta_result_.st_;
    const char *lob_data = result.meta_result_.info_.lob_data_.ptr();
    if (param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY) {
      transform_query_result_charset(param.coll_type_, lob_data,
        result.meta_result_.info_.byte_len_, byte_len, byte_st);
    }
    if (data.write(lob_data + byte_st, byte_len) != byte_len) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("failed to write buffer to output_data.", K(ret), K(data),
                K(result.meta_result_.st_), K(result.meta_result_.len_));
    }
  }
  return ret;
}

int ObLobManager::check_handle_size(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  ObLobCommon *lob_common = param.lob_common_;
  int64_t expected_len = sizeof(ObLobCommon);
  if (lob_common->is_init_) {
    expected_len += sizeof(ObLobData);
  }
  if (!lob_common->in_row_) {
    expected_len += sizeof(ObLobDataOutRowCtx);
  } else {
    expected_len += param.byte_size_;
  }
  if (param.handle_size_ < expected_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle size is too small", K(ret), K(expected_len), K(param));
  } else {
    uint64_t max_handle_lob_len = 64 * 1024L * 1024L;
    if (lob_common->use_big_endian_ == 0 && param.byte_size_ > max_handle_lob_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unable to process little endian lob with length over 64M",
        K(ret), K(lob_common->use_big_endian_), K(param));
    }
  }
  return ret;
}

int ObLobManager::query(
    ObLobAccessParam& param,
    ObString& output_data)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (lob_common->in_row_) {
      ObString data;
      if (lob_common->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(lob_common->buffer_, param.byte_size_);
      }
      uint32_t byte_offset = param.offset_;
      uint32_t max_len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length()) - byte_offset;
      uint32_t byte_len = (param.len_ > max_len) ? max_len : param.len_;
      transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
      if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("data length is not enough.", K(ret), KPC(lob_common), KPC(param.lob_data_), K(byte_offset), K(byte_len));
      } else if (output_data.write(data.ptr() + byte_offset, byte_len) != byte_len) {
        ret = OB_ERR_INTERVAL_INVALID;
        LOG_WARN("failed to write buffer to output_data.", K(ret), K(output_data), K(byte_offset), K(byte_len));
      }
    } else if (0) { // loc->is_remote()
      // TODO remote loc
    } else {
      ObLobMetaScanIter meta_iter;
      ObLobCtx lob_ctx = lob_ctx_;
      if (!lob_common->is_init_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob common header for out row.", K(ret), KPC(lob_common));
      } else {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter))) {
          LOG_WARN("do lob meta scan failed.", K(ret), K(param));
        } else {
          ObLobQueryResult result;
          while (OB_SUCC(ret)) {
            ret = meta_iter.get_next_row(result.meta_result_);
            if (OB_FAIL(ret)) {
              if (ret == OB_ITER_END) {
              } else {
                LOG_WARN("failed to get next row.", K(ret));
              }
            } else if (ObTimeUtility::current_time() > param.timeout_) {
              ret = OB_TIMEOUT;
              int64_t cur_time = ObTimeUtility::current_time();
              LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
              /* TODO: weiyouchao.wyc should set param.asscess_ptable_ as false 2022.4.7 */
            } else if (param.asscess_ptable_ /* not operate piece table currently */ &&
                      OB_FAIL(lob_ctx.lob_piece_mngr_->get(param, result.meta_result_.info_.piece_id_, result.piece_info_))) {
              LOG_WARN("get lob piece failed.", K(ret), K(result));
            } else if (OB_FAIL(get_real_data(param, result, output_data))) {
              LOG_WARN("failed to write data to output buf.", K(ret), K(result), K(output_data));
            }
          }
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
          }
        }
      }
    } 
  }
  return ret;
}


int ObLobManager::query(
    ObLobAccessParam& param,
    ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  bool is_in_row = false;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (lob_common->in_row_) {
      ObString data;
      if (lob_common->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(lob_common->buffer_, param.byte_size_);
      }
      uint64_t byte_offset = param.offset_;
      uint32_t byte_len = param.len_;
      if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("data length is not enough.", K(ret), K(byte_offset), K(param.len_));
      } else {
        ObLobQueryIter* iter = common::sop_borrow(ObLobQueryIter);
        if (OB_ISNULL(iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc lob meta scan iterator fail", K(ret));
        } else if (OB_FAIL(iter->open(data))) {
          LOG_WARN("do lob meta scan failed.", K(ret), K(data));
        } else {
          result = iter;
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
          iter->reset();
          common::sop_return(ObLobQueryIter, iter);
        }
      }
    } else if (0) { // loc->is_remote()
      // TODO remote loc
    } else {
      if (!lob_common->is_init_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob common header for out row.", K(ret), KPC(lob_common));
      } else {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        ObLobQueryIter* iter = common::sop_borrow(ObLobQueryIter);
        ObLobCtx lob_ctx = lob_ctx_;
        if (OB_ISNULL(iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc lob meta scan iterator fail", K(ret));
        } else if (OB_FAIL(iter->open(param, lob_ctx))) {
          LOG_WARN("do lob meta scan failed.", K(ret), K(param));
        } else {
          result = iter;
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
          iter->reset();
          common::sop_return(ObLobQueryIter, iter);
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write_one_piece(ObLobAccessParam& param,
                                  common::ObTabletID& piece_tablet_id,
                                  ObLobCtx& lob_ctx,
                                  ObLobMetaInfo& meta_info,
                                  ObString& data,
                                  bool need_alloc_macro_id)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo meta_row = meta_info;

  meta_row.lob_data_.assign_ptr(data.ptr(), data.length());
  if (OB_FAIL(lob_ctx.lob_meta_mngr_->write(param, meta_row))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(update_out_ctx(param, nullptr, meta_row))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else {
    param.lob_data_->byte_size_ += meta_row.byte_len_;
    param.byte_size_ = param.lob_data_->byte_size_;
  }

  return ret;
}

int ObLobManager::update_one_piece(ObLobAccessParam& param,
                                   ObLobCtx& lob_ctx,
                                   ObLobMetaInfo& old_meta_info,
                                   ObLobMetaInfo& new_meta_info,
                                   ObLobPieceInfo& piece_info,
                                   ObString& data)
{
  int ret = OB_SUCCESS;
  new_meta_info.lob_data_.assign_ptr(data.ptr(), data.length());
  if (OB_FAIL(lob_ctx.lob_meta_mngr_->update(param, old_meta_info, new_meta_info))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(update_out_ctx(param, &old_meta_info, new_meta_info))) {
    LOG_WARN("failed update checksum.", K(ret));
  } else {
    param.lob_data_->byte_size_ -= old_meta_info.byte_len_;
    param.lob_data_->byte_size_ += new_meta_info.byte_len_;
    param.byte_size_ = param.lob_data_->byte_size_;
  }

  return ret;
}

int ObLobManager::erase_one_piece(ObLobAccessParam& param,
                                  ObLobCtx& lob_ctx,
                                  ObLobMetaInfo& meta_info,
                                  ObLobPieceInfo& piece_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lob_ctx.lob_meta_mngr_->erase(param, meta_info))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(update_out_ctx(param, nullptr, meta_info))) { // old row
    LOG_WARN("failed update checksum.", K(ret));
  } else {
    param.lob_data_->byte_size_ -= meta_info.byte_len_;
    param.byte_size_ = param.lob_data_->byte_size_;
  }

  return ret;
}

int ObLobManager::check_need_out_row(
    ObLobAccessParam& param,
    ObString &data,
    bool &need_out_row)
{
  int ret = OB_SUCCESS;
  need_out_row = (param.byte_size_ + data.length()) > LOB_IN_ROW_MAX_LENGTH;
  // in_row : 0 | need_out_row : 0  --> invalid
  // in_row : 0 | need_out_row : 1  --> do nothing, keep out_row
  // in_row : 1 | need_out_row : 0  --> do nothing, keep in_row
  // in_row : 1 | need_out_row : 1  --> in_row to out_row 
  if (!param.lob_common_->in_row_ && !need_out_row) {
    if (!param.lob_common_->is_init_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob data", K(ret), KPC(param.lob_common_), K(data));
    } else {
      need_out_row = true;
    }
  } else if (param.lob_common_->in_row_ && need_out_row) {
    // combine lob_data->buffer and data
    uint64_t total_size = param.byte_size_ + data.length();
    char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buf failed.", K(ret), K(total_size));
    } else {
      MEMCPY(buf, param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
      MEMCPY(buf + param.byte_size_, data.ptr(), data.length());
      data.assign_ptr(buf, total_size);
      // refresh in_row flag
      param.lob_common_->in_row_ = 0;
    }

    // alloc full lob out row header
    if (OB_SUCC(ret)) {
      buf = static_cast<char*>(param.allocator_->alloc(LOB_OUTROW_HEADER_SIZE));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        MEMCPY(buf, param.lob_common_, sizeof(ObLobCommon));
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        if (new_lob_common->is_init_) {
          MEMCPY(new_lob_common->buffer_, param.lob_common_->buffer_, sizeof(ObLobData));
        } else {
          // init lob data and alloc lob id(when not init)
          ObLobData *new_lob_data = new(new_lob_common->buffer_)ObLobData();
          new_lob_data->id_.tablet_id_ = param.tablet_id_.id();
          if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, new_lob_data->id_.lob_id_))) {
            LOG_WARN("get lob id failed.", K(ret), K(param));
          } else {
            new_lob_common->is_init_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          param.lob_common_ = new_lob_common;
          param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
          // init out row ctx
          ObLobDataOutRowCtx *ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
        }
      }
    }
  }
  return ret;
}

int ObLobManager::init_out_row_ctx(
    ObLobAccessParam& param,
    uint64_t len,
    ObLobDataOutRowCtx::OpType op)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtx *out_row_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(param.lob_data_->buffer_);
  if (param.seq_no_st_ == -1) {
    // pre-calc seq_no_cnt and init seq_no_st
    // for insert, most oper len/128K + 2
    // for erase, most oper len/128K + 2
    // for append, most oper len/256K + 1
    // for sql update, calc erase+insert
    int64_t N = ((len + param.update_len_) / (ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE / 2) + 2) * 2;
    param.seq_no_st_ = ObSequence::get_and_inc_max_seq_no(N);
    param.used_seq_cnt_ = 0;
    param.total_seq_cnt_ = N;
  }
  if (OB_SUCC(ret)) {
    out_row_ctx->seq_no_st_ = param.seq_no_st_;
    out_row_ctx->is_full_ = 1;
    out_row_ctx->offset_ = param.offset_;
    out_row_ctx->check_sum_ = param.checksum_;
    out_row_ctx->seq_no_cnt_ = param.used_seq_cnt_;
    out_row_ctx->del_seq_no_cnt_ = param.used_seq_cnt_; // for sql update, first delete then insert
    out_row_ctx->op_ = static_cast<uint8_t>(op);
    out_row_ctx->modified_len_ = 0;
    out_row_ctx->first_meta_offset_ = 0;
  }
  return ret;
}

int ObLobManager::update_out_ctx(
    ObLobAccessParam& param,
    ObLobMetaInfo *old_info,
    ObLobMetaInfo& new_info)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtx *out_row_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(param.lob_data_->buffer_);
  // update seq no
  out_row_ctx->seq_no_cnt_ = param.used_seq_cnt_;
  // update checksum
  ObBatchChecksum bc;
  if (old_info != nullptr) {
    bc.fill(&out_row_ctx->check_sum_, sizeof(out_row_ctx->check_sum_));
    bc.fill(&old_info->lob_id_, sizeof(old_info->lob_id_));
    bc.fill(old_info->seq_id_.ptr(), old_info->seq_id_.length());
    bc.fill(old_info->lob_data_.ptr(), old_info->lob_data_.length());
    out_row_ctx->check_sum_ = bc.calc();
    bc.reset();
  }
  bc.fill(&out_row_ctx->check_sum_, sizeof(out_row_ctx->check_sum_));
  bc.fill(&new_info.lob_id_, sizeof(new_info.lob_id_));
  bc.fill(new_info.seq_id_.ptr(), new_info.seq_id_.length());
  bc.fill(new_info.lob_data_.ptr(), new_info.lob_data_.length());
  out_row_ctx->check_sum_ = bc.calc();
  // update modified_len
  int64_t old_meta_len = (old_info == nullptr) ? 0 : old_info->byte_len_;
  int64_t new_meta_len = (new_info.byte_len_);
  out_row_ctx->modified_len_ += std::abs(new_meta_len - old_meta_len);
  return ret;
}

int ObLobManager::append(
    ObLobAccessParam& param,
    ObString& data)
{
  int ret = OB_SUCCESS;
  bool save_is_reverse = param.scan_backward_;
  uint64_t save_param_len = param.len_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_ISNULL(lob_common)) {
      // alloc new lob_data
      void *tbuf = param.allocator_->alloc(LOB_OUTROW_HEADER_SIZE);
      if (OB_ISNULL(tbuf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for LobData", K(ret));
      } else {
        // init full out row
        lob_common = new(tbuf)ObLobCommon();
        lob_data = new(lob_common->buffer_)ObLobData();
        lob_data->id_.tablet_id_ = param.tablet_id_.id();
        ObLobDataOutRowCtx *outrow_ctx = new(lob_data->buffer_)ObLobDataOutRowCtx();
        param.lob_data_ = lob_data;
        param.lob_common_ = lob_common;
        param.handle_size_ = LOB_OUTROW_HEADER_SIZE;
        alloc_inside = true;
      }
    } else if (lob_common->is_init_) {
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
      lob_data = param.lob_data_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(check_need_out_row(param, data, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(data));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      uint64_t total_size = cur_handle_size + data.length();
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        MEMCPY(new_lob_common, lob_common, cur_handle_size);
        MEMCPY(buf + cur_handle_size, data.ptr(), data.length());
        // refresh lob info
        param.byte_size_ += data.length();
        if (new_lob_common->is_init_) {
          ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
          new_lob_data->byte_size_ += data.length();
        }
        if (alloc_inside) {
          param.allocator_->free(param.lob_common_);
        }
        param.lob_common_ = new_lob_common;
      }
    } else if (0) { // loc->is_remote
      // TODO remote loc
    } else if (need_out_row) {
      ObLobMetaWriteIter iter(data, param.allocator_, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(init_out_row_ctx(param, data.length(), ObLobDataOutRowCtx::OpType::SQL))) {
        LOG_WARN("init lob data out row ctx failed", K(ret));
      } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->append(param, iter))) {
        LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param));
      } else {
        ObLobMetaWriteResult result;
        while (OB_SUCC(ret)) {
          // split append data into data pieces 250k/piece
          ret = iter.get_next_row(result);
          if (OB_FAIL(ret)) {
            if (ret == OB_ITER_END) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next row.", K(ret));
            }
          } else if (ObTimeUtility::current_time() > param.timeout_) {
            ret = OB_TIMEOUT;
            int64_t cur_time = ObTimeUtility::current_time();
            LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
          } else {
            // get len and pos from meta_info
            common::ObTabletID piece_tablet_id; // TODO get piece tablet id
            if (OB_FAIL(write_one_piece(param,
                                        piece_tablet_id,
                                        lob_ctx,
                                        result.info_,
                                        result.data_,
                                        result.need_alloc_macro_id_))) {
              LOG_WARN("failed write data.", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    param.len_ = save_param_len;
    param.scan_backward_ = save_is_reverse;
  }
  return ret;
}

// input is lob piece row
int ObLobManager::flush(const common::ObTabletID &tablet_id, common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id is invalid.", K(ret), K(tablet_id));
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("flush not implement.", K(ret), K(tablet_id));
  }
  return ret;
}

int ObLobManager::erase_imple_inner(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
  ObLobMetaScanIter meta_iter;
  ObLobCtx lob_ctx = lob_ctx_;
  if (OB_FAIL(init_out_row_ctx(param, param.lob_data_->byte_size_, ObLobDataOutRowCtx::OpType::SQL))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param));
  } else {
    bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
    ObLobQueryResult result;
    while (OB_SUCC(ret)) {
      bool del_piece = false;
      ret = meta_iter.get_next_row(result.meta_result_);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else if (ObTimeUtility::current_time() > param.timeout_) {
        ret = OB_TIMEOUT;
        int64_t cur_time = ObTimeUtility::current_time();
        LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
      } else if (param.asscess_ptable_ /* TODO: weiyouchao.wyc should set param.asscess_ptable_ as false 2022.4.7 */ &&
                 OB_FAIL(lob_ctx.lob_piece_mngr_->get(param, result.meta_result_.info_.piece_id_, result.piece_info_))) {
        LOG_WARN("get lob piece failed.", K(ret), K(result));
      } else if (meta_iter.is_range_begin(result.meta_result_.info_) || meta_iter.is_range_end(result.meta_result_.info_)) {
        // 1. read data 
        // 2. rebuild data
        // 3. write data into lob data oper
        // 4. write meta tablet
        // 5. write piece tablet
        ObLobMetaInfo new_meta_row = result.meta_result_.info_;
        char* tmp_buf = static_cast<char*>(param.allocator_->alloc(result.meta_result_.info_.byte_len_));
        ObString read_data;
        read_data.assign_buffer(tmp_buf, result.meta_result_.info_.byte_len_);
        
        // save variable
        uint32_t tmp_st = result.meta_result_.st_;
        uint32_t tmp_len = result.meta_result_.len_;
        
        // read all piece data
        result.meta_result_.st_ = 0;
        result.meta_result_.len_ = result.meta_result_.info_.char_len_;
        if (OB_ISNULL(tmp_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate tmp buffer.", K(ret), K(result.meta_result_.info_.byte_len_));
        } else if (OB_FAIL(get_real_data(param, result, read_data))) {
          LOG_WARN("failed to write data to read buf.", K(ret), K(result));
        } else {
          result.meta_result_.st_ = tmp_st;
          result.meta_result_.len_ = tmp_len;

          // global pos, from 0
          uint64_t cur_piece_end = meta_iter.get_cur_pos();
          uint64_t cur_piece_begin = cur_piece_end - result.meta_result_.info_.char_len_;

          // local pos, from current piece;
          // if is_char, char pos; else byte pos
          uint32_t local_begin = param.offset_ - cur_piece_begin;
          uint32_t local_end = local_begin + param.len_;

          // char len
          uint32_t piece_byte_len = result.meta_result_.info_.byte_len_;
          uint32_t piece_char_len = result.meta_result_.info_.char_len_;
          uint32_t piece_data_st = 0;
          
          if (is_char) {
            transform_query_result_charset(param.coll_type_,
                                            read_data.ptr(),
                                            read_data.length(), 
                                            piece_char_len,
                                            piece_data_st);
            if (piece_char_len != piece_byte_len || piece_data_st > 0) {
              ret  = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to erase.", K(ret), K(result));
            } else {
              piece_byte_len = piece_char_len;
              piece_char_len = result.meta_result_.info_.char_len_;                  
            }
          }

          if (OB_FAIL(ret)) {
          } else if (meta_iter.is_range_begin(result.meta_result_.info_) &&
                      meta_iter.is_range_end(result.meta_result_.info_)) {
            if (local_end - local_begin != piece_char_len) {
              uint32_t by_st = local_begin;
              uint32_t by_len = local_end - local_begin;
              if (is_char) {
                transform_query_result_charset(param.coll_type_,
                                                read_data.ptr(),
                                                read_data.length(),
                                                by_len,
                                                by_st);
              }
              new_meta_row.char_len_ -= (local_end - local_begin);
              MEMCPY(read_data.ptr() + by_st, read_data.ptr() + (by_st + by_len), piece_byte_len - (by_st + by_len));
              read_data.assign_ptr(read_data.ptr(), read_data.length() - by_len);
            } else {
              del_piece = true;
            }
          } else if (meta_iter.is_range_begin(result.meta_result_.info_)) {
            if (local_begin == 0) {
              del_piece = true;
            } else {
              uint32_t by_st = 0;
              uint32_t by_len = local_begin;
              if (is_char) {
                transform_query_result_charset(param.coll_type_,
                                                read_data.ptr(),
                                                read_data.length(),
                                                by_len,
                                                by_st);
              }
              read_data.assign_ptr(read_data.ptr(), by_len);
              new_meta_row.byte_len_ = by_len;
              new_meta_row.char_len_ = local_begin;
            }
          } else {
            if (local_end == piece_char_len) {
              del_piece = true;
            } else {
              uint32_t by_st = 0;
              uint32_t by_len = local_end;
              if (is_char) {
                transform_query_result_charset(param.coll_type_,
                                                read_data.ptr(),
                                                read_data.length(),
                                                by_len,
                                                by_st);
              }
              new_meta_row.char_len_ = piece_char_len - local_end;
              new_meta_row.byte_len_ = read_data.length() - by_len;
              MEMMOVE(read_data.ptr(), read_data.ptr() + by_len, read_data.length() - by_len);
              read_data.assign_ptr(read_data.ptr(), read_data.length() - by_len);
            }
          }

          if (OB_FAIL(ret)) {
          } else if (del_piece) {
            if (OB_FAIL(erase_one_piece(param, 
                                        lob_ctx,
                                        result.meta_result_.info_,
                                        result.piece_info_))) {
              LOG_WARN("failed erase one piece", K(ret), K(result));
            }
          } else {
            if (OB_FAIL(update_one_piece(param,
                                         lob_ctx,
                                         result.meta_result_.info_,
                                         new_meta_row,
                                         result.piece_info_,
                                         read_data))) {
              LOG_WARN("failed to update.", K(ret), K(result), K(read_data));
            }
          }
        }
        param.allocator_->free(tmp_buf);
      } else {
        if (OB_FAIL(erase_one_piece(param, 
                                    lob_ctx,
                                    result.meta_result_.info_,
                                    result.piece_info_))) {
          LOG_WARN("failed erase one piece", K(ret), K(result));
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobManager::erase(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    if (OB_FAIL(OB_ISNULL(param.lob_common_))) {
      LOG_WARN("get lob locator null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (param.lob_common_->in_row_) {
      if (param.lob_common_->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
      }
      ObString data;
      if (param.lob_data_ != nullptr) {
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(param.lob_common_->buffer_, param.byte_size_);
      }
      uint32_t byte_offset = param.offset_;
      if (OB_UNLIKELY(data.length() < byte_offset)) {
        // offset overflow, do nothing
      } else {
        // allow erase len oversize, get max(param.len_, actual_len)
        uint32_t max_len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length()) - byte_offset;
        uint32_t byte_len = (param.len_ > max_len) ? max_len : param.len_;
        transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
        if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("data length is not enough.", K(ret), KPC(param.lob_data_), K(byte_offset), K(byte_len));
        } else {
          char* dst_start = data.ptr() + byte_offset;
          char* src_start = data.ptr() + byte_offset + byte_len;
          uint32_t cp_len = data.length() - (byte_len + byte_offset);
          if (cp_len > 0) {
            MEMMOVE(dst_start, src_start, cp_len);
          }
          param.byte_size_ -= byte_len;
          if (param.lob_data_ != nullptr) {
            param.lob_data_->byte_size_ = param.byte_size_;
          }
        }
      }
    } else if (0) { // loc->is_remote()
      // TODO remote loc
    } else if (OB_FAIL(erase_imple_inner(param))) {
      LOG_WARN("failed erase", K(ret));
    }
  }
  return ret;
}

/*************ObLobQueryIter*****************/
int ObLobQueryIter::open(ObLobAccessParam &param, ObLobCtx& lob_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_ctx.lob_meta_mngr_) ||
      OB_ISNULL(lob_ctx.lob_piece_mngr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob ctx.", K(ret), K(lob_ctx));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter_))) {
    LOG_WARN("open meta iter failed.");
  } else {
    param_ = param;
    lob_ctx_ = lob_ctx;
    is_inited_ = true;
    is_in_row_ = false;
  }
  return ret;
}

int ObLobQueryIter::open(ObString &data)
{
  int ret = OB_SUCCESS;
  inner_data_.assign_ptr(data.ptr(), data.length());
  is_inited_ = true;
  is_in_row_ = true;
  return ret;
}

int ObLobQueryIter::get_next_row(ObLobQueryResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else {
    ret = meta_iter_.get_next_row(result.meta_result_);
    if (OB_FAIL(ret)) {
      if (ret == OB_ITER_END) {
      } else {
        LOG_WARN("failed to get lob meta next row.", K(ret));
      }
    } else if (param_.asscess_ptable_  /* TODO: weiyouchao.wyc should set param.asscess_ptable_ as false 2022.4.7 */ &&
               OB_FAIL(lob_ctx_.lob_piece_mngr_->get(param_, result.meta_result_.info_.piece_id_, result.piece_info_))) {
      LOG_WARN("get lob piece failed.", K(ret), K(result));
    }
  }
  return ret;
}

int ObLobQueryIter::get_next_row(ObString& data)
{
  int ret = OB_SUCCESS;
  ObLobQueryResult result;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else if (is_in_row_) {
    uint64_t read_size = data.size();
    if (cur_pos_ + read_size > inner_data_.length()) {
      read_size = inner_data_.length() - cur_pos_;
    }
    if (cur_pos_ == inner_data_.length()) {
      ret = OB_ITER_END;
    } else if (data.write(data.ptr() + cur_pos_, read_size) != read_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to write output data.", K(data), K(cur_pos_), K(read_size), K(inner_data_));
    } else {
      cur_pos_ += read_size;
    }
  } else if (OB_FAIL(get_next_row(result))) {
    LOG_WARN("get next query result failed.", K(ret));
  } else {
    // do get real data
    ObLobManager *lob_mngr = MTL(ObLobManager*);
    if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get lob mngr.", K(ret));
    } else if (OB_FAIL(lob_mngr->get_real_data(param_, result, data))) {
      LOG_WARN("get real data failed.", K(ret));
    }
  }
  return ret;
}

void ObLobQueryIter::reset()
{
  meta_iter_.reset();
  inner_data_.reset();
  is_in_row_ = false;
  is_inited_ = false;
}

} // storage
} // oceanbase
