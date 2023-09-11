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
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
namespace storage
{

int ObLobManager::mtl_new(ObLobManager *&m) {
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  auto attr = SET_USE_500("LobManager");
  m = OB_NEW(ObLobManager, attr, tenant_id);
  if (OB_ISNULL(m)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObLobManager::mtl_destroy(ObLobManager *&m)
{
  if (OB_UNLIKELY(nullptr == m)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "meta mem mgr is nullptr", KP(m));
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
  } else if (OB_FAIL(allocator_.init(common::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("init allocator failed.", K(ret));
  } else if (OB_FAIL(lob_ctxs_.create(DEFAULT_LOB_META_BUCKET_CNT, &allocator_))) {
    LOG_WARN("Init lob meta maps falied.", K(ret));
  } else {
    OB_ASSERT(sizeof(ObLobCommon) == sizeof(uint32));
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
    if (column_ids.at(i).col_type_.is_lob_storage()) {
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


// delta tmp lob locator
// Content:
// ObMemLobCommon |
// tmp delta disk locator | -> [ObLobCommon : {inrow : 1, init : 0}]
// inline buffer | [tmp_header][persis disk locator][tmp_diff][inline_data]
int ObLobManager::build_tmp_delta_lob_locator(ObIAllocator &allocator,
    ObLobLocatorV2 *persist,
    const ObString &data,
    bool is_locator,
    ObLobDiffFlags flags,
    uint8_t op,
    uint64_t offset, // ori offset
    uint64_t len, // ori len
    uint64_t dst_offset,
    ObLobLocatorV2 &out)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(persist) || !persist->is_persist_lob()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persist lob locator", K(ret), KPC(persist));
  } else {
    // calc res len
    uint64_t res_len = ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN;
    uint64_t data_len = data.length();
    ObString persist_disk_loc;
    bool need_out_row = false;
    if (need_out_row) {
      ret = OB_NOT_IMPLEMENT;
    } else {
      if (OB_FAIL(persist->get_disk_locator(persist_disk_loc))) {
        LOG_WARN("get persist disk locator failed.", K(ret));
      } else {
        if (!is_locator) {
          data_len += sizeof(ObMemLobCommon) + sizeof(ObLobCommon);
        }
        res_len += sizeof(ObLobCommon) + persist_disk_loc.length() + sizeof(ObLobDiffHeader) + sizeof(ObLobDiff) + data_len;
      }
    }
    char *buf = nullptr;
    if (OB_SUCC(ret)) {
      buf = reinterpret_cast<char*>(allocator.alloc(res_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for new lob locator", K(ret), K(res_len));
      }
    }

    // build lob locator common
    if (OB_SUCC(ret)) {
      ObMemLobCommon *mem_common = new(buf)ObMemLobCommon(ObMemLobType::TEMP_DELTA_LOB, false);
      // build disk locator
      ObLobCommon *lob_common = new(mem_common->data_)ObLobCommon();
      // build inline buffer
      ObLobDiffHeader *diff_header = new(lob_common->buffer_)ObLobDiffHeader();
      diff_header->diff_cnt_ = 1;
      diff_header->persist_loc_size_ = persist_disk_loc.length();

      // copy persist locator
      MEMCPY(diff_header->data_, persist_disk_loc.ptr(), persist_disk_loc.length());
      char *diff_st = diff_header->data_ + persist_disk_loc.length();
      ObLobDiff *diff = new(diff_st)ObLobDiff();
      diff->ori_offset_ = offset;
      diff->ori_len_ = len;
      diff->offset_ = 0;
      diff->byte_len_ = data_len;
      diff->dst_offset_ = dst_offset;
      diff->type_ = static_cast<ObLobDiff::DiffType>(op);
      diff->flags_ = flags;

      char *diff_data = diff_header->get_inline_data_ptr();
      if (!is_locator) {
        ObMemLobCommon *diff_mem_common = new(diff_data)ObMemLobCommon(ObMemLobType::TEMP_FULL_LOB, false);
        ObLobCommon *diff_lob_common = new(diff_mem_common->data_)ObLobCommon();
        diff_data = diff_lob_common->buffer_;
      }
      MEMCPY(diff_data, data.ptr(), data.length());

      out.ptr_ = buf;
      out.size_ = res_len;
      out.has_lob_header_ = true;
    }
  }
  return ret;
}

// full tmp lob locator
// Content:
// ObMemLobCommon |
// ObMemLobOraCommon |
// disk locator | -> [ObLobCommon : {inrow : 1, init : 0}]
// inline buffer | [inline_data]
int ObLobManager::build_tmp_full_lob_locator(ObIAllocator &allocator,
    const ObString &data,
    common::ObCollationType coll_type,
    ObLobLocatorV2 &out)
{
  int ret = OB_SUCCESS;
  uint64_t res_len = ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN;
  bool need_outrow = false;
  if (need_outrow) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    res_len += sizeof(ObLobCommon) + data.length();
    char *buf = reinterpret_cast<char*>(allocator.alloc(res_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for new lob locator", K(ret), K(res_len));
    } else {
      ObMemLobCommon *mem_common = new(buf)ObMemLobCommon(ObMemLobType::TEMP_FULL_LOB, false);
      mem_common->set_read_only(false);
      char *next_ptr = mem_common->data_;
      // build disk locator
      ObLobCommon *lob_common = new(next_ptr)ObLobCommon();
      // copy data
      if (data.length() > 0) {
        MEMCPY(lob_common->buffer_, data.ptr(), data.length());
      }
      out.ptr_ = buf;
      out.size_ = res_len;
      out.has_lob_header_ = true;
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
    if (param.scan_backward_ && data.write_front(lob_data + byte_st, byte_len) != byte_len) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("failed to write buffer to output_data.", K(ret),
                K(data.length()), K(data.remain()), K(byte_st), K(byte_len));
    } else if (!param.scan_backward_ && data.write(lob_data + byte_st, byte_len) != byte_len) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("failed to write buffer to output_data.", K(ret),
                K(data.length()), K(data.remain()), K(byte_st), K(byte_len));
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

int ObLobManager::get_ls_leader(ObLobAccessParam& param, const uint64_t tenant_id,
    const share::ObLSID &ls_id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else {
    uint32_t renew_count = 0;
    const uint32_t max_renew_count = 10;
    const int64_t retry_us = 200 * 1000;
    do {
      if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader))) {
        if (OB_LS_LOCATION_NOT_EXIST == ret && renew_count++ < max_renew_count) {  // retry ten times
          LOG_WARN("failed to get location and force renew", K(ret), K(tenant_id), K(ls_id), K(cluster_id));
          if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
            LOG_WARN("failed to nonblock renew from location cache", K(tmp_ret), K(ls_id), K(cluster_id));
          } else if (ObTimeUtility::current_time() > param.timeout_) {
            renew_count = max_renew_count;
          } else {
            usleep(retry_us);
          }
        }
      } else {
        LOG_INFO("get ls leader", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
      }
    } while (OB_LS_LOCATION_NOT_EXIST == ret && renew_count < max_renew_count);

    if (OB_SUCC(ret) && !leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(ls_id), K(leader), K(cluster_id));
    }
  }
  return ret;
}

int ObLobManager::is_remote(ObLobAccessParam& param, bool& is_remote, common::ObAddr& dst_addr)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  const ObAddr &self_addr = MYADDR;
  if (lob_locator == nullptr) {
    is_remote = false;
  } else if (!lob_locator->is_persist_lob()) {
    is_remote = false;
  } else {
    uint64_t tenant_id = param.tenant_id_;
    if (OB_FAIL(get_ls_leader(param, tenant_id, param.ls_id_, dst_addr))) {
      LOG_WARN("failed to get ls leader", K(ret), K(tenant_id), K(param.ls_id_));
    } else {
      // lob from other tenant also should read by rpc
      is_remote = (dst_addr != self_addr) || (tenant_id != MTL_ID());
      if (param.from_rpc_ == true && is_remote) {
        ret = OB_NOT_MASTER;
        LOG_WARN("call from rpc, but remote again", K(ret), K(dst_addr), K(self_addr), K(tenant_id), K(MTL_ID()));
      }
    }
  }
  return ret;
}

bool ObLobManager::is_remote_ret_can_retry(int ret)
{
  return (ret == OB_NOT_MASTER);
}

int ObLobManager::lob_remote_query_with_retry(
    ObLobAccessParam &param,
    common::ObAddr& dst_addr,
    ObLobQueryArg& arg,
    int64_t timeout,
    common::ObDataBuffer& rpc_buffer,
    obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY>& handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = (MTL(ObLSService *));
  obrpc::ObStorageRpcProxy *svr_rpc_proxy = ls_service->get_storage_rpc_proxy();
  int64_t retry_max = REMOTE_LOB_QUERY_RETRY_MAX;
  int64_t retry_cnt = 0;
  bool is_continue = true;
  do {
    ret = svr_rpc_proxy->to(dst_addr).by(arg.tenant_id_)
                    .dst_cluster_id(GCONF.cluster_id)
                    .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                    .timeout(timeout)
                    .lob_query(arg, rpc_buffer, handle);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do remote query", K(ret), K(arg), K(dst_addr), K(timeout));
      if (is_remote_ret_can_retry(ret)) {
        retry_cnt++;
        switch (ret) {
          case OB_NOT_MASTER: {
            bool remote_bret = false;
            // refresh leader
            if (OB_FAIL(is_remote(param, remote_bret, dst_addr))) {
              LOG_WARN("fail to refresh leader addr", K(ret), K(param));
              is_continue = false;
            } else {
              LOG_INFO("refresh leader location", K(retry_cnt), K(retry_max), K(remote_bret), K(dst_addr), K(param));
            }
            break;
          }
          default: {
            LOG_INFO("do nothing, just retry", K(ret), K(retry_cnt), K(retry_max));
          }
        }
      } else {
        is_continue = false;
      }
    } else {
      is_continue = false;
    }
  } while (is_continue && retry_cnt <= retry_max);
  return ret;
}

int ObLobManager::query_remote(ObLobAccessParam& param, common::ObAddr& dst_addr, ObString& data)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> handle;
  common::ObDataBuffer rpc_buffer;
  ObLobQueryRemoteReader reader;
  if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob locator is null.", K(ret), K(param));
  } else if (OB_FAIL(reader.open(param, rpc_buffer))) {
    LOG_WARN("fail to open lob remote reader", K(ret));
  } else {
    SMART_VAR(ObLobQueryArg, arg) {
      // build arg
      arg.tenant_id_ = param.tenant_id_;
      arg.offset_ = param.offset_;
      arg.len_ = param.len_;
      arg.cs_type_ = param.coll_type_;
      arg.scan_backward_ = param.scan_backward_;
      arg.qtype_ = ObLobQueryArg::QueryType::READ;
      arg.lob_locator_.ptr_ = param.lob_locator_->ptr_;
      arg.lob_locator_.size_ = param.lob_locator_->size_;
      arg.lob_locator_.has_lob_header_ = param.lob_locator_->has_lob_header_;
      int64_t timeout = param.timeout_ - ObTimeUtility::current_time();
      if (timeout < ObStorageRpcProxy::STREAM_RPC_TIMEOUT) {
        timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
      }
      if (OB_FAIL(lob_remote_query_with_retry(param, dst_addr, arg, timeout, rpc_buffer, handle))) {
        LOG_WARN("failed to do remote query", K(ret), K(arg));
      } else {
        ObLobQueryBlock block;
        ObString block_data;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.get_next_block(param, rpc_buffer, handle, block, block_data))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next lob query block", K(ret));
            }
          } else {
            if (param.scan_backward_) {
              if (data.write_front(block_data.ptr(), block_data.length()) != block_data.length()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to write data buffer", K(ret), K(data.remain()), K(block_data.length()));
              }
            } else {
              if (data.write(block_data.ptr(), block_data.length()) != block_data.length()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to write data buffer", K(ret), K(data.remain()), K(block_data.length()));
              }
            }
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
      }
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
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (lob_common->in_row_ || (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data())) {
      ObString data;
      if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
        if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
          LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
        }
      } else { // lob_common->in_row_
        if (lob_common->is_init_) {
          param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
          data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
        } else {
          data.assign_ptr(lob_common->buffer_, param.byte_size_);
        }
      }
      uint32_t byte_offset = param.offset_ > data.length() ? data.length() : param.offset_;
      uint32_t max_len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length()) - byte_offset;
      uint32_t byte_len = (param.len_ > max_len) ? max_len : param.len_;
      transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
      if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("data length is not enough.", K(ret), KPC(lob_common), KPC(param.lob_data_), K(byte_offset), K(byte_len));
      } else if (param.inrow_read_nocopy_) {
        output_data.assign_ptr(data.ptr() + byte_offset, byte_len);
      } else if (output_data.write(data.ptr() + byte_offset, byte_len) != byte_len) {
        ret = OB_ERR_INTERVAL_INVALID;
        LOG_WARN("failed to write buffer to output_data.", K(ret), K(output_data), K(byte_offset), K(byte_len));
      }
    } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
    } else {
      bool is_remote_lob = false;
      common::ObAddr dst_addr;
      if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
        LOG_WARN("check is remote failed.", K(ret), K(param));
      } else if (is_remote_lob) {
        if (OB_FAIL(query_remote(param, dst_addr, output_data))) {
          LOG_WARN("do remote query failed.", K(ret), K(param), K(dst_addr));
        }
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
  }
  return ret;
}

int ObLobManager::query_inrow_get_iter(
    ObLobAccessParam& param,
    ObString &data,
    uint32_t offset,
    bool scan_backward,
    ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t byte_offset = offset;
  uint32_t byte_len = param.len_;
  if (byte_offset > data.length()) {
    byte_offset = data.length();
  }
  if (byte_len + byte_offset > data.length()) {
    byte_len = data.length() - byte_offset;
  }
  if (is_char) {
    transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
  }
  if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("data length is not enough.", K(ret), K(byte_offset), K(param.len_));
  } else {
    ObLobQueryIter* iter = OB_NEW(ObLobQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc lob meta scan iterator fail", K(ret));
    } else if (OB_FAIL(iter->open(data, byte_offset, byte_len, param.coll_type_, scan_backward))) {
      LOG_WARN("do lob meta scan failed.", K(ret), K(data));
    } else {
      result = iter;
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
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
      ObString data;
      if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
        LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
      } else if (OB_FAIL(query_inrow_get_iter(param, data, param.offset_, param.scan_backward_, result))) {
        LOG_WARN("fail to get inrow query iter", K(ret));
        if (OB_NOT_NULL(result)) {
          result->reset();
          OB_DELETE(ObLobQueryIter, "unused", result);
          result = nullptr;
        }
      }
    } else if (lob_common->in_row_) {
      ObString data;
      if (lob_common->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(lob_common->buffer_, param.byte_size_);
      }
      if (OB_FAIL(query_inrow_get_iter(param, data, param.offset_, param.scan_backward_, result))) {
        LOG_WARN("fail to get inrow query iter", K(ret));
        if (OB_NOT_NULL(result)) {
          result->reset();
          OB_DELETE(ObLobQueryIter, "unused", result);
          result = nullptr;
        }
      }
    } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
    } else {
      bool is_remote_lob = false;
      common::ObAddr dst_addr;
      ObLobCtx lob_ctx = lob_ctx_;
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
      ObLobQueryIter* iter = OB_NEW(ObLobQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc lob meta scan iterator fail", K(ret));
      } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
        LOG_WARN("check is remote failed.", K(ret), K(param));
      } else if (is_remote_lob) {
        if (OB_FAIL(iter->open(param, dst_addr))) {
          LOG_WARN("open remote iter query failed.", K(ret), K(param), K(dst_addr));
        }
      } else {
        if (OB_FAIL(iter->open(param, lob_ctx))) {
          LOG_WARN("open local meta scan iter failed", K(ret), K(param));
        }
      }
      if (OB_SUCC(ret)) {
        result = iter;
      } else if (OB_NOT_NULL(iter)) {
        iter->reset();
        OB_DELETE(ObLobQueryIter, "unused", iter);
      }
    }
  }
  return ret;
}

int ObLobManager::compare(ObLobLocatorV2& lob_left,
                          ObLobLocatorV2& lob_right,
                          ObLobCompareParams& cmp_params,
                          int64_t& result) {
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get lob manager handle null.", K(ret));
  } else if(!lob_left.has_lob_header() || !lob_right.has_lob_header()) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid lob. should have lob locator", K(ret));
  } else {
    // get lob access param
    ObLobAccessParam param_left;
    ObLobAccessParam param_right;
    param_left.tx_desc_ = cmp_params.tx_desc_;
    param_right.tx_desc_ = cmp_params.tx_desc_;
    if (OB_FAIL(build_lob_param(param_left, tmp_allocator, cmp_params.collation_left_,
                cmp_params.offset_left_, cmp_params.compare_len_, cmp_params.timeout_, lob_left))) {
      LOG_WARN("fail to build read param left", K(ret), K(lob_left), K(cmp_params));
    } else if(OB_FAIL(build_lob_param(param_right, tmp_allocator, cmp_params.collation_right_,
                cmp_params.offset_right_, cmp_params.compare_len_, cmp_params.timeout_, lob_right))) {
      LOG_WARN("fail to build read param new", K(ret), K(lob_right));
    } else if(OB_FAIL(compare(param_left, param_right, result))) {
      LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right), K(cmp_params));
    }
  }
  return ret;
}

int ObLobManager::compare(ObLobAccessParam& param_left,
                          ObLobAccessParam& param_right,
                          int64_t& result) {
  INIT_SUCC(ret);
  common::ObCollationType collation_left = param_left.coll_type_;
  common::ObCollationType collation_right = param_right.coll_type_;
  common::ObCollationType cmp_collation = collation_left;
  ObIAllocator* tmp_allocator = param_left.allocator_;
  ObLobQueryIter *iter_left = nullptr;
  ObLobQueryIter *iter_right = nullptr;
  if(OB_ISNULL(tmp_allocator)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid alloctor param", K(ret), K(param_left));
  } else if((collation_left == CS_TYPE_BINARY && collation_right != CS_TYPE_BINARY)
            || (collation_left != CS_TYPE_BINARY && collation_right == CS_TYPE_BINARY)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid collation param", K(ret), K(param_left), K(param_right));
  } else if (OB_FAIL(query(param_left, iter_left))) {
    LOG_WARN("query param left by iter failed.", K(ret), K(param_left));
  } else if (OB_FAIL(query(param_right, iter_right))) {
    LOG_WARN("query param right by iter failed.", K(ret), K(param_right));
  } else {
    uint64_t read_buff_size = ObLobManager::LOB_READ_BUFFER_LEN;
    char *read_buff = nullptr;
    char *charset_convert_buff_ptr = nullptr;
    uint64_t charset_convert_buff_size = read_buff_size * ObCharset::CharConvertFactorNum;

    if (OB_ISNULL((read_buff = static_cast<char*>(tmp_allocator->alloc(read_buff_size * 2))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
    } else if (OB_ISNULL((charset_convert_buff_ptr = static_cast<char*>(tmp_allocator->alloc(charset_convert_buff_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc charset convert buffer failed.", K(ret), K(charset_convert_buff_size));
    } else {
      ObDataBuffer charset_convert_buff(charset_convert_buff_ptr, charset_convert_buff_size);
      ObString read_buffer_left;
      ObString read_buffer_right;
      read_buffer_left.assign_buffer(read_buff, read_buff_size);
      read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);

      // compare right after charset convert
      ObString convert_buffer_right;
      convert_buffer_right.assign_ptr(nullptr, 0);

      while (OB_SUCC(ret) && result == 0) {
        if (read_buffer_left.length() == 0) {
          // reset buffer and read next block
          read_buffer_left.assign_buffer(read_buff, read_buff_size);
          if (OB_FAIL(iter_left->get_next_row(read_buffer_left))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for left lob.", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }

        if (OB_SUCC(ret) && convert_buffer_right.length() == 0) {
          read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);
          charset_convert_buff.set_data(charset_convert_buff_ptr, charset_convert_buff_size);
          convert_buffer_right.assign_ptr(nullptr, 0);

          if (OB_FAIL(iter_right->get_next_row(read_buffer_right))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for right lob", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            // convert right lob to left charset if necessary
            if(OB_FAIL(ObExprUtil::convert_string_collation(
                                  read_buffer_right, collation_right,
                                  convert_buffer_right, cmp_collation,
                                  charset_convert_buff))) {
                LOG_WARN("fail to convert string collation", K(ret),
                          K(read_buffer_right), K(collation_right),
                          K(convert_buffer_right), K(cmp_collation));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (read_buffer_left.length() == 0 && convert_buffer_right.length() == 0) {
            result = 0;
            ret = OB_ITER_END;
          } else if (read_buffer_left.length() == 0 && convert_buffer_right.length() > 0) {
            result = -1;
          } else if (read_buffer_left.length() > 0 && convert_buffer_right.length() == 0) {
            result = 1;
          } else {
            uint64_t cmp_len = read_buffer_left.length() > convert_buffer_right.length() ?
                                    convert_buffer_right.length() : read_buffer_left.length();
            ObString substr_lob_left;
            ObString substr_lob_right;
            substr_lob_left.assign_ptr(read_buffer_left.ptr(), cmp_len);
            substr_lob_right.assign_ptr(convert_buffer_right.ptr(), cmp_len);
            result = common::ObCharset::strcmp(cmp_collation, substr_lob_left, substr_lob_right);
            if (result > 0) {
              result = 1;
            } else if (result < 0) {
              result = -1;
            }

            read_buffer_left.assign_ptr(read_buffer_left.ptr() + cmp_len, read_buffer_left.length() - cmp_len);
            convert_buffer_right.assign_ptr(convert_buffer_right.ptr() + cmp_len, convert_buffer_right.length() - cmp_len);
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(read_buff)) {
      tmp_allocator->free(read_buff);
    }
    if (OB_NOT_NULL(charset_convert_buff_ptr)) {
      tmp_allocator->free(charset_convert_buff_ptr);
    }
  }
  if (OB_NOT_NULL(iter_left)) {
    iter_left->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_left);
  }
  if (OB_NOT_NULL(iter_right)) {
    iter_right->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_right);
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
  if (meta_info.char_len_ > meta_info.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(meta_info));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->write(param, meta_row))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(update_out_ctx(param, nullptr, meta_row))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else {
    param.lob_data_->byte_size_ += meta_row.byte_len_;
    param.byte_size_ = param.lob_data_->byte_size_;
    if (lob_handle_has_char_len(param)) {
      int64_t *len = get_char_len_ptr(param);
      *len = *len + meta_row.char_len_;
      OB_ASSERT(*len >= 0);
    }
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
  if (new_meta_info.char_len_ > new_meta_info.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(new_meta_info));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->update(param, old_meta_info, new_meta_info))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(update_out_ctx(param, &old_meta_info, new_meta_info))) {
    LOG_WARN("failed update checksum.", K(ret));
  } else {
    param.lob_data_->byte_size_ -= old_meta_info.byte_len_;
    param.lob_data_->byte_size_ += new_meta_info.byte_len_;
    if (lob_handle_has_char_len(param)) {
      int64_t *len = get_char_len_ptr(param);
      *len = *len - old_meta_info.char_len_;
      *len = *len + new_meta_info.char_len_;
      OB_ASSERT(*len >= 0);
    }
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
    if (lob_handle_has_char_len(param)) {
      int64_t *len = get_char_len_ptr(param);
      *len = *len - meta_info.char_len_;
      OB_ASSERT(*len >= 0);
    }
    param.byte_size_ = param.lob_data_->byte_size_;
  }

  return ret;
}

int ObLobManager::check_need_out_row(
    ObLobAccessParam& param,
    int64_t add_len,
    ObString &data,
    bool need_combine_data,
    bool alloc_inside,
    bool &need_out_row)
{
  int ret = OB_SUCCESS;
  need_out_row = (param.byte_size_ + add_len) > LOB_IN_ROW_MAX_LENGTH;
  if (param.lob_locator_ != nullptr) {
    // TODO @lhd remove after tmp lob support outrow
    if (!param.lob_locator_->is_persist_lob()) {
      need_out_row = false;
    }
  }
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
    if (need_combine_data) {
      if (param.byte_size_ > 0) {
        uint64_t total_size = param.byte_size_ + data.length();
        char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(total_size));
        } else {
          MEMCPY(buf, param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
          MEMCPY(buf + param.byte_size_, data.ptr(), data.length());
          data.assign_ptr(buf, total_size);
        }
      }
    } else {
      data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
    }

    // alloc full lob out row header
    if (OB_SUCC(ret)) {
      char *buf = static_cast<char*>(param.allocator_->alloc(LOB_OUTROW_FULL_SIZE));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret));
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
          if (alloc_inside) {
            param.allocator_->free(param.lob_common_);
          }
          param.lob_common_ = new_lob_common;
          param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
          // refresh in_row flag
          param.lob_common_->in_row_ = 0;
          // init out row ctx
          ObLobDataOutRowCtx *ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
          // init char len
          uint64_t *char_len = reinterpret_cast<uint64_t*>(ctx + 1);
          *char_len = 0;
          param.handle_size_ = LOB_OUTROW_FULL_SIZE;
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
  if (!param.seq_no_st_.is_valid()) {
    // pre-calc seq_no_cnt and init seq_no_st
    // for insert, most oper len/128K + 2
    // for erase, most oper len/128K + 2
    // for append, most oper len/256K + 1
    // for sql update, calc erase+insert
    int64_t N = ((len + param.update_len_) / (ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE / 2) + 2) * 2;
    param.seq_no_st_ = param.tx_desc_->get_and_inc_tx_seq(param.parent_seq_no_.get_branch(), N);
    param.used_seq_cnt_ = 0;
    param.total_seq_cnt_ = N;
  }
  if (OB_SUCC(ret)) {
    out_row_ctx->seq_no_st_ = param.seq_no_st_.cast_to_int();
    out_row_ctx->is_full_ = 1;
    out_row_ctx->offset_ = param.offset_;
    out_row_ctx->check_sum_ = param.checksum_;
    out_row_ctx->seq_no_cnt_ = param.used_seq_cnt_;
    out_row_ctx->del_seq_no_cnt_ = param.used_seq_cnt_; // for sql update, first delete then insert
    out_row_ctx->op_ = static_cast<uint8_t>(op);
    out_row_ctx->modified_len_ = len;
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
    ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else if (!lob.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else if (!lob.has_lob_header()) { // 4.0 text tc compatiable
    ObString data;
    data.assign_ptr(lob.ptr_, lob.size_);
    if (OB_FAIL(append(param, data))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret), K(param), K(data));
    }
  } else if (lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else if (lob.has_inrow_data()) {
    ObString data;
    if (OB_FAIL(lob.get_inrow_data(data))) {
      LOG_WARN("get inrow data int insert lob col failed", K(lob), K(data));
    } else if (OB_FAIL(append(param, data))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret), K(param), K(data));
    }
  } else {
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_FAIL(prepare_lob_common(param, alloc_inside))) {
      LOG_WARN("fail to prepare lob common", K(ret), K(param));
    }
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    int64_t append_lob_len = 0;
    ObString ori_inrow_data;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote append", K(ret), K(param));
    } else if (OB_FAIL(lob.get_lob_data_byte_len(append_lob_len))) {
      LOG_WARN("fail to get append lob byte len", K(ret), K(lob));
    } else if (OB_FAIL(check_need_out_row(param, append_lob_len, ori_inrow_data, false, alloc_inside, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(lob));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      int32_t ptr_offset = 0;
      if (OB_NOT_NULL(param.lob_locator_)) {
        ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
        cur_handle_size += ptr_offset;
      }
      uint64_t total_size = cur_handle_size + append_lob_len;
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        if (OB_NOT_NULL(param.lob_locator_)) {
          MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
        }
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
        MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
        ObString data;
        data.assign_buffer(buf + cur_handle_size, append_lob_len);
        SMART_VAR(ObLobAccessParam, read_param) {
          read_param.tx_desc_ = param.tx_desc_;
          read_param.tenant_id_ = param.src_tenant_id_;
          if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                      0, UINT64_MAX, param.timeout_, lob))) {
            LOG_WARN("fail to build read param", K(ret), K(lob));
          } else if (OB_FAIL(query(read_param, data))) {
            LOG_WARN("fail to read src lob", K(ret), K(read_param));
          }
        }
        if (OB_SUCC(ret)) {
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
          param.handle_size_ = total_size;
          if (OB_NOT_NULL(param.lob_locator_)) {
            param.lob_locator_->ptr_ = buf;
            param.lob_locator_->size_ = total_size;
            if (OB_FAIL(fill_lob_locator_extern(param))) {
              LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
            }
          }
        }
      }
    } else {
      // prepare out row ctx
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(init_out_row_ctx(param, append_lob_len, param.op_type_))) {
        LOG_WARN("init lob data out row ctx failed", K(ret));
      }
      // prepare write buffer
      ObString write_buffer;
      int64_t buf_len = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
      char *buf = nullptr;
      if (OB_SUCC(ret)) {
        buf = reinterpret_cast<char*>(param.allocator_->alloc(buf_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buffer failed.", K(buf_len));
        } else {
          write_buffer.assign_buffer(buf, buf_len);
        }
      }

      // scan get last info
      ObLobMetaScanIter meta_iter;
      bool use_buffer = false;
      bool do_update = false;
      ObLobMetaInfo scan_last_info;
      ObLobMetaInfo last_info;
      ObLobSeqId seq_id(param.allocator_);
      if (OB_SUCC(ret)) {
        bool need_init_info = false;
        if (ori_inrow_data.length() > 0) {
          // inrow to outrow, do not need scan, fill ori data
          need_init_info = true;
        } else {
          // do reverse scan
          param.scan_backward_ = true;
          if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter))) {
            LOG_WARN("do lob meta scan failed.", K(ret), K(param));
          } else {
            ObLobMetaScanResult scan_res;
            ret = meta_iter.get_next_row(scan_res);
            if (OB_FAIL(ret)) {
              if (ret == OB_ITER_END) {
                // empty table
                ret = OB_SUCCESS;
                need_init_info = true;
              } else {
                LOG_WARN("failed to get next row.", K(ret));
              }
            } else if (scan_res.info_.byte_len_ > ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE / 2) {
              need_init_info = true;
              seq_id.set_seq_id(scan_res.info_.seq_id_);
            } else {
              use_buffer = true;
              do_update = true;
              scan_last_info = scan_res.info_;
              last_info = scan_res.info_;
              MEMCPY(write_buffer.ptr(), scan_res.info_.lob_data_.ptr(), last_info.byte_len_);
              write_buffer.set_length(last_info.byte_len_);
              last_info.lob_data_ = write_buffer;
              seq_id.set_seq_id(scan_res.info_.seq_id_);
            }
          }
        }
        if (OB_SUCC(ret) && need_init_info) {
          use_buffer = (ori_inrow_data.length() > 0);
          last_info.lob_id_ = param.lob_data_->id_;
          last_info.piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
          if (ori_inrow_data.length() > 0) {
            last_info.byte_len_ = ori_inrow_data.length();
            last_info.char_len_ = ObCharset::strlen_char(param.coll_type_, ori_inrow_data.ptr(), ori_inrow_data.length());
            last_info.lob_data_ = write_buffer;
            MEMCPY(last_info.lob_data_.ptr(), ori_inrow_data.ptr(), ori_inrow_data.length());
            last_info.lob_data_.set_length(ori_inrow_data.length());
          } else {
            last_info.byte_len_ = 0;
            last_info.char_len_ = 0;
            last_info.lob_data_.reset();
          }
          if (OB_FAIL(seq_id.get_next_seq_id(last_info.seq_id_))) {
            LOG_WARN("failed to next seq id.", K(ret));
          }
        }
      }

      // refresh outrow ctx
      if (OB_SUCC(ret)) {
        if (last_info.byte_len_ > 0) {
          ObLobDataOutRowCtx *out_row_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(param.lob_data_->buffer_);
          out_row_ctx->first_meta_offset_ = last_info.byte_len_;
        }
      }

      // prepare read full lob
      if (OB_SUCC(ret)) {
        SMART_VAR(ObLobAccessParam, read_param) {
          read_param.tx_desc_ = param.tx_desc_;
          read_param.tenant_id_ = param.src_tenant_id_;
          if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                      0, UINT64_MAX, param.timeout_, lob))) {
            LOG_WARN("fail to build read param", K(ret), K(lob));
          } else {
            ObLobQueryIter *iter = nullptr;
            if (OB_FAIL(query(read_param, iter))) {
              LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
            } else {
              // prepare read buffer
              ObString read_buffer;
              uint64_t read_buff_size = LOB_READ_BUFFER_LEN;
              char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
              if (OB_ISNULL(read_buff)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
              }
              while (OB_SUCC(ret)) {
                read_buffer.assign_buffer(read_buff, read_buff_size);
                if (OB_FAIL(iter->get_next_row(read_buffer))) {
                  if (ret != OB_ITER_END) {
                    LOG_WARN("failed to get next buffer.", K(ret));
                  }
                } else {
                  int64_t read_offset = 0;
                  bool fill_full = false;
                  while (OB_SUCC(ret) && read_offset < read_buffer.length()) {
                    // fill data to last info
                    int64_t left_size = read_buffer.length() - read_offset;
                    int64_t by_len = 0;
                    int64_t char_len = 0;
                    if (use_buffer) {
                      fill_full = (left_size >= last_info.lob_data_.remain());
                      by_len = (last_info.lob_data_.remain() > left_size) ? left_size : last_info.lob_data_.remain();
                      by_len = ObCharset::max_bytes_charpos(param.coll_type_,
                                                            read_buffer.ptr() + read_offset,
                                                            read_buffer.length() - read_offset,
                                                            by_len,
                                                            char_len);
                      by_len = ob_lob_writer_length_validation(param.coll_type_, read_buffer.length() - read_offset, by_len, char_len);
                      if (last_info.lob_data_.write(read_buffer.ptr() + read_offset, by_len) != by_len) {
                        ret = OB_ERR_UNEXPECTED;
                        LOG_WARN("failed to write data to inner buffer", K(ret), K(last_info.lob_data_), K(by_len));
                      }
                    } else {
                      // only after reset can access here
                      OB_ASSERT(last_info.byte_len_ == 0);
                      if (left_size >= buf_len) {
                        // read left size is bigger than one lob meta size, do not need set
                        by_len = ObCharset::max_bytes_charpos(param.coll_type_,
                                                              read_buffer.ptr() + read_offset,
                                                              left_size,
                                                              buf_len,
                                                              char_len);
                        by_len = ob_lob_writer_length_validation(param.coll_type_, left_size, by_len, char_len);
                        last_info.lob_data_.assign_ptr(read_buffer.ptr() + read_offset, by_len);
                        fill_full = true;
                      } else {
                        use_buffer = true;
                        last_info.lob_data_.assign_buffer(buf, buf_len);
                        by_len = ObCharset::max_bytes_charpos(param.coll_type_,
                                                              read_buffer.ptr() + read_offset,
                                                              left_size,
                                                              left_size,
                                                              char_len);
                        by_len = ob_lob_writer_length_validation(param.coll_type_, left_size, by_len, char_len);
                        if (last_info.lob_data_.write(read_buffer.ptr() + read_offset, by_len) != by_len) {
                          ret = OB_ERR_UNEXPECTED;
                          LOG_WARN("failed to write data to inner buffer", K(ret), K(last_info.lob_data_), K(by_len));
                        }
                      }
                    }
                    // update last info and offset
                    if (OB_SUCC(ret)) {
                      last_info.byte_len_ += by_len;
                      last_info.char_len_ += char_len;
                      read_offset += by_len;
                    }
                    // if fill full, do update or write
                    if (OB_SUCC(ret) && fill_full) {
                      if (do_update) {
                        ObLobPieceInfo piece_info;
                        if (OB_FAIL(update_one_piece(param,
                                                     lob_ctx,
                                                     scan_last_info,
                                                     last_info,
                                                     piece_info,
                                                     last_info.lob_data_))) {
                          LOG_WARN("failed to update.", K(ret), K(last_info), K(scan_last_info));
                        } else {
                          do_update = false;
                        }
                      } else {
                        common::ObTabletID piece_tablet_id; // TODO get piece tablet id
                        if (OB_FAIL(write_one_piece(param,
                                                    piece_tablet_id,
                                                    lob_ctx,
                                                    last_info,
                                                    last_info.lob_data_,
                                                    false))) {
                          LOG_WARN("failed write data.", K(ret), K(last_info));
                        }
                      }
                      if (OB_SUCC(ret)) {
                        // reset last_info
                        last_info.byte_len_ = 0;
                        last_info.char_len_ = 0;
                        last_info.lob_data_.reset();
                        use_buffer = false;
                        fill_full = false;
                        if (OB_FAIL(seq_id.get_next_seq_id(last_info.seq_id_))) {
                          LOG_WARN("failed to next seq id.", K(ret));
                        }
                      }
                    }
                  }
                }
              }
              if (ret == OB_ITER_END) {
                ret = OB_SUCCESS;
              }
              if (OB_SUCC(ret) && last_info.byte_len_ > 0) {
                // do update or write
                if (do_update) {
                  ObLobPieceInfo piece_info;
                  if (OB_FAIL(update_one_piece(param,
                                                lob_ctx,
                                                scan_last_info,
                                                last_info,
                                                piece_info,
                                                last_info.lob_data_))) {
                    LOG_WARN("failed to update.", K(ret), K(last_info), K(scan_last_info));
                  }
                } else {
                  common::ObTabletID piece_tablet_id;
                  if (OB_FAIL(write_one_piece(param,
                                              piece_tablet_id,
                                              lob_ctx,
                                              last_info,
                                              last_info.lob_data_,
                                              false))) {
                    LOG_WARN("failed write data.", K(ret), K(last_info));
                  }
                }
              }
              if (OB_NOT_NULL(read_buff)) {
                param.allocator_->free(read_buff);
              }
            }
            if (OB_NOT_NULL(iter)) {
              iter->reset();
              OB_DELETE(ObLobQueryIter, "unused", iter);
            }
          }
        }
      }
      if (OB_NOT_NULL(buf)) {
        param.allocator_->free(buf);
      }
    }
  }
  return ret;
}

int ObLobManager::prepare_lob_common(ObLobAccessParam& param, bool &alloc_inside)
{
  int ret = OB_SUCCESS;
  alloc_inside = false;
  if (OB_ISNULL(param.lob_common_)) {
    // alloc new lob_data
    void *tbuf = param.allocator_->alloc(LOB_OUTROW_FULL_SIZE);
    if (OB_ISNULL(tbuf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for LobData", K(ret));
    } else {
      // init full out row
      param.lob_common_ = new(tbuf)ObLobCommon();
      param.lob_data_ = new(param.lob_common_->buffer_)ObLobData();
      param.lob_data_->id_.tablet_id_ = param.tablet_id_.id();
      ObLobDataOutRowCtx *outrow_ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
      // init char len
      uint64_t *char_len = reinterpret_cast<uint64_t*>(outrow_ctx + 1);
      *char_len = 0;
      param.handle_size_ = LOB_OUTROW_FULL_SIZE;
      alloc_inside = true;
    }
  } else if (param.lob_common_->is_init_) {
    param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
  }
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
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_FAIL(prepare_lob_common(param, alloc_inside))) {
      LOG_WARN("fail to prepare lob common", K(ret), K(param));
    }
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote append", K(ret), K(param));
    } else if (OB_FAIL(check_need_out_row(param, data.length(), data, true, alloc_inside, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(data));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      int32_t ptr_offset = 0;
      if (OB_NOT_NULL(param.lob_locator_)) {
        ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
        cur_handle_size += ptr_offset;
      }
      uint64_t total_size = cur_handle_size + data.length();
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        if (OB_NOT_NULL(param.lob_locator_)) {
          MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
        }
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
        MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
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
        param.handle_size_ = total_size;
        if (OB_NOT_NULL(param.lob_locator_)) {
          param.lob_locator_->ptr_ = buf;
          param.lob_locator_->size_ = total_size;
          if (OB_FAIL(fill_lob_locator_extern(param))) {
            LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
          }
        }
      }
    } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
    } else { // outrow
      ObLobMetaWriteIter iter(data, param.allocator_, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(init_out_row_ctx(param, data.length(), param.op_type_))) {
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
            if (result.is_update_) {
              ObLobPieceInfo piece_info;
              if (OB_FAIL(update_one_piece(param,
                                            lob_ctx,
                                            result.old_info_,
                                            result.info_,
                                            piece_info,
                                            result.data_))) {
                LOG_WARN("failed to update.", K(ret), K(result.info_), K(result.old_info_));
              }
            } else {
              common::ObTabletID piece_tablet_id; // TODO get piece tablet id
              if (OB_FAIL(write_one_piece(param,
                                          piece_tablet_id,
                                          lob_ctx,
                                          result.info_,
                                          result.data_,
                                          result.need_alloc_macro_id_))) {
                LOG_WARN("failed write data.", K(ret), K(result.info_));
              }
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

int ObLobManager::prepare_for_write(
    ObLobAccessParam& param,
    ObString &old_data,
    bool &need_out_row)
{
  int ret = OB_SUCCESS;
  int64_t max_bytes_in_char = 4;
  uint64_t modified_end = param.offset_ + param.len_;
  if (param.coll_type_ != CS_TYPE_BINARY) {
    modified_end *= max_bytes_in_char;
  }
  uint64_t total_size = param.byte_size_ > modified_end ? param.byte_size_ : modified_end;
  need_out_row = (total_size > LOB_IN_ROW_MAX_LENGTH);
  if (param.lob_common_->in_row_) {
    old_data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
  }
  if (param.lob_locator_ != nullptr) {
    // @lhd remove after tmp lob support outrow
    if (!param.lob_locator_->is_persist_lob()) {
      need_out_row = false;
    }
  }
  // in_row : 0 | need_out_row : 0  --> invalid
  // in_row : 0 | need_out_row : 1  --> do nothing, keep out_row
  // in_row : 1 | need_out_row : 0  --> do nothing, keep in_row
  // in_row : 1 | need_out_row : 1  --> in_row to out_row
  if (!param.lob_common_->in_row_ && !need_out_row) {
    if (!param.lob_common_->is_init_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob data", K(ret), KPC(param.lob_common_));
    } else {
      need_out_row = true;
    }
  } else if (param.lob_common_->in_row_ && need_out_row) {
    // alloc full lob out row header
    if (OB_SUCC(ret)) {
      char* buf = static_cast<char*>(param.allocator_->alloc(LOB_OUTROW_FULL_SIZE));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        MEMCPY(buf, param.lob_common_, sizeof(ObLobCommon));
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        new_lob_common->in_row_ = 0;
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
          // init char len
          uint64_t *char_len = reinterpret_cast<uint64_t*>(ctx + 1);
          *char_len = 0;
          param.handle_size_ = LOB_OUTROW_FULL_SIZE;
        }
      }
    }
  }
  return ret;
}

int ObLobManager::process_delta(ObLobAccessParam& param, ObLobLocatorV2& lob_locator)
{
  int ret = OB_SUCCESS;
  if (lob_locator.is_delta_temp_lob()) {
    ObString data;
    ObLobCommon *lob_common = nullptr;
    if (OB_FAIL(lob_locator.get_disk_locator(lob_common))) {
      LOG_WARN("get disk locator failed.", K(ret), K(lob_locator));
    } else if (!lob_common->in_row_) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport out row delta tmp lob locator", K(ret), KPC(lob_common));
    } else {
      ObLobDiffHeader *diff_header = reinterpret_cast<ObLobDiffHeader*>(lob_common->buffer_);
      if (param.lob_common_ == nullptr) {
        ObLobCommon *persis_lob = diff_header->get_persist_lob();
        param.lob_locator_ = nullptr;
        param.lob_common_ = persis_lob;
        param.handle_size_ = diff_header->persist_loc_size_;
        param.byte_size_ = persis_lob->get_byte_size(param.handle_size_);
      }
      ObLobDiff *diffs = diff_header->get_diff_ptr();
      char *data_ptr = diff_header->get_inline_data_ptr();
      // process diffs
      for (int64_t i = 0 ; OB_SUCC(ret) && i < diff_header->diff_cnt_; ++i) {
        ObString tmp_data(diffs[i].byte_len_, data_ptr + diffs[i].offset_);
        param.offset_ = diffs[i].ori_offset_;
        switch (diffs[i].type_) {
          case ObLobDiff::DiffType::APPEND: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::APPEND;
            param.len_ = diffs[i].ori_len_;
            ObLobLocatorV2 src_lob(tmp_data);
            if (OB_FAIL(append(param, src_lob))) {
              LOG_WARN("failed to do lob append", K(ret), K(param), K(src_lob));
            }
            if (ret == OB_SNAPSHOT_DISCARDED && src_lob.is_persist_lob()) {
              ret = OB_ERR_LOB_SPAN_TRANSACTION;
              LOG_WARN("fail to read src lob, make update inner sql do not retry", K(ret));
            }
            break;
          }
          case ObLobDiff::DiffType::WRITE: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::WRITE;
            param.len_ = diffs[i].ori_len_;
            bool can_do_append = false;
            if (diffs[i].flags_.can_do_append_) {
              if (lob_handle_has_char_len(param)) {
                int64_t *len = get_char_len_ptr(param);
                if (*len == param.offset_) {
                  can_do_append = true;
                  param.offset_ = 0;
                }
              }
            }

            ObLobLocatorV2 src_lob(tmp_data);
            if (can_do_append) {
              if (OB_FAIL(append(param, src_lob))) {
                LOG_WARN("failed to do lob append", K(ret), K(param), K(src_lob));
              }
            } else {
              if (OB_FAIL(write(param, src_lob, diffs[i].dst_offset_))) {
                LOG_WARN("failed to do lob write", K(ret), K(param), K(src_lob));
              }
            }
            if (ret == OB_SNAPSHOT_DISCARDED && src_lob.is_persist_lob()) {
              ret = OB_ERR_LOB_SPAN_TRANSACTION;
              LOG_WARN("fail to read src lob, make update inner sql do not retry", K(ret));
            }
            break;
          }
          case ObLobDiff::DiffType::ERASE: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::ERASE;
            param.len_ = diffs[i].ori_len_;
            if (OB_FAIL(erase(param))) {
              LOG_WARN("failed to do lob erase", K(ret), K(param));
            }
            break;
          }
          case ObLobDiff::DiffType::ERASE_FILL_ZERO: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::WRITE;
            param.len_ = diffs[i].ori_len_;
            param.is_fill_zero_ = true;
            if (OB_FAIL(erase(param))) {
              LOG_WARN("failed to do lob erase", K(ret), K(param));
            }
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid diff type", K(ret), K(i), K(diffs[i]));
          }
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator type", K(ret), K(lob_locator));
  }
  return ret;
}

int ObLobManager::getlength_remote(ObLobAccessParam& param, common::ObAddr& dst_addr, uint64_t &len)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> handle;
  common::ObDataBuffer rpc_buffer;
  ObLobQueryBlock header;
  char *buf = nullptr;
  int64_t buffer_len = header.get_serialize_size();
  if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob locator is null.", K(ret), K(param));
  } else if (OB_ISNULL(buf = static_cast<char*>(param.allocator_->alloc(buffer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed alloc buffer.", K(ret), K(buffer_len));
  } else if (!rpc_buffer.set_data(buf, buffer_len)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret), K(buffer_len));
  } else {
    SMART_VAR(ObLobQueryArg, arg) {
      // build arg
      arg.tenant_id_ = param.tenant_id_;
      arg.offset_ = param.offset_;
      arg.len_ = param.len_;
      arg.cs_type_ = param.coll_type_;
      arg.scan_backward_ = param.scan_backward_;
      arg.qtype_ = ObLobQueryArg::QueryType::GET_LENGTH;
      arg.lob_locator_.ptr_ = param.lob_locator_->ptr_;
      arg.lob_locator_.size_ = param.lob_locator_->size_;
      arg.lob_locator_.has_lob_header_ = param.lob_locator_->has_lob_header_;
      int64_t timeout = param.timeout_ - ObTimeUtility::current_time();
      if (timeout < ObStorageRpcProxy::STREAM_RPC_TIMEOUT) {
        timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
      }
      if (OB_FAIL(lob_remote_query_with_retry(param, dst_addr, arg, timeout, rpc_buffer, handle))) {
        LOG_WARN("failed to do remote query", K(ret), K(arg));
      } else {
        int64_t cur_position = rpc_buffer.get_position();
        while (OB_SUCC(ret) && handle.has_more()) {
          cur_position = rpc_buffer.get_position();
          if (OB_FAIL(handle.get_more(rpc_buffer))) {
            ret = OB_DATA_SOURCE_TIMEOUT;
          } else if (rpc_buffer.get_position() < 0) {
            ret = OB_ERR_SYS;
          } else if (cur_position == rpc_buffer.get_position()) {
            if (!handle.has_more()) {
              ret = OB_ITER_END;
              LOG_DEBUG("empty rpc buffer, no more data", K(rpc_buffer));
            } else {
              ret = OB_ERR_SYS;
              LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
            }
          } else {
            LOG_DEBUG("get more data", K(rpc_buffer));
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
        // do header decode
        if (OB_SUCC(ret)) {
          int64_t rpc_buffer_pos = 0;
          if (OB_FAIL(serialization::decode(rpc_buffer.get_data(),
            rpc_buffer.get_position(), rpc_buffer_pos, header))) {
            LOG_WARN("failed to decode lob query block", K(ret), K(rpc_buffer));
          } else if (!header.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid header", K(ret), K(header));
          } else {
            len = static_cast<uint64_t>(header.size_);
          }
        }
      }
    }
  }
  return ret;
}

bool ObLobManager::lob_handle_has_char_len(ObLobAccessParam& param)
{
  return (param.lob_common_ != nullptr && !param.lob_common_->in_row_ &&
          param.handle_size_ >= LOB_OUTROW_FULL_SIZE);
}

int64_t* ObLobManager::get_char_len_ptr(ObLobAccessParam& param)
{
  char *ptr = reinterpret_cast<char*>(param.lob_common_);
  return reinterpret_cast<int64_t*>(ptr + LOB_WITH_OUTROW_CTX_SIZE);
}

int ObLobManager::fill_lob_locator_extern(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(param.lob_locator_)) {
    if (param.lob_locator_->has_extern()) {
      ObMemLobExternHeader *ext_header = nullptr;
      if (OB_FAIL(param.lob_locator_->get_extern_header(ext_header))) {
        LOG_WARN("get extern header failed", K(ret), KPC(param.lob_locator_));
      } else {
        ext_header->payload_size_ = param.byte_size_;
      }
    }
  }
  return ret;
}

int ObLobManager::getlength(ObLobAccessParam& param, uint64_t &len)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (!is_char) { // return byte len
      len = lob_common->get_byte_size(param.handle_size_);
    } else if (lob_handle_has_char_len(param)) {
      len = *get_char_len_ptr(param);
    } else if (lob_common->in_row_ || // calc char len
               (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data())) {
      ObString data;
      if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
        if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
          LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
        }
      } else {
        if (lob_common->is_init_) {
          param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
          data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
        } else {
          data.assign_ptr(lob_common->buffer_, param.byte_size_);
        }
      }
      if (OB_SUCC(ret)) {
        len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length());
      }
    } else { // do meta scan
      bool is_remote_lob = false;
      common::ObAddr dst_addr;
      if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
        LOG_WARN("check is remote failed.", K(ret), K(param));
      } else if (is_remote_lob) {
        if (OB_FAIL(getlength_remote(param, dst_addr, len))) {
          LOG_WARN("fail to get length remote", K(ret));
        }
      } else {
        ObLobMetaScanIter meta_iter;
        ObLobCtx lob_ctx = lob_ctx_;
        if (!lob_common->is_init_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob common header for out row.", K(ret), KPC(lob_common));
        } else {
          param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
          // mock do full scan
          param.offset_ = 0;
          param.len_ = UINT64_MAX;
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
              } else {
                len += result.meta_result_.info_.char_len_;
              }
            }
            if (ret == OB_ITER_END) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write_inrow_inner(ObLobAccessParam& param, ObString& data, ObString& old_data)
{
  int ret = OB_SUCCESS;
  ObLobCommon *lob_common = param.lob_common_;
  int64_t cur_handle_size = lob_common->get_handle_size(param.byte_size_) - param.byte_size_;
  int64_t ptr_offset = 0;
  if (OB_NOT_NULL(param.lob_locator_)) {
    ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
    cur_handle_size += ptr_offset;
  }
  int64_t lob_cur_mb_len = ObCharset::strlen_char(param.coll_type_, lob_common->get_inrow_data_ptr(), param.byte_size_);
  int64_t offset_byte_len = 0;
  int64_t amount_byte_len = 0;
  int64_t lob_replaced_byte_len = 0;
  int64_t res_len = 0;
  if (param.offset_ >= lob_cur_mb_len) {
    offset_byte_len = param.byte_size_ + (param.offset_ - lob_cur_mb_len);
    amount_byte_len = ObCharset::charpos(param.coll_type_, data.ptr(), data.length(), param.len_);
    res_len = offset_byte_len + amount_byte_len;
  } else {
    offset_byte_len = ObCharset::charpos(param.coll_type_,
                                          old_data.ptr(),
                                          old_data.length(),
                                          param.offset_);
    amount_byte_len = ObCharset::charpos(param.coll_type_, data.ptr(), data.length(), param.len_);
    lob_replaced_byte_len = ObCharset::charpos(param.coll_type_,
                                                old_data.ptr() + offset_byte_len,
                                                old_data.length() - offset_byte_len,
                                                (param.len_ + param.offset_ > lob_cur_mb_len) ? (lob_cur_mb_len - param.offset_) : param.len_);
    res_len = old_data.length() - lob_replaced_byte_len + amount_byte_len;
  }

  res_len += cur_handle_size;
  char *buf = static_cast<char*>(param.allocator_->alloc(res_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf failed.", K(ret), K(res_len));
  } else {
    ObString space = ObCharsetUtils::get_const_str(param.coll_type_, ' ');
    if (param.coll_type_ == CS_TYPE_BINARY) {
      MEMSET(buf, 0x00, res_len);
    } else {
      uint32_t space_len = space.length();
      if (res_len%space_len != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid res-len", K(ret), K(res_len), K(space_len));
      } else if (space_len > 1) {
        for (int i = 0; i < res_len/space_len; i++) {
          MEMCPY(buf + i * space_len, space.ptr(), space_len);
        }
      } else {
        MEMSET(buf, *space.ptr(), res_len);
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_NOT_NULL(param.lob_locator_)) {
        MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
      }
      ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
      MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
      char* new_data_ptr = const_cast<char*>(new_lob_common->get_inrow_data_ptr());
      if (offset_byte_len >= old_data.length()) {
        MEMCPY(new_data_ptr, old_data.ptr(), old_data.length());
        MEMCPY(new_data_ptr + offset_byte_len, data.ptr(), amount_byte_len);
      } else {
        MEMCPY(new_data_ptr, old_data.ptr(), offset_byte_len);
        MEMCPY(new_data_ptr + offset_byte_len, data.ptr(), amount_byte_len);
        if (offset_byte_len + amount_byte_len < old_data.length()) {
          MEMCPY(new_data_ptr + offset_byte_len + amount_byte_len,
                  old_data.ptr() + offset_byte_len + lob_replaced_byte_len,
                  old_data.length() - offset_byte_len - lob_replaced_byte_len);
        }
      }

      // refresh lob info
      param.byte_size_ = res_len - cur_handle_size;
      if (new_lob_common->is_init_) {
        ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
        new_lob_data->byte_size_ = res_len - cur_handle_size;
      }
      param.lob_common_ = new_lob_common;
      param.handle_size_ = res_len;
      if (OB_NOT_NULL(param.lob_locator_)) {
        param.lob_locator_->ptr_ = buf;
        param.lob_locator_->size_ = res_len;
        if (OB_FAIL(fill_lob_locator_extern(param))) {
          LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write_inrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObLobAccessParam, read_param) {
    read_param.tx_desc_ = param.tx_desc_;
    if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                offset, param.len_, param.timeout_, lob))) {
      LOG_WARN("fail to build read param", K(ret), K(lob));
    } else {
      ObLobQueryIter *iter = nullptr;
      if (OB_FAIL(query(read_param, iter))) {
        LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
      } else {
        // prepare read buffer
        ObString read_buffer;
        uint64_t read_buff_size = LOB_READ_BUFFER_LEN;
        char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
        if (OB_ISNULL(read_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
        } else {
          read_buffer.assign_buffer(read_buff, read_buff_size);
        }

        uint64_t write_offset = param.offset_;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter->get_next_row(read_buffer))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer.", K(ret));
            }
          } else {
            param.offset_ = write_offset;
            uint64_t read_char_len = ObCharset::strlen_char(param.coll_type_, read_buffer.ptr(), read_buffer.length());
            param.len_ = read_char_len;
            if (OB_FAIL(write_inrow_inner(param, read_buffer, old_data))) {
              LOG_WARN("failed to do write", K(ret), K(param));
            } else {
              // update offset and len
              write_offset += read_char_len;
              old_data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
            }
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
        if (OB_NOT_NULL(read_buff)) {
          param.allocator_->free(read_buff);
        }
      }
      if (OB_NOT_NULL(iter)) {
        iter->reset();
        OB_DELETE(ObLobQueryIter, "unused", iter);
      }
    }
  }
  return ret;
}

int ObLobManager::prepare_write_buffers(ObLobAccessParam& param, ObString &remain_buf, ObString &tmp_buf)
{
  int ret = OB_SUCCESS;
  // prepare remain buf
  char *buf = reinterpret_cast<char*>(param.allocator_->alloc(ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf failed.", K(ret));
  } else {
    remain_buf.assign_buffer(buf, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
  }

  // prepare tmp buf
  if (OB_FAIL(ret)) {
  } else {
    buf = reinterpret_cast<char*>(param.allocator_->alloc(ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buf failed.", K(ret));
    } else {
      tmp_buf.assign_buffer(buf, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
    }
  }
  return ret;
}

int ObLobManager::write_outrow_result(ObLobAccessParam& param, ObLobMetaWriteIter &write_iter)
{
  int ret = OB_SUCCESS;
  ObLobMetaWriteResult result;
  int cnt = 0;
  while (OB_SUCC(ret)) {
    // split append data into data pieces 250k/piece
    ret = write_iter.get_next_row(result);
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
      cnt++;
      // get len and pos from meta_info
      common::ObTabletID piece_tablet_id; // TODO get piece tablet id
      if (OB_FAIL(write_one_piece(param,
                                  piece_tablet_id,
                                  lob_ctx_,
                                  result.info_,
                                  result.data_,
                                  result.need_alloc_macro_id_))) {
        LOG_WARN("failed write data.", K(ret), K(cnt), K(result.info_));
      }
    }
  }
  return ret;
}

int ObLobManager::write_outrow_inner(ObLobAccessParam& param, ObLobQueryIter *iter, ObString& read_buf, ObString& old_data)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObLobMetaScanIter, meta_iter) {
    uint64_t modified_len = param.len_;
    int64_t mbmaxlen = 1;
    if (param.coll_type_ != CS_TYPE_BINARY) {
      if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(param.coll_type_, mbmaxlen))) {
        LOG_WARN("fail to get mbmaxlen", K(ret), K(param.coll_type_));
      } else {
        modified_len *= mbmaxlen;
      }
    }

    // consider offset is bigger than char len, add padding size modified len
    int64_t least_char_len = param.byte_size_ / mbmaxlen;
    if (lob_handle_has_char_len(param)) {
      least_char_len = *get_char_len_ptr(param);
    }
    if (param.offset_ > least_char_len) {
      modified_len += (param.offset_ - least_char_len);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_out_row_ctx(param, modified_len + old_data.length(), param.op_type_))) {
      LOG_WARN("init lob data out row ctx failed", K(ret));
    } else {
      bool found_begin = false;
      bool found_end = false;
      ObLobMetaInfo range_begin;
      ObLobMetaInfo range_end;
      ObString post_data;
      ObString remain_buf;
      ObString tmp_buf; // use for read piece data in replace_process_meta_info
      uint64_t padding_size = 0;
      uint64_t pos = 0;
      if (old_data.length() == 0) {
        if (param.scan_backward_) {
          LOG_INFO("param scan_backward is true. Make it be false.", K(param));
          param.scan_backward_ = false;
        }
        if (OB_FAIL(prepare_write_buffers(param, remain_buf, tmp_buf))) {
          LOG_WARN("fail to prepare buffers", K(ret));
        } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->scan(param, meta_iter))) {
          LOG_WARN("do lob meta scan failed.", K(ret), K(param));
        } else {
          // 1. do replace and get range begin and range end when old data out row
          ObLobQueryResult result;
          while (OB_SUCC(ret)) {
            ret = meta_iter.get_next_row(result.meta_result_);
            if (OB_FAIL(ret)) {
              if (ret != OB_ITER_END) {
                LOG_WARN("failed to get next row.", K(ret));
              }
            } else if (ObTimeUtility::current_time() > param.timeout_) {
              ret = OB_TIMEOUT;
              int64_t cur_time = ObTimeUtility::current_time();
              LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
            } else {
              if (meta_iter.is_range_begin(result.meta_result_.info_)) {
                if (OB_FAIL(range_begin.deep_copy(*param.allocator_, result.meta_result_.info_))) {
                  LOG_WARN("deep copy meta info failed", K(ret), K(meta_iter));
                } else {
                  found_begin = true;
                }
              }
              if (OB_SUCC(ret) && meta_iter.is_range_end(result.meta_result_.info_)) {
                if (OB_FAIL(range_end.deep_copy(*param.allocator_, result.meta_result_.info_))) {
                  LOG_WARN("deep copy meta info failed", K(ret), K(meta_iter));
                } else {
                  found_end = true;
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(replace_process_meta_info(param, meta_iter, result, iter, read_buf, remain_buf, tmp_buf))) {
                LOG_WARN("process erase meta info failed.", K(ret), K(param), K(result));
              }
            }
          }
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
          }
        }
      } else {
        // process inrow to outrow
        int64_t old_char_len = ObCharset::strlen_char(param.coll_type_, old_data.ptr(), old_data.length());
        if (param.offset_ > old_char_len) {
          // calc padding size
          padding_size = param.offset_ - old_char_len;
          // do append => [old_data][padding][data]
          post_data = old_data;
        } else {
          // combine data and old data
          // [old_data][data]
          int64_t offset_byte_len = ObCharset::charpos(param.coll_type_,
                                                       old_data.ptr(),
                                                       old_data.length(),
                                                       param.offset_);
          post_data.assign_ptr(old_data.ptr(), offset_byte_len);
        }
      }

      // insert situation for range begin and end
      // found_begin  found end  => result
      // true         true          do range insert, seq_id in [end, next]
      // false        false         do padding and append in [end, max]
      // true         false         do range append, seq_id in [end, max]
      // other situations are invalid
      uint32_t inrow_st = 0;
      ObString seq_id_st, seq_id_ed;
      if (old_data.length() > 0) {
        // inrow to outrow, set st 0, set ed null
        seq_id_st.assign_ptr(reinterpret_cast<char*>(&inrow_st), sizeof(uint32_t));
        seq_id_ed.assign_ptr(nullptr, 0);
      } else if (found_begin && found_end) {
        seq_id_st = range_end.seq_id_;
        seq_id_ed = meta_iter.get_cur_info().seq_id_;
        if (seq_id_ed.compare(seq_id_st) == 0) {
          // only found one and this is the last lob meta, just set end to max
          seq_id_ed.assign_ptr(nullptr, 0);
        }
      } else if (found_begin && !found_end) {
        seq_id_st = meta_iter.get_cur_info().seq_id_;
        seq_id_ed.assign_ptr(nullptr, 0);
      } else if (!found_begin && !found_end) {
        uint64_t total_char_len = meta_iter.get_cur_pos();
        padding_size = param.offset_ - total_char_len;
        seq_id_st = meta_iter.get_cur_info().seq_id_;
        seq_id_ed.assign_ptr(nullptr, 0);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown state for range.", K(ret), K(found_begin), K(found_end));
      }

      if (OB_SUCC(ret)) {
        // prepare write iter
        ObLobMetaWriteIter write_iter(read_buf, param.allocator_, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
        if (OB_FAIL(write_iter.open(param, iter, read_buf, padding_size, post_data, remain_buf, seq_id_st, seq_id_ed))) {
          LOG_WARN("failed to open meta writer", K(ret), K(write_iter), K(meta_iter), K(found_begin), K(found_end),
                   K(range_begin), K(range_end));
        } else if (OB_FAIL(write_outrow_result(param, write_iter))) {
          LOG_WARN("failed to write outrow result", K(ret), K(write_iter), K(meta_iter), K(found_begin), K(found_end),
                   K(range_begin), K(range_end));
        }
        write_iter.close();
      }
    }
  }
  return ret;
}

int ObLobManager::write_outrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObLobAccessParam, read_param) {
    read_param.tx_desc_ = param.tx_desc_;
    if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                offset, param.len_, param.timeout_, lob))) {
      LOG_WARN("fail to build read param", K(ret), K(lob));
    } else {
      ObLobQueryIter *iter = nullptr;
      if (OB_FAIL(query(read_param, iter))) {
        LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
      } else {
        // prepare read buffer
        ObString read_buffer;
        uint64_t read_buff_size = LOB_READ_BUFFER_LEN;
        char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
        if (OB_ISNULL(read_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
        } else if (FALSE_IT(read_buffer.assign_buffer(read_buff, read_buff_size))) {
        } else if (OB_FAIL(write_outrow_inner(param, iter, read_buffer, old_data))) {
          LOG_WARN("fail to do write outrow inner", K(ret), K(param));
        }

        if (OB_NOT_NULL(read_buff)) {
          param.allocator_->free(read_buff);
        }
      }
      if (OB_NOT_NULL(iter)) {
        iter->reset();
        OB_DELETE(ObLobQueryIter, "unused", iter);
      }
    }
  }
  return ret;
}

int ObLobManager::write(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("null lob common", K(ret), K(param));
    } else if (lob_common->is_init_) {
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
    }
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote write", K(ret), K(param));
    } else {
      ObString old_data;
      bool out_row = false;
      if (OB_FAIL(prepare_for_write(param, old_data, out_row))) {
        LOG_WARN("prepare for write failed.", K(ret));
      } else {
        if (!out_row) {
          if (OB_FAIL(write_inrow(param, lob, offset, old_data))) {
            LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(offset));
          }
        } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
          ret = OB_NOT_IMPLEMENT;
          LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
        } else {
          if (OB_FAIL(write_outrow(param, lob, offset, old_data))) {
            LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(offset));
          }
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write(ObLobAccessParam& param, ObString& data)
{
  int ret = OB_SUCCESS;
  bool save_is_reverse = param.scan_backward_;
  uint64_t save_param_len = param.len_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("null lob common", K(ret), K(param));
    } else if (lob_common->is_init_) {
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
    }
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote write", K(ret), K(param));
    } else {
      ObString old_data;
      bool out_row = false;
      if (OB_FAIL(prepare_for_write(param, old_data, out_row))) {
        LOG_WARN("prepare for write failed.", K(ret));
      } else if (!out_row) {
        if (OB_FAIL(write_inrow_inner(param, data, old_data))) {
          LOG_WARN("fail to write inrow inner", K(param), K(data));
        }
      } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
      } else {
        ObLobQueryIter *iter = nullptr;
        // prepare read buffer
        ObString read_buffer;
        uint64_t read_buff_size = LOB_READ_BUFFER_LEN;
        char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
        if (OB_ISNULL(read_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
        } else if (FALSE_IT(read_buffer.assign_buffer(read_buff, read_buff_size))) {
        } else if (OB_FAIL(query_inrow_get_iter(param, data, 0, false, iter))) {
          LOG_WARN("fail to get query iter", K(param), K(data));
        } else if (OB_FAIL(write_outrow_inner(param, iter, read_buffer, old_data))) {
          LOG_WARN("fail to write outrow", K(ret), K(param));
        }
        if (OB_NOT_NULL(read_buff)) {
          param.allocator_->free(read_buff);
        }
        if (OB_NOT_NULL(iter)) {
          iter->reset();
          OB_DELETE(ObLobQueryIter, "unused", iter);
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

int ObLobManager::replace_process_meta_info(ObLobAccessParam& param,
                                            ObLobMetaScanIter &meta_iter,
                                            ObLobQueryResult &result,
                                            ObLobQueryIter *iter,
                                            ObString& read_buf,
                                            ObString &remain_data,
                                            ObString &tmp_buf)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  bool del_piece = false;
  if (meta_iter.is_range_begin(result.meta_result_.info_) ||
      meta_iter.is_range_end(result.meta_result_.info_)) {
    // 1. read data
    // 2. rebuild data
    // 3. write data into lob data oper
    // 4. write meta tablet
    // 5. write piece tablet
    ObLobMetaInfo new_meta_row = result.meta_result_.info_;
    ObString read_data;
    read_data.assign_buffer(tmp_buf.ptr(), tmp_buf.size());

    // save variable
    uint32_t tmp_st = result.meta_result_.st_;
    uint32_t tmp_len = result.meta_result_.len_;

    // read all piece data
    result.meta_result_.st_ = 0;
    result.meta_result_.len_ = result.meta_result_.info_.char_len_;
    if (OB_FAIL(get_real_data(param, result, read_data))) {
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
      uint32_t local_end = param.offset_ + param.len_ - cur_piece_begin;

      // char len
      uint32_t piece_byte_len = result.meta_result_.info_.byte_len_;
      uint32_t piece_char_len = result.meta_result_.info_.char_len_;

      if (OB_FAIL(ret)) {
      } else if (meta_iter.is_range_begin(result.meta_result_.info_) &&
                 meta_iter.is_range_end(result.meta_result_.info_)) {
        uint32_t replace_char_st = local_begin;
        uint32_t replace_char_len = local_end - local_begin;
        uint32_t replace_byte_st = replace_char_st;
        uint32_t replace_byte_len = replace_char_len;
        if (is_char) {
          transform_query_result_charset(param.coll_type_,
                                         read_data.ptr(),
                                         read_data.length(),
                                         replace_byte_len,
                                         replace_byte_st);
        }
        ObString temp_read_buf;
        uint32_t temp_read_size = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE - replace_byte_st;
        temp_read_buf.assign_buffer(read_buf.ptr(), temp_read_size);
        if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
          LOG_WARN("fail to do get next read buffer", K(ret));
        } else if (piece_byte_len - replace_byte_len + temp_read_buf.length() <= ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE) {
          // after replace, can store in one this meta
          if (temp_read_buf.length() != replace_byte_len) {
            uint32_t move_len = piece_byte_len - (replace_byte_st + replace_byte_len);
            MEMMOVE(read_data.ptr() + replace_byte_st + temp_read_buf.length(), read_data.ptr() + replace_byte_st + replace_byte_len, move_len);
          }
          MEMCPY(read_data.ptr() + replace_byte_st, temp_read_buf.ptr(), temp_read_buf.length());
          read_data.set_length(piece_byte_len - replace_byte_len + temp_read_buf.length());
        } else {
          // after replace, cannot store in one this meta
          // keep remain data for next insert
          // [0, replace_byte_st]
          // [replace_byte_st, replace_byte_st+replace_byte_len]
          // [replace_byte_st+replace_byte_len, piece_byte_len]
          uint32_t remain_st = replace_byte_st + replace_byte_len;
          uint32_t remain_len = piece_byte_len - remain_st;
          MEMCPY(remain_data.ptr(), read_data.ptr() + remain_st, remain_len);
          remain_data.set_length(remain_len);
          // try copy data to meta
          int64_t max_byte = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE - replace_byte_st;
          int64_t data_char_len = 0;
          size_t data_by_len = ObCharset::max_bytes_charpos(param.coll_type_, temp_read_buf.ptr(), temp_read_buf.length(), max_byte, data_char_len);
          data_by_len = ob_lob_writer_length_validation(param.coll_type_, temp_read_buf.length(), data_by_len, data_char_len);
          MEMCPY(read_data.ptr() + replace_byte_st, temp_read_buf.ptr(), data_by_len);
          read_data.set_length(replace_byte_st + data_by_len);
          if (data_by_len == temp_read_buf.length()) {
            // try copy remain data to meta if data has copy fully
            max_byte = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE - read_data.length();
            size_t rdata_by_len = ObCharset::max_bytes_charpos(param.coll_type_, remain_data.ptr(), remain_data.length(), max_byte, data_char_len);
            rdata_by_len = ob_lob_writer_length_validation(param.coll_type_, remain_data.length(), rdata_by_len, data_char_len);
            if (rdata_by_len >= remain_data.length()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid len", K(ret), K(remain_data), K(rdata_by_len), K(max_byte));
            } else if (rdata_by_len > 0) {
              MEMCPY(read_data.ptr() + read_data.length(), remain_data.ptr(), rdata_by_len);
              read_data.set_length(read_data.length() + rdata_by_len);
              MEMMOVE(remain_data.ptr(), remain_data.ptr() + rdata_by_len, remain_data.length() - rdata_by_len);
              remain_data.set_length(remain_data.length() - rdata_by_len);
            }
          }
        }
        if (OB_SUCC(ret)) {
          new_meta_row.byte_len_ = read_data.length();
          new_meta_row.char_len_ = ObCharset::strlen_char(param.coll_type_, read_data.ptr(), read_data.length());
        }
      } else if (meta_iter.is_range_begin(result.meta_result_.info_)) {
        uint32_t by_st = 0;
        uint32_t by_len = local_begin;
        if (is_char) {
          transform_query_result_charset(param.coll_type_,
                                          read_data.ptr(),
                                          read_data.length(),
                                          by_len,
                                          by_st);
        }
        // [0, by_len][by_len, 256K]
        // try copy data to meta in [by_len, 256K]
        ObString temp_read_buf;
        uint32_t max_byte = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE - by_len;
        temp_read_buf.assign_buffer(read_buf.ptr(), max_byte);
        if (iter->is_end()) {
        } else if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to do get next read buffer", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (iter->is_end()) {
          if (local_begin == 0) {
            del_piece = true;
          } else {
            read_data.assign_ptr(read_data.ptr(), by_len);
            new_meta_row.byte_len_ = by_len;
            new_meta_row.char_len_ = local_begin;
          }
        } else {
          int64_t data_char_len = 0;
          size_t data_by_len = ObCharset::max_bytes_charpos(param.coll_type_, temp_read_buf.ptr(),
                                                            temp_read_buf.length(), max_byte, data_char_len);
          data_by_len = ob_lob_writer_length_validation(param.coll_type_, temp_read_buf.length(), data_by_len, data_char_len);
          MEMCPY(read_data.ptr() + by_len, temp_read_buf.ptr(), data_by_len);
          read_data.set_length(by_len + data_by_len);
          new_meta_row.byte_len_ = read_data.length();
          new_meta_row.char_len_ = local_begin + data_char_len;
        }
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
        // calc data
        ObString temp_read_buf;
        int64_t max_byte = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE - (piece_byte_len - by_len);
        temp_read_buf.assign_buffer(read_buf.ptr(), max_byte);
        if (iter->is_end()) {
        } else if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to do get next read buffer", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (iter->is_end()) {
          if (local_end == piece_char_len) {
            del_piece = true;
          } else {
            new_meta_row.char_len_ = piece_char_len - local_end;
            new_meta_row.byte_len_ = read_data.length() - by_len;
            MEMMOVE(read_data.ptr(), read_data.ptr() + by_len, read_data.length() - by_len);
            read_data.assign_ptr(read_data.ptr(), read_data.length() - by_len);
          }
        } else {
          // calc data
          int64_t data_char_len = 0;
          size_t data_by_len = ObCharset::max_bytes_charpos(param.coll_type_, temp_read_buf.ptr(), temp_read_buf.length(), max_byte, data_char_len);
          data_by_len = ob_lob_writer_length_validation(param.coll_type_, temp_read_buf.length(), data_by_len, data_char_len);
          MEMMOVE(read_data.ptr() + data_by_len, read_data.ptr() + by_len, piece_byte_len - by_len);
          MEMCPY(read_data.ptr(), temp_read_buf.ptr(), data_by_len);
          read_data.set_length(piece_byte_len - by_len + data_by_len);
          new_meta_row.char_len_ = piece_char_len - local_end + data_char_len;
          new_meta_row.byte_len_ = piece_byte_len - by_len + data_by_len;
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
  } else {
    // do replace for whole meta
    int64_t max_byte = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
    ObString temp_read_buf;
    temp_read_buf.assign_buffer(read_buf.ptr(), max_byte);
    if (iter->is_end()) {
    } else if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to do get next read buffer", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!iter->is_end()) {
      ObLobMetaInfo new_meta_row = result.meta_result_.info_;
      ObString read_data;
      read_data.assign_buffer(tmp_buf.ptr(), tmp_buf.size());

      int64_t data_char_len = 0;
      size_t data_by_len = ObCharset::max_bytes_charpos(param.coll_type_, temp_read_buf.ptr(), temp_read_buf.length(), max_byte, data_char_len);
      data_by_len = ob_lob_writer_length_validation(param.coll_type_, temp_read_buf.length(), data_by_len, data_char_len);
      MEMCPY(read_data.ptr(), temp_read_buf.ptr(), data_by_len);
      read_data.set_length(data_by_len);
      new_meta_row.byte_len_ = read_data.length();
      new_meta_row.char_len_ = data_char_len;
      if (OB_FAIL(update_one_piece(param,
                                    lob_ctx,
                                    result.meta_result_.info_,
                                    new_meta_row,
                                    result.piece_info_,
                                    read_data))) {
        LOG_WARN("failed to update.", K(ret), K(result), K(read_data));
      }
    } else {
      if (OB_FAIL(erase_one_piece(param,
                                  lob_ctx,
                                  result.meta_result_.info_,
                                  result.piece_info_))) {
        LOG_WARN("failed erase one piece", K(ret), K(result));
      }
    }
  }
  return ret;
}

int ObLobManager::do_delete_one_piece(ObLobAccessParam& param, ObLobQueryResult &result, ObString &tmp_buff)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  // do fill zero for whole meta
  if (param.is_fill_zero_) {
    ObLobMetaInfo new_meta_row = result.meta_result_.info_;
    uint32_t fill_len = new_meta_row.char_len_;
    char* tmp_buf = tmp_buff.ptr();
    if (OB_ISNULL(tmp_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fill zero buf is null.", K(ret), K(tmp_buff));
    } else {
      ObString space = ObCharsetUtils::get_const_str(param.coll_type_, ' ');
      if (param.coll_type_ == CS_TYPE_BINARY) {
        MEMSET(tmp_buf, 0x00, fill_len);
      } else {
        uint32_t space_len = space.length();
        if (fill_len%space_len != 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid res-len", K(ret), K(fill_len), K(space_len));
        } else if (space_len > 1) {
          for (int i = 0; i < fill_len/space_len; i++) {
            MEMCPY(tmp_buf + i * space_len, space.ptr(), space_len);
          }
        } else {
          MEMSET(tmp_buf, *space.ptr(), fill_len);
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        ObString read_data;
        read_data.assign_ptr(tmp_buf, fill_len);
        new_meta_row.byte_len_ = read_data.length();
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
  } else {
    if (OB_FAIL(erase_one_piece(param,
                                lob_ctx,
                                result.meta_result_.info_,
                                result.piece_info_))) {
      LOG_WARN("failed erase one piece", K(ret), K(result));
    }
  }
  return ret;
}

int ObLobManager::fill_zero(char *ptr, uint64_t length, bool is_char,
  const ObCollationType coll_type, uint32_t byte_len, uint32_t byte_offset, uint32_t &char_len)
{
  int ret = OB_SUCCESS;
  ObString space = ObCharsetUtils::get_const_str(coll_type, ' ');
  uint32_t space_len = space.length();
  uint32_t converted_len = space.length() * char_len;
  if (converted_len > byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill zero for length invalid", K(ret), K(space_len), K(char_len), K(byte_len));
  } else {
    char* dst_start = ptr + byte_offset + converted_len;
    char* src_start = ptr + byte_offset + byte_len;
    uint32_t cp_len = length - (byte_len + byte_offset);
    if (cp_len > 0 && dst_start != src_start) {
      MEMMOVE(dst_start, src_start, cp_len);
    }
    if (!is_char) {
      MEMSET(ptr + byte_offset, 0x00, converted_len);
    } else {
      if (space_len > 1) {
        for (int i = 0; i < char_len; i++) {
          MEMCPY(ptr + byte_offset + i * space_len, space.ptr(), space_len);
        }
      } else {
        MEMSET(ptr + byte_offset, ' ', char_len);
      }
    }
    char_len = converted_len;
  }
  return ret;
}

int ObLobManager::erase_process_meta_info(ObLobAccessParam& param, ObLobMetaScanIter &meta_iter,
    ObLobQueryResult &result, ObString &tmp_buff)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  bool del_piece = false;
  if (meta_iter.is_range_begin(result.meta_result_.info_) ||
      meta_iter.is_range_end(result.meta_result_.info_)) {
    // 1. read data
    // 2. rebuild data
    // 3. write data into lob data oper
    // 4. write meta tablet
    // 5. write piece tablet
    ObLobMetaInfo new_meta_row = result.meta_result_.info_;
    char* tmp_buf = tmp_buff.ptr();
    ObString read_data;
    if (param.is_fill_zero_) {
      read_data.assign_buffer(tmp_buf, tmp_buff.size());
    } else {
      read_data = result.meta_result_.info_.lob_data_;
    }

    // save variable
    uint32_t tmp_st = result.meta_result_.st_;
    uint32_t tmp_len = result.meta_result_.len_;

    // read all piece data
    result.meta_result_.st_ = 0;
    result.meta_result_.len_ = result.meta_result_.info_.char_len_;
    if (OB_ISNULL(tmp_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp buffer is null.", K(ret), K(tmp_buff));
    } else if (param.is_fill_zero_ && OB_FAIL(get_real_data(param, result, read_data))) {
      LOG_WARN("failed to write data to read buf.", K(ret), K(result));
    } else {
      result.meta_result_.st_ = tmp_st;
      result.meta_result_.len_ = tmp_len;

      // global pos, from 0
      uint64_t cur_piece_end = meta_iter.get_cur_pos();
      uint64_t cur_piece_begin = cur_piece_end - result.meta_result_.info_.char_len_;

      // local pos, from current piece;
      // if is_char, char pos; else byte pos
      // consider 3 situation according to this lob meta how to hit range
      // meta both hit begin and end : cur_piece_begin > offset > offset+len > cur_piece_end
      // meta hit begin : cur_piece_begin > offset > cur_piece_end
      // meta hit end : cur_piece_begin > offset+len > cur_piece_end
      uint32_t local_begin = param.offset_ - cur_piece_begin;
      uint32_t local_end = param.offset_ + param.len_ - cur_piece_begin;

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
          LOG_WARN("failed to erase.", K(ret), K(result), K(piece_char_len),
                   K(piece_byte_len), K(piece_data_st), K(read_data));
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
          if (param.is_fill_zero_) {
            uint32_t fill_char_len = local_end - local_begin;
            if (OB_FAIL(fill_zero(read_data.ptr(), read_data.length(), is_char, param.coll_type_, by_len, by_st, fill_char_len))) {
              LOG_WARN("failed to fill zero", K(ret));
            } else {
              read_data.set_length(read_data.length() - by_len + fill_char_len);
              new_meta_row.byte_len_ = read_data.length();
            }
          } else {
            read_data.assign_buffer(tmp_buf, tmp_buff.size());
            if (OB_FAIL(get_real_data(param, result, read_data))) {
              LOG_WARN("failed to write data to read buf.", K(ret), K(result));
            } else {
              new_meta_row.byte_len_ -= (by_len);
              new_meta_row.char_len_ -= (local_end - local_begin);
              MEMMOVE(read_data.ptr() + by_st, read_data.ptr() + (by_st + by_len), piece_byte_len - (by_st + by_len));
              read_data.assign_ptr(read_data.ptr(), read_data.length() - by_len);
            }
          }
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
          if (param.is_fill_zero_) {
            uint32_t fill_char_len = piece_char_len - local_begin;
            if (OB_FAIL(fill_zero(read_data.ptr(), read_data.length(), is_char, param.coll_type_,
                      read_data.length() - by_len, by_len, fill_char_len))) {
              LOG_WARN("failed to fill zero", K(ret));
            } else {
              read_data.set_length(by_len + fill_char_len);
              new_meta_row.byte_len_ = read_data.length();
            }
          } else {
            read_data.assign_ptr(read_data.ptr(), by_len);
            new_meta_row.byte_len_ = by_len;
            new_meta_row.char_len_ = local_begin;
          }
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
          if (param.is_fill_zero_) {
            uint32_t fill_char_len = local_end;
            if (OB_FAIL(fill_zero(read_data.ptr(), read_data.length(), is_char, param.coll_type_, by_len, 0, fill_char_len))) {
              LOG_WARN("failed to fill zero", K(ret));
            } else {
              read_data.set_length(fill_char_len + read_data.length() - by_len);
              new_meta_row.byte_len_ = read_data.length();
            }
          } else {
            read_data.assign_buffer(tmp_buf, result.meta_result_.info_.byte_len_);
            if (OB_FAIL(get_real_data(param, result, read_data))) {
              LOG_WARN("failed to write data to read buf.", K(ret), K(result));
            } else {
              new_meta_row.char_len_ = piece_char_len - local_end;
              new_meta_row.byte_len_ = read_data.length() - by_len;
              MEMMOVE(read_data.ptr(), read_data.ptr() + by_len, read_data.length() - by_len);
              read_data.assign_ptr(read_data.ptr(), read_data.length() - by_len);
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (del_piece) {
        if (OB_FAIL(do_delete_one_piece(param, result, tmp_buff))) {
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
  } else {
    if (OB_FAIL(do_delete_one_piece(param, result, tmp_buff))) {
      LOG_WARN("do delete one lob meta failed", K(ret), K(param), K(result));
    }
  }
  return ret;
}

int ObLobManager::prepare_erase_buffer(ObLobAccessParam& param, ObString &tmp_buff)
{
  int ret = OB_SUCCESS;
  char* tmp_buf = static_cast<char*>(param.allocator_->alloc(ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE));
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed.", K(ret));
  } else {
    tmp_buff.assign_buffer(tmp_buf, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
  }
  return ret;
}

int ObLobManager::erase_imple_inner(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
  ObLobMetaScanIter meta_iter;
  ObLobCtx lob_ctx = lob_ctx_;
  ObString tmp_buff;
  if (OB_FAIL(init_out_row_ctx(param, param.lob_data_->byte_size_, param.op_type_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param));
  } else if(OB_FAIL(prepare_erase_buffer(param, tmp_buff))) {
    LOG_WARN("fail to prepare buffers", K(ret), K(param));
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
      } else if (param.asscess_ptable_ /* TODO: weiyouchao.wyc should set param.asscess_ptable_ as false 2022.4.7 */ &&
                 OB_FAIL(lob_ctx.lob_piece_mngr_->get(param, result.meta_result_.info_.piece_id_, result.piece_info_))) {
        LOG_WARN("get lob piece failed.", K(ret), K(result));
      } else if (OB_FAIL(erase_process_meta_info(param, meta_iter, result, tmp_buff))) {
        LOG_WARN("process erase meta info failed.", K(ret), K(param), K(result));
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
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else {
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(OB_ISNULL(param.lob_common_))) {
      LOG_WARN("get lob locator null.", K(ret));
    } else if (OB_FAIL(check_handle_size(param))) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote erase", K(ret), K(param));
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
        uint32_t char_len = (param.len_ > max_len) ? max_len : param.len_;
        uint32_t byte_len = char_len;
        transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
        if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("data length is not enough.", K(ret), KPC(param.lob_data_), K(byte_offset), K(byte_len));
        } else {
          if (param.is_fill_zero_) { // do fill zero
            bool is_char = (param.coll_type_ != CS_TYPE_BINARY);
            if (OB_FAIL(fill_zero(data.ptr(), data.length(), is_char, param.coll_type_, byte_len, byte_offset, char_len))) {
              LOG_WARN("failed to fill zero", K(ret));
            } else {
              param.byte_size_ = param.byte_size_ - byte_len + char_len;
              if (param.lob_data_ != nullptr) {
                param.lob_data_->byte_size_ = param.byte_size_;
              }
              if (OB_NOT_NULL(param.lob_locator_)) {
                param.lob_locator_->size_ = param.lob_locator_->size_ - byte_len + char_len;
                if (OB_FAIL(fill_lob_locator_extern(param))) {
                  LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
                }
              }
            }
          } else { // do erase
            char* dst_start = data.ptr() + byte_offset;
            char* src_start = data.ptr() + byte_offset + byte_len;
            uint32_t cp_len = data.length() - (byte_len + byte_offset);
            if (cp_len > 0) {
              MEMMOVE(dst_start, src_start, cp_len);
            }
            param.byte_size_ -= byte_len;
            param.handle_size_ -= byte_len;
            if (param.lob_data_ != nullptr) {
              param.lob_data_->byte_size_ = param.byte_size_;
            }
            if (OB_NOT_NULL(param.lob_locator_)) {
              param.lob_locator_->size_ -= byte_len;
              if (OB_FAIL(fill_lob_locator_extern(param))) {
                LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
              }
            }
          }
        }
      }
    } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
    } else if (OB_FAIL(erase_imple_inner(param))) {
      LOG_WARN("failed erase", K(ret));
    }
  }
  return ret;
}

int ObLobManager::build_lob_param(ObLobAccessParam& param,
                                  ObIAllocator &allocator,
                                  ObCollationType coll_type,
                                  uint64_t offset,
                                  uint64_t len,
                                  int64_t timeout,
                                  ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param.set_lob_locator(&lob))) {
    LOG_WARN("set lob locator failed");
  } else {
    param.coll_type_ = coll_type;
    if (param.coll_type_ == CS_TYPE_INVALID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get collation type failed.", K(ret));
    } else {
      // common arg
      param.allocator_ = &allocator;
      param.byte_size_ = param.lob_common_->get_byte_size(param.handle_size_);
      param.offset_ = offset;
      param.len_ = len;
      param.timeout_ = timeout;
      // outrow arg for do lob meta scan
      if (OB_SUCC(ret) && lob.is_persist_lob() && !lob.has_inrow_data()) {
        ObMemLobTxInfo *tx_info = nullptr;
        ObMemLobLocationInfo *location_info = nullptr;
        if (OB_FAIL(lob.get_tx_info(tx_info))) {
          LOG_WARN("failed to get tx info", K(ret), K(lob));
        } else if (OB_FAIL(lob.get_location_info(location_info))) {
          LOG_WARN("failed to get location info", K(ret), K(lob));
        } else {
          auto snapshot_tx_seq = transaction::ObTxSEQ::cast_from_int(tx_info->snapshot_seq_);
          if (OB_ISNULL(param.tx_desc_) ||
              param.tx_desc_->get_tx_id().get_id() == tx_info->snapshot_tx_id_ || // read in same tx
              (tx_info->snapshot_tx_id_ == 0 && !snapshot_tx_seq.is_valid() && tx_info->snapshot_version_ > 0)) { // read not in tx
            param.snapshot_.core_.version_.convert_for_tx(tx_info->snapshot_version_);
            param.snapshot_.core_.tx_id_ = tx_info->snapshot_tx_id_;
            param.snapshot_.core_.scn_ = snapshot_tx_seq;
            param.snapshot_.valid_ = true;
            param.snapshot_.source_ = transaction::ObTxReadSnapshot::SRC::LS;
            param.snapshot_.snapshot_lsid_ = share::ObLSID(location_info->ls_id_);
          } else {
            // When param for write, param.tx_desc_ should not be null
            // If tx indfo from lob locator is old, produce new read snapshot directly
            transaction::ObTransService* txs = MTL(transaction::ObTransService*);
            transaction::ObTxIsolationLevel tx_level = transaction::ObTxIsolationLevel::RC;
            if (OB_FAIL(txs->get_ls_read_snapshot(*param.tx_desc_, tx_level, share::ObLSID(location_info->ls_id_),
                                                  param.timeout_, param.snapshot_))) {
              LOG_WARN("fail to get read snapshot", K(ret));
            }
          }

          param.ls_id_ = share::ObLSID(location_info->ls_id_);
          param.tablet_id_ = ObTabletID(location_info->tablet_id_);
        }
      }
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
    last_data_buf_len_ = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
    last_data_ptr_ = reinterpret_cast<char*>(param.allocator_->alloc(last_data_buf_len_));
    if (OB_ISNULL(last_data_ptr_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buffer failed.", K(ret), K(last_data_buf_len_));
    } else {
      param_ = param;
      lob_ctx_ = lob_ctx;
      is_inited_ = true;
      is_in_row_ = false;
      is_reverse_ = param.scan_backward_;
      cs_type_ = param.coll_type_;
      last_data_.assign_buffer(last_data_ptr_, last_data_buf_len_);
    }
  }
  return ret;
}

int ObLobQueryIter::open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse)
{
  int ret = OB_SUCCESS;
  cur_pos_ = 0;
  inner_data_.assign_ptr(data.ptr() + byte_offset, byte_len);
  is_inited_ = true;
  is_in_row_ = true;
  is_reverse_ = is_reverse;
  cs_type_ = cs;
  return ret;
}

int ObLobQueryIter::open(ObLobAccessParam &param, common::ObAddr dst_addr)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob locator is null.", K(ret), K(param));
  } else if (OB_FAIL(remote_reader_.open(param, rpc_buffer_))) {
    LOG_WARN("failed to open remote reader", K(ret));
  } else {
    ObLobManager *lob_manager = MTL(ObLobManager*);
    // build arg
    query_arg_.tenant_id_ = param.tenant_id_;
    query_arg_.offset_ = param.offset_;
    query_arg_.len_ = param.len_;
    query_arg_.cs_type_ = param.coll_type_;
    query_arg_.qtype_ = ObLobQueryArg::QueryType::READ;
    query_arg_.scan_backward_ = param.scan_backward_;
    query_arg_.lob_locator_.ptr_ = param.lob_locator_->ptr_;
    query_arg_.lob_locator_.size_ = param.lob_locator_->size_;
    query_arg_.lob_locator_.has_lob_header_ = param.lob_locator_->has_lob_header_;
    int64_t timeout = param.timeout_ - ObTimeUtility::current_time();
    if (timeout < ObStorageRpcProxy::STREAM_RPC_TIMEOUT) {
      timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
    }
    if (OB_FAIL(lob_manager->lob_remote_query_with_retry(param, dst_addr, query_arg_, timeout, rpc_buffer_, handle_))) {
      LOG_WARN("failed to do remote query", K(ret), K(query_arg_));
    } else {
      param_ = param;
      is_reverse_ = param.scan_backward_;
      cs_type_ = param.coll_type_;
      is_inited_ = true;
      is_remote_ = true;
    }
  }
  return ret;
}

int ObLobQueryIter::get_next_row(ObLobQueryResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else if (is_in_row_ || is_remote_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is unsupport for get meta result.", K(ret));
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

bool ObLobQueryIter::fill_buffer_to_data(ObString& data)
{
  int bret = false;
  if (last_data_.length() > 0) {
    uint64_t write_size = last_data_.length();
    int64_t write_char_len = 0;
    if (last_data_.length() > data.remain()) {
      write_size = data.remain();
      if (is_reverse_) {
        int64_t remain_size = last_data_.length() - write_size;
        if (cs_type_ != CS_TYPE_BINARY) {
          // get write size from end
          remain_size = ObCharset::max_bytes_charpos(cs_type_, last_data_.ptr(), last_data_.length(), remain_size, write_char_len);
          remain_size = ob_lob_writer_length_validation(cs_type_, last_data_.length(), remain_size, write_char_len);
          if (remain_size < last_data_.length() - write_size) {
            int64_t ex_size = ObCharset::charpos(cs_type_, last_data_.ptr() + remain_size,
              last_data_.length() - remain_size, 1);
            remain_size += ex_size;
          }
          write_size = last_data_.length() - remain_size;
        }
        write_size = data.write_front(last_data_.ptr() + remain_size, write_size);
        bret = true;
        last_data_.set_length(remain_size);
      } else {
        if (cs_type_ != CS_TYPE_BINARY) {
          write_size = ObCharset::max_bytes_charpos(cs_type_, last_data_.ptr(), last_data_.length(), write_size, write_char_len);
          write_size = ob_lob_writer_length_validation(cs_type_, last_data_.length(), write_size, write_char_len);
        }
        write_size = data.write(last_data_.ptr(), write_size);
        bret = true;
        last_data_.assign_ptr(last_data_.ptr() + write_size, last_data_.length() - write_size);
      }
    } else {
      // do full write, we can regard with char or byte
      if (is_reverse_) {
        write_size = data.write_front(last_data_.ptr(), last_data_.length());
      } else {
        write_size = data.write(last_data_.ptr(), last_data_.length());
      }
      // reset last data buffer
      last_data_.assign_buffer(last_data_ptr_, last_data_buf_len_);
    }
  }
  return bret;
}

int ObLobQueryIter::get_next_row(ObString& data)
{
  int ret = OB_SUCCESS;
  ObLobQueryResult result;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else if (is_in_row_) {
    if (cur_pos_ == inner_data_.length()) {
      ret = OB_ITER_END;
    } else {
      char *read_ptr = inner_data_.ptr();
      uint64_t read_size = data.size();
      int64_t read_char_len = 0;
      if (cur_pos_ + read_size > inner_data_.length()) {
        read_size = inner_data_.length() - cur_pos_;
      }
      if (cs_type_ != CS_TYPE_BINARY) { //clob
        if (is_reverse_) {
          int64_t N = inner_data_.length();
          // [0, N-cur_pos-read_size][N-cur_pos-read_size, N-cur_pos][N-cur_pos, N]
          // check [0, N-cur_pos-read_size] with max byte char
          // if result not equal to N-cur_pos-read_size, should consider next one more char pos
          int64_t remain_size = N - cur_pos_ - read_size;
          if (N - cur_pos_ - read_size > 0) {
            remain_size = ObCharset::max_bytes_charpos(cs_type_, read_ptr, remain_size, remain_size, read_char_len);
            remain_size = ob_lob_writer_length_validation(cs_type_, remain_size, remain_size, read_char_len);
            if (remain_size < N - cur_pos_ - read_size) {
              int64_t ex_size = ObCharset::charpos(cs_type_, read_ptr + remain_size, N - remain_size, 1);
              remain_size += ex_size;
            }
            read_size = N - cur_pos_ - remain_size;
          }
          read_ptr = inner_data_.ptr() + N - cur_pos_ - read_size;
        } else {
          read_ptr = inner_data_.ptr() + cur_pos_;
          read_size = ObCharset::max_bytes_charpos(cs_type_, read_ptr,
            inner_data_.length() - cur_pos_, read_size, read_char_len);
          read_size = ob_lob_writer_length_validation(cs_type_, inner_data_.length() - cur_pos_, read_size, read_char_len);
        }
      } else { // blob
        if (is_reverse_) {
          read_ptr = inner_data_.ptr() + inner_data_.length() - cur_pos_ - read_size;
        } else {
          read_ptr = inner_data_.ptr() + cur_pos_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_reverse_ && data.write_front(read_ptr, read_size) != read_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write front output data.", K(data), K(cur_pos_), K(read_size), K(inner_data_.length()));
      } else if (!is_reverse_ && data.write(read_ptr, read_size) != read_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write output data.", K(data), K(cur_pos_), K(read_size), K(inner_data_.length()));
      } else {
        cur_pos_ += read_size;
      }
    }
  } else if (is_remote_) {
    bool has_fill_full = false;
    uint64_t st_len = data.length();
    ObLobQueryBlock block;
    ObString cur_buffer;
    while (OB_SUCC(ret) && !has_fill_full) {
      // first try fill buffer remain data to output
      has_fill_full = fill_buffer_to_data(data);
      if (has_fill_full) {
        // data has been filled full, do nothing
      } else if (OB_FAIL(remote_reader_.get_next_block(param_, rpc_buffer_, handle_, block, last_data_))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get block from remote reader", K(ret));
        }
      }
    }
    if (ret == OB_ITER_END && data.length() > st_len) {
      ret = OB_SUCCESS; // has fill data, just return OK
    }
  } else {
    bool has_fill_full = false;
    uint64_t st_len = data.length();
    while (OB_SUCC(ret) && !has_fill_full) {
      // first try fill buffer remain data to output
      has_fill_full = fill_buffer_to_data(data);
      if (has_fill_full) {
        // do nothing
      } else if (OB_FAIL(get_next_row(result))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("get next query result failed.", K(ret));
        }
      } else {
        // do get real data
        ObLobManager *lob_mngr = MTL(ObLobManager*);
        if (OB_ISNULL(lob_mngr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get lob mngr.", K(ret));
        } else {
          if (result.meta_result_.info_.byte_len_ < data.remain()) {
            if (OB_FAIL(lob_mngr->get_real_data(param_, result, data))) {
              LOG_WARN("get real data failed.", K(ret));
            }
          } else {
            if (OB_FAIL(lob_mngr->get_real_data(param_, result, last_data_))) {
              LOG_WARN("get real data failed.", K(ret));
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END && data.length() > st_len) {
      ret = OB_SUCCESS; // has fill data, just return OK
    }
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);
  return ret;
}

void ObLobQueryIter::reset()
{
  meta_iter_.reset();
  inner_data_.reset();
  is_end_ = false;
  is_in_row_ = false;
  is_inited_ = false;
  is_remote_ = false;
  last_data_.reset();
  if (last_data_ptr_ != nullptr) {
    param_.allocator_->free(last_data_ptr_);
    last_data_ptr_ = nullptr;
  }
}


/**********ObLobQueryRemoteReader****************/

int ObLobQueryRemoteReader::open(ObLobAccessParam& param, common::ObDataBuffer &rpc_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
  if (NULL == (buf = reinterpret_cast<char*>(param.allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (!rpc_buffer.set_data(buf, buf_len)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret));
  } else if (NULL == (buf = reinterpret_cast<char*>(param.allocator_->alloc(buf_len)))) {
    LOG_WARN("failed to alloc buf", K(ret));
  } else {
    data_buffer_.assign_buffer(buf, buf_len);
  }
  return ret;
}

int ObLobQueryRemoteReader::get_next_block(ObLobAccessParam& param,
                                           common::ObDataBuffer &rpc_buffer,
                                           obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle,
                                           ObLobQueryBlock &block,
                                           ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_fetch_rpc_buffer(param, rpc_buffer, handle))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer.get_data(),
      rpc_buffer.get_position(),
      rpc_buffer_pos_,
      block))) {
    STORAGE_LOG(WARN, "failed to decode macro block size", K(ret), K(rpc_buffer), K(rpc_buffer_pos_));
  } else if (OB_FAIL(data_buffer_.set_length(0))) {
    LOG_WARN("failed to set data buffer pos", K(ret), K(data_buffer_));
  } else {
    while (OB_SUCC(ret)) {
      if (data_buffer_.length() > block.size_) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than occupy size", K(ret), K(data_buffer_), K(block));
      } else if (data_buffer_.length() == block.size_) {
        data.assign_ptr(data_buffer_.ptr(), data_buffer_.length());
        LOG_DEBUG("get_next_macro_block", K(rpc_buffer), K(rpc_buffer_pos_), K(block));
        break;
      } else if (OB_FAIL(do_fetch_rpc_buffer(param, rpc_buffer, handle))) {
        LOG_WARN("failed to fetch next buffer if need", K(ret));
      } else {
        int64_t need_size = block.size_ - data_buffer_.length();
        int64_t rpc_remain_size = rpc_buffer.get_position() - rpc_buffer_pos_;
        int64_t copy_size = std::min(need_size, rpc_remain_size);
        if (copy_size > data_buffer_.remain()) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, macro block data must not larger than data buffer",
              K(ret), K(copy_size), K(data_buffer_), K(data_buffer_.remain()));
        } else {
          LOG_DEBUG("copy rpc to data buffer",
              K(need_size), K(rpc_remain_size), K(copy_size), "header_size", block.size_, K(rpc_buffer_pos_));
          if (data_buffer_.write(rpc_buffer.get_data() + rpc_buffer_pos_, copy_size) != copy_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to write data buffer", K(ret), K(data_buffer_.remain()), K(copy_size));
          } else {
            rpc_buffer_pos_ += copy_size;
          }
        }
      }
    }
  }
  return ret;
}

int ObLobQueryRemoteReader::do_fetch_rpc_buffer(ObLobAccessParam& param,
                                                common::ObDataBuffer &rpc_buffer,
                                                obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle)
{
  int ret = OB_SUCCESS;
  if (rpc_buffer_pos_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_pos_));
  } else if (rpc_buffer.get_position() - rpc_buffer_pos_ > 0) {
    // do nothing
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer), K(rpc_buffer_pos_));
  } else {
    rpc_buffer.get_position() = 0;
    rpc_buffer_pos_ = 0;
    if (handle.has_more()) {
      if (OB_SUCCESS != (ret = handle.get_more(rpc_buffer))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
      } else if (rpc_buffer.get_position() < 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
      } else if (0 == rpc_buffer.get_position()) {
        if (!handle.has_more()) {
          ret = OB_ITER_END;
          LOG_DEBUG("empty rpc buffer, no more data", K(rpc_buffer), K(rpc_buffer_pos_));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
        }
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer), K(rpc_buffer_pos_));
      }
    } else {
      ret = OB_ITER_END;
      LOG_DEBUG("no more data", K(rpc_buffer), K(rpc_buffer_pos_));
    }
  }
  return ret;
}

} // storage
} // oceanbase
