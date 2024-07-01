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
#include "sql/das/ob_das_utils.h"
#include "storage/lob/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

static int check_write_length(ObLobAccessParam& param, int64_t expected_len)
{
  int ret = OB_SUCCESS;
  if (ObLobDataOutRowCtx::OpType::SQL != param.op_type_) {
    // skip not full write
  } else if (param.byte_size_ != expected_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size not match", K(ret), K(expected_len), K(param.byte_size_));
  }
  return ret;
}

const ObLobCommon ObLobManager::ZERO_LOB = ObLobCommon();

static int prepare_data_buffer(ObLobAccessParam& param, ObString &buffer, int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  char *ptr = nullptr;
  if (OB_ISNULL(param.get_tmp_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(param));
  } else if (OB_ISNULL(ptr = static_cast<char*>(param.get_tmp_allocator()->alloc(buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed.", K(ret));
  } else {
    buffer.assign_buffer(ptr, buffer_size);
  }
  return ret;
}

// for only one lob meta in mysql mode, we can have no char len here
static int is_store_char_len(ObLobAccessParam& param, int64_t store_chunk_size, int64_t add_len)
{
  int ret = OB_SUCCESS;
  if (! lib::is_mysql_mode()) {
    LOG_DEBUG("not mysql mode", K(add_len), K(store_chunk_size), K(param));
  } else if (! param.is_char()) {
    LOG_DEBUG("not text", K(add_len), K(store_chunk_size), K(param));
  } else if (store_chunk_size <= (param.byte_size_ + add_len)) {
    LOG_DEBUG("not single", K(add_len), K(store_chunk_size), K(param));
  } else if (param.tablet_id_.is_inner_tablet()) {
    LOG_DEBUG("inner table skip", K(add_len), K(store_chunk_size), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get data version", K(ret), K(store_chunk_size), K(tenant_id));
    } else if (data_version < MOCK_DATA_VERSION_4_2_3_0) {
      LOG_DEBUG("before 4.2.3", K(add_len), K(store_chunk_size), K(param));
    } else if (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_2_0) {
      LOG_DEBUG("before 4.3.2", K(add_len), K(store_chunk_size), K(param));
    } else {
      // sinlge not need char_len after >= 4.3.2 or (>= 4.2.3 and < 4.3.0
      param.is_store_char_len_ = false;
      LOG_DEBUG("not store char_len for single piece", K(add_len), K(store_chunk_size), K(param));
    }
  }
  return ret;
}

static bool lob_handle_has_char_len_field(ObLobAccessParam& param)
{
  bool bret = false;
  if (param.lob_common_ != nullptr && !param.lob_common_->in_row_) {
    if (param.handle_size_ >= ObLobManager::LOB_OUTROW_FULL_SIZE) {
      bret = true;
    } else {
      LOG_INFO("old old data", K(param));
    }
  }
  return bret;
}

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
  } else if (OB_FAIL(ext_info_log_allocator_.init(
      common::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "ExtInfoLog", ObCtxIds::LOB_CTX_ID)))) {
    LOG_WARN("init ext info log allocator failed.", K(ret));
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
        LOG_DEBUG("get ls leader", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
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
  uint64_t tenant_id = param.tenant_id_;
  const ObAddr &self_addr = MYADDR;
  if (lob_locator == nullptr) {
    is_remote = false;
  } else if (!lob_locator->is_persist_lob()) {
    is_remote = false;
  } else if (param.from_rpc_ == true) {
    is_remote = false;
  } else {
    bool has_retry_info = false;
    ObMemLobExternHeader *extern_header = nullptr;
    if (OB_SUCC(lob_locator->get_extern_header(extern_header))) {
      has_retry_info = extern_header->flags_.has_retry_info_;
    }
    uint64_t min_ver = GET_MIN_CLUSTER_VERSION();
    if ((min_ver <= CLUSTER_VERSION_4_2_2_0) ||
        (min_ver >= CLUSTER_VERSION_4_3_0_0 && min_ver <= CLUSTER_VERSION_4_3_1_0)) {
      // compat old version
      has_retry_info = false;
    }
    if (has_retry_info) {
      ObMemLobRetryInfo *retry_info = nullptr;
      if (OB_FAIL(lob_locator->get_retry_info(retry_info))) {
        LOG_WARN("fail to get retry info", K(ret), KPC(lob_locator));
      } else {
        dst_addr = retry_info->addr_;
      }
    } else {
      if (OB_FAIL(get_ls_leader(param, tenant_id, param.ls_id_, dst_addr))) {
        LOG_WARN("failed to get ls leader", K(ret), K(tenant_id), K(param.ls_id_));
      }
    }
    if (OB_SUCC(ret)) {
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
  return (ret == OB_REPLICA_NOT_READABLE) ||
         (ret == OB_RPC_CONNECT_ERROR) ||
         (ret == OB_RPC_SEND_ERROR) ||
         (ret == OB_RPC_POST_ERROR) ||
         (ret == OB_NOT_MASTER) ||
         (ret == OB_NO_READABLE_REPLICA) ||
         (ret == OB_TABLET_NOT_EXIST) ||
         (ret == OB_LS_NOT_EXIST);
}

int ObLobManager::lob_query_with_retry(ObLobAccessParam &param, ObAddr &dst_addr,
    bool &remote_bret, ObLobMetaScanIter& iter,
    ObLobQueryArg::QueryType qtype, void *&ctx)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  bool is_continue = true;
  ObLSService *ls_service = (MTL(ObLSService *));
  obrpc::ObStorageRpcProxy *svr_rpc_proxy = ls_service->get_storage_rpc_proxy();
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  do {
    if (remote_bret) {
      // first try to init remote ctx
      if (OB_FAIL(lob_remote_query_init_ctx(param, qtype, ctx))) {
        LOG_WARN("fail to init remote query ctx", K(ret));
      } else {
        ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(ctx);
        int64_t timeout = param.timeout_ - ObTimeUtility::current_time();
        if (timeout < ObStorageRpcProxy::STREAM_RPC_TIMEOUT) {
          timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
        }
        ret = svr_rpc_proxy->to(dst_addr).by(remote_ctx->query_arg_.tenant_id_)
                        .dst_cluster_id(GCONF.cluster_id)
                        .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                        .timeout(timeout)
                        .lob_query(remote_ctx->query_arg_, remote_ctx->rpc_buffer_, remote_ctx->handle_);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to do remote lob query", K(ret), K(remote_ctx->query_arg_), K(dst_addr), K(timeout));
        }
      }
    } else {
      if (OB_FAIL(lob_ctx_.lob_meta_mngr_->scan(param, iter))) {
        LOG_WARN("failed to do local lob query and show retry cnt and mem usage", K(ret), K(param),
                 K(dst_addr), K(retry_cnt), K(param.allocator_->total()), K(param.allocator_->used()));
        // reset iter for maybe has done alloc for iter
        iter.reset();
      }
    }
    if (OB_FAIL(ret)) {
      // check timeout
      if (param.from_rpc_) { // from rpc should not do retry, just return ret
        is_continue = false;
      } else if (is_remote_ret_can_retry(ret)) {
        retry_cnt++;
        if (retry_cnt >= 100 && retry_cnt % 50L == 0) {
          LOG_INFO("[LOB RETRY] The LOB query has been retried multiple times without success, "
                    "and the execution may be blocked by a specific exception", KR(ret),
                    "continuous_retry_cnt", retry_cnt, K(param), K(remote_bret), K(dst_addr));
        }
        if (ObTimeUtility::current_time() > param.timeout_) {
          is_continue = false;
          ret = OB_TIMEOUT;
          int64_t cur_time = ObTimeUtility::current_time();
          LOG_WARN("[LOB RETRY] query timeout", K(cur_time), K(param.timeout_), K(ret));
        } else if (IS_INTERRUPTED()) { // for worker interrupted
          is_continue = false;
          LOG_INFO("[LOB RETRY] Retry is interrupted by worker interrupt signal", KR(ret));
        } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
          is_continue = false;
          ret = OB_KILLED_BY_THROTTLING;
          LOG_INFO("[LOB RETRY] Retry is interrupted by worker check wait", KR(ret));
        } else {
          switch (ret) {
            case  OB_REPLICA_NOT_READABLE:
            case  OB_RPC_CONNECT_ERROR:
            case  OB_RPC_SEND_ERROR:
            case  OB_RPC_POST_ERROR:
            case  OB_NOT_MASTER:
            case  OB_NO_READABLE_REPLICA:
            case  OB_TABLET_NOT_EXIST:
            case  OB_LS_NOT_EXIST: {
              remote_bret = false;
              // refresh location
              if (OB_FAIL(lob_refresh_location(param, dst_addr, remote_bret, ret, retry_cnt))) {
                LOG_WARN("fail to do refresh location", K(ret));
                is_continue = false;
              }
              break;
            }
            default: {
              LOG_INFO("do nothing, just retry", K(ret), K(retry_cnt));
            }
          }
        }
      } else {
        is_continue = false;
      }
    } else {
      is_continue = false;
    }
  } while (is_continue);
  return ret;
}

int ObLobManager::lob_check_tablet_not_exist(ObLobAccessParam &param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  bool tablet_exist = false;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", KR(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(param.tenant_id_, schema_guard))) {
    // tenant could be deleted
    LOG_WARN("get tenant schema guard fail", KR(ret), K(param.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param.tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    //table could be dropped
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist, fast fail das task", K(table_id));
  } else if (OB_FAIL(table_schema->check_if_tablet_exists(param.tablet_id_, tablet_exist))) {
    LOG_WARN("check if tablet exists failed", K(ret), K(param), K(table_id));
  } else if (!tablet_exist) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("partition not exist, maybe dropped by DDL", K(ret), K(param), K(table_id));
  }
  return ret;
}

int ObLobManager::lob_refresh_location(ObLobAccessParam &param, ObAddr &dst_addr, bool &remote_bret, int last_err, int retry_cnt)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  const ObAddr &self_addr = MYADDR;
  ObMemLobExternHeader *extern_header = NULL;
  bool has_retry_info = false;
  if (OB_NOT_NULL(lob_locator) && OB_SUCC(lob_locator->get_extern_header(extern_header))) {
    has_retry_info = extern_header->flags_.has_retry_info_;
  }
  uint64_t min_ver = GET_MIN_CLUSTER_VERSION();
  if ((min_ver <= CLUSTER_VERSION_4_2_2_0) ||
      (min_ver >= CLUSTER_VERSION_4_3_0_0 && min_ver <= CLUSTER_VERSION_4_3_1_0)) {
    // compat old version
    has_retry_info = false;
  }
  if (param.tenant_id_ != MTL_ID()) { // query over tenant id
    has_retry_info = false;
  }
  if (!has_retry_info) {
    // do check remote
    if (OB_FAIL(is_remote(param, remote_bret, dst_addr))) {
      LOG_WARN("fail to do check is remote", K(ret));
    }
  } else if (OB_FAIL(ObDASUtils::wait_das_retry(retry_cnt))) {
    LOG_WARN("wait das retry failed", K(ret));
  } else {
    // do location refresh
    ObArenaAllocator tmp_allocator("LobRefLoc", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObDASLocationRouter router(tmp_allocator);
    router.set_last_errno(last_err);
    ObDASTableLocMeta loc_meta(tmp_allocator);
    loc_meta.ref_table_id_ = extern_header->table_id_;
    ObDASTabletLoc tablet_loc;
    ObMemLobRetryInfo *retry_info = nullptr;
    if (last_err == OB_TABLET_NOT_EXIST && OB_FAIL(lob_check_tablet_not_exist(param, extern_header->table_id_))) {
      LOG_WARN("fail to check tablet not exist", K(ret), K(extern_header->table_id_));
    } else if (OB_FAIL(lob_locator->get_retry_info(retry_info))) {
      LOG_WARN("fail to get retry info", K(ret), KPC(lob_locator));
    } else if (OB_FALSE_IT(loc_meta.select_leader_ = retry_info->is_select_leader_)) {
       // use main tablet id to get location, for lob meta tablet is same location as main tablet
    } else if (OB_FAIL(router.get_tablet_loc(loc_meta, param.tablet_id_, tablet_loc))) {
      LOG_WARN("fail to refresh location", K(ret));
    } else {
      dst_addr = tablet_loc.server_;
      remote_bret = (dst_addr != self_addr);
      if (tablet_loc.ls_id_ != param.ls_id_) {
        LOG_INFO("[LOB RETRY] lob retry find tablet ls id is changed",
                 K(param.tablet_id_), K(param.ls_id_), K(tablet_loc.ls_id_));
        param.ls_id_ = tablet_loc.ls_id_;
      }
    }
  }
  LOG_DEBUG("[LOB RETRY] after do fresh location", K(ret), K(has_retry_info), K(dst_addr), K(remote_bret));
  return ret;
}

int ObLobManager::lob_remote_query_init_ctx(
    ObLobAccessParam &param,
    ObLobQueryArg::QueryType qtype,
    void *&ctx)
{
  int ret = OB_SUCCESS;
  if (ctx != nullptr) {
    // do nothing, has been init
  } else {
    void *buff = param.allocator_->alloc(sizeof(ObLobRemoteQueryCtx));
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc lob remote query ctx", K(ret));
    } else {
      ObLobRemoteQueryCtx *remote_ctx = new(buff)ObLobRemoteQueryCtx();
      if (OB_FAIL(remote_ctx->remote_reader_.open(param, remote_ctx->rpc_buffer_))) {
        LOG_WARN("fail to open lob remote reader", K(ret));
      } else {
        // build arg
        remote_ctx->query_arg_.tenant_id_ = param.tenant_id_;
        remote_ctx->query_arg_.offset_ = param.offset_;
        remote_ctx->query_arg_.len_ = param.len_;
        remote_ctx->query_arg_.cs_type_ = param.coll_type_;
        remote_ctx->query_arg_.scan_backward_ = param.scan_backward_;
        remote_ctx->query_arg_.qtype_ = qtype;
        remote_ctx->query_arg_.lob_locator_.ptr_ = param.lob_locator_->ptr_;
        remote_ctx->query_arg_.lob_locator_.size_ = param.lob_locator_->size_;
        remote_ctx->query_arg_.lob_locator_.has_lob_header_ = param.lob_locator_->has_lob_header_;
        //set ctx ptr
        ctx = buff;
      }
    }
  }
  return ret;
}

int ObLobManager::query_remote(ObLobAccessParam& param, ObString& data)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob locator is null.", K(ret), K(param));
  } else if (OB_ISNULL(param.remote_query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get remote query ctx nullptr", K(ret), K(param));
  } else {
    ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(param.remote_query_ctx_);
    ObLobQueryBlock block;
    ObString block_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(remote_ctx->remote_reader_.get_next_block(param, remote_ctx->rpc_buffer_, remote_ctx->handle_, block, block_data))) {
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

    if (OB_SUCC(ret) && param.is_full_read() && param.byte_size_ != data.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read size not macth", K(ret), "current_length", data.length(), "data_length", param.byte_size_, K(param), K(data));
    }
  }
  return ret;
}

static int read_all(
  ObLobAccessParam& param,
  ObLobMetaScanIter& meta_iter,
  ObString& output_data)
{
  int ret = OB_SUCCESS;
  ObLobQueryResult result;
  meta_iter.set_not_calc_char_len(true);
  meta_iter.set_not_need_last_info(true);
  while (OB_SUCC(ret)) {
    ret = meta_iter.get_next_row(result.meta_result_);
    const char *lob_data = result.meta_result_.info_.lob_data_.ptr();
    uint32_t byte_len = result.meta_result_.info_.lob_data_.length();
    if (OB_FAIL(ret)) {
      if (ret == OB_ITER_END) {
      } else {
        LOG_WARN("failed to get next row.", K(ret));
      }
    } else if (ObTimeUtility::current_time() > param.timeout_) {
      ret = OB_TIMEOUT;
      int64_t cur_time = ObTimeUtility::current_time();
      LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
    } else if (param.scan_backward_ && output_data.write_front(lob_data, byte_len) != byte_len) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("failed to write buffer to output_data.", K(ret),
                K(output_data.length()), K(output_data.remain()), K(byte_len));
    } else if (!param.scan_backward_ && output_data.write(lob_data, byte_len) != byte_len) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("failed to write buffer to output_data.", K(ret),
                K(output_data.length()), K(output_data.remain()), K(byte_len));
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && param.byte_size_ != output_data.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read size not macth", K(ret), "read_length", output_data.length(), "read_length", param.byte_size_, K(param), K(output_data));
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
      ObLobMetaScanIter meta_iter;
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
      if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
        LOG_WARN("check is remote failed.", K(ret), K(param));
      } else if (OB_FAIL(lob_query_with_retry(param, dst_addr, is_remote_lob, meta_iter,
                                              ObLobQueryArg::QueryType::READ, param.remote_query_ctx_))) {
        LOG_WARN("fail to do lob query with retry", K(ret), K(is_remote_lob), K(dst_addr));
      } else if (is_remote_lob) {
        if (OB_FAIL(query_remote(param, output_data))) {
          LOG_WARN("do remote query failed.", K(ret), K(param), K(dst_addr));
        }
      } else {
        if (!lob_common->is_init_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob common header for out row.", K(ret), KPC(lob_common));
        } else if (param.is_full_read()) {
          if (OB_FAIL(read_all(param, meta_iter, output_data))) {
            LOG_WARN("read_all fail", K(ret), K(param));
          }
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
            } else if (OB_FAIL(get_real_data(param, result, output_data))) {
              LOG_WARN("failed to write data to output buf.", K(ret), K(result), K(output_data));
            }
          }
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
          }
        }
      }
      // finish query, release resource
      if (OB_NOT_NULL(param.remote_query_ctx_)) {
        ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(param.remote_query_ctx_);
        remote_ctx->~ObLobRemoteQueryCtx();
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
      } else if (OB_FAIL(iter->open(param, lob_ctx, dst_addr, is_remote_lob))) {
        LOG_WARN("open local meta scan iter failed", K(ret), K(param), K(dst_addr), K(is_remote_lob));
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

int ObLobManager::query(ObString& data, ObLobQueryIter *&result)
{
  INIT_SUCC(ret);
  ObLobQueryIter* iter = OB_NEW(ObLobQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc lob meta scan iterator fail", K(ret));
  } else if (OB_FAIL(iter->open(data, 0, data.length(), CS_TYPE_BINARY, false))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(data));
  } else {
    result = iter;
  }
  return ret;
}

int ObLobManager::load_all(ObLobAccessParam &param, ObLobPartialData &partial_data)
{
  INIT_SUCC(ret);
  char *output_buf = nullptr;
  uint64_t output_len = param.byte_size_;
  ObString output_data;
  if (OB_ISNULL(output_buf = static_cast<char*>(param.allocator_->alloc(output_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(param));
  } else if (OB_FALSE_IT(output_data.assign_buffer(output_buf, output_len))) {
  } else if (OB_FAIL(query(param, output_data))) {
    LOG_WARN("do remote query fail", K(ret), K(param), K(output_len));
  } else if (OB_FAIL(partial_data.data_.push_back(ObLobChunkData(output_data)))) {
    LOG_WARN("push_back lob chunk data fail", KR(ret));
  } else {
    ObLobSeqId seq_id_generator(param.allocator_);
    ObString seq_id;
    uint64_t offset = 0;
    int64_t chunk_count = (param.byte_size_ + partial_data.chunk_size_ - 1)/partial_data.chunk_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < chunk_count; ++i) {
      ObLobChunkIndex chunk_index;
      chunk_index.offset_ = offset;
      chunk_index.pos_ = offset;
      chunk_index.byte_len_ = std::min(output_len, offset + partial_data.chunk_size_) - offset;
      chunk_index.data_idx_ = 0;
      if (OB_FAIL(seq_id_generator.get_next_seq_id(seq_id))) {
        LOG_WARN("failed to next seq id", K(ret), K(chunk_index));
      } else if (OB_FAIL(ob_write_string(*param.allocator_, seq_id, chunk_index.seq_id_))) {
        LOG_WARN("ob_write_stringt seq id fail", K(ret), K(chunk_count), K(output_len), K(partial_data.chunk_size_), K(chunk_index), K(seq_id));
      } else if (OB_FAIL(partial_data.push_chunk_index(chunk_index))) {
        LOG_WARN("push_back lob chunk index fail", KR(ret), K(chunk_count), K(output_len), K(partial_data.chunk_size_), K(chunk_index));
      } else {
        offset += partial_data.chunk_size_;
      }
    }
  }
  return ret;
}

int ObLobManager::query(
    ObIAllocator *allocator,
    ObLobLocatorV2 &locator,
    int64_t query_timeout_ts,
    bool is_load_all,
    ObLobPartialData *partial_data,
    ObLobCursor *&cursor)
{
  INIT_SUCC(ret);
  ObLobAccessParam *param = nullptr;
  bool is_remote_lob = false;
  bool is_partial_data_alloc = false;
  common::ObAddr dst_addr;
  if (! locator.has_lob_header() || ! locator.is_persist_lob() || locator.is_inrow()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid locator", KR(ret), K(locator));
  } else if (OB_ISNULL(cursor = OB_NEWx(ObLobCursor, allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObLobCursor));
  } else if (OB_ISNULL(param = OB_NEWx(ObLobAccessParam, allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObLobAccessParam));
  } else if (OB_FAIL(build_lob_param(*param, *allocator, CS_TYPE_BINARY,
                      0, UINT64_MAX, query_timeout_ts, locator))) {
    LOG_WARN("build_lob_param fail", K(ret));
  } else if (! param->lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob common not init", K(ret), KPC(param->lob_common_), KPC(param));
  } else if (OB_ISNULL(param->lob_data_ = reinterpret_cast<ObLobData*>(param->lob_common_->buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data is null", K(ret), KPC(param->lob_common_), KPC(param));
  } else if (OB_FAIL(is_remote(*param, is_remote_lob, dst_addr))) {
    LOG_WARN("check is remote fail", K(ret), K(param));
  } else if (OB_ISNULL(partial_data)) {
    is_partial_data_alloc = true;
    if (OB_ISNULL(partial_data = OB_NEWx(ObLobPartialData, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc lob param fail", K(ret), "size", sizeof(ObLobPartialData));
    } else if (OB_FAIL(partial_data->init())) {
      LOG_WARN("map create fail", K(ret));
    } else if (OB_FAIL(locator.get_chunk_size(partial_data->chunk_size_))) {
      LOG_WARN("get_chunk_size fail", K(ret), K(locator));
    } else {
      partial_data->data_length_ = param->byte_size_;
      partial_data->locator_.assign_ptr(locator.ptr_, locator.size_);
      // new alloc partial_data do load data if need
      if ((is_load_all || is_remote_lob) && OB_FAIL(load_all(*param, *partial_data))) {
        LOG_WARN("load_all fail", K(ret));
      }
    }
  }
  if (is_remote_lob) {
    LOG_INFO("remote_lob", KPC(param->lob_common_), KPC(param->lob_data_), K(dst_addr));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cursor->init(allocator, param, partial_data, lob_ctx_))) {
    LOG_WARN("cursor init fail", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(cursor)) {
      cursor->~ObLobCursor();
      cursor = nullptr;
    }
    if (OB_NOT_NULL(partial_data) && is_partial_data_alloc) {
      partial_data->~ObLobPartialData();
      partial_data = nullptr;
    }
  }
  return ret;
}

int ObLobManager::equal(ObLobLocatorV2& lob_left,
                        ObLobLocatorV2& lob_right,
                        ObLobCompareParams& cmp_params,
                        bool& result)
{
  INIT_SUCC(ret);
  int64_t old_len = 0;
  int64_t new_len = 0;
  int64_t cmp_res = 0;
  if (OB_FAIL(lob_left.get_lob_data_byte_len(old_len))) {
    LOG_WARN("fail to get old byte len", K(ret), K(lob_left));
  } else if (OB_FAIL(lob_right.get_lob_data_byte_len(new_len))) {
    LOG_WARN("fail to get new byte len", K(ret), K(lob_right));
  } else if (new_len != old_len) {
    result = false;
  } else if (lob_left.has_inrow_data() && lob_right.has_inrow_data()) {
    // do both inrow check
    ObString left_str;
    ObString right_str;
    if (OB_FAIL(lob_left.get_inrow_data(left_str))) {
      LOG_WARN("fail to get old inrow data", K(ret), K(lob_left));
    } else if (OB_FAIL(lob_right.get_inrow_data(right_str))) {
      LOG_WARN("fail to get new inrow data", K(ret), K(lob_left));
    } else {
      result = (0 == MEMCMP(left_str.ptr(), right_str.ptr(), left_str.length()));
    }
  } else if (OB_FAIL(compare(lob_left, lob_right, cmp_params, cmp_res))) {
    LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right));
  } else {
    result = (0 == cmp_res);
  }
  return ret;
}

int ObLobManager::compare(ObLobLocatorV2& lob_left,
                          ObLobLocatorV2& lob_right,
                          ObLobCompareParams& cmp_params,
                          int64_t& result) {
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator("LobCmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
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
    uint64_t read_buff_size = OB_MALLOC_MIDDLE_BLOCK_SIZE; // 64KB
    char *read_buff = nullptr;
    char *charset_convert_buff_ptr = nullptr;
    bool need_convert_charset = (collation_left != CS_TYPE_BINARY);
    uint64_t charset_convert_buff_size = need_convert_charset ?
                                         read_buff_size * ObCharset::CharConvertFactorNum : 0;

    if (OB_ISNULL((read_buff = static_cast<char*>(tmp_allocator->alloc(read_buff_size * 2))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
    } else if (need_convert_charset &&
               OB_ISNULL((charset_convert_buff_ptr = static_cast<char*>(tmp_allocator->alloc(charset_convert_buff_size))))) {
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
          } else if (need_convert_charset) {
            // convert right lob to left charset if necessary
            if(OB_FAIL(ObExprUtil::convert_string_collation(
                                  read_buffer_right, collation_right,
                                  convert_buffer_right, cmp_collation,
                                  charset_convert_buff))) {
                LOG_WARN("fail to convert string collation", K(ret),
                          K(read_buffer_right), K(collation_right),
                          K(convert_buffer_right), K(cmp_collation));
            }
          } else {
            convert_buffer_right.assign_ptr(read_buffer_right.ptr(), read_buffer_right.length());
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
  if (param.is_store_char_len_ && meta_info.char_len_ > meta_info.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(meta_info));
  } else if (0 == meta_row.byte_len_ || meta_row.byte_len_ != meta_row.lob_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("byte length invalid", K(ret), K(meta_row));
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
  if (!param.is_store_char_len_ && lob_handle_has_char_len_field(param)) {
    old_meta_info.char_len_ = UINT32_MAX;
  }
  if (param.is_store_char_len_ && new_meta_info.char_len_ > new_meta_info.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(new_meta_info));
  } else if (! param.is_store_char_len_ && new_meta_info.char_len_ != UINT32_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char length invalid", K(ret), K(new_meta_info), K(param));
  } else if (0 == new_meta_info.byte_len_ || new_meta_info.byte_len_ != new_meta_info.lob_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("byte length invalid", K(ret), K(new_meta_info));
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
  if (!lob_handle_has_char_len(param) && lob_handle_has_char_len_field(param)) {
    meta_info.char_len_ = UINT32_MAX;
  }
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

void ObLobManager::transform_lob_id(uint64_t src, uint64_t &dst)
{
  dst = htonll(src << 1);
  char *bytes = reinterpret_cast<char*>(&dst);
  bytes[7] |= 0x01;
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
  need_out_row = (param.byte_size_ + add_len) > param.get_inrow_threshold();
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
    } else if (param.byte_size_ > 0) {
      LOG_DEBUG("update keey outrow ", K(param.byte_size_), K(add_len), KPC(param.lob_common_),KPC(param.lob_data_));
      need_out_row = true;
    } else {
      // currently only insert support outrow -> inrow
      LOG_DEBUG("insert outrow to inrow", K(param.byte_size_), K(add_len), KPC(param.lob_common_),KPC(param.lob_data_));
      ObLobCommon *lob_common = nullptr;
      if (OB_ISNULL(lob_common = OB_NEWx(ObLobCommon, param.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), "size", sizeof(ObLobCommon));
      } else {
        lob_common->in_row_ = 1;
        param.lob_common_ = lob_common;
        param.lob_data_ = nullptr;
        param.lob_locator_ = nullptr;
        param.handle_size_ = sizeof(ObLobCommon);
      }
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
      } else if (OB_FAIL(is_store_char_len(param, param.get_schema_chunk_size(), add_len))) {
        LOG_WARN("cacl is_store_char_len failed.", K(ret), K(add_len), K(param));
      } else {
        MEMCPY(buf, param.lob_common_, sizeof(ObLobCommon));
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        if (new_lob_common->is_init_) {
          MEMCPY(new_lob_common->buffer_, param.lob_common_->buffer_, sizeof(ObLobData));
        } else {
          // init lob data and alloc lob id(when not init)
          ObLobData *new_lob_data = new(new_lob_common->buffer_)ObLobData();
          new_lob_data->id_.tablet_id_ = param.tablet_id_.id();
          if (param.spec_lob_id_.is_valid()) {
            new_lob_data->id_ = param.spec_lob_id_;
          } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, new_lob_data->id_.lob_id_))) {
            LOG_WARN("get lob id failed.", K(ret), K(param));
          }
          if (OB_SUCC(ret)) {
            transform_lob_id(new_lob_data->id_.lob_id_, new_lob_data->id_.lob_id_);
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
          ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
          // init char len
          uint64_t *char_len = reinterpret_cast<uint64_t*>(ctx + 1);
          *char_len = (param.is_store_char_len_) ? 0 : UINT64_MAX;
          param.handle_size_ = LOB_OUTROW_FULL_SIZE;
        }
      }
    }
  } else if (! param.lob_common_->in_row_ && need_out_row) {
    // outrow -> outrow : keep outrow
    int64_t store_chunk_size = 0;
    bool has_char_len = lob_handle_has_char_len(param);
    if (add_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add_len is negative", K(ret), K(param));
    } else if (add_len == 0) {
      // no data add, keep char_len state
      param.is_store_char_len_ = has_char_len;
    } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
      LOG_WARN("get_store_chunk_size fail", K(ret), K(param));
    } else if (OB_FAIL(is_store_char_len(param, store_chunk_size, add_len))) {
      LOG_WARN("cacl is_store_char_len failed.", K(ret), K(store_chunk_size), K(add_len), K(param));
    } else if (param.op_type_ != ObLobDataOutRowCtx::OpType::SQL) {
      if (! param.is_store_char_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret), K(param), K(has_char_len));
      }
    } else if (0 != param.offset_ || 0 != param.byte_size_) {
      if (! param.is_store_char_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret), K(param), K(has_char_len));
      }
    } else if (has_char_len && param.is_store_char_len_) {
      // keep char_len
    } else if (! has_char_len && ! param.is_store_char_len_) {
      // keep no char_len
    } else if (has_char_len && ! param.is_store_char_len_) {
      // old data has char , but new data no char_len
      // reset char_len to UINT64_MAX from 0
      int64_t *char_len = ObLobManager::get_char_len_ptr(param);
      if (OB_ISNULL(char_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("char_len ptr is null", K(ret), K(param), K(has_char_len));
      } else if (*char_len != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("char_len should be zero", K(ret), K(param), K(has_char_len), K(*char_len));
      } else {
        *char_len = UINT64_MAX;
        LOG_DEBUG("has_char_len to no_char_len", K(param));
      }
    } else if (! has_char_len && param.is_store_char_len_) {
      if (param.handle_size_ < LOB_OUTROW_FULL_SIZE) {
        LOG_INFO("old old data", K(param));
        param.is_store_char_len_ = true;
      } else if (param.is_full_insert()) {
        // reset char_len to 0 from UINT64_MAX
        int64_t *char_len = ObLobManager::get_char_len_ptr(param);
        if (OB_ISNULL(char_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("char_len ptr is null", K(ret), K(param), K(has_char_len));
        } else if (*char_len != UINT64_MAX) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("char_len should be zero", K(ret), K(param), K(has_char_len), K(*char_len));
        } else {
          *char_len = 0;
          LOG_DEBUG("no_char_len to has_char_len", K(param));
        }
      } else {
        // partial update aloways store char_len beacaure is only support in oracle mode
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupport situation", K(ret), K(param), K(has_char_len));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unkown situation", K(ret), K(param), K(has_char_len));
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
    int64_t N = 0;
    // use store chunk size for erase, append, partial update
    if (param.has_store_chunk_size()) {
      int64_t store_chunk_size = 0;
      if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
        LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
      } else {
        N += ((len + param.update_len_) / (store_chunk_size / 2) + 2);
      }
    } else {
      LOG_DEBUG("no store chunk size", K(param));
    }
    if (OB_SUCC(ret)) {
      // use shema chunk size for full insert and default
      N += ((len + param.update_len_) / (param.get_schema_chunk_size() / 2) + 2);
      if (nullptr != param.tx_desc_) {
        if (OB_FAIL(param.tx_desc_->get_and_inc_tx_seq(param.parent_seq_no_.get_branch(),
                                                       N,
                                                       param.seq_no_st_))) {
          LOG_WARN("get and inc tx seq failed", K(ret), K(N));
        }
      } else {
        // do nothing, for direct load has no tx desc, do not use seq no
        LOG_DEBUG("tx_desc is null", K(param));
      }
      param.used_seq_cnt_ = 0;
      param.total_seq_cnt_ = N;
    }
  }
  if (OB_SUCC(ret)) {
    out_row_ctx->seq_no_st_ = param.seq_no_st_.cast_to_int();
    if (ObLobDataOutRowCtx::OpType::SQL == op) {
      out_row_ctx->is_full_ = 1;
    } else {
      out_row_ctx->is_full_ = 0;
    }
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

  if (ObLobDataOutRowCtx::OpType::DIFF == out_row_ctx->op_) {
  } else {
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
  }
  return ret;
}

int ObLobManager::append(
    ObLobAccessParam& param,
    ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("LobTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  param.set_tmp_allocator(&tmp_allocator);

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
    } else if (OB_ISNULL(lob_common = param.lob_common_)) { // check_need_out_row may change lob_common
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_commob is nul", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(lob));
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
          if (OB_FAIL(build_lob_param(read_param, *param.get_tmp_allocator(), param.coll_type_,
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
    } else if (OB_FAIL(append_outrow(param, lob, append_lob_len, ori_inrow_data))) {
      LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(append_lob_len), K(ori_inrow_data));
    } else if (OB_FAIL(check_write_length(param, append_lob_len))) {
      LOG_WARN("check_write_length fail", K(ret), K(param), K(lob), K(append_lob_len));
    }
  }
  param.set_tmp_allocator(nullptr);
  return ret;
}

int ObLobManager::append(ObLobAccessParam& param, ObLobLocatorV2& lob, ObLobMetaWriteIter &iter)
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
  } else if (lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
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
        iter.set_end();
      }
    } else if (!lob.has_lob_header()) {
      ObString data;
      data.assign_ptr(lob.ptr_, lob.size_);
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(lob_ctx.lob_meta_mngr_->append(param, iter))) {
        LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param));
      }
    } else {
      // prepare out row ctx
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(init_out_row_ctx(param, append_lob_len, param.op_type_))) {
        LOG_WARN("init lob data out row ctx failed", K(ret));
      }
      // prepare read buffer
      ObString read_buffer;
      uint64_t read_buff_size = OB_MIN(LOB_READ_BUFFER_LEN, append_lob_len);
      char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(read_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
      } else {
        read_buffer.assign_buffer(read_buff, read_buff_size);
      }

      // prepare read full lob
      if (OB_SUCC(ret)) {
        ObLobAccessParam *read_param = reinterpret_cast<ObLobAccessParam*>(param.allocator_->alloc(sizeof(ObLobAccessParam)));
        if (OB_ISNULL(read_param)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc read param failed.", K(ret), K(sizeof(ObLobAccessParam)));
        } else {
          read_param = new(read_param)ObLobAccessParam();
          read_param->tx_desc_ = param.tx_desc_;
          read_param->tenant_id_ = param.src_tenant_id_;
          if (OB_FAIL(build_lob_param(*read_param, *param.allocator_, param.coll_type_,
                      0, UINT64_MAX, param.timeout_, lob))) {
            LOG_WARN("fail to build read param", K(ret), K(lob));
          } else {
            ObLobQueryIter *qiter = nullptr;
            if (OB_FAIL(query(*read_param, qiter))) {
              LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
            } else if (OB_FAIL(iter.open(param, qiter, read_param, read_buffer))) {
              LOG_WARN("open lob meta write iter failed.", K(ret));
            }
          }
        }
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
      if (param.spec_lob_id_.is_valid()) {
        param.lob_data_->id_ = param.spec_lob_id_;
      }
      ObLobDataOutRowCtx *outrow_ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
      outrow_ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
      // init char len
      uint64_t *char_len = reinterpret_cast<uint64_t*>(outrow_ctx + 1);
      *char_len = 0;
      param.handle_size_ = LOB_OUTROW_FULL_SIZE;
      alloc_inside = true;
    }
  } else if (param.lob_common_->is_init_) {
    param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);

    if (0 == param.lob_data_->byte_size_) {
      // that is insert when lob_data_->byte_size_ is zero.
      // so should update chunk size
      ObLobDataOutRowCtx *outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(param.lob_data_->buffer_);
      outrow_ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
    }
  }
  return ret;
}

int ObLobManager::batch_insert(ObLobAccessParam& param, ObLobMetaWriteIter &iter)
{
  int ret = OB_SUCCESS;
  ObLobPersistInsertIter insert_iter;
  if (OB_FAIL(insert_iter.init(&param, &iter))) {
    LOG_WARN("init insert iter fail", K(ret));
  } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->batch_insert(param, insert_iter))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  }
  return ret;
}

int ObLobManager::append(
    ObLobAccessParam& param,
    ObString& data)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("LobTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  param.set_tmp_allocator(&tmp_allocator);
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
    bool ori_is_inrow = (lob_common == nullptr) ? false : (lob_common->in_row_ == 1);
    common::ObAddr dst_addr;
    int64_t store_chunk_size = 0;
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
    } else if (OB_ISNULL(lob_common = param.lob_common_)) { // check_need_out_row may change lob_common
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_commob is nul", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(data));
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
    } else if (OB_FAIL(append_outrow(param, ori_is_inrow, data))) {
      LOG_WARN("append_outrow fail", K(ret), K(param), K(data));
    } else if (OB_FAIL(check_write_length(param, data.length()))) {
      LOG_WARN("check_write_length fail", K(ret), K(param), K(data.length()));
    }
  }
  if (OB_SUCC(ret)) {
    param.len_ = save_param_len;
    param.scan_backward_ = save_is_reverse;
  }
  param.set_tmp_allocator(nullptr);
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
  need_out_row = (total_size > param.get_inrow_threshold());
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
          if (param.spec_lob_id_.is_valid()) {
            new_lob_data->id_ = param.spec_lob_id_;
          } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, new_lob_data->id_.lob_id_))) {
            LOG_WARN("get lob id failed.", K(ret), K(param));
          }
          if (OB_SUCC(ret)) {
            transform_lob_id(new_lob_data->id_.lob_id_, new_lob_data->id_.lob_id_);
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
          case ObLobDiff::DiffType::WRITE_DIFF : {
            if (i != 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("first type must be write_diff", K(ret), K(i), K(diff_header), K(diffs[i]));
            } else if (OB_FAIL(process_diff(param, lob_locator, diff_header))) {
              LOG_WARN("process_diff fail", K(ret), K(param), K(i), K(*diff_header));
            } else {
              i = diff_header->diff_cnt_;
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

static int get_extra_diff_data(ObLobLocatorV2 &lob_locator, ObLobDiffHeader *diff_header, ObString &extra_diff_data)
{
  INIT_SUCC(ret);
  char *data_ptr = diff_header->get_inline_data_ptr();
  int64_t extra_data_len = lob_locator.size_ -  (data_ptr - lob_locator.ptr_);
  if (extra_data_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extra data length", K(ret), K(extra_data_len), K(lob_locator), K(*diff_header));
  } else {
    extra_diff_data.assign_ptr(data_ptr, extra_data_len);
  }
  return ret;
}

int ObLobManager::process_diff(ObLobAccessParam& param, ObLobLocatorV2& delta_locator, ObLobDiffHeader *diff_header)
{
  INIT_SUCC(ret);
  ObTabletID piece_tablet_id;
  ObLobPieceInfo piece_info;
  ObLobDiff *diffs = diff_header->get_diff_ptr();
  int64_t pos = 0;
  ObString new_lob_data(diff_header->persist_loc_size_, diff_header->data_);
  ObString extra_diff_data;
  int64_t store_chunk_size = 0;
  ObLobPartialUpdateRowIter iter;
  if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else if (OB_ISNULL(param.lob_common_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null lob common", K(ret), K(param));
  } else if (! param.lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob common not init", K(ret), KPC(param.lob_common_), K(param));
  } else if (param.coll_type_ != ObCollationType::CS_TYPE_BINARY) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("delta lob coll_type must be binary", K(ret), K(param));
  } else if (OB_ISNULL(param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_data_ is null", KR(ret), K(param));
  } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size)))  {
    LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
  } else if (OB_FAIL(get_extra_diff_data(delta_locator, diff_header, extra_diff_data))) {
    LOG_WARN("get_extra_diff_data", K(ret), K(param));
  } else if (OB_FAIL(iter.open(param, delta_locator, diff_header))) {
    LOG_WARN("open iter fail", K(ret), K(param), K(diff_header));
  } else if (iter.get_chunk_size() != store_chunk_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk size not match", K(ret), K(iter.get_chunk_size()), K(store_chunk_size), KPC(param.lob_common_), K(param));
  } else {
    int64_t seq_cnt = iter.get_modified_chunk_cnt();
    param.used_seq_cnt_ = 0;
    param.total_seq_cnt_ = seq_cnt;
    param.op_type_ = ObLobDataOutRowCtx::OpType::DIFF;
    if (OB_FAIL(param.tx_desc_->get_and_inc_tx_seq(param.parent_seq_no_.get_branch(),
                                                   seq_cnt,
                                                   param.seq_no_st_))) {
      LOG_WARN("get and inc tx seq failed", K(ret), K(seq_cnt));
    } else if (OB_FAIL(init_out_row_ctx(param, 0, param.op_type_))) {
      LOG_WARN("init lob data out row ctx failed", K(ret));
    }

    while(OB_SUCC(ret)) {
      ObLobMetaInfo *new_meta_row = nullptr;
      ObLobMetaInfo *old_meta_row = nullptr;
      int64_t offset = 0;
      if (OB_FAIL(iter.get_next_row(offset, old_meta_row, new_meta_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get_next_row fail", K(ret), K(param), K(diff_header));
        }
      } else if (OB_ISNULL(old_meta_row)) {
        int32_t seq_id_int = 0;
        ObString seq_id_st(sizeof(seq_id_int), reinterpret_cast<char*>(&seq_id_int));
        ObString seq_id_ed;
        ObLobMetaWriteIter write_iter(param.allocator_, store_chunk_size);
        ObString post_data;
        ObString remain_buf;
        if (OB_FAIL(ObLobSeqId::get_seq_id(offset/store_chunk_size - 1, seq_id_st))) {
          LOG_WARN("get_seq_id fail", K(ret), K(offset));
        } else if (OB_FAIL(write_iter.open(param, new_meta_row->lob_data_, 0, post_data, remain_buf, seq_id_st, seq_id_ed))) {
          LOG_WARN("failed to open meta writer", K(ret), K(write_iter), K(param.byte_size_), K(offset), K(store_chunk_size));
        } else if (OB_FAIL(write_outrow_result(param, write_iter))) {
          LOG_WARN("failed to write outrow result", K(ret), K(write_iter), K(param.byte_size_), K(offset), K(store_chunk_size));
        }
        write_iter.close();
      } else if (OB_FAIL(update_one_piece(
          param,
          lob_ctx_,
          *old_meta_row,
          *new_meta_row,
          piece_info,
          new_meta_row->lob_data_))) {
        LOG_WARN("update_one_piece fail", K(ret), K(offset), K(store_chunk_size));
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else {
    param.ext_info_log_.set_raw(extra_diff_data.ptr(), extra_diff_data.length());
  }
  return ret;
}

int ObLobManager::getlength_remote(ObLobAccessParam& param, common::ObAddr& dst_addr, uint64_t &len)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  ObLobQueryBlock header;
  if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob locator is null.", K(ret), K(param));
  } else if (OB_ISNULL(param.remote_query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get remote query ctx nullptr", K(ret), K(param));
  } else {
    ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(param.remote_query_ctx_);
    int64_t cur_position = remote_ctx->rpc_buffer_.get_position();
    while (OB_SUCC(ret) && remote_ctx->handle_.has_more()) {
      cur_position = remote_ctx->rpc_buffer_.get_position();
      if (OB_FAIL(remote_ctx->handle_.get_more(remote_ctx->rpc_buffer_))) {
        ret = OB_DATA_SOURCE_TIMEOUT;
      } else if (remote_ctx->rpc_buffer_.get_position() < 0) {
        ret = OB_ERR_SYS;
      } else if (cur_position == remote_ctx->rpc_buffer_.get_position()) {
        if (!remote_ctx->handle_.has_more()) {
          ret = OB_ITER_END;
          LOG_DEBUG("empty rpc buffer, no more data", K(remote_ctx->rpc_buffer_));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc buffer has no data", K(ret), K(remote_ctx->rpc_buffer_));
        }
      } else {
        LOG_DEBUG("get more data", K(remote_ctx->rpc_buffer_));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    // do header decode
    if (OB_SUCC(ret)) {
      int64_t rpc_buffer_pos = 0;
      if (OB_FAIL(serialization::decode(remote_ctx->rpc_buffer_.get_data(),
        remote_ctx->rpc_buffer_.get_position(), rpc_buffer_pos, header))) {
        LOG_WARN("failed to decode lob query block", K(ret), K(remote_ctx->rpc_buffer_));
      } else if (!header.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid header", K(ret), K(header));
      } else {
        len = static_cast<uint64_t>(header.size_);
      }
    }
  }
  return ret;
}

bool ObLobManager::lob_handle_has_char_len(ObLobAccessParam& param)
{
  bool bret = false;
  if (param.lob_common_ != nullptr && !param.lob_common_->in_row_ && param.handle_size_ >= LOB_OUTROW_FULL_SIZE) {
    char *ptr = reinterpret_cast<char*>(param.lob_common_);
    uint64_t *len = reinterpret_cast<uint64_t*>(ptr + LOB_WITH_OUTROW_CTX_SIZE);
    if (*len != UINT64_MAX) {
      bret = true;
    }
  }
  return bret;
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
      ObLobMetaScanIter meta_iter;
      param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
      // mock do full scan
      param.offset_ = 0;
      param.len_ = UINT64_MAX;
      if (OB_FAIL(is_remote(param, is_remote_lob, dst_addr))) {
        LOG_WARN("check is remote failed.", K(ret), K(param));
      } else if (OB_FAIL(lob_query_with_retry(param, dst_addr, is_remote_lob, meta_iter,
                         ObLobQueryArg::QueryType::GET_LENGTH, param.remote_query_ctx_))) {
        LOG_WARN("fail to do lob query with retry", K(ret), K(is_remote_lob), K(dst_addr));
      } else if (is_remote_lob) {
        if (OB_FAIL(getlength_remote(param, dst_addr, len))) {
          LOG_WARN("fail to get length remote", K(ret));
        }
      } else {
        if (!lob_common->is_init_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob common header for out row.", K(ret), KPC(lob_common));
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
      // release remote query resource
      if (OB_NOT_NULL(param.remote_query_ctx_)) {
        ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(param.remote_query_ctx_);
        remote_ctx->~ObLobRemoteQueryCtx();
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
        uint64_t read_buff_size = OB_MIN(LOB_READ_BUFFER_LEN, read_param.byte_size_);
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
        LOG_WARN("failed to get next row.", K(ret), K(cnt));
      }
    } else if (ObTimeUtility::current_time() > param.timeout_) {
      ret = OB_TIMEOUT;
      int64_t cur_time = ObTimeUtility::current_time();
      LOG_WARN("query timeout", K(cur_time), K(param.timeout_), K(ret));
    } else {
      cnt++;
      if (result.is_update_) {
        ObLobPieceInfo piece_info;
        if (OB_FAIL(update_one_piece(param,
                                      lob_ctx_,
                                      result.old_info_,
                                      result.info_,
                                      piece_info,
                                      result.data_))) {
          LOG_WARN("failed to update.", K(ret), K(cnt), K(result.info_), K(result.old_info_));
        }
      } else {
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
  }
  return ret;
}

int ObLobManager::write_outrow_inner(ObLobAccessParam& param, ObLobQueryIter *iter, ObString& read_buf, ObString& old_data)
{
  int ret = OB_SUCCESS;
  int64_t store_chunk_size = 0;
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
    } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
      LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
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
        if(OB_FAIL(prepare_data_buffer(param, remain_buf, store_chunk_size))) {
          LOG_WARN("fail to prepare buffers", K(ret), K(param));
        } else if(OB_FAIL(prepare_data_buffer(param, tmp_buf, store_chunk_size))) {
          LOG_WARN("fail to prepare buffers", K(ret), K(param));
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
              if (OB_SUCC(ret) && OB_FAIL(replace_process_meta_info(param, store_chunk_size, meta_iter, result, iter, read_buf, remain_buf, tmp_buf))) {
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
          // here has four situation
          // [old][new][old]       --> cover part of old data
          // [new_data][old_data]  --> cover front part
          // [old_data][new_data]  --> cover back part
          // [new_data]            --> full cover old data
          int64_t offset_byte_len = ObCharset::charpos(param.coll_type_,
                                                       old_data.ptr(),
                                                       old_data.length(),
                                                       param.offset_);
          if (offset_byte_len > 0) { // offset is not 0, must have some old data at front
            post_data.assign_ptr(old_data.ptr(), offset_byte_len);
          }
          if (param.offset_ + param.len_ < old_char_len) { // not full cover, must have some old data at back
            int64_t end_byte_len = ObCharset::charpos(param.coll_type_,
                                                      old_data.ptr(),
                                                      old_data.length(),
                                                      param.offset_ + param.len_);
            if (end_byte_len >= old_data.length()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get byte len is bigger then data length", K(ret), K(end_byte_len), K(old_data.length()), K(param));
            } else {
              remain_buf.assign_ptr(old_data.ptr() + end_byte_len, old_data.length() - end_byte_len);
            }
          }
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
      int64_t store_chunk_size = 0;
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

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
        LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
      } else {
        // prepare write iter
        ObLobMetaWriteIter write_iter(param.allocator_, store_chunk_size);
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
        uint64_t read_buff_size = OB_MIN(LOB_READ_BUFFER_LEN, read_param.byte_size_);
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
        uint64_t read_buff_size = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
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

int ObLobManager::batch_delete(ObLobAccessParam& param, ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  ObLobPersistDeleteIter delete_iter;
  iter.set_not_calc_char_len(true);
  iter.set_not_need_last_info(true);
  if (OB_FAIL(delete_iter.init(&param, &iter))) {
    LOG_WARN("init insert iter fail", K(ret));
  } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->batch_delete(param, delete_iter))) {
    LOG_WARN("write lob meta row failed.", K(ret));
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
  int64_t store_chunk_size = 0;
  if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
  } else if (OB_FAIL(init_out_row_ctx(param, param.lob_data_->byte_size_, param.op_type_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->scan(param, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param));
  } else if (param.is_full_delete()) {
    if (OB_FAIL(batch_delete(param, meta_iter))) {
      LOG_WARN("batch_delete fail", K(ret), K(param));
    }
  } else if(OB_FAIL(prepare_data_buffer(param, tmp_buff, store_chunk_size))) {
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
      } else if (OB_FAIL(erase_process_meta_info(param, store_chunk_size, meta_iter, result, tmp_buff))) {
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
    } else if (param.is_fill_zero_) {
      if (OB_FAIL(fill_outrow_with_zero(param))) {
        LOG_WARN("fill_outrow_with_zero fail", K(ret), K(param));
      }
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
        ObMemLobRetryInfo *retry_info = nullptr;
        ObMemLobExternHeader *extern_header = NULL;
        if (OB_FAIL(lob.get_tx_info(tx_info))) {
          LOG_WARN("failed to get tx info", K(ret), K(lob));
        } else if (OB_FAIL(lob.get_location_info(location_info))) {
          LOG_WARN("failed to get location info", K(ret), K(lob));
        } else if (OB_FAIL(lob.get_extern_header(extern_header))) {
          LOG_WARN("failed to get extern header", K(ret), K(lob));
        } else if (extern_header->flags_.has_retry_info_ && OB_FAIL(lob.get_retry_info(retry_info))) {
          LOG_WARN("failed to get retry info", K(ret), K(lob));
        } else {
          transaction::ObTxSEQ snapshot_tx_seq = transaction::ObTxSEQ::cast_from_int(tx_info->snapshot_seq_);
          if (OB_ISNULL(param.tx_desc_) ||
              param.tx_desc_->get_tx_id().get_id() == tx_info->snapshot_tx_id_ || // read in same tx
              (tx_info->snapshot_tx_id_ == 0 && !snapshot_tx_seq.is_valid() && tx_info->snapshot_version_ > 0)) { // read not in tx
            param.snapshot_.core_.version_.convert_for_tx(tx_info->snapshot_version_);
            param.snapshot_.core_.tx_id_ = tx_info->snapshot_tx_id_;
            param.snapshot_.core_.scn_ = snapshot_tx_seq;
            param.snapshot_.valid_ = true;
            param.snapshot_.source_ = transaction::ObTxReadSnapshot::SRC::LS;
            param.snapshot_.snapshot_lsid_ = share::ObLSID(location_info->ls_id_);
            if (OB_NOT_NULL(retry_info)) param.read_latest_ = retry_info->read_latest_;
            if (param.read_latest_ && OB_NOT_NULL(param.tx_desc_)) {
              // tx_info->snapshot_seq_ is seq_abs when read_latest is true
              param.snapshot_.core_.scn_ = param.tx_desc_->get_tx_seq(tx_info->snapshot_seq_);
            }
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

int ObLobManager::append_outrow(ObLobAccessParam& param, bool ori_is_inrow, ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo last_info;
  int64_t store_chunk_size = 0;
  bool need_get_last_info = ! (ori_is_inrow || param.byte_size_ == 0);
  if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
  } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
  } else if (OB_FAIL(init_out_row_ctx(param, data.length(), param.op_type_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (param.is_full_insert()) {
    ObLobMetaWriteIter iter(param.get_tmp_allocator(), store_chunk_size);
    if (OB_FAIL(iter.open(param, data))) {
      LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param));
    } else if (OB_FAIL(batch_insert(param, iter))) {
      LOG_WARN("batch_insert fail", K(ret), K(param));
    }
  } else { // outrow
    ObLobMetaWriteIter iter(param.get_tmp_allocator(), store_chunk_size);
    if (OB_FAIL(iter.open(param, data, need_get_last_info ? lob_ctx_.lob_meta_mngr_ : nullptr))) {
      LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param));
    } else if (OB_FAIL(write_outrow_result(param, iter))) {
      LOG_WARN("write_outrow_result fail", K(ret), K(param));
    }
  }
  return ret;
}

int ObLobManager::append_outrow(
    ObLobAccessParam& param,
    ObLobLocatorV2& lob,
    int64_t append_lob_len,
    ObString& ori_inrow_data)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  ObString read_buffer;
  int64_t store_chunk_size = 0;
  bool need_get_last_info = ! (ori_inrow_data.length() > 0 || param.byte_size_ == 0);
  if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
  } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
  } else if (OB_FAIL(init_out_row_ctx(param, append_lob_len, param.op_type_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if(OB_FAIL(prepare_data_buffer(param, read_buffer, OB_MIN(append_lob_len, store_chunk_size)))) {
    LOG_WARN("fail to prepare reaed buffer", K(ret), K(param));
  } else {
    SMART_VAR(ObLobAccessParam, read_param) {
      read_param.tx_desc_ = param.tx_desc_;
      read_param.tenant_id_ = param.src_tenant_id_;
      if (OB_FAIL(build_lob_param(read_param, *param.get_tmp_allocator(), param.coll_type_,
                  0, UINT64_MAX, param.timeout_, lob))) {
        LOG_WARN("fail to build read param", K(ret), K(lob));
      } else {
        ObLobQueryIter *iter = nullptr;
        if (OB_FAIL(query(read_param, iter))) {
          LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
        } else {
          ObLobMetaWriteIter write_iter(param.get_tmp_allocator(), store_chunk_size);
          ObString remain_buf;
          ObString seq_id_st;
          ObString seq_id_ed;
          if (OB_FAIL(write_iter.open(
              param, iter, read_buffer, 0/*padding_size*/,
              ori_inrow_data, remain_buf, seq_id_st, seq_id_ed,
              need_get_last_info ? lob_ctx.lob_meta_mngr_ : nullptr))) {
            LOG_WARN("failed to open meta writer", K(ret), K(param), K(write_iter));
          } else if (OB_FAIL(write_outrow_result(param, write_iter))) {
            LOG_WARN("failed to write outrow result", K(ret), K(param), K(write_iter));
          }
        }
        if (OB_NOT_NULL(iter)) {
          iter->reset();
          OB_DELETE(ObLobQueryIter, "unused", iter);
        }
      }
    }
  }
  return ret;
}

int ObLobManager::fill_outrow_with_zero(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
  ObLobMetaScanIter meta_iter;
  ObString write_data_buffer;
  int64_t store_chunk_size = 0;
  if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", KR(ret), K(param));
  } else if (OB_FAIL(init_out_row_ctx(param, param.lob_data_->byte_size_, param.op_type_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->scan(param, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param));
  } else if(OB_FAIL(prepare_data_buffer(param, write_data_buffer, store_chunk_size))) {
    LOG_WARN("fail to prepare buffers", K(ret), K(param));
  } else {
    ObLobQueryResult result;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(meta_iter.get_next_row(result.meta_result_))) {
        if (ret == OB_ITER_END) {
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else if (OB_FAIL(param.is_timeout())) {
        LOG_WARN("access timeout", K(ret), K(param));
      } else if (OB_FAIL(do_fill_outrow_with_zero(param, store_chunk_size, meta_iter, result, write_data_buffer))) {
        LOG_WARN("process erase meta info failed.", K(ret), K(param), K(result));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobManager::do_fill_outrow_with_zero(
    ObLobAccessParam& param,
    const int64_t store_chunk_size,
    ObLobMetaScanIter &meta_iter,
    ObLobQueryResult &result,
    ObString &write_buf)
{
  int ret = OB_SUCCESS;
  bool is_char = param.is_char();

  ObLobWriteBuffer buffer(param.coll_type_, store_chunk_size, param.is_store_char_len_);

  int64_t cur_piece_end = meta_iter.get_cur_pos();
  int64_t cur_piece_begin = cur_piece_end - (is_char ? result.meta_result_.info_.char_len_ : result.meta_result_.info_.byte_len_);

  int64_t piece_write_begin = param.offset_ > cur_piece_begin ? param.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param.offset_ + param.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param.offset_ + param.len_ - cur_piece_begin;

  uint32_t piece_byte_len = result.meta_result_.info_.byte_len_;
  uint32_t piece_char_len = result.meta_result_.info_.char_len_;

  ObLobMetaInfo new_meta_row = result.meta_result_.info_;

  if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
    LOG_WARN("set_buffer fail", K(ret));
  } else if (OB_FAIL(buffer.append(result.meta_result_.info_.lob_data_.ptr(), result.meta_result_.info_.lob_data_.length()))) {
    LOG_WARN("append fail", K(ret), K(param));
  } else if (OB_FAIL(buffer.char_fill_zero(
      piece_write_begin,
      piece_write_end - piece_write_begin))) {
    LOG_WARN("fill_zero empty", K(ret), K(result));
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  } else if (OB_FAIL(update_one_piece(
        param,
        lob_ctx_,
        result.meta_result_.info_,
        new_meta_row,
        result.piece_info_,
        new_meta_row.lob_data_))) {
    LOG_WARN("update_one_piece fail", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  }
  return ret;
}

int ObLobManager::replace_process_meta_info(ObLobAccessParam& param,
                                            const int64_t store_chunk_size,
                                            ObLobMetaScanIter &meta_iter,
                                            ObLobQueryResult &result,
                                            ObLobQueryIter *iter,
                                            ObString& read_buf,
                                            ObString &remain_data,
                                            ObString &write_buf)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  bool is_char = param.is_char();
  bool del_piece = false;
  ObString temp_read_buf;
  ObLobMetaInfo new_meta_row = result.meta_result_.info_;

  ObLobWriteBuffer buffer(param.coll_type_, store_chunk_size, param.is_store_char_len_);

  uint64_t cur_piece_end = meta_iter.get_cur_pos();
  uint64_t cur_piece_begin = cur_piece_end - (is_char ? result.meta_result_.info_.char_len_ : result.meta_result_.info_.byte_len_);

  int64_t piece_write_begin = param.offset_ > cur_piece_begin ? param.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param.offset_ + param.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param.offset_ + param.len_ - cur_piece_begin;

  uint32_t piece_byte_len = result.meta_result_.info_.byte_len_;
  uint32_t piece_char_len = result.meta_result_.info_.char_len_;

  int64_t replace_byte_st = ObCharset::charpos(param.coll_type_, result.meta_result_.info_.lob_data_.ptr(), result.meta_result_.info_.lob_data_.length(), piece_write_begin);
  int64_t temp_read_size = store_chunk_size - replace_byte_st;
  temp_read_buf.assign_buffer(read_buf.ptr(), temp_read_size);
  if (iter->is_end()) {
  } else if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do get next read buffer", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (iter->is_end() && piece_write_end - piece_write_begin == piece_char_len) {
    del_piece = true;
  } else if (piece_write_end - piece_write_begin != piece_char_len) {
    // use buffer because need concat data
    if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
      LOG_WARN("set_buffer fail", K(ret));
    } else if (OB_FAIL(buffer.append(result.meta_result_.info_.lob_data_.ptr(), result.meta_result_.info_.lob_data_.length()))) {
      LOG_WARN("append fail", K(ret), K(param));
    }
  }

  if (OB_FAIL(ret) || del_piece) {
  } else if (OB_FAIL(buffer.char_write(piece_write_begin, piece_write_end - piece_write_begin, temp_read_buf, remain_data))) {
    LOG_WARN("char_write fail", K(ret), K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (del_piece) {
    if (OB_FAIL(erase_one_piece(param,
                                lob_ctx,
                                result.meta_result_.info_,
                                result.piece_info_))) {
      LOG_WARN("failed erase one piece", K(ret), K(result),
          K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
    }
  } else if (OB_FAIL(update_one_piece(param,
                                  lob_ctx,
                                  result.meta_result_.info_,
                                  new_meta_row,
                                  result.piece_info_,
                                  new_meta_row.lob_data_))) {
    LOG_WARN("failed to update.", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
  }
  return ret;
}

int ObLobManager::erase_process_meta_info(ObLobAccessParam& param, const int64_t store_chunk_size, ObLobMetaScanIter &meta_iter,
    ObLobQueryResult &result, ObString &write_buf)
{
  int ret = OB_SUCCESS;
  ObLobCtx lob_ctx = lob_ctx_;
  bool is_char = param.is_char();
  bool del_piece = false;

  uint64_t cur_piece_end = meta_iter.get_cur_pos();
  uint64_t cur_piece_begin = cur_piece_end - (is_char ? result.meta_result_.info_.char_len_ : result.meta_result_.info_.byte_len_);

  int64_t piece_write_begin = param.offset_ > cur_piece_begin ? param.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param.offset_ + param.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param.offset_ + param.len_ - cur_piece_begin;

  uint32_t piece_byte_len = result.meta_result_.info_.byte_len_;
  uint32_t piece_char_len = result.meta_result_.info_.char_len_;

  ObLobMetaInfo new_meta_row = result.meta_result_.info_;

  ObLobWriteBuffer buffer(param.coll_type_, store_chunk_size, param.is_store_char_len_);

  if (piece_write_end - piece_write_begin == piece_char_len) {
    del_piece = true;
    LOG_DEBUG("just delete", K(piece_write_begin), K(piece_write_end), K(piece_char_len));
  } else if (meta_iter.is_range_begin(result.meta_result_.info_)) {
    if (OB_FAIL(buffer.char_append(result.meta_result_.info_.lob_data_, 0/*char_offset*/, piece_write_begin /*char_len*/))) { // is_range_begin
      LOG_WARN("char_append fail", K(ret));
    }
  } else if (meta_iter.is_range_end(result.meta_result_.info_)) {
    // use buffer because need copy data
    if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
      LOG_WARN("set_buffer fail", K(ret));
    } else if (OB_FAIL(buffer.char_append(result.meta_result_.info_.lob_data_, piece_write_begin, piece_write_end - piece_write_begin))) {
      LOG_WARN("char_append fail", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unkown piece fail", K(ret), K(result), K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  } else if (del_piece) {
    if (OB_FAIL(erase_one_piece(param, lob_ctx, result.meta_result_.info_, result.piece_info_))) {
      LOG_WARN("failed erase one piece", K(ret), K(result),
          K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
    }
  } else if (OB_FAIL(update_one_piece(param,
                                  lob_ctx,
                                  result.meta_result_.info_,
                                  new_meta_row,
                                  result.piece_info_,
                                  new_meta_row.lob_data_))) {
    LOG_WARN("failed to update.", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
  }
  return ret;
}

/*************ObLobQueryIter*****************/
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

int ObLobQueryIter::open(ObLobAccessParam &param, ObLobCtx& lob_ctx, common::ObAddr &dst_addr, bool &is_remote)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_manager = MTL(ObLobManager*);
  if (OB_FAIL(lob_manager->lob_query_with_retry(param, dst_addr, is_remote, meta_iter_,
              ObLobQueryArg::QueryType::READ, remote_query_ctx_))) {
    LOG_WARN("fail to do lob query with retry", K(ret), K(is_remote), K(dst_addr));
  } else if (is_remote) { // init remote scan
    param_ = param;
    is_reverse_ = param.scan_backward_;
    cs_type_ = param.coll_type_;
    is_inited_ = true;
    is_remote_ = true;
  } else { // init local scan
    last_data_buf_len_ = OB_MIN(ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE, param.byte_size_);
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
        last_data_.assign_ptr(last_data_.ptr(), remain_size);
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
    ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(remote_query_ctx_);
    while (OB_SUCC(ret) && !has_fill_full) {
      // first try fill buffer remain data to output
      has_fill_full = fill_buffer_to_data(data);
      if (has_fill_full) {
        // data has been filled full, do nothing
      } else if (OB_FAIL(remote_ctx->remote_reader_.get_next_block(param_,
                         remote_ctx->rpc_buffer_, remote_ctx->handle_, block, last_data_))) {
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
  // release remote query resource
  if (OB_NOT_NULL(remote_query_ctx_)) {
    ObLobRemoteQueryCtx *remote_ctx = reinterpret_cast<ObLobRemoteQueryCtx*>(remote_query_ctx_);
    remote_ctx->~ObLobRemoteQueryCtx();
  }
}


/**********ObLobQueryRemoteReader****************/

int ObLobQueryRemoteReader::open(ObLobAccessParam& param, common::ObDataBuffer &rpc_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) {
    buf_len *= ObLobQueryArg::OB_LOB_QUERY_OLD_LEN_REFACTOR; // compat with old vesion
  }
  if (NULL == (buf = reinterpret_cast<char*>(param.allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (!rpc_buffer.set_data(buf, buf_len)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret));
  } else if (NULL == (buf = reinterpret_cast<char*>(param.allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
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

/********** ObLobCursor ****************/
ObLobCursor::~ObLobCursor()
{
  // meta_cache_.destroy();
  // modified_metas_.destroy();
  if (nullptr != param_) {
    param_->~ObLobAccessParam();
    param_ = nullptr;
  }
  if (nullptr != partial_data_) {
    partial_data_->~ObLobPartialData();
    partial_data_ = nullptr;
  }
}
int ObLobCursor::init(ObIAllocator *allocator, ObLobAccessParam* param, ObLobPartialData *partial_data, ObLobCtx &lob_ctx)
{
  int ret = OB_SUCCESS;
  param_ = param;
  allocator_ = allocator;
  partial_data_ = partial_data;
  update_buffer_.set_allocator(allocator_);
  if (OB_ISNULL(partial_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data is null", KR(ret));
  } else if (OB_FAIL(partial_data->get_ori_data_length(ori_data_length_))) {
    LOG_WARN("get_ori_data_length fail", KR(ret));
  } else if (partial_data->is_full_mode()) {
    if (OB_FAIL(init_full(allocator, partial_data))){
      LOG_WARN("init_full fail", KR(ret));
    }
  } else if (OB_FAIL(lob_ctx.lob_meta_mngr_->open(*param_, &getter_))) {
    LOG_WARN("ObLobMetaSingleGetter open fail", K(ret));
  }
  return ret;
}

int ObLobCursor::init_full(ObIAllocator *allocator, ObLobPartialData *partial_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_buffer_.append(partial_data->data_[0].data_))) {
    LOG_WARN("append data to update buffer fail", KR(ret), K(partial_data->data_.count()));
  } else {
    partial_data_->data_[0].data_ = update_buffer_.string();
    is_full_mode_ = true;
  }
  return ret;
}

int ObLobCursor::get_ptr(int64_t offset, int64_t len, const char *&ptr)
{
  INIT_SUCC(ret);
  ObString data;
  int64_t start_offset = offset;
  int64_t end_offset = offset + len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  if (start_chunk_pos != end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else if (OB_FAIL(get_chunk_data(start_chunk_pos, data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else if (data.empty() || data.length() < start_offset - get_chunk_offset(start_chunk_pos) + len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data not enough", KR(ret), K(offset), K(len), K(start_offset), "data_len", data.length());
  } else {
    ptr = data.ptr() + offset - get_chunk_offset(start_chunk_pos);
  }
  return ret;
}

int ObLobCursor::get_ptr_for_write(int64_t offset, int64_t len, char *&ptr)
{
  INIT_SUCC(ret);
  ObString data;
  int64_t start_offset = offset;
  int64_t end_offset = offset + len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  if (start_chunk_pos != end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else {
    for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      int chunk_idx = -1;
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i), K(start_chunk_pos), K(end_chunk_pos));
      } else if (OB_FAIL(record_chunk_old_data(chunk_idx))) {
        LOG_WARN("record_chunk_old_data fail", KR(ret), K(i));
      }
    }
    if(OB_FAIL(ret)) {
    } else if (OB_FAIL(get_chunk_data(start_chunk_pos, data))) {
      LOG_WARN("get_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
    } else if (data.empty() || data.length() < start_offset - get_chunk_offset(start_chunk_pos) + len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data not enough", KR(ret), K(offset), K(len), K(start_offset), "data_len", data.length());
    } else {
      ptr = data.ptr() + offset - get_chunk_offset(start_chunk_pos);
    }
  }
  return ret;
}

int ObLobCursor::get_chunk_data_start_pos(const int cur_chunk_pos, int &start_pos)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  if (OB_FAIL(get_chunk_data(cur_chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(cur_chunk_pos));
  } else {
    start_pos = get_chunk_pos(chunk_index->offset_ - chunk_index->pos_);
  }
  return ret;
}

int ObLobCursor::get_chunk_data_end_pos(const int cur_chunk_pos, int &end_pos)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  if (OB_FAIL(get_chunk_data(cur_chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(cur_chunk_pos));
  } else {
    end_pos = get_chunk_pos(chunk_index->offset_ - chunk_index->pos_ + chunk_data->data_.length() - 1);
  }
  return ret;
}

int ObLobCursor::merge_chunk_data(int start_chunk_pos, int end_chunk_pos)
{
  INIT_SUCC(ret);
  bool need_merge = false;
  ObSEArray<int, 10> chunk_idx_array;
  // get the fisrt and last chunk pos of chunk data that start_chunk_pos use
  if (OB_FAIL(get_chunk_data_start_pos(start_chunk_pos, start_chunk_pos))) {
    LOG_WARN("get_chunk_data_start_pos fail", KR(ret), K(start_chunk_pos));
  } else if (OB_FAIL(get_chunk_data_end_pos(end_chunk_pos, end_chunk_pos))) {
    LOG_WARN("get_chunk_data_start_pos fail", KR(ret), K(end_chunk_pos));
  }
  // get chunk_index data array index
  for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
    const ObLobChunkIndex *chunk_index = nullptr;
    const ObLobChunkData *chunk_data = nullptr;
    if (OB_FAIL(get_chunk_data(i, chunk_index, chunk_data))) {
      LOG_WARN("get_chunk_data fail", KR(ret), K(i), K(start_chunk_pos), K(end_chunk_pos));
    // some chunk share same data area, so no need push again
    // and it will only be shared with adjacent chunks, so only need to check the last
    } else if (! chunk_idx_array.empty() && chunk_idx_array[chunk_idx_array.count() - 1] == chunk_index->data_idx_) {// skip
    } else if (OB_FAIL(chunk_idx_array.push_back(chunk_index->data_idx_))) {
      LOG_WARN("push_back idx fail", KR(ret), K(i));
    }
  }
  int64_t merge_len = 0;
  bool use_update_buffer = false;
  if (OB_SUCC(ret)) {
    // should merge if has multi data area
    need_merge = chunk_idx_array.count() != 1;
    for (int i = 0; OB_SUCC(ret) && need_merge && i < chunk_idx_array.count(); ++i) {
      const ObLobChunkData &chunk_data = partial_data_->data_[chunk_idx_array[i]];
      merge_len += chunk_data.data_.length();

      if (! update_buffer_.empty() && update_buffer_.ptr() == chunk_data.data_.ptr()) {
        // if update_buffer_ is uesed, last chunk should have some pointer with update_buffer_
        if (i != chunk_idx_array.count() - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid chunk data", KR(ret), K(i), K(chunk_idx_array), K(start_chunk_pos), K(end_chunk_pos));
        } else {
          use_update_buffer = true;
          LOG_DEBUG("set use update buffer", K(i), K(chunk_idx_array.count()));
        }
      }
    }
  }

  // get merge buffer ptr
  char *buf = nullptr;
  if (OB_FAIL(ret) || ! need_merge) {
  } else if (use_update_buffer) {
    // old data also record in chunk data, so there just reset update_buffer_ but not free
    // and reserve new buffer for merge
    ObString old_data;
    if (OB_FAIL(update_buffer_.get_result_string(old_data))) {
      LOG_WARN("alloc fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array), K(update_buffer_));
    } else if (OB_FAIL(update_buffer_.reserve(merge_len))) {
      LOG_WARN("reserve buffer fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array), K(update_buffer_));
    } else {
      buf = update_buffer_.ptr();
      update_buffer_.set_length(merge_len);
    }
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_->alloc(merge_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array));
  }

  // do merge if need
  if (OB_FAIL(ret) || ! need_merge) {
  } else {
    int new_chunk_data_idx = chunk_idx_array[0];
    // copy data from old area to merge area
    int64_t pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < chunk_idx_array.count(); ++i) {
      ObLobChunkData &chunk_data = partial_data_->data_[chunk_idx_array[i]];
      MEMCPY(buf + pos, chunk_data.data_.ptr(), chunk_data.data_.length());
      pos += chunk_data.data_.length();
      allocator_->free(chunk_data.data_.ptr());
      chunk_data.data_.reset();
    }
    if (OB_SUCC(ret) && pos != merge_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data merge len incorrect", KR(ret), K(pos), K(merge_len));
    }
    // update chunk index offset info
    pos = 0;
    bool append_chunk_set = false;
    for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      int chunk_idx = -1;
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i));
      } else if (append_chunk_set && chunk_index(chunk_idx).is_add_) {
      } else {
        chunk_index(chunk_idx).pos_ = pos;
        chunk_index(chunk_idx).data_idx_ = new_chunk_data_idx;
        pos += chunk_index(chunk_idx).byte_len_;
        if (chunk_index(chunk_idx).is_add_) append_chunk_set = true;
      }
    }
    // update chunk data pointer
    if (OB_SUCC(ret)) {
      partial_data_->data_[new_chunk_data_idx].data_.assign_ptr(buf, merge_len);
    }
    // defensive check
    if (OB_SUCC(ret) && OB_FAIL(check_data_length())) {
      LOG_WARN("check len fail", KR(ret));
    }
  }
  return ret;
}

int ObLobCursor::check_data_length()
{
  INIT_SUCC(ret);
  int64_t check_data_len = 0;
  for (int i = 0; i < partial_data_->data_.count(); ++i) {
    check_data_len += partial_data_->data_[i].data_.length();
  }
  int64_t check_index_len = 0;
  for (int i = 0; i < partial_data_->index_.count(); ++i) {
    check_index_len += partial_data_->index_[i].byte_len_;
  }
  if (check_data_len != check_index_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check len fail", KR(ret), K(check_data_len), K(check_index_len));
  }
  return ret;
}

int ObLobCursor::get_chunk_data(int chunk_pos, ObString &data)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(get_chunk_data(chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(chunk_pos));
  // all append chunk will share same chunk index. so the real data pos need subtract append chunk offset
  // for normal exist chunk, get_chunk_offset(chunk_pos) is equal to chunk_index->offset_
  } else if (0 > (pos = chunk_index->pos_ + get_chunk_offset(chunk_pos) - chunk_index->offset_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", KR(ret), K(pos), K(chunk_index->pos_), K(get_chunk_offset(chunk_pos)), K(chunk_pos), K(chunk_index->offset_));
  } else {
    data.assign_ptr(chunk_data->data_.ptr() + pos, chunk_data->data_.length() - pos);
  }
  return ret;
}

int ObLobCursor::get_last_chunk_data_idx(int &chunk_idx)
{
  INIT_SUCC(ret);
  int chunk_pos = get_chunk_pos(partial_data_->data_length_ - 1);
  if (OB_FAIL(get_chunk_idx(chunk_pos, chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(partial_data_->data_length_), K(chunk_pos));
  }
  return ret;
}

bool ObLobCursor::is_append_chunk(int chunk_pos) const
{
  int64_t chunk_offset = get_chunk_offset(chunk_pos);
  return chunk_offset >= ori_data_length_;
}

int ObLobCursor::get_chunk_idx(int chunk_pos, int &chunk_idx)
{
  INIT_SUCC(ret);
  ObLobMetaInfo meta_info;
  ObLobChunkIndex new_chunk_index;
  ObLobChunkData chunk_data;
  int real_idx = -1;
  if (is_append_chunk(chunk_pos)) {
    int append_chunk_pos = get_chunk_pos(ori_data_length_ + partial_data_->chunk_size_ - 1);
    if (OB_FAIL(partial_data_->search_map_.get_refactored(append_chunk_pos, real_idx))) {
      LOG_WARN("get append chunk fail", KR(ret), K(chunk_pos), K(append_chunk_pos), K(ori_data_length_));
    } else {
      chunk_idx = real_idx;
    }
  } else if (OB_SUCC(partial_data_->search_map_.get_refactored(chunk_pos, real_idx))) {
    chunk_idx = real_idx;
  } else if (OB_FAIL(fetch_meta(chunk_pos, meta_info))) {
    LOG_WARN("fetch_meta fail", KR(ret), K(chunk_pos));
  // data return by storage points origin data memory
  // should copy if the data may be modified, or old data may be corrupted
  } else if (OB_FAIL(ob_write_string(*allocator_, meta_info.lob_data_, chunk_data.data_))) {
    LOG_WARN("copy data fail", KR(ret), K(chunk_pos), K(meta_info));
  } else if (OB_FAIL(ob_write_string(*allocator_, meta_info.seq_id_, new_chunk_index.seq_id_))) {
    LOG_WARN("copy seq_id data fail", KR(ret), K(chunk_pos), K(meta_info));
  } else if (OB_FAIL(partial_data_->data_.push_back(chunk_data))) {
    LOG_WARN("push_back data fail", KR(ret), K(chunk_pos), K(chunk_data));
  } else {
    new_chunk_index.offset_ = chunk_pos * partial_data_->chunk_size_;
    new_chunk_index.byte_len_ = meta_info.byte_len_;
    new_chunk_index.data_idx_ = partial_data_->data_.count() - 1;
    if (OB_FAIL(partial_data_->push_chunk_index(new_chunk_index))) {
      LOG_WARN("push_back index fail", KR(ret), K(chunk_pos), K(new_chunk_index));
    } else {
      chunk_idx = partial_data_->index_.count() - 1;
    }
  }
  return ret;
}

int ObLobCursor::get_chunk_data(int chunk_pos, const ObLobChunkIndex *&chunk_index, const ObLobChunkData *&chunk_data)
{
  INIT_SUCC(ret);
  int chunk_idx = -1;
  if (OB_FAIL(get_chunk_idx(chunk_pos, chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(chunk_pos));
  } else {
    chunk_index = &partial_data_->index_[chunk_idx];
    chunk_data = &partial_data_->data_[chunk_index->data_idx_];
  }
  return ret;
}


int ObLobCursor::get_chunk_pos(int64_t offset) const
{
  return offset / partial_data_->chunk_size_;
}

int64_t ObLobCursor::get_chunk_offset(int pos) const
{
  return pos * partial_data_->chunk_size_;
}

int ObLobCursor::fetch_meta(int idx, ObLobMetaInfo &meta_info)
{
  INIT_SUCC(ret);
  uint32_t seq_id_buf = 0;
  ObString seq_id(sizeof(uint32_t), (char*)(&seq_id_buf));
  if (OB_FAIL(ObLobSeqId::get_seq_id(idx, seq_id))) {
    LOG_WARN("get_seq_id fail", K(ret), K(idx));
  } else if (OB_FAIL(getter_.get_next_row(seq_id, meta_info))) {
    LOG_WARN("get_next_row fail", K(ret), K(seq_id));
  }
  return ret;
}

int ObLobCursor::append(const char* buf, int64_t buf_len)
{
  return set(partial_data_->data_length_, buf, buf_len);
}

int ObLobCursor::get_data(ObString &data) const
{
  INIT_SUCC(ret);
  if (is_full_mode()) {
    data = update_buffer_.string();
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLobCursor::reset_data(const ObString &data)
{
  INIT_SUCC(ret);
  if (is_full_mode()) {
    update_buffer_.reuse();
    ret = update_buffer_.append(data);
    partial_data_->data_[0].data_ = update_buffer_.string();
    partial_data_->data_length_ = update_buffer_.length();
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLobCursor::move_data_to_update_buffer(ObLobChunkData *chunk_data)
{
  INIT_SUCC(ret);
  if (update_buffer_.length() == 0) {
    if (OB_FAIL(update_buffer_.append(chunk_data->data_))) {
      LOG_WARN("update buffer reserve fail", KR(ret), KPC(chunk_data), K(update_buffer_));
    } else {
      chunk_data->data_ = update_buffer_.string();
    }
  } else if (update_buffer_.ptr() != chunk_data->data_.ptr() || update_buffer_.length() != chunk_data->data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update buffer state incorrect", KR(ret), K(update_buffer_), KPC(chunk_data));
  }
  return ret;
}

int ObLobCursor::push_append_chunk(int64_t append_len)
{
  INIT_SUCC(ret);
  int last_chunk_idx = -1;
  if (OB_FAIL(get_last_chunk_data_idx(last_chunk_idx))) {
    LOG_WARN("get_last_chunk_data fail", KR(ret));
  } else if (! chunk_index(last_chunk_idx).is_add_ && chunk_index(last_chunk_idx).byte_len_ + append_len > partial_data_->chunk_size_) {
    ObLobChunkIndex new_chunk_index;
    new_chunk_index.offset_ = chunk_index(last_chunk_idx).offset_ + partial_data_->chunk_size_;
    new_chunk_index.pos_ = chunk_index(last_chunk_idx).pos_ + partial_data_->chunk_size_;
    new_chunk_index.byte_len_ = 0;
    new_chunk_index.is_add_ = 1;
    new_chunk_index.data_idx_ = chunk_index(last_chunk_idx).data_idx_;
    if (OB_FAIL(record_chunk_old_data(last_chunk_idx))) {
      LOG_WARN("record_chunk_old_data fail", KR(ret), K(last_chunk_idx));
    } else if (OB_FAIL(partial_data_->push_chunk_index(new_chunk_index))) {
      LOG_WARN("push_back index fail", KR(ret), K(new_chunk_index));
    } else {
      // should be careful. this may cause check_data_length fail
      chunk_index(last_chunk_idx).byte_len_ = partial_data_->chunk_size_;
    }
  }
  return ret;
}

int ObLobCursor::set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove)
{
  INIT_SUCC(ret);
  int64_t start_offset = offset;
  int64_t end_offset = offset + buf_len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int old_end_chunk_pos = get_chunk_pos(partial_data_->data_length_ - 1);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  int64_t append_len = end_offset > partial_data_->data_length_ ? end_offset - partial_data_->data_length_ : 0;
  int start_chunk_idx = -1;
  if (start_chunk_pos < old_end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, old_end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(old_end_chunk_pos), K(offset), K(buf_len), K(end_chunk_pos));
  } else if (append_len > 0 && OB_FAIL(push_append_chunk(append_len))) {
    LOG_WARN("push_append_chunk fail", KR(ret), K(append_len));
  } else if (OB_FAIL(get_chunk_idx(start_chunk_pos, start_chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(start_chunk_pos));
  } else if (append_len > 0 && OB_FAIL(move_data_to_update_buffer(&chunk_data(start_chunk_idx)))) {
    LOG_WARN("move_data_to_update_buffer fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FAIL(update_buffer_.reserve(append_len))) {
    LOG_WARN("reserve fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FAIL(update_buffer_.set_length(update_buffer_.length() + append_len))) {
    LOG_WARN("set_length fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FALSE_IT(chunk_data(start_chunk_idx).data_ = update_buffer_.string())) {
  } else {
    for (int i = start_chunk_pos, chunk_idx = -1; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i));
      } else if (OB_FAIL(record_chunk_old_data(chunk_idx))) {
        LOG_WARN("record_chunk_old_data fail", KR(ret), K(i));
      } else if (i == end_chunk_pos && append_len > 0) {
        chunk_index(chunk_idx).byte_len_ = (end_offset - chunk_index(chunk_idx).offset_);
      }
    }
    if (OB_SUCC(ret)) {
      if (use_memmove) {
        MEMMOVE(chunk_data(start_chunk_idx).data_.ptr() + chunk_index(start_chunk_idx).pos_ + (start_offset - chunk_index(start_chunk_idx).offset_), buf, buf_len);
      } else {
        MEMCPY(chunk_data(start_chunk_idx).data_.ptr() + chunk_index(start_chunk_idx).pos_ + (start_offset - chunk_index(start_chunk_idx).offset_), buf, buf_len);
      }
      partial_data_->data_length_ += append_len;
    }
    // defensive check
    if (OB_SUCC(ret) && OB_FAIL(check_data_length())) {
      LOG_WARN("check len fail", KR(ret));
    }
  }
  return ret;
}

int ObLobCursor::record_chunk_old_data(int chunk_idx)
{
  INIT_SUCC(ret);
  ObLobChunkIndex &chunk_index = partial_data_->index_[chunk_idx];
  if (OB_FAIL(set_old_data(chunk_index))) {
    LOG_WARN("record_chunk_old_data fail", KR(ret), K(chunk_index));
  }
  return ret;
}

int ObLobCursor::set_old_data(ObLobChunkIndex &chunk_index)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (chunk_index.old_data_idx_ >= 0) { // has set old
  } else if (chunk_index.is_add_) { // add no old
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(chunk_index.byte_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", KR(ret), K(chunk_index));
  } else {
    const ObLobChunkData &chunk_data = partial_data_->data_[chunk_index.data_idx_];
    MEMCPY(buf, chunk_data.data_.ptr() + chunk_index.pos_, chunk_index.byte_len_);
    if (OB_FAIL(partial_data_->old_data_.push_back(ObLobChunkData(ObString(chunk_index.byte_len_, buf))))) {
      LOG_WARN("push_back fail", KR(ret), K(chunk_index));
    } else {
      chunk_index.old_data_idx_ = partial_data_->old_data_.count() - 1;
      chunk_index.is_modified_ = 1;
    }
  }
  return ret;
}

int ObLobCursor::get(int64_t offset, int64_t len, ObString &data) const
{
  INIT_SUCC(ret);
  const char *ptr = nullptr;;
  if (OB_FAIL(get_ptr(offset, len, ptr))) {
    LOG_WARN("get_ptr fail", KR(ret), K(offset), K(len), K(data.length()));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_ptr fail", KR(ret), K(offset), K(len), K(data.length()));
  } else {
    data.assign_ptr(ptr, len);
  }
  return ret;
}

// if lob has only one chunk and contains all data, will return true
bool ObLobCursor::has_one_chunk_with_all_data()
{
  bool res = false;
  if (OB_ISNULL(partial_data_)) {
  } else if (1 != partial_data_->index_.count()) {
  } else {
    res = (ori_data_length_ == chunk_data(0).data_.length());
  }
  return res;
}

int ObLobCursor::get_one_chunk_with_all_data(ObString &data)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(partial_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ is null", KR(ret), KPC(this));
  } else if (1 != partial_data_->index_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ has not only one chunk", KR(ret), K(partial_data_->index_.count()));
  } else if (ori_data_length_ != chunk_data(0).data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ data length incorrect", KR(ret), K(ori_data_length_), K(chunk_data(0)));
  } else {
    data = chunk_data(0).data_;
    LOG_DEBUG("get chunk data success", K(chunk_data(0)), K(data));
  }
  return ret;
}

/********** ObLobCursor ****************/

/********** ObLobPartialUpdateRowIter ****************/

ObLobPartialUpdateRowIter::~ObLobPartialUpdateRowIter()
{
}

int ObLobPartialUpdateRowIter::open(ObLobAccessParam &param, ObLobLocatorV2 &delta_lob, ObLobDiffHeader *diff_header)
{
  int ret = OB_SUCCESS;
  param_ = &param;
  delta_lob_ = delta_lob;
  char *buf = diff_header->data_;
  int64_t data_len = diff_header->persist_loc_size_;
  int64_t pos = 0;
  if (OB_FAIL(partial_data_.init())) {
    LOG_WARN("map create fail", K(ret));
  } else if (OB_FAIL(partial_data_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize partial data fail", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(partial_data_.sort_index())) {
    LOG_WARN("sort_index fail", K(ret), K(data_len), K(partial_data_));
  }
  return ret;
}

int ObLobPartialUpdateRowIter::get_next_row(int64_t &offset, ObLobMetaInfo *&old_info, ObLobMetaInfo *&new_info)
{
  int ret = OB_SUCCESS;
  bool found_row = false;
  for(; OB_SUCC(ret) && ! found_row && chunk_iter_ < partial_data_.index_.count(); ++chunk_iter_) {
    ObLobChunkIndex &idx =  partial_data_.index_[chunk_iter_];
    if (1 == idx.is_modified_ || 1 == idx.is_add_) {
      ObLobChunkData &chunk_data = partial_data_.data_[idx.data_idx_];
      found_row = true;
      ObString old_data;
      if (! idx.is_add_ && idx.old_data_idx_ >= 0) {
        old_data = partial_data_.old_data_[idx.old_data_idx_].data_;
      }
      if (! idx.is_add_) {
        if (OB_FAIL(ObLobMetaUtil::construct(
            *param_, param_->lob_data_->id_, idx.seq_id_,
            old_data.length(), old_data.length(), old_data,
            old_meta_info_))) {
          LOG_WARN("construct old lob_meta_info fail", K(ret), K(idx), K(old_data));
        } else if (OB_FAIL(ObLobMetaUtil::construct(
            *param_, param_->lob_data_->id_, idx.seq_id_,
            idx.byte_len_, idx.byte_len_,
            ObString(idx.byte_len_, chunk_data.data_.ptr() + idx.pos_),
            new_meta_info_))) {
          LOG_WARN("construct new lob_meta_info fail", K(ret), K(idx), K(chunk_data));
        } else {
          offset = idx.offset_;
          old_info = &old_meta_info_;
          new_info = &new_meta_info_;
        }
      } else if (OB_FAIL(ObLobMetaUtil::construct(
          *param_, param_->lob_data_->id_, ObString(),
          idx.byte_len_, idx.byte_len_,
          ObString(idx.byte_len_, chunk_data.data_.ptr() + idx.pos_),
          new_meta_info_))) {
        LOG_WARN("construct new lob_meta_info fail", K(ret), K(idx), K(chunk_data));
      } else {
        offset = idx.offset_;
        new_info = &new_meta_info_;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found_row) {
  } else if (chunk_iter_ == partial_data_.index_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}
/********** ObLobPartialUpdateRowIter ****************/

} // storage
} // oceanbase
