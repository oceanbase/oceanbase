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

#include "ob_lob_remote.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{


/**********ObLobQueryRemoteReader****************/

int ObLobQueryRemoteReader::open(ObLobAccessParam& param, common::ObDataBuffer &rpc_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
  if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_2_2_0) {
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

int ObLobQueryRemoteReader::get_next_block(common::ObDataBuffer &rpc_buffer,
                                           obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle,
                                           ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobQueryBlock block;
  if (OB_FAIL(do_fetch_rpc_buffer(rpc_buffer, handle))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer.get_data(),
      rpc_buffer.get_position(),
      rpc_buffer_pos_,
      block))) {
    STORAGE_LOG(WARN, "failed to decode macro block size", K(ret), K(rpc_buffer), K(rpc_buffer_pos_));
  } else if (data_buffer_.set_length(0) != 0) {
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
      } else if (OB_FAIL(do_fetch_rpc_buffer(rpc_buffer, handle))) {
        LOG_WARN("failed to fetch next buffer if need", K(ret), K(block));
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
              K(need_size), K(rpc_remain_size), K(copy_size), K(block), K(rpc_buffer), K(rpc_buffer_pos_));
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

int ObLobQueryRemoteReader::do_fetch_rpc_buffer(
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
      if (OB_FAIL(handle.get_more(rpc_buffer))) {
        LOG_WARN("get_more(send request) failed", K(ret), K(rpc_buffer_pos_));
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

int ObLobRemoteUtil::query(ObLobAccessParam& param, const ObLobQueryArg::QueryType qtype, const ObAddr &dst_addr, ObLobRemoteQueryCtx *&remote_ctx)
{
  int ret = OB_SUCCESS;
  if (param.from_rpc_ && ! param.enable_remote_retry_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("call from rpc, but remote again", K(ret), K(dst_addr), K(MTL_ID()), K(param));
  } else if (OB_FAIL(remote_query_init_ctx(param, qtype, remote_ctx))) {
    LOG_WARN("fail to init remote query ctx", K(ret));
  } else {
    ObLSService *ls_service = (MTL(ObLSService *));
    obrpc::ObStorageRpcProxy *svr_rpc_proxy = ls_service->get_storage_rpc_proxy();
    int64_t timeout = param.timeout_ - ObTimeUtility::current_time();
    if (timeout < ObStorageRpcProxy::STREAM_RPC_TIMEOUT) {
      timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
    }
    
    if (OB_FAIL(svr_rpc_proxy->to(dst_addr).by(remote_ctx->query_arg_.tenant_id_)
                    .dst_cluster_id(GCONF.cluster_id)
                    .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                    .timeout(timeout)
                    .lob_query(remote_ctx->query_arg_, remote_ctx->rpc_buffer_, remote_ctx->handle_))) {
      LOG_WARN("call rpc fail", K(ret), K(dst_addr), K(param));
    } else {
      LOG_TRACE("remote query start", KPC(param.lob_data_), K(remote_ctx->rpc_buffer_), K(dst_addr), K(qtype), K(timeout), K(lbt()));
    }
  }
  return ret;
}

int ObLobRemoteUtil::remote_query_init_ctx(ObLobAccessParam &param, const ObLobQueryArg::QueryType qtype, ObLobRemoteQueryCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObLobRemoteQueryCtx *remote_ctx = nullptr;
  if (ctx != nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null null", K(ret), K(param), KP(ctx), K(qtype));
  } else if (OB_ISNULL(param.lob_locator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob locator is null", K(ret), K(param));
  } else if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(param));
  } else if (OB_ISNULL(remote_ctx = OB_NEWx(ObLobRemoteQueryCtx, param.allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc lob remote query ctx", K(ret), K(param));
  } else if (OB_FAIL(remote_ctx->remote_reader_.open(param, remote_ctx->rpc_buffer_))) {
    LOG_WARN("fail to open lob remote reader", K(ret), K(param));
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
    remote_ctx->query_arg_.enable_remote_retry_ = param.is_across_tenant();
    //set ctx ptr
    ctx = remote_ctx;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(remote_ctx)) {
    remote_ctx ->~ObLobRemoteQueryCtx();
    remote_ctx = nullptr;
  }
  return ret;
}

} // storage
} // oceanbase