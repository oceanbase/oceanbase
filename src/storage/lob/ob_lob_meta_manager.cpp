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
#include "ob_lob_meta_manager.h"
#include "storage/lob/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

int ObLobMetaManager::write(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote write not support", K(ret), K(param));
  } else if (OB_FAIL(persistent_lob_adapter_.write_lob_meta(param, in_row))) {
    LOG_WARN("write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::batch_insert(ObLobAccessParam& param, blocksstable::ObDatumRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote write not support", K(ret), K(param));
  } else if (OB_FAIL(persistent_lob_adapter_.write_lob_meta(param, iter))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::batch_delete(ObLobAccessParam& param, blocksstable::ObDatumRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote delete not support", K(ret), K(param));
  } else if (OB_FAIL(persistent_lob_adapter_.erase_lob_meta(param, iter))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// append
int ObLobMetaManager::append(ObLobAccessParam& param, ObLobMetaWriteIter& iter)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  UNUSED(iter);
  return ret;
}

// generate LobMetaRow at specified range on demands
int ObLobMetaManager::insert(ObLobAccessParam& param, ObLobMetaWriteIter& iter)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  UNUSED(iter);
  return ret;
}
// rebuild specified range
int ObLobMetaManager::rebuild(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  return ret;
}
// get specified range LobMeta info
int ObLobMetaManager::scan(ObLobAccessParam& param, ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    if (OB_FAIL(remote_scan(param, iter))) {
      LOG_WARN("open remote lob scan iter failed.", K(ret), K(param));
    }
  } else if (OB_FAIL(local_scan(param, iter))) {
    LOG_WARN("open local lob scan iter failed.", K(ret), K(param));
  }
  return ret;
}


int ObLobMetaManager::local_scan(ObLobAccessParam& param, ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.open_local(param, &persistent_lob_adapter_))) {
    LOG_WARN("open lob scan iter failed.", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::remote_scan(ObLobAccessParam& param, ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.open_remote(param))) {
    LOG_WARN("open remote fail", K(ret), K(param));
    // reset iter for maybe has done alloc for iter
    iter.reset();
  }
  return ret;
}

int ObLobMetaManager::open(ObLobAccessParam &param, ObLobMetaSingleGetter* getter)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote not support", K(ret), K(param));
  } else if (OB_FAIL(getter->open(param, &persistent_lob_adapter_))) {
    LOG_WARN("open lob scan iter failed.", K(ret), K(param));
  }
  return ret;
}

// erase specified range
int ObLobMetaManager::erase(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote erase not support", K(ret), K(param));
  } else if (OB_FAIL(persistent_lob_adapter_.erase_lob_meta(param, in_row))) {
    LOG_WARN("erase lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// update specified range
int ObLobMetaManager::update(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote erase not support", K(ret), K(param));
  } else if (OB_FAIL(persistent_lob_adapter_.update_lob_meta(param, old_row, new_row))) {
    LOG_WARN("update lob meta failed.");
  }
  return ret;
}

int ObLobMetaManager::fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id)
{
  int ret = OB_SUCCESS;
  if (nullptr != param.lob_id_geneator_) {
    if (OB_FAIL(param.lob_id_geneator_->next_value(lob_id))) {
      LOG_WARN("fail to get next lob_id", K(ret), KPC(param.lob_id_geneator_));
    }
  } else if (OB_FAIL(persistent_lob_adapter_.fetch_lob_id(param, lob_id))) {
    LOG_WARN("fetch lob id failed.", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::getlength(ObLobAccessParam &param, uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  if (param.is_remote()) {
    if (OB_FAIL(getlength_remote(param, char_len))) {
      LOG_WARN("get length remote fail", K(ret), K(param));
    }
  } else if (OB_FAIL(getlength_local(param, char_len))) {
    LOG_WARN("get length local fail", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::getlength_local(ObLobAccessParam &param, uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  ObLobMetaScanIter meta_iter;
  ObLobMetaScanResult result;
  if (OB_FAIL(local_scan(param, meta_iter))) {
    LOG_WARN("open local lob scan iter failed.", K(ret), K(param));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(meta_iter.get_next_row(result))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next row.", K(ret));
      }
    } else if (OB_FAIL(param.is_timeout())) {
      LOG_WARN("access timeout", K(ret), K(param));
    } else {
      char_len += result.info_.char_len_;
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLobMetaManager::getlength_remote(ObLobAccessParam &param, uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  ObLobRemoteQueryCtx *remote_ctx = nullptr;
  if (OB_FAIL(ObLobRemoteUtil::query(param, ObLobQueryArg::QueryType::GET_LENGTH, param.addr_, remote_ctx))) {
    LOG_WARN("fail to init remote query ctx", K(ret));
  } else {
    ObLobQueryBlock header;
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
        char_len = static_cast<uint64_t>(header.size_);
      }
    }
  }

  if (OB_NOT_NULL(remote_ctx)) {
    remote_ctx->~ObLobRemoteQueryCtx();
    remote_ctx = nullptr;
  }
  return ret;
}


} // storage
} // oceanbase
