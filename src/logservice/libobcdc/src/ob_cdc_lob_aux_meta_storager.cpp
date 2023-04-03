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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_meta_storager.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
int LobAuxMetaKey::compare(const LobAuxMetaKey &other) const
{
  int cmp_ret = 0;

  if (tenant_id_ > other.tenant_id_) {
    cmp_ret = 1;
  } else if (tenant_id_ < other.tenant_id_) {
    cmp_ret = -1;
  } else if (trans_id_ > other.trans_id_) {
    cmp_ret = 1;
  } else if (trans_id_ < other.trans_id_) {
    cmp_ret = -1;
  } else if (aux_lob_meta_tid_ > other.aux_lob_meta_tid_) {
    cmp_ret = 1;
  } else if (aux_lob_meta_tid_ < other.aux_lob_meta_tid_) {
    cmp_ret = -1;
  } else if (lob_id_ > other.lob_id_) {
    cmp_ret = 1;
  } else if (lob_id_ < other.lob_id_) {
    cmp_ret = -1;
  } else if (seq_no_ > other.seq_no_) {
    cmp_ret = 1;
  } else if (seq_no_ < other.seq_no_) {
    cmp_ret = -1;
  }

  return cmp_ret;
}

ObCDCLobAuxMetaStorager::ObCDCLobAuxMetaStorager() :
    is_inited_(false),
    lob_aux_meta_map_(),
    lob_aux_meta_allocator_()
{

}

ObCDCLobAuxMetaStorager::~ObCDCLobAuxMetaStorager()
{
  destroy();
}

int ObCDCLobAuxMetaStorager::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCLobAuxMetaStorager has been initialized", KR(ret));
  } else if (OB_FAIL(lob_aux_meta_map_.init("LobAuxMetaMap"))) {
    LOG_ERROR("lob_aux_meta_map_ init failed", KR(ret));
  } else if (OB_FAIL(lob_aux_meta_allocator_.init(LOB_AUX_META_ALLOCATOR_TOTAL_LIMIT,
          LOB_AUX_META_ALLOCATOR_HOLD_LIMIT,
          LOB_AUX_META_ALLOCATOR_PAGE_SIZE))) {
    LOG_ERROR("init lob_aux_meta_allocator_ fail", KR(ret));
  } else {
    lob_aux_meta_allocator_.set_label("LobAuxMetaData");

    is_inited_ = true;

    LOG_INFO("ObCDCLobAuxMetaStorager init succ");
  }

  return ret;
}

void ObCDCLobAuxMetaStorager::destroy()
{
  if (is_inited_) {
    lob_aux_meta_map_.destroy();
    lob_aux_meta_allocator_.destroy();
    is_inited_ = false;
  }
}

int ObCDCLobAuxMetaStorager::put(
    const LobAuxMetaKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxMetaStorager has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! key.is_valid()) || OB_ISNULL(key_type)
      || OB_ISNULL(lob_data) || OB_UNLIKELY(lob_data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(key), K(lob_data), K(lob_data_len));
  } else {
    void *alloc_lob_data = lob_aux_meta_allocator_.alloc(lob_data_len);

    if (OB_ISNULL(alloc_lob_data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    } else {
      memcpy(alloc_lob_data, lob_data, lob_data_len);
      LobAuxMetaValue lob_aux_meta_value(static_cast<char *>(alloc_lob_data), lob_data_len);

      if (OB_FAIL(lob_aux_meta_map_.insert(key, lob_aux_meta_value))) {
        LOG_ERROR("lob_aux_meta_map_ insert failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
      } else {
        LOG_DEBUG("lob_aux_meta_map_ insert succ", K(key), K(lob_aux_meta_value));
      }
    }
  }

  return ret;
}

int ObCDCLobAuxMetaStorager::get(
    const LobAuxMetaKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  lob_data = nullptr;
  lob_data_len = 0;
  LobAuxMetaValue lob_aux_meta_value;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxMetaStorager has not been initialized", KR(ret));
  } else if (OB_FAIL(lob_aux_meta_map_.get(key, lob_aux_meta_value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("lob_aux_meta_map_ get failed", KR(ret), K(key));
    } else if (REACH_TIME_INTERVAL(10 * _SEC_)) {
      LOG_WARN("lob_aux_meta_map_ get not exist, need retry", KR(ret), K(key));
    }
  } else {
    lob_data = lob_aux_meta_value.lob_data_;
    lob_data_len = lob_aux_meta_value.lob_data_len_;
  }

  return ret;
}

int ObCDCLobAuxMetaStorager::del(
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObCDCLobAuxMetaStorager del", K(lob_data_out_row_ctx_list));
  const uint64_t tenant_id = lob_data_out_row_ctx_list.get_tenant_id();
  const transaction::ObTransID &trans_id = lob_data_out_row_ctx_list.get_trans_id();
  const uint64_t aux_lob_meta_tid = lob_data_out_row_ctx_list.get_aux_lob_meta_table_id();
  ObLobDataGetCtxList &lob_data_get_ctx_list = lob_data_out_row_ctx_list.get_lob_data_get_ctx_list();
  ObLobDataGetCtx *cur_lob_data_get_ctx = lob_data_get_ctx_list.head_;

  while (OB_SUCC(ret) && ! stop_flag && cur_lob_data_get_ctx) {
    if (OB_FAIL(del_lob_col_value_(tenant_id, trans_id, aux_lob_meta_tid, *cur_lob_data_get_ctx, stop_flag))) {
      LOG_ERROR("del_lob_col_value_ failed", KR(ret), K(tenant_id), K(trans_id), K(aux_lob_meta_tid));
    } else {
      cur_lob_data_get_ctx = cur_lob_data_get_ctx->get_next();
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObCDCLobAuxMetaStorager::del_lob_col_value_(
    const uint64_t tenant_id,
    const transaction::ObTransID &trans_id,
    const uint64_t aux_lob_meta_tid,
    ObLobDataGetCtx &lob_data_get_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const ObLobDataOutRowCtx *lob_data_out_row_ctx = nullptr;

  if (OB_FAIL(lob_data_get_ctx.get_lob_out_row_ctx(lob_data_out_row_ctx))) {
    LOG_ERROR("lob_data_get_ctx get_lob_out_row_ctx failed", KR(ret), K(lob_data_get_ctx));
  } else if (OB_ISNULL(lob_data_out_row_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_data_out_row_ctx is nullptr", KR(ret), K(lob_data_get_ctx));
  } else {
    const uint64_t seq_no_start = lob_data_out_row_ctx->seq_no_st_;
    const uint32_t seq_no_cnt = lob_data_out_row_ctx->seq_no_cnt_;
    uint64_t seq_no = seq_no_start;
    const ObLobId &lob_id = lob_data_get_ctx.get_new_lob_data()->id_;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < seq_no_cnt; ++idx, ++seq_no) {
      LobAuxMetaKey lob_aux_meta_key(tenant_id, trans_id, aux_lob_meta_tid, lob_id, seq_no);
      LobAuxMetaValue lob_aux_meta_value;

      if (OB_FAIL(lob_aux_meta_map_.erase(lob_aux_meta_key, lob_aux_meta_value))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("lob_aux_meta_map_ erase failed", KR(ret), K(lob_aux_meta_value));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        char *lob_data_ptr = lob_aux_meta_value.lob_data_;

        if (OB_ISNULL(lob_data_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("lob_data_ptr is nullptr", KR(ret), K(lob_data_get_ctx));
        } else {
          lob_aux_meta_allocator_.free(lob_data_ptr);
        }
      }
    } // for
  }

  return ret;
}

int ObCDCLobAuxMetaStorager::del(
    const uint64_t tenant_id,
    const transaction::ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  struct LobAuxMetaDataPurger lob_aux_meata_purger(*this, tenant_id, trans_id);

  if (OB_FAIL(lob_aux_meta_map_.remove_if(lob_aux_meata_purger))) {
    LOG_ERROR("lob_aux_meta_map_ remove_if failed", KR(ret), K(tenant_id), K(trans_id));
  } else {
    if (lob_aux_meata_purger.purge_count_ > 0) {
      LOG_INFO("ObCDCLobAuxMetaStorager del", K(tenant_id), K(trans_id), "purge_count",
          lob_aux_meata_purger.purge_count_);
    }
  }

  return ret;
}

bool ObCDCLobAuxMetaStorager::LobAuxMetaDataPurger::operator()(
    const LobAuxMetaKey &lob_aux_meta_key,
    LobAuxMetaValue &lob_aux_meta_value)
{
  int ret = OB_SUCCESS;
  const bool need_purge = (tenant_id_ == lob_aux_meta_key.tenant_id_)
    && (trans_id_ == lob_aux_meta_key.trans_id_);

  if (need_purge) {
    char *lob_data_ptr = lob_aux_meta_value.lob_data_;

    if (OB_ISNULL(lob_data_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob_data_ptr is nullptr", KR(ret), K(lob_aux_meta_key), K(lob_aux_meta_value));
    } else {
      host_.lob_aux_meta_allocator_.free(lob_data_ptr);
      ++purge_count_;
    }
  }

  return need_purge;
}

bool ObCDCLobAuxMetaStorager::LobAuxMetaDataPrinter::operator()(
    const LobAuxMetaKey &lob_aux_meta_key,
    LobAuxMetaValue &lob_aux_meta_value)
{
  LOG_INFO("LobAuxMetaKey for_each", K(lob_aux_meta_key));
  return true;
}

} // namespace libobcdc
} // namespace oceanbase
