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
#include "ob_log_store_service.h"                   // IObStoreService

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
int LobAuxMetaKey::compare(const LobAuxMetaKey &other) const
{
  int cmp_ret = 0;

  if (commit_version_ > other.commit_version_) {
    cmp_ret = 1;
  } else if (commit_version_ < other.commit_version_) {
    cmp_ret = -1;
  } else if (tenant_id_ > other.tenant_id_) {
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

int LobAuxMetaKey::get_key(std::string &key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob store key is not valid", KR(ret), KPC(this));
  } else {
    key.append(std::to_string(commit_version_));
    // to isolate tenant resources, each tenant data store in separate column famliy
    // so no need to add tenant_id_ in key
    // key.append(std::to_string(tenant_id_));
    key.append("_");
    key.append(std::to_string(trans_id_.get_id()));
    key.append("_");
    key.append(std::to_string(aux_lob_meta_tid_));
    key.append("_");
    key.append(std::to_string(lob_id_.tablet_id_));
    key.append("_");
    key.append(std::to_string(lob_id_.lob_id_));
    key.append("_");
    key.append(std::to_string(seq_no_.cast_to_int()));
  }
  return ret;
}

void ObCDCLobAuxDataCleanTask::reset()
{
  is_task_push_ = false;
  commit_version_ = OB_INVALID_VERSION;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

ObCDCLobAuxMetaStorager::ObCDCLobAuxMetaStorager() :
    is_inited_(false),
    lob_aux_meta_map_(),
    lob_aux_meta_allocator_(),
    store_service_(nullptr),
    memory_limit_(0),
    enable_memory_(true)
{

}

ObCDCLobAuxMetaStorager::~ObCDCLobAuxMetaStorager()
{
  destroy();
}

int ObCDCLobAuxMetaStorager::init(IObStoreService *store_service)
{
  int ret = OB_SUCCESS;
  enable_memory_ = TCONF.enable_lob_data_storage_memory;
  memory_limit_ = TCONF.lob_data_storage_memory_limit.get();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCLobAuxMetaStorager has been initialized", KR(ret));
  } else if (enable_memory_) {
    if (memory_limit_ <= 0) {
      LOG_ERROR("invalid arguments", KR(ret), K(memory_limit_));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(lob_aux_meta_map_.init("LobAuxMetaMap"))) {
      LOG_ERROR("lob_aux_meta_map_ init failed", KR(ret));
    } else if (OB_FAIL(lob_aux_meta_allocator_.init(memory_limit_,
            LOB_AUX_META_ALLOCATOR_HOLD_LIMIT,
            LOB_AUX_META_ALLOCATOR_PAGE_SIZE))) {
      LOG_ERROR("init lob_aux_meta_allocator_ fail", KR(ret));
    } else {
      lob_aux_meta_allocator_.set_label("LobAuxMetaData");
    }
  }
  if (OB_SUCC(ret)) {
    store_service_ = store_service;
    is_inited_ = true;

    LOG_INFO("ObCDCLobAuxMetaStorager init succ", K(enable_memory_), K(memory_limit_));
  }

  return ret;
}

void ObCDCLobAuxMetaStorager::destroy()
{
  if (is_inited_) {
    if (enable_memory_) {
      lob_aux_meta_map_.destroy();
      lob_aux_meta_allocator_.destroy();
      memory_limit_ = 0;
      enable_memory_ = false;
    }
    store_service_ = nullptr;
    is_inited_ = false;
  }
}

bool ObCDCLobAuxMetaStorager::is_need_to_disk_(int64_t alloc_size)
{
  return lob_aux_meta_allocator_.allocated() + alloc_size > memory_limit_;
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
  } else if (! enable_memory_ || is_need_to_disk_(lob_data_len)) {
    if (OB_FAIL(disk_put_(key, key_type, lob_data, lob_data_len))) {
      LOG_ERROR("disk_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    }
  } else if (OB_FAIL(memory_put_(key, key_type, lob_data, lob_data_len))) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret) {
      LOG_ERROR("memory_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    } else if (OB_FAIL(disk_put_(key, key_type, lob_data, lob_data_len))) {
      LOG_ERROR("disk_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    }
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::memory_put_(
    const LobAuxMetaKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;
  void *alloc_lob_data = lob_aux_meta_allocator_.alloc(lob_data_len);
  if (OB_ISNULL(alloc_lob_data)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
  } else {
    memcpy(alloc_lob_data, lob_data, lob_data_len);
    LobAuxMetaValue value(static_cast<char *>(alloc_lob_data), lob_data_len);
    if (OB_FAIL(lob_aux_meta_map_.insert(key, value))) {
      LOG_ERROR("lob_aux_meta_map_ insert failed", KR(ret), KCSTRING(key_type), K(key), K(lob_data), K(lob_data_len));
    } else {
      LOG_DEBUG("lob_aux_meta_map_ insert succ", KCSTRING(key_type), K(key), K(value));
    }
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::disk_put_(
    const LobAuxMetaKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), KCSTRING(key_type), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), KCSTRING(key_type), K(key));
  } else if (OB_FAIL(store_service_->put(cf_handle, key_str, ObSlice(lob_data, lob_data_len)))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service_ put fail", KR(ret), KCSTRING(key_type), K(key), KCSTRING(key_str.c_str()), K(lob_data_len));
    }
  } else {
    LOG_DEBUG("store_service_ insert succ", KCSTRING(key_type), K(key), KCSTRING(key_str.c_str()), K(lob_data_len));
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::get(
    ObIAllocator &allocator,
    const LobAuxMetaKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxMetaStorager has not been initialized", KR(ret));
  } else if (! enable_memory_) {
    if (OB_FAIL(disk_get_(allocator, key, lob_data, lob_data_len))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("disk_get_ failed", KR(ret), K(key));
      }
    }
  } else if (OB_FAIL(memory_get_(key, lob_data, lob_data_len))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("memory_get_ failed", KR(ret), K(key));
    } else if (OB_FAIL(disk_get_(allocator, key, lob_data, lob_data_len))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("disk_get_ failed", KR(ret), K(key));
      }
    }
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::memory_get_(
    const LobAuxMetaKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  lob_data = nullptr;
  lob_data_len = 0;
  LobAuxMetaValue value;
  if (OB_FAIL(lob_aux_meta_map_.get(key, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("lob_aux_meta_map_ get failed", KR(ret), K(key));
    } else if (REACH_TIME_INTERVAL(10 * _SEC_)) {
      LOG_WARN("lob_aux_meta_map_ get not exist, need retry", KR(ret), K(key));
    }
  } else {
    lob_data = value.lob_data_;
    lob_data_len = value.lob_data_len_;
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::disk_get_(
    ObIAllocator &allocator,
    const LobAuxMetaKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  std::string value;
  char *lob_data_ptr = nullptr;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->get(cf_handle, key_str, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get failed", KR(ret), K(key));
    } else if (REACH_TIME_INTERVAL(10 * _SEC_)) {
      LOG_WARN("get not exist, need retry", KR(ret), K(key));
    }
  } else if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("data is emptry", KR(ret), K(key));
  } else if (OB_ISNULL(lob_data_ptr = static_cast<char*>(allocator.alloc(value.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc fail", KR(ret), "size", value.length(), K(key));
  } else {
    MEMCPY(lob_data_ptr, value.c_str(), value.length());
    lob_data = lob_data_ptr;
    lob_data_len = value.length();
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::del(
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObCDCLobAuxMetaStorager del", K(lob_data_out_row_ctx_list));
  const PartTransTask &part_trans_task = lob_data_out_row_ctx_list.get_dml_stmt_task()->get_host();
  const int64_t commit_version = part_trans_task.get_trans_commit_version();
  const uint64_t tenant_id = lob_data_out_row_ctx_list.get_tenant_id();
  const transaction::ObTransID &trans_id = lob_data_out_row_ctx_list.get_trans_id();
  const uint64_t aux_lob_meta_tid = lob_data_out_row_ctx_list.get_aux_lob_meta_table_id();
  ObLobDataGetCtxList &lob_data_get_ctx_list = lob_data_out_row_ctx_list.get_lob_data_get_ctx_list();
  ObLobDataGetCtx *cur_lob_data_get_ctx = lob_data_get_ctx_list.head_;

  while (OB_SUCC(ret) && ! stop_flag && cur_lob_data_get_ctx) {
    if (OB_FAIL(del_lob_col_value_(commit_version, tenant_id, trans_id, aux_lob_meta_tid, *cur_lob_data_get_ctx, stop_flag))) {
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
    const int64_t commit_version,
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
    auto seq_no = transaction::ObTxSEQ::cast_from_int(seq_no_start);
    const ObLobId &lob_id = lob_data_get_ctx.get_new_lob_data()->id_;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < seq_no_cnt; ++idx, ++seq_no) {
      LobAuxMetaKey key(commit_version, tenant_id, trans_id, aux_lob_meta_tid, lob_id, seq_no);
      if (OB_FAIL(del(key))) {
        LOG_ERROR("del fail", KR(ret), K(key));
      }
    } // for
  }

  return ret;
}

int ObCDCLobAuxMetaStorager::del(const LobAuxMetaKey &key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxMetaStorager has not been initialized", KR(ret));
  } else if (! enable_memory_) {
    if (OB_FAIL(disk_del_(key))) {
      LOG_ERROR("disk_del_ fail", KR(ret), K(key));
    }
  } else if (OB_FAIL(memory_del_(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("memory_get_ fail", KR(ret), K(key));
    } else if (OB_FAIL(disk_del_(key))) {
      LOG_ERROR("disk_del_ fail", KR(ret), K(key));
    }
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::memory_del_(const LobAuxMetaKey &key)
{
  int ret = OB_SUCCESS;
  LobAuxMetaValue value;
  if (OB_FAIL(lob_aux_meta_map_.erase(key, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("lob_aux_meta_map_ erase failed", KR(ret), K(key));
    }
  } else {
    char *lob_data_ptr = value.lob_data_;
    if (OB_ISNULL(lob_data_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob_data_ptr is nullptr", KR(ret), K(key));
    } else {
      lob_aux_meta_allocator_.free(lob_data_ptr);
    }
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::disk_del_(const LobAuxMetaKey &key)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->del(cf_handle, key_str))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("del failed", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    LOG_DEBUG("store_service_ del succ", K(key), KCSTRING(key_str.c_str()));
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::clean_unused_data(ObCDCLobAuxDataCleanTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_clean_unused_data_(task))) {
    LOG_WARN("do_clean_unused_data_ fail", KR(ret));
  } else {
    ATOMIC_STORE(&task->is_task_push_, false);
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::do_clean_unused_data_(ObCDCLobAuxDataCleanTask *task)
{
  int ret = OB_SUCCESS;
  void *cf_handle = nullptr;
  if (enable_memory_ && OB_FAIL(memory_del_(task->tenant_id_, task->commit_version_))) {
    LOG_ERROR("memory_del_ failed", KR(ret), KPC(task));
  } else if (OB_FAIL(get_cf_handle(task->tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), KPC(task));
  } else if (OB_FAIL(disk_del_(cf_handle, task->commit_version_))) {
    LOG_ERROR("disk_del_ failed", KR(ret), KPC(task));
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::memory_del_(const uint64_t tenant_id, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  LobAuxMetaDataPurger lob_aux_meata_purger(*this, tenant_id, commit_version);
  if (OB_FAIL(lob_aux_meta_map_.remove_if(lob_aux_meata_purger))) {
    LOG_ERROR("lob_aux_meta_map_ remove_if failed", KR(ret), K(tenant_id), K(commit_version));
  } else {
    LOG_INFO("ObCDCLobAuxMetaStorager del", K(tenant_id), K(commit_version), "purge_count",
        lob_aux_meata_purger.purge_count_, "map_count", lob_aux_meta_map_.count(),
        "memory_used", lob_aux_meta_allocator_.allocated());
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::disk_del_(void* cf_family, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  std::string begin_key;
  std::string end_key;
  // some trans may have same commit_version,
  // to avoid accidentally deleting uncommitted transactions,
  // we set end_key to "commit_version - 1".
  end_key.append(std::to_string(commit_version - 1));
  end_key.append("_}");
  if (OB_FAIL(store_service_->del_range(cf_family, begin_key, end_key))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service_ del fail", KR(ret), "begin_key", begin_key.c_str(),
          "end_key", end_key.c_str(), K(commit_version));
    }
  } else {
    LOG_INFO("store_service_ del_range succ", K(commit_version), KCSTRING(end_key.c_str()));
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::get_cf_handle(const uint64_t tenant_id, void *&cf_handle)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = nullptr;
  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    cf_handle = tenant->get_lob_storage_cf_handle();
  }
  return ret;
}

int ObCDCLobAuxMetaStorager::get_clean_task(const uint64_t tenant_id, ObCDCLobAuxDataCleanTask *&task)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = nullptr;
  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    task = &tenant->get_lob_storage_clean_task();
  }
  return ret;
}

bool ObCDCLobAuxMetaStorager::LobAuxMetaDataPurger::operator()(
    const LobAuxMetaKey &lob_aux_meta_key,
    LobAuxMetaValue &lob_aux_meta_value)
{
  int ret = OB_SUCCESS;
  const bool need_purge = (tenant_id_ == lob_aux_meta_key.tenant_id_)
    && (commit_version_ > lob_aux_meta_key.commit_version_);

  if (need_purge) {
    char *lob_data_ptr = lob_aux_meta_value.lob_data_;

    if (OB_ISNULL(lob_data_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob_data_ptr is nullptr", KR(ret), K(lob_aux_meta_key), K(lob_aux_meta_value));
    } else {
      host_.lob_aux_meta_allocator_.free(lob_data_ptr);
      ++purge_count_;
      LOG_DEBUG("purge succ", K(lob_aux_meta_key), K(purge_count_));
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
