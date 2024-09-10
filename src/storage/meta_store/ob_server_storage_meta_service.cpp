/**
 * Copyright (c) 2023 OceanBase
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

#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/ob_file_system_router.h"
namespace oceanbase
{
namespace storage
{

ObServerStorageMetaService &ObServerStorageMetaService::get_instance()
{
  static ObServerStorageMetaService instance_;
  return instance_;
}
ObServerStorageMetaService::ObServerStorageMetaService()
  : is_inited_(false),
    is_started_(false),
    is_shared_storage_(false),
    persister_(),
    replayer_(),
    slogger_mgr_(),
    server_slogger_(nullptr),
    ckpt_slog_handler_() {}

int ObServerStorageMetaService::init(const bool is_shared_storage)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else if (!is_shared_storage) {
    if (OB_FAIL(slogger_mgr_.init(
        OB_FILE_SYSTEM_ROUTER.get_slog_dir(),
	OB_FILE_SYSTEM_ROUTER.get_sstable_dir(),
	ObLogConstants::MAX_LOG_FILE_SIZE,
        OB_FILE_SYSTEM_ROUTER.get_slog_file_spec()))) {
      LOG_WARN("fail to init slogger manager", K(ret));
    } else if (OB_FAIL(slogger_mgr_.get_server_slogger(server_slogger_))) {
      LOG_WARN("fail to get server slogger", K(ret));
    } else if (OB_FAIL(ckpt_slog_handler_.init(server_slogger_))) {
      LOG_WARN("fail to init server checkpoint slog hander", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(persister_.init(is_shared_storage, server_slogger_))) {
    LOG_WARN("fail to init persister", K(ret));
  } else if (OB_FAIL(replayer_.init(is_shared_storage, persister_, ckpt_slog_handler_))) {
    LOG_WARN("fail to init replayer", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    is_inited_ = true;
  }
  return ret;
}

int ObServerStorageMetaService::start()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_ && OB_FAIL(slogger_mgr_.start())) {
    LOG_WARN("fail to start slogger mgr", K(ret));
  } else if (OB_FAIL(replayer_.start_replay()))  {
    LOG_WARN("fail to start replayer", K(ret));
  } else {
    ATOMIC_STORE(&is_started_, true);
  }
  const int64_t cost_time_us = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("finish start server storage meta service", K(ret), K(cost_time_us));
  return ret;
}

void ObServerStorageMetaService::stop()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_mgr_.stop();
      ckpt_slog_handler_.stop();
    }
  }
}
void ObServerStorageMetaService::wait()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_mgr_.wait();
      ckpt_slog_handler_.wait();
    }
  }
}
void ObServerStorageMetaService::destroy()
{
  slogger_mgr_.destroy();
  server_slogger_ = nullptr;
  ckpt_slog_handler_.destroy();
  persister_.destroy();
  replayer_.destroy();
  is_inited_ = false;
}

int ObServerStorageMetaService::get_meta_block_list(
    ObIArray<blocksstable::MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(ckpt_slog_handler_.get_meta_block_list(meta_block_list))) {
      LOG_WARN("fail to get meta block list", K(ret));
    }
  }
  return ret;
}

int ObServerStorageMetaService::get_reserved_size(int64_t &reserved_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(slogger_mgr_.get_reserved_size(reserved_size))) {
      LOG_WARN("fail to get reserved size", K(ret));
    }
  } else {
    reserved_size = 0;
  }
  return ret;
}

int ObServerStorageMetaService::get_server_slogger(ObStorageLogger *&slogger) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    slogger = server_slogger_;
  } else {
    slogger = nullptr;
  }
  return ret;
}

int ObServerStorageMetaService::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    // do nothing
  } else if (OB_FAIL(ckpt_slog_handler_.write_checkpoint(is_force))) {
    LOG_WARN("fail to write checkpoint", K(ret));
  }
  return ret;
}

int ObServerStorageMetaService::ObTenantItemIterator::init()
{
  OB_STORAGE_OBJECT_MGR.get_server_super_block_by_copy(server_super_block_);
  is_inited_ = true;
  return OB_SUCCESS;
}
int ObServerStorageMetaService::ObTenantItemIterator::get_next_tenant_item(
      storage::ObTenantItem &item)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (idx_ == server_super_block_.body_.tenant_cnt_) {
    ret = OB_ITER_END;
  } else {
    item = server_super_block_.body_.tenant_item_arr_[idx_++];
  }
  return ret;
}
int ObServerStorageMetaService::get_tenant_items_by_status(
    const storage::ObTenantCreateStatus status,
    ObIArray<storage::ObTenantItem> &tenant_items/*OUT*/)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (storage::ObTenantCreateStatus::MAX == status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    HEAP_VAR(ObTenantItemIterator, tenant_item_iter) {
      tenant_item_iter.init();
      tenant_items.reuse();
      ObTenantItem tenant_item;
      while (OB_SUCC(tenant_item_iter.get_next_tenant_item(tenant_item))) {
        if (tenant_item.status_ == status &&
            OB_FAIL(tenant_items.push_back(tenant_item))) {
          LOG_WARN("failed to push back tenant_item", K(ret), K(tenant_item), K(tenant_items));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tenant items by status", K(ret), K(tenant_item), K(tenant_items));
      }
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
