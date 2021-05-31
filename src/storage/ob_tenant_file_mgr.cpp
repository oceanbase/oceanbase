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

#include "ob_tenant_file_mgr.h"
#include "storage/ob_server_log.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_file_system_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFileMgr::ObTenantFileMgr()
    : tenant_file_map_(), allocator_(nullptr), tenant_id_(OB_INVALID_ID), is_inited_(false)
{}

ObTenantFileMgr::~ObTenantFileMgr()
{
  destroy();
}

int ObTenantFileMgr::init(const uint64_t tenant_id, common::ObConcurrentFIFOAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_file_map_.create(
                 TENANT_MAX_FILE_CNT, ObModIds::OB_TENANT_FILE_MGR, ObModIds::OB_TENANT_FILE_MGR))) {
    LOG_WARN("fail to create tenant map", K(ret));
  } else {
    allocator_ = &allocator;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantFileMgr::alloc_file(const bool is_sys_table, ObStorageFileHandle& file_handle, const bool write_slog)
{
  int ret = OB_SUCCESS;
  file_handle.reset();
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else {
    ObTenantFileValue* value = nullptr;
    if (OB_FAIL(choose_tenant_file(is_sys_table, true /*need create new file*/, tenant_file_map_, value))) {
      LOG_WARN("fail to choose tenant file", K(ret));
    }

    if (OB_SUCC(ret) && nullptr == value) {
      const int64_t curr_file_cnt = tenant_file_map_.size();
      const int64_t create_file_cnt = 1L;
      if (OB_FAIL(create_new_tenant_file(write_slog, is_sys_table, create_file_cnt))) {
        LOG_WARN("fail to create new files", K(ret));
      } else if (OB_FAIL(choose_tenant_file(
                     is_sys_table, false /*do not need create new file*/, tenant_file_map_, value))) {
        LOG_WARN("fail to choose tenant file", K(ret));
      } else if (nullptr == value) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, can not get file after create file",
            K(ret),
            K(is_sys_table),
            K(curr_file_cnt),
            K(create_file_cnt));
      }
    }

    if (OB_SUCC(ret)) {
      file_handle.set_storage_file_with_ref(value->storage_file_);
    }
  }
  return ret;
}

int ObTenantFileMgr::open_file(const ObTenantFileKey& tenant_file_key, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(tenant_file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get from tenant file map", K(ret));
    }
  } else {
    file_handle.set_storage_file_with_ref(value->storage_file_);
  }
  return ret;
}

int ObTenantFileMgr::alloc_exist_file(const ObTenantFileInfo& file_info, const bool write_slog, const bool open_file,
    const bool is_owner, const bool from_svr_ckpt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else {
    // try to open file
    const int open_flag = ObFileSystemUtil::WRITE_FLAGS;
    const int64_t tenant_id = file_info.tenant_key_.tenant_id_;
    const int64_t file_id = file_info.tenant_key_.file_id_;
    ObStorageFile* tmp_file = nullptr;
    void* buf = nullptr;
    ObTenantFileValue* value = nullptr;
    if (open_file &&
        OB_FAIL(ObFileSystemUtil::open_tenant_file(GCTX.self_addr_, tenant_id, file_id, open_flag, tmp_file))) {
      LOG_WARN("fail to open tenant file", K(ret));
    } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObTenantFileValue)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for ObTenantFileValue", K(ret));
    } else {
      if (write_slog) {
        if (OB_FAIL(write_update_slog(
                file_info.tenant_key_, false /*in slog trans*/, file_info.tenant_file_super_block_))) {
          LOG_WARN("fail to write update slog", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const ObTenantFileKey tenant_file_key = file_info.tenant_key_;
        value = new (buf) ObTenantFileValue();
        value->storage_file_.file_ = tmp_file;
        value->is_owner_ = is_owner;
        value->from_svr_ckpt_ |= from_svr_ckpt;  // if already load from svr ckpt, DO NOT overwrite
        value->meta_block_handle_.set_storage_file(value->storage_file_.file_);
        ObStorageFileHandle file_handle;
        if (OB_FAIL(const_cast<ObTenantFileInfo&>(file_info).deep_copy(value->file_info_))) {
          LOG_WARN("fail to deep copy file info", K(ret));
        } else if (OB_FAIL(tenant_file_map_.set_refactored(tenant_file_key, value))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_ENTRY_EXIST;
          }
          LOG_WARN("fail to set to tenant file map", K(ret), K(tenant_file_key));
          if (write_slog) {  // used in fast recovery
            if (OB_ENTRY_EXIST != ret) {
              ob_abort();
            }
          }
        } else {
          LOG_WARN("update new file value", K(from_svr_ckpt), K(*value));
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (nullptr != tmp_file) {
        ObFileSystemUtil::close_file(tmp_file);
        tmp_file = nullptr;
      }
      if (nullptr != value) {
        value->storage_file_.file_ = nullptr;
        value->~ObTenantFileValue();
        allocator_->free(value);
        value = nullptr;
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::choose_tenant_file(
    const bool is_sys_table, const bool need_create_file, TENANT_FILE_MAP& tenant_file_map, ObTenantFileValue*& value)
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (!is_sys_table && tenant_file_map.size() < TENANT_MIN_FILE_CNT && need_create_file) {
    // alloc one more file
  } else {
    for (TENANT_FILE_MAP::const_iterator iter = tenant_file_map.begin(); OB_SUCC(ret) && iter != tenant_file_map.end();
         ++iter) {
      ObTenantFileValue* tmp_value = iter->second;
      if (tmp_value->file_info_.tenant_file_super_block_.is_sys_table_file_ != is_sys_table) {
        // do nothing
      } else if ((tmp_value->file_info_.get_pg_cnt() < ObTenantFileValue::MAX_REF_CNT_PER_FILE || is_sys_table) &&
                 TENANT_FILE_NORMAL == tmp_value->file_info_.tenant_file_super_block_.status_ &&
                 tmp_value->storage_file_.file_->get_mark_and_sweep_status()) {
        if (nullptr == value) {
          value = iter->second;
        } else {
          value = value->storage_file_.get_ref() > tmp_value->storage_file_.get_ref() ? tmp_value : value;
        }
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::generate_unique_file_id(int64_t& file_id)
{
  int ret = OB_SUCCESS;
  file_id = ObTimeUtility::current_time();
  return ret;
}

int ObTenantFileMgr::create_new_tenant_file(const bool write_slog, const bool create_sys_table, const int64_t file_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(file_cnt <= 0 || (create_sys_table && file_cnt > 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_cnt), K(create_sys_table));
  } else if (write_slog && OB_FAIL(SLOGGER.begin(OB_LOG_UPDATE_TENANT_FILE_INFO))) {
    LOG_WARN("fail to begin trans", K(ret));
  } else {
    ObArray<ObTenantFileValue*> new_file_values;
    bool need_free_memory = true;
    if (OB_FAIL(new_file_values.reserve(file_cnt))) {
      LOG_WARN("fail to reserve new files array", K(ret));
    }

    // create file and write slog
    for (int64_t i = 0; OB_SUCC(ret) && i < file_cnt; ++i) {
      void* buf = nullptr;
      ObTenantFileValue* value = nullptr;
      int64_t file_id = -1;
      if (OB_FAIL(generate_unique_file_id(file_id))) {
        LOG_WARN("fail to generate unique file id", K(file_id));
      } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObTenantFileValue)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for tenant file value", K(ret));
      } else {
        ObTenantFileKey tenant_key;
        tenant_key.tenant_id_ = tenant_id_;
        tenant_key.file_id_ = file_id;
        value = new (buf) ObTenantFileValue();
        value->file_info_.tenant_key_.tenant_id_ = tenant_id_;
        value->file_info_.tenant_key_.file_id_ = tenant_key.file_id_;
        value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_INIT;
        value->file_info_.tenant_file_super_block_.is_sys_table_file_ = create_sys_table;
        if (write_slog && OB_FAIL(write_update_slog(
                              tenant_key, true /*in slog trans*/, value->file_info_.tenant_file_super_block_))) {
          LOG_WARN("fail to write update slog", K(ret));
        } else if (OB_FAIL(new_file_values.push_back(value))) {
          LOG_WARN("fail to push back file value", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != value) {
          value->~ObTenantFileValue();
          allocator_->free(value);
          value = nullptr;
        }
      }
    }

    if (write_slog) {
      if (OB_SUCC(ret)) {
        int64_t lsn = 0;
        if (OB_FAIL(SLOGGER.commit(lsn))) {
          LOG_WARN("fail to commit slog", K(ret), K(lsn));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          LOG_WARN("fail to abort slog", K(ret));
        }
      }
    }

    // set in memory
    for (int64_t i = 0; OB_SUCC(ret) && i < new_file_values.count(); ++i) {
      ObTenantFileValue* value = new_file_values.at(i);
      value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_INIT;
      const ObTenantFileKey& key = value->file_info_.tenant_key_;
      if (OB_FAIL(tenant_file_map_.set_refactored(key, value))) {
        LOG_WARN("fail to set refactored", K(ret), K(key));
        ob_abort();
      } else {
        LOG_INFO("add into tenant file map", K(key), K(*value));
      }
    }

    need_free_memory = false;

    const int open_flag = ObFileSystemUtil::CREATE_FLAGS;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_file_values.count(); ++i) {
      const int64_t file_id = new_file_values.at(i)->file_info_.tenant_key_.file_id_;
      ObStorageFile* tmp_file = nullptr;
      ObStorageFile* tmp_info_file = nullptr;
      ObTenantFileValue* value = new_file_values.at(i);
      if (OB_FAIL(ObFileSystemUtil::open_tenant_info_file(
              GCTX.self_addr_, tenant_id_, file_id, open_flag, tmp_info_file))) {
        LOG_WARN("fail to open tenant data info file", K(ret));
      } else {
        value->info_file_ = tmp_info_file;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(
                     ObFileSystemUtil::open_tenant_file(GCTX.self_addr_, tenant_id_, file_id, open_flag, tmp_file))) {
        LOG_WARN("fail to open file", K(ret), K(tenant_id_), K(file_id));
      } else {
        value->storage_file_.ref_cnt_ = 0;
        value->storage_file_.file_ = tmp_file;
        value->meta_block_handle_.set_storage_file(value->storage_file_.file_);
        value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_NORMAL;
        tmp_file->enable_mark_and_sweep();
      }
    }

    if (OB_FAIL(ret) && need_free_memory) {
      for (int64_t i = 0; i < new_file_values.count(); ++i) {
        ObTenantFileValue* value = new_file_values.at(i);
        value->~ObTenantFileValue();
        allocator_->free(value);
        new_file_values.at(i) = nullptr;
      }
      new_file_values.reset();
    }
  }
  return ret;
}

int ObTenantFileMgr::replay_alloc_file(const ObTenantFileInfo& file_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(alloc_exist_file(file_info,
                 false /*do not write slog*/,
                 false /*do not open file*/,
                 true /*is owner*/,
                 true /*from_svr_ckpt*/))) {
    LOG_WARN("fail to alloc exist file", K(ret));
  } else {
    LOG_INFO("replay alloc file", K(file_info));
  }
  return ret;
}

int ObTenantFileMgr::replay_open_files(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    const int open_flag = ObFileSystemUtil::WRITE_FLAGS;
    for (TENANT_FILE_MAP::iterator iter = tenant_file_map_.begin(); OB_SUCC(ret) && iter != tenant_file_map_.end();
         ++iter) {
      ObTenantFileKey& key = iter->first;
      ObTenantFileValue* value = iter->second;
      ObStorageFile* tmp_file = value->storage_file_.file_;
      ObStorageFile* tmp_info_file = nullptr;
      ObTenantFileInfo file_info;
      if (nullptr == tmp_file) {
        const bool ignore_file_not_exist = TENANT_FILE_NORMAL != value->file_info_.tenant_file_super_block_.status_;
        bool has_all_file = true;
        if (OB_FAIL(ObFileSystemUtil::open_tenant_info_file(
                server, key.tenant_id_, key.file_id_, open_flag, tmp_info_file))) {
          if (ignore_file_not_exist && OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
            ret = OB_SUCCESS;
            has_all_file = false;
            LOG_INFO("file not exist", K(key));
          } else {
            LOG_WARN("fail to open tenant info file", K(ret));
          }
        } else {
          value->info_file_ = tmp_info_file;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObFileSystemUtil::open_tenant_file(
                       GCTX.self_addr_, key.tenant_id_, key.file_id_, open_flag, tmp_file))) {
          if (ignore_file_not_exist && OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
            ret = OB_SUCCESS;
            has_all_file = false;
            LOG_INFO("file not exist", K(ret), K(key));
          } else {
            LOG_WARN("fail to open tenant file", K(ret), K(key), K(file_info));
          }
        } else {
          value->storage_file_.file_ = tmp_file;
          value->meta_block_handle_.set_storage_file(value->storage_file_.file_);
        }

        if (OB_SUCC(ret) && has_all_file && value->file_info_.is_init_status()) {
          value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_NORMAL;
        }
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::update_tenant_file_meta_info(
    const ObTenantFileCheckpointEntry& file_meta, const bool write_checkpoint_slog)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  ObTenantFileValue* value = nullptr;
  const ObTenantFileKey& file_key = file_meta.tenant_file_key_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (write_checkpoint_slog && OB_FAIL(write_update_file_info_slog(file_key, file_meta.super_block_))) {
    LOG_WARN("fail to write slog", K(ret));
  } else {
    if (OB_FAIL(update_tenant_file_super_block_in_map(file_key, file_meta.super_block_))) {
      LOG_WARN("fail to update tenant file super block in map", K(ret));
    } else if (OB_FAIL(
                   update_tenant_file_meta_blocks_impl(file_key, file_meta.meta_block_handle_.get_meta_block_list()))) {
      LOG_WARN("fail to update tenant file meta blocks", K(ret));
    } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
      LOG_WARN("fail to get from tenant file map", K(ret));
    }
    if (OB_FAIL(ret) && write_checkpoint_slog) {
      ob_abort();
    }
  }
  return ret;
}

int ObTenantFileMgr::update_tenant_file_meta_blocks_impl(
    const ObTenantFileKey& file_key, const ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
    LOG_WARN("fail to get from tenant file map", K(ret));
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, tenant file value must not be null", K(ret), K(file_key));
  } else if (OB_FAIL(value->meta_block_handle_.add_macro_blocks(meta_block_list, true /*need switch*/))) {
    LOG_WARN("fail to add macro blocks", K(ret));
  } else {
    FLOG_INFO("update tenant file meta blocks", K(file_key), K(meta_block_list));
  }
  return ret;
}

int ObTenantFileMgr::update_tenant_file_meta_blocks(
    const ObTenantFileKey& file_key, const ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  TCWLockGuard guard(lock_);
  return update_tenant_file_meta_blocks_impl(file_key, meta_block_list);
}

int ObTenantFileMgr::update_tenant_file_super_block_in_map(
    const ObTenantFileKey& key, const ObTenantFileSuperBlock& tenant_file_super_block)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(key, value))) {
    LOG_WARN("fail to get from tenant file map", K(ret));
  } else {
    // file status can only be modified internal
    ObTenantFileStatus org_file_status = value->file_info_.tenant_file_super_block_.status_;
    value->file_info_.tenant_file_super_block_ = tenant_file_super_block;
    value->file_info_.tenant_file_super_block_.status_ = org_file_status;
  }
  return ret;
}

int ObTenantFileMgr::update_file_status(const ObTenantFileKey& file_key, ObTenantFileStatus status)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || status <= TENANT_FILE_INVALID || status >= TENANT_FILE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_key), K(status));
  } else {
    TCWLockGuard guard(lock_);
    if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
      LOG_WARN("fail to get from tenant file map", K(ret));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file value is null", K(ret), KP(value));
    } else {
      value->file_info_.tenant_file_super_block_.status_ = status;
    }
  }
  return ret;
}

int ObTenantFileMgr::write_update_file_info_slog(
    const ObTenantFileKey& file_key, const ObTenantFileSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_UPDATE_TENANT_FILE_INFO))) {
    LOG_WARN("fail to begin update tenant file info trans", K(ret));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
    LOG_WARN("fail to get from tenant file map", K(ret), K(file_key));
  } else {
    const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_FILE, REDO_LOG_UPDATE_TENANT_FILE_INFO);
    const int64_t pg_subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_WRITE_FILE_CHECKPOINT);
    const ObStorageLogAttribute attr(file_key.tenant_id_, file_key.file_id_);
    ObUpdateTenantFileInfoLogEntry log_entry;
    if (OB_FAIL(value->file_info_.deep_copy(log_entry.file_info_))) {
      LOG_WARN("fail to deep copy file info", K(ret));
    } else if (FALSE_IT(log_entry.file_info_.tenant_file_super_block_ = super_block)) {
    } else if (OB_FAIL(SLOGGER.write_log(subcmd, attr, log_entry))) {
      LOG_WARN("fail to write slog", K(ret), K(file_key));
    } else if (OB_FAIL(SLOGGER.write_log(pg_subcmd, attr, log_entry))) {
      LOG_WARN("fail to write slog", K(ret), K(file_key));
    } else {
      int64_t lsn = 0;
      if (OB_FAIL(SLOGGER.commit(lsn))) {
        LOG_WARN("fail to commit slog", K(ret), K(lsn));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          LOG_WARN("fail to abort slog", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::write_update_slog(
    const ObTenantFileKey& file_key, const bool in_slog_trans, const ObTenantFileSuperBlock& tenant_file_super_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (!in_slog_trans && OB_FAIL(SLOGGER.begin(OB_LOG_UPDATE_TENANT_FILE_INFO))) {
    LOG_WARN("fail to begin update tenant file info trans", K(ret));
  } else {
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_FILE, REDO_LOG_UPDATE_TENANT_FILE_SUPER_BLOCK);
    ObUpdateTenantFileSuperBlockLogEntry log_entry;
    log_entry.file_key_ = file_key;
    log_entry.super_block_ = tenant_file_super_block;
    if (OB_FAIL(SLOGGER.write_log(subcmd, ObStorageLogAttribute(file_key.tenant_id_, file_key.file_id_), log_entry))) {
      LOG_WARN("fail to write log entry", K(ret));
    } else {
      if (!in_slog_trans) {
        if (OB_SUCC(ret)) {
          int64_t lsn = 0;
          if (OB_FAIL(SLOGGER.commit(lsn))) {
            LOG_WARN("fail to commit slog", K(ret), K(lsn));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
            LOG_WARN("fail to abort slog", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::write_add_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid() || !file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key), K(file_key));
  } else {
    ObAddPGToTenantFileLogEntry log_entry(file_key, pg_key);
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_FILE, REDO_LOG_ADD_PG_TO_TENANT_FILE);
    if (OB_FAIL(SLOGGER.write_log(subcmd, ObStorageLogAttribute(file_key.tenant_id_, file_key.file_id_), log_entry))) {
      LOG_WARN("fail to write macro block meta slog", K(ret));
    }
  }
  return ret;
}

int ObTenantFileMgr::add_pg(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* file_value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, file_value))) {
    LOG_WARN("fail to get from tenant file map", K(ret), K(file_key));
  } else if (OB_FAIL(file_value->file_info_.exist_pg(pg_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(file_value->file_info_.add_pg(pg_key))) {
        LOG_WARN("fail to add pg to file", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::write_remove_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else {
    ObRemovePGFromTenantFileLogEntry log_entry(file_key, pg_key);
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_FILE, REDO_LOG_REMOVE_PG_FROM_TENANT_FILE);
    if (OB_FAIL(SLOGGER.write_log(subcmd, ObStorageLogAttribute(file_key.tenant_id_, file_key.file_id_), log_entry))) {
      LOG_WARN("fail to write macro block meta slog", K(ret));
    }
  }
  return ret;
}

int ObTenantFileMgr::remove_pg(const ObTenantFileKey& file_key, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* file_value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, file_value))) {
    LOG_WARN("fail to get from tenant file map", K(ret), K(file_key));
  } else if (OB_FAIL(file_value->file_info_.exist_pg(pg_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check exist pg", K(ret), K(pg_key));
    }
  } else if (OB_FAIL(file_value->file_info_.remove_pg(pg_key))) {
    LOG_WARN("fail to remove pg from file", K(ret), K(pg_key));
  } else {
    if (file_value->file_info_.is_empty_file()) {
      file_value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_DELETING;
      LOG_INFO("mark file deleting", K(file_key));
    }
  }
  return ret;
}

int ObTenantFileMgr::is_from_svr_ckpt(const ObTenantFileKey& file_key, bool& from_svr_skpt) const
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* file_value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, file_value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get from tenant file map", K(ret), K(file_key));
    }
  } else {
    from_svr_skpt = file_value->from_svr_ckpt_;
  }
  return ret;
}

int ObTenantFileMgr::write_remove_slog(const ObTenantFileKey& file_key, const bool delete_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_UPDATE_TENANT_FILE_INFO))) {
    LOG_WARN("fail to begin trans", K(ret));
  } else {
    ObRemoveTenantFileSuperBlockLogEntry log_entry(file_key, delete_file);
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_FILE, REDO_LOG_REMOVE_TENANT_FILE_SUPER_BLOCK);
    if (OB_FAIL(SLOGGER.write_log(subcmd, ObStorageLogAttribute(file_key.tenant_id_, file_key.file_id_), log_entry))) {
      LOG_WARN("fail to write macro block meta slog", K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t lsn = 0;
      if (OB_FAIL(SLOGGER.commit(lsn))) {
        LOG_WARN("fail to commit slog", K(ret), K(lsn));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        LOG_WARN("fail to abort slog", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::get_tenant_file(const ObTenantFileKey& file_key, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get from tenant file map", K(ret));
    }
  } else {
    file_handle.set_storage_file_with_ref(value->storage_file_);
  }
  return ret;
}

int ObTenantFileMgr::get_all_tenant_file(ObStorageFilesHandle& files_handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else {
    for (TENANT_FILE_MAP::const_iterator iter = tenant_file_map_.begin();
         OB_SUCC(ret) && iter != tenant_file_map_.end();
         ++iter) {
      ObTenantFileValue* value = iter->second;
      const bool ignore_file_not_exist = TENANT_FILE_NORMAL != value->file_info_.tenant_file_super_block_.status_;
      if (ignore_file_not_exist) {
        // skip inexistent file.
        continue;
      } else if (OB_FAIL(files_handle.add_storage_file(value->storage_file_))) {
        LOG_WARN("fail to add storage file", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::get_tenant_file_info(const ObTenantFileKey& tenant_file_key, ObTenantFileInfo& tenant_file_info)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  ObTenantFileValue* value = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(tenant_file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get from tenant file map", K(ret));
  } else if (OB_FAIL(value->file_info_.deep_copy(tenant_file_info))) {
    LOG_WARN("fail to deep copy tenant file info", K(ret));
  }
  return ret;
}

int ObTenantFileMgr::get_all_tenant_file_infos(ObIAllocator& allocator, ObIArray<ObTenantFileInfo*>& tenant_file_infos)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else {
    for (TENANT_FILE_MAP::const_iterator tenant_file_iter = tenant_file_map_.begin();
         OB_SUCC(ret) && tenant_file_iter != tenant_file_map_.end();
         ++tenant_file_iter) {
      ObTenantFileValue* value = tenant_file_iter->second;
      ObTenantFileInfo* copy_file_info = nullptr;
      void* buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTenantFileInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for tenant file info", K(ret));
      } else if (FALSE_IT(copy_file_info = new (buf) ObTenantFileInfo())) {
      } else if (OB_FAIL(value->file_info_.deep_copy(*copy_file_info))) {
        LOG_WARN("fail to deep copy tenant file info", K(ret));
      } else if (OB_FAIL(tenant_file_infos.push_back(copy_file_info))) {
        LOG_WARN("fail to push back tenant file infos", K(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != copy_file_info) {
          copy_file_info->~ObTenantFileInfo();
          copy_file_info = nullptr;
        }
        if (nullptr != buf) {
          allocator.free(buf);
          buf = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::clear_replay_map()
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else {
    for (TENANT_FILE_MAP::iterator tenant_file_iter = tenant_file_map_.begin();
         OB_SUCC(ret) && tenant_file_iter != tenant_file_map_.end();
         ++tenant_file_iter) {
      ObTenantFileValue* value = tenant_file_iter->second;
      if (TENANT_FILE_NORMAL != value->file_info_.tenant_file_super_block_.status_) {
        // do nothing
      } else if (OB_ISNULL(value->storage_file_.file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file is null", K(ret), K(*value));
      } else {
        value->storage_file_.file_->get_replay_map().destroy();
      }
    }
  }
  return ret;
}

int ObTenantFileMgr::replay_update_tenant_file_super_block(
    const ObTenantFileKey& file_key, const ObTenantFileSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get from map", K(ret), K(file_key));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr != value) {
      FLOG_INFO("file is load from checkpoint, skip create file super block slog.", K(super_block));
    } else {
      ObTenantFileInfo file_info;
      file_info.tenant_key_ = file_key;
      file_info.tenant_file_super_block_ = super_block;
      if (OB_FAIL(alloc_exist_file(file_info,
              false /*write slog*/,
              false /*do not open file*/,
              true /*is owner*/,
              false /*from_svr_ckpt*/))) {
        LOG_WARN("fail to replay add file", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to replay update tenant file supe block", K(super_block));
    }
  }
  return ret;
}

int ObTenantFileMgr::replay_update_tenant_file_info(const ObTenantFileInfo& file_info)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_info.tenant_key_, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get from hashmap", K(ret), K(file_info.tenant_key_));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr != value) {
      if (value->from_svr_ckpt_) {
        FLOG_INFO("skip update file info due to already load from checkpoint", K(value));
      } else if (OB_FAIL(const_cast<ObTenantFileInfo&>(file_info).deep_copy(value->file_info_))) {
        LOG_WARN("fail to deep copy file info", K(ret), K(file_info));
      }
    } else {
      if (OB_FAIL(alloc_exist_file(file_info,
              false /*write slog*/,
              false /*do not open file*/,
              true /*is owner*/,
              false /*from_svr_ckpt*/))) {
        LOG_WARN("fail to replay add file", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to replay update tenant file info", K(file_info));
  }
  return ret;
}

int ObTenantFileMgr::replay_remove_tenant_file_super_block(const ObTenantFileKey& file_key, const bool delete_file)
{
  int ret = OB_SUCCESS;
  UNUSED(delete_file);
  ObTenantFileValue* value = nullptr;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_file_map_.erase_refactored(file_key, &value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from map", K(ret), K(file_key));
    }
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get nullptr file info", K(ret), K(file_key), KP(value));
  } else {
    LOG_INFO("replay remove tenant file", K(*value));
    if (OB_NOT_NULL(value->storage_file_.file_)) {
      value->meta_block_handle_.reset();
      ObFileSystemUtil::close_file(value->storage_file_.file_);
      value->storage_file_.file_ = nullptr;
    }
    value->~ObTenantFileValue();
    allocator_->free(value);
    value = nullptr;
  }
  return ret;
}

int ObTenantFileMgr::get_tenant_file_meta_blocks(
    const ObTenantFileKey& file_key, common::ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get from tenant file mgr", K(ret));
  } else if (OB_FAIL(meta_block_list.assign(value->meta_block_handle_.get_meta_block_list()))) {
    LOG_WARN("fail to assign meta block list", K(ret));
  }
  return ret;
}

int ObTenantFileMgr::get_macro_meta_replay_map(
    const ObTenantFileKey& tenant_file_key, ObMacroMetaReplayMap*& replay_map)
{
  int ret = OB_SUCCESS;
  ObTenantFileValue* value = nullptr;
  replay_map = nullptr;
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(tenant_file_map_.get_refactored(tenant_file_key, value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get from tenant file mgr", K(ret));
  } else {
    replay_map = &(value->storage_file_.file_->get_replay_map());
  }
  return ret;
}

void ObTenantFileMgr::destroy()
{
  TCWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (is_inited_) {
    for (TENANT_FILE_MAP::const_iterator tenant_file_iter = tenant_file_map_.begin();
         OB_SUCC(ret) && tenant_file_iter != tenant_file_map_.end();
         ++tenant_file_iter) {
      ObTenantFileValue* value = tenant_file_iter->second;
      if (OB_FAIL(tenant_file_map_.erase_refactored(tenant_file_iter->first))) {
        LOG_WARN("fail to erase from tenant file map", K(ret));
      }
      value->~ObTenantFileValue();
      allocator_->free(value);
    }
    tenant_file_map_.destroy();
    allocator_ = nullptr;
    is_inited_ = false;
  }
}

int ObBaseFileMgr::alloc_file(
    const uint64_t tenant_id, const bool is_sys_table, ObStorageFileHandle& handle, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(tenant_id, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->alloc_file(is_sys_table, handle, write_slog))) {
    LOG_WARN("fail to alloc file", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::replay_alloc_file(const ObTenantFileInfo& file_info)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_info));
  } else if (OB_FAIL(get_tenant_mgr(file_info.tenant_key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->replay_alloc_file(file_info))) {
    LOG_WARN("fail to replay add file", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::get_tenant_mgr(const uint64_t tenant_id, ObTenantFileMgr*& tenant_mgr)
{
  int ret = OB_SUCCESS;
  tenant_mgr = nullptr;
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_mgr_map_.get_refactored(tenant_id, tenant_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get from hashmap", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret) && nullptr == tenant_mgr) {
    void* buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTenantFileMgr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tenant file mgr", K(ret));
    } else {
      tenant_mgr = new (buf) ObTenantFileMgr();
      if (OB_FAIL(tenant_mgr->init(tenant_id, allocator_))) {
        LOG_WARN("fail to init tenant mgr", K(ret));
      } else if (OB_FAIL(tenant_mgr_map_.set_refactored(tenant_id, tenant_mgr))) {
        LOG_WARN("fail to get from tenant mgr map", K(ret), K(tenant_id));
      }
    }
    if (OB_FAIL(ret) && nullptr != tenant_mgr) {
      tenant_mgr->~ObTenantFileMgr();
      allocator_.free(tenant_mgr);
      tenant_mgr = nullptr;
    }
  }
  return ret;
}

int ObBaseFileMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerFileMgr has already been inited", K(ret));
  } else if (OB_FAIL(
                 tenant_mgr_map_.create(MAX_TENANT_CNT, ObModIds::OB_TENANT_FILE_MGR, ObModIds::OB_TENANT_FILE_MGR))) {
    LOG_WARN("fail to create tenant mgr map", K(ret));
  } else if (OB_FAIL(allocator_.init(MEM_LIMIT, MEM_LIMIT, OB_MALLOC_BIG_BLOCK_SIZE))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObBaseFileMgr::destroy()
{
  if (is_inited_) {
    int tmp_ret = OB_SUCCESS;
    lib::ObMutexGuard guard(lock_);
    for (TENANT_MGR_MAP::const_iterator iter = tenant_mgr_map_.begin(); iter != tenant_mgr_map_.end(); ++iter) {
      ObTenantFileMgr* tenant_mgr = iter->second;
      if (OB_SUCCESS != (tmp_ret = tenant_mgr_map_.erase_refactored(iter->first))) {
        LOG_WARN("fail to erase from tenant mgr map", K(tmp_ret));
      }
      tenant_mgr->~ObTenantFileMgr();
      allocator_.free(tenant_mgr);
    }
    tenant_mgr_map_.destroy();
    allocator_.destroy();
    is_inited_ = false;
  }
}

int ObBaseFileMgr::get_all_tenant_file_infos(
    common::ObIAllocator& allocator, ObIArray<ObTenantFileInfo*>& tenant_file_infos)
{
  int ret = OB_SUCCESS;
  tenant_file_infos.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else {
    lib::ObMutexGuard guard(lock_);
    for (TENANT_MGR_MAP::const_iterator iter = tenant_mgr_map_.begin(); OB_SUCC(ret) && iter != tenant_mgr_map_.end();
         ++iter) {
      ObTenantFileMgr* tenant_mgr = iter->second;
      if (OB_FAIL(tenant_mgr->get_all_tenant_file_infos(allocator, tenant_file_infos))) {
        LOG_WARN("fail to get all tenant file infos", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseFileMgr::update_tenant_file_meta_info(
    const ObTenantFileCheckpointEntry& file_entry, const bool skip_write_file_slog)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  const ObTenantFileKey& file_key = file_entry.tenant_file_key_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->update_tenant_file_meta_info(file_entry, skip_write_file_slog))) {
    LOG_WARN("fail to update tenant file meta info", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::update_tenant_file_meta_blocks(
    const ObTenantFileKey& file_key, const ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->update_tenant_file_meta_blocks(file_key, meta_block_list))) {
    LOG_WARN("fail to update tenant file meta blocks", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::update_tenant_file_status(const ObTenantFileKey& file_key, ObTenantFileStatus status)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->update_file_status(file_key, status))) {
    LOG_WARN("fail to update file status", K(ret), K(file_key), K(status));
  }
  return ret;
}

int ObBaseFileMgr::get_tenant_file(const ObTenantFileKey& file_key, blocksstable::ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  file_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->get_tenant_file(file_key, file_handle))) {
    LOG_WARN("fail to get tenant file", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::get_all_tenant_file(ObStorageFilesHandle& files_handle)
{
  int ret = OB_SUCCESS;
  files_handle.reset();
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else {
    for (TENANT_MGR_MAP::const_iterator iter = tenant_mgr_map_.begin(); OB_SUCC(ret) && iter != tenant_mgr_map_.end();
         ++iter) {
      ObTenantFileMgr* tenant_mgr = iter->second;
      if (OB_FAIL(tenant_mgr->get_all_tenant_file(files_handle))) {
        LOG_WARN("fail to get all tenant file", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseFileMgr::replay(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const char* buf = param.buf_;
  const int64_t len = param.buf_len_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
    int32_t sub_type = 0;
    ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
    if (OB_REDO_LOG_TENANT_FILE != main_type) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, redo log main type is not expected", K(ret), K(main_type), K(param));
    } else {
      switch (sub_type) {
        case REDO_LOG_UPDATE_TENANT_FILE_SUPER_BLOCK:
          if (OB_FAIL(replay_update_tenant_file_super_block(buf, len))) {
            LOG_WARN("fail to replay update pg super block", K(ret), K(param));
          }
          break;
        case REDO_LOG_REMOVE_TENANT_FILE_SUPER_BLOCK:
          if (OB_FAIL(replay_remove_tenant_file_super_block(buf, len))) {
            LOG_WARN("fail to replay remove tenant file info", K(ret), K(param));
          }
          break;
        case REDO_LOG_ADD_PG_TO_TENANT_FILE:
          if (OB_FAIL(replay_add_pg_to_file(buf, len))) {
            LOG_WARN("fail to replay add pg to file", K(ret), K(param));
          }
          break;
        case REDO_LOG_REMOVE_PG_FROM_TENANT_FILE:
          if (OB_FAIL(replay_remove_pg_from_file(buf, len))) {
            LOG_WARN("fail to remove pg from file", K(ret), K(param));
          }
          break;
        case REDO_LOG_UPDATE_TENANT_FILE_INFO:
          if (OB_FAIL(replay_update_tenant_file_info(buf, len))) {
            LOG_WARN("fail to replay update tenant file info", K(ret));
          }
          break;
        default:
          ret = OB_ERR_SYS;
          LOG_WARN("error sys, sub_type is not as expected", K(ret), K(sub_type), K(param));
      }
    }
  }
  return ret;
}

int ObBaseFileMgr::replay_update_tenant_file_info(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObUpdateTenantFileInfoLogEntry log_entry;
  int64_t pos = 0;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(log_entry.file_info_.tenant_key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->replay_update_tenant_file_info(log_entry.file_info_))) {
    LOG_WARN("fail to replay update tenant file info", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::replay_update_tenant_file_super_block(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObUpdateTenantFileSuperBlockLogEntry log_entry;
  int64_t pos = 0;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(log_entry.file_key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->replay_update_tenant_file_super_block(log_entry.file_key_, log_entry.super_block_))) {
    LOG_WARN("fail to replay update tenant file info", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::replay_add_pg_to_file(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObAddPGToTenantFileLogEntry log_entry;
  int64_t pos = 0;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(log_entry.file_key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->add_pg(log_entry.file_key_, log_entry.pg_key_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      FLOG_INFO("tenant file not exist when replay add pg to file", K(log_entry));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to replay add pg to file", K(ret), K(log_entry));
    }
  }
  return ret;
}

int ObBaseFileMgr::replay_remove_pg_from_file(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObRemovePGFromTenantFileLogEntry log_entry;
  int64_t pos = 0;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(log_entry.file_key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->remove_pg(log_entry.file_key_, log_entry.pg_key_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      FLOG_INFO("tenant file not exist when replay remove pg to file", K(log_entry));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to replay remove pg from file", K(ret), K(log_entry));
    }
  } else {
    FLOG_INFO("replay remove pg from file success", K(log_entry));
  }
  return ret;
}

int ObBaseFileMgr::replay_remove_tenant_file_super_block(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObRemoveTenantFileSuperBlockLogEntry log_entry;
  int64_t pos = 0;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_tenant_mgr(log_entry.key_.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->replay_remove_tenant_file_super_block(log_entry.key_, log_entry.delete_file_))) {
    LOG_WARN("fail to replay remove tenant file info", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = OB_REDO_LOG_TENANT_FILE;
  int32_t sub_type = 0;

  ObIRedoModule::parse_subcmd(subcmd, main_type, sub_type);
  if (nullptr == buf || len <= 0 || nullptr == stream) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len), KP(stream));
  } else if (OB_REDO_LOG_TENANT_FILE != main_type) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, invalid main type", K(ret), K(main_type));
  } else if (fprintf(stream, "tenant file mgr slog") < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to print stream", K(ret));
  } else {
    switch (sub_type) {
      case REDO_LOG_REMOVE_TENANT_FILE_SUPER_BLOCK: {
        PrintSLogEntry(ObRemoveTenantFileSuperBlockLogEntry);
        break;
      }
      case REDO_LOG_UPDATE_TENANT_FILE_SUPER_BLOCK: {
        PrintSLogEntry(ObUpdateTenantFileSuperBlockLogEntry);
        break;
      }
      case REDO_LOG_ADD_PG_TO_TENANT_FILE: {
        PrintSLogEntry(ObAddPGToTenantFileLogEntry);
        break;
      }
      case REDO_LOG_REMOVE_PG_FROM_TENANT_FILE: {
        PrintSLogEntry(ObRemovePGFromTenantFileLogEntry);
        break;
      }
      case REDO_LOG_UPDATE_TENANT_FILE_INFO: {
        PrintSLogEntry(ObUpdateTenantFileInfoLogEntry);
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, invalid sub log type", K(ret), K(sub_type));
    }
  }
  return ret;
}

int ObBaseFileMgr::replay_over()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else {
    for (TENANT_MGR_MAP::iterator iter = tenant_mgr_map_.begin(); OB_SUCC(ret) && iter != tenant_mgr_map_.end();
         ++iter) {
      ObTenantFileMgr* tenant_mgr = iter->second;
      if (OB_FAIL(tenant_mgr->clear_replay_map())) {
        LOG_WARN("fail to clear replay map", K(ret), K(tenant_mgr->get_tenant_id()));
      }
    }
  }
  return ret;
}

int ObBaseFileMgr::open_file(const ObTenantFileKey& tenant_file_key, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(get_tenant_mgr(tenant_file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->open_file(tenant_file_key, file_handle))) {
    LOG_WARN("fail to open file", K(ret), K(tenant_file_key));
  }
  return ret;
}

int ObBaseFileMgr::get_tenant_file_info(const ObTenantFileKey& tenant_file_key, ObTenantFileInfo& tenant_file_info)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(get_tenant_mgr(tenant_file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->get_tenant_file_info(tenant_file_key, tenant_file_info))) {
    LOG_WARN("fail to get tenant file info", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::replay_open_files(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    for (TENANT_MGR_MAP::iterator iter = tenant_mgr_map_.begin(); OB_SUCC(ret) && iter != tenant_mgr_map_.end();
         ++iter) {
      ObTenantFileMgr* tenant_mgr = iter->second;
      if (OB_FAIL(tenant_mgr->replay_open_files(server))) {
        LOG_WARN("fail to replay open files", K(ret), K(server));
      }
    }
  }
  return ret;
}

int ObBaseFileMgr::get_tenant_file_meta_blocks(const ObTenantFileKey& file_key, ObIArray<MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->get_tenant_file_meta_blocks(file_key, meta_block_list))) {
    LOG_WARN("fail to get tenant file meta blocks", K(ret));
  }
  return ret;
}

int ObBaseFileMgr::write_add_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->write_add_pg_slog(file_key, pg_key))) {
    LOG_WARN("fail to write add pg slog", K(ret), K(file_key), K(pg_key));
  }
  return ret;
}

int ObBaseFileMgr::add_pg(const ObTenantFileKey& file_key, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->add_pg(file_key, pg_key))) {
    LOG_WARN("fail to add pg", K(ret), K(file_key), K(pg_key));
  }
  return ret;
}

int ObBaseFileMgr::write_remove_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->write_remove_pg_slog(file_key, pg_key))) {
    LOG_WARN("fail to write remove pg slog", K(ret), K(file_key), K(pg_key));
  }
  return ret;
}

int ObBaseFileMgr::remove_pg(const ObTenantFileKey& file_key, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(pg_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret));
  } else if (OB_FAIL(tenant_mgr->remove_pg(file_key, pg_key))) {
    LOG_WARN("fail to add pg", K(ret), K(file_key));
  }
  return ret;
}

int ObBaseFileMgr::is_from_svr_ckpt(const ObTenantFileKey& file_key, bool& from_svr_skpt)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else if (OB_FAIL(get_tenant_mgr(file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(file_key));
  } else if (OB_FAIL(tenant_mgr->is_from_svr_ckpt(file_key, from_svr_skpt))) {
    LOG_WARN("fail to add pg", K(ret), K(file_key));
  }
  return ret;
}

int ObBaseFileMgr::get_macro_meta_replay_map(const ObTenantFileKey& tenant_file_key, ObMacroMetaReplayMap*& replay_map)
{
  int ret = OB_SUCCESS;
  ObTenantFileMgr* tenant_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBaseFileMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(get_tenant_mgr(tenant_file_key.tenant_id_, tenant_mgr))) {
    LOG_WARN("fail to get tenant mgr", K(ret), K(tenant_file_key));
  } else if (OB_FAIL(tenant_mgr->get_macro_meta_replay_map(tenant_file_key, replay_map))) {
    LOG_WARN("fail to get tenant file macro meta replay map", K(ret));
  }
  return ret;
}

ObBaseFileMgr::ObBaseFileMgr() : lock_(), tenant_mgr_map_(), allocator_(), is_inited_(false)
{}

ObBaseFileMgr::~ObBaseFileMgr()
{
  destroy();
}

int ObBaseFileMgr::recycle_file()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("base file mgr not init", K(ret));
  } else {
    lib::ObMutexGuard base_mgr_guard(lock_);
    for (TENANT_MGR_MAP::const_iterator tenant_iter = tenant_mgr_map_.begin();
         OB_SUCC(ret) && tenant_iter != tenant_mgr_map_.end();
         ++tenant_iter) {
      ObTenantFileMgr* tenant_mgr = tenant_iter->second;
      if (OB_ISNULL(tenant_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_mgr_is null", K(ret), KP(tenant_mgr));
      } else if (OB_FAIL(tenant_mgr->recycle_file())) {
        LOG_WARN("fail to mark recycle file", K(ret), K(tenant_mgr->get_tenant_id()));
      }
    }
  }
  return ret;
}

void ObFileRecycleTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_mgr_.recycle_file())) {
    LOG_WARN("fail to recycle file", K(ret));
  }
}

int ObTenantFileMgr::recycle_file()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool tenant_has_been_dropped = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("fail to check formal schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id_, tenant_has_been_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tenant_id_));
  } else {
    TCWLockGuard tenant_mgr_guard(lock_);
    // ignore ret
    for (TENANT_FILE_MAP::const_iterator file_value_iter = tenant_file_map_.begin();
         file_value_iter != tenant_file_map_.end();
         ++file_value_iter) {
      const ObTenantFileKey& file_key = file_value_iter->first;
      ObTenantFileValue* file_value = file_value_iter->second;
      LOG_INFO("iterate tenant file", K(*file_value));
      if (OB_ISNULL(file_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant file value is null", K(ret), K(file_key), KP(file_value));
      } else if (tenant_has_been_dropped && file_value->file_info_.is_normal_status() &&
                 file_value->file_info_.is_empty_file()) {
        file_value->file_info_.tenant_file_super_block_.status_ = TENANT_FILE_DELETING;
      } else if (TENANT_FILE_DELETING == file_value->file_info_.tenant_file_super_block_.status_ &&
                 file_value->storage_file_.ref_cnt_ <= 0) {
        const bool need_remove_file = file_value->is_owner_;
        if (nullptr != file_value->storage_file_.file_) {
          if (need_remove_file) {
            if (OB_FAIL(file_value->storage_file_.file_->unlink())) {
              if (OB_FILE_NOT_EXIST != ret) {
                LOG_WARN("fail to delete storage file", K(ret));
              } else {
                LOG_INFO("tenant data file not exist", K(file_key));
                ret = OB_SUCCESS;
              }
            } else {
              LOG_INFO("succ to delete tenant data file", K(file_key));
            }
          }

          if (OB_SUCC(ret)) {
            file_value->meta_block_handle_.reset();
            ObFileSystemUtil::close_file(file_value->storage_file_.file_);
            file_value->storage_file_.file_ = nullptr;
          }
        }

        if (OB_SUCC(ret) && nullptr != file_value->info_file_) {
          if (need_remove_file) {
            if (OB_FAIL(file_value->info_file_->unlink())) {
              if (OB_FILE_NOT_EXIST != ret) {
                LOG_WARN("fail to unlink ofs file", K(ret));
              } else {
                LOG_INFO("tenant data info file already deleted", K(file_key));
                ret = OB_SUCCESS;
              }
            } else {
              LOG_INFO("succeed to delete tenant data info file", K(file_key));
            }
          }

          if (OB_SUCC(ret)) {
            ObFileSystemUtil::close_file(file_value->info_file_);
            file_value->info_file_ = nullptr;
          }
        }

        if (OB_SUCC(ret)) {
          // remove in memory record at last, when previous steps failed, can retry
          ObTenantFileValue* tmp_file_value = nullptr;
          if (OB_FAIL(tenant_file_map_.erase_refactored(file_key, &tmp_file_value))) {
            LOG_WARN("fail to erase entry from tenan_file_mgr", K(ret), K(file_key));
            ret = OB_SUCCESS;  // ignore this error to continue delete file
          } else {
            tmp_file_value->~ObTenantFileValue();
            allocator_->free(tmp_file_value);
          }
        }
      }
    }
  }
  return ret;
}

ObServerFileMgr& ObServerFileMgr::get_instance()
{
  static ObServerFileMgr instance;
  return instance;
}

int ObServerFileMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(recycle_timer_.init("DATA_FILE_REYCYLE"))) {
    LOG_WARN("fail to init recycle timer", K(ret));
  } else if (OB_FAIL(recycle_timer_.schedule(recycle_task_, ObFileRecycleTask::RECYCLE_CYCLE_US, true /*repeate*/))) {
    LOG_WARN("fail to schedule recycle task", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObBaseFileMgr::init())) {
      LOG_WARN("fail to init base file mgr", K(ret));
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObServerFileMgr::destroy()
{
  recycle_timer_.destroy();
  ObBaseFileMgr::destroy();
}

int ObServerFileMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

void ObServerFileMgr::stop()
{
  recycle_timer_.stop();
}

void ObServerFileMgr::wait()
{
  recycle_timer_.wait();
}

int ObTenantFileSLogFilter::filter(const ObISLogFilter::Param& param, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  ObTenantFileInfo file_info;
  if (ObPartitionService::get_instance().is_replay_old()) {
    // do nothing
  } else if (OB_VIRTUAL_DATA_FILE_ID == param.attr_.data_file_id_) {
    is_filtered = false;
  } else {
    ObTenantFileKey file_key(param.attr_.tenant_id_, param.attr_.data_file_id_);
    ObTenantFileInfo file_info;
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_tenant_file_info(file_key, file_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        is_filtered = true;
        ret = OB_SUCCESS;
      }
    } else {
      is_filtered = !file_info.is_normal_status();
    }
  }
  return ret;
}
