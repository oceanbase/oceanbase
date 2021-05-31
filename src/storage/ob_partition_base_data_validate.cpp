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
#include "storage/ob_partition_base_data_validate.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "storage/ob_partition_migrator.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "lib/restore/ob_storage.h"
#include "archive/ob_archive_entry_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace storage {

ObBackupMetaIndexStore::ObBackupMetaIndexStore() : is_inited_(false)
{}

int ObBackupMetaIndexStore::init(const ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  ObBackupPath path;
  ObStorageUtil util(false);
  ObArray<ObString> index_file_names;
  if (OB_UNLIKELY(!path_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path info is invalid", K(ret));
  } else if (OB_FAIL(meta_index_map_.create(bucket_num, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to init meta index map", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, path))) {
    STORAGE_LOG(WARN, "failed to get tenant data inc backup set path", K(ret));
  } else if (OB_FAIL(
                 util.list_files(path.get_obstr(), path_info.dest_.get_storage_info(), allocator_, index_file_names))) {
    STORAGE_LOG(WARN, "failed to list file", K(ret));
  } else {
    ObBackupPath index_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      index_path.reset();
      ObString& file_name = index_file_names.at(i);
      if (!file_name.prefix_match(OB_STRING_BACKUP_META_INDEX)) {
        STORAGE_LOG(INFO, "skip none meta index file", K(ret));
      } else if (OB_FAIL(index_path.init(path.get_obstr()))) {
        STORAGE_LOG(WARN, "failed to init base index path", K(ret));
      } else if (OB_FAIL(index_path.join(file_name))) {
        STORAGE_LOG(WARN, "failed to init index path", K(ret));
      } else if (OB_FAIL(init_from_remote_file(index_path.get_obstr(), path_info.dest_.get_storage_info()))) {
        STORAGE_LOG(WARN, "failed to init index file", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    meta_index_map_.clear();
  }
  return ret;
}

void ObBackupMetaIndexStore::reset()
{
  meta_index_map_.clear();
  allocator_.clear();
  is_inited_ = false;
}

int ObBackupMetaIndexStore::get_meta_index(
    const common::ObPartitionKey& pkey, const share::ObBackupMetaType& type, share::ObBackupMetaIndex& meta_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta index store do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid partition key", K(ret), K(pkey));
  } else {
    ObMetaIndexKey key(pkey.get_table_id(), pkey.get_partition_id(), type);
    if (OB_FAIL(meta_index_map_.get_refactored(key, meta_index))) {
      STORAGE_LOG(WARN, "failed to get meta index", K(ret), K(key), K(meta_index_map_.size()));
    } else {
      STORAGE_LOG(INFO, "get meta index success", K(key), K(meta_index));
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::init_from_remote_file(const common::ObString& path, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("initing from remote file", K(path), K(storage_info));
  char* buf = NULL;
  int64_t data_len = 0;
  if (OB_FAIL(ObValidateBackupUtil::read_single_file(path, storage_info, allocator_, buf, data_len))) {
    STORAGE_LOG(WARN, "failed to read meta index file", K(ret));
  } else if (data_len <= 0) {
    STORAGE_LOG(INFO, "may be empty meta index file", K(ret));
  } else {
    ObBufferReader buffer_reader(buf, data_len, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupMetaIndex meta_index;
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "failed to read meta index common header", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "meta index common header is not valid", K(ret));
      } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
        STORAGE_LOG(INFO, "meta index file reach end mark", K(ret));
        break;
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_read not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        STORAGE_LOG(WARN, "common header checksum error", K(ret));
      } else {
        int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
        for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
          if (OB_FAIL(buffer_reader.read_serialize(meta_index))) {
            STORAGE_LOG(WARN, "failed to read meta index", K(ret));
          } else {
            const share::ObMetaIndexKey key(meta_index.table_id_, meta_index.partition_id_, meta_index.meta_type_);
            if (OB_FAIL(meta_index_map_.set_refactored(key, meta_index, 1))) {
              STORAGE_LOG(WARN, "failed to set index", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret) && common_header->align_length_ > 0) {
        if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
          STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
        }
      }
    }
  }
  return ret;
}

ObBackupMacroIndexStore::ObBackupMacroIndexStore() : is_inited_(false)
{}

int ObBackupMacroIndexStore::init(const share::ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = 1024;
  ObStorageUtil util(false);
  ObExternPGListMgr pg_list_mgr;
  ObArray<ObPGKey> pg_keys;
  ObArray<ObPGKey> normal_pg_keys;
  ObArray<ObPGKey> sys_pg_keys;
  ObFakeBackupLeaseService fake_lease_service;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "backup macro index store init twice", K(ret));
  } else if (OB_FAIL(macro_index_map_.create(bucket_num, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create macro index map", K(ret));
  } else if (OB_FAIL(pg_list_mgr.init(path_info.tenant_id_,
                 path_info.full_backup_set_id_,
                 path_info.inc_backup_set_id_,
                 path_info.dest_,
                 fake_lease_service))) {
    STORAGE_LOG(WARN, "failed to init extern pg list mgr", K(ret));
  } else if (OB_FAIL(pg_list_mgr.get_normal_pg_list(normal_pg_keys))) {
    STORAGE_LOG(WARN, "failed to get extern pg list", K(ret));
  } else if (OB_FAIL(pg_list_mgr.get_sys_pg_list(sys_pg_keys))) {
    STORAGE_LOG(WARN, "failed to get extern pg list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_pg_keys.count(); ++i) {
      const ObPartitionKey& pkey = normal_pg_keys.at(i);
      if (OB_FAIL(pg_keys.push_back(pkey))) {
        STORAGE_LOG(WARN, "failed to push back pkey", K(ret), K(pkey));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_pg_keys.count(); ++i) {
      const ObPartitionKey& pkey = sys_pg_keys.at(i);
      if (OB_FAIL(pg_keys.push_back(pkey))) {
        STORAGE_LOG(WARN, "failed to push back pkey", K(ret), K(pkey));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      bool is_exist = true;
      ObBackupPath path;
      const ObPGKey& pg_key = pg_keys.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && is_exist; ++j) {
        path.reset();
        if (OB_FAIL(ObBackupPathUtil::get_macro_block_index_path(
                path_info, pg_key.get_table_id(), pg_key.get_partition_id(), j, path))) {
          STORAGE_LOG(WARN, "failed to get macro block index path", K(ret));
        } else if (OB_FAIL(util.is_exist(path.get_obstr(), path_info.dest_.get_storage_info(), is_exist))) {
          STORAGE_LOG(WARN, "failed to check macro index file exist or not", K(ret));
        } else if (!is_exist) {
          break;
        } else if (OB_FAIL(init_from_remote_file(pg_key, path.get_obstr(), path_info.dest_.get_storage_info()))) {
          STORAGE_LOG(WARN, "failed to init from remote file", K(ret));
        }
      }
      LOG_INFO("init from pkey", K(ret), K(pg_key));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    macro_index_map_.clear();
  }
  return ret;
}

void ObBackupMacroIndexStore::reset()
{
  macro_index_map_.clear();
  allocator_.clear();
  is_inited_ = false;
}

int ObBackupMacroIndexStore::get_macro_block_index(
    const common::ObPGKey& pg_key, ObArray<ObBackupMacroIndex>*& index_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMacroIndex>* tmp_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (OB_FAIL(macro_index_map_.get_refactored(pg_key, tmp_list))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "the corresponding pg is empty, skip", K(ret), K(pg_key));
    } else {
      STORAGE_LOG(WARN, "failed to get macro block index", K(ret), K(pg_key));
    }
  } else {
    index_list = tmp_list;
    tmp_list = NULL;
  }
  return ret;
}

int ObBackupMacroIndexStore::init_from_remote_file(
    const common::ObPGKey& pg_key, const common::ObString& path, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  LOG_INFO("initing from remote file", K(path), K(storage_info));
  int64_t data_len = 0;
  if (OB_FAIL(ObValidateBackupUtil::read_single_file(path, storage_info, allocator_, buf, data_len))) {
    STORAGE_LOG(WARN, "failed to read macro index file", K(ret));
  } else if (data_len <= 0) {
    STORAGE_LOG(INFO, "may be empty macro index file", K(ret));
  } else {
    ObBufferReader buffer_reader(buf, data_len, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupMacroIndex macro_index;
    ObArray<ObBackupMacroIndex> macro_index_list;
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "failed to read macro index common header", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "macro index common header is invalid", K(ret));
      } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
        STORAGE_LOG(INFO, "macro index file reach end mark", K(ret));
        break;
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_read not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        // log error
      } else {
        int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
        for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
          if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
            STORAGE_LOG(WARN, "failed to read macro index", K(ret));
          } else {
            if (0 == macro_index_list.count()) {
              if (OB_FAIL(macro_index_list.push_back(macro_index))) {
                STORAGE_LOG(WARN, "failed to put macro index", K(ret), K(macro_index));
              }
            } else {
              if (OB_FAIL(add_sstable_index(pg_key, macro_index_list))) {
                STORAGE_LOG(WARN, "failed to put index array to map", K(ret));
              } else {
                macro_index_list.reuse();
                if (OB_FAIL(macro_index_list.push_back(macro_index))) {
                  STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && macro_index_list.count() > 0) {
          if (OB_FAIL(add_sstable_index(pg_key, macro_index_list))) {
            STORAGE_LOG(WARN, "failed to put index array to map", K(ret));
          } else {
            macro_index_list.reset();
          }
        }

        if (OB_SUCC(ret) && common_header->align_length_ > 0) {
          if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
            STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupMacroIndexStore::add_sstable_index(
    const common::ObPGKey& pg_key, const ObIArray<ObBackupMacroIndex>& index_list)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObArray<ObBackupMacroIndex>* new_block_list = NULL;

  int64_t block_count = index_list.count();
  if (0 == block_count) {
    STORAGE_LOG(INFO, "no block index need add", K(block_count), K(pg_key));
  } else if (OB_FAIL(macro_index_map_.get_refactored(pg_key, new_block_list))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get old block list fail", K(ret), K(pg_key));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<ObBackupMacroIndex>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
      } else if (OB_ISNULL(new_block_list = new (buf) ObArray<ObBackupMacroIndex>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to new block list", K(ret));
      } else if (OB_FAIL(macro_index_map_.set_refactored(pg_key, new_block_list, 1 /*rewrite*/))) {
        STORAGE_LOG(WARN, "failed to set macro index map", K(ret));
        new_block_list->~ObArray();
      }
    }
  } else if (OB_ISNULL(new_block_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exist block list should not be null here", K(ret), K(pg_key));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupMacroIndex& index = index_list.at(i);  // check
    if (index.sstable_macro_index_ < new_block_list->count()) {
      // skip
    } else if (OB_UNLIKELY(index.sstable_macro_index_ != new_block_list->count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "sstable macro index is not continued",
          K(ret),
          K(index),
          "new_block_list_count",
          new_block_list->count(),
          K(*new_block_list));
    } else if (OB_FAIL(new_block_list->push_back(index))) {
      STORAGE_LOG(WARN, "failed to push back block index", K(ret), K(i));
    }
  }
  return ret;
}

ObValidateBackupPGCtx::SubTask::SubTask()
{
  macro_block_count_ = 0;
  macro_block_infos_ = NULL;
}

ObValidateBackupPGCtx::SubTask::~SubTask()
{
  reset();
}

void ObValidateBackupPGCtx::SubTask::reset()
{
  macro_block_count_ = 0;
  macro_block_infos_ = NULL;
}

ObValidateBackupPGCtx::ObValidateBackupPGCtx()
    : is_inited_(false),
      is_dropped_tenant_(false),
      need_validate_clog_(false),
      only_in_clog_(false),
      job_id_(-1),
      result_(0),
      retry_count_(0),
      archive_round_(-1),
      backup_set_id_(-1),
      current_task_idx_(-1),
      min_clog_file_id_(-1),
      max_clog_file_id_(-1),
      last_replay_log_id_(-1),
      current_clog_file_id_(-1),
      start_log_id_(-1),
      end_log_id_(-1),
      total_log_size_(0),
      clog_end_timestamp_(-1),
      sub_task_cnt_(-1),
      total_partition_count_(0),
      finish_partition_count_(0),
      total_macro_block_count_(0),
      finish_macro_block_count_(0),
      macro_block_count_per_sub_task_(-1),
      sub_tasks_(NULL),
      sql_proxy_(NULL),
      rs_rpc_proxy_(NULL),
      migrate_ctx_(NULL)
{}

ObValidateBackupPGCtx::~ObValidateBackupPGCtx()
{
  reset();
}

int ObValidateBackupPGCtx::init(
    storage::ObMigrateCtx& migrate_ctx, common::ObInOutBandwidthThrottle& bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  const ObPhysicalValidateArg& validate_arg = migrate_ctx.replica_op_arg_.validate_arg_;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (OB_FAIL(validate_arg.get_backup_base_data_info(path_info_))) {
    STORAGE_LOG(WARN, "failed to get backup base data info", K(ret), K(validate_arg));
  } else {
    migrate_ctx_ = &migrate_ctx;
    bandwidth_throttle_ = &bandwidth_throttle;
    job_id_ = validate_arg.job_id_;
    archive_round_ = validate_arg.archive_round_;
    backup_set_id_ = validate_arg.backup_set_id_;
    pg_key_ = validate_arg.pg_key_;
    clog_end_timestamp_ = validate_arg.clog_end_timestamp_;
    is_dropped_tenant_ = validate_arg.is_dropped_tenant_;
    need_validate_clog_ = validate_arg.need_validate_clog_;
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

void ObValidateBackupPGCtx::reset()
{
  allocator_.clear();
  is_inited_ = false;
}

bool ObValidateBackupPGCtx::is_valid() const
{
  return pg_key_.is_valid();
}

ObValidatePrepareTask::ObValidatePrepareTask()
    : ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), migrate_ctx_(NULL), validate_pg_ctx_(NULL)
{}

ObValidatePrepareTask::~ObValidatePrepareTask()
{}

int ObValidatePrepareTask::init(ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx)
{
  int ret = OB_SUCCESS;
  const ObBackupBaseDataPathInfo& path_info = validate_pg_ctx.path_info_;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (!migrate_ctx.is_valid() || !validate_pg_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(validate_pg_ctx.archive_file_store_.init(path_info.dest_.dest_.root_path_,
                 path_info.dest_.dest_.storage_info_,
                 path_info.dest_.cluster_name_,
                 path_info.dest_.cluster_id_,
                 path_info.tenant_id_,
                 path_info.dest_.incarnation_,
                 validate_pg_ctx.archive_round_))) {
    STORAGE_LOG(WARN, "failed to init archive log file store", K(ret));
  } else {
    migrate_ctx_ = &migrate_ctx;
    validate_pg_ctx_ = &validate_pg_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObValidatePrepareTask::process()
{
  int ret = OB_SUCCESS;
  int64_t start_offset = 0;
  uint64_t min_file_id = 1;
  uint64_t max_file_id = 1;
  uint64_t last_replay_log_id = 0;
  const int64_t TIMEOUT = 60 * 1000 * 1000L;
  bool need_limit_bandwidth = true;
  LOG_INFO("start to process validate prepare task",
      K(min_file_id),
      K(max_file_id),
      "pg_key",
      validate_pg_ctx_->pg_key_,
      "backup_set_id",
      validate_pg_ctx_->backup_set_id_);

  bool first_log_entry = true;
  clog::ObLogEntry log_entry;
  archive::ObArchiveEntryIterator iter;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "validate prepare task do not init", K(ret));
  } else if (validate_pg_ctx_->is_pg_key_only_in_clog()) {
    STORAGE_LOG(INFO, "no need to check pg key is continue if only in clog", K(ret));
  } else if (OB_FAIL(validate_pg_ctx_->archive_file_store_.get_data_file_id_range(
                 validate_pg_ctx_->pg_key_, min_file_id, max_file_id))) {
    STORAGE_LOG(WARN, "failed to get min and max file id", K(ret));
  } else if (OB_FAIL(iter.init(&(validate_pg_ctx_->archive_file_store_),
                 validate_pg_ctx_->pg_key_,
                 max_file_id,
                 start_offset,
                 TIMEOUT,
                 need_limit_bandwidth))) {
    STORAGE_LOG(WARN, "failed to init archive entry iterator", K(ret), K(validate_pg_ctx_->pg_key_));
  } else {
    validate_pg_ctx_->min_clog_file_id_ = min_file_id;
    validate_pg_ctx_->max_clog_file_id_ = max_file_id;
    last_replay_log_id = migrate_ctx_->pg_meta_.storage_info_.get_data_info().get_last_replay_log_id();
    validate_pg_ctx_->last_replay_log_id_ = last_replay_log_id;
    while (OB_SUCC(ret) && first_log_entry) {
      if (OB_FAIL(iter.next_entry(log_entry))) {
        break;
      } else if (first_log_entry) {
        uint64_t log_id = log_entry.get_header().get_log_id();
        first_log_entry = false;
        if (log_id > last_replay_log_id) {
          // TODO ret = OB_INVALID_DATA;
          break;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    migrate_ctx_->set_result_code(OB_SUCCESS);
    validate_pg_ctx_->result_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

ObValidateClogDataTask::ObValidateClogDataTask()
    : ObITask(TASK_TYPE_VALIDATE_BACKUP),
      is_inited_(false),
      cur_clog_file_id_(-1),
      migrate_ctx_(NULL),
      validate_pg_ctx_(NULL),
      archive_log_file_store_(NULL)
{}

ObValidateClogDataTask::~ObValidateClogDataTask()
{}

int ObValidateClogDataTask::init(
    const int64_t clog_file_id, ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "validate clog data task init twice", K(ret));
  } else if (!migrate_ctx.is_valid() || !validate_pg_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else {
    cur_clog_file_id_ = clog_file_id;
    migrate_ctx_ = &migrate_ctx;
    validate_pg_ctx_ = &validate_pg_ctx;
    archive_log_file_store_ = &(validate_pg_ctx_->archive_file_store_);
    clog_end_timestamp_ = validate_pg_ctx_->clog_end_timestamp_;
    is_inited_ = true;
  }
  return ret;
}

int ObValidateClogDataTask::generate_next_task(share::ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  ObIDag* dag = get_dag();
  ObValidateClogDataTask* tmp_next_task = NULL;
  const int64_t next_clog_file_id_ = cur_clog_file_id_ + 1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "validate clog data task do not init", K(ret));
  } else if (next_clog_file_id_ > validate_pg_ctx_->max_clog_file_id_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be null", K(ret));
  } else if (OB_FAIL(dag->alloc_task(tmp_next_task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_clog_file_id_, *migrate_ctx_, *validate_pg_ctx_))) {
    STORAGE_LOG(WARN, "failed to init next task", K(ret));
  } else {
    next_task = tmp_next_task;
    tmp_next_task = NULL;
  }
  return ret;
}

int ObValidateClogDataTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to validate clog data", K(cur_clog_file_id_), K(validate_pg_ctx_->pg_key_));
  clog::ObLogEntry log_entry;
  archive::ObArchiveEntryIterator iter;
  const int64_t start_offset = 0;
  const bool need_limit_bandwidth = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "validate clog data task do not init", K(ret));
  } else if (!validate_pg_ctx_->need_validate_clog_) {
    STORAGE_LOG(INFO, "no need to validate clog", K(ret));
  } else if (OB_FAIL(iter.init(archive_log_file_store_,
                 validate_pg_ctx_->pg_key_,
                 cur_clog_file_id_,
                 start_offset,
                 TIMEOUT,
                 need_limit_bandwidth))) {
    STORAGE_LOG(WARN, "failed to init ObArchiveEntryIterator", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_entry(log_entry))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next entry", K(ret));
        }
      } else {
        int64_t log_id = log_entry.get_header().get_log_id();
        int64_t generation_ts = log_entry.get_header().get_generation_timestamp();
        if (-1 == validate_pg_ctx_->start_log_id_) {
          validate_pg_ctx_->start_log_id_ = log_id;
        }
        if (clog_end_timestamp_ < generation_ts) {
          if (-1 == validate_pg_ctx_->end_log_id_) {
            validate_pg_ctx_->end_log_id_ = log_id;
          }
          break;
        }
        validate_pg_ctx_->total_log_size_ += log_entry.get_total_len();
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    validate_pg_ctx_->result_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

ObValidateBaseDataTask::ObValidateBaseDataTask()
    : ObITask(TASK_TYPE_VALIDATE_BACKUP),
      is_inited_(false),
      task_idx_(-1),
      migrate_ctx_(NULL),
      validate_pg_ctx_(NULL),
      sub_task_(NULL),
      allocator_(NULL),
      macro_index_store_(NULL)
{}

ObValidateBaseDataTask::~ObValidateBaseDataTask()
{}

int ObValidateBaseDataTask::init(
    const int64_t task_idx, ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (!migrate_ctx.is_valid() || !validate_pg_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(validate_pg_ctx.is_valid()),
        K(migrate_ctx.is_valid()),
        K(validate_pg_ctx),
        K(migrate_ctx));
  } else {
    task_idx_ = task_idx;
    migrate_ctx_ = &migrate_ctx;
    validate_pg_ctx_ = &validate_pg_ctx;
    sub_task_ = &(validate_pg_ctx_->sub_tasks_[task_idx_]);
    allocator_ = &(validate_pg_ctx_->allocator_);
    macro_index_store_ = &(validate_pg_ctx_->macro_index_store_);
    is_inited_ = true;
  }
  return ret;
}

int ObValidateBaseDataTask::generate_next_task(share::ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  ObIDag* dag = get_dag();
  const int64_t next_task_idx = task_idx_ + 1;
  ObValidateBaseDataTask* tmp_next_task = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (next_task_idx >= validate_pg_ctx_->sub_task_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "err unexpected, dag must not be null", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_task_idx, *migrate_ctx_, *validate_pg_ctx_))) {
    STORAGE_LOG(WARN, "failed to init next task", K(ret));
  } else {
    next_task = tmp_next_task;
    tmp_next_task = NULL;
  }
  return ret;
}

int ObValidateBaseDataTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to process validate base data task", K(validate_pg_ctx_->pg_key_));
  bool is_valid = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "validate base data task do not init", K(ret));
  } else if (validate_pg_ctx_->is_pg_key_only_in_clog()) {
    STORAGE_LOG(INFO, "no data in base data, skip", K(ret));
  } else if (OB_FAIL(check_base_data_valid(is_valid))) {
    STORAGE_LOG(WARN, "failed to check base data", K(ret));
  } else if (!is_valid) {
    ret = OB_CHECKSUM_ERROR;
  }
  if (OB_FAIL(ret)) {
    validate_pg_ctx_->result_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObValidateBaseDataTask::check_base_data_valid(bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "validate base data task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_task_->macro_block_count_; ++i) {
      const ObBackupMacroIndex& macro_index = sub_task_->macro_block_infos_[i];
      blocksstable::ObBufferReader buffer_reader;
      if (OB_FAIL(fetch_macro_block_with_retry(macro_index, MAX_RETRY_TIME, buffer_reader))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          // TODO : move the report code to ObPartitionService
          continue;
        } else {
          STORAGE_LOG(WARN, "failed to fetch macro block with retry", K(ret));
        }
      } else if (OB_FAIL(checksum_macro_block_data(buffer_reader, is_valid))) {
        STORAGE_LOG(WARN, "failed to checksum macro block data", K(ret));
      } else if (FALSE_IT(++validate_pg_ctx_->finish_macro_block_count_)) {
        // do nothing
      }
    }
  }
  return ret;
}

int ObValidateBaseDataTask::fetch_macro_block_with_retry(
    const ObBackupMacroIndex& macro_index, const int64_t retry_cnt, blocksstable::ObBufferReader& macro_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t retry_times = 0;
    while (retry_times < retry_cnt) {
      if (OB_FAIL(fetch_macro_block(macro_index, macro_data))) {
        ++retry_times;
        usleep(FETCH_MACRO_BLOCK_RETRY_INTERVAL);
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObValidateBaseDataTask::fetch_macro_block(
    const ObBackupMacroIndex& macro_index, blocksstable::ObBufferReader& macro_data)
{
  int ret = OB_SUCCESS;
  common::ObString storage_info = validate_pg_ctx_->path_info_.dest_.get_storage_info();
  ObMacroBlockMeta macro_meta;
  ObBackupPath backup_path;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_file_path(validate_pg_ctx_->path_info_,
                 macro_index.table_id_,
                 macro_index.partition_id_,
                 validate_pg_ctx_->backup_set_id_,
                 macro_index.sub_task_id_,
                 backup_path))) {
    LOG_WARN("failed to get macro block file path", K(ret), K(backup_path));
  } else if (OB_FAIL(
                 read_macro_block_data(backup_path.get_ptr(), storage_info, macro_index, *allocator_, macro_data))) {
    LOG_WARN("failed to read macro block data", K(ret));
  } else {
    LOG_INFO("fetch macro block success", K(ret), K(macro_index));
  }
  return ret;
}

int ObValidateBaseDataTask::read_macro_block_data(const common::ObString& path, const common::ObString& storage_info,
    const ObBackupMacroIndex& macro_index, common::ObIAllocator& allocator, blocksstable::ObBufferReader& macro_data)
{
  int ret = OB_SUCCESS;
  char* read_buf = NULL;
  int64_t buf_size = macro_index.data_length_ + DIO_READ_ALIGN_SIZE;

  if (OB_FAIL(macro_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(macro_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(macro_index));
  } else if (OB_FAIL(
                 ObValidateBackupUtil::read_part_file(path, storage_info, read_buf, buf_size, macro_index.offset_))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(macro_index));
  } else {
    macro_data = ObBufferReader(read_buf, buf_size);
  }
  return ret;
}

int ObValidateBaseDataTask::checksum_macro_block_data(blocksstable::ObBufferReader& buffer_reader, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObBackupCommonHeader* common_header = NULL;
  blocksstable::ObMacroBlockMeta tmp_meta;
  common::ObObj* endkey = NULL;
  if (OB_ISNULL(endkey = reinterpret_cast<ObObj*>(allocator_->alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory for macro block end key", K(ret));
  } else if (OB_ISNULL(tmp_meta.endkey_ = new (endkey) ObObj[common::OB_MAX_COLUMN_NUMBER])) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro meta endkey is null", K(ret));
  } else if (OB_FAIL(buffer_reader.get(common_header))) {
    STORAGE_LOG(WARN, "read macro data common header fail", K(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta index common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    STORAGE_LOG(WARN, "common_header is not valid", K(ret));
  } else if (common_header->data_zlength_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
  } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
    LOG_WARN("failed to check data checksum", K(ret));
  } else {
    is_valid = true;
  }
  return ret;
}

ObValidateFinishTask::ObValidateFinishTask() : ObITask(TASK_TYPE_VALIDATE_FINISH), is_inited_(false), migrate_ctx_(NULL)
{}

ObValidateFinishTask::~ObValidateFinishTask()
{}

int ObValidateFinishTask::init(ObMigrateCtx& migrate_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (!migrate_ctx.is_valid() || OB_ISNULL(validate_pg_ctx_.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(updater_.init(*(validate_pg_ctx_.sql_proxy_), validate_pg_ctx_.is_dropped_tenant_))) {
    STORAGE_LOG(WARN, "failed to init pg validate task updater", K(ret));
  } else {
    migrate_ctx_ = &migrate_ctx;
    is_inited_ = true;
  }
  return ret;
}

// TODO : move the report code to ObPartitionService
int ObValidateFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObPGValidateTaskInfo pg_info;
  const ObPGKey& pg_key = validate_pg_ctx_.pg_key_;
  const int64_t backup_set_id = validate_pg_ctx_.backup_set_id_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(updater_.get_pg_validate_task(validate_pg_ctx_.job_id_,
                 pg_key.get_tenant_id(),
                 OB_START_INCARNATION,
                 backup_set_id,
                 pg_key,
                 pg_info))) {
    STORAGE_LOG(WARN,
        "failed to get pg validate task",
        K(ret),
        K(backup_set_id),
        K(validate_pg_ctx_.job_id_),
        K(pg_key.get_tenant_id()),
        K(pg_key));
  } else {
    ObPGValidateTaskInfo update_pg_info = pg_info;
    ObArray<ObPGValidateTaskInfo> pg_infos;
    update_pg_info.total_macro_block_count_ = validate_pg_ctx_.total_macro_block_count_;
    update_pg_info.finish_macro_block_count_ = validate_pg_ctx_.finish_macro_block_count_;
    update_pg_info.status_ = ObPGValidateTaskInfo::FINISHED;
    update_pg_info.result_ = validate_pg_ctx_.result_;
    if (OB_FAIL(databuff_printf(update_pg_info.log_info_,
            OB_DEFAULT_LOG_INFO_LENGTH,
            "[%ld, %ld)",
            validate_pg_ctx_.start_log_id_,
            validate_pg_ctx_.end_log_id_))) {
      STORAGE_LOG(WARN, "failed to databuff_printf", K(ret));
    } else if (FALSE_IT(update_pg_info.log_size_ = validate_pg_ctx_.total_log_size_)) {
      // do nothing
    } else if (OB_FAIL(pg_infos.push_back(update_pg_info))) {
      STORAGE_LOG(WARN, "failed to push back pg validate task info", K(ret), K(update_pg_info));
    } else if (OB_FAIL(updater_.batch_report_pg_task(pg_infos))) {
      STORAGE_LOG(WARN, "failed to report pg infos", K(ret), K(pg_info));
    } else {
      STORAGE_LOG(INFO, "pg validation task finished", K(ret), K(validate_pg_ctx_.pg_key_));
    }
  }
  return ret;
}

int ObValidateBackupUtil::read_single_file(
    const ObString& path, const ObString& storage_info, ObIAllocator& allocator, char*& buf, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  int64_t file_length = 0;
  read_size = 0;
  int64_t buffer_len = 0;

  if (OB_UNLIKELY(path.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(ret), K(path));
  } else if (OB_FAIL(util.get_file_length(path, storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(path));
  } else if (OB_UNLIKELY(file_length <= 0)) {
    STORAGE_LOG(INFO, "current file is empty", K(path), K(file_length));
  } else {
    buffer_len = file_length;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (OB_FAIL(util.read_single_file(path, storage_info, buf, file_length, read_size))) {
      STORAGE_LOG(WARN, "failed to read all data", K(ret), K(path));
    } else if (file_length != read_size) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size is invalid", K(ret), K(read_size), K(file_length));
    }
  }
  return ret;
}

int ObValidateBackupUtil::read_part_file(
    const ObString& path, const ObString& storage_info, char* buf, const int64_t read_size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  int64_t real_read_size = 0;

  if (OB_UNLIKELY(path.empty() || read_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(util.read_part_file(path, storage_info, buf, read_size, offset, real_read_size))) {
    STORAGE_LOG(WARN, "failed to read part file", K(ret));
  } else if (OB_UNLIKELY(read_size != real_read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "do not read enough data", K(ret));
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
