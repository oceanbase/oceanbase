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

#define USING_LOG_PREFIX RS

#include "ob_backup_table_list_mgr.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
using namespace oceanbase;
using namespace storage;
using namespace common;

namespace rootserver
{
bool ObGetMaxTableListPartNoOp::is_valid() const
{
  return scn_.is_valid() && max_table_list_part_no_ >= 0;
}

int ObGetMaxTableListPartNoOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char prefix[OB_MAX_FILE_NAME_LENGTH] = { 0 };
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name) || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null", K(ret));
  } else if (OB_FAIL(databuff_printf(prefix, OB_MAX_FILE_NAME_LENGTH, "%s.%lu",
                     OB_STR_TABLE_LIST, scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to databuff print", K(ret), K_(scn));
  } else if (0 == strncmp(entry->d_name, prefix, strlen(prefix))) {
    int64_t part_no = 0;
    if (OB_FAIL(ObBackupPath::parse_partial_table_list_file_name(entry->d_name, scn_, part_no))) {
      LOG_WARN("failed to get table list part_no", K(ret), KP(entry->d_name));
    } else if (part_no > max_table_list_part_no_) {
      max_table_list_part_no_ = part_no;
    }
  }
  return ret;
}

bool ObGetMaxTableListSCNOp::is_valid() const
{
  return max_scn_.is_valid();
}

int ObGetMaxTableListSCNOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name) || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null", K(ret));
  } else if (OB_NOT_NULL(strstr(entry->d_name, OB_STR_TABLE_LIST_META_INFO))) {
    share::SCN scn = share::SCN::min_scn();
    if (OB_FAIL(ObBackupPath::parse_table_list_meta_file_name(entry->d_name, scn))) {
      LOG_WARN("failed to get table list scn", K(ret), KP(entry->d_name));
    } else if (scn > max_scn_) {
      max_scn_ = scn;
    }
  }
  return ret;
}

bool ObGetTableListPartialMetasOp::is_valid() const
{
  return scn_.is_valid() && backup_set_dest_->is_valid();
}

int ObGetTableListPartialMetasOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char prefix[OB_MAX_FILE_NAME_LENGTH] = { 0 };
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name) || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null", K(ret));
  } else if (OB_FAIL(databuff_printf(prefix, OB_MAX_FILE_NAME_LENGTH, "%s.%lu",
                                     OB_STR_TABLE_LIST, scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to databuff print", K(ret), K_(scn));
  } else if (0 == strncmp(entry->d_name, prefix, strlen(prefix))) {
    HEAP_VAR(ObBackupPartialTableListDesc, desc) {
      ObBackupDataStore store;
      char path[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
      if (OB_FAIL(store.init(*backup_set_dest_))) {
        LOG_WARN("fail to init backup data store", K(ret), KP_(backup_set_dest));
      } else if (OB_FAIL(store.read_table_list_file(entry->d_name, desc))) {
        LOG_WARN("fail to read table list file", K(ret), K(entry->d_name));
      } else if (desc.count() > 0) {
        ObBackupPartialTableListMeta partial_meta;
        if (OB_FAIL(partial_meta.start_key_.assign(desc.items_.at(0)))
            || OB_FAIL(partial_meta.end_key_.assign(desc.items_.at(desc.count() - 1)))) {
          LOG_WARN("fail to assign partial meta", K(ret));
        } else if (OB_FAIL(partial_metas_.push_back(partial_meta))) {
          LOG_WARN("fail to push back", K(ret), K(partial_meta));
        }
      }
    }
  }
  return ret;
}

ObBackupTableListMgr::ObBackupTableListMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    snapshot_point_(SCN::min_scn()),
    backup_set_dest_(),
    tmp_file_(),
    sql_proxy_(NULL),
    backup_service_(nullptr)
{
}

int ObBackupTableListMgr::init(
                        const uint64_t tenant_id,
                        const share::SCN &snapshot_point,
                        const ObBackupDest &backup_set_dest,
                        rootserver::ObBackupDataService &backup_service,
                        common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupTableListMgr init twice.", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || share::SCN::min_scn() == snapshot_point
             || !backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_point), K(backup_set_dest));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("fail to deep copy backup set dest", K(ret) ,K(backup_set_dest));
  } else {
    tenant_id_ = tenant_id;
    snapshot_point_ = snapshot_point;
    sql_proxy_ = &sql_proxy;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTableListMgr::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
  snapshot_point_ = share::SCN::min_scn();
  backup_set_dest_.reset();
  partial_metas_.reset();
}

int ObBackupTableListMgr::backup_table_list()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t count = 0;
  bool is_meta_exist = false;
  ObBackupDataStore store;
  ObArray<int64_t> serialize_size_array; //serialize_size of each batch
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup table list mgr is not inited", K(ret));
  } else if (OB_FAIL(store.init(backup_set_dest_))) {
    LOG_WARN("fail to init backup store", K(ret), K_(backup_set_dest));
  } else if (OB_FAIL(store.is_table_list_meta_exist(snapshot_point_, is_meta_exist))) {
    LOG_WARN("fail to check is table list meta exist", K(ret), K_(snapshot_point), K_(backup_set_dest));
  } else if (is_meta_exist) { // do nothing
  } else if (OB_FAIL(tmp_file_.open(MTL_ID()))) {
    LOG_WARN("fail to open tmp file", K(ret), K_(tenant_id));
  } else if (OB_FAIL(backup_table_list_to_tmp_file_(count, serialize_size_array))) {
    LOG_WARN("fail to backup table list to tmp file", K(ret), K_(snapshot_point));
  } else if (OB_FAIL(backup_table_list_to_extern_device_(count, serialize_size_array))) {
    LOG_WARN("fail to backup table list to external device", K(ret), K_(snapshot_point), K(count), K(serialize_size_array));
  }
  if (tmp_file_.is_opened() && OB_SUCCESS != (tmp_ret = tmp_file_.close())) {
    LOG_WARN("fail to close tmp file", K(ret), K(tmp_ret), K_(tmp_file));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObBackupTableListMgr::backup_table_list_to_tmp_file_(int64_t &count, ObIArray<int64_t> &serialize_size_array)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  ObSqlString sql;
  int64_t query_timeout = 30 * 60 * 1000000L; //30min
  if (OB_FAIL(sql.append_fmt("SELECT /* QUERY_TIMEOUT(%ld) */ t1.database_name, t2.table_name "
                            "FROM %s as of snapshot %lu AS t1 "
                            "JOIN %s as of snapshot %lu AS t2 "
                            "ON t1.database_id = t2.database_id "
                            "WHERE t2.table_type IN (%d, %d) "
                            "ORDER BY t1.database_name, t2.table_name ASC",
      query_timeout, OB_ALL_DATABASE_TNAME, snapshot_point_.get_val_for_inner_table_field(),
      OB_ALL_TABLE_TNAME, snapshot_point_.get_val_for_inner_table_field(),
      share::schema::ObTableType::USER_TABLE, share::schema::ObTableType::MATERIALIZED_VIEW))) {
    LOG_WARN("failed to assign sql", K(ret), K_(snapshot_point));
  } else {
    HEAP_VARS_2((ObISQLClient::ReadResult, result), (ObBackupPartialTableListDesc, table_list)) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id_), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        partial_metas_.reset();
        common::sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          ObBackupTableListItem item;
          ObString database_name;
          ObString table_name;
          EXTRACT_VARCHAR_FIELD_MYSQL(res, "database_name", database_name);
          EXTRACT_VARCHAR_FIELD_MYSQL(res, "table_name",  table_name);
          if (FAILEDx(item.database_name_.assign(database_name))) {
            LOG_WARN("fail to assign database name", K(ret), K(database_name));
          } else if (OB_FAIL(item.table_name_.assign(table_name))) {
            LOG_WARN("fail to assign table name", K(ret), K(table_name));
          } else if (OB_FAIL(table_list.items_.push_back(item))) {
            LOG_WARN("fail to push back", K(ret), K(item));
          }

          if (OB_SUCC(ret) && BATCH_SIZE == table_list.count()) {
            int64_t serialize_size = 0;
            ObBackupPartialTableListMeta partial_meta;
            if (OB_FAIL(write_to_tmp_file_(table_list, serialize_size))) {
              LOG_WARN("fail to write table list to tmp file", K(ret), K_(snapshot_point), K_(tmp_file));
            } else if (OB_FAIL(serialize_size_array.push_back(serialize_size))) {
              LOG_WARN("fail to push back", K(ret), K(serialize_size));
            } else if (OB_FAIL(partial_metas_.push_back(partial_meta))) {
              LOG_WARN("fail to push back", K(ret), K(partial_meta));
            } else {
              count += table_list.count();
              table_list.reset();
            }
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          int64_t serialize_size = 0;
          ObBackupPartialTableListMeta partial_meta;
          if (0 == table_list.count()) { //skip
          } else if (OB_FAIL(write_to_tmp_file_(table_list, serialize_size))) {
            LOG_WARN("fail to write table list to tmp file", K(ret), K_(snapshot_point), K_(tmp_file));
          } else if (OB_FAIL(serialize_size_array.push_back(serialize_size))) {
            LOG_WARN("fail to push back", K(ret), K(serialize_size));
          } else if (OB_FAIL(partial_metas_.push_back(partial_meta))) {
            LOG_WARN("fail to push back", K(ret), K(partial_meta));
          } else {
            count += table_list.count();
          }
        } else {
          if (OB_SUCC(ret)) {
            ret = OB_ERR_UNEXPECTED;
          }
          LOG_WARN("construct results failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupTableListMgr::backup_table_list_to_extern_device_(const int64_t &count, const ObIArray<int64_t> &serialize_size_array)
{
  int ret = OB_SUCCESS;
  if (count < 0 || serialize_size_array.count() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count), K(serialize_size_array));
  } else {
    int64_t max_file_part_no = 0; //file part_no starts from 1
    if (OB_FAIL(get_max_complete_file_part_no_(max_file_part_no))) {
      LOG_WARN("fail to get max complete file part_no", K(ret), K_(snapshot_point), K_(backup_set_dest));
    } else {
      int64_t read_offset = 0;
      ARRAY_FOREACH_X(serialize_size_array, i, cnt, OB_SUCC(ret)) {
        int64_t serialize_size = serialize_size_array.at(i);
        int64_t part_no = i + 1;
        if (part_no <= max_file_part_no) { // skip
          read_offset += serialize_size;
          LOG_INFO("table list part file already exists", K(part_no), K(max_file_part_no));
        } else if (OB_FAIL(do_backup_table_list_(serialize_size, read_offset, part_no))) {
          LOG_WARN("fail to do backup table list", K(ret),
                                                   K_(snapshot_point),
                                                   K(serialize_size),
                                                   K(read_offset),
                                                   K(part_no));
        } else {
          read_offset += serialize_size;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(backup_service_->check_leader())) {
          LOG_WARN("fail to check leader", K(ret));
        } else if (OB_FAIL(write_table_list_meta_(count, BATCH_SIZE))) {
          LOG_WARN("fail to write table list meta", K(ret), K_(backup_set_dest), K(count), K_(snapshot_point));
        }
      }
    }
  }
  return ret;
}

int ObBackupTableListMgr::write_to_tmp_file_(const ObBackupPartialTableListDesc &table_list, int64_t &serialize_size)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter buffer_writer(ObModIds::BACKUP);
  int64_t last_pos = buffer_writer.pos();
  const int64_t table_list_count = table_list.count();
  serialize_size = table_list.get_serialize_size();

  if (!table_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_list_count), K(serialize_size));
  } else if (OB_FAIL(buffer_writer.write_serialize(table_list))) {
    LOG_WARN("fail to write serialize", K(ret), K(table_list_count));
  } else if (buffer_writer.pos() - last_pos > serialize_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("actual write  size must not be larger than need write size",
            K(ret),
            "cur_pos",
            buffer_writer.pos(),
            K(last_pos),
            K(serialize_size),
            K(table_list_count));
  } else if (OB_FAIL(tmp_file_.write(buffer_writer.data(), buffer_writer.pos()))) {
    LOG_WARN("failed to write to tmp file", K(ret), K(buffer_writer));
  } else {
    buffer_writer.reuse();
  }
  return ret;
}

int ObBackupTableListMgr::read_from_tmp_file_(const int64_t read_size, const int64_t offset, ObBackupPartialTableListDesc  &table_list)
{
  int ret = OB_SUCCESS;
  table_list.reset();
  blocksstable::ObTmpFileIOInfo io_info;
  blocksstable::ObTmpFileIOHandle handle;
  io_info.fd_ = tmp_file_.get_fd();
  io_info.tenant_id_ = tmp_file_.get_tenant_id();
  io_info.dir_id_ = tmp_file_.get_dir();
  io_info.io_desc_.set_wait_event(2);
  io_info.size_ = read_size;
  common::ObArenaAllocator allocator;
  char *buf = NULL;
  if (read_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(read_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(read_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(read_size));
  } else if (FALSE_IT(io_info.buf_ = buf)) {
  } else if (OB_FAIL(blocksstable::ObTmpFileManager::get_instance().pread(io_info, offset, handle))) {
    LOG_WARN("failed to pread from tmp file", K(ret), K(io_info), K(offset), K(read_size));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, read_size);
    if (OB_FAIL(buffer_reader.read_serialize(table_list))) {
      LOG_WARN("failed to read serialize", K(ret));
    }
  }
  return ret;
}

int ObBackupTableListMgr::do_backup_table_list_(const int64_t read_size, const int64_t offset, const int64_t part_no)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  if (read_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid read size", K(ret), K(read_size));
  } else {
    HEAP_VAR(ObBackupPartialTableListDesc, table_list) {
      if (OB_FAIL(store.init(backup_set_dest_))) {
        LOG_WARN("fail to init backup store", K(ret), K_(backup_set_dest));
      } else if (OB_FAIL(read_from_tmp_file_(read_size, offset, table_list))) {
        LOG_WARN("fail to read table list from tmp file", K(read_size), K(offset), K_(tmp_file));
      } else if (!table_list.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table list read from tmp file is invalid", K(ret), K(read_size), K(offset), K_(tmp_file));
      } else if (OB_FAIL(store.write_single_table_list_part_file(snapshot_point_, part_no, table_list))) {
        LOG_WARN("fail to write single table list file",K(ret), K_(snapshot_point), K(part_no),  K_(backup_set_dest));
      }
    }
  }
  return ret;
}

int ObBackupTableListMgr::write_table_list_meta_(const int64_t total_count, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  DEBUG_SYNC(BEFORE_WRITE_TABLE_LIST_META_INFO);
  HEAP_VAR(ObBackupTableListMetaInfoDesc, desc) {
    desc.scn_ = snapshot_point_;
    desc.count_ = total_count;
    desc.batch_size_ = batch_size;
    if (OB_FAIL(desc.partial_metas_.assign(partial_metas_))) {
      LOG_WARN("fail to assign partial metas", K(ret), K_(partial_metas));
    } else if (OB_FAIL(store.init(backup_set_dest_))) {
      LOG_WARN("fail to init backup store", K(ret), K_(backup_set_dest));
    } else if (OB_FAIL(store.write_table_list_meta_info(snapshot_point_, desc))) {
      LOG_WARN("fail to write meta file", K(ret),K_(snapshot_point), K_(backup_set_dest), K(total_count));
    } else {
      LOG_INFO("write table list finish", K(ret),K_(snapshot_point), K_(backup_set_dest));
    }
  }

  return ret;
}

int ObBackupTableListMgr::get_max_complete_file_part_no_(int64_t &part_no)
{
  int ret = OB_SUCCESS;
  part_no = 0;
  share::ObBackupPath path;
  ObBackupStorageInfo *storage_info;
  ObBackupIoAdapter util;
  ObGetMaxTableListPartNoOp max_part_no_op(snapshot_point_, part_no);

   if (OB_FAIL(share::ObBackupPathUtil::get_table_list_dir_path(backup_set_dest_, path))) {
      LOG_WARN("fail to get table list dir path", K(ret), K_(backup_set_dest));
  } else if (OB_ISNULL(storage_info = backup_set_dest_.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get storage_info", K(ret), K_(backup_set_dest));
  } else if (OB_FAIL(util.list_files(path.get_obstr(), storage_info, max_part_no_op))) {
    LOG_WARN("fail to get max complete file part_no", K(ret), K(path), K_(backup_set_dest));
  }

  return ret;
}

} //namespace rootserver
} //namespace oceanbase