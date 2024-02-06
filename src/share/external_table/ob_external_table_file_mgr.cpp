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

#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "ob_external_table_file_mgr.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "observer/ob_inner_sql_connection.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "share/external_table/ob_external_table_utils.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace transaction::tablelock;
namespace share
{

int ObExternalTableFilesKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
   int ret = OB_SUCCESS;
   ObExternalTableFilesKey *new_value = NULL;
   ObDataBuffer allocator(buf, buf_len);
   if (OB_ISNULL(new_value = OB_NEWx(ObExternalTableFilesKey, &allocator))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("fail to allocate memory", K(ret));
   } else {
     new_value->tenant_id_ = this->tenant_id_;
     new_value->table_id_ = this->table_id_;
     new_value->partition_id_ = this->partition_id_;
     key = new_value;
   }
   return ret;
 }

int64_t ObExternalTableFiles::size() const
{
   int64_t size = sizeof(*this) + sizeof(ObString) * file_urls_.count()
                  + sizeof(int64_t) * file_ids_.count() + sizeof(int64_t) * file_sizes_.count();
   for (int i = 0; i < file_urls_.count(); ++i) {
     size += file_urls_.at(i).length();
   }
   return size;
 }

int ObExternalTableFiles::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(buf, buf_len);
  ObExternalTableFiles *new_value = NULL;
  if (OB_ISNULL(new_value = OB_NEWx(ObExternalTableFiles, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  }

  if (OB_SUCC(ret) && this->file_urls_.count() > 0) {
    if (OB_FAIL(new_value->file_urls_.allocate_array(allocator, this->file_urls_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < this->file_urls_.count(); i++) {
        OZ (ob_write_string(allocator, this->file_urls_.at(i), new_value->file_urls_.at(i)));
      }
    }
  }

  if (OB_SUCC(ret) && this->file_ids_.count() > 0) {
    if (OB_FAIL(new_value->file_ids_.allocate_array(allocator, this->file_ids_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      MEMCPY(new_value->file_ids_.get_data(), this->file_ids_.get_data(),
             sizeof(int64_t) * this->file_ids_.count());
    }
  }

  if (OB_SUCC(ret) && this->file_sizes_.count() > 0) {
    if (OB_FAIL(new_value->file_sizes_.allocate_array(allocator, this->file_sizes_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      MEMCPY(new_value->file_sizes_.get_data(), this->file_sizes_.get_data(),
             sizeof(int64_t) * this->file_sizes_.count());
    }
  }
  if (OB_SUCC(ret)) {
    new_value->create_ts_ = this->create_ts_;
  }
  value = new_value;
  return ret;
}

int ObExternalTableFileManager::flush_cache(const uint64_t tenant_id, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObExternalTableFilesKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.partition_id_ = 0;
  if (OB_FAIL(kv_cache_.erase(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to erase value", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObExternalTableFileManager::clear_inner_table_files(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;
  OZ (delete_sql.assign_fmt("DELETE FROM %s WHERE TABLE_ID = %lu AND PART_ID = %lu",
                            OB_ALL_EXTERNAL_TABLE_FILE_TNAME, table_id, 0L));
  OZ (trans.write(tenant_id, delete_sql.ptr(), affected_rows));
  LOG_DEBUG("check clear rows", K(affected_rows));
  return ret;
}

int ObExternalTableFileManager::init()
{
  int ret = OB_SUCCESS;
  OZ (kv_cache_.init("external_table_file_cache"));
  return ret;
}

ObExternalTableFileManager &ObExternalTableFileManager::get_instance()
{
  static ObExternalTableFileManager instance_;
  return instance_;
}

int ObExternalTableFileManager::get_external_files(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const bool is_local_file_on_disk,
    ObIAllocator &allocator,
    ObIArray<ObExternalFileInfo> &external_files,
    ObIArray<ObNewRange *> *range_filter /*default = NULL*/)
{
  return get_external_files_by_part_id(tenant_id, table_id, 0UL, is_local_file_on_disk,  allocator, external_files, range_filter);
}

int ObExternalTableFileManager::get_external_files_by_part_id(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t partition_id,
    const bool is_local_file_on_disk,
    ObIAllocator &allocator,
    ObIArray<ObExternalFileInfo> &external_files,
    ObIArray<ObNewRange *> *range_filter /*default = NULL*/)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObExternalTableFiles *ext_files = NULL;
  ObExternalTableFilesKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.partition_id_ = partition_id;
  if (OB_FAIL(kv_cache_.get(key, ext_files, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from KVCache", K(ret), K(key));
    }
  }

  if ((OB_SUCC(ret) && is_cache_value_timeout(*ext_files))
      || OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(fill_cache_from_inner_table(key, ext_files, handle))) {
      LOG_WARN("fail to fill cache from inner table", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < ext_files->file_urls_.count(); ++i) {
    bool in_ranges = false;
    if (range_filter != NULL && OB_FAIL(ObExternalTableUtils::is_file_id_in_ranges(*range_filter,
                                                                        ext_files->file_ids_.at(i),
                                                                        in_ranges))) {
      LOG_WARN("failed to judge file id in ranges", K(ret));
    } else if (range_filter == NULL || in_ranges) {
      ObExternalFileInfo file_info;
      ObString file_url = ext_files->file_urls_.at(i);
      file_info.file_id_ = ext_files->file_ids_.at(i);
      file_info.file_size_ = ext_files->file_sizes_.at(i);
      if (is_local_file_on_disk) {
        ObString ip_port = file_url.split_on('%');
        OZ (file_info.file_addr_.parse_from_string(ip_port));
      }
      OZ (ob_write_string(allocator, file_url, file_info.file_url_));
      OZ (external_files.push_back(file_info));
    }
  }
  LOG_DEBUG("get external file list result", K(table_id), K(external_files));
  return ret;
}

int ObExternalTableFileManager::update_inner_table_file_list(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  OZ (trans.start(GCTX.sql_proxy_, tenant_id));
  OZ (lock_for_refresh(trans, tenant_id, table_id));
  OZ (update_inner_table_files_list_one_part(tenant_id, table_id, 0, trans, file_urls, file_sizes));
  OZ (trans.end(true));

  if (trans.is_started()) {
    trans.end(false);
  }
  return ret;
}

int ObExternalTableFileManager::get_external_file_list_on_device(const ObString &location,
                                                 ObIArray<ObString> &file_urls,
                                                 ObIArray<int64_t> &file_sizes,
                                                 const ObString &access_info,
                                                 ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  sql::ObExternalDataAccessDriver driver;
  if (OB_FAIL(driver.init(location, access_info))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_file_list(location, file_urls, allocator))) {
    LOG_WARN("get file urls failed", K(ret));
  } else if (OB_FAIL(driver.get_file_sizes(location, file_urls, file_sizes))) {
    LOG_WARN("get file sizes failed", K(ret));
  }
  if (driver.is_opened()) {
    driver.close();
  }

  LOG_DEBUG("show external table files", K(file_urls), K(access_info));
  return ret;
}

int ObExternalTableFileManager::update_inner_table_files_list_one_part(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t partition_id,
    ObMySQLTransaction &trans,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtil::current_time();
  ObSEArray<ObString, 16> old_file_urls;
  ObSEArray<int64_t, 16> old_file_ids;
  ObSEArray<ObString, 16> insert_file_urls;
  ObSEArray<int64_t, 16> insert_file_ids;
  ObSEArray<int64_t, 16> insert_file_sizes;
  ObSEArray<ObString, 16> update_file_urls;
  ObSEArray<int64_t, 16> update_file_sizes;
  ObSEArray<int64_t, 16> update_file_ids;
  ObSEArray<ObString, 16> delete_file_urls;
  ObSEArray<int64_t, 16> delete_file_ids;
  ObArenaAllocator allocator;
  ObSqlString update_sql;
  ObSqlString insert_sql;
  ObSqlString delete_sql;
  int64_t update_rows = 0;
  int64_t insert_rows = 0;
  int64_t max_file_id = 0;// ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID - 1
  common::hash::ObHashMap<ObString, int64_t> hash_map;
  OZ(hash_map.create(std::max(file_urls.count(), old_file_urls.count()) + 1, "ExternalFile"));
  OZ(get_all_records_from_inner_table(allocator, tenant_id, table_id, partition_id, old_file_urls, old_file_ids));
  for (int64_t i = 0; OB_SUCC(ret) && i < old_file_urls.count(); i++) {
    OZ(hash_map.set_refactored(old_file_urls.at(i), old_file_ids.at(i)));
    max_file_id = old_file_ids.at(i) > max_file_id ? old_file_ids.at(i) : max_file_id;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
    int64_t file_id = 0;
    OZ(hash_map.get_refactored(file_urls.at(i), file_id));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OZ(insert_file_urls.push_back(file_urls.at(i)));
      OZ(insert_file_sizes.push_back(file_sizes.at(i)));
      OZ(insert_file_ids.push_back(++max_file_id));
    } else if (ret == OB_SUCCESS) {
      OZ(update_file_urls.push_back(file_urls.at(i)));
      OZ(update_file_sizes.push_back(file_sizes.at(i)));
      OZ(update_file_ids.push_back(file_id));
    }
  }
  OZ(hash_map.reuse());
  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
    OZ(hash_map.set_refactored(file_urls.at(i), 1));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_file_urls.count(); i++) {
    int64_t existed = 0;
    OZ(hash_map.get_refactored(old_file_urls.at(i), existed));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OZ(delete_file_urls.push_back(old_file_urls.at(i)));
      OZ(delete_file_ids.push_back(old_file_ids.at(i)));
    }
  }
  if (OB_SUCC(ret) && delete_file_urls.count() > 0) {
    OZ(delete_sql.assign_fmt("UPDATE %s SET DELETE_VERSION = %ld WHERE (TABLE_ID, PART_ID, FILE_ID) IN (",
                              OB_ALL_EXTERNAL_TABLE_FILE_TNAME, cur_time));
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_file_urls.count(); i++) {
      OZ(delete_sql.append_fmt("%c(%ld, %ld, %ld)", (0 == i) ? ' ' : ',', table_id, partition_id,
                                                      delete_file_ids.at(i)));
    }
    OZ(delete_sql.append(")"));
    OZ(trans.write(tenant_id, delete_sql.ptr(), update_rows));
  }
  if (OB_SUCC(ret) && update_file_urls.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_file_urls.count(); i++) {
      OZ(update_sql.assign_fmt("UPDATE %s SET"
                              " CREATE_VERSION = CASE WHEN DELETE_VERSION != %ld THEN %ld ELSE CREATE_VERSION end,"
                              " DELETE_VERSION = %ld, FILE_SIZE = %ld WHERE TABLE_ID = %lu AND PART_ID = %lu AND FILE_ID=%ld",
                              OB_ALL_EXTERNAL_TABLE_FILE_TNAME,
                              MAX_VERSION, cur_time,
                              MAX_VERSION, update_file_sizes.at(i), table_id, partition_id,
                              update_file_ids.at(i)));
      OZ (trans.write(tenant_id, update_sql.ptr(), update_rows));
    }
  }
  if (OB_SUCC(ret) && insert_file_urls.count() > 0) {
    OZ(insert_sql.assign_fmt("INSERT INTO %s(TABLE_ID,PART_ID,FILE_ID,FILE_URL,CREATE_VERSION,DELETE_VERSION,FILE_SIZE) VALUES",
                                OB_ALL_EXTERNAL_TABLE_FILE_TNAME));
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_file_urls.count(); i++) {
        OZ(insert_sql.append_fmt("%c(%lu,%lu,%ld,'%.*s',%ld,%ld,%ld)",
                                  (0 == i) ? ' ' : ',', table_id, partition_id,
                                  insert_file_ids.at(i),
                                  insert_file_urls.at(i).length(), insert_file_urls.at(i).ptr(),
                                  cur_time, MAX_VERSION, insert_file_sizes.at(i)));
    }
    OZ(trans.write(tenant_id, insert_sql.ptr(), insert_rows));
  }

  return ret;
}

int ObExternalTableFileManager::get_all_records_from_inner_table(ObIAllocator &allocator,
                                                                  int64_t tenant_id,
                                                                  int64_t table_id,
                                                                  int64_t partition_id,
                                                                  ObIArray<ObString> &file_urls,
                                                                  ObIArray<int64_t> &file_ids)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    OZ (sql.append_fmt("SELECT file_url, file_id FROM %s"
                        " WHERE table_id = %lu AND part_id = %lu",
                        OB_ALL_EXTERNAL_TABLE_FILE_TNAME, table_id, partition_id));
    OZ (GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()));
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObString file_url;
          int64_t file_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "file_url", file_url);
          EXTRACT_INT_FIELD_MYSQL(*result, "file_id", file_id, int64_t);
          ObString tmp_url;
          OZ (ob_write_string(allocator, file_url, tmp_url));
          OZ (file_urls.push_back(tmp_url));
          OZ (file_ids.push_back(file_id));
        }
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
          LOG_WARN("get next result failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}
int ObExternalTableFileManager::fill_cache_from_inner_table(
    const ObExternalTableFilesKey &key,
    const ObExternalTableFiles *&ext_files,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;

  //only one worker need do the job
  int64_t bucket_id = key.hash() % LOAD_CACHE_LOCK_CNT;
  int64_t total_wait_secs = 0;

  while (OB_FAIL(fill_cache_locks_[bucket_id].lock(LOCK_TIMEOUT))
         && OB_TIMEOUT == ret && !THIS_WORKER.is_timeout()) {
    total_wait_secs += LOAD_CACHE_LOCK_CNT;
    LOG_WARN("fill external table cache wait", K(total_wait_secs));
  }
  if (OB_SUCC(ret)) {
    //try fetch again
    if (OB_FAIL(kv_cache_.get(key, ext_files, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get from KVCache", K(ret), K(key));
      }
    }

    if ((OB_SUCC(ret) && is_cache_value_timeout(*ext_files))
        || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        int64_t cur_time = ObTimeUtil::current_time();

        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
        }

        OZ (sql.append_fmt("SELECT file_url, file_id, file_size FROM %s"
                           " WHERE table_id = %lu AND part_id = %lu"
                           " AND create_version <=%ld AND %ld < delete_version",
                           OB_ALL_EXTERNAL_TABLE_FILE_TNAME, key.table_id_, key.partition_id_,
                           cur_time, cur_time));
        OZ (GCTX.sql_proxy_->read(res, key.tenant_id_, sql.ptr()));

        if (OB_SUCC(ret)) {
          if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret));
          } else {
            ObSEArray<ObString, 16> temp_file_urls;
            ObSEArray<int64_t, 16> temp_file_ids;
            ObSEArray<int64_t, 16> temp_file_sizes;
            ObArenaAllocator allocator;
            while (OB_SUCC(result->next())) {
              ObString file_url;
              ObString tmp_url;
              int64_t file_id = INT64_MAX;
              int64_t file_size = 0;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "file_url", tmp_url);
              EXTRACT_INT_FIELD_MYSQL(*result, "file_id", file_id, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "file_size", file_size, int64_t);
              OZ (ob_write_string(allocator, tmp_url, file_url));
              OZ (temp_file_urls.push_back(file_url));
              OZ (temp_file_ids.push_back(file_id));
              OZ (temp_file_sizes.push_back(file_size));
            }
            if (OB_FAIL(ret) && OB_ITER_END != ret) {
              LOG_WARN("get next result failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              ObExternalTableFiles temp_ext_files;
              temp_ext_files.create_ts_ = cur_time;
              temp_ext_files.file_urls_ = ObArrayWrap<ObString>(temp_file_urls.get_data(), temp_file_urls.count());
              temp_ext_files.file_ids_ = ObArrayWrap<int64_t>(temp_file_ids.get_data(), temp_file_ids.count());
              temp_ext_files.file_sizes_ = ObArrayWrap<int64_t>(temp_file_sizes.get_data(), temp_file_sizes.count());
              OZ (kv_cache_.put_and_fetch(key, temp_ext_files, ext_files, handle, true));
            }
          }
        }
      }
      LOG_TRACE("external table fill cache", K(ext_files), K(key));
    }
  }
  if (fill_cache_locks_[bucket_id].self_locked()) {
    fill_cache_locks_[bucket_id].unlock();
  }
  return ret;
}

int ObExternalTableFileManager::lock_for_refresh(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t object_id)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  if (OB_ISNULL(conn = dynamic_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    ObLockObjRequest lock_arg;
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_EXTERNAL_TABLE_REFRESH;
    lock_arg.obj_id_ = object_id;
    lock_arg.owner_id_ = ObTableLockOwnerID(get_tid_cache());
    lock_arg.lock_mode_ = EXCLUSIVE;
    lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = 1000L * 1000L * 2; //2s
    while (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn)) && !THIS_WORKER.is_timeout()) {
      LOG_WARN("lock failed try again", K(ret));
    }
  }


  return ret;
}

OB_SERIALIZE_MEMBER(ObExternalFileInfo, file_url_, file_id_, file_addr_, file_size_);

}
}
