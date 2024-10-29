/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX COMMON
#include "share/vector_index/ob_tenant_pq_center_cache.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{
/*
* ObTablePQCenters Impl
*/
int ObTablePQCenters::init(const int64_t tenant_id, const ObArray<common::ObIArray<ObTypeVector *> *> &array)
{
  int ret = OB_SUCCESS;
  if (array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    seg_ = array.count();
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "TblIvfPQCts"));
    if (OB_ISNULL(seg_center_count_ = static_cast<int64_t *>(allocator_.alloc(seg_ * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator pq centers", K(ret), K(seg_));
    } else {
      count_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < seg_; ++i) {
        if (OB_ISNULL(array.at(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(i));
        } else {
          seg_center_count_[i] = array.at(i)->count();
          count_ += seg_center_count_[i];
        }
      }
      if (OB_ISNULL(pq_centers_ = static_cast<ObTypeVector *>(allocator_.alloc(count_ * sizeof(ObTypeVector))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator pq centers", K(ret), K(seg_));
      } else {
        int64_t offset = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < seg_; ++i) {
          for (int64_t j = 0; OB_SUCC(ret) && j < seg_center_count_[i]; ++j) {
            if (FALSE_IT(pq_centers_[offset + j].reset())) {
            } else if (OB_FAIL(pq_centers_[offset + j].deep_copy(*array.at(i)->at(j), allocator_))) {
              LOG_WARN("failed to deep copy vector", K(ret), K(*array.at(i)->at(j)));
            }
          }
          offset += seg_center_count_[i];
        }
      }
    }
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

int ObTablePQCenters::init(const int64_t tenant_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (0 >= count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count));
  } else {
    count_ = count;
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "TblIvfPQCts"));
    if (OB_ISNULL(pq_centers_ = static_cast<ObTypeVector *>(allocator_.alloc(count_ * sizeof(ObTypeVector))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator pq centers", K(ret), K(count_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTablePQCenters::add(const int64_t seg_idx, const int64_t center_idx, const ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  if (seg_idx >= count_ || OB_ISNULL(pq_centers_) || center_idx >= seg_center_count_[seg_idx]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(count), K(center_idx), K(vector), K_(pq_centers));
  }
  
  if (OB_SUCC(ret)) {
    int64_t offset = 0;
    for (int64_t i = 0; i < seg_idx; ++i) {
      offset += seg_center_count_[i];
    }
    offset += center_idx;
    if (FALSE_IT(pq_centers_[offset].reset())) {
    } else if (OB_FAIL(pq_centers_[offset].deep_copy(vector, allocator_))) {
      LOG_WARN("failed to deep copy vector", K(ret), K(vector));
    }
  }
  
  return ret;
}

void ObTablePQCenters::destroy()
{
  pq_centers_ = nullptr;
  seg_center_count_ = nullptr;
  allocator_.reset();
}

const ObTypeVector &ObTablePQCenters::at(const int64_t seg_idx, const int64_t center_idx) const
{
  OB_ASSERT(seg_idx >= 0 && seg_idx < seg_ && center_idx >= 0 && center_idx < seg_center_count_[seg_idx]);
  int64_t offset = 0;
  for (int64_t i = 0; i < seg_idx; ++i) {
    offset += seg_center_count_[i];
  }
  offset += center_idx;
  return pq_centers_[offset];
}

const ObTypeVector &ObTablePQCenters::at(const int64_t idx) const
{
  OB_ASSERT(idx >= 0 && idx < count_);
  return pq_centers_[idx];
}


/*
* ObTenantPQCenterCache Impl
*/
int ObTenantPQCenterCache::mtl_init(ObTenantPQCenterCache *&pq_center_cache)
{
  return pq_center_cache->init(MTL_ID());
}

int ObTenantPQCenterCache::init(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantPQCenterCache has already been initiated", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), PAGE_SIZE, lib::ObMemAttr(tenant_id, "PQCtCache"), 0))) {
    LOG_WARN("failed to init fifo allocator", K(ret));
  } else if (OB_FAIL(map_.create(BUCKET_LIMIT, "PQCtCacheBkt", "PQCtCacheNode", tenant_id))) {
    LOG_WARN("failed to create map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantPQCenterCache::destroy()
{
  is_inited_ = false;
  if (map_.created()) {
    for (TablePQCenterMap::iterator iter = map_.begin();
        iter != map_.end(); ++iter) {
      ObTablePQCenters *pq_centers = iter->second;
      if (OB_ISNULL(pq_centers)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "centers should not be NULL", KPC(this), KPC(pq_centers));
      } else {
        pq_centers->~ObTablePQCenters();
        allocator_.free(pq_centers);
      }
    }
    map_.destroy();
  }
}

int ObTenantPQCenterCache::put(
    const int64_t table_id,
    const int64_t partition_idx,
    const ObVectorDistanceType dis_type,
    const ObArray<common::ObIArray<ObTypeVector *> *> &array)
{
  int ret = OB_SUCCESS;
  ObTablePQCenters *entry = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantPQCenterCache is not inited", K(ret), K(table_id));
  } else if (OB_INVALID_ID == table_id || INVALID_DISTANCE_TYPE == dis_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(dis_type));
  } else {
    common::SpinWLockGuard WLockGuard(rwlock_);
    if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, partition_idx), entry))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get from map", K(ret), K(table_id), K(partition_idx));
      }
    }
    if (OB_SUCC(ret)) {
      // free old entry
      if (OB_NOT_NULL(entry)) {
        entry->~ObTablePQCenters();
        allocator_.free(entry);
        entry = nullptr;
      }
      if (OB_FAIL(create_map_entry(array, entry))) {
        LOG_WARN("failed to create entry", K(ret), K(table_id));
      } else if (FALSE_IT(entry->set_dis_type(dis_type))) {
      } else if (OB_FAIL(map_.set_refactored(ObTableCenterKey(table_id, partition_idx), entry))) {
        LOG_WARN("failed to set entry", K(ret), K(table_id));
        // free
        if (OB_NOT_NULL(entry)) {
          entry->~ObTablePQCenters();
          allocator_.free(entry);
          entry = nullptr;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("success to put pq centers", K(ret), K(table_id), K(partition_idx), K(dis_type));
  }
  return ret;
}

int ObTenantPQCenterCache::get(const int64_t table_id, const int64_t partition_idx, ObTablePQCenters *&pq_centers)
{
  int ret = OB_SUCCESS;
  pq_centers = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantPQCenterCache is not inited", K(ret), K(table_id));
  } else {
    common::SpinRLockGuard RLockGuard(rwlock_);
    if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, partition_idx), pq_centers))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObTenantPQCenterCache::drop(const int64_t table_id, const int64_t part_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantPQCenterCache is not inited", K(ret), K(table_id));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else {
    common::SpinRLockGuard WLockGuard(rwlock_);
    if (0 == part_count) {
      if (OB_FAIL(erase_map_entry(table_id, -1))) {
        LOG_WARN("failed to erase map entry", K(ret), K(table_id));
      }
    } else {
      // ignore and overwrite error code
      for (int64_t i = 0; i < part_count; ++i) {
        if (OB_FAIL(erase_map_entry(table_id, i))) {
          LOG_WARN("failed to erase map entry", K(ret), K(table_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("success to drop cached pq centers", K(ret), K(table_id), K(part_count));
  }
  return ret;
}

int ObTenantPQCenterCache::erase_map_entry(const int64_t table_id, const int64_t part_idx)
{
  int ret = OB_SUCCESS;
  ObTablePQCenters *pq_centers = nullptr;
  if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, part_idx), pq_centers))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get from map", K(ret), K(table_id), K(part_idx));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    pq_centers->~ObTablePQCenters();
    allocator_.free(pq_centers);
    pq_centers = nullptr;
    if (OB_FAIL(map_.erase_refactored(ObTableCenterKey(table_id, part_idx)))) {
      LOG_WARN("failed to erase from map", K(ret), K(table_id), K(part_idx));
    }
  }
  return ret;
}

int ObTenantPQCenterCache::set_partition_name(
    const int64_t tenant_id,
    const int64_t table_id,
    const common::ObTabletID &tablet_id,
    ObIAllocator &allocator,
    ObString &partition_name,
    int64_t &partition_index)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id));
  }
  partition_name.reset();
  const schema::ObTableSchema *table_schema = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  partition_index = -1;
  int64_t sub_part_id = -1;
  const ObPartition *part = nullptr;
  char* str_buf = nullptr;
  int64_t pos = 0;
  int64_t str_buf_len = 0;
  if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), KP(table_schema), K(tenant_id), K(table_id));
  } else if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
    // do nothing
  } else if (OB_FAIL(table_schema->get_part_idx_by_tablet(tablet_id, partition_index, sub_part_id))) {
    LOG_WARN("fail to get part id by tablet_id", K(ret), K(tablet_id));
  } else if (OB_FAIL(table_schema->get_partition_by_partition_index(partition_index, CHECK_PARTITION_MODE_NORMAL, part))) {
    LOG_WARN("fail to get partition", K(ret), K(partition_index));
  } else if (OB_UNLIKELY(OB_ISNULL(part))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null", K(ret));
  } else {
    const ObString& part_name = part->get_part_name();
    if (OB_FAIL(databuff_printf(str_buf, str_buf_len, pos, allocator, "%.*s",
                                static_cast<int>(part_name.length()),
                                part_name.ptr()))) {
      LOG_WARN("fail to copy part_name", K(ret), K(part_name));
    } else {
      partition_name.assign_ptr(str_buf, pos);
    }
  }
  return ret;
}

int ObTenantPQCenterCache::get_pq_centers(
    ObTablePQCenters *&pq_centers,
    const uint64_t tenant_id,
    const int64_t table_id,
    const int64_t partition_idx,
    const ObString partition_name,
    const ObTableSchema *index_table_schema,
    const ObTableSchema *second_container_table_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ObSqlString count_str;
  const ObString &table_name = second_container_table_schema->get_table_name_str();
  const uint64_t database_id = second_container_table_schema->get_database_id();
  ObString database_name;
  const schema::ObDatabaseSchema *db_schema = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(tenant_id), K(database_id), K(second_container_table_schema));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
  } else {
    database_name = db_schema->get_database_name_str();
  }
  if (OB_SUCC(ret)) {
    bool is_shadow_column = false;
    int64_t col_id = 0;
    ObArray<schema::ObColDesc> column_ids;
    ObArray<ObColumnNameInfo> column_names;
    const schema::ObColumnSchemaV2 *column_schema = nullptr;
    if (OB_FAIL(second_container_table_schema->get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        if (FALSE_IT(col_id = column_ids.at(i).col_id_)) {
        } else if (OB_ISNULL(column_schema = second_container_table_schema->get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret), K(col_id));
        } else if (OB_FAIL(column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), is_shadow_column)))) {
          LOG_WARN("fail to push back rowkey column name", K(ret), K(column_schema));
        }
      }
    }
    ObSqlString query_column_sql_string;
    if (FAILEDx(ObDDLUtil::generate_column_name_str(column_names, false/*oracle_mode*/, true/*with origin name*/, true/*with alias name*/, false/*use_heap_table_ddl_plan*/, query_column_sql_string))) {
      LOG_WARN("fail to generate column name str", K(ret));
    } else if (partition_name.empty() && OB_FAIL(sql_str.assign_fmt("SELECT %.*s from `%.*s`.`%.*s` order by seg_idx, center_idx",
        static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(table_name.length()), table_name.ptr()))) {
      LOG_WARN("fail to assign select sql string", K(ret));
    } else if (OB_FAIL(count_str.assign_fmt("SELECT count(*) as cnt from `%.*s`.`%.*s`",
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(table_name.length()), table_name.ptr()))) {
      LOG_WARN("fail to assign select sql string", K(ret));
    } else if (!partition_name.empty() && OB_FAIL(sql_str.assign_fmt("SELECT %.*s from `%.*s`.`%.*s` partition(%.*s) order by seg_idx, center_idx" ,
        static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(table_name.length()), table_name.ptr(),
        static_cast<int>(partition_name.length()), partition_name.ptr()))) {
      LOG_WARN("fail to append select sql string", K(ret));
    } else if (!partition_name.empty() && OB_FAIL(count_str.append_fmt(" partition(%.*s)",
        static_cast<int>(partition_name.length()), partition_name.ptr()))) {
      LOG_WARN("fail to append select sql string", K(ret));
    } else {
      ObTypeVector tmp_vec;
      int64_t count = 0;
      int64_t column_id = 0;
      int64_t center_idx = 0;
      int64_t seg_column_id = 1;
      int64_t seg_idx = 0;
      ObVectorDistanceType distance_type = static_cast<ObVectorDistanceType>(index_table_schema->get_vector_distance_func());
      SMART_VAR(ObISQLClient::ReadResult, read_res) {
        sqlclient::ObMySQLResult *result = NULL;
        // get count
        if (OB_FAIL(GCTX.sql_proxy_->read(read_res, tenant_id, count_str.ptr()))) {
          LOG_WARN("fail to scan index table", K(ret), K(tenant_id));
        } else if (OB_ISNULL(result = read_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret), KP(result));
        }  else if (OB_FAIL((result->next()))) {
          LOG_WARN("fail to get result", K(ret));
        } else  {
          EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
          read_res.reset();
          result = nullptr;
        }
        if (count <= 0) {
          LOG_WARN("empty pq centers", K(ret), K(count));
        } else {
          // get pq centers
          if (FAILEDx(create_map_entry(count, pq_centers))) {
            LOG_WARN("failed to create pq centers", K(ret), K(count));
          } else if (FALSE_IT(pq_centers->set_dis_type(distance_type))) {
          } else if (OB_FAIL(GCTX.sql_proxy_->read(read_res, tenant_id, sql_str.ptr()))) {
            LOG_WARN("fail to scan index table", K(ret), K(tenant_id));
          } else if (OB_ISNULL(result = read_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get sql result", K(ret), KP(result));
          } else {
            while (OB_SUCC(ret)) {
              if (OB_FAIL(result->next())) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("fail to get next row", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (FALSE_IT(column_id = 0)) {
              } else if (OB_FAIL(result->get_int(column_id, center_idx))) {
                LOG_WARN("fail to get center idx", K(ret), K(column_id));
              } else if (FALSE_IT(seg_column_id = 0)) {
              } else if (OB_FAIL(result->get_int(seg_column_id, seg_idx))) {
                LOG_WARN("fail to get seg idx", K(ret), K(seg_column_id));
              } else if (OB_FAIL(result->get_vector(result->get_row()->get_count() - 1, tmp_vec))) {
                LOG_WARN("fail to get vector", K(ret), "column_id", result->get_row()->get_count() - 1);
              } else if (OB_FAIL(pq_centers->add(seg_idx, center_idx, tmp_vec))) {
                LOG_WARN("fail to add vector", K(ret));
              }
            }
            if (FAILEDx(map_.set_refactored(ObTableCenterKey(table_id, partition_idx), pq_centers))) {
              LOG_WARN("failed to set entry", K(ret), K(table_id), K(partition_idx));
              // free
              if (OB_NOT_NULL(pq_centers)) {
                pq_centers->~ObTablePQCenters();
                allocator_.free(pq_centers);
                pq_centers = nullptr;
              }
            } else {
              LOG_TRACE("success to cache pq centers", K(ret), K(table_id), K(partition_idx));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObTenantPQCenterCache::convert_to_pq_index_vec(
    ObIAllocator &allocator,
    const ObTypeVector &qvector,
    const int64_t table_id,
    const ObTabletID &tablet_id,
    ObObj &cell)
{
  int ret = OB_SUCCESS;
  ObTablePQCenters *pq_centers = nullptr;
  const uint64_t tenant_id = MTL_ID();
  ObArenaAllocator block_allocator(ObModIds::BLOCK_ALLOC);
  block_allocator.set_attr(ObMemAttr(tenant_id, "IvfPQCent"));
  ObString partition_name;
  int64_t partition_idx = -1;
  int64_t seg_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantPQCenterCache is not inited", K(ret), K(table_id), K(tablet_id));
  } else if (OB_FAIL(set_partition_name(tenant_id, table_id, tablet_id, block_allocator, partition_name, partition_idx))) {
    LOG_WARN("failed to set partition name", K(ret), K(tenant_id), K(table_id), K(tablet_id));
  } else {
    common::SpinWLockGuard RLockGuard(rwlock_);
    if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, partition_idx), pq_centers))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", K(ret), K(table_id), K(partition_idx), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      LOG_TRACE("get pq centers from cache", K(ret), K(table_id), K(tablet_id), K(partition_idx));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(pq_centers)) {
      common::SpinWLockGuard WLockGuard(rwlock_);
      const schema::ObTableSchema *index_table_schema = nullptr;
      const schema::ObTableSchema *container_table_schema = nullptr;
      const schema::ObTableSchema *second_container_table_schema = nullptr;
      schema::ObSchemaGetterGuard schema_guard;
      ObString second_container_table_name;
      ObString container_table_name;
      ObString index_name;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get tenant schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("fail to get table schema", K(ret), KP(index_table_schema), K(tenant_id), K(table_id));
      } else if (OB_FAIL(index_table_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret), K(table_id));
      } else if (FALSE_IT(seg_num = index_table_schema->get_vector_pq_seg())) {
      } else if (OB_FAIL(schema::ObTableSchema::build_index_table_name(block_allocator, table_id, index_name, container_table_name))) {
        LOG_WARN("failed to get container name");
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                      index_table_schema->get_database_id(),
                                                      container_table_name,
                                                      true,
                                                      container_table_schema))) {
        LOG_WARN("fail to get container table schema", KR(ret), K(container_table_name), K(container_table_schema));
      } else if (OB_ISNULL(container_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get container table schema failed", K(ret), "database_id", index_table_schema->get_database_id(), K(index_table_schema));
      } else if (OB_FAIL(schema::ObTableSchema::build_index_table_name(block_allocator, container_table_schema->get_table_id(), index_name, second_container_table_name))) {
        LOG_WARN("failed to get second container name");
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                      index_table_schema->get_database_id(),
                                                      second_container_table_name,
                                                      true,
                                                      second_container_table_schema))) {
        LOG_WARN("fail to get second container schema", KR(ret), K(second_container_table_name), K(second_container_table_schema));
      } else if (OB_ISNULL(second_container_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get second container table schema failed", K(ret), "database_id", container_table_schema->get_database_id(), K(container_table_schema));
      } else if (OB_FAIL(get_pq_centers(pq_centers, tenant_id, table_id, partition_idx, partition_name, index_table_schema, second_container_table_schema))) {
        LOG_WARN("failed to get pq centers", K(ret), K(tenant_id), K(table_id), K(tablet_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(pq_centers)) {
        double distance = 0;
        double min_distance = DBL_MAX;
        int64_t min_idx = -1;
        const ObVectorDistanceType distance_type = pq_centers->get_dis_type() == INNER_PRODUCT ? COSINE : pq_centers->get_dis_type();
        float *pq_index_vec = nullptr;
        if (OB_ISNULL(pq_index_vec = static_cast<float *>(allocator.alloc(pq_centers->get_seg() * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret), K(pq_centers->get_seg()));
        } else {
          for (int64_t qseg_idx = 0; OB_SUCC(ret) && qseg_idx < pq_centers->get_seg(); ++qseg_idx) {
            ObTypeVector *qvector_qseg = nullptr;
            qvector.split(qvector_qseg, pq_centers->at(0).dims(), qseg_idx);
            if (OB_ISNULL(qvector_qseg)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected qvector_qseg", K(ret), K(qseg_idx), K(qvector));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < pq_centers->get_seg_center_count()[qseg_idx]; ++i) {
                const ObTypeVector &tmp_vec = pq_centers->at(qseg_idx, i);
                if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(*qvector_qseg, distance))) {
                  LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), KPC(qvector_qseg));
                } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, *qvector_qseg, distance))) {
                  LOG_WARN("failed to cal distance", K(ret), K(tmp_vec), KPC(qvector_qseg));
                } else if (distance < min_distance) {
                  min_distance = distance;
                  min_idx = i;
                }
              }
            }
            pq_index_vec[qseg_idx] = min_idx;
          }
          if (OB_SUCC(ret)) {
            cell.set_vector(pq_index_vec, pq_centers->get_seg());
          }
        }
      } else {
        float *pq_index_vec = nullptr;
        if (seg_num <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected seg num", K(ret), K(seg_num));
        } else if (OB_ISNULL(pq_index_vec = static_cast<float *>(allocator.alloc(seg_num * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret), K(seg_num));
        } else {
          for (int64_t qseg_idx = 0; OB_SUCC(ret) && qseg_idx < seg_num; ++qseg_idx) {
            pq_index_vec[qseg_idx] = -1;
          }
          if (OB_SUCC(ret)) {
            cell.set_vector(pq_index_vec, seg_num);
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantPQCenterCache::create_map_entry(const ObArray<common::ObIArray<ObTypeVector *> *> &array, ObTablePQCenters *&entry)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTablePQCenters)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(entry = new (buf) ObTablePQCenters())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry is nullptr", K(ret));
  } else if (OB_FAIL(entry->init(MTL_ID(), array))) {
    LOG_WARN("failed to init entry", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(entry)) {
      entry->~ObTablePQCenters();
      entry = nullptr;
    }
    allocator_.free(buf);
  }
  return ret;
}

int ObTenantPQCenterCache::create_map_entry(const int64_t count, ObTablePQCenters *&entry)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTablePQCenters)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(entry = new (buf) ObTablePQCenters())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry is nullptr", K(ret));
  } else if (OB_FAIL(entry->init(MTL_ID(), count))) {
    LOG_WARN("failed to init entry", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(entry)) {
      entry->~ObTablePQCenters();
      entry = nullptr;
    }
    allocator_.free(buf);
  }
  return ret;
}
} // share
} // oceanbase
