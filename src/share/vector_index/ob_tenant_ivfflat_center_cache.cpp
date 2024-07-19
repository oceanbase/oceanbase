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
#include "share/vector_index/ob_tenant_ivfflat_center_cache.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{
/*
* ObTableIvfflatCenters Impl
*/
int ObTableIvfflatCenters::init(const int64_t tenant_id, const common::ObIArray<ObTypeVector *> &array)
{
  int ret = OB_SUCCESS;
  if (array.empty()) {
    count_ = 0;
    centers_ = nullptr;
    // ret = OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret));
  } else {
    count_ = array.count();
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "TblIvfCts"));
    if (OB_ISNULL(centers_ = static_cast<ObTypeVector *>(allocator_.alloc(count_ * sizeof(ObTypeVector))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator centers", K(ret), K(count_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
        if (FALSE_IT(centers_[i].reset())) {
        } else if (OB_FAIL(centers_[i].deep_copy(*array.at(i), allocator_))) {
          LOG_WARN("failed to deep copy vector", K(ret), K(*array.at(i)));
        }
      }
    }
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

int ObTableIvfflatCenters::init(const int64_t tenant_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (0 >= count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count));
  } else {
    count_ = count;
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "TblIvfCts"));
    if (OB_ISNULL(centers_ = static_cast<ObTypeVector *>(allocator_.alloc(count_ * sizeof(ObTypeVector))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator centers", K(ret), K(count_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTableIvfflatCenters::add(const int64_t center_idx, const ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  if (center_idx >= count_ || OB_ISNULL(centers_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(count), K(center_idx), K(vector), K_(centers));
  } else if (FALSE_IT(centers_[center_idx].reset())) {
  } else if (OB_FAIL(centers_[center_idx].deep_copy(vector, allocator_))) {
    LOG_WARN("failed to deep copy vector", K(ret), K(vector));
  }
  return ret;
}

void ObTableIvfflatCenters::destroy()
{
  centers_ = nullptr;
  allocator_.reset();
}

const ObTypeVector &ObTableIvfflatCenters::at(const int64_t idx) const
{
  OB_ASSERT(idx >= 0 && idx < count_);
  return centers_[idx];
}

/*
* ObTenantIvfflatCenterCache Impl
*/
int ObTenantIvfflatCenterCache::mtl_init(ObTenantIvfflatCenterCache *&ivfflat_center_cache)
{
  return ivfflat_center_cache->init(MTL_ID());
}

int ObTenantIvfflatCenterCache::init(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantIvfflatCenterCache has already been initiated", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), PAGE_SIZE, lib::ObMemAttr(tenant_id, "CtCache"), 0))) {
    LOG_WARN("failed to init fifo allocator", K(ret));
  } else if (OB_FAIL(map_.create(BUCKET_LIMIT, "CtCacheBkt", "CtCacheNode", tenant_id))) {
    LOG_WARN("failed to create map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantIvfflatCenterCache::destroy()
{
  is_inited_ = false;
  if (map_.created()) {
    for (TableCenterMap::iterator iter = map_.begin();
        iter != map_.end(); ++iter) {
      ObTableIvfflatCenters *centers = iter->second;
      if (OB_ISNULL(centers)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "centers should not be NULL", KPC(this), KPC(centers));
      } else {
        centers->~ObTableIvfflatCenters();
        allocator_.free(centers);
      }
    }
    map_.destroy();
  }
}

int ObTenantIvfflatCenterCache::decode_centers(
    common::ObIArray<ObTypeVector *> &array,
    ObIAllocator &allocator,
    ObVectorArray &vec_array,
    const int64_t dims,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  int64_t part_cnt = -1;
  int64_t part_offset = -1;
  float* centers = nullptr; 
  if (OB_FAIL(vec_array.get_part_cnt(index, part_cnt))) {
    LOG_WARN("fail to get partition cluster cnt", K(ret), K(index));
  } else if (OB_FAIL(vec_array.get_part_offset(index, dims, part_offset))) {
    LOG_WARN("fail to get partition cluster offset", K(ret), K(index));
  } else {
    centers = vec_array.ptr_ + part_offset;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; ++i) {
    ObTypeVector *vector = nullptr;
    if (OB_ISNULL(vector = static_cast<ObTypeVector*>(allocator.alloc(sizeof(ObTypeVector))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vector", K(ret));
    } else if (FALSE_IT(vector->assign(centers + dims * i, dims))) {
    } else if (OB_FAIL(array.push_back(vector))) {
      LOG_WARN("failed to push back array", K(ret));
    } else {
      LOG_INFO("######ivfflatindexcache###### decode centers from schema", K(index), K(*vector));
    }
  }
  return ret;
}

int ObTenantIvfflatCenterCache::put(const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObTableIvfflatCenters *entry = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantIvfflatCenterCache is not inited", K(ret));
  } else if (!table_schema.is_valid() || !table_schema.is_using_ivfflat_index()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema));
  } else {
    ObArenaAllocator allocator(lib::ObLabel("CtCache"));
    const int64_t partition_num = PARTITION_LEVEL_ZERO == table_schema.get_part_level() ? 1 : table_schema.get_partition_num();
    const int64_t length = table_schema.get_vector_ivfflat_centers_str().length();
    const char* ptr = table_schema.get_vector_ivfflat_centers();
    ObArray<ObTypeVector *> array;
    array.set_attr(ObMemAttr(MTL_ID(), "CtCache"));
    ObVectorArray centers;
    int64_t pos = 0;
    if (OB_FAIL(centers.deserialize(ptr, length, pos))) {
      LOG_WARN("failed to deserialize center array", K(ret));
    }
    // TODO:(@wangmiao) change the method to get dims.
    int64_t dims = -1;
    const schema::ObColumnSchemaV2 *column_schema = table_schema.get_column_schema(table_schema.get_index_info().get_column(0)->column_id_);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
    } else if (OB_FALSE_IT(dims = static_cast<int64_t>(column_schema->get_accuracy().get_length()))) {
    } else if (OB_UNLIKELY(partition_num != centers.part_cnt_length_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partiton num not match", K(ret), K(partition_num), K(centers.part_cnt_length_));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      const int64_t idx = PARTITION_LEVEL_ZERO == table_schema.get_part_level() ? -1 : i;
      array.reuse();
      if (OB_FAIL(decode_centers(array, allocator, centers, dims, i))) {
        LOG_WARN("failed to decode centers", K(ret));
      } else if (OB_FAIL(put(table_schema.get_table_id(),
                             idx, static_cast<ObVectorDistanceType>(table_schema.get_vector_distance_func()), array))) {
        LOG_WARN("failed to put into cache", K(ret), K(table_schema));
      }
    }
  }
  return ret;
}

int ObTenantIvfflatCenterCache::put(
    const int64_t table_id,
    const int64_t partition_idx,
    const ObVectorDistanceType dis_type,
    const common::ObIArray<ObTypeVector *> &array)
{
  int ret = OB_SUCCESS;
  ObTableIvfflatCenters *entry = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantIvfflatCenterCache is not inited", K(ret), K(table_id));
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
        entry->~ObTableIvfflatCenters();
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
          entry->~ObTableIvfflatCenters();
          allocator_.free(entry);
          entry = nullptr;
        }
      } else {
        LOG_INFO("######ivfflatindexcache###### set new entry", K(table_id), K(partition_idx));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("success to put centers", K(ret), K(table_id), K(partition_idx), K(dis_type));
  }
  return ret;
}

int ObTenantIvfflatCenterCache::get(const int64_t table_id, const int64_t partition_idx, ObTableIvfflatCenters *&centers)
{
  int ret = OB_SUCCESS;
  centers = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantIvfflatCenterCache is not inited", K(ret), K(table_id));
  } else {
    common::SpinRLockGuard RLockGuard(rwlock_);
    if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, partition_idx), centers))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObTenantIvfflatCenterCache::drop(const int64_t table_id, const int64_t part_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantIvfflatCenterCache is not inited", K(ret), K(table_id));
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
    LOG_INFO("success to drop cached centers", K(ret), K(table_id), K(part_count));
  }
  return ret;
}

int ObTenantIvfflatCenterCache::erase_map_entry(const int64_t table_id, const int64_t part_idx)
{
  int ret = OB_SUCCESS;
  ObTableIvfflatCenters *centers = nullptr;
  if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, part_idx), centers))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get from map", K(ret), K(table_id), K(part_idx));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    centers->~ObTableIvfflatCenters();
    allocator_.free(centers);
    centers = nullptr;
    if (OB_FAIL(map_.erase_refactored(ObTableCenterKey(table_id, part_idx)))) {
      LOG_WARN("failed to erase from map", K(ret), K(table_id), K(part_idx));
    }
  }
  return ret;
}

int ObTenantIvfflatCenterCache::check_ivfflat_index_table_id_valid(
    const int64_t tenant_id,
    const int64_t ivfflat_index_table_id,
    const int64_t base_table_id)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table_schema = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == ivfflat_index_table_id || OB_INVALID == base_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ivfflat_index_table_id), K(base_table_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ivfflat_index_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(ivfflat_index_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exists", K(ret), K(tenant_id), K(ivfflat_index_table_id));
  } else if (OB_UNLIKELY(base_table_id != table_schema->get_data_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ivfflat_index_table_id is not the index table of base_table_id", K(ret), K(ivfflat_index_table_id), K(base_table_id));
  }
  return ret;
}

int ObTenantIvfflatCenterCache::get_partition_index_with_tablet_id(
      const int64_t tenant_id,
      const int64_t table_id,
      const uint64_t tablet_id,
      int64_t &partition_index)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table_schema = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  partition_index = -1;
  int64_t sub_part_id = -1;
  const ObPartition *part = nullptr;
  common::ObTabletID struct_tablet_id(tablet_id);
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exists", K(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(
              PARTITION_LEVEL_ZERO != table_schema->get_part_level() &&
              PARTITION_LEVEL_ONE != table_schema->get_part_level())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multi-level partition is not supported", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_part_idx_by_tablet(struct_tablet_id, partition_index, sub_part_id))) {
    LOG_WARN("fail to get part id by tablet_id", K(ret), K(tablet_id));
  }
  return ret;
}

int ObTenantIvfflatCenterCache::set_partition_name(
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
int ObTenantIvfflatCenterCache::get_nearest_center(
    const ObTypeVector &qvector,
    const int64_t table_id,
    const ObTabletID &tablet_id,
    ObObj &cell)
{
  int ret = OB_SUCCESS;
  ObTableIvfflatCenters *centers = nullptr;
  const uint64_t tenant_id = MTL_ID();
  ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
  allocator.set_attr(ObMemAttr(tenant_id, "IvfCent"));
  ObString partition_name;
  int64_t partition_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantIvfflatCenterCache is not inited", K(ret), K(table_id), K(tablet_id));
  } else if (OB_FAIL(set_partition_name(tenant_id, table_id, tablet_id, allocator, partition_name, partition_idx))) {
    LOG_WARN("failed to set partition name", K(ret), K(tenant_id), K(table_id), K(tablet_id));
  } else {
    common::SpinWLockGuard RLockGuard(rwlock_);
    if (OB_FAIL(map_.get_refactored(ObTableCenterKey(table_id, partition_idx), centers))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", K(ret), K(table_id), K(partition_idx), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      LOG_INFO("get centers from cache", K(ret), K(table_id), K(tablet_id), K(partition_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(centers)) {
      common::SpinWLockGuard WLockGuard(rwlock_);
      const schema::ObTableSchema *index_table_schema = nullptr;
      const schema::ObTableSchema *container_table_schema = nullptr;
      schema::ObSchemaGetterGuard schema_guard;
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
      } else if (OB_FAIL(schema::ObTableSchema::build_index_table_name(allocator, table_id, index_name, container_table_name))) {
        LOG_WARN("failed to get index name");
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                      index_table_schema->get_database_id(),
                                                      container_table_name,
                                                      true,
                                                      container_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(container_table_name), K(container_table_schema));
      } else if (OB_ISNULL(container_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index table schema failed", K(ret), "database_id", index_table_schema->get_database_id(), K(index_table_schema));
      } else {
        ObSqlString sql_str;
        ObSqlString count_str;
        const ObString &table_name = container_table_schema->get_table_name_str();
        const uint64_t database_id = container_table_schema->get_database_id();
        ObString database_name;
        const schema::ObDatabaseSchema *db_schema = nullptr;
        if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, db_schema))) {
          LOG_WARN("fail to get database schema", K(ret), K(tenant_id), K(database_id), K(container_table_schema));
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
          if (OB_FAIL(container_table_schema->get_column_ids(column_ids))) {
            LOG_WARN("fail to get column ids", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
              if (FALSE_IT(col_id = column_ids.at(i).col_id_)) {
              } else if (OB_ISNULL(column_schema = container_table_schema->get_column_schema(col_id))) {
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
          } else if (partition_name.empty() && OB_FAIL(sql_str.assign_fmt("SELECT %.*s from `%.*s`.`%.*s` order by center_idx",
              static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(table_name.length()), table_name.ptr()))) {
            LOG_WARN("fail to assign select sql string", K(ret));
          } else if (OB_FAIL(count_str.assign_fmt("SELECT count(*) as cnt from `%.*s`.`%.*s`",
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(table_name.length()), table_name.ptr()))) {
            LOG_WARN("fail to assign select sql string", K(ret));
          } else if (!partition_name.empty() && OB_FAIL(sql_str.assign_fmt("SELECT %.*s from `%.*s`.`%.*s` partition(%.*s) order by center_idx" ,
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
              // get centers
              if (FAILEDx(create_map_entry(count, centers))) {
                LOG_WARN("failed to create centers", K(ret), K(count));
              } else if (FALSE_IT(centers->set_dis_type(distance_type))) {
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
                    LOG_WARN("fail to get int", K(ret), K(column_id));
                  } else if (OB_FAIL(result->get_vector(result->get_row()->get_count() - 1, tmp_vec))) {
                    LOG_WARN("fail to get vector", K(ret), "column_id", result->get_row()->get_count() - 1);
                  } else if (OB_FAIL(centers->add(center_idx, tmp_vec))) {
                    LOG_WARN("fail to add vector", K(ret));
                  }
                }
                if (FAILEDx(map_.set_refactored(ObTableCenterKey(table_id, partition_idx), centers))) {
                  LOG_WARN("failed to set entry", K(ret), K(table_id), K(partition_idx), K(tablet_id));
                  // free
                  if (OB_NOT_NULL(centers)) {
                    centers->~ObTableIvfflatCenters();
                    allocator_.free(centers);
                    centers = nullptr;
                  }
                } else {
                  LOG_INFO("success to cache centers", K(ret), K(table_id), K(partition_idx), K(tablet_id));
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(centers)) {
      double distance = 0;
      double min_distance = DBL_MAX;
      int64_t min_idx = -1;
      const ObVectorDistanceType distance_type = centers->get_dis_type() == INNER_PRODUCT ? COSINE : centers->get_dis_type();
      for (int64_t i = 0; OB_SUCC(ret) && i < centers->count(); ++i) {
        const ObTypeVector &tmp_vec = centers->at(i);
        if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(qvector, distance))) {
          LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector));
        } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, qvector, distance))) {
          LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector));
        } else if (distance < min_distance) {
          min_distance = distance;
          min_idx = i;
        }
      }
      if (OB_SUCC(ret)) {
        if (-1 == min_idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(min_idx));
        } else {
          cell.set_int(min_idx);
        }
      }
    }
  }
  return ret;
}

int ObTenantIvfflatCenterCache::create_map_entry(const common::ObIArray<ObTypeVector *> &array, ObTableIvfflatCenters *&entry)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableIvfflatCenters)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(entry = new (buf) ObTableIvfflatCenters())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry is nullptr", K(ret));
  } else if (OB_FAIL(entry->init(MTL_ID(), array))) {
    LOG_WARN("failed to init entry", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(entry)) {
      entry->~ObTableIvfflatCenters();
      entry = nullptr;
    }
    allocator_.free(buf);
  }
  return ret;
}

int ObTenantIvfflatCenterCache::create_map_entry(const int64_t count, ObTableIvfflatCenters *&entry)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableIvfflatCenters)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(entry = new (buf) ObTableIvfflatCenters())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry is nullptr", K(ret));
  } else if (OB_FAIL(entry->init(MTL_ID(), count))) {
    LOG_WARN("failed to init entry", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(entry)) {
      entry->~ObTableIvfflatCenters();
      entry = nullptr;
    }
    allocator_.free(buf);
  }
  return ret;
}
} // share
} // oceanbase