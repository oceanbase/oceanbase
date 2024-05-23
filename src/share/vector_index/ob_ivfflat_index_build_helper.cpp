/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON
#include "share/vector_index/ob_ivfflat_index_build_helper.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"
#include "share/ob_ddl_common.h"
#include "share/object/ob_obj_cast.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_param.h"
#include "share/vector_index/ob_tenant_ivfflat_center_cache.h"

namespace oceanbase {
namespace share {
/*
 * ObIvfflatIndexBuildHelper Impl
 */
int ObIvfflatIndexBuildHelper::init(const int64_t tenant_id,
                                    const int64_t lists,
                                    const ObVectorDistanceType distance_type) {
  int ret = OB_SUCCESS;
  if (0 >= lists_ ||
      ObVectorDistanceType::INVALID_DISTANCE_TYPE == distance_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lists), K(distance_type));
  } else if (OB_FAIL(allocator_.init(
                 lib::ObMallocAllocator::get_instance(), PAGE_SIZE,
                 lib::ObMemAttr(tenant_id,
                                "IvfflatBuild")))) { // TODO(@jingshui) limit
                                                     // the max memory size
    LOG_WARN("failed to init fifo fallocator");
  } else if (FALSE_IT(arena_allocator_.set_attr(
                 ObMemAttr(tenant_id, "IvfflatArena")))) {
  } else if (OB_FAIL(center_vectors_[0].reserve(lists))) {
    LOG_WARN("failed to reserve cur_center_vectors", K(ret), K(lists));
  } else if (OB_FAIL(center_vectors_[1].reserve(lists))) {
    LOG_WARN("failed to reserve new_center_vectors", K(ret), K(lists));
  } else {
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      max_iterate_times_ = tenant_config->vector_ivfflat_iters_count;
      elkan_kmeans_ = tenant_config->vector_ivfflat_elkan;
    }
    distance_type_ = distance_type;
    tenant_id_ = tenant_id;
    lists_ = lists;
    init_lists_ = lists;
    is_init_ = true;
  }
  return ret;
}

void ObIvfflatIndexBuildHelper::reuse() {
  cur_idx_ = 0;
  total_cnt_ = 0;
  failed_times_ = 0;
  iterate_times_ = 0;

  status_ = PREPARE_CENTERS;
  reuse_array(center_vectors_[0]);
  reuse_array(center_vectors_[1]);
  if (OB_NOT_NULL(nearest_centers_)) {
    allocator_.free(nearest_centers_);
    nearest_centers_ = nullptr;
  }
  if (OB_NOT_NULL(lower_bounds_)) {
    allocator_.free(lower_bounds_);
    lower_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(upper_bounds_)) {
    allocator_.free(upper_bounds_);
    upper_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(weight_)) {
    arena_allocator_.free(weight_);
    weight_ = nullptr;
  }

  select_sql_str_.reset();
  // allocator_.reset();
  arena_allocator_.reset();
}

void ObIvfflatIndexBuildHelper::destroy() {
  for (int64_t i = 0; i < 2; ++i) {
    reuse_array(center_vectors_[i]);
    center_vectors_[i].reset();
  }
  if (OB_NOT_NULL(nearest_centers_)) {
    allocator_.free(nearest_centers_);
    nearest_centers_ = nullptr;
  }
  if (OB_NOT_NULL(lower_bounds_)) {
    allocator_.free(lower_bounds_);
    lower_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(upper_bounds_)) {
    allocator_.free(upper_bounds_);
    upper_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(weight_)) {
    arena_allocator_.free(weight_);
    weight_ = nullptr;
  }
  allocator_.reset();
  arena_allocator_.reset();
  // LOG_INFO("ObIvfflatIndexBuildHelper destroy finish");
}

int64_t ObIvfflatIndexBuildHelper::to_string(char *buf,
                                             const int64_t buf_len) const {
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(lists), K_(total_cnt), "center_count",
         center_vectors_[cur_idx_].count(), K_(iterate_times),
         K_(max_iterate_times), K_(status), K_(elkan_kmeans));
    // for (int64_t i = 0; i < center_vectors_[cur_idx_].count(); ++i) {
    //   J_COMMA();
    //   BUF_PRINTO(center_vectors_[cur_idx_].at(i));
    // }
    J_OBJ_END();
  }
  return pos;
}

void ObIvfflatIndexBuildHelper::destory_vector(ObTypeVector *&vector) {
  if (OB_NOT_NULL(vector)) {
    vector->destroy(allocator_); // free ptr
    allocator_.free(vector);
    vector = nullptr;
  }
}

void ObIvfflatIndexBuildHelper::reuse_array(ObIArray<ObTypeVector *> &array) {
  for (int64_t i = 0; i < array.count(); ++i) {
    if (OB_ISNULL(array.at(i))) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpect nullptr", K(i), "count",
                    array.count());
    } else {
      destory_vector(array.at(i));
    }
  }
  array.reuse();
}

int ObIvfflatIndexBuildHelper::alloc_random_vector(ObTypeVector *&vector,
                                                   const int64_t vector_size) {
  int ret = OB_SUCCESS;
  vector = nullptr;
  void *buf = nullptr;
  float *val = nullptr;
  if (nullptr == (buf = allocator_.alloc(sizeof(ObTypeVector)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObTypeVector", K(ret));
  } else if (FALSE_IT(vector = new (buf) ObTypeVector())) {
  } else if (nullptr == (val = static_cast<float *>(
                             allocator_.alloc(sizeof(float) * vector_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc double array", K(ret));
  } else {
    ObMysqlRandom random;
    const int64_t current_time = ObClockGenerator::getClock();
    random.init(static_cast<uint64_t>(current_time),
                static_cast<uint64_t>(current_time / 2));
    for (int64_t i = 0; i < vector_size; ++i) {
      val[i] = random.get_double();
    }
    vector->assign(val, vector_size);
  }
  if (OB_FAIL(ret)) {
    destory_vector(vector);
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::alloc_and_copy_vector(const ObTypeVector &other,
                                                     ObTypeVector *&vector) {
  int ret = OB_SUCCESS;
  vector = nullptr;
  void *buf = nullptr;
  if (nullptr == (buf = allocator_.alloc(sizeof(ObTypeVector)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObTypeVector", K(ret));
  } else if (FALSE_IT(vector = new (buf) ObTypeVector())) {
  } else if (OB_FAIL(vector->deep_copy(other, allocator_))) {
    LOG_WARN("failed to deep copy vector", K(ret), K(vector));
  }
  if (OB_FAIL(ret)) {
    destory_vector(vector);
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::alloc_vector(ObTypeVector *&vector,
                                            const int64_t vector_size) {
  int ret = OB_SUCCESS;
  vector = nullptr;
  void *buf = nullptr;
  float *val = nullptr;
  if (nullptr == (buf = allocator_.alloc(sizeof(ObTypeVector)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObTypeVector", K(ret));
  } else if (FALSE_IT(vector = new (buf) ObTypeVector())) {
  } else if (nullptr == (val = static_cast<float *>(
                             allocator_.alloc(sizeof(float) * vector_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc double array", K(ret));
  } else {
    MEMSET(val, 0, sizeof(float) * vector_size);
    vector->assign(val, vector_size);
  }
  if (OB_FAIL(ret)) {
    destory_vector(vector);
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::construct_batch_insert_container_sql_simple(
    common::ObSqlString &container_string, const int64_t dest_table_id,
    const int64_t container_table_id) {
  int ret = OB_SUCCESS;
  if (ObIvfflatBuildStatus::FINISH != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else {
    ObTypeVector vector;
    int64_t nearest_center_idx = -1;
    double min_distance = DBL_MAX;
    const schema::ObTableSchema *container_table_schema = nullptr;
    const schema::ObTableSchema *dest_table_schema = nullptr;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
                    .get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("fail to check formal guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(
                   tenant_id_, container_table_id, container_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id),
               K(container_table_id));
    } else if (OB_ISNULL(container_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(container_table_schema),
               K_(tenant_id), K(container_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id,
                                                     dest_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id),
               K(dest_table_id));
    } else if (OB_ISNULL(dest_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(dest_table_schema),
               K_(tenant_id), K(dest_table_id));
    } else {
      const ObString &container_table_name =
          container_table_schema->get_table_name_str();
      ObSqlString query_column_sql_string;
      const uint64_t dest_database_id =
          container_table_schema->get_database_id();
      ObString dest_database_name;
      const schema::ObDatabaseSchema *db_schema = nullptr;
      if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, dest_database_id,
                                                   db_schema))) {
        LOG_WARN("fail to get database schema", K(ret), K_(tenant_id),
                 K(dest_database_id), K(container_table_id));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, database schema must not be nullptr",
                 K(ret));
      } else {
        dest_database_name = db_schema->get_database_name_str();
        bool is_shadow_column = false;
        ObArray<schema::ObColDesc> column_ids;
        ObArray<ObColumnNameInfo> column_names;
        const ObRowkeyInfo &rowkey_info = dest_table_schema->get_rowkey_info();
        const ObRowkeyColumn *rowkey_column = nullptr;
        const schema::ObColumnSchemaV2 *column_schema = nullptr;
        int64_t col_id = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
          if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, rowkey column must not be nullptr",
                     K(ret));
          } else if (FALSE_IT(is_shadow_column = rowkey_column->column_id_ >=
                                                 OB_MIN_SHADOW_COLUMN_ID)) {
          } else if (FALSE_IT(col_id = is_shadow_column
                                           ? rowkey_column->column_id_ -
                                                 OB_MIN_SHADOW_COLUMN_ID
                                           : rowkey_column->column_id_)) {
          } else if (OB_ISNULL(
                         column_schema =
                             dest_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr",
                     K(ret), K(col_id));
          } else if (column_schema->is_generated_column() &&
                     !dest_table_schema->is_spatial_index()) {
            // generated columns cannot be row key.
          } else if (OB_FAIL(column_names.push_back(
                         ObColumnNameInfo(column_schema->get_column_name_str(),
                                          is_shadow_column)))) {
            LOG_WARN("fail to push back rowkey column name", K(ret));
          }
        }
        if (FAILEDx(dest_table_schema->get_column_ids(column_ids))) {
          LOG_WARN("fail to get column ids", K(ret));
        } else {
          const int64_t col_id = column_ids.at(column_ids.count() - 1).col_id_;
          if (OB_ISNULL(column_schema =
                            dest_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr",
                     K(ret));
          } else if (OB_FAIL(column_names.push_back(
                         ObColumnNameInfo(column_schema->get_column_name_str(),
                                          false /* is_shadow_column */)))) {
            LOG_WARN("fail to push back column name", K(ret));
          } else if (OB_FAIL(ObDDLUtil::generate_column_name_str(
                         column_names, false /*oracle_mode*/,
                         true /*with origin name*/, false /*with alias name*/,
                         false /*use_heap_table_ddl_plan*/,
                         query_column_sql_string))) {
            LOG_WARN("fail to generate column name str", K(ret));
          }
        }
      }
      if (FAILEDx(container_string.assign_fmt(
              "INSERT INTO `%.*s`.`%.*s` (%.*s) values",
              static_cast<int>(dest_database_name.length()),
              dest_database_name.ptr(),
              static_cast<int>(container_table_name.length()),
              container_table_name.ptr(),
              static_cast<int>(query_column_sql_string.length()),
              query_column_sql_string.ptr()))) {
        LOG_WARN("fail to assign sql string", K(ret), K(container_string));
      } else {
        ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
        allocator.set_attr(ObMemAttr(tenant_id_, "IvfArena"));
        ObCastCtx cast_ctx(&allocator, NULL, CM_NONE,
                           ObCharset::get_system_collation());
        ObObj obj;
        ObObj cast_obj;
        ObString tmp_str;
        ObString patch_str;
        if (OB_FAIL(get_patch_pkeys_for_center_dummy_pkeys_array(patch_str))) {
          LOG_WARN("fail to get patch pkeys string", K(ret));
        }
        // construct container_string
        for (int64_t i = 0;
             OB_SUCC(ret) && i < center_vectors_[cur_idx_].count(); ++i) {
          obj.reset();
          ObTypeVector *center = center_vectors_[cur_idx_].at(i);
          obj.set_vector(center->ptr(), center->dims());
          if (0 == i) {
            if (OB_FAIL(container_string.append("("))) {
              LOG_WARN("fail to append sql string", K(ret));
            }
          } else if (OB_FAIL(container_string.append(",("))) {
            LOG_WARN("fail to append sql string", K(ret));
          }
          if (FAILEDx(ObObjCaster::to_type(ObVarcharType, cast_ctx, obj,
                                           cast_obj))) {
            LOG_WARN("failed to cast to varchar", K(ret));
          } else if (OB_FAIL(cast_obj.get_string(tmp_str))) {
            LOG_WARN("failed to get string", K(ret), K(cast_obj));
          } else if (!center_dummy_pkeys_.at(i).empty() &&
                     OB_FAIL(container_string.append_fmt(
                         "%ld,%.*s,'%.*s'", i,
                         center_dummy_pkeys_.at(i).length(),
                         center_dummy_pkeys_.at(i).ptr(), tmp_str.length(),
                         tmp_str.ptr()))) {
            LOG_WARN("failed to append sql string", K(ret),
                     K(container_string));
          } else if (center_dummy_pkeys_.at(i).empty() &&
                     OB_FAIL(container_string.append_fmt(
                         "%ld,%.*s,'%.*s'", i, patch_str.length(),
                         patch_str.ptr(), tmp_str.length(), tmp_str.ptr()))) {
            LOG_WARN("failed to append sql string", K(ret),
                     K(container_string));
          } else if (OB_FAIL(container_string.append(")"))) {
            LOG_WARN("fail to append sql string", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          LOG_TRACE("success to construct insert container table sql", K(ret),
                    K(container_string));
        }
      }
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::construct_batch_insert_index_sql_simple(
    common::ObSqlString &index_string, const int64_t dest_table_id) {
  int ret = OB_SUCCESS;
  if (ObIvfflatBuildStatus::FINISH != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else {
    const schema::ObTableSchema *dest_table_schema = nullptr;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
                    .get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("fail to check formal guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id,
                                                     dest_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id),
               K(dest_table_id));
    } else if (OB_ISNULL(dest_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(dest_table_schema),
               K_(tenant_id), K(dest_table_id));
    } else {
      const ObString &dest_table_name = dest_table_schema->get_table_name_str();
      const uint64_t dest_database_id = dest_table_schema->get_database_id();
      ObString dest_database_name;
      ObSqlString query_column_sql_string;
      const schema::ObDatabaseSchema *db_schema = nullptr;
      if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, dest_database_id,
                                                   db_schema))) {
        LOG_WARN("fail to get database schema", K(ret), K_(tenant_id),
                 K(dest_database_id), K(dest_table_id));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, database schema must not be nullptr",
                 K(ret));
      } else {
        dest_database_name = db_schema->get_database_name_str();
        bool is_shadow_column = false;
        ObArray<schema::ObColDesc> column_ids;
        ObArray<ObColumnNameInfo> column_names;
        const ObRowkeyInfo &rowkey_info = dest_table_schema->get_rowkey_info();
        const ObRowkeyColumn *rowkey_column = nullptr;
        const schema::ObColumnSchemaV2 *column_schema = nullptr;
        int64_t col_id = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
          if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, rowkey column must not be nullptr",
                     K(ret));
          } else if (FALSE_IT(is_shadow_column = rowkey_column->column_id_ >=
                                                 OB_MIN_SHADOW_COLUMN_ID)) {
          } else if (FALSE_IT(col_id = is_shadow_column
                                           ? rowkey_column->column_id_ -
                                                 OB_MIN_SHADOW_COLUMN_ID
                                           : rowkey_column->column_id_)) {
          } else if (OB_ISNULL(
                         column_schema =
                             dest_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr",
                     K(ret), K(col_id));
          } else if (column_schema->is_generated_column() &&
                     !dest_table_schema->is_spatial_index()) {
            // generated columns cannot be row key.
          } else if (OB_FAIL(column_names.push_back(
                         ObColumnNameInfo(column_schema->get_column_name_str(),
                                          is_shadow_column)))) {
            LOG_WARN("fail to push back rowkey column name", K(ret));
          }
        }
        if (FAILEDx(dest_table_schema->get_column_ids(column_ids))) {
          LOG_WARN("fail to get column ids", K(ret));
        } else {
          const int64_t col_id = column_ids.at(column_ids.count() - 1)
                                     .col_id_; // get vector center column
          if (OB_ISNULL(column_schema =
                            dest_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr",
                     K(ret));
          } else if (OB_FAIL(column_names.push_back(
                         ObColumnNameInfo(column_schema->get_column_name_str(),
                                          false /* is_shadow_column */)))) {
            LOG_WARN("fail to push back column name", K(ret));
          } else if (OB_FAIL(ObDDLUtil::generate_column_name_str(
                         column_names, false /*oracle_mode*/,
                         true /*with origin name*/, false /*with alias name*/,
                         false /*use_heap_table_ddl_plan*/,
                         query_column_sql_string))) {
            LOG_WARN("fail to generate column name str", K(ret));
          } else if (OB_FAIL(index_string.assign_fmt(
                         "INSERT INTO `%.*s`.`%.*s` (%.*s) values",
                         static_cast<int>(dest_database_name.length()),
                         dest_database_name.ptr(),
                         static_cast<int>(dest_table_name.length()),
                         dest_table_name.ptr(),
                         static_cast<int>(query_column_sql_string.length()),
                         query_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret), K(index_string));
          } else {
            LOG_TRACE("success to prepare insert index table sql", K(ret),
                      K(index_string));
          }
        }
      }
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::init_center_dummy_pkeys_array() {
  int ret = OB_SUCCESS;
  center_dummy_pkeys_.reset();
  int64_t center_cnt = center_vectors_[cur_idx_].count();
  for (int64_t i = 0; OB_SUCC(ret) && i < center_cnt; ++i) {
    if (OB_FAIL(center_dummy_pkeys_.push_back(ObString()))) {
      LOG_WARN("fail to add empty pkey array to center_dummy_pkeys_", K(ret));
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::get_patch_pkeys_for_center_dummy_pkeys_array(
    ObString &patch_str) {
  int ret = OB_SUCCESS;
  int64_t center_cnt = center_vectors_[cur_idx_].count();
  for (int64_t i = 0; i < center_cnt; ++i) {
    if (!center_dummy_pkeys_.at(i).empty()) {
      patch_str = center_dummy_pkeys_.at(i);
      break;
    }
  }
  if (OB_UNLIKELY(patch_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("patch str cannot be empty", K(ret));
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::set_partition_name(common::ObTabletID &tablet_id,
                                                  uint64_t base_table_id) {
  return ObTenantIvfflatCenterCache::set_partition_name(
      tenant_id_, base_table_id, tablet_id, allocator_for_partition_name_,
      partition_name_, partition_idx_);
}

int ObIvfflatIndexBuildHelper::construct_batch_insert_index_sql(
    sqlclient::ObMySQLResult &result, common::ObSqlString &index_string,
    int64_t &row_count, int64_t &idx) {
  int ret = OB_SUCCESS;
  if (ObIvfflatBuildStatus::FINISH != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else {
    row_count = 0;
    ObTypeVector vector;
    int64_t nearest_center_idx = -1;
    double min_distance = DBL_MAX;
    ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
    allocator.set_attr(ObMemAttr(tenant_id_, "IvfArena"));
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE,
                       ObCharset::get_system_collation());
    bool first_row = true;
    ObObjMeta type;
    ObObj obj;
    ObObj cast_obj;
    ObString tmp_str;
    const ObNewRow *row = nullptr;
    // construct index_string
    while (OB_SUCC(ret) && row_count < BATCH_INSERT_SIZE) {
      if (OB_FAIL(result.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(row = result.get_row())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect nullptr row", K(ret));
      } else {
        const int64_t column_cnt = row->count_;
        if (first_row) {
          if (OB_FAIL(index_string.append("("))) {
            LOG_WARN("fail to append sql string", K(ret));
          } else {
            first_row = false;
          }
        } else if (OB_FAIL(index_string.append(",("))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
        if (FAILEDx(result.get_type(column_cnt - 1, type))) {
          LOG_WARN("failed to get type");
        } else if (ObVectorType != type.get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect index column type", K(ret), K(type));
        } else if (OB_FAIL(result.get_vector(column_cnt - 1, vector))) {
          LOG_WARN("get obj failed", K(ret), K(column_cnt));
        } else if (OB_FAIL(get_nearest_center(vector, nearest_center_idx,
                                              min_distance))) {
          LOG_WARN("failed to get nearest center");
          // } else if (elkan_kmeans_ && FALSE_IT(nearest_center_idx =
          // nearest_centers_[idx])) {
          //   LOG_WARN("failed to get nearest center");
        } else if (FAILEDx(
                       index_string.append_fmt("%ld,", nearest_center_idx))) {
          LOG_WARN("failed to append sql string", K(ret), K(index_string));
        }
        ObString &pkey_str = center_dummy_pkeys_.at(nearest_center_idx);
        bool need_to_set_dummy_pkeys = pkey_str.empty();
        char *pkey_str_raw_ptr = nullptr;
        int64_t pkey_str_raw_ptr_pos = 0;
        int64_t pkey_str_raw_ptr_buf_len = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          if (FALSE_IT(obj.reset())) {
          } else if (FALSE_IT(cast_obj.reset())) {
          } else if (OB_FAIL(result.get_obj(i, obj))) {
            LOG_WARN("failed to get obj", K(ret), K(i));
          } else if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, obj,
                                                  cast_obj))) {
            LOG_WARN("failed to cast to varchar", K(ret));
          } else if (OB_FAIL(cast_obj.get_string(tmp_str))) {
            LOG_WARN("failed to get string", K(ret), K(cast_obj));
          } else {
            if (i == column_cnt - 1) { // vector column
              if (OB_FAIL(index_string.append_fmt("'%.*s')", tmp_str.length(),
                                                  tmp_str.ptr()))) {
                LOG_WARN("failed to append sql string", K(ret),
                         K(index_string));
              }
            } else if (OB_FAIL(index_string.append_fmt(
                           obj.is_string_or_lob_locator_type() ? "'%.*s',"
                                                               : "%.*s,",
                           tmp_str.length(), tmp_str.ptr()))) {
              LOG_WARN("failed to append sql string", K(ret), K(index_string));
            }
            if (OB_UNLIKELY(i != column_cnt - 1 && need_to_set_dummy_pkeys)) {
              if (OB_FAIL(databuff_printf(
                      pkey_str_raw_ptr, pkey_str_raw_ptr_buf_len,
                      pkey_str_raw_ptr_pos, arena_allocator_,
                      OB_ISNULL(pkey_str_raw_ptr)
                          ? (obj.is_string_or_lob_locator_type() ? "'%.*s'"
                                                                 : "%.*s")
                          : (obj.is_string_or_lob_locator_type() ? ",'%.*s'"
                                                                 : ",%.*s"),
                      static_cast<int>(tmp_str.length()), tmp_str.ptr()))) {
                LOG_WARN("fail to print data", K(ret), K(tmp_str));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(need_to_set_dummy_pkeys)) {
            pkey_str.assign_ptr(pkey_str_raw_ptr, pkey_str_raw_ptr_pos);
          }
          ++row_count;
          ++idx;
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("success to construct batch insert index table sql", K(ret),
                K(index_string), K(row_count));
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::construct_select_sql_string_simple(
    common::ObSqlString &select_string, const int64_t dest_table_id,
    const int64_t data_table_id) {
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *source_table_schema = nullptr;
  const schema::ObTableSchema *desc_table_schema = nullptr;
  ObSqlString query_column_sql_string;
  bool oracle_mode = false;
  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id ||
                  OB_INVALID_ID == dest_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(tenant_id), K(data_table_id),
             K(dest_table_id));
  } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
                         .get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("fail to check formal guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id,
                                                   source_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K_(tenant_id),
             K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id,
                                                   desc_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K_(tenant_id),
             K(dest_table_id));
  } else if (OB_ISNULL(source_table_schema) || OB_ISNULL(desc_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), KP(source_table_schema),
             KP(desc_table_schema), K_(tenant_id), K(data_table_id),
             K(dest_table_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                 tenant_id_, data_table_id, oracle_mode))) {
    LOG_WARN("check if oracle mode failed", K(ret), K(data_table_id));
  } else {
    // get table_name and database_name
    const ObString &source_table_name =
        source_table_schema->get_table_name_str();
    const uint64_t source_database_id = source_table_schema->get_database_id();
    ObString source_database_name;
    const schema::ObDatabaseSchema *db_schema = nullptr;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, source_database_id,
                                                 db_schema))) {
      LOG_WARN("fail to get database schema", K(ret), K_(tenant_id),
               K(source_database_id), K(data_table_id));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
    } else {
      source_database_name = db_schema->get_database_name_str();
    }
    // fill select columns
    bool is_shadow_column = false;
    ObArray<schema::ObColDesc> column_ids;
    ObArray<ObColumnNameInfo> column_names;
    const ObRowkeyInfo &rowkey_info = source_table_schema->get_rowkey_info();
    const ObRowkeyColumn *rowkey_column = nullptr;
    const schema::ObColumnSchemaV2 *first_rowkey_schema = nullptr;
    const schema::ObColumnSchemaV2 *column_schema = nullptr;
    int64_t col_id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, rowkey column must not be nullptr", K(ret));
      } else if (FALSE_IT(is_shadow_column = rowkey_column->column_id_ >=
                                             OB_MIN_SHADOW_COLUMN_ID)) {
      } else if (FALSE_IT(col_id = is_shadow_column
                                       ? rowkey_column->column_id_ -
                                             OB_MIN_SHADOW_COLUMN_ID
                                       : rowkey_column->column_id_)) {
      } else if (OB_ISNULL(
                     column_schema =
                         source_table_schema->get_column_schema(col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not be nullptr", K(ret),
                 K(col_id));
      } else if (column_schema->is_generated_column() &&
                 !source_table_schema->is_spatial_index()) {
        // generated columns cannot be row key.
      } else if (OB_FAIL(column_names.push_back(
                     ObColumnNameInfo(column_schema->get_column_name_str(),
                                      is_shadow_column)))) {
        LOG_WARN("fail to push back rowkey column name", K(ret));
      } else if (i == 0) {
        first_rowkey_schema = column_schema;
      }
    }
    if (FAILEDx(desc_table_schema->get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      const int64_t col_id = column_ids.at(column_ids.count() - 1)
                                 .col_id_; // get vector center column
      if (OB_ISNULL(column_schema =
                        desc_table_schema->get_column_schema(col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
      } else if (OB_FAIL(column_names.push_back(
                     ObColumnNameInfo(column_schema->get_column_name_str(),
                                      false /* is_shadow_column */)))) {
        LOG_WARN("fail to push back column name", K(ret));
      } else if (OB_FAIL(ObDDLUtil::generate_column_name_str(
                     column_names, oracle_mode, true /*with origin name*/,
                     true /*with alias name*/,
                     false /*use_heap_table_ddl_plan*/,
                     query_column_sql_string))) {
        LOG_WARN("fail to generate column name str", K(ret));
      } else if (partition_name_.empty() &&
                 OB_FAIL(select_string.assign_fmt(
                     "SELECT %.*s from `%.*s`.`%.*s`",
                     static_cast<int>(query_column_sql_string.length()),
                     query_column_sql_string.ptr(),
                     static_cast<int>(source_database_name.length()),
                     source_database_name.ptr(),
                     static_cast<int>(source_table_name.length()),
                     source_table_name.ptr()))) {
        LOG_WARN("fail to assign select sql string", K(ret));
      } else if (!partition_name_.empty() &&
                 OB_FAIL(select_string.assign_fmt(
                     "SELECT %.*s from `%.*s`.`%.*s` partition(%.*s)",
                     static_cast<int>(query_column_sql_string.length()),
                     query_column_sql_string.ptr(),
                     static_cast<int>(source_database_name.length()),
                     source_database_name.ptr(),
                     static_cast<int>(source_table_name.length()),
                     source_table_name.ptr(),
                     static_cast<int>(partition_name_.length()),
                     partition_name_.ptr()))) {
        LOG_WARN("fail to assign select sql string", K(ret));
      } else if (elkan_kmeans_ &&
                 OB_FAIL(select_string.append_fmt(
                     " order by %.*s",
                     static_cast<int>(
                         first_rowkey_schema->get_column_name_str().length()),
                     first_rowkey_schema->get_column_name_str().ptr()))) {
        LOG_WARN("fail to append select sql string", K(ret));
      } else if (OB_FAIL(select_sql_str_.assign(select_string))) {
        LOG_WARN("fail to assign string", K(ret), K(select_string));
      } else {
        LOG_TRACE("success to construct select sql from data table", K(ret),
                  K(select_string));
      }
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::set_sample_cache(
    ObIvfflatFixSampleCache *cache) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache) || !cache->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cache));
  } else {
    cache_ = cache;
    total_cnt_ = cache_->get_sample_cnt();
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::set_center_cache(const int64_t table_id) {
  int ret = OB_SUCCESS;
  if (ObIvfflatBuildStatus::FINISH != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_FAIL(MTL(ObTenantIvfflatCenterCache *)
                      ->put(table_id, partition_idx_, distance_type_,
                            center_vectors_[cur_idx_]))) {
        LOG_WARN("failed to put int cache", K(ret));
      }
    }
  }
  return ret;
}

// TODO(@jingshui) : need to opt! select * from data_table too many times now.
int ObIvfflatIndexBuildHelper::build() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cache", K(ret));
  } else if (OB_FAIL(cache_->read())) {
    LOG_WARN("failed to read from cache", K(ret));
  } else {
    switch (status_) {
    case ObIvfflatBuildStatus::PREPARE_CENTERS: {
      lists_ = init_lists_; // reinit
      if (elkan_kmeans_ && OB_FAIL(init_first_center_elkan())) {
        LOG_WARN("failed to init first center", K(ret));
      } else if (!elkan_kmeans_ && OB_FAIL(init_first_center())) {
        LOG_WARN("failed to init first center", K(ret));
      }
      break;
    }
    case ObIvfflatBuildStatus::INIT_CENTERS: {
      if (elkan_kmeans_ && OB_FAIL(init_centers_elkan())) {
        LOG_WARN("failed to init centers", K(ret));
      } else if (!elkan_kmeans_ && OB_FAIL(init_centers())) {
        LOG_WARN("failed to init centers", K(ret));
      }
      break;
    }
    case ObIvfflatBuildStatus::RUNNING_KMEANS: {
      if (elkan_kmeans_ && OB_FAIL(ivfflat_elkan_kmeans())) {
        LOG_WARN("failed to do ivfflat kmeans", K(ret));
      } else if (!elkan_kmeans_ && OB_FAIL(ivfflat_kmeans())) {
        LOG_WARN("failed to do ivfflat kmeans", K(ret));
      }
      break;
    }
    case ObIvfflatBuildStatus::FINISH: {
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K_(status));
      break;
    }
    }
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ++failed_times_;
      if (MAX_RETRY_CNT >= failed_times_) {
        ret = OB_NEED_RETRY; // change the error code to retry
      }
    }
    // tmp code // reset
    if (OB_ALLOCATE_MEMORY_FAILED == ret && elkan_kmeans_) {
      destroy();
      elkan_kmeans_ = false;
      status_ = PREPARE_CENTERS;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::init_first_center() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double max_distance = 0;
  double min_distance = DBL_MAX;
  if (ObIvfflatBuildStatus::PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (0 == total_cnt_) {
    status_ = FINISH; // do nothing
  } else {
    LOG_INFO("start to init_first_center", K(ret), KPC(this));
    // scan cnt 1
    if (OB_FAIL(cache_->get_random_vector(vector))) {
      LOG_WARN("failed to get random vector", K(ret));
    } else {
      ObTypeVector *first_center = nullptr;
      if (OB_FAIL(alloc_and_copy_vector(vector, first_center))) {
        LOG_WARN("failed to alloc and copy vector", K(ret));
      } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(first_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      reuse_array(center_vectors_[cur_idx_]);
    } else {
      if (lists_ < center_vectors_[cur_idx_].count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected center counts is larger than lists", K(ret),
                 "center_count", center_vectors_[cur_idx_].count(), K_(lists));
      } else if (lists_ == center_vectors_[cur_idx_].count()) {
        status_ = RUNNING_KMEANS; // skip INIT_CENTERS
      } else {
        status_ = INIT_CENTERS;
      }
      LOG_INFO("success to init_first_center", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::quick_init_centers() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  ObTypeVector *center = nullptr;
  int64_t idx = 0;
  int64_t vector_size = 0;
  while (OB_SUCC(ret) && lists_ > center_vectors_[cur_idx_].count()) {
    if (OB_FAIL(cache_->get_next_vector(vector))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", K(ret));
      }
      break;
    } else {
      vector_size = vector.dims();
      for (idx = 0; idx < center_vectors_[cur_idx_].count(); ++idx) {
        if (0 == center_vectors_[cur_idx_].at(idx)->vector_cmp(vector)) {
          break;
        }
      }
      if (idx == center_vectors_[cur_idx_]
                     .count()) { // vector is not exist in center array
        center = nullptr;
        if (OB_FAIL(alloc_and_copy_vector(vector, center))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(center))) {
          LOG_WARN("failed to push back into array", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    reuse_array(center_vectors_[cur_idx_]);
    destory_vector(center);
    status_ = PREPARE_CENTERS; // retry from PREPARE_CENTERS // TODO(@jingshui)
  } else {
    status_ = FINISH; // don't need to kmeans
    LOG_INFO("success to quick_init_centers", K(ret), KPC(this));
  }
  return ret;
}

// TODO(@jingshui): sample counts may be less than center counts, we can select
// count(*) first to opt
int ObIvfflatIndexBuildHelper::init_centers() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double max_distance = 0;
  double min_distance = DBL_MAX;
  ObTypeVector *next_center = nullptr;
  int64_t nearest_center_idx = -1;
  if (ObIvfflatBuildStatus::INIT_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (total_cnt_ <= lists_) {
    lists_ = total_cnt_;
    if (OB_FAIL(quick_init_centers())) {
      LOG_WARN("failed to quick init centers", K(ret));
    }
  } else {
    LOG_INFO("start to init_centers", K(ret), KPC(this));
    // scan cnt lists-2
    while (OB_SUCC(ret)) {
      if (OB_FAIL(cache_->get_next_vector(vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
        break;
      } else if (OB_FAIL(get_nearest_center(vector, nearest_center_idx,
                                            min_distance))) {
        LOG_WARN("failed to get nearest center", K(ret), K(vector));
      } else if (min_distance > max_distance) {
        max_distance = min_distance;
        if (OB_NOT_NULL(next_center)) {
          if (OB_FAIL(next_center->deep_copy(vector))) {
            LOG_WARN("failed to deep copy", K(ret), K(vector));
          }
        } else if (OB_FAIL(alloc_and_copy_vector(vector, next_center))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(next_center)) {
      if (OB_FAIL(center_vectors_[cur_idx_].push_back(next_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      destory_vector(next_center);
      // stay in INIT_CENTERS
    }
  }
  if (OB_SUCC(ret)) {
    if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               KPC(this));
    } else if (lists_ == center_vectors_[cur_idx_].count()) {
      status_ = RUNNING_KMEANS;
      LOG_INFO("success to init_centers", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::ivfflat_kmeans() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  int64_t nearest_center_idx = -1;
  double min_distance = DBL_MAX;
  double distance = DBL_MAX;
  const int64_t next_idx = get_next_idx();
  if (ObIvfflatBuildStatus::RUNNING_KMEANS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (lists_ != center_vectors_[cur_idx_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K_(lists), "count",
             center_vectors_[cur_idx_].count());
  } else if (max_iterate_times_ == iterate_times_) {
    status_ = FINISH;
  } else {
    ++iterate_times_;
    // 1. init next_idx array
    ObTypeVector *new_vector = nullptr;
    const int64_t vector_length = center_vectors_[cur_idx_].at(0)->dims();
    while (OB_SUCC(ret) && lists_ > center_vectors_[next_idx].count()) {
      if (OB_FAIL(alloc_vector(new_vector, vector_length))) {
        LOG_WARN("failed to alloc vector", K(ret));
      } else if (OB_FAIL(center_vectors_[next_idx].push_back(new_vector))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      destory_vector(new_vector);
      // stay in RUNNING_KMEANS
    } else {
      // 2. update centers
      bool is_finish = true;
      common::ObArray<int64_t> data_cnt_array;
      data_cnt_array.set_attr(ObMemAttr(tenant_id_, "DataCnts"));
      if (FAILEDx(data_cnt_array.reserve(lists_))) {
        LOG_WARN("failed to reserve array", K(ret), K_(lists));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
          if (OB_FAIL(data_cnt_array.push_back(0))) { // init
            LOG_WARN("failed to push back into array", K(ret));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(cache_->get_next_vector(vector))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("get next result failed", K(ret));
            }
            break;
          } else if (OB_FAIL(get_nearest_center(vector, nearest_center_idx,
                                                min_distance))) {
            LOG_WARN("failed to get nearest center", K(ret), K(vector));
          } else if (OB_FAIL(center_vectors_[next_idx]
                                 .at(nearest_center_idx)
                                 ->add(vector))) {
            LOG_WARN("failed to add vector", K(ret));
          } else if (FALSE_IT(++data_cnt_array.at(nearest_center_idx))) {
          }
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
            if (data_cnt_array.at(i) > 0) {
              if (OB_FAIL(center_vectors_[next_idx].at(i)->divide(
                      data_cnt_array.at(i)))) {
                LOG_WARN("failed to divide vector", K(ret), "data_count",
                         data_cnt_array.at(i));
              } else if (L2 == distance_type_ &&
                         OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_l2_square(
                             *center_vectors_[next_idx].at(i), distance))) {
                LOG_WARN("failed to cal distance", K(ret), K_(distance_type));
              } else if (L2 != distance_type_ &&
                         OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_distance(
                             distance_type_, *center_vectors_[next_idx].at(i),
                             distance))) {
                LOG_WARN("failed to cal distance", K(ret), K_(distance_type));
              } else if (L2 == distance_type_) {
                if (L2_SQUARE_ERROR_THRESHOLD < distance) {
                  is_finish = false;
                }
              } else if (L2_DISTANCE_ERROR_THRESHOLD < distance) {
                is_finish = false;
              }
            } else if (OB_FAIL(center_vectors_[next_idx].at(i)->deep_copy(
                           *center_vectors_[cur_idx_].at(i)))) {
              LOG_WARN("fail to copy vector", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        switch_cur_idx();
        status_ = is_finish ? FINISH : status_;
      }
      // clear swap array
      for (int64_t i = 0; i < lists_; ++i) {
        center_vectors_[get_next_idx()].at(i)->clear_vals();
      }
    }
    LOG_INFO("success to ivfflat kmeans", K(ret), KPC(this));
  }
  return ret;
}

//// elkan kmeans
int ObIvfflatIndexBuildHelper::get_vector_by_sql(const int64_t offset,
                                                 ObTypeVector *&next_vector) {
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.assign(select_sql_str_))) {
    LOG_WARN("fail to assign sql string", K(ret), K_(select_sql_str));
  } else if (OB_FAIL(select_sql.append_fmt(" limit %ld,1", offset))) {
    LOG_WARN("fail to append sql string", K(ret), K_(select_sql_str),
             K(offset));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      sqlclient::ObMySQLResult *res = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(
              result, tenant_id_, select_sql.ptr()))) { // open another connect
        LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(select_sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(select_sql));
      } else {
        ObMysqlResultIterator iter(*res);
        ObTypeVector vector;
        if (OB_FAIL(iter.get_next_vector(vector))) {
          LOG_WARN("get next result failed", K(ret));
        } else if (OB_FAIL(alloc_and_copy_vector(vector, next_vector))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::init_first_center_elkan() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  if (ObIvfflatBuildStatus::PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (0 == total_cnt_) {
    status_ = FINISH; // do nothing
  } else {
    LOG_INFO("start to init_first_center", K(ret), KPC(this));
    // scan cnt 1
    if (OB_FAIL(cache_->get_random_vector(vector))) {
      LOG_WARN("failed to get random vector", K(ret));
    } else {
      ObTypeVector *first_center = nullptr;
      if (OB_FAIL(alloc_and_copy_vector(vector, first_center))) {
        LOG_WARN("failed to alloc and copy vector", K(ret));
      } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(first_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      reuse_array(center_vectors_[cur_idx_]);
    } else if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               "center_count", center_vectors_[cur_idx_].count(), K_(lists));
    } else if (lists_ == center_vectors_[cur_idx_].count()) {
      status_ = FINISH; // skip INIT_CENTERS and RUNNING_KMEANS
    } else if (lists_ >= total_cnt_) {
      status_ = INIT_CENTERS; // quick_init_centers
    } else {
      if (OB_ISNULL(nearest_centers_ = static_cast<int32_t *>(
                        allocator_.alloc(sizeof(int32_t) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(lower_bounds_ =
                               static_cast<float *>(allocator_.alloc(
                                   sizeof(float) * total_cnt_ * lists_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(upper_bounds_ = static_cast<float *>(
                               allocator_.alloc(sizeof(float) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(
                     weight_ = static_cast<float *>(
                         arena_allocator_.alloc(sizeof(float) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else {
        status_ = INIT_CENTERS;
        MEMSET(nearest_centers_, 0, sizeof(int32_t) * total_cnt_);
        MEMSET(lower_bounds_, 0, sizeof(float) * total_cnt_ * lists_);
        MEMSET(upper_bounds_, 0, sizeof(float) * total_cnt_);
        for (int64_t i = 0; i < total_cnt_; ++i) {
          weight_[i] = FLT_MAX;
        }
      }
    }

    if (OB_FAIL(ret)) {
      reuse_array(center_vectors_[cur_idx_]);
    } else if (OB_SUCC(ret)) {
      LOG_INFO("success to init_first_center", K(ret), KPC(this));
    }
  }
  return ret;
}

// kmeans++
int ObIvfflatIndexBuildHelper::init_centers_elkan() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double distance = 0;
  ObTypeVector *next_center = nullptr;
  bool is_finish = false;
  if (ObIvfflatBuildStatus::INIT_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (total_cnt_ <= lists_) {
    lists_ = total_cnt_;
    if (OB_FAIL(quick_init_centers())) {
      LOG_WARN("failed to quick init centers", K(ret));
    }
  } else {
    LOG_INFO("start to init_centers", K(ret), KPC(this));
    int64_t center_idx = center_vectors_[cur_idx_].count() - 1;
    is_finish = lists_ == center_vectors_[cur_idx_].count();
    int64_t idx = 0;
    double sum = 0;
    double random_weight = 0;
    ObTypeVector *cur_center = center_vectors_[cur_idx_].at(center_idx);

    // cal every sample weight
    while (OB_SUCC(ret)) {
      if (OB_FAIL(cache_->get_next_vector(vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
        break;
      } else {
        if (OB_FAIL(vector.cal_kmeans_distance(distance_type_, *cur_center,
                                               distance))) {
          LOG_WARN("failed to cal distance", K(ret), K_(distance_type));
        } else {
          lower_bounds_[idx * lists_ + center_idx] = distance;
          if (!is_finish) { // is_finish means no need to add new center, only
                            // update lower_bounds_
            distance *= distance;
            if (distance < weight_[idx]) {
              weight_[idx] = distance;
            }
            sum += weight_[idx];
          }
        }
      }
      ++idx;
    }
    // get the next center randomly
    if (OB_SUCC(ret) && !is_finish) {
      if (idx != total_cnt_) {
        // ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data count has changed during building index", K(idx),
                 K_(total_cnt));
      }
      random_weight = (double)ObRandom::rand(1, 100) / 100.0 * sum;
      for (idx = 0; idx < total_cnt_; ++idx) {
        if ((random_weight -= weight_[idx]) <= 0.0) {
          break;
        }
      }

      if (idx >= total_cnt_) {
        idx = total_cnt_ - 1 < 0 ? 0 : total_cnt_ - 1;
      }
      if (OB_FAIL(get_vector_by_sql(idx, next_center))) {
        LOG_WARN("fail to get vector by sql", K(ret), K(idx), K_(total_cnt));
      } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(next_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      destory_vector(next_center);
      // stay in INIT_CENTERS
    }
  }
  if (OB_SUCC(ret)) {
    if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               KPC(this));
    } else if (is_finish) {
      for (int64_t i = 0; i < total_cnt_; ++i) {
        double min_distance = DBL_MAX;
        int64_t nearest_center_idx = 0;
        for (int64_t j = 0; j < lists_; ++j) {
          double distance = lower_bounds_[i * lists_ + j];
          if (distance < min_distance) {
            min_distance = distance;
            nearest_center_idx = j;
          }
        }
        upper_bounds_[i] = min_distance; // 离初始最近中心的距离为初始上界
        nearest_centers_[i] = nearest_center_idx;
      }

      if (OB_NOT_NULL(weight_)) {
        arena_allocator_.free(weight_);
        weight_ = nullptr;
      }
      arena_allocator_.reset();

      status_ = RUNNING_KMEANS;
      LOG_INFO("finish init_centers", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::ivfflat_elkan_kmeans() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  int64_t nearest_center_idx = -1;
  double min_distance = DBL_MAX;
  double distance = DBL_MAX;
  const int64_t next_idx = get_next_idx();
  if (ObIvfflatBuildStatus::RUNNING_KMEANS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (lists_ != center_vectors_[cur_idx_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K_(lists), "count",
             center_vectors_[cur_idx_].count());
  } else if (max_iterate_times_ == iterate_times_) {
    status_ = FINISH;
  } else {
    // init tmp structs
    float *half_centers_distance =
        nullptr; // 每两个中心之间距离的一半, 因为2D(x, c1) <= D(c1, c2) 等价于
                 // D(x,c1) <= 0.5 * D(c1,c2)
    float *half_center_min_distance = nullptr; // 每个中心离其他中心的最短距离
    int32_t *data_cnt_in_center = nullptr; // 每个中心包含数据点的数量
    float *center_distance_diff = nullptr; // 更新center前后两个center的距离
    if (OB_ISNULL(
            half_centers_distance = static_cast<float *>(
                arena_allocator_.alloc(sizeof(float) * lists_ * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(half_center_min_distance = static_cast<float *>(
                             arena_allocator_.alloc(sizeof(float) * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(
                   data_cnt_in_center = static_cast<int32_t *>(
                       arena_allocator_.alloc(sizeof(int32_t) * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      MEMSET(half_centers_distance, 0, sizeof(float) * lists_ * lists_);
      MEMSET(half_center_min_distance, 0, sizeof(float) * lists_);
      MEMSET(data_cnt_in_center, 0, sizeof(int32_t) * lists_);
    }

    // 1. cal distance between centers
    for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < lists_; ++j) {
        if (OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_kmeans_distance(
                distance_type_, *center_vectors_[cur_idx_].at(j), distance))) {
          LOG_WARN("fail to cal distance", K(ret), K(i), K(j));
        } else {
          distance = 0.5 * distance;
          half_centers_distance[i * lists_ + j] = distance;
          half_centers_distance[j * lists_ + i] = distance;
        }
      }
    }
    // 2. cal the nearest distance between the center and other centers
    for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
      half_center_min_distance[i] = DBL_MAX;
      for (int64_t j = 0; OB_SUCC(ret) && j < lists_; ++j) {
        if (i == j) {
          // do nothing
        } else {
          distance = half_centers_distance[i * lists_ + j];
          if (distance < half_center_min_distance[i]) {
            half_center_min_distance[i] = distance;
          }
        }
      }
    }
    // 3. init next_idx array // only once
    ObTypeVector *new_vector = nullptr;
    const int64_t vector_length = center_vectors_[cur_idx_].at(0)->dims();
    while (OB_SUCC(ret) && lists_ > center_vectors_[next_idx].count()) {
      if (OB_FAIL(alloc_vector(new_vector, vector_length))) {
        LOG_WARN("failed to alloc vector", K(ret));
      } else if (OB_FAIL(center_vectors_[next_idx].push_back(new_vector))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    // 4. cal to update centers
    if (OB_SUCC(ret)) {
      bool is_finish = true;
      int64_t idx = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(cache_->get_next_vector(vector))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get next result failed", K(ret));
          }
          break;
        } else if (upper_bounds_[idx] <=
                   half_center_min_distance[nearest_centers_[idx]]) {
          // do nothing
          LOG_TRACE("upper_bound <= half center mini distance", K(idx),
                    K(upper_bounds_[idx]),
                    K(half_center_min_distance[nearest_centers_[idx]]));
          // D(x,c1) <= 0.5 * D(c1,c2) , c2是离c1最近的中心, 此时D(x, c1) <=
          // D(x, c2), 上界已经最小了, 不会有距离更近的中心
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
            if (i == nearest_centers_[idx]) {
              // do nothing
            } else if (upper_bounds_[idx] <= lower_bounds_[idx * lists_ + i]) {
              // do nothing
              // ub <= lb, skip the center
              LOG_TRACE("upper_bound <= lower_bound", K(idx),
                        K(upper_bounds_[idx]),
                        K(lower_bounds_[idx * lists_ + i]));
            } else if (upper_bounds_[idx] <=
                       half_centers_distance[nearest_centers_[idx] * lists_ +
                                             i]) {
              // do nothing
              LOG_TRACE(
                  "upper_bound <= half centers distance", K(idx),
                  K(upper_bounds_[idx]),
                  K(half_centers_distance[nearest_centers_[idx] * lists_ + i]));
            } else {
              if (0 == iterate_times_) {
                min_distance = upper_bounds_[idx];
              } else if (OB_FAIL(vector.cal_kmeans_distance(
                             distance_type_,
                             *center_vectors_[cur_idx_].at(
                                 nearest_centers_[idx]),
                             min_distance))) {
                LOG_WARN("fail to cal distance", K(ret));
              }
              if (OB_SUCC(ret)) {
                LOG_TRACE(
                    "try to update min distance", K(idx), K(min_distance),
                    K(lower_bounds_[idx * lists_ + i]),
                    K(half_centers_distance[nearest_centers_[idx] * lists_ +
                                            i]));
                if (min_distance > lower_bounds_[idx * lists_ + i] ||
                    min_distance >
                        half_centers_distance[nearest_centers_[idx] * lists_ +
                                              i]) {
                  if (OB_FAIL(vector.cal_kmeans_distance(
                          distance_type_, *center_vectors_[cur_idx_].at(i),
                          distance))) {
                    LOG_WARN("fail to cal distance", K(ret));
                  } else {
                    lower_bounds_[idx * lists_ + i] = distance;
                    if (distance < min_distance) {
                      nearest_centers_[idx] = i;
                      upper_bounds_[idx] = distance;
                      is_finish = false;
                    }
                  }
                }
              }
            }
          }
        }
        // sum vector store in center_vectors_[next_idx]
        if (OB_SUCC(ret)) {
          if (OB_FAIL(center_vectors_[next_idx]
                          .at(nearest_centers_[idx])
                          ->add(vector))) {
            LOG_WARN("fail to add vector", K(ret));
          } else {
            ++data_cnt_in_center[nearest_centers_[idx]];
          }
        }
        ++idx;
      }
      // 5. cal the new centers
      if (OB_ISNULL(center_distance_diff = static_cast<float *>(
                        arena_allocator_.alloc(sizeof(float) * lists_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        MEMSET(center_distance_diff, 0, sizeof(float) * lists_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
        if (data_cnt_in_center[i] > 0) {
          if (OB_FAIL(center_vectors_[next_idx].at(i)->divide(
                  data_cnt_in_center[i]))) {
            LOG_WARN("failed to divide vector", K(ret), "data_count",
                     data_cnt_in_center[i]);
          } else if (OB_FAIL(
                         center_vectors_[next_idx].at(i)->cal_kmeans_distance(
                             distance_type_, *center_vectors_[cur_idx_].at(i),
                             distance))) {
            LOG_WARN("failed to cal distance", K(ret));
          } else {
            center_distance_diff[i] = distance;
          }
        } else if (OB_FAIL(center_vectors_[next_idx].at(i)->deep_copy(
                       *center_vectors_[cur_idx_].at(
                           i)))) { // TODO(@jingshui): use random vector?
          LOG_WARN("fail to copy vector", K(ret));
        }
      }
      // 6. adjust ub & lb
      for (int64_t i = 0; i < total_cnt_; ++i) {
        upper_bounds_[i] += center_distance_diff[nearest_centers_[i]];
        for (int64_t j = 0; j < lists_; ++j) {
          distance = lower_bounds_[i * lists_ + j] - center_distance_diff[j];
          distance = distance < 0 ? 0 : distance;
          lower_bounds_[i * lists_ + j] = distance;
        }
      }
      // 7. switch center array
      if (OB_SUCC(ret)) {
        is_finish = is_finish && iterate_times_ != 0;
        ++iterate_times_;
        switch_cur_idx();
        status_ = is_finish ? FINISH : status_;
        LOG_INFO("success to ivfflat kmeans", K(ret), KPC(this));
      }
      // 8. clear tmp swap array
      for (int64_t i = 0; i < lists_; ++i) {
        center_vectors_[get_next_idx()].at(i)->clear_vals();
      }
    }
    // free memory
    if (OB_NOT_NULL(half_centers_distance)) {
      arena_allocator_.free(half_centers_distance);
      half_centers_distance = nullptr;
    }
    if (OB_NOT_NULL(half_center_min_distance)) {
      arena_allocator_.free(half_center_min_distance);
      half_center_min_distance = nullptr;
    }
    if (OB_NOT_NULL(data_cnt_in_center)) {
      arena_allocator_.free(data_cnt_in_center);
      data_cnt_in_center = nullptr;
    }
    if (OB_NOT_NULL(center_distance_diff)) {
      arena_allocator_.free(center_distance_diff);
      center_distance_diff = nullptr;
    }
    arena_allocator_.reset();
    if (OB_FAIL(ret)) {
      destory_vector(new_vector);
      // stay in RUNNING_KMEANS
    }
  }
  return ret;
}

int ObIvfflatIndexBuildHelper::get_nearest_center(const ObTypeVector &vector,
                                                  int64_t &nearest_center_idx,
                                                  double &min_distance) {
  int ret = OB_SUCCESS;
  nearest_center_idx = -1;
  int64_t center_count = center_vectors_[cur_idx_].count();
  double distance = 0;
  min_distance = DBL_MAX;
  ObVectorDistanceType distance_type =
      distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
  for (int64_t i = 0; OB_SUCC(ret) && i < center_count; ++i) {
    ObTypeVector *cur_center = center_vectors_[cur_idx_].at(i);
    if (L2 == distance_type &&
        OB_FAIL(cur_center->cal_l2_square(vector, distance))) {
      LOG_WARN("failed to cal distance", K(ret), K(distance_type),
               K(cur_center), K(vector));
    } else if (L2 != distance_type && OB_FAIL(cur_center->cal_distance(
                                          distance_type, vector, distance))) {
      LOG_WARN("failed to cal distance", K(ret), K(distance_type),
               K(cur_center), K(vector));
    } else if (distance < min_distance) {
      min_distance = distance;
      nearest_center_idx = i;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase