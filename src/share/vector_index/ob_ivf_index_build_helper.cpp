/**
 * Copyright (c) 2024 OceanBase
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
#include "lib/oblog/ob_log_module.h"
#include "share/vector_index/ob_ivf_index_build_helper.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"
#include "share/ob_ddl_common.h"
#include "share/object/ob_obj_cast.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_param.h"
#include "share/vector_index/ob_tenant_ivf_center_cache.h"

namespace oceanbase {
namespace share {
/*
 * ObIvfIndexBuildHelper Impl
 */
int ObIvfIndexBuildHelper::init(const int64_t tenant_id,
                                    const int64_t lists,
                                    const ObVectorDistanceType distance_type) {
  int ret = OB_SUCCESS;
  if (0 >= lists ||
      ObVectorDistanceType::INVALID_DISTANCE_TYPE == distance_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lists), K(distance_type));
  } else if (OB_FAIL(allocator_.init(
                 lib::ObMallocAllocator::get_instance(), PAGE_SIZE,
                 lib::ObMemAttr(tenant_id, build_label_)))) { // TODO(@jingshui) limit
                                                             // the max memory size
    LOG_WARN("failed to init fifo fallocator");
  } else {
    tenant_id_ = tenant_id;
    
    center_helper_.set_label(basic_label_);
  }
  return ret;
}

void ObIvfIndexBuildHelper::reuse() {
  center_helper_.reuse();
  // allocator_.reset();
}

void ObIvfIndexBuildHelper::destroy() {
  center_helper_.destroy();
  allocator_.reset();
  // LOG_TRACE("ObIvfIndexBuildHelper destroy finish");
}

int64_t ObIvfIndexBuildHelper::to_string(char *buf,
                                             const int64_t buf_len) const {
  return center_helper_.to_string(buf, buf_len);
}

int ObIvfIndexBuildHelper::construct_batch_insert_container_sql_simple(
    common::ObSqlString &container_string, const int64_t dest_table_id,
    const int64_t container_table_id, bool need_seg_idx) {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
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
      if (need_seg_idx && OB_FAIL(column_names.push_back(
                                  ObColumnNameInfo("seg_idx",
                                                  is_shadow_column)))) {
        LOG_WARN("fail to push back seg_idx column name", K(ret));
      }
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
    }
  }
  return ret;
}

int ObIvfIndexBuildHelper::construct_batch_insert_container_sql(
    common::ObSqlString &container_string, const int64_t dest_table_id,
    const int64_t container_table_id) {
  int ret = OB_SUCCESS;
  if (ObIvfKMeansStatus::FINISH != center_helper_.get_status()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(center_helper_.get_status()));
  } else if (OB_FAIL(construct_batch_insert_container_sql_simple(
          container_string, dest_table_id, container_table_id))) {
    LOG_WARN("fail to construct batch insert container sql simple", K(ret));
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
          OB_SUCC(ret) && i < center_helper_.get_center_vectors().count(); ++i) {
      obj.reset();
      ObTypeVector *center = center_helper_.get_center_vectors().at(i);
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
  LOG_TRACE("construct_batch_insert_container_sql", K(ret), K(container_string), K(dest_table_id), K(container_table_id));
  return ret;
}

int ObIvfIndexBuildHelper::construct_batch_insert_index_sql_simple(
    common::ObSqlString &index_string, const int64_t dest_table_id) {
  int ret = OB_SUCCESS;
  ObIvfKMeansStatus status = center_helper_.get_status();
  if (ObIvfKMeansStatus::FINISH != status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(center_helper_.get_status()));
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
  LOG_TRACE("construct_batch_insert_index_sql_simple", K(ret), K(index_string), K(dest_table_id));
  return ret;
}

int ObIvfIndexBuildHelper::init_center_dummy_pkeys_array() {
  int ret = OB_SUCCESS;
  center_dummy_pkeys_.reset();
  int64_t center_cnt = center_helper_.get_center_vectors().count();
  for (int64_t i = 0; OB_SUCC(ret) && i < center_cnt; ++i) {
    if (OB_FAIL(center_dummy_pkeys_.push_back(ObString()))) {
      LOG_WARN("fail to add empty pkey array to center_dummy_pkeys_", K(ret));
    }
  }
  return ret;
}

int ObIvfIndexBuildHelper::get_patch_pkeys_for_center_dummy_pkeys_array(
    ObString &patch_str) {
  int ret = OB_SUCCESS;
  int64_t center_cnt = center_helper_.get_center_vectors().count();
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

int ObIvfIndexBuildHelper::set_partition_name(common::ObTabletID &tablet_id,
                                                  uint64_t base_table_id) {
  return ObTenantIvfCenterCache::set_partition_name(
      tenant_id_, base_table_id, tablet_id, allocator_for_partition_name_,
      partition_name_, partition_idx_);
}

int ObIvfIndexBuildHelper::pre_fill_data_insert_index_sql(
    sqlclient::ObMySQLResult &result, ObTypeVector &vector, common::ObSqlString &index_string,
    int64_t &column_cnt, double &min_distance, int64_t &nearest_center_idx, bool &first_row) {
  int ret = OB_SUCCESS;
  ObObjMeta type;
  const ObNewRow *row = nullptr;
  if (OB_FAIL(result.next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next result failed", K(ret));
    }
  } else if (OB_ISNULL(row = result.get_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect nullptr row", K(ret));
  } else {
    column_cnt = row->count_;
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
    } else if (OB_FAIL(center_helper_.get_nearest_center(vector, nearest_center_idx,
                                          min_distance))) {
      LOG_WARN("failed to get nearest center");
    } else if (FAILEDx(
                    index_string.append_fmt("%ld,", nearest_center_idx))) {
      LOG_WARN("failed to append sql string", K(ret), K(index_string));
    }
  }
  return ret;
}

int ObIvfIndexBuildHelper::post_fill_data_insert_index_sql(
    ObCastCtx &cast_ctx, sqlclient::ObMySQLResult &result, common::ObSqlString &index_string,
    const ObString &vector_str, int64_t &column_cnt, const int64_t nearest_center_idx) {
  int ret = OB_SUCCESS;
  ObObj obj;
  ObObj cast_obj;
  ObString tmp_str;
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
        if (!vector_str.empty()) {
          tmp_str = vector_str;
        } 
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
                pkey_str_raw_ptr_pos, allocator_,
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
  if (OB_SUCC(ret) && OB_UNLIKELY(need_to_set_dummy_pkeys)) {
    pkey_str.assign_ptr(pkey_str_raw_ptr, pkey_str_raw_ptr_pos);
  }
  return ret;
}

int ObIvfIndexBuildHelper::construct_batch_insert_index_sql(
    sqlclient::ObMySQLResult &result, common::ObSqlString &index_string,
    int64_t &row_count, int64_t &idx) {
  int ret = OB_SUCCESS;
  if (ObIvfKMeansStatus::FINISH != center_helper_.get_status()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(center_helper_.get_status()));
  } else {
    row_count = 0;
    int64_t nearest_center_idx = -1;
    double min_distance = DBL_MAX;
    ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
    allocator.set_attr(ObMemAttr(tenant_id_, "IvfArena"));
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE,
                       ObCharset::get_system_collation());
    bool first_row = true;
    // construct index_string
    while (OB_SUCC(ret) && row_count < BATCH_INSERT_SIZE) {
      ObTypeVector vector;
      int64_t column_cnt;
      if (OB_FAIL(pre_fill_data_insert_index_sql(result, vector, index_string,
                  column_cnt, min_distance, nearest_center_idx, first_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to pre fill data insert index sql", K(ret));
        }
        break;
      } else if (OB_FAIL(post_fill_data_insert_index_sql(cast_ctx, result, index_string,
                  "", column_cnt, nearest_center_idx))) {
        LOG_WARN("failed to post fill data insert index sql", K(ret));
      }
      if (OB_SUCC(ret)) {
        ++row_count;
        ++idx;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("success to construct batch insert index table sql", K(ret),
                K(index_string), K(row_count));
    }
  }
  LOG_TRACE("construct_batch_insert_index_sql", K(ret), K(index_string), K(row_count), K(idx));
  return ret;
}

int ObIvfIndexBuildHelper::construct_select_sql_string(
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
      } else if (center_helper_.is_using_elkan_kmeans() &&
                 OB_FAIL(select_string.append_fmt(
                     " order by %.*s",
                     static_cast<int>(
                         first_rowkey_schema->get_column_name_str().length()),
                     first_rowkey_schema->get_column_name_str().ptr()))) {
        LOG_WARN("fail to append select sql string", K(ret));
      } else if (OB_FAIL(center_helper_.set_select_sql_str(select_string))) {
        LOG_WARN("fail to assign string", K(ret), K(select_string));
      } else {
        LOG_TRACE("success to construct select sql from data table", K(ret),
                  K(select_string));
      }
    }
  }
  LOG_TRACE("construct_select_sql_string_simple", K(ret), K(select_string), K(dest_table_id), K(data_table_id));
  return ret;
}

int ObIvfIndexBuildHelper::set_sample_cache(
        ObIndexSampleCache *cache) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache) || !cache->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cache));
  } else {
    center_helper_.set_cache(cache);
  }
  return ret;
}

int ObIvfIndexBuildHelper::set_center_cache(const int64_t table_id) {
  int ret = OB_SUCCESS;
  if (!ObIvfIndexBuildHelper::is_finish()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(center_helper_.get_status()));
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_FAIL(MTL(ObTenantIvfCenterCache *)
                      ->put(table_id, partition_idx_, center_helper_.get_distance_type(),
                            center_helper_.get_center_vectors()))) {
        LOG_WARN("failed to put int cache", K(ret));
      }
    }
  }
  return ret;
}

// TODO(@jingshui) : need to opt! select * from data_table too many times now.
int ObIvfIndexBuildHelper::build() {
  int ret = OB_SUCCESS;
  if (!center_helper_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("kmeans center helper not init", K(ret), K(center_helper_.is_inited()));
  } else {
    ret = center_helper_.build();
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
