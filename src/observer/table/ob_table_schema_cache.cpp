/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_schema_cache.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/plan_cache/ob_plan_cache.h"

namespace oceanbase
{

namespace table
{

int ObKvSchemaCacheKey::deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObKvSchemaCacheKey &key = static_cast<const ObKvSchemaCacheKey&>(other);
  table_id_ = key.table_id_;
  schema_version_ = key.schema_version_;
  return ret;
}

uint64_t ObKvSchemaCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  return hash_val;
}

bool ObKvSchemaCacheKey::is_equal(const ObILibCacheKey &other) const
{
  const ObKvSchemaCacheKey &key = static_cast<const ObKvSchemaCacheKey&>(other);
  bool cmp_ret = table_id_ == key.table_id_ &&
                 schema_version_ == key.schema_version_;

  return cmp_ret;
}

void ObKvSchemaCacheKey::reset()
{
  table_id_ = common::OB_INVALID_ID;
  schema_version_ = -1;
  namespace_ = ObLibCacheNameSpace::NS_KV_SCHEMA;
}

int ObKvSchemaCacheObj::cons_table_info(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObKVAttr attr;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, table_schema->get_table_name_str(), table_name_, true))) {
    LOG_WARN("fail to copy column name", K(ret), K(table_schema->get_table_name_str()));
  } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(table_schema->get_kv_attributes(), kv_attributes_))) {
    LOG_WARN("fail to parse kv attributes", K(ret), K(table_schema->get_kv_attributes()));
  } else {
    auto_inc_cache_size_ = table_schema->get_auto_increment_cache_size();
    set_is_ttl_table(!table_schema->get_ttl_definition().empty());
    set_is_partitioned_table(table_schema->is_partitioned_table());
    if (ObHTableUtils::is_htable_schema(*table_schema)) {
      uint64_t data_version = 0;
      if (OB_NOT_NULL(table_schema->get_column_schema(ObHTableConstants::TTL_CNAME_STR))) {
        flags_.has_hbase_ttl_column_ = true;
        if (GET_MIN_DATA_VERSION(MTL_ID(), data_version)) {
          LOG_WARN("get data version failed", K(ret));
        } else if (data_version < DATA_VERSION_4_3_5_1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support ttl column with data version less than 4_3_5_1", K(ret), K(data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ttl column with data version less than 4_3_5_1");
        }
      }
    }
    if (OB_SUCC(ret) && flags_.is_ttl_table_) {
      if (OB_FAIL(ob_write_string(allocator_, table_schema->get_ttl_definition(), ttl_definition_))) {
        LOG_WARN("fail to copy ttl definaitions", K(ret));
      }
    }
  }
  return ret;
}

int ObKvSchemaCacheObj::cons_index_info(ObSchemaGetterGuard *schema_guard,
                                        uint64_t tenant_id,
                                        common::ObTableID table_id)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE];
  if (OB_ISNULL(schema_guard) || !schema_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL or not inited", K(ret));
  } else if (OB_FAIL(schema_guard->get_can_write_index_array(tenant_id,
                                                             table_id,
                                                             tids,
                                                             index_cnt,
                                                             false /*only global*/))) {
    LOG_WARN("fail to get can write index array", K(ret), K(table_id));
  } else if (OB_FAIL(local_index_tids_.init(index_cnt))) {
    LOG_WARN("fail to init local index tids", K(ret));
  } else if (OB_FAIL(global_index_tids_.init(index_cnt))) {
    LOG_WARN("fail to init global index tids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id_, tids[i], index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(tids[i]), K(i), K(index_cnt));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("null index schema", K(ret));
      } else if (index_schema->is_index_local_storage()) {
        if (OB_FAIL(local_index_tids_.push_back(tids[i]))) {
          LOG_WARN("fail to push back local index tids", K(ret));
        } else {
          flags_.has_local_index_ = true;
          if (index_schema->is_fts_index()) {
            flags_.has_fts_index_ = true;
          }
        }
      } else if (index_schema->is_global_index_table()) {
        if (OB_FAIL(global_index_tids_.push_back(tids[i]))) {
          LOG_WARN("fail to push back local index tids", K(ret));
        } else {
          flags_.has_global_index_ = true;
        }
      }
    }
  }
  return ret;
}

int ObKvSchemaCacheObj::cons_rowkey_array(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  rowkey_info_array_.set_allocator(&allocator_);
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(rowkey_info_array_.init(table_schema->get_rowkey_column_num()))) {
    LOG_WARN("fail to alloc rowkey info array", K(ret), K(table_schema->get_rowkey_column_num()));
  } else {
    rowkey_cnt_ = table_schema->get_rowkey_column_num();
    const common::ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt_; i++) {
      ObTableRowkeyInfo *rowkey = nullptr;
      if (OB_ISNULL(rowkey = OB_NEWx(ObTableRowkeyInfo, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory to construct rowkey info", K(ret), K(i));
      } else if (OB_FAIL(rowkey_info.get_column_id(i, rowkey->column_id_))) {
        LOG_WARN("fail to get column id", K(ret), K(i));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rowkey_info_array_.push_back(rowkey))) {
        LOG_WARN("fail to push back rowkey info", K(ret), K(i));
      }
    } // end for
  }
  return ret;
}

int ObKvSchemaCacheObj::cons_columns_array(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  column_info_array_.set_allocator(&allocator_);
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(column_info_array_.init(table_schema->get_column_count()))) {
    LOG_WARN("fail to alloc column info array", K(ret), K(table_schema->get_column_count()));
  } else {
    column_cnt_ = table_schema->get_column_count();
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObTableSchema::const_column_iterator iter = table_schema->column_begin();
    ObTableSchema::const_column_iterator end = table_schema->column_end();
    for (int64_t i = 0; OB_SUCC(ret) && iter != end; ++iter, i++) {
      ObTableColumnInfo *col_info = nullptr;
      col_schema = *iter;
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is NULL", K(ret), K(i));
      } else if (OB_ISNULL(col_info = OB_NEWx(ObTableColumnInfo, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory to construct conlumn info", K(ret), K(i));
      } else {
        ObSEArray<uint64_t, 8> cascaded_column_ids;
        col_info->col_idx_ = i;
        col_info->column_id_ = col_schema->get_column_id();
        col_info->table_id_ = col_schema->get_table_id();
        col_info->column_flags_ = col_schema->get_column_flags();
        col_info->is_auto_increment_ = col_schema->is_autoincrement();
        col_info->is_nullable_ = col_schema->is_nullable();
        col_info->is_tbl_part_key_column_ = col_schema->is_tbl_part_key_column();
        col_info->is_rowkey_column_ = col_schema->is_rowkey_column();
        col_info->auto_filled_timestamp_ = col_schema->is_on_update_current_timestamp();
        col_info->rowkey_position_ = col_schema->get_rowkey_position();
        col_info->tbl_part_key_pos_ = col_schema->get_tbl_part_key_pos();
        col_info->type_.set_type(col_schema->get_data_type());
        col_info->type_.set_accuracy(col_schema->get_accuracy());
        col_info->type_.set_result_flag(ObRawExprUtils::calc_column_result_flag(*col_schema));
        if (col_info->type_.has_result_flag(ZEROFILL_FLAG)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "modifing column with ZEROFILL flag");
          LOG_WARN("modifing column with ZEROFILL flag is not supported", K(ret), K(i));
        } else if (ob_is_string_type(col_schema->get_data_type()) || ob_is_json(col_schema->get_data_type())) {
          col_info->type_.set_collation_type(col_schema->get_collation_type());
          col_info->type_.set_collation_level(CS_LEVEL_IMPLICIT);
        } else {
          col_info->type_.set_collation_type(CS_TYPE_BINARY);
          col_info->type_.set_collation_level(CS_LEVEL_NUMERIC);
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(allocator_, col_schema->get_column_name_str(), col_info->column_name_, true))) {
          LOG_WARN("fail to copy column name", K(ret), K(col_schema->get_column_name_str()), K(i));
        } else {
          // copy col default value
          ObObj obj = col_schema->get_cur_default_value();
          if (obj.need_deep_copy()) {
            const int64_t copy_size = obj.get_deep_copy_size();
            char *buf = nullptr;
            int64_t pos = 0;
            if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(copy_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret));
            } else {
              if (OB_FAIL(col_info->default_value_.deep_copy(obj, buf, copy_size, pos))) {
                LOG_WARN("fail to deep copy obj", K(ret), K(obj));
              }
              if (OB_FAIL(ret)) {
                allocator_.free(buf);
              }
            }
          } else {
            col_info->default_value_ = obj;
          }

          if (OB_FAIL(ret)) {
          } else if (col_info->is_generated_column()) {
            col_info->cascaded_column_ids_.set_allocator(&allocator_);
            if (OB_FAIL(col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
              LOG_WARN("fail to get cascaded column ids", K(ret), K(i));
            } else if (OB_FAIL(col_info->cascaded_column_ids_.init(cascaded_column_ids.count()))) {
              LOG_WARN("fail to init cascaded_column_ids array", K(cascaded_column_ids.count()));
            } else if (OB_FAIL(col_info->cascaded_column_ids_.assign(cascaded_column_ids))) {
              LOG_WARN("fail to assign cascaded_column_ids to column info", K(ret), K(i));
            } else {
              // for generated column, its default values is generated expr str and its obj type need deep copy
              col_info->generated_expr_str_ = col_info->default_value_.get_string();
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_info_array_.push_back(col_info))) {
        LOG_WARN("fail to push back column info", K(ret), K(i));
      } else {
        // mark the flags for some special columns
        if (!flags_.has_auto_inc_ && col_info->is_auto_increment_) {
          set_has_auto_inc(col_info->is_auto_increment_);
        }
        if (!flags_.has_generated_column_ && col_info->is_generated_column()) {
          set_has_generated_column(col_info->is_generated_column());
        }
        if (!flags_.has_lob_column_ && is_lob_storage(col_info->type_.get_type())) {
          set_has_lob_column(true);
        }
      }
    } // end for

    if (OB_SUCC(ret) && OB_FAIL(build_index_map())) {
      LOG_WARN("fail to build index map", K(ret));
    }
  }
  return ret;
}

int ObKvSchemaCacheObj::build_index_map()
{
  int ret = OB_SUCCESS;
  int64_t column_size = column_info_array_.count();
  if (!col_name_idx_map_.created() &&
        OB_FAIL(col_name_idx_map_.create(column_size, &name_idx_allocator_, &bucket_allocator_wrapper_))) {
    LOG_WARN("fail to create colname_idx_map", K(ret), K(column_size));
  } else if (!col_id_idx_map_.created() &&
        OB_FAIL(col_id_idx_map_.create(column_size, &colid_idx_allocator_, &bucket_allocator_wrapper_))) {
    LOG_WARN("fail to create colid_idx_map", K(ret), K(column_size));
  } else {
    for (int64_t i = 0; i < column_size && OB_SUCC(ret); i++) {
      ObTableColumnInfo *col_info = nullptr;
      if (OB_ISNULL(col_info = column_info_array_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (OB_FAIL(col_id_idx_map_.set_refactored(col_info->column_id_, i))) {
        LOG_WARN("fail to add entry into colname map", K(ret), K(col_info->column_id_), K(i));
      } else {
        ObColumnSchemaHashWrapper column_name_key(col_info->column_name_);
        if (OB_FAIL(col_name_idx_map_.set_refactored(column_name_key, i))) {
          LOG_WARN("fail to add entry into colname map", K(ret), K(col_info->column_name_), K(i));
        }
      }
    }
  }
  return ret;
}

ObKvSchemaCacheObj::~ObKvSchemaCacheObj()
{
  col_id_idx_map_.destroy();
  col_name_idx_map_.destroy();
  column_info_array_.reset();
  rowkey_info_array_.reset();
}

int ObKvSchemaCacheGuard::init(uint64_t tenant_id, uint64_t table_id, int64_t schema_version, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (MTL_ID() != tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unmatched tenant_id", K(MTL_ID()), K(tenant_id), K(lbt()));
  } else if (OB_ISNULL(lib_cache_ = MTL(ObPlanCache*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan cache", K(ret));
  } else {
    cache_key_.table_id_ = table_id;
    cache_key_.schema_version_ = schema_version;
    if (OB_FAIL(get_or_create_cache_obj(schema_guard))) {
      LOG_WARN("fail to get or create cache obj", K(ret), K(is_use_cache_));
    } else {
      is_init_ = true;
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_or_create_cache_obj(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lib_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is NULL", K(ret));
  } else if (OB_FAIL(lib_cache_->get_cache_obj(cache_ctx_, &cache_key_, cache_guard_))) {
    LOG_TRACE("fail to get cache obj, try create cache obj", K(ret), K(cache_key_));
    is_use_cache_ = false;
    if (OB_FAIL(create_schema_cache_obj(schema_guard))) {
      LOG_WARN("fail to create schema cache obj", K(ret));
    }
  } else {
    is_use_cache_ = true;
  }
  return ret;
}

int ObKvSchemaCacheGuard::create_schema_cache_obj(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = nullptr;
  // get full table_schema
  if (!schema_guard.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema getter guard is not inited", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), cache_key_.table_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(MTL_ID()), K(cache_key_.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is not exist, get table schema is NULL", K(ret));
  } else if (cache_key_.schema_version_ != table_schema->get_schema_version()) {
    cache_key_.schema_version_ = table_schema->get_schema_version();
    LOG_WARN("schema version has change", K(cache_key_.table_id_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObCacheObjectFactory::alloc(cache_guard_,
                                               ObLibCacheNameSpace::NS_KV_SCHEMA,
                                               lib_cache_->get_tenant_id()))) {
    LOG_WARN("fail to alloc new cache obj", K(ret));
  } else {
    ObKvSchemaCacheObj *cache_obj = nullptr;
    if (OB_ISNULL(cache_obj = static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache obj is NULL", K(ret));
    } else if (OB_FAIL(cache_obj->cons_table_info(table_schema))) {
      LOG_WARN("fail to construct table info", K(ret));
    } else if (OB_FAIL(cache_obj->cons_index_info(&schema_guard, MTL_ID(), table_schema->get_table_id()))) {
      LOG_WARN("fail to construct index info", K(ret));
    } else if (OB_FAIL(cache_obj->cons_columns_array(table_schema))) {
      LOG_WARN("fail to construct column info array", K(ret));
    } else if (OB_FAIL(cache_obj->cons_rowkey_array(table_schema))) {
      LOG_WARN("fail to construct column info array", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(lib_cache_->add_cache_obj(cache_ctx_, &cache_key_, cache_obj))) {
        LOG_WARN("fail to add cache obj to lib cache", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_info_idx(const ObString &col_name, int64_t &idx)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    ColNameIdxMap& colname_idx_map = cache_obj->get_col_name_map();
    const ObTableColumInfoArray& column_info_array = cache_obj->get_column_info_array();
    ObColumnSchemaHashWrapper column_name_key(col_name);
    if (OB_FAIL(colname_idx_map.get_refactored(column_name_key, idx))) {
      LOG_WARN("fail to get column info ", K(ret), K(col_name));
    } else if (idx < 0 || idx >= column_info_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column info array idx", K(idx));
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_info(const ObString &col_name, const ObTableColumnInfo *&col_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(get_column_info_idx(col_name, idx))) {
    LOG_WARN("fail to get column idx by name", K(ret), K(col_name), K(idx));
  } else {
    const ObIArray<ObTableColumnInfo *>& column_info_array = get_column_info_array();
    if (OB_ISNULL(col_info = column_info_array.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(idx), K(column_info_array));
    }
  }

  return ret;
}

int ObKvSchemaCacheGuard::get_column_info_idx(uint64_t col_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    ColIdIdxMap& colid_idx_map = cache_obj->get_col_id_map();
    const ObTableColumInfoArray& column_info_array = cache_obj->get_column_info_array();
    if (OB_FAIL(colid_idx_map.get_refactored(col_id, idx))) {
      LOG_WARN("fail to get column info ", K(ret), K(col_id));
    } else if (idx < 0 || idx >= column_info_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column info array idx", K(idx));
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_kv_attributes(ObKVAttr &kv_attributes)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    kv_attributes = cache_obj->get_kv_attributes();
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_ttl_definition(ObString &ttl_definition)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    ttl_definition = cache_obj->get_ttl_definition();
  }
  return ret;
}

int ObKvSchemaCacheGuard::has_generated_column(bool& has_generated_column)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    has_generated_column = cache_obj->get_schema_flags().has_generated_column_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::is_ttl_table(bool &is_ttl_table)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    is_ttl_table = cache_obj->get_schema_flags().is_ttl_table_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::is_redis_ttl_table(bool &is_redis_ttl_table)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    is_redis_ttl_table = cache_obj->get_kv_attributes().is_redis_ttl_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::has_hbase_ttl_column(bool &has_ttl_column)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    has_ttl_column = cache_obj->get_schema_flags().has_hbase_ttl_column_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::is_partitioned_table(bool &is_partitioned_table)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    is_partitioned_table = cache_obj->get_schema_flags().is_partitioned_table_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::has_global_index(bool &has_gloabl_index)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    has_gloabl_index = cache_obj->get_schema_flags().has_global_index_;
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_count(int64_t& column_count)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    column_count = cache_obj->get_column_count();
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    const ObTableRowkeyInfoArray &rowkey_info_array = cache_obj->get_rowkey_info_array();
    int64_t rowkey_size = rowkey_info_array.count();
    for (int64_t i = 0; i < rowkey_size && OB_SUCC(ret); i++) {
      ObTableRowkeyInfo *rowkey_info = nullptr;
      if (OB_ISNULL(rowkey_info = rowkey_info_array.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey info is NULL", K(ret), K(i));
      } else if (OB_FAIL(column_ids.push_back(rowkey_info->column_id_))) {
        LOG_WARN("Fail to add rowkey id to scan", K(ret), K(rowkey_info->column_id_), K(i));
      }
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_rowkey_column_num(int64_t& rowkey_column_num) {
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    rowkey_column_num = cache_obj->get_rowkey_count();
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_ids(ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    const ObTableColumInfoArray &column_info_array = cache_obj->get_column_info_array();
    int64_t column_size = column_info_array.count();
    for (int64_t i = 0; i < column_size && OB_SUCC(ret); i++) {
      ObTableColumnInfo *col_info = nullptr;
      if (OB_ISNULL(col_info = column_info_array.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (OB_FAIL(column_ids.push_back(col_info->column_id_))) {
        LOG_WARN("Fail to add column id to scan", K(ret), K(col_info->column_id_), K(i));
      }
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_rowkey_column_id(const int64_t index, uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    const ObTableRowkeyInfoArray& column_info_array = cache_obj->get_rowkey_info_array();
    if (OB_UNLIKELY(index < 0 || index >= column_info_array.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument.", K(index), K(column_info_array.count()), K(ret));
    } else {
      column_id = column_info_array.at(index)->column_id_;
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_info_by_idx(const int64_t idx, const ObTableColumnInfo *&col_info)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheObj *cache_obj = nullptr;
  if (OB_FAIL(get_cache_obj(cache_obj))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    const ObIArray<ObTableColumnInfo *>& column_info_array = cache_obj->get_column_info_array();
    if (OB_ISNULL(col_info = column_info_array.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(idx), K(column_info_array));
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_column_info(uint64_t col_id, const ObTableColumnInfo *&col_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(get_column_info_idx(col_id, idx))) {
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    const ObIArray<ObTableColumnInfo *>& column_info_array = get_column_info_array();
    if (OB_ISNULL(col_info = column_info_array.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(idx), K(column_info_array));
    }
  }
  return ret;
}

int ObKvSchemaCacheGuard::get_cache_obj(ObKvSchemaCacheObj *&cache_obj)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache guard is not init", K(ret));
  } else if (OB_ISNULL(cache_obj = static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is NULL", K(ret));
  }
  return ret;
}

void ObKvSchemaCacheGuard::reset()
{
  int ret = cache_guard_.force_early_release(lib_cache_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to release cache guard", K(ret));
  }
  cache_key_.reset();
  is_init_ = false;
  is_use_cache_ = false;
  lib_cache_ = nullptr;
}

} // end namespace table
} // end namespace oceanbase
