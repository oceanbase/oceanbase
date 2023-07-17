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
#include "ob_table_context.h"
#include "ob_table_service.h"
#include "ob_table_cg_service.h" // for generate_table_loc_meta
#include "sql/das/ob_das_define.h" // for ObDASTableLocMeta
#include "lib/utility/utility.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
namespace oceanbase
{
namespace table
{

int ObTableCtx::get_tablet_by_rowkey(const ObRowkey &rowkey,
                                     ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRowkey, 1> rowkeys;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> part_ids;
  SMART_VAR(sql::ObTableLocation, location_calc) {
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    } else if (OB_ISNULL(table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(get_session_info(),
                                                                       schema_guard_,
                                                                       table_schema_->get_table_id(),
                                                                       rowkeys,
                                                                       tablet_ids,
                                                                       part_ids))) {
      LOG_WARN("fail to calc partition id", K(ret));
    } else if (1 != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should have one tablet", K(ret), K(tablet_ids));
    } else {
      tablet_id = tablet_ids.at(0);
    }
  }
  return ret;
}

int ObTableCtx::init_sess_info(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  // try get session from session pool
  if (OB_FAIL(GCTX.table_service_->get_sess_mgr().get_sess_info(credential, sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  } else if (OB_ISNULL(sess_guard_.get_sess_node_val())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(credential));
  }

  return ret;
}

int ObTableCtx::init_common(ObTableApiCredential &credential,
                            const common::ObTabletID &arg_tablet_id,
                            const common::ObString &arg_table_name,
                            const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  const ObTenantSchema *tenant_schema = nullptr;
  const uint64_t tenant_id = credential.tenant_id_;
  const uint64_t database_id = credential.database_id_;
  ObTabletID tablet_id = arg_tablet_id;

  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id), K(arg_table_name));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
                                                    database_id,
                                                    arg_table_name,
                                                    false, /* is_index */
                                                    table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(arg_table_name));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("fail get table schema by table name", K(ret), K(tenant_id), K(database_id), K(arg_table_name));
  } else if (OB_FAIL(schema_guard_.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (OB_FAIL(init_sess_info(credential))) {
    LOG_WARN("fail to init session info", K(ret), K(credential));
  } else if (!arg_tablet_id.is_valid()) {
    if (is_scan_) { // 扫描场景使用table_schema上的tablet id,客户端已经做了路由分发
      if (table_schema_->is_partitioned_table()) { // 不支持分区表
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("partitioned table not supported", K(ret), K(arg_table_name));
      } else {
        tablet_id = table_schema_->get_tablet_id();
      }
    } else { // dml场景使用rowkey计算出tablet id
      if (!table_schema_->is_partitioned_table()) {
        tablet_id = table_schema_->get_tablet_id();
      } else if (OB_ISNULL(entity_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("entity is null", K(ret));
      } else if (OB_FAIL(get_tablet_by_rowkey(entity_->get_rowkey(), tablet_id))) {
        LOG_WARN("fail to get tablet id by rowkey", K(ret), K(entity_->get_rowkey()));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                                 tablet_id,
                                                 0, /* expire_renew_time */
                                                 is_cache_hit,
                                                 ls_id_))) {
    LOG_WARN("fail to get ls id", K(ret), K(tablet_id), K(arg_table_name));
  } else if (!is_scan_ && OB_FAIL(adjust_entity())) {
    LOG_WARN("fail to adjust entity", K(ret));
  } else {
    tenant_id_ = tenant_id;
    database_id_ = database_id;
    table_name_ = arg_table_name;
    ref_table_id_ = table_schema_->get_table_id();
    index_table_id_ = ref_table_id_;
    tablet_id_ = tablet_id;
    index_tablet_id_ = tablet_id_;
    tenant_schema_version_ = tenant_schema->get_schema_version();
    timeout_ts_ = timeout_ts;
  }

  return ret;
}

// 获取rowkey或者二级索引的columns type
int ObTableCtx::generate_columns_type(ObIArray<ObExprResType> &columns_type)
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_column_type;
  const ObColumnSchemaV2 *column_schema = nullptr;

  if (is_index_scan_) {
    const int64_t N = index_col_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(column_schema = index_schema_->get_column_schema(index_col_ids_.at(i)))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(index_col_ids_.at(i)));
      } else if (OB_FAIL(cons_column_type(*column_schema, tmp_column_type))) {
        LOG_WARN("fail to cons column type", K(ret));
      } else if (OB_FAIL(columns_type.push_back(tmp_column_type))) {
        LOG_WARN("fail to push back column type", K(ret), K(tmp_column_type));
      }
    }
  } else { // primary key
    const ObRowkeyInfo &rowkey_info = table_schema_->get_rowkey_info();
    const int64_t N = rowkey_info.get_size();
    uint64_t column_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i));
      } else if (OB_ISNULL(column_schema = table_schema_->get_column_schema(column_id))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(column_id));
      } else if (OB_FAIL(cons_column_type(*column_schema, tmp_column_type))) {
        LOG_WARN("fail to cons column type", K(ret));
      } else if (OB_FAIL(columns_type.push_back(tmp_column_type))) {
        LOG_WARN("fail to push back column type", K(ret), K(tmp_column_type));
      }
    }
  }

  return ret;
}

int ObTableCtx::cons_column_type(const ObColumnSchemaV2 &column_schema,
                                 ObExprResType &column_type)
{
  int ret = OB_SUCCESS;
  column_type.set_type(column_schema.get_data_type());
  column_type.set_result_flag(ObRawExprUtils::calc_column_result_flag(column_schema));

  if (ob_is_string_type(column_schema.get_data_type()) || ob_is_json(column_schema.get_data_type())) {
    column_type.set_collation_type(column_schema.get_collation_type());
    column_type.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    column_type.set_collation_type(CS_TYPE_BINARY);
    column_type.set_collation_level(CS_LEVEL_NUMERIC);
  }
  const ObAccuracy &accuracy = column_schema.get_accuracy();
  column_type.set_accuracy(accuracy);
  const bool is_zerofill = column_type.has_result_flag(ZEROFILL_FLAG);
  if (is_zerofill) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("modifing column with ZEROFILL flag is not supported", K(ret), K(column_schema));
  }

  return ret;
}

int ObTableCtx::convert_lob(ObIAllocator &allocator, ObObj &obj)
{
  int ret = OB_SUCCESS;

  ObString full_data;
  if (obj.has_lob_header()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object should not have lob header", K(ret), K(obj));
  } else if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(obj, allocator))) { // add lob header
    LOG_WARN("fail to add lob header to obj", K(ret), K(obj));
  }

  return ret;
}

int ObTableCtx::read_real_lob(ObIAllocator &allocator, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObString full_data;

  if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, obj, full_data))) {
    LOG_WARN("Lob: failed to get full data", K(ret), K(obj));
  } else {
    obj.set_string(obj.get_type(), full_data);
    obj.set_inrow();
  }

  return ret;
}

int ObTableCtx::adjust_column_type(const ObExprResType &column_type,
                                   ObObj &obj)
{
  int ret = OB_SUCCESS;
  const bool is_not_nullable = column_type.is_not_null_for_read();
  const ObCollationType cs_type = column_type.get_collation_type();

  // 1. check nullable
  if (is_not_nullable && obj.is_null()) {
    ret = OB_BAD_NULL_ERROR;
  } else if (obj.is_null()) {
    // continue
  } else if (column_type.get_type() != obj.get_type()
             && !(ob_is_string_type(column_type.get_type()) && ob_is_string_type(obj.get_type()))) {
    // 2. data type mismatch
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("object type mismatch with column type", K(ret), K(column_type), K(obj));
  } else {
    // 3. check collation
    if (!ob_is_string_type(obj.get_type())) {
      // not string type, continue
    } else {
      if (cs_type == obj.get_collation_type()) {
        // same collation type
      } else if (cs_type == CS_TYPE_BINARY) {
        // any collation type can be compatible with cs_type_binary
        obj.set_collation_type(cs_type);
      } else if (ObCharset::charset_type_by_coll(cs_type) == ObCharset::charset_type_by_coll(obj.get_collation_type())) {
        // same charset, convert it
        obj.set_collation_type(cs_type);
      } else {
        ret = OB_ERR_COLLATION_MISMATCH;
        LOG_WARN("collation type mismatch with column", K(ret), K(column_type), K(obj));
      }
      if (OB_SUCC(ret)) {
        // convert obj type to the column type (char, varchar or text)
        obj.set_type(column_type.get_type());
        if (is_lob_storage(obj.get_type()) && cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0) {
          if (OB_FAIL(convert_lob(ctx_allocator_, obj))) {
            LOG_WARN("fail to convert lob", K(ret), K(obj));
          }
        }
      }
    }
    // 4. check accuracy
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_obj_accuracy_check_only(column_type.get_accuracy(), cs_type, obj))) {
        LOG_WARN("accuracy check failed", K(ret), K(obj), K(column_type));
      }
    }
  }

  return ret;
}

int ObTableCtx::adjust_column(const ObColumnSchemaV2 &col_schema, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObExprResType column_type;

  if (OB_FAIL(cons_column_type(col_schema, column_type))) {
    LOG_WARN("fail to construct column type", K(ret), K(col_schema));
  } else if (OB_FAIL(adjust_column_type(column_type, obj))) {
    LOG_WARN("fail to adjust rowkey column type", K(ret), K(obj));
  }

  return ret;
}

int ObTableCtx::adjust_rowkey()
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey = entity_->get_rowkey();

  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    if (rowkey.get_obj_cnt() != table_schema_->get_rowkey_column_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity rowkey count mismatch table schema rowkey count",
           K(ret), K(rowkey.get_obj_cnt()), K(table_schema_->get_rowkey_column_num()));
    } else {
      const ObRowkeyInfo &rowkey_info = table_schema_->get_rowkey_info();
      const ObColumnSchemaV2 *col_schema = nullptr;
      uint64_t column_id = OB_INVALID_ID;
      ObObj *obj_ptr = rowkey.get_obj_ptr();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); i++) {
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          LOG_WARN("fail to get column id", K(ret), K(i));
        } else if (OB_ISNULL(col_schema = table_schema_->get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", K(ret), K(column_id));
        } else if (OB_FAIL(adjust_column(*col_schema, obj_ptr[i]))) {
          LOG_WARN("fail to adjust column", K(ret), K(obj_ptr[i]));
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::adjust_properties()
{
  int ret = OB_SUCCESS;

  bool is_get = (ObTableOperationType::Type::GET == operation_type_);
  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    ObTableEntity *entity = static_cast<ObTableEntity*>(const_cast<ObITableEntity *>(entity_));
    const ObIArray<ObString> &prop_names = entity->get_properties_names();
    const ObIArray<ObObj> &prop_objs = entity->get_properties_values();
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < prop_names.count(); i++) {
      const ObString &col_name = prop_names.at(i);
      ObObj &prop_obj = const_cast<ObObj &>(prop_objs.at(i));
      if (OB_ISNULL(col_schema = table_schema_->get_column_schema(col_name))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("fail to get column schema", K(ret), K(col_name));
      } else if (is_get) {
        // do nothing
      } else if (col_schema->is_rowkey_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("property should not be rowkey column", K(ret), K(prop_names), K(i));
      } else if (OB_FAIL(adjust_column(*col_schema, prop_obj))) {
        LOG_WARN("fail to adjust column", K(ret), K(prop_obj));
      }
    }
  }

  return ret;
}

int ObTableCtx::adjust_entity()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (!is_htable() && OB_FAIL(adjust_rowkey())) {
    LOG_WARN("fail to adjust rowkey", K(ret));
  } else if (OB_FAIL(adjust_properties())) {
    LOG_WARN("fail to check properties", K(ret));
  }

  return ret;
}

bool ObTableCtx::has_exist_in_columns(const ObIArray<ObString> &columns,
                                      const ObString &name,
                                      int64_t *idx /* =nullptr */) const
{
  bool exist = false;
  int64_t num = columns.count();
  for (int64_t i = 0; i < num && !exist; i++) {
    if (0 == name.case_compare(columns.at(i))) {
      exist = true;
      if (idx != NULL) {
        *idx = i;
      }
    }
  }
  return exist;
}

int ObTableCtx::generate_key_range(const ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  int64_t padding_num = -1;
  ObArray<sql::ObExprResType> columns_type;
  int64_t N = scan_ranges.count();

  if (OB_FAIL(generate_columns_type(columns_type))) {
    LOG_WARN("fail to generate columns type", K(ret));
  } else if (is_index_scan_) {
    // 索引扫描场景下用户可能没有填写rowkey的key_range，需要加上
    padding_num = index_schema_->get_rowkey_column_num() - index_col_ids_.count();
  }
  // check obj type in ranges
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) { // foreach range
    const ObNewRange &range = scan_ranges.at(i);
    // check column type
    for (int64_t j = 0; OB_SUCCESS == ret && j < 2; ++j) {
      const ObRowkey *p_key = nullptr;
      if (0 == j) {
        p_key = &range.get_start_key();
      } else {
        p_key = &range.get_end_key();
      }
      if (p_key->is_min_row() || p_key->is_max_row()) {
        // do nothing
      } else {
        if (p_key->get_obj_cnt() != columns_type.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong rowkey size", K(ret), K(i), K(j), K(*p_key), K(columns_type));
        } else {
          const int64_t M = p_key->get_obj_cnt();
          for (int64_t k = 0; OB_SUCCESS == ret && k < M; ++k) {
            ObObj &obj = const_cast<ObObj&>(p_key->get_obj_ptr()[k]);
            if (obj.is_min_value() || obj.is_max_value()) {
              // do nothing
            } else if (OB_FAIL(adjust_column_type(columns_type.at(k), obj))) {
              LOG_WARN("fail to adjust column type", K(ret), K(columns_type.at(k)), K(obj));
            }
          }
        }
      }
    }
    if (OB_UNLIKELY(padding_num > 0)) {
      // index scan need fill primary key object
      ObNewRange index_key_range = range;
      for (int64_t j = 0; OB_SUCCESS == ret && j < 2; ++j) {
        const ObRowkey *p_key = nullptr;
        if (0 == j) {
          p_key = &range.get_start_key();
        } else {
          p_key = &range.get_end_key();
        }
        if (p_key->is_min_row() || p_key->is_max_row()) {
          // do nothing
        } else {
          const int64_t old_objs_num = p_key->get_obj_cnt();
          const int64_t new_objs_num = old_objs_num + padding_num;
          ObObj *new_objs = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj)*new_objs_num));
          if (OB_ISNULL(new_objs)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc new objs", K(ret));
          } else {
            const ObObj *old_objs = p_key->get_obj_ptr();
            for (int64_t k = 0; k < old_objs_num; ++k) {
              new_objs[k] = old_objs[k];  // shallow copy
            }
            if (0 == j) {  // padding for startkey
              for (int64_t k = 0; k < padding_num; ++k) {
                new_objs[k+old_objs_num] = ObObj::make_min_obj();
              }
              index_key_range.start_key_.assign(new_objs, new_objs_num);
            } else {  // padding for endkey
              for (int64_t k = 0; k < padding_num; ++k) {
                new_objs[k+old_objs_num] = ObObj::make_max_obj();
              }
              index_key_range.end_key_.assign(new_objs, new_objs_num);
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(key_ranges_.push_back(index_key_range))) {
          LOG_WARN("fail to push back key range", K(ret), K(index_key_range));
        }
      }
    } else {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(key_ranges_.push_back(range))) {
          LOG_WARN("fail to push back key range", K(ret), K(range));
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_scan(const ObTableQuery &query,
                          const bool &is_wead_read)
{
  int ret = OB_SUCCESS;
  const ObString &index_name = query.get_index_name();
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  const bool select_all_columns = select_columns.empty(); // select all when query column is empty.
  const ObColumnSchemaV2 *column_schema = nullptr;
  operation_type_ = ObTableOperationType::Type::SCAN;
  // init is_weak_read_,scan_order_
  is_weak_read_ = is_wead_read;
  scan_order_ = query.get_scan_order();
  // init limit_,offset_
  bool is_query_with_filter = query.get_htable_filter().is_valid() ||
                              query.get_filter_string().length() > 0;
  limit_ = is_query_with_filter ? -1 : query.get_limit(); // // query with filter can't pushdown limit
  offset_ = query.get_offset();
  // init is_index_scan_
  if (index_name.empty() || 0 == index_name.case_compare(ObIndexHint::PRIMARY_KEY)) { // scan with primary key
    index_table_id_ = ref_table_id_;
    is_index_back_ = false;
  } else {
    is_index_scan_ = true;
    // init index_table_id_,index_schema_
    if (OB_FAIL(init_index_info(index_name))) {
      LOG_WARN("fail to init index info", K(ret), K(index_name));
    } else {
      // init index_col_ids_
      if (OB_SUCC(ret)) {
        const common::ObIndexInfo &index_info = index_schema_->get_index_info();
        if (OB_FAIL(index_info.get_column_ids(index_col_ids_))) {
          LOG_WARN("fail to get index column ids", K(ret), K(index_info));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // init key_ranges_
    if (OB_FAIL(generate_key_range(query.get_scan_ranges()))) {
      LOG_WARN("fail to generate key ranges", K(ret));
    } else {
      // select_col_ids用schema序
      for (ObTableSchema::const_column_iterator iter = table_schema_->column_begin();
          OB_SUCC(ret) && iter != table_schema_->column_end(); ++iter) {
        const ObColumnSchemaV2 *column_schema = *iter;
        ObString column_name;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is NULL", K(ret));
        } else if (FALSE_IT(column_name = column_schema->get_column_name_str())) {
        } else if (select_all_columns) {
          if (OB_FAIL(select_col_ids_.push_back(column_schema->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (OB_FAIL(query_col_ids_.push_back(column_schema->get_column_id()))) {
            LOG_WARN("fail to push back column id", K(ret), K(column_schema->get_column_id()));
          } else if (OB_FAIL(query_col_names_.push_back(column_name))) {
            LOG_WARN("fail to push back column name", K(ret), K(column_name));
          } else if (is_index_scan_ && !is_index_back_ &&
              OB_ISNULL(column_schema = index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true; // 判断是否需要回表,查询的列不在索引表上即需要回表
          }
        } else if (has_exist_in_columns(select_columns, column_schema->get_column_name_str())) {
          if (OB_FAIL(select_col_ids_.push_back(column_schema->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (is_index_scan_ && !is_index_back_ &&
              OB_ISNULL(column_schema = index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true; // 判断是否需要回表,查询的列不在索引表上即需要回表
          }
        }
      }
      if (OB_SUCC(ret)) {
        if ((select_col_ids_.count() != select_columns.count()) && !select_all_columns) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("select_col_ids or select_metas count is not equal to select_columns",
              K(select_columns), K(select_col_ids_));
        } else if (!select_all_columns) {
          // query_col_ids_是用户查询序
          for (int64_t i = 0; OB_SUCC(ret) && i < select_columns.count(); i++) {
            if (OB_ISNULL(column_schema = table_schema_->get_column_schema(select_columns.at(i)))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("select column not found in schema", K(ret), K(select_columns.at(i)));
            } else if (OB_FAIL(query_col_ids_.push_back(column_schema->get_column_id()))) {
              LOG_WARN("fail to push back column id", K(ret), K(column_schema->get_column_id()));
            } else if (OB_FAIL(query_col_names_.push_back(column_schema->get_column_name_str()))) {
              LOG_WARN("fail to push back column name", K(ret), K(column_schema->get_column_name_str()));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_insert()
{
  return init_dml_related_tid();
}

int ObTableCtx::init_assign_ids(ObAssignIds &assign_ids,
                                const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> storage_col_ids;

  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(table_schema_->get_column_ids(storage_col_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(assign_ids.init(storage_col_ids.count()))) {
    LOG_WARN("fail to init assign_ids capacity", K(ret), K(storage_col_ids.count()));
  } else {
    ObObj prop_value; // useless
    const share::schema::ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_col_ids.count(); i++) {
      ObString cname;
      ObAssignId assign_id;
      if (OB_ISNULL(col_schema = table_schema_->get_column_schema(storage_col_ids.at(i)))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("column not exists", K(ret), K(cname));
      } else if (col_schema->is_stored_generated_column()) {
        assign_id.column_id_ = storage_col_ids.at(i);
        assign_id.idx_ = i;
        if (OB_FAIL(assign_ids.push_back(assign_id))) {
          LOG_WARN("fail to push back assign id", K(ret), K(assign_id));
        }
      } else if (FALSE_IT(cname = col_schema->get_column_name_str())) {
        // do nothing
      } else if (OB_SUCCESS != entity.get_property(cname, prop_value)) {
        // 这一列没有被更新，跳过
      } else {
        assign_id.column_id_ = storage_col_ids.at(i);
        assign_id.idx_ = i;
        if (OB_FAIL(assign_ids.push_back(assign_id))) {
          LOG_WARN("fail to push back assign id", K(ret), K(assign_id));
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_update()
{
  int ret = OB_SUCCESS;
  is_for_update_ = true;
  const bool is_batch = OB_ISNULL(batch_op_) ? false : true;
  const ObTableEntity *entity = nullptr;
  ObAssignIds assign_ids(allocator_);
  // 1. init assign_ids_
  if (OB_FAIL(init_assign_ids(assign_ids_, static_cast<const ObTableEntity&>(*entity_)))) {
    LOG_WARN("fail to init assign ids", K(ret), K(*entity_));
  }

  // 2. init scan
  if (OB_SUCC(ret)) {
    index_table_id_ = ref_table_id_;
    is_index_scan_ = false;
    is_index_back_ = false;
    is_get_ = true;
    scan_order_ = ObQueryFlag::Forward;
    if (OB_FAIL(table_schema_->get_column_ids(select_col_ids_))) { // init select_col_ids_
      LOG_WARN("fail to get column ids", K(ret));
    }
  }

  // 3. init related index table id
  if (OB_SUCC(ret) && OB_FAIL(init_dml_related_tid())) {
    LOG_WARN("fail to init dml related table ids", K(ret));
  }

  return ret;
}

int ObTableCtx::init_delete()
{
  int ret = OB_SUCCESS;
  // 1. init scan
  const int64_t column_num = table_schema_->get_column_count();
  index_table_id_ = ref_table_id_;
  is_index_scan_ = false;
  is_index_back_ = false;
  is_get_ = is_htable() ? false : true;
  scan_order_ = ObQueryFlag::Forward;

  // 2. init select_col_ids_
  const ObColumnSchemaV2 *column_schema = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_num; i++) {
    if (OB_ISNULL(column_schema = table_schema_->get_column_schema_by_idx(i))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get column schema bu idx", K(ret), K(i));
    } else if (column_schema->is_generated_column()) {
      // skip
    } else if (OB_FAIL(select_col_ids_.push_back(column_schema->get_column_id()))) {
      LOG_WARN("fail to push back column id", K(ret), K(column_schema->get_column_id()));
    } else if (OB_FAIL(query_col_names_.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("fail to push back column name", K(ret));
    }
  }

  // 3. init related index table id
  if (OB_SUCC(ret) && OB_FAIL(init_dml_related_tid())) {
    LOG_WARN("fail to init dml related table ids", K(ret));
  }

  return ret;
}

int ObTableCtx::init_replace()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_dml_related_tid())) {
    LOG_WARN("fail to init dml related tids", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObPhysicalPlanCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObPhysicalPlanCtx", K(ret), K(sizeof(ObPhysicalPlanCtx)));
    } else {
      ObPhysicalPlanCtx *phy_plan_ctx = new(buf) ObPhysicalPlanCtx(allocator_);
      phy_plan_ctx->set_timeout_timestamp(timeout_ts_); // ObConflictChecker::init_das_scan_rtdef 需要
      phy_plan_ctx->set_tenant_schema_version(tenant_schema_version_);
      exec_ctx_.set_physical_plan_ctx(phy_plan_ctx);
    }
  }

  return ret;
}

int ObTableCtx::init_insert_up()
{
  int ret = OB_SUCCESS;
  is_for_insertup_ = true;

  if (OB_FAIL(init_update())) {
    LOG_WARN("fail to init update", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObPhysicalPlanCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObPhysicalPlanCtx", K(ret), K(sizeof(ObPhysicalPlanCtx)));
    } else {
      ObPhysicalPlanCtx *phy_plan_ctx = new(buf) ObPhysicalPlanCtx(allocator_);
      phy_plan_ctx->set_timeout_timestamp(timeout_ts_); // ObConflictChecker::init_das_scan_rtdef 需要
      phy_plan_ctx->set_tenant_schema_version(tenant_schema_version_);
      exec_ctx_.set_physical_plan_ctx(phy_plan_ctx);
    }
  }

  // reset for update flag
  is_for_update_ = false;
  return ret;
}

int ObTableCtx::init_get()
{
  int ret = OB_SUCCESS;
  // init scan
  index_table_id_ = ref_table_id_;
  is_index_scan_ = false;
  is_index_back_ = false;
  is_get_ = true;
  scan_order_ = ObQueryFlag::Forward;
  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scheam is null", K(ret));
  } else if (OB_FAIL(entity_->get_properties_names(query_col_names_))) {
    LOG_WARN("fail to get entity properties names", K(ret));
  } else {
    const bool need_get_all_column = query_col_names_.empty(); // 未设置get的列，默认返回全部列
    // init select_col_ids, query_col_names_
    for (ObTableSchema::const_column_iterator iter = table_schema_->column_begin();
        OB_SUCC(ret) && iter != table_schema_->column_end();
        ++iter) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is NULL", K(ret));
      } else if (OB_FAIL(select_col_ids_.push_back(column_schema->get_column_id()))) {
        LOG_WARN("fail to add column id", K(ret));
      } else if (need_get_all_column
          && OB_FAIL(query_col_names_.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("fail to push back column name", K(ret));
      }
    }
  }
  return ret;
}

int ObTableCtx::init_append(bool return_affected_entity, bool return_rowkey)
{
  int ret = OB_SUCCESS;
  return_affected_entity_ = return_affected_entity;
  return_rowkey_ = return_rowkey;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;

  if (OB_FAIL(init_insert_up())) {
    LOG_WARN("fail to init insert up", K(ret));
  } else if (OB_FAIL(entity_->get_properties(properties))) {
    LOG_WARN("fail to get properties", K(ret));
  } else {
    const int64_t N = properties.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      ObObj &delta = properties.at(i).second;
      if (delta.is_null()) {
        ret = OB_OBJ_TYPE_ERROR;
        LOG_WARN("append NULL is illegal", K(ret), K(delta));
      } else if (OB_UNLIKELY(!ob_is_string_type(delta.get_type()))) {
        ret = OB_OBJ_TYPE_ERROR;
        LOG_WARN("can only append string type", K(ret), K(delta));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 构造生成列表达式字符串"concat_ws('', `column_name`, `column_name`)"
    const share::schema::ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_ids_.count(); i++) {
      uint64_t column_id = assign_ids_.at(i).column_id_;
      ObString column_name;
      if (OB_ISNULL(col_schema = table_schema_->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", K(ret), K(column_id));
      } else if (FALSE_IT(column_name = col_schema->get_column_name_str())) {
      } else {
        const int64_t total_len = 22 + column_name.length() + column_name.length();
        int64_t actual_len = -1;
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc memory", K(ret), K(total_len));
        } else if ((actual_len = snprintf(buf, total_len, "concat_ws('', `%s`, `%s`)",
            column_name.ptr(), column_name.ptr())) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to construct concat_ws expr string", K(ret), K(total_len));
        } else {
          ObString expr_str(actual_len, buf);
          if (OB_FAIL(expr_strs_.push_back(expr_str))) {
            LOG_WARN("fail to push back expr str", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableCtx::init_increment(bool return_affected_entity, bool return_rowkey)
{
  int ret = OB_SUCCESS;
  return_affected_entity_ = return_affected_entity;
  return_rowkey_ = return_rowkey;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;

  if (OB_FAIL(init_insert_up())) {
    LOG_WARN("fail to init insert up", K(ret));
  } else if (OB_FAIL(entity_->get_properties(properties))) {
    LOG_WARN("fail to get properties", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < properties.count(); i++) {
      ObObj &delta = properties.at(i).second;
      if (!ob_is_int_tc(delta.get_type())) {
        ret = OB_OBJ_TYPE_ERROR;
        LOG_WARN("delta should only be signed integer type", K(ret), K(delta));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 构造生成列表达式字符串"ifnull(C1, 0) + C1"
    const share::schema::ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_ids_.count(); i++) {
      uint64_t column_id = assign_ids_.at(i).column_id_;
      ObString column_name;
      if (OB_ISNULL(col_schema = table_schema_->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", K(ret), K(column_id));
      } else if (FALSE_IT(column_name = col_schema->get_column_name_str())) {
      } else {
        const int64_t total_len = column_name.length() + column_name.length() + 16;
        int64_t actual_len = -1;
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc memory", K(ret), K(total_len));
        } else if ((actual_len = snprintf(buf, total_len, "IFNULL(%s, 0) + %s",
            column_name.ptr(), column_name.ptr())) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to construct increment expr string", K(ret), K(total_len));
        } else {
          ObString expr_str(actual_len, buf);
          if (OB_FAIL(expr_strs_.push_back(expr_str))) {
            LOG_WARN("fail to push back expr str", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableCtx::classify_scan_exprs()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr *> *exprs = is_for_update_ ? &old_row_exprs_ : &all_exprs_.get_expr_array();
  int64_t exprs_cnt = exprs->count();

  if (0 == exprs_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exprs is empty", K(ret));
  } else if (!select_exprs_.empty()) {
    // had classify, do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_cnt; i++) {
      if (exprs->at(i)->is_column_ref_expr()) {
        ObColumnRefRawExpr *expr = static_cast<ObColumnRefRawExpr *>(exprs->at(i));
        if (expr->is_rowkey_column() && OB_FAIL(rowkey_exprs_.push_back(expr))) {
          LOG_WARN("fail to push back rowkey expr", K(ret));
        } else if (has_exist_in_array(select_col_ids_, expr->get_column_id())
                  && OB_FAIL(select_exprs_.push_back(expr))) {
          LOG_WARN("fail to push back select expr", K(ret));
        } else if (is_index_scan_
            && has_exist_in_array(index_col_ids_, expr->get_column_id())
            && OB_FAIL(index_exprs_.push_back(expr))) {
          LOG_WARN("fail to push back index column expr", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_exec_ctx()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_context(exec_ctx_.get_das_ctx()))) {
    LOG_WARN("fail to init das context", K(ret));
  } else {
    // init exec_ctx_.my_session_
    exec_ctx_.set_my_session(&get_session_info());
  }
  return ret;
}

// 初始化 das context 中的 table_loc 和 tablet_loc
int ObTableCtx::init_das_context(ObDASCtx &das_ctx)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *table_loc = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  int64_t part_idx = OB_INVALID_ID;
  int64_t subpart_idx = OB_INVALID_ID;

  if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(*this, loc_meta_))) {
    LOG_WARN("fail to generate table location meta", K(ret));
  } else if (OB_FAIL(exec_ctx_.get_das_ctx().extended_table_loc(loc_meta_, table_loc))) {
    LOG_WARN("fail to extend table loc", K(ret), K(loc_meta_));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null", K(ret));
  } else if (table_schema_->is_partitioned_table() &&
      OB_FAIL(table_schema_->get_part_idx_by_tablet(tablet_id_, part_idx, subpart_idx))) {
    LOG_WARN("fail to get part idx by tablet", K(ret), K_(tablet_id));
  } else {
    DASRelatedTabletMap &related_tablet_map = das_ctx.get_related_tablet_map();
    for (int64_t i = 0; OB_SUCC(ret) && i < related_index_ids_.count(); i++) {
      ObTableID related_table_id = related_index_ids_.at(i);
      const ObSimpleTableSchemaV2 *relative_table_schema = nullptr;
      ObObjectID related_part_id = OB_INVALID_ID;
      ObObjectID related_first_level_part_id = OB_INVALID_ID;
      ObTabletID related_tablet_id;
      if (OB_FAIL(schema_guard_.get_simple_table_schema(tenant_id_,
                                                        related_table_id,
                                                        relative_table_schema))) {
        LOG_WARN("get_table_schema fail", K(ret), K(tenant_id_), K(related_table_id));
      } else if (OB_ISNULL(relative_table_schema)) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("fail to get table schema", KR(ret), K(related_table_id));
      } else if (OB_FAIL(relative_table_schema->get_part_id_and_tablet_id_by_idx(part_idx,
                                                                                 subpart_idx,
                                                                                 related_part_id,
                                                                                 related_first_level_part_id,
                                                                                 related_tablet_id))) {
        LOG_WARN("get part by idx failed", K(ret), K(part_idx), K(subpart_idx), K(related_table_id));
      } else if (OB_FAIL(related_tablet_map.add_related_tablet_id(tablet_id_,
                                                                  related_table_id,
                                                                  related_tablet_id,
                                                                  related_part_id,
                                                                  related_first_level_part_id))) {
        LOG_WARN("fail to add related tablet id", K(ret),
                  K(tablet_id_), K(related_table_id), K(related_part_id), K(related_tablet_id));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(exec_ctx_.get_das_ctx().extended_tablet_loc(*table_loc,
                                                                          index_tablet_id_,
                                                                          tablet_loc))) {
    LOG_WARN("fail to extend tablet loc", K(ret), K(index_tablet_id_));
  }

  return ret;
}

int ObTableCtx::init_trans(transaction::ObTxDesc *trans_desc,
                           const transaction::ObTxReadSnapshot &tx_snapshot)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans desc is null", K(ret));
  } else {
    sql::ObSQLSessionInfo &session = get_session_info();
    sql::ObSQLSessionInfo::LockGuard guard(session.get_thread_data_lock());
    session.get_tx_desc() = trans_desc;
    exec_ctx_.get_das_ctx().set_snapshot(tx_snapshot);
  }

  return ret;
}

// for query
int ObTableCtx::init_index_info(const ObString &index_name)
{
  int ret = OB_SUCCESS;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE];
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;

  if (OB_FAIL(schema_guard_.get_can_read_index_array(tenant_id_,
                                                     ref_table_id_,
                                                     tids,
                                                     index_cnt,
                                                     false))) {
    LOG_WARN("fail to get can read index", K(ret), K_(tenant_id), K_(ref_table_id));
  } else if (index_cnt > OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index count is bigger than OB_MAX_INDEX_PER_TABLE", K(ret), K(index_cnt));
  } else {
    const share::schema::ObTableSchema *index_schema = nullptr;
    ObString this_index_name;
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt && !is_found; i++) {
      if (OB_FAIL(schema_guard_.get_table_schema(tenant_id_, tids[i], index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K_(tenant_id), K(tids[i]));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema is null", K(ret), K_(tenant_id), K(tids[i]));
      } else if (OB_FAIL(index_schema->get_index_name(this_index_name))) {
        LOG_WARN("fail to get index name", K(ret));
      } else if (0 == this_index_name.case_compare(index_name)) {
        is_found = true;
        index_table_id_ = tids[i];
        index_schema_ = index_schema;
        index_tablet_id_ = index_schema->get_tablet_id();
      }
    }

    if (OB_SUCC(ret) && !is_found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index name", K(ret), K(index_name));
    }
  }

  return ret;
}

int ObTableCtx::init_dml_related_tid()
{
  int ret = OB_SUCCESS;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE];
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
  const ObTableSchema *index_schema = nullptr;

  if (OB_FAIL(schema_guard_.get_can_write_index_array(tenant_id_,
                                                      ref_table_id_,
                                                      tids,
                                                      index_cnt,
                                                      false /*only global*/))) {
    LOG_WARN("fail to get can write index array", K(ret), K_(ref_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; i++) {
      if (OB_FAIL(schema_guard_.get_table_schema(tenant_id_, tids[i], index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(tids[i]), K(i), K(index_cnt));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("null index schema", K(ret));
      } else if (index_schema->is_index_local_storage()) {
        if (OB_FAIL(related_index_ids_.push_back(index_schema->get_table_id()))) {
          LOG_WARN("fail to add related index ids", K(ret), K(index_schema->get_table_id()));
        }
      }
    }
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
