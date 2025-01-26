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
#include "ob_table_cg_service.h" // for generate_table_loc_meta
#include "ob_table_aggregation.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "src/share/table/ob_table_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

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
    } else if (OB_ISNULL(simple_table_schema_) || OB_ISNULL(schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema or shema guard is null", K(ret), KP(simple_table_schema_), KP(schema_guard_));
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(get_session_info(),
                                                                       *schema_guard_,
                                                                       simple_table_schema_->get_table_id(),
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

/*
  1. ObTableColumnItem record some column info, such as column id、column name、and so on.
  2. we record some specific elements like is_stored_generated_column()
    is_auto_increment_、auto_filled_timestamp_ for specific function.
  3. cascaded_column_ids_ is for update stored generate column.
    such as:
    - create table t(
      `c1` int primary key,
      `c2` varchar(20),
      `c3` varchar(20) generated always as (substring(`c2`, 1, 4) stored));
    - insert into t(`c1`, `c2`) values(1, 'hello');
    - update t set `c2`='world' where `c1`=1;
    `c3` should be updated as well.
*/
int ObTableCtx::cons_column_items_for_cg()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is null or not inited", K(ret), KP(schema_cache_guard_));
  } else {
    const ObIArray<ObTableColumnInfo *>& col_info_array = schema_cache_guard_->get_column_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      ObTableColumnInfo *col_info = col_info_array.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else {
        ObTableColumnItem item(col_info);
        if (OB_FAIL(column_items_.push_back(item))) {
          LOG_WARN("fail to push back column item", K(ret), K_(column_items), K(item));
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (is_for_update_ || is_for_insertup_) {
      // add column item to ObTableAssignment for cg stage
      for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count(); i++) {
        ObTableAssignment &assign = assigns_.at(i);
        if (OB_ISNULL(assign.column_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assign column info is NULL", K(ret), K(i));
        } else if (assign.column_info_->is_generated_column()) {
          void *buf = ctx_allocator_.alloc(sizeof(ObTableColumnItem));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc ObTableColumnItem", K(ret), K(sizeof(ObTableColumnItem)));
          } else {
            ObTableColumnItem *tmp_item = new(buf) ObTableColumnItem(assign.column_info_);
            assign.column_item_ = tmp_item;
          }
        } else {
          assign.column_item_ = &column_items_.at(assign.column_info_->col_idx_);
        }
      }
    }
  }


  return ret;
}

int ObTableCtx::get_column_item_by_column_id(uint64_t column_id, const ObTableColumnItem *&item) const
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is invalid", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(schema_cache_guard_->get_column_info_idx(column_id, idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      item = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get column info idx", K(column_id));
    }
  } else {
    item = &column_items_.at(idx);
  }

  return ret;
}

int ObTableCtx::get_column_item_by_expr(ObRawExpr *raw_expr, const ObTableColumnItem *&item) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr is null", K(ret));
  } else if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; i < column_items_.count() && !found; i++) {
      const ObTableColumnItem &tmp_item = column_items_.at(i);
      if (tmp_item.raw_expr_ == raw_expr) {
        found = true;
        item = &tmp_item;
      }
    }
  }

  return ret;
}

int ObTableCtx::get_column_item_by_expr(ObColumnRefRawExpr *expr, const ObTableColumnItem *&item) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr is null", K(ret));
  } else if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; i < column_items_.count() && !found; i++) {
      const ObTableColumnItem &tmp_item = column_items_.at(i);
      if (tmp_item.expr_ == expr) {
        found = true;
        item = &tmp_item;
      }
    }
  }

  return ret;
}

int ObTableCtx::get_expr_from_column_items(const ObString &col_name, ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is invalid", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(schema_cache_guard_->get_column_info_idx(col_name, idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      expr = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get column info idx", K(col_name));
    }
  } else {
    expr = column_items_.at(idx).raw_expr_;
  }
  return ret;
}

int ObTableCtx::get_expr_from_column_items(const ObString &col_name, ObColumnRefRawExpr *&expr) const
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is invalid", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(schema_cache_guard_->get_column_info_idx(col_name, idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      expr = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get column info idx", K(col_name));
    }
  } else {
    expr = column_items_.at(idx).expr_;
  }

  return ret;
}

int ObTableCtx::get_expr_from_assignments(const ObString &col_name, ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;

  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count() && !found; i++) {
    const ObTableAssignment &assign = assigns_.at(i);
    if (OB_ISNULL(assign.column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign column info is null", K(ret));
    } else if (0 == assign.column_info_->column_name_.case_compare(col_name)) {
      found = true;
      expr = assign.expr_;
    }
  }

  return ret;
}

int ObTableCtx::get_assignment_by_column_id(uint64_t column_id, const ObTableAssignment *&assign) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  assign = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count() && !found; i++) {
    const ObTableAssignment &tmp_assign = assigns_.at(i);
    if (OB_ISNULL(tmp_assign.column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign column info is null", K(ret));
    } else if (tmp_assign.column_info_->column_id_ == column_id) {
      found = true;
      assign = &tmp_assign;
    }
  }
  return ret;
}


/*
  1. ObConflictChecker need ObPhysicalPlanCtx.
  2. now() expr need ObPhysicalPlanCtx.cur_time_.
  3. das need ObPhysicalPlanCtx to check task should retry or not.
*/
void ObTableCtx::init_physical_plan_ctx(int64_t timeout_ts, int64_t tenant_schema_version)
{
  phy_plan_ctx_.set_timeout_timestamp(timeout_ts); // ObConflictChecker::init_das_scan_rtdef 需要
  phy_plan_ctx_.set_tenant_schema_version(tenant_schema_version);
  phy_plan_ctx_.set_cur_time(ObTimeUtility::fast_current_time());
  exec_ctx_.set_physical_plan_ctx(&phy_plan_ctx_);
}

/*
  init table context common param, such as tenant_id_, database_id_，and so on. we also do extra things:
  - init some marks from schema cache, such as has_global_index_, has_local_index...
  - adjust entity
*/
int ObTableCtx::init_common(ObTableApiCredential &credential,
                            const ObTabletID &arg_tablet_id,
                            const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  credential_ = &credential;
  tenant_id_ = credential.tenant_id_;
  database_id_ = credential.database_id_;
  if (OB_FAIL(init_schema_info_from_cache())) {
    LOG_WARN("fail to init schema info from cache", K(ret), K(tenant_id_), K(database_id_));
  } else {
    if (OB_ISNULL(simple_table_schema_) || OB_ISNULL(schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema or schema guard is NULL", K(ret), KP(simple_table_schema_), KP(schema_guard_));
    } else if (OB_FAIL(inner_init_common(arg_tablet_id, simple_table_schema_->get_table_name(), timeout_ts))) {
      LOG_WARN("fail to inner init common", KR(ret), K(credential), K(timeout_ts));
    }
  }

  return ret;
}

int ObTableCtx::inner_init_common(const ObTabletID &arg_tablet_id,
                                  const ObString &table_name,
                                  const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  const ObTenantSchema *tenant_schema = nullptr;
  ObTabletID tablet_id = arg_tablet_id;
  bool is_tablet_exists = true;

  if (OB_ISNULL(simple_table_schema_) || OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table schema or schema guard is null", K(ret), K(table_name), K(simple_table_schema_), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("fail to get tenant schema", K(ret), K_(tenant_id));
  } else if (FALSE_IT(init_physical_plan_ctx(timeout_ts, tenant_schema_version_))) {
    LOG_WARN("fail to init physical plan ctx", K(ret));
  } else if (!arg_tablet_id.is_valid() && !is_scan_) {
    // for scan scene, we will process it in init_scan when tablet_id is invalid
    // because we need to know if use index and index_type
    if (!simple_table_schema_->is_partitioned_table()) {
      tablet_id = simple_table_schema_->get_tablet_id();
    } else {
      // trigger client to refresh table entry
      // maybe drop a non-partitioned table and create a
      // partitioned table with same name
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partitioned table should pass right tablet id from client", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_scan_ && !ls_id_.is_valid() && OB_FAIL(GCTX.location_service_->get(tenant_id_,
                                                                                    tablet_id,
                                                                                    0, /* expire_renew_time */
                                                                                    is_cache_hit,
                                                                                    ls_id_))) {
    LOG_WARN("fail to get ls id", K(ret), K(tablet_id), K(table_name));
  } else if (has_auto_inc_ && OB_FAIL(add_auto_inc_param())) {
    LOG_WARN("fail to add auto inc param", K(ret));
  } else if (!is_scan_ && OB_FAIL(adjust_entity())) {
    LOG_WARN("fail to adjust entity", K(ret));
  } else if (OB_ISNULL(sess_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess guard is NULL", K(ret));
  } else {
    binlog_row_image_type_ = TABLEAPI_SESS_POOL_MGR->get_binlog_row_image();
    table_name_ = table_name;
    ref_table_id_ = simple_table_schema_->get_table_id();
    index_table_id_ = ref_table_id_;
    tablet_id_ = tablet_id;
    index_tablet_id_ = tablet_id_;
    timeout_ts_ = timeout_ts;
  }

  return ret;
}

int ObTableCtx::init_schema_info_from_cache()
{
  int ret = OB_SUCCESS;
  ObKVAttr attr;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", KP(schema_cache_guard_));
  } else if (OB_FAIL(schema_cache_guard_->get_kv_attributes(attr))) {
    LOG_WARN("fail to get kv attributes", K(ret));
  } else {
    is_redis_ttl_table_ = attr.is_redis_ttl_;
    ObTableSchemaFlags flags = schema_cache_guard_->get_schema_flags();
    flags_ = flags;
    has_auto_inc_ = flags.has_auto_inc_;
    is_ttl_table_ = flags.is_ttl_table_;
    if (OB_FAIL(schema_cache_guard_->get_ttl_definition(ttl_definition_))) {
      LOG_WARN("fail to get ttl definition", K(ret));
    }
    has_generated_column_ = flags.has_generated_column_;
    has_lob_column_ = flags.has_lob_column_;
    if (is_dml()) {
      has_global_index_ = flags.has_global_index_;
      has_local_index_ = flags.has_local_index_;
    }
    has_fts_index_ = flags.has_fts_index_;
    if (has_fts_index_ && OB_ISNULL(fts_ctx_)) {
      if (OB_ISNULL(fts_ctx_ = OB_NEWx(ObTableFtsCtx, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc fts_ctx", K(ret));
      }
    }
  }
  return ret;
}

// get columns info from index_schema or primary table schema
int ObTableCtx::generate_column_infos(ObIArray<const ObTableColumnInfo *> &columns_infos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", KP(schema_cache_guard_));
  } else {
    if (is_index_scan_) {
      const int64_t N = index_col_ids_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        const ObTableColumnInfo *column_info = nullptr;
        uint64_t col_id = index_col_ids_.at(i);
        if (OB_FAIL(schema_cache_guard_->get_column_info(col_id, column_info))) {
          LOG_WARN("fail to get column info", K(ret), K(index_col_ids_.at(i)));
        } else if (OB_ISNULL(column_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is NULL", K(ret), K(i));
        } else if (OB_FAIL(columns_infos.push_back(column_info))) {
          LOG_WARN("fail to push back column info", K(ret), KPC(column_info));
        }
      }
    } else {  // primary key
      int64_t N = 0;
      if (OB_FAIL(schema_cache_guard_->get_rowkey_column_num(N))) {
        LOG_WARN("failed to get rowkey column num", K(ret));
      } else {
        uint64_t column_id = OB_INVALID_ID;
        for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
          const ObTableColumnInfo *column_info = nullptr;
          if (OB_FAIL(schema_cache_guard_->get_rowkey_column_id(i, column_id))) {
            LOG_WARN("failed to get column id", K(ret), K(i));
          } else if (OB_FAIL(schema_cache_guard_->get_column_info(column_id, column_info))) {
            LOG_WARN("fail to get column info", K(ret), K(index_col_ids_.at(i)));
          } else if (OB_ISNULL(column_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column info is NULL", K(ret), K(i));
          } else if (OB_FAIL(columns_infos.push_back(column_info))) {
            LOG_WARN("fail to push back column info", K(ret), KPC(column_info));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::convert_lob(ObIAllocator &allocator, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 locator(obj.get_string(), obj.has_lob_header());
  if (obj.is_persist_lob() || locator.is_inrow_disk_lob_locator()) {
    // do nothing
  } else if (obj.has_lob_header()) { // we add lob header in write_datum
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "lob object should not have lob header");
    LOG_WARN("object should not have lob header", K(ret), K(obj), K(locator));
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

/*
  check column type
  - nullable
  - check data type mismatch or not
  - check collation for string type and convert obj type to the column type (char, varchar or text)
  - check accuracy
*/
int ObTableCtx::adjust_column_type(const ObTableColumnInfo &column_info, ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObExprResType &column_type = column_info.type_;
  const ObCollationType cs_type = column_type.get_collation_type();

  // 1. check nullable
  if (!column_info.is_nullable_ && obj.is_null()) {
    if (!column_info.is_auto_increment_) {
      ret = OB_BAD_NULL_ERROR;
      LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_info.column_name_.length(), column_info.column_name_.ptr());
    }
  } else if (obj.is_null() || is_inc_or_append()) { // increment or append check in init_increment() and init_append()
    if (is_inc_or_append() && ob_is_string_type(obj.get_type())) {
      obj.set_type(column_type.get_type()); // set obj to column type to add lob header when column type is lob, obj is varchar(varchar will not add lob header).
      obj.set_collation_type(cs_type);
    }
  } else if (column_type.get_type() != obj.get_type()
             && !(ob_is_string_type(column_type.get_type()) && ob_is_string_type(obj.get_type()))
             && !(ob_is_mysql_date_tc(column_type.get_type()) && ob_is_date_tc(obj.get_type()))
             && !(ob_is_mysql_datetime(column_type.get_type()) && ob_is_datetime(obj.get_type()))) {
    // 2. data type mismatch
    ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
    const char *schema_type_str = ob_obj_type_str(column_type.get_type());
    const char *obj_type_str = ob_obj_type_str(obj.get_type());
    LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH, column_info.column_name_.length(), column_info.column_name_.ptr(),
        static_cast<int>(strlen(schema_type_str)), schema_type_str, static_cast<int>(strlen(obj_type_str)), obj_type_str);
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
        ret = OB_KV_COLLATION_MISMATCH;
        const char *schema_coll_str = ObCharset::collation_name(cs_type);
        const char *obj_coll_str = ObCharset::collation_name(obj.get_collation_type());
        LOG_USER_ERROR(OB_KV_COLLATION_MISMATCH, column_info.column_name_.length(), column_info.column_name_.ptr(),
            static_cast<int>(strlen(schema_coll_str)), schema_coll_str, static_cast<int>(strlen(obj_coll_str)), obj_coll_str);
        LOG_WARN("collation type mismatch with column", K(ret), K(column_type), K(obj));
      }
      if (OB_SUCC(ret)) {
        // convert obj type to the column type (char, varchar or text)
        obj.set_type(column_type.get_type());
      }
    }
    // 4. check accuracy
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_obj_accuracy_check_only(column_type.get_accuracy(), cs_type, obj))) {
        if (ret == OB_DATA_OUT_OF_RANGE) {
          int64_t row_num = 0;
          LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, column_info.column_name_.length(), column_info.column_name_.ptr(), row_num);
        } else if (ret == OB_OPERATE_OVERFLOW) {
          const char *type_str = ob_obj_type_str(column_type.get_type());
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, type_str, column_info.column_name_.ptr());
        } else if (ret == OB_ERR_DATA_TOO_LONG) {
          int64_t row_num = 0;
          LOG_USER_ERROR(OB_ERR_DATA_TOO_LONG, column_info.column_name_.length(), column_info.column_name_.ptr(), row_num);
        }
        LOG_WARN("accuracy check failed", K(ret), K(obj), K(column_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // add lob header when is lob storage
    if (is_lob_storage(obj.get_type()) && cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0) {
      // use the processor's allocator to ensure the lifecycle of lob object
      if (OB_FAIL(convert_lob(allocator_, obj))) {
        LOG_WARN("fail to convert lob", K(ret), K(obj));
      }
    }
  }

  return ret;
}

/*
  check user rowkey is valid or not.
  1. rowkey count should equal schema rowkey count, except for auto increment.
    1.1. when rowkey has auto incrment column and it is not filled, do delete/update is not allow
  2. rowkey value should be valid.
*/
int ObTableCtx::adjust_rowkey()
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey = entity_->get_rowkey();
  int64_t schema_rowkey_cnt;

  if (OB_ISNULL(simple_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is invalid", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(schema_cache_guard_->get_rowkey_column_num(schema_rowkey_cnt))) {
    LOG_WARN("faild to get rowkey column num", K(ret));
  } else {
    const int64_t entity_rowkey_cnt = rowkey.get_obj_cnt();
    bool has_auto_inc = false; // only one auto increment column in a table
    bool is_full_filled = entity_rowkey_cnt == schema_rowkey_cnt; // allow not full filled when rowkey has auto_increment;
    uint64_t column_id = OB_INVALID_ID;
    ObObj *obj_ptr = rowkey.get_obj_ptr();
    for (int64_t i = 0, idx = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; i++) {
      bool need_check = true;
      const ObTableColumnInfo *col_info = nullptr;
      if (OB_FAIL(schema_cache_guard_->get_rowkey_column_id(i, column_id))) {
        LOG_WARN("fail to get column id", K(ret), K(i));
      } else if (OB_FAIL(schema_cache_guard_->get_column_info(column_id, col_info))) {
        LOG_WARN("fail to get column info", K(ret), K(column_id));
      } else if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret));
      } else if (!has_auto_inc && col_info->is_auto_increment_) {
        has_auto_inc = true;
        if (col_info->tbl_part_key_pos_ > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto increment column set to be partition column");
          LOG_WARN("auto increment column could not be partition column", K(ret));
        } else if (!is_full_filled && !need_full_rowkey_op()) {
          // curr column is auto_increment and user not fill，no need to check
          need_check = false;
        }
      }
      if (entity_type_ == ObTableEntityType::ET_HKV &&
          (operation_type_ == ObTableOperationType::Type::DEL || is_scan_)) {
        need_check = false;
      }

      if (OB_SUCC(ret) && need_check) {
        if (!is_full_filled && need_full_rowkey_op()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation is not supported to partially fill rowkey columns");
          LOG_WARN("rowkey columns is not fullfilled", K(ret), K_(operation_type), K(entity_rowkey_cnt), K(schema_rowkey_cnt), K(rowkey));
        } else if (idx >= entity_rowkey_cnt) {
          ret = OB_KV_ROWKEY_COUNT_NOT_MATCH;
          LOG_USER_ERROR(OB_KV_ROWKEY_COUNT_NOT_MATCH, schema_rowkey_cnt, entity_rowkey_cnt);
          LOG_WARN("entity rowkey count mismatch table schema rowkey count", K(ret),
                    K(entity_rowkey_cnt), K(schema_rowkey_cnt), K(rowkey));
        } else if (OB_FAIL(adjust_column_type(*col_info, obj_ptr[idx]))) { // [c1][c2][c3] [c1][c3]
          LOG_WARN("fail to adjust rowkey column type", K(ret), K(obj_ptr[idx]), KPC(col_info));
        } else {
          idx++;
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!has_auto_inc && entity_rowkey_cnt != schema_rowkey_cnt) {
      ret = OB_KV_ROWKEY_COUNT_NOT_MATCH;
      LOG_USER_ERROR(OB_KV_ROWKEY_COUNT_NOT_MATCH, schema_rowkey_cnt, entity_rowkey_cnt);
      LOG_WARN("entity rowkey count mismatch table schema rowkey count",
           K(ret), K(entity_rowkey_cnt), K(schema_rowkey_cnt));
    }
  }

  return ret;
}

/*
  check user properties is valid or not.
  1. rowkey column should not appear in properties, except for get operation.
  2. we do not check column when do get operation, cause property value is empty.
*/
int ObTableCtx::adjust_properties()
{
  int ret = OB_SUCCESS;
  bool skip_adjust_prop = (ObTableOperationType::Type::GET == operation_type_ ||
                          ObTableOperationType::Type::DEL == operation_type_);
  if (skip_adjust_prop) {
    // do nothing
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is invalid", K(ret), KP(schema_cache_guard_));
  } else {
    ObTableEntity *entity = static_cast<ObTableEntity*>(const_cast<ObITableEntity *>(entity_));
    const ObIArray<ObString> &prop_names = entity->get_properties_names();
    const ObIArray<ObObj> &prop_objs = entity->get_properties_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < prop_names.count(); i++) {
      const ObString &col_name = prop_names.at(i);
      ObObj &prop_obj = const_cast<ObObj &>(prop_objs.at(i));
      const ObTableColumnInfo *col_info = nullptr;
      if (OB_FAIL(schema_cache_guard_->get_column_info(col_name, col_info))) {
        LOG_WARN("fail to get column schema", K(ret), K(col_name));
        ret = OB_ERR_BAD_FIELD_ERROR;
        const ObString &table = simple_table_schema_->get_table_name_str();
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), table.length(), table.ptr());
      } else if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (col_info->rowkey_position_ > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutate rowkey column");
        LOG_WARN("property should not be rowkey column", K(ret), K(prop_names), K(i));
      } else if (col_info->is_generated_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "The specified value for generated column");
        LOG_WARN("The specified value for generated column is not allowed", K(ret), K(col_info->column_name_));
      } else if (OB_FAIL(adjust_column_type(*col_info, prop_obj))) {
        LOG_WARN("fail to adjust rowkey column type", K(ret), K(prop_obj), KPC(col_info));
      }
    }
  }

  return ret;
}

/*
  check the legality of entity.
    - we do not check htable's rowkey, cause htable's rowkey is in properties.
*/
int ObTableCtx::adjust_entity()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(adjust_rowkey())) {
    LOG_WARN("fail to adjust rowkey", K(ret));
  } else if (OB_FAIL(adjust_properties())) {
    LOG_WARN("fail to check properties", K(ret));
  }

  return ret;
}

int ObTableCtx::check_is_cs_replica_query(bool &is_cs_replica_query) const
{
  int ret = OB_SUCCESS;
  is_cs_replica_query = false;
  if (table_index_info_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_index_info_ is empty", K(ret));
  } else {
    ObDASTableLocMeta *loc_meta = table_index_info_.at(0).loc_meta_;
    if (OB_ISNULL(loc_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loc_meta is null", K(ret));
    } else if (ObRoutePolicyType::INVALID_POLICY == loc_meta->route_policy_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid route policy", K(ret));
    } else {
      is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == loc_meta->route_policy_;
    }
    LOG_TRACE("[CS-Replica] check cs replica query", K(ret), K(is_cs_replica_query), KPC(loc_meta));
  }
  return ret;
}



int ObTableCtx::generate_fts_search_range(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObKVParamsBase *kv_params_base = query.get_ob_params().ob_params_;
  // 1. extract search text from ObKVParams
  if (OB_ISNULL(kv_params_base) || kv_params_base->get_param_type() != ParamType::FTS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kv_params_base is NULL", K(ret), KPC(kv_params_base));
  } else if (OB_ISNULL(fts_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret), K(has_fts_index_));
  } else {
    ObFTSParam *fts_param = static_cast<ObFTSParam *>(kv_params_base);
    fts_ctx_->search_text_ = fts_param->get_search_text();
  }
  // 2. init scan range with [MIN:MAX]
  if (OB_SUCC(ret)) {
    int64_t padding_num = index_schema_->get_rowkey_column_num();
    key_ranges_.reset();
    ObNewRange index_key_range;
    index_key_range.set_whole_range();
    if (OB_FAIL(key_ranges_.push_back(index_key_range))) {
      LOG_WARN("fail to push back key_range", K(ret));
    }
  }
  return ret;
}

/*
  genreate key range when do scan.
    1. check the legality of column obj in query range.
    2. fill primary key object when do index scan when user not filled them.
*/
int ObTableCtx::generate_key_range(const ObIArray<ObString> &scan_ranges_columns, const ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  int64_t padding_num = -1;
  ObSEArray<const ObTableColumnInfo*, 16> columns_infos;
  int64_t N = scan_ranges.count();
  const uint64_t scan_ranges_columns_cnt = scan_ranges_columns.count();

  if (OB_FAIL(generate_column_infos(columns_infos))) {
    LOG_WARN("fail to generate columns infos", K(ret));
  } else if (scan_ranges_columns_cnt != 0) {
    if (scan_ranges_columns_cnt > columns_infos.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "scan ranges columns cnt greatter than columns_infos count");
      LOG_WARN("scan ranges columns cnt greatter than columns_infos count",
        K(ret), K(scan_ranges_columns_cnt), K(columns_infos.count()));
    }

    for (int i = 0; OB_SUCC(ret) && i < scan_ranges_columns_cnt; i++) {
      if (scan_ranges_columns.at(i).case_compare(columns_infos[i]->column_name_) != 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "scan ranges columns is not prefix of columns_infos");
        LOG_WARN("scan ranges columns is not prefix of columns_infos", K(ret), K(scan_ranges_columns), K(columns_infos));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_index_scan_) {
      padding_num = index_schema_->get_rowkey_column_num() - scan_ranges_columns_cnt;
    } else {
      padding_num = columns_infos.count() - scan_ranges_columns_cnt;
    }
  } else if (is_index_scan_) {
    // 索引扫描场景下用户可能没有填写rowkey的key_range，需要加上
    padding_num = index_schema_->get_rowkey_column_num() - index_col_ids_.count();
  }

  // check obj type in ranges
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) { // foreach range
    const ObNewRange &range = scan_ranges.at(i);
    is_full_table_scan_ = is_full_table_scan_ ? is_full_table_scan_ : range.is_whole_range();
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
        if (scan_ranges_columns_cnt != 0 && p_key->get_obj_cnt() != scan_ranges_columns_cnt) {
          ret = OB_KV_SCAN_RANGE_MISSING;
          LOG_USER_ERROR(OB_KV_SCAN_RANGE_MISSING, p_key->get_obj_cnt(), scan_ranges_columns_cnt);
          LOG_WARN("wrong scan range size", K(ret), K(i), K(j), K(*p_key), K(scan_ranges_columns_cnt));
        } else if (scan_ranges_columns_cnt == 0 && p_key->get_obj_cnt() != columns_infos.count()) {
          ret = OB_KV_SCAN_RANGE_MISSING;
          LOG_USER_ERROR(OB_KV_SCAN_RANGE_MISSING, p_key->get_obj_cnt(), columns_infos.count());
          LOG_WARN("wrong rowkey size", K(ret), K(i), K(j), K(*p_key), K(columns_infos));
        } else {
          const int64_t M = p_key->get_obj_cnt();
          for (int64_t k = 0; OB_SUCCESS == ret && k < M; ++k) {
            ObObj &obj = const_cast<ObObj&>(p_key->get_obj_ptr()[k]);
            if (obj.is_min_value() || obj.is_max_value()) {
              // do nothing
            } else if (OB_ISNULL(columns_infos.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column info is NULL", K(ret), K(k));
            } else if (OB_FAIL(adjust_column_type(*columns_infos.at(k), obj))) {
              LOG_WARN("fail to adjust column type", K(ret), K(columns_infos.at(k)), K(obj));
            } else if (ob_is_mysql_date_tc(columns_infos.at(k)->type_.get_type()) && ob_is_date_tc(obj.get_type())) {
              ObMySQLDate mdate = 0;
              if (OB_FAIL(ObTimeConverter::date_to_mdate(obj.get_date(), mdate))) {
                LOG_WARN("fail to convert date to mysql date", K(ret), K(obj));
              } else {
                obj.set_mysql_date(mdate);
              }
            } else if (ob_is_mysql_datetime(columns_infos.at(k)->type_.get_type()) && ob_is_datetime(obj.get_type())) {
              ObMySQLDateTime mdatetime = 0;
              if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(obj.get_datetime(), mdatetime))) {
                LOG_WARN("fail to convert datetime to mysql datetime", K(ret), K(obj));
              } else {
                obj.set_mysql_datetime(mdatetime);
              }
            }
          }
        }
      }
    }
    if (OB_UNLIKELY(padding_num > 0)) {
      // index scan need fill primary key object
      // prefix scan need fill object
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
                // if inclusive start, should padding min value. else padding max value
                if (index_key_range.border_flag_.inclusive_start()) {
                  new_objs[k+old_objs_num] = ObObj::make_min_obj();
                } else {
                  new_objs[k+old_objs_num] = ObObj::make_max_obj();
                }
              }
              index_key_range.start_key_.assign(new_objs, new_objs_num);
            } else {  // padding for endkey
              for (int64_t k = 0; k < padding_num; ++k) {
                // if inclusive end, should padding max value. else padding min value
                if (index_key_range.border_flag_.inclusive_end()) {
                  new_objs[k+old_objs_num] = ObObj::make_max_obj();
                } else {
                  new_objs[k+old_objs_num] = ObObj::make_min_obj();
                }
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

/*
  init scan parameters.
    1. init some common scan parameters such as is_weak_read_、scan_order_、limit_、is_index_back_ and so on.
    2. genreate key range。
    3. check is_index_back_ when do index scan, need index back if the query column is not in the index table.
    4. select_col_ids_ is same order with schema,
      query_col_ids_ and query_col_names_ is same with user query column order.
*/
int ObTableCtx::init_scan(const ObTableQuery &query,
                          const bool &is_wead_read,
                          const uint64_t arg_table_id)
{
  int ret = OB_SUCCESS;
  const ObString &index_name = query.get_index_name();
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  bool has_filter = (query.get_htable_filter().is_valid() || query.get_filter_string().length() > 0);
  const bool select_all_columns = select_columns.empty() || query.is_aggregate_query() || (has_filter && !is_htable());
  operation_type_ = ObTableOperationType::Type::SCAN;
  if (query.is_aggregate_query() && query.get_aggregations().count() == 1) {
    // support later
    // is_count_all_ = query.get_aggregations().at(0).is_agg_all_column();
  }
  // init is_weak_read_,scan_order_
  is_weak_read_ = is_wead_read;
  scan_order_ = query.get_scan_order();
  // init limit_,offset_
  bool is_query_with_filter = query.get_htable_filter().is_valid() ||
                              query.get_filter_string().length() > 0;
  limit_ = is_query_with_filter || is_ttl_table() ? -1 : query.get_limit(); // query with filter or ttl table can't pushdown limit
  offset_ = is_ttl_table() ? 0 : query.get_offset();
  if (!tablet_id_.is_valid()) {
    if (simple_table_schema_->is_partitioned_table()) {
      tablet_id_ = ObTabletID::INVALID_TABLET_ID;;
    } else {
      tablet_id_ = simple_table_schema_->get_tablet_id();
    }
  }
  // init is_index_scan_
  if (OB_FAIL(ret)) {
  } else if (index_name.empty() || 0 == index_name.case_compare(ObIndexHint::PRIMARY_KEY)) { // scan with primary key
    if (!tablet_id_.is_valid()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partitioned table should pass right tablet id from client", K(ret));
    } else {
      index_table_id_ = ref_table_id_;
      index_tablet_id_ = tablet_id_;
      is_index_back_ = false;
    }
  } else {
    is_index_scan_ = true;
    // init index_table_id_,index_schema_
    if (OB_FAIL(init_index_info(index_name, arg_table_id))) {
      LOG_WARN("fail to init index info", K(ret), K(index_name));
    } else {
      // init index_col_ids_
      if (OB_SUCC(ret)) {
        const ObIndexInfo &index_info = index_schema_->get_index_info();
        if (OB_FAIL(index_info.get_column_ids(index_col_ids_))) {
          LOG_WARN("fail to get index column ids", K(ret), K(index_info));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool is_cache_hit = false;
    if (OB_FAIL(GCTX.location_service_->get(tenant_id_,
                                            index_tablet_id_,
                                            0, /* expire_renew_time */
                                            is_cache_hit,
                                            ls_id_))) {
      LOG_WARN("fail to get ls id", K(ret), K(index_tablet_id_), K(table_name_));
    } else if (is_text_retrieval_scan() && OB_FAIL(generate_fts_search_range(query))) {
      LOG_WARN("fail to generate fts search ranges", K(ret), K(query));
    } else if (!is_text_retrieval_scan() && OB_FAIL(generate_key_range(query.get_scan_range_columns(), query.get_scan_ranges()))) {// init key_ranges_
      LOG_WARN("fail to generate key ranges", K(ret));
    } else if (query.is_aggregate_query() && OB_FAIL(init_agg_cell_proj(query.get_aggregations().count()))) {
      LOG_WARN("fail to init agg cell proj", K(ret));
    } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema cache guard is null or not inited", K(ret), KP(schema_cache_guard_));
    } else {
      // select_col_ids_ is same order with schema
      const ObIArray<ObTableColumnInfo *> &col_info_array = schema_cache_guard_->get_column_info_array();
      ObSEArray<ObString, 4> ttl_columns;
      if (is_ttl_table_) {
        if (OB_FAIL(ObTTLUtil::get_ttl_columns(ttl_definition_, ttl_columns))) {
          LOG_WARN("fail to get ttl columns", K(ret));
        }
      }

      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_info_array.count(); cell_idx++) {
        ObTableColumnInfo *col_info = col_info_array.at(cell_idx);
        ObString column_name;
        if (OB_ISNULL(col_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is NULL", K(ret), K(cell_idx));
        } else if (FALSE_IT(column_name = col_info->column_name_)) {
        } else if (select_all_columns) {
          if (!is_tsc_with_doc_id() && col_info->is_fulltext_column() ) {
            // skip to add into the select coulmn id
          } else if (OB_FAIL(select_col_ids_.push_back(col_info->column_id_))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (OB_FAIL(query_col_ids_.push_back(col_info->column_id_))) {
            LOG_WARN("fail to push back column id", K(ret), K(col_info->column_id_));
          } else if (OB_FAIL(query_col_names_.push_back(column_name))) {
            LOG_WARN("fail to push back column name", K(ret), K(column_name));
          } else if (query.is_aggregate_query() &&
                      OB_FAIL(add_aggregate_proj(cell_idx, column_name, query.get_aggregations()))) {
            LOG_WARN("failed to add aggregate projector", K(ret), K(cell_idx), K(column_name));
          } else if (is_index_scan_ && !is_index_back_ && OB_ISNULL(index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true;
          }
        } else if (ObTableUtils::has_exist_in_columns(select_columns, col_info->column_name_)
            || (is_ttl_table_ && has_exist_in_array(ttl_columns, col_info->column_name_))) {
          if (OB_FAIL(select_col_ids_.push_back(col_info->column_id_))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (is_index_scan_ && !is_index_back_ && OB_ISNULL(index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true;
          }
        }
      }  // end for

      if (OB_SUCC(ret)) {
        if (OB_FAIL(init_scan_index_info())) {
          LOG_WARN("fail to scan index info", K(ret));
        } else if (!select_all_columns) {
          // query_col_ids_ is user query order
          for (int64_t i = 0; OB_SUCC(ret) && i < select_columns.count(); i++) {
            const ObTableColumnInfo *col_info;
            if (OB_FAIL(schema_cache_guard_->get_column_info(select_columns.at(i), col_info))) {
              LOG_WARN("get column info error", K(ret), K(select_columns.at(i)));
            } else if (OB_ISNULL(col_info)) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("column info not found in schema", K(ret), K(select_columns.at(i)));
            } else if (OB_FAIL(query_col_ids_.push_back(col_info->column_id_))) {
              LOG_WARN("fail to push back column id", K(ret), K(col_info->column_id_));
            } else if (OB_FAIL(query_col_names_.push_back(col_info->column_name_))) {
              LOG_WARN("fail to push back column name", K(ret), K(col_info->column_name_));
            }
          }
        }
      }
    }
  }

  return ret;
}

/*
  init index info for scan op:
    - primary table index info
    - index scan: add global index and local index info
*/
int ObTableCtx::init_scan_index_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_primary_index_info())) {
    LOG_WARN("fail to init primary table index info", K(ret));
  } else if (is_index_scan_) {
    ObTableIndexInfo index_info;
    if (OB_ISNULL(index_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is NULL", K(ret));
    } else {
      index_info.data_table_id_ = ref_table_id_;
      index_info.index_table_id_ = index_table_id_;
      index_info.index_schema_ = index_schema_;
      if (OB_FAIL(table_index_info_.push_back(index_info))) {
        LOG_WARN("fail to push back index info", K(ret));
      }
    }
  }

  return ret;
}

/*
  add index info for dml op:
    - add primary table index info, and its related_index_ids_ contain local index table ids;
    - if has global index, add global index info;
*/
int ObTableCtx::init_dml_index_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_primary_index_info())) {
    LOG_WARN("fail to init primary table index info", K(ret));
  } else {
    if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema cache guard is NULL or not inited", K(ret));
    } else if (has_global_index_) {
      const ObIArray<uint64_t> &global_index_tids = schema_cache_guard_->get_global_index_tids();
      const ObTableSchema *index_schema = nullptr;
      ObTableIndexInfo index_info;
      bool is_exist = false;
      for (int64_t i = 0; i < global_index_tids.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, global_index_tids.at(i), index_schema))) {
          LOG_WARN("fail to get index schema", K(ret));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema is NULL", K(ret));
        } else if (OB_FAIL(check_if_can_skip_update_index(index_schema, is_exist))) {
          LOG_WARN("fail to check skip build index info", K(ret), K(i), K(is_exist));
        } else if (is_exist) {
          // skip build index info
        } else {
          index_info.data_table_id_ = ref_table_id_;
          index_info.index_table_id_ = global_index_tids.at(i);
          index_info.index_schema_ = index_schema;
          if (OB_FAIL(table_index_info_.push_back(index_info))) {
            LOG_WARN("fail to push back index info", K(ret));
          }
        }
      } // end for
    }
  }

  return ret;
}

int ObTableCtx::check_if_can_skip_update_index(const ObTableSchema *index_schema, bool &can_skip)
{
  int ret = OB_SUCCESS;
  can_skip = false;
  if (operation_type_ == ObTableOperationType::UPDATE) {
    bool found = false;
    bool is_full_index = false;
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is NULL", K(ret));
    } else {
      is_full_index = index_schema->is_fts_index();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count() && !found; i++) {
      const ObTableAssignment &assign = assigns_.at(i);
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is null", K(ret), K(assign));
      } else if (is_full_index) {
        // if assigins has fts col, we can not skip fts index schema
        // and columns in fts index schema may be not contained in assigns,
        // such as rowkey-doc and doc-rowkey table
        found = assign.column_info_->is_fulltext_column();
      } else if (OB_NOT_NULL(index_schema->get_column_schema(assign.column_info_->column_id_))) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      can_skip = true;
    }
  }
  return ret;
}

// add primary index info
int ObTableCtx::init_primary_index_info()
{
  int ret = OB_SUCCESS;
  ObTableIndexInfo index_info;
  index_info.data_table_id_ = ref_table_id_;
  index_info.index_table_id_ = ref_table_id_;
  // index_schema_ now is null, when use, need get_table_schema!
  index_info.index_schema_ = nullptr;
  index_info.is_primary_index_ = true;
  ObIArray<common::ObTableID> &related_tids = index_info.related_index_ids_;
  if (need_related_table_id()) {
    if (OB_FAIL(init_dml_related_tid(related_tids))) {
      LOG_WARN("fail to init data table related tids", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(table_index_info_.push_back(index_info))) {
    LOG_WARN("fail to push back primary index info", K(ret));
  }
  return ret;
}

int ObTableCtx::init_insert()
{
  return init_dml_index_info();
}

int ObTableCtx::init_put(bool allow_insup)
{
  int ret = OB_SUCCESS;

  if (ObTableOperationType::PUT != operation_type_ &&
      (allow_insup && ObTableOperationType::INSERT_OR_UPDATE != operation_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid operation type use put");
    LOG_WARN("invalid operation type", K(ret), K_(operation_type));
  } else if (OB_FAIL(init_dml_index_info())) {
    LOG_WARN("fail to init index info for put", K(ret));
  } else if (has_secondary_index()) { // has index, can not use put
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with index use put");
    LOG_WARN("table with index use put is not supported", K(ret));
  } else if (has_lob_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with lob column use put");
    LOG_WARN("table with lob column use put is not supported", K(ret));
  } else if (is_total_quantity_log() && !is_htable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "binlog_row_image is full use put");
    LOG_WARN("binlog_row_image is full use put is not supported", K(ret));
  } else {
    is_client_set_put_ = true;
  }

  return ret;
}

/*
  stored generate column should be updated when reference column was updated.
  such as:
    - create table t(
      `c1` int primary key,
      `c2` varchar(20),
      `c3` varchar(20) generated always as (substring(`c2`, 1, 4) stored));
    - insert into t(`c1`, `c2`) values(1, 'hello');
    - update t set `c2`='world' where `c1`=1;
    c3 should be update.
  1. match stored generated column by column_id, generated column record cascaded_column_ids_.
    c1-16 c2-17 c3-18
    cascaded_column_ids_-(17)
  2. add new ObTableAssignment.
*/
int ObTableCtx::add_generated_column_assignment()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not init", K(ret));
  } else {
    const ObIArray<ObTableColumnInfo *> &col_info_array = schema_cache_guard_->get_column_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      ObTableColumnInfo *col_info =nullptr;
      if (OB_ISNULL(col_info = col_info_array.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (col_info->is_generated_column()) {
        bool match = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < col_info->cascaded_column_ids_.count() && !match; j++) {
          const uint64_t column_id = col_info->cascaded_column_ids_.at(j);
          const ObTableAssignment *assign = nullptr;
          if (OB_FAIL(get_assignment_by_column_id(column_id, assign))) {
            LOG_WARN("fail to get assignment", K(ret), K(column_id));
          } else if (OB_NOT_NULL(assign)) {
            match = true;
          }
        }
        if (OB_SUCC(ret) && match) {
          ObTableAssignment tmp_assign(col_info);
          if (OB_FAIL(assigns_.push_back(tmp_assign))) {
            LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(tmp_assign));
          }
        } // end if
      }
    }
  }
  return ret;
}

/*
  init assignments when do update or insertUp.
    1. user assignment should be add.
    2. stored generate column should be add when reference column is updated.
      such as:
        - create table t(
          `c1` int primary key,
          `c2` varchar(20),
          `c3` varchar(20) generated always as (substring(`c2`, 1, 4) stored));
        - insert into t(`c1`, `c2`) values(1, 'hello');
        - update t set `c2`='world' where `c1`=1;
        c3 should be update.
    3. on update current timestamp should be add.
*/
int ObTableCtx::init_assignments(const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObObj prop_obj;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not init", K(ret));
  } else {
    const ObIArray<ObTableColumnInfo *> &column_info_array = schema_cache_guard_->get_column_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_info_array.count(); i++) {
      ObTableColumnInfo *col_info =nullptr;
      if (OB_ISNULL(col_info = column_info_array.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (OB_SUCCESS == entity.get_property(col_info->column_name_, prop_obj)) {
        if (col_info->rowkey_position_ > 0) {
          ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_UPDATE_ROWKEY_COLUMN);
          LOG_WARN("can not update rowkey column", K(ret));
        } else {
          ObTableAssignment assign(col_info);
          assign.assign_value_ = prop_obj; // shadow copy when prop_obj is string type
          assign.is_assigned_ = true;
          if (OB_FAIL(assigns_.push_back(assign))) {
            LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(assign));
          }
        }
      } else if (col_info->auto_filled_timestamp_) { // on update current timestamp
        ObTableAssignment assign(col_info);
        if (OB_FAIL(assigns_.push_back(assign))) {
          LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(assign));
        }
      }
    }

    if (OB_SUCC(ret) && has_generated_column_ && OB_FAIL(add_generated_column_assignment())) {
      LOG_WARN("fail to add generated column assignment", K(ret));
    }
  }

  if (assigns_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty assignment");
    LOG_WARN("empty assignment is not supported", K(ret), K_(operation_type));
  }

  return ret;
}

/*
  init scan parameters.
    1. init assignments
    2. init scan parameters for child executor(scan executor) of update executor.
    3. init related index table ids, cause we need to write index table as well.
*/
int ObTableCtx::init_update()
{
  int ret = OB_SUCCESS;
  is_for_update_ = true;

  // 1. init assignments
  if (OB_FAIL(init_assignments(static_cast<const ObTableEntity&>(*entity_)))) {
    LOG_WARN("fail to init assignments", K(ret), K(*entity_));
  } else {
    // 2. init scan
    index_table_id_ = ref_table_id_;
    is_index_scan_ = false;
    is_index_back_ = false;
    is_get_ = true;
    scan_order_ = ObQueryFlag::Forward;
    if (OB_FAIL(schema_cache_guard_->get_column_ids(select_col_ids_))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else if (OB_FAIL(init_dml_index_info())) { // 3. init index info, include related_index_ids
      LOG_WARN("fail to init index info for update", K(ret));
    }
  }

  return ret;
}

/*
  init delete parameters.
    1. init scan parameters for child executor(scan executor) of delete executor.
      - in htable, we should not query stored generate column.
        create table if not exists htable1_cf1 (
          `K` varbinary(1024),
          `Q` varbinary(256),
          `T` bigint,
          `V` varbinary(1024),
          `K_PREFIX` varbinary(1024) GENERATED ALWAYS AS (substr(`K`,1,32)) STORED, primary key(`K`, `Q`, `T`));
        `K_PREFIX` should not be query.
      - htable use query and delete, is_get_ should not be set.
    2. init related index table ids, cause we need to delete index table as well.
*/
int ObTableCtx::init_delete()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not init", K(ret));
  } else {
    // 1. init scan
    int64_t column_num;
    if (OB_FAIL(schema_cache_guard_->get_column_count(column_num))) {
      LOG_WARN("get column count failed", K(ret));
    } else {
      index_table_id_ = ref_table_id_;
      is_index_scan_ = false;
      is_index_back_ = false;
      is_get_ = is_htable() ? false : true;
      scan_order_ = ObQueryFlag::Forward;
      // 2. init select_col_ids_
      const ObTableColumnInfo *col_info = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_num; i++) {
        if (OB_FAIL(schema_cache_guard_->get_column_info_by_idx(i, col_info))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("fail to get column schema bu idx", K(ret), K(i));
        } else if (col_info->is_virtual_generated_column() && !col_info->is_doc_id_column()) {
          // skip
        } else if (OB_FAIL(select_col_ids_.push_back(col_info->column_id_))) {
          LOG_WARN("fail to push back column id", K(ret), K(col_info->column_id_));
        } else if (OB_FAIL(query_col_names_.push_back(col_info->column_name_))) {
          LOG_WARN("fail to push back column name", K(ret));
        }
      }
    }
    // 3. init related index table id
    if (OB_SUCC(ret) && OB_FAIL(init_dml_index_info())) {
      LOG_WARN("fail to init index info ids", K(ret));
    }
  }

  return ret;
}

int ObTableCtx::init_ttl_delete(const ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  set_is_ttl_table(false);
  ObTableQuery query;
  ObIArray<ObNewRange> &query_scan_ranges = query.get_scan_ranges();
  if (OB_FAIL(query_scan_ranges.assign(scan_ranges))) {
    LOG_WARN("fail to assign scan ranges", KR(ret), K(query_scan_ranges));
  } else if (OB_NOT_NULL(fts_ctx_)) {
    fts_ctx_->need_tsc_with_doc_id_ = true;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_scan(query, false/* is_weak_read */, index_table_id_))) {
    LOG_WARN("fail to init scan ctx", KR(ret), K(query));
  } else {
    // ttl delete task need global snapshot
    has_global_index_ = flags_.has_global_index_;
  }

  return ret;
}

/*
  init replace parameters.
    - init related index table ids, cause we need to replace index table as well.
*/
int ObTableCtx::init_replace()
{
  return init_dml_index_info();
}

/*
  init insertUp parameters.
    1. init update
    2. reset for is_for_update_ flag, cause init_update() had set is_for_update_=true
*/
int ObTableCtx::init_insert_up(bool is_client_set_put)
{
  int ret = OB_SUCCESS;
  is_for_insertup_ = true;
  is_client_set_put_ = is_client_set_put;

  if (OB_FAIL(init_update())) {
    LOG_WARN("fail to init update", K(ret));
  }

  // reset for update flag
  is_for_update_ = false;
  return ret;
}

/*
  init get parameters, get operation use scan executor.
    1. init scan parameters
    2. get all column when user not filled select columns.
*/
int ObTableCtx::init_get()
{
  int ret = OB_SUCCESS;
  // init scan
  index_table_id_ = ref_table_id_;
  is_index_scan_ = false;
  is_index_back_ = false;
  is_get_ = true;
  scan_order_ = ObQueryFlag::Forward;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not init", K(ret));
  } else if (OB_FAIL(init_primary_index_info())) {
    LOG_WARN("fail to init primary index info", K(ret));
  } else if (OB_FAIL(entity_->get_properties_names(query_col_names_))) {
    LOG_WARN("fail to get entity properties names", K(ret));
  } else {
    const bool need_get_all_column = query_col_names_.empty(); // 未设置get的列，默认返回全部列
    // init select_col_ids, query_col_names_
    const ObIArray<ObTableColumnInfo *>& col_info_array = schema_cache_guard_->get_column_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      ObTableColumnInfo *col_info = col_info_array.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (col_info->is_fulltext_column()) {
        // skip
      } else if (OB_FAIL(select_col_ids_.push_back(col_info->column_id_))) {
        LOG_WARN("fail to add column id", K(ret));
      } else if (need_get_all_column
          && OB_FAIL(query_col_names_.push_back(col_info->column_name_))) {
        LOG_WARN("fail to push back column name", K(ret));
      }
    }
  }
  return ret;
}

/*
  init append parameters, append operation use insertUp executor.
    1. init return_affected_entity_ and return_rowkey_
    2. init insertUp, cause append operation use insertUp executor.
    3. construct generated_expr_str, cause append column use stored to generate columns for functionality.
      - expr string: concat_ws('', `%s`, `%s`), `%s` mean column name
      3.1 no record in database
        do: append(c1, "abc")
        expr: concat_ws('', `c1`, `c1-delta`)
        result: "abc", cause c1 is null
      3.1 "abc" in database
        do: append(c1, "efg")
        expr: concat_ws('', `c1`, `c1-delta`)
        result: "abcefg"
*/
int ObTableCtx::init_append(bool return_affected_entity, bool return_rowkey)
{
  int ret = OB_SUCCESS;
  return_affected_entity_ = return_affected_entity;
  return_rowkey_ = return_rowkey;

  if (OB_FAIL(init_update())) {
    LOG_WARN("fail to init insert up", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count(); i++) {
      ObTableAssignment &assign = assigns_.at(i);
      const ObObj &delta = assign.assign_value_;
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret), K(assign));
      } else if (assign.column_info_->auto_filled_timestamp_ || assign.column_info_->is_generated_column()) {
        // do nothing
      } else if (delta.is_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "append null");
        LOG_WARN("append NULL is illegal", K(ret), K(delta));
      } else if (OB_UNLIKELY(!ob_is_string_type(assign.column_info_->type_.get_type()))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-string types for append operation");
        LOG_WARN("invalid type for append", K(ret), K(assign));
      } else if (OB_UNLIKELY(!ob_is_string_type(delta.get_type()))) {
        ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
        const char *schema_type_str = "stringTc";
        const char *obj_type_str = ob_obj_type_str(delta.get_type());
        LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH, assign.column_info_->column_name_.length(), assign.column_info_->column_name_.ptr(),
            static_cast<int>(strlen(schema_type_str)), schema_type_str, static_cast<int>(strlen(obj_type_str)), obj_type_str);
        LOG_WARN("can only append string type", K(ret), K(delta));
      } else {
        const ObString &column_name = assign.column_info_->column_name_;
        const int64_t total_len = strlen("concat_ws('', ``, ``)") + 1
            + column_name.length() + column_name.length();
        int64_t actual_len = -1;
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(total_len));
        } else if ((actual_len = snprintf(buf, total_len, "concat_ws('', `%s`, `%s`)",
            column_name.ptr(), column_name.ptr())) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to construct concat_ws expr string", K(ret), K(total_len), K(delta));
        } else {
          ObString generated_expr_str(actual_len, buf);
          assign.generated_expr_str_ = generated_expr_str;
          assign.is_inc_or_append_ = true;
        }
      }
    }
  }

  return ret;
}

/*
  init increment parameters, append operation use insertUp executor.
    1. init return_affected_entity_ and return_rowkey_
    2. init insertUp, cause increment operation use insertUp executor.
    3. construct generated_expr_str, cause increment column use stored to generate columns for functionality.
      - expr string: IFNULL(`%s`, 0) + `%s`, `%s` mean column name
      3.1 no record in database
        do: increment(c1, 1)
        expr: IFNULL(`c1`, 0) + `c1_delta`
        result: 1, cause c1 is null
      3.1 1 in database
        do: increment(c1, 1)
        expr: IFNULL(`c1`, 0) + `c1_delta`
        result: 2, cause c1 is null
*/
int ObTableCtx::init_increment(bool return_affected_entity, bool return_rowkey)
{
  int ret = OB_SUCCESS;
  return_affected_entity_ = return_affected_entity;
  return_rowkey_ = return_rowkey;

  if (OB_FAIL(init_update())) {
    LOG_WARN("fail to init insert up", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count(); i++) {
      ObTableAssignment &assign = assigns_.at(i);
      const ObObj &delta = assign.assign_value_;
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret), K(assign));
      } else if (assign.column_info_->is_auto_increment_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "increment auto increment column");
        LOG_WARN("not support increment auto increment column", K(ret), K(assign));
      } else if (assign.column_info_->auto_filled_timestamp_) {
        // do nothing
      } else if (OB_UNLIKELY(!ob_is_int_tc(assign.column_info_->type_.get_type()) &&
                !ob_is_varbinary_type(assign.column_info_->type_.get_type(),
                                      assign.column_info_->type_.get_collation_type()))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-integer or varbinary types for increment operation");
        LOG_WARN("invalid type for increment", K(ret), K(assign));
      } else {
        const ObString &column_name = assign.column_info_->column_name_;
        int64_t total_len = 0;
        ObString format_str;
        if (ob_is_varbinary_type(assign.column_info_->type_.get_type(), assign.column_info_->type_.get_collation_type())
            && ob_is_varbinary_type(delta.get_type(), delta.get_collation_type())) {
          // for Redis varbinary increment
          total_len = strlen("trim(trailing '.' from trim(trailing '0' from IFNULL(CAST(`` AS DECIMAL(65,17)), 0) + CAST(`` AS DECIMAL(65,17))))")
                          + 1 + column_name.length() + column_name.length();
          format_str = "trim(trailing '.' from trim(trailing '0' from IFNULL(CAST(`%s` AS DECIMAL(65,17)), 0) + CAST(`%s` AS DECIMAL(65,17))))";
        } else if (ob_is_int_tc(assign.column_info_->type_.get_type()) && ob_is_int_tc(delta.get_type())) {
          total_len = strlen("IFNULL(``, 0) + ``") + 1
            + column_name.length() + column_name.length();
          format_str = "IFNULL(`%s`, 0) + `%s`";
        } else {
          ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
          const char *schema_type_str = ob_obj_type_str(assign.column_info_->type_.get_type());
          const char *obj_type_str = ob_obj_type_str(delta.get_type());
          LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH, column_name.length(), column_name.ptr(),
              static_cast<int>(strlen(schema_type_str)), schema_type_str, static_cast<int>(strlen(obj_type_str)), obj_type_str);
          LOG_WARN("delta should only be signed integer type or varbinary", K(ret), K(delta));
        }
        int64_t actual_len = -1;
        char *buf = NULL;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(total_len));
        } else if ((actual_len = snprintf(buf, total_len, format_str.ptr(),
            column_name.ptr(), column_name.ptr())) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to construct increment expr string", K(ret), K(total_len), K(delta));
        } else {
          ObString generated_expr_str(actual_len, buf);
          assign.generated_expr_str_ = generated_expr_str;
          assign.is_inc_or_append_ = true;
        }
      }
    }
  }

  return ret;
}

/*
  classify scan expr for get/delete/update/scan operation
*/
int ObTableCtx::classify_scan_exprs()
{
  int ret = OB_SUCCESS;
  if (!select_exprs_.empty()) {
    // had classify, do nothing
  } else {
    ObSEArray<uint64_t, 8> rowkey_column_ids;
    // for select exprs, its order is from user input
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
      const ObTableColumnItem &tmp_item = column_items_.at(i);
      const ObTableColumnInfo *col_info = nullptr;
      if (OB_ISNULL(col_info = tmp_item.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col info is NULL", K(ret), K(i));
      } else if (has_exist_in_array(select_col_ids_, col_info->column_id_)) {
        if (OB_ISNULL(tmp_item.expr_)) {
          // use column ref exprs, cause select exprs no need to calculate
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column ref expr is NULL", K(ret), K(i));
        } else if (OB_FAIL(select_exprs_.push_back(tmp_item.expr_))) {
          LOG_WARN("fail to push back select expr", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(table_schema_->get_rowkey_column_ids(rowkey_column_ids))) {
      LOG_WARN("fail to get rowkey column ids", K(ret));
    }
    // for rowkey exprs: its order is schema define, case like:
    // create table (c1 int, c2 int, c3 int, primary key(c2, c1)), rowkey_exprs store order must be [c2, c1]
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); i++) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(get_column_item_by_column_id(rowkey_column_ids.at(i), item))) {
        LOG_WARN("fail to get column item", K(ret), K(rowkey_column_ids), K(i));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret));
      } else if (OB_FAIL(rowkey_exprs_.push_back(item->raw_expr_))) {
        LOG_WARN("fail to push back rowkey expr", K(ret), K(i));
      }
    }

    // for index exprs, its order is schema define
    if (OB_SUCC(ret) && is_index_scan_ && !is_text_retrieval_scan()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_col_ids_.count(); i++) {
        const ObTableColumnItem *item = nullptr;
        if (OB_FAIL(get_column_item_by_column_id(index_col_ids_.at(i), item))) {
          LOG_WARN("fail to get column item", K(ret), K(index_col_ids_), K(i));
        } else if (OB_ISNULL(item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item is null", K(ret));
        } else if (OB_FAIL(index_exprs_.push_back(item->raw_expr_))) {
          LOG_WARN("fail to push back index expr", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_fts_schema()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_) || OB_ISNULL(fts_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema or fts ctx is NULL", K(ret), KP(fts_ctx_));
  } else if (OB_FAIL(table_schema_->get_rowkey_doc_tid(fts_ctx_->rowkey_doc_tid_))) {
    LOG_WARN("failed to get rowkey doc tid", K(ret), KPC(table_schema_));
  } else if (OB_FAIL(table_schema_->get_doc_id_rowkey_tid(fts_ctx_->doc_rowkey_tid_))) {
    LOG_WARN("failed to get rowkey doc tid", K(ret), KPC(table_schema_));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is nullptr", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, fts_ctx_->rowkey_doc_tid_, fts_ctx_->rowkey_doc_schema_))) {
    LOG_WARN("fail to get rowkey doc table schema", K(ret), K(fts_ctx_->rowkey_doc_tid_));
  } else if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, fts_ctx_->doc_rowkey_tid_, fts_ctx_->doc_rowkey_schema_))) {
    LOG_WARN("fail to get doc rowkey table schema", K(ret), K(fts_ctx_->doc_rowkey_tid_));
  }
  return ret;
}

int ObTableCtx::init_exec_ctx()
{
  int ret = OB_SUCCESS;
  /*
  1. calc_partitio_and_tablet_id need sql_ctx
  2. in expr: calc_partition_id need sql_ctx.schema_guard_ to get_das_tablet_mapper
  */
  sql_ctx_.schema_guard_ = schema_guard_;
  exec_ctx_.get_das_ctx().set_sql_ctx(&sql_ctx_);
  if (OB_FAIL(init_das_context(exec_ctx_.get_das_ctx()))) {
    LOG_WARN("fail to init das context", K(ret));
  } else {
    // init exec_ctx_.my_session_
    exec_ctx_.set_my_session(&get_session_info());
  }
  return ret;
}

/*
  init the das context:
    - generate table loc for all the index table and add to das context
    - generate the local tablet loc and add to das context,
      "local tablet loc" means tablet id has been calculated and is in local observer
      - for dml, get and primary index scan: primary table;
      - for global/local index scan: index table;
*/
int ObTableCtx::init_das_context(ObDASCtx &das_ctx)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *table_loc = nullptr;
  ObDASTableLoc *local_table_loc = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  if (table_index_info_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table index info is empty", K(ret));
  }

  for (int64_t i = 0; i < table_index_info_.count() && OB_SUCC(ret); i++) {
    ObTableIndexInfo &index_info = table_index_info_.at(i);
    const ObSimpleTableSchemaV2 *index_schema = index_info.is_primary_index_ ? simple_table_schema_ : index_info.index_schema_;
    ObIArray<ObTableID> *related_index_tids = index_info.is_primary_index_ ? &index_info.related_index_ids_ : nullptr;
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is NULL", K(ret));
    } else if (OB_ISNULL(index_info.loc_meta_ = OB_NEWx(ObDASTableLocMeta, (&allocator_), allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc loc_meta", K(ret));
    } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(*this,
                                                                    *index_schema,
                                                                    *index_info.loc_meta_,
                                                                    related_index_tids))) {
      LOG_WARN("fail to generate table location meta", K(ret));
    } else if (OB_FAIL(exec_ctx_.get_das_ctx().extended_table_loc(*index_info.loc_meta_, table_loc))) {
      LOG_WARN("fail to extend table loc", K(ret), K(*index_info.loc_meta_));
    } else if (index_info.is_primary_index_) {
      if (need_related_table_id() && OB_FAIL(init_related_tablet_map(das_ctx))) {
        LOG_WARN("fail to init related tablet map", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
   if (OB_ISNULL(local_table_loc = exec_ctx_.get_das_ctx().get_table_loc_by_id(ref_table_id_, index_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local_table_loc is NULL", K(ret));
   } else if (OB_FAIL(exec_ctx_.get_das_ctx().extended_tablet_loc(*local_table_loc,
                                                                  index_tablet_id_,
                                                                  tablet_loc))) {
      LOG_WARN("fail to extend tablet loc", K(ret), K(index_tablet_id_));
    }
  }

  return ret;
}

int ObTableCtx::init_related_tablet_map(ObDASCtx &das_ctx)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_ID;
  int64_t subpart_idx = OB_INVALID_ID;
  if (OB_ISNULL(simple_table_schema_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null", K(ret));
  } else if (simple_table_schema_->is_partitioned_table() &&
      OB_FAIL(simple_table_schema_->get_part_idx_by_tablet(tablet_id_, part_idx, subpart_idx))) {
    LOG_WARN("fail to get part idx by tablet", K(ret), K_(tablet_id));
  } else {
    DASRelatedTabletMap &related_tablet_map = das_ctx.get_related_tablet_map();
    ObTableIndexInfo &primary_index_info = get_primary_index_info();
    ObIArray<common::ObTableID> &related_index_ids = primary_index_info.related_index_ids_;
    for (int64_t i = 0; OB_SUCC(ret) && i < related_index_ids.count(); i++) {
      ObTableID related_table_id = related_index_ids.at(i);
      const ObSimpleTableSchemaV2 *relative_table_schema = nullptr;
      ObObjectID related_part_id = OB_INVALID_ID;
      ObObjectID related_first_level_part_id = OB_INVALID_ID;
      ObTabletID related_tablet_id;
      if (OB_FAIL(schema_guard_->get_simple_table_schema(tenant_id_,
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
    } // end for
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
    OZ (exec_ctx_.get_das_ctx().set_snapshot(tx_snapshot));
  }

  return ret;
}

// for query
int ObTableCtx::init_index_info(const ObString &index_name, const uint64_t arg_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE];
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;

  if (OB_FAIL(schema_guard_->get_can_read_index_array(tenant_id_,
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
      if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, tids[i], index_schema))) {
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
        is_global_index_scan_ = index_schema->is_global_index_table();
        if (index_schema->is_fts_index_aux()) {
          if (OB_ISNULL(fts_ctx_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ftx_ctx is NULL", K(ret));
          } else {
            fts_ctx_->is_text_retrieval_scan_ = true;
            is_index_back_ = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!is_global_index_scan_ && OB_FAIL(get_related_tablet_id(*index_schema, index_tablet_id_))) {
          LOG_WARN("fail to get index tablet id", K(ret));
        } else if (is_global_index_scan_) {
          if (index_table_id_ != arg_table_id) {
            /*
              check the route info if is valid when use global index to scan, for the reason that:
              when scan with global index, the client should send the request using global index route,
              meanings the arg_table_id should be represented global index table. Also, arg_table_name
              is always primary table name.
            */
            ret = OB_ERR_KV_GLOBAL_INDEX_ROUTE;
            LOG_WARN("the route info is incorrect for global index scan", K(ret), K(index_table_id_), K(arg_table_id));
          } else if (!index_tablet_id_.is_valid()) {
            if (!index_schema_->is_partitioned_table()) {
              index_tablet_id_ = index_schema_->get_tablet_id();
            } else {
              // trigger client to refresh table entry
              // maybe drop a non-partitioned table and create a
              // partitioned table with same name
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("partitioned table should pass right tablet id from client", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && !is_found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index name", K(ret), K(index_name));
    }
  }

  return ret;
}

int ObTableCtx::init_dml_related_tid(ObIArray<common::ObTableID> &related_index_tids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", K(ret));
  } else {
    const ObIArray<uint64_t> &local_index_tids = schema_cache_guard_->get_local_index_tids();
    for (int64_t i = 0; OB_SUCC(ret) && i < local_index_tids.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, local_index_tids.at(i), index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(local_index_tids.at(i)), K(i));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("null index schema", K(ret));
      } else if (index_schema->is_index_local_storage()) {
        bool can_skip = false;
        if (OB_FAIL(check_if_can_skip_update_index(index_schema, can_skip))) {
          LOG_WARN("fail to check assign column exist in index table", K(ret));
        } else if (can_skip) {
          // do nothing
        } else if (OB_FAIL(related_index_tids.push_back(index_schema->get_table_id()))) {
          LOG_WARN("fail to add related index ids", K(ret), K(index_schema->get_table_id()));
        } else if (has_fts_index_ && index_schema->is_rowkey_doc_id()) {
          if (OB_ISNULL(fts_ctx_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fts_ctx is NULL", K(ret));
          } else {
            fts_ctx_->need_tsc_with_doc_id_ = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::init_agg_cell_proj(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(agg_cell_proj_.prepare_allocate(size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size));
  } else {
    for (int64_t i = 0; i < agg_cell_proj_.count(); i++) {
      agg_cell_proj_.at(i) = ObTableAggCalculator::INVALID_PROJECT_ID;
    }
  }
  return ret;
}

int ObTableCtx::add_aggregate_proj(int64_t cell_idx,
                                   const ObString &column_name,
                                   const ObIArray<ObTableAggregation> &aggregations)
{
  int ret = OB_SUCCESS;
  if (aggregations.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null aggregations", K(ret));
  } else {
    for (int64_t i = 0; i < aggregations.count(); i++) {
      if (aggregations.at(i).get_column().case_compare(column_name) == 0
          || aggregations.at(i).is_agg_all_column()) {
        agg_cell_proj_.at(i) = cell_idx;
      }
    }
  }
  return ret;
}

/*
  add auto increment parameters
  - each auto increment column contains a param
  - only support one auto increment column in mysql mode
*/
int ObTableCtx::add_auto_inc_param()
{
  int ret = OB_SUCCESS;
  ObIArray<AutoincParam> &auto_params = exec_ctx_.get_physical_plan_ctx()->get_autoinc_params();
  int64_t auto_increment_cache_size = -1;
  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not init", K(ret));
  } else if (OB_FAIL(get_session_info().get_auto_increment_cache_size(auto_increment_cache_size))) {
    LOG_WARN("fail to get increment factor", K(ret));
  } else {
    const ObIArray<ObTableColumnInfo *>& col_info_array = schema_cache_guard_->get_column_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      ObTableColumnInfo *col_info = col_info_array.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (col_info->is_auto_increment_) {
        if (!auto_params.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected auto params count, should be empty", K(ret));
        } else {
          AutoincParam param;
          param.tenant_id_ = tenant_id_;
          param.autoinc_table_id_ = simple_table_schema_->get_table_id();
          param.autoinc_first_part_num_ = simple_table_schema_->get_first_part_num();
          param.autoinc_table_part_num_ = simple_table_schema_->get_all_part_num();
          param.autoinc_col_id_ = col_info->column_id_;
          param.auto_increment_cache_size_ = get_auto_increment_cache_size(
            schema_cache_guard_->get_auto_inc_cache_size(), auto_increment_cache_size);
          param.part_level_ = simple_table_schema_->get_part_level();
          ObObjType column_type = col_info->type_.get_type();
          param.autoinc_col_type_ = column_type;
          param.autoinc_desired_count_ = 0;
          param.autoinc_mode_is_order_ = simple_table_schema_->is_order_auto_increment_mode();
          param.autoinc_version_ = simple_table_schema_->get_truncate_version();
          param.total_value_count_ = 1;
          param.autoinc_increment_ = 1;
          param.autoinc_offset_ = 1;
          param.part_value_no_order_ = true;
          if (col_info->is_tbl_part_key_column_) {
            // don't keep intra-partition value asc order when partkey column is auto inc
            param.part_value_no_order_ = true;
          }
          if (OB_FAIL(auto_params.prepare_allocate(1))) { // only one auto inc in a table
            LOG_WARN("fail to init auto params", K(ret));
          } else {
            auto_params.at(0) = param;
            has_auto_inc_ = true;
          }
        }
      }
    }
  }

  return ret;
}

/*
  update auto increment local and gobal value.
  - we need to update local and gobal value after user set specific value.
*/
int ObTableCtx::update_auto_inc_value()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(phy_plan_ctx_.sync_last_value_local())) {
    LOG_WARN("fail to sync last value local", K(ret));
  } else if (OB_FAIL(phy_plan_ctx_.sync_last_value_global())) {
    LOG_WARN("fail to sync last value global", K(ret));
  }
  return ret;
}

// get index table's tablet id
int ObTableCtx::get_related_tablet_id(const share::schema::ObTableSchema &index_schema,
                                      ObTabletID &related_tablet_id)
{
  int ret = OB_SUCCESS;

  if (!index_schema.is_partitioned_table()) {
    related_tablet_id = index_schema.get_tablet_id();
  } else {
    int64_t part_idx = OB_INVALID_ID;
    int64_t subpart_idx = OB_INVALID_ID;
    ObObjectID related_part_id = OB_INVALID_ID;
    ObObjectID related_first_level_part_id = OB_INVALID_ID;
    ObTabletID tmp_tablet_id;
    // 先从主表获取part_idx和subpart_idx，索引表的part_idx和subpart_idx是和主表一致的
    if (OB_ISNULL(simple_table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(simple_table_schema_->get_part_idx_by_tablet(tablet_id_, part_idx, subpart_idx))) {
      LOG_WARN("fail to get part idx", K(ret), K_(tablet_id));
    } else if (OB_FAIL(index_schema.get_part_id_and_tablet_id_by_idx(part_idx,
                                                                     subpart_idx,
                                                                     related_part_id,
                                                                     related_first_level_part_id,
                                                                     tmp_tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret), K(part_idx), K(subpart_idx));
    } else {
      related_tablet_id = tmp_tablet_id;
    }
  }

  return ret;
}

/*
  check insert up operation can use put implement or not
  1. can not have any index.
  2. all column must be filled.
  3. ob-hbase use put cause CDC has supported
  4. tableapi with full binlog image can not use put
*/
int ObTableCtx::check_insert_up_can_use_put(bool &use_put)
{
  int ret = OB_SUCCESS;
  if (ObTableOperationType::INSERT_OR_UPDATE != operation_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation type", K(ret), K_(operation_type));
  } else if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(check_insert_up_can_use_put(*schema_cache_guard_,
                                                 entity_,
                                                 is_client_set_put_,
                                                 is_htable(),
                                                 is_total_quantity_log(),
                                                 use_put))) {
    LOG_WARN("fail to check insert up if can use put", K(ret));
  }
  return ret;
}

int ObTableCtx::check_insert_up_can_use_put(ObKvSchemaCacheGuard &schema_cache_guard,
                                            const ObITableEntity *entity,
                                            bool is_client_set_put,
                                            bool is_htable,
                                            bool is_full_binlog_image,
                                            bool &use_put)
{
  int ret = OB_SUCCESS;
  use_put = false;
  bool can_use_put = true;
  ObTableSchemaFlags flags = schema_cache_guard.get_schema_flags();
  bool has_secondary_index = flags.has_local_index_ || flags.has_global_index_;
  bool has_lob_column = flags.has_lob_column_;
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is NULL", K(ret));
  } else if (is_client_set_put && has_secondary_index) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with index use put");
    LOG_WARN("client set use_put flag, but has local index is not support", K(ret));
  } else if (has_secondary_index) {
    /* has index, can not use put:
       for local/global index: insert a new row insert of covering the old row
       when the assign value of index column is new. */
    can_use_put = false;
  } else if (has_lob_column) {
    // has lob column cannot use put: may cause lob storeage leak when put row to lob meta table
    can_use_put = false;
  } else if (is_htable) { // htable has no index and alway full filled.
    can_use_put = true;
  } else {
    bool is_all_columns_filled = false;
    int64_t column_count;
    int64_t rowkey_column_num;
    if (OB_ISNULL(entity)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("entity is null", K(ret));
    } else if (OB_FAIL(schema_cache_guard.get_column_count(column_count))) {
      LOG_WARN("get column count failed", K(ret));
    } else if (OB_FAIL(schema_cache_guard.get_rowkey_column_num(rowkey_column_num))) {
      LOG_WARN("get rowkey column num failed", K(ret));
    } else if (FALSE_IT(is_all_columns_filled = column_count - rowkey_column_num <=
                entity->get_properties_count())) {  // all columns are filled
    } else if (is_client_set_put || is_all_columns_filled) {
      can_use_put = true;
    } else { // some columns are missing
      can_use_put = false;
    }
  }

  if (OB_SUCC(ret) && can_use_put) {
    use_put = true;
    if (!is_htable && is_full_binlog_image) { // tableapi with full binlog image can not use put
      use_put = false;
    }
  }
  return ret;
}

/*
  only for genarate spec or exprs, to generate full table_schema
*/
int ObTableCtx::generate_table_schema_for_cg()
{
  int ret = OB_SUCCESS;

  // generate table_schema
  if (OB_NOT_NULL(table_schema_)) {
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(simple_table_schema_->get_tenant_id(),
                                                    simple_table_schema_->get_table_id(),
                                                    table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(simple_table_schema_->get_table_id()));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(simple_table_schema_->get_tenant_id()), K(simple_table_schema_->get_table_id()));
  }

  // generate primary_index_schema
  if (OB_SUCC(ret)) {
    bool find_primary_index = false;
    for (int i = 0; i < table_index_info_.count() && !find_primary_index; ++i) {
      ObTableIndexInfo& index_info = table_index_info_.at(i);
      if (index_info.is_primary_index_) {
        find_primary_index = true;
        index_info.index_schema_ = table_schema_;
      }
    }
  }

  // for rowkey_doc_id scan in fulltext index
  if (OB_SUCC(ret) && (is_tsc_with_doc_id() || is_text_retrieval_scan())) {
    if (OB_FAIL(init_fts_schema())) {
      LOG_WARN("fail to init rowkey doc schema", K(ret));
    }
  }

  return ret;
}

int ObTableCtx::init_insert_when_inc_append()
{
  int ret = OB_SUCCESS;
  inc_append_stage_ = ObTableIncAppendStage::TABLE_INCR_APPEND_INSERT;
  // from init_update
  is_for_update_ = false;
  is_get_ = false;
  assigns_.reset();
  // from cons_column_items_for_cg
  column_items_.reset();
  // from generate_exprs
  all_exprs_.reuse();
  select_exprs_.reset();
  rowkey_exprs_.reset();
  index_exprs_.reset();
  filter_exprs_.reset();
  table_index_info_.reset();
  expr_info_ = nullptr;
  if (OB_FAIL(init_insert_up(is_client_set_put_))) {
    LOG_WARN("fail to init ttl insert", K(ret));
  }
  return ret;
}

int ObTableCtx::prepare_text_retrieval_scan()
{
  int ret = OB_SUCCESS;
  uint64_t doc_id_rowkey_tid = OB_INVALID_ID;
  uint64_t inv_index_tid = index_table_id_;
  uint64_t fwd_idx_tid = OB_INVALID_ID;
  ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ref_table_id_));
  } else if (OB_FAIL(table_schema_->get_simple_index_infos(index_infos))) {
    LOG_WARN("fail to get index infos", K(ret));
  } else if (OB_FAIL(table_schema_->get_doc_id_rowkey_tid(doc_id_rowkey_tid))) {
    LOG_WARN("fail to get doc_id rowkey index tid", K(ret));
  } else if (OB_ISNULL(index_schema_) || !index_schema_->is_fts_index_aux()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(index_table_id_), KP(index_schema_));
  } else {
    // find fwd index schema
    bool found_fwd_idx = false;
    const ObString &inv_idx_name = index_schema_->get_table_name_str();
    const ObTableSchema *fwd_idx_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      const ObAuxTableMetaInfo &index_info = index_infos.at(i);
      if (!share::schema::is_fts_doc_word_aux(index_info.index_type_)) {
        // skip
      } else if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, index_info.table_id_, fwd_idx_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(fwd_idx_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpecter nullptr to fwd idx schema", K(ret));
      } else {
        const ObString &fwd_idx_name = fwd_idx_schema->get_table_name_str();
        int64_t fwd_idx_suffix_len = strlen("_fts_doc_word");
        ObString fwd_idx_prefix_name;
        if (OB_UNLIKELY(fwd_idx_name.length() <= fwd_idx_suffix_len)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(fwd_idx_name), K(fwd_idx_suffix_len));
        } else if (OB_FALSE_IT(fwd_idx_prefix_name.assign_ptr(fwd_idx_name.ptr(),
                                                              fwd_idx_name.length() - fwd_idx_suffix_len))) {
        } else if (fwd_idx_prefix_name.compare(inv_idx_name) == 0) {
          found_fwd_idx = true;
          fwd_idx_tid = fwd_idx_schema->get_table_id();
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (!found_fwd_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find fwd index", K(ret), K(inv_idx_name), K(index_table_id_));
      } else if (OB_ISNULL(fts_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fts_ctx is NULL", K(ret));
      } else if (OB_ISNULL(fts_ctx_->text_retrieval_info_ = OB_NEWx(ObTextRetrievalInfo, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObTextRetrievalInfo", K(ret));
      } else {
        fts_ctx_->text_retrieval_info_->inv_idx_tid_ = index_table_id_;
        fts_ctx_->text_retrieval_info_->fwd_idx_tid_ = fwd_idx_tid;
        fts_ctx_->text_retrieval_info_->doc_id_idx_tid_ = doc_id_rowkey_tid;
        fts_ctx_->text_retrieval_info_->need_calc_relevance_ = true;
      }
    }
  }

  return ret;
}

const ObDASScanCtDef* ObTableApiScanCtDef::get_lookup_ctdef() const
{
  ObDASScanCtDef *lookup_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_spec_.attach_ctdef_;
  if (nullptr == attach_ctdef) {
    lookup_ctdef = lookup_ctdef_;
  } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (DAS_OP_TABLE_SCAN == attach_ctdef->children_[1]->op_type_) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[1]);
    } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(doc_id_merge_ctdef->children_[0]);
    }
  }
  return lookup_ctdef;
}

}  // namespace table
}  // namespace oceanbase
