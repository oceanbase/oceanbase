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
#include "ob_table_aggregation.h"

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

/*
  init session info
  1. expr/das/translation need session.
  2. we use the session pool for authentication because the authentication information is stored in the session pool.
*/
int ObTableCtx::init_sess_info(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  // try get session from session pool
  if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential, sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  } else if (OB_ISNULL(sess_guard_.get_sess_node_val())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(credential));
  }
  return ret;
}

int ObTableCtx::init_common(ObTableApiCredential &credential,
                            const ObTabletID &arg_tablet_id,
                            const uint64_t table_id,
                            const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = credential.tenant_id_;
  const uint64_t database_id = credential.database_id_;

  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
                                                    table_id,
                                                    table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("fail get table schema by table id", K(ret), K(tenant_id), K(database_id), K(table_id));
  } else if (OB_FAIL(inner_init_common(credential, arg_tablet_id, table_schema_->get_table_name(), timeout_ts))) {
    LOG_WARN("fail to inner init common", KR(ret), K(credential), K(arg_tablet_id), K(timeout_ts));
  }

  return ret;
}

/*
  1. ObTableColumnItem record some column info, such as column id、column name、and so on.
  2. we record some specific elements like is_stored_generated_column_
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
int ObTableCtx::construct_column_items()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObTableSchema::const_column_iterator iter = table_schema_->column_begin();
    ObTableSchema::const_column_iterator end = table_schema_->column_end();
    for (int64_t i = 0; OB_SUCC(ret) && iter != end; ++iter, i++) {
      col_schema = *iter;
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is NULL", K(ret));
      } else {
        ObTableColumnItem item;
        item.col_idx_ = i;
        item.column_id_ = col_schema->get_column_id();
        item.table_id_ = col_schema->get_table_id();
        item.column_name_ = col_schema->get_column_name_str();
        item.default_value_ = col_schema->get_cur_default_value();
        item.is_generated_column_ = col_schema->is_generated_column();
        item.is_stored_generated_column_ = col_schema->is_stored_generated_column();
        item.is_virtual_generated_column_ = col_schema->is_virtual_generated_column();
        item.is_auto_increment_ = col_schema->is_autoincrement();
        item.is_nullable_ = col_schema->is_nullable();
        item.generated_expr_str_ = item.default_value_.get_string();
        item.auto_filled_timestamp_ = col_schema->is_on_update_current_timestamp();
        item.rowkey_position_ = col_schema->get_rowkey_position();
        item.column_type_ = col_schema->get_meta_type().get_type();
        if (item.is_auto_increment_ && OB_FAIL(add_auto_inc_param(*col_schema))) {
          LOG_WARN("fail to add auto inc param", K(ret), K(item));
        } else if (item.is_generated_column_
            && OB_FAIL(col_schema->get_cascaded_column_ids(item.cascaded_column_ids_))) {
          LOG_WARN("fail to get cascaded column ids", K(ret), K(item), K(*col_schema));
        } else if (OB_FAIL(column_items_.push_back(item))) {
          LOG_WARN("fail to push back column item", K(ret), K_(column_items), K(item));
        } else if (!has_lob_column_ && is_lob_storage(item.column_type_)) {
          has_lob_column_ = true;
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::get_column_item_by_column_id(uint64_t column_id, const ObTableColumnItem *&item) const
{
  int ret = OB_SUCCESS;

  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; i < column_items_.count() && !found; i++) {
      const ObTableColumnItem &tmp_item = column_items_.at(i);
      if (tmp_item.column_id_ == column_id) {
        found = true;
        item = &tmp_item;
      }
    }
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

  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count() && !found; i++) {
      const ObTableColumnItem &item = column_items_.at(i);
      if (0 == item.column_name_.case_compare(col_name)) {
        if (OB_ISNULL(item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item expr is null", K(ret), K(item));
        } else {
          found = true;
          expr = item.raw_expr_;
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::get_expr_from_column_items(const ObString &col_name, ObColumnRefRawExpr *&expr) const
{
  int ret = OB_SUCCESS;

  if (column_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count() && !found; i++) {
      const ObTableColumnItem &item = column_items_.at(i);
      if (0 == item.column_name_.case_compare(col_name)) {
        if (OB_ISNULL(item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item expr is null", K(ret), K(item));
        } else {
          found = true;
          expr = item.expr_;
        }
      }
    }
  }

  return ret;
}

int ObTableCtx::get_expr_from_assignments(const ObString &col_name, ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;

  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count() && !found; i++) {
    const ObTableAssignment &assign = assigns_.at(i);
    if (OB_ISNULL(assign.column_item_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign column item is null", K(ret));
    } else if (0 == assign.column_item_->column_name_.case_compare(col_name)) {
      found = true;
      expr = assign.expr_;
    }
  }

  return ret;
}

/*
  1. ObConflictChecker need ObPhysicalPlanCtx.
  2. now() expr need ObPhysicalPlanCtx.cur_time_.
  3. das need ObPhysicalPlanCtx to check task should retry or not.
*/
int ObTableCtx::init_physical_plan_ctx(int64_t timeout_ts, int64_t tenant_schema_version)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObPhysicalPlanCtx));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObPhysicalPlanCtx", K(ret), K(sizeof(ObPhysicalPlanCtx)));
  } else {
    ObPhysicalPlanCtx *phy_plan_ctx = new(buf) ObPhysicalPlanCtx(allocator_);
    phy_plan_ctx->set_timeout_timestamp(timeout_ts); // ObConflictChecker::init_das_scan_rtdef 需要
    phy_plan_ctx->set_tenant_schema_version(tenant_schema_version);
    phy_plan_ctx->set_cur_time(ObTimeUtility::current_time());
    exec_ctx_.set_physical_plan_ctx(phy_plan_ctx);
  }
  return ret;
}

/*
  init table context common param, such as tenant_id_, database_id_，and so on. we also do extra things:
  - get session info
  - construct column items
  - adjust entity
*/
int ObTableCtx::init_common(ObTableApiCredential &credential,
                            const ObTabletID &arg_tablet_id,
                            const ObString &arg_table_name,
                            const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = credential.tenant_id_;
  const uint64_t database_id = credential.database_id_;

  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id), K(arg_table_name));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
                                                    database_id,
                                                    arg_table_name,
                                                    false, /* is_index */
                                                    table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(arg_table_name));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    ObString db("");
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, db.ptr(), arg_table_name.ptr());
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(database_id), K(arg_table_name));
  } else if (OB_FAIL(inner_init_common(credential, arg_tablet_id, arg_table_name, timeout_ts))) {
    LOG_WARN("fail to inner init common", KR(ret), K(credential), K(arg_tablet_id),
      K(arg_table_name), K(timeout_ts));
  }

  return ret;
}

int ObTableCtx::inner_init_common(ObTableApiCredential &credential,
                                  const ObTabletID &arg_tablet_id,
                                  const ObString &table_name,
                                  const int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  const ObTenantSchema *tenant_schema = nullptr;
  tenant_id_ = credential.tenant_id_;
  database_id_ = credential.database_id_;
  ObTabletID tablet_id = arg_tablet_id;

  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table schema is null", K(ret), K(table_name));
  } else if (OB_FAIL(schema_guard_.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (OB_FAIL(init_sess_info(credential))) {
    LOG_WARN("fail to init session info", K(ret), K(credential));
  } else if (OB_FAIL(init_physical_plan_ctx(timeout_ts, tenant_schema->get_schema_version()))) {
    LOG_WARN("fail to init physical plan ctx", K(ret));
  } else if (!arg_tablet_id.is_valid()) {
    if (is_scan_) { // 扫描场景使用table_schema上的tablet id,客户端已经做了路由分发
      if (table_schema_->is_partitioned_table()) { // 不支持分区表
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid tablet id in partition table when do scan");
        LOG_WARN("partitioned table not supported", K(ret), K(table_name));
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
  } else if (OB_FAIL(GCTX.location_service_->get(tenant_id_,
                                                 tablet_id,
                                                 0, /* expire_renew_time */
                                                 is_cache_hit,
                                                 ls_id_))) {
    LOG_WARN("fail to get ls id", K(ret), K(tablet_id), K(table_name));
  } else if (OB_FAIL(construct_column_items())) {
    LOG_WARN("fail to construct column items", K(ret));
  } else if (!is_scan_ && OB_FAIL(adjust_entity())) {
    LOG_WARN("fail to adjust entity", K(ret));
  } else if (OB_FAIL(sess_guard_.get_sess_info().get_binlog_row_image(binlog_row_image_type_))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    table_name_ = table_name;
    ref_table_id_ = table_schema_->get_table_id();
    index_table_id_ = ref_table_id_;
    tablet_id_ = tablet_id;
    index_tablet_id_ = tablet_id_;
    tenant_schema_version_ = tenant_schema->get_schema_version();
    timeout_ts_ = timeout_ts;
    is_ttl_table_ = !table_schema_->get_ttl_definition().empty();
  }

  return ret;
}

// get columns info from index_schema or primary table schema
int ObTableCtx::generate_column_infos(ObIArray<ObTableColumnInfo> &columns_infos)
{
  int ret = OB_SUCCESS;
  ObTableColumnInfo tmp_column_info;
  const ObColumnSchemaV2 *column_schema = nullptr;

  if (is_index_scan_) {
    const int64_t N = index_col_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(column_schema = index_schema_->get_column_schema(index_col_ids_.at(i)))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(index_col_ids_.at(i)));
      } else if (OB_FAIL(cons_column_info(*column_schema, tmp_column_info))) {
        LOG_WARN("fail to cons column info", K(ret));
      } else if (OB_FAIL(columns_infos.push_back(tmp_column_info))) {
        LOG_WARN("fail to push back column info", K(ret), K(tmp_column_info));
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
      } else if (OB_FAIL(cons_column_info(*column_schema, tmp_column_info))) {
        LOG_WARN("fail to cons column info", K(ret));
      } else if (OB_FAIL(columns_infos.push_back(tmp_column_info))) {
        LOG_WARN("fail to push back column info", K(ret), K(tmp_column_info));
      }
    }
  }

  return ret;
}

int ObTableCtx::cons_column_info(const ObColumnSchemaV2 &column_schema,
                                 ObTableColumnInfo &column_info)
{
  int ret = OB_SUCCESS;
  column_info.type_.set_type(column_schema.get_data_type());
  column_info.type_.set_result_flag(ObRawExprUtils::calc_column_result_flag(column_schema));
  column_info.column_name_ = column_schema.get_column_name_str();
  column_info.is_auto_inc_ = column_schema.is_autoincrement();
  column_info.is_nullable_ = column_schema.is_nullable();

  if (ob_is_string_type(column_schema.get_data_type()) || ob_is_json(column_schema.get_data_type())) {
    column_info.type_.set_collation_type(column_schema.get_collation_type());
    column_info.type_.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    column_info.type_.set_collation_type(CS_TYPE_BINARY);
    column_info.type_.set_collation_level(CS_LEVEL_NUMERIC);
  }
  const ObAccuracy &accuracy = column_schema.get_accuracy();
  column_info.type_.set_accuracy(accuracy);
  const bool is_zerofill = column_info.type_.has_result_flag(ZEROFILL_FLAG);
  if (is_zerofill) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "modifing column with ZEROFILL flag");
    LOG_WARN("modifing column with ZEROFILL flag is not supported", K(ret), K(column_schema));
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
    if (!column_info.is_auto_inc_) {
      ret = OB_BAD_NULL_ERROR;
      LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_info.column_name_.length(), column_info.column_name_.ptr());
    }
  } else if (obj.is_null() || is_inc_or_append()) { // increment or append check in init_increment() and init_append()
    if (is_append() && ob_is_string_type(obj.get_type())) {
      obj.set_type(column_type.get_type()); // set obj to column type to add lob header when column type is lob, obj is varchar(varchar will not add lob header).
    }
  } else if (column_type.get_type() != obj.get_type()
             && !(ob_is_string_type(column_type.get_type()) && ob_is_string_type(obj.get_type()))) {
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
  check user obj is valid or not, we check:
    - column type
    - collation type
    - nullable
    - accuracy.
*/
int ObTableCtx::adjust_column(const ObColumnSchemaV2 &col_schema, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObTableColumnInfo column_info;

  if (OB_FAIL(cons_column_info(col_schema, column_info))) {
    LOG_WARN("fail to construct column info", K(ret), K(col_schema));
  } else if (OB_FAIL(adjust_column_type(column_info, obj))) {
    LOG_WARN("fail to adjust rowkey column type", K(ret), K(obj), K(column_info));
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

  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    const int64_t schema_rowkey_cnt = table_schema_->get_rowkey_column_num();
    const int64_t entity_rowkey_cnt = rowkey.get_obj_cnt();
    bool has_auto_inc = false; // only one auto increment column in a table
    bool is_full_filled = entity_rowkey_cnt == schema_rowkey_cnt; // allow not full filled when rowkey has auto_increment;
    const ObRowkeyInfo &rowkey_info = table_schema_->get_rowkey_info();
    const ObColumnSchemaV2 *col_schema = nullptr;
    uint64_t column_id = OB_INVALID_ID;
    ObObj *obj_ptr = rowkey.get_obj_ptr();
    for (int64_t i = 0, idx = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; i++) {
      bool need_check = true;
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("fail to get column id", K(ret), K(i));
      } else if (OB_ISNULL(col_schema = table_schema_->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", K(ret), K(column_id));
      } else if (!has_auto_inc && col_schema->is_autoincrement()) {
        has_auto_inc = true;
        if (col_schema->is_part_key_column()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto increment column set to be partition column");
          LOG_WARN("auto increment column could not be partition column", K(ret), KPC(col_schema));
        } else if (!is_full_filled &&
                  ! (ObTableOperationType::Type::DEL == operation_type_ ||
                   ObTableOperationType::Type::UPDATE == operation_type_)) {
          // curr column is auto_increment and user not fill，no need to check
          need_check = false;
        }
      }

      if (OB_SUCC(ret) && need_check) {
        if (!is_full_filled && (ObTableOperationType::Type::DEL == operation_type_ ||
            ObTableOperationType::Type::UPDATE == operation_type_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete or update operation is not supported to partially fill rowkey columns");
          LOG_WARN("rowkey columns is not fullfilled", K(ret), K(entity_rowkey_cnt), K(schema_rowkey_cnt), K(rowkey));
        } else if (idx >= entity_rowkey_cnt) {
          ret = OB_KV_ROWKEY_COUNT_NOT_MATCH;
          LOG_USER_ERROR(OB_KV_ROWKEY_COUNT_NOT_MATCH, schema_rowkey_cnt, entity_rowkey_cnt);
          LOG_WARN("entity rowkey count mismatch table schema rowkey count", K(ret),
                    K(entity_rowkey_cnt), K(schema_rowkey_cnt), K(rowkey));
        } else if (OB_FAIL(adjust_column(*col_schema, obj_ptr[idx]))) { // [c1][c2][c3] [c1][c3]
          LOG_WARN("fail to adjust column", K(ret), K(obj_ptr[idx]));
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
        ret = OB_ERR_BAD_FIELD_ERROR;
        const ObString &table = table_schema_->get_table_name_str();
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), table.length(), table.ptr());
        LOG_WARN("fail to get column schema", K(ret), K(col_name));
      } else if (is_get) {
        // do nothing
      } else if (col_schema->is_rowkey_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutate rowkey column");
        LOG_WARN("property should not be rowkey column", K(ret), K(prop_names), K(i));
      } else if (col_schema->is_generated_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "The specified for generated column is not allowed");
        LOG_WARN("The specified for generated column is not allowed", K(ret), K(col_name));
      } else if (OB_FAIL(adjust_column(*col_schema, prop_obj))) {
        LOG_WARN("fail to adjust column", K(ret), K(prop_obj));
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

/*
  genreate key range when do scan.
    1. check the legality of column obj in query range.
    2. fill primary key object when do index scan when user not filled them.
*/
int ObTableCtx::generate_key_range(const ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  int64_t padding_num = -1;
  ObSEArray<ObTableColumnInfo, 32> columns_infos;
  int64_t N = scan_ranges.count();

  if (OB_FAIL(generate_column_infos(columns_infos))) {
    LOG_WARN("fail to generate columns infos", K(ret));
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
        if (p_key->get_obj_cnt() != columns_infos.count()) {
          ret = OB_KV_SCAN_RANGE_MISSING;
          LOG_USER_ERROR(OB_KV_SCAN_RANGE_MISSING, p_key->get_obj_cnt(), columns_infos.count());
          LOG_WARN("wrong rowkey size", K(ret), K(i), K(j), K(*p_key), K(columns_infos));
        } else {
          const int64_t M = p_key->get_obj_cnt();
          for (int64_t k = 0; OB_SUCCESS == ret && k < M; ++k) {
            ObObj &obj = const_cast<ObObj&>(p_key->get_obj_ptr()[k]);
            if (obj.is_min_value() || obj.is_max_value()) {
              // do nothing
            } else if (OB_FAIL(adjust_column_type(columns_infos.at(k), obj))) {
              LOG_WARN("fail to adjust column type", K(ret), K(columns_infos.at(k)), K(obj));
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
                          const bool &is_wead_read)
{
  int ret = OB_SUCCESS;
  const ObString &index_name = query.get_index_name();
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  bool has_filter = (query.get_htable_filter().is_valid() || query.get_filter_string().length() > 0);
  const bool select_all_columns = select_columns.empty() || query.is_aggregate_query() || (has_filter && !is_htable());
  const ObColumnSchemaV2 *column_schema = nullptr;
  operation_type_ = ObTableOperationType::Type::SCAN;
  // init is_weak_read_,scan_order_
  is_weak_read_ = is_wead_read;
  scan_order_ = query.get_scan_order();
  // init limit_,offset_
  bool is_query_with_filter = query.get_htable_filter().is_valid() ||
                              query.get_filter_string().length() > 0;
  limit_ = is_query_with_filter || is_ttl_table() ? -1 : query.get_limit(); // query with filter or ttl table can't pushdown limit
  offset_ = is_ttl_table() ? 0 : query.get_offset();
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
        const ObIndexInfo &index_info = index_schema_->get_index_info();
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
    } else if (query.is_aggregate_query() && OB_FAIL(init_agg_cell_proj(query.get_aggregations().count()))) {
      LOG_WARN("fail to init agg cell proj", K(ret));
    } else {
      // select_col_ids_ is same order with schema
      int64_t cell_idx = 0;
      ObSEArray<ObString, 4> ttl_columns;
      if (is_ttl_table_) {
        const ObString &ttl_definition = table_schema_->get_ttl_definition();
        if (OB_FAIL(ObTTLUtil::get_ttl_columns(ttl_definition, ttl_columns))) {
          LOG_WARN("fail to get ttl columns", K(ret));
        }
      }
      for (ObTableSchema::const_column_iterator iter = table_schema_->column_begin();
          OB_SUCC(ret) && iter != table_schema_->column_end(); ++iter, cell_idx++) {
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
          } else if (query.is_aggregate_query() && OB_FAIL(add_aggregate_proj(cell_idx, column_name, query.get_aggregations()))) {
            LOG_WARN("failed to add aggregate projector", K(ret), K(cell_idx), K(column_name));
          } else if (is_index_scan_ && !is_index_back_ &&
              OB_ISNULL(column_schema = index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true;
          }
        } else if (has_exist_in_columns(select_columns, column_schema->get_column_name_str())
            || (is_ttl_table_ && has_exist_in_array(ttl_columns, column_schema->get_column_name_str()))) {
          if (OB_FAIL(select_col_ids_.push_back(column_schema->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (is_index_scan_ && !is_index_back_ &&
              OB_ISNULL(column_schema = index_schema_->get_column_schema(column_name))) {
            is_index_back_ = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (!select_all_columns) {
          // query_col_ids_ is user query order
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
  2. alloc ObTableColumnItem for new ObTableAssignment.
*/
int ObTableCtx::add_generated_column_assignment(const ObTableAssignment &assign)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    ObTableColumnItem &item = column_items_.at(i);
    if (item.is_generated_column_) {
      bool match = false;
      for (int64_t j = 0; j < item.cascaded_column_ids_.count() && !match; j++) {
        const uint64_t column_id = item.cascaded_column_ids_.at(j);
        if (column_id == assign.column_item_->column_id_) {
          match = true;
        }
      }
      if (match) {
        void *buf = ctx_allocator_.alloc(sizeof(ObTableColumnItem));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObTableColumnItem", K(ret), K(sizeof(ObTableColumnItem)));
        } else {
          ObTableColumnItem *tmp_item = new(buf) ObTableColumnItem();
          *tmp_item = item;
          ObTableAssignment tmp_assign(tmp_item);
          if (OB_FAIL(assigns_.push_back(tmp_assign))) {
            ctx_allocator_.free(buf);
            LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(tmp_assign));
          }
        }
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

  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    ObTableColumnItem &item = column_items_.at(i);
    if (OB_SUCCESS == entity.get_property(item.column_name_, prop_obj)) {
      if (item.rowkey_position_ > 0) {
        ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_UPDATE_ROWKEY_COLUMN);
        LOG_WARN("can not update rowkey column", K(ret));
      } else {
        ObTableAssignment assign(&item);
        assign.assign_value_ = prop_obj; // shadow copy when prop_obj is string type
        assign.is_assigned_ = true;
        if (OB_FAIL(assigns_.push_back(assign))) {
          LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(assign));
        } else if (table_schema_->has_generated_column()
            && OB_FAIL(add_generated_column_assignment(assign))) {
          LOG_WARN("fail to add soterd generated column assignment", K(ret), K(assign));
        }
      }
    } else if (item.auto_filled_timestamp_) { // on update current timestamp
      ObTableAssignment assign(&item);
      if (OB_FAIL(assigns_.push_back(assign))) {
        LOG_WARN("fail to push back assignment", K(ret), K_(assigns), K(assign));
      }
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
    if (OB_FAIL(table_schema_->get_column_ids(select_col_ids_))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else if (OB_FAIL(init_dml_related_tid())) { // 3. init related index table id
      LOG_WARN("fail to init dml related table ids", K(ret));
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
    } else if (column_schema->is_virtual_generated_column()) {
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

int ObTableCtx::init_ttl_delete(ObRowkey &start_key)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  ObNewRange range;
  ObRowkey real_start_key;
  range.end_key_.set_max_row();
  set_is_ttl_table(false);
  if (!start_key.is_valid()) {
    real_start_key.set_min_row();
  } else {
    real_start_key = start_key;
  }

  range.start_key_ = real_start_key;
  if (OB_FAIL(query.add_scan_range(range))) {
    LOG_WARN("fail to generate key ranges", KR(ret), K(range));
  } else if (OB_FAIL(init_scan(query, false/* is_weak_read */))) {
    LOG_WARN("fail to init scan ctx", KR(ret), K(query));
  }

  // 2. init related index table id
  if (OB_SUCC(ret) && OB_FAIL(init_dml_related_tid())) {
    LOG_WARN("fail to init dml related table ids", K(ret));
  }

  return ret;
}

/*
  init replace parameters.
    - init related index table ids, cause we need to replace index table as well.
*/
int ObTableCtx::init_replace()
{
  return init_dml_related_tid();
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

  if (OB_FAIL(init_insert_up(false))) {
    LOG_WARN("fail to init insert up", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count(); i++) {
      ObTableAssignment &assign = assigns_.at(i);
      const ObObj &delta = assign.assign_value_;
      if (OB_ISNULL(assign.column_item_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret), K(assign));
      } else if (assign.column_item_->auto_filled_timestamp_) {
        // do nothing
      } else if (delta.is_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "append null");
        LOG_WARN("append NULL is illegal", K(ret), K(delta));
      } else if (OB_UNLIKELY(!ob_is_string_type(assign.column_item_->column_type_))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-string types for append operation");
        LOG_WARN("invalid type for append", K(ret), K(assign));
      } else if (OB_UNLIKELY(!ob_is_string_type(delta.get_type()))) {
        ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
        const char *schema_type_str = "stringTc";
        const char *obj_type_str = ob_obj_type_str(delta.get_type());
        LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH, assign.column_item_->column_name_.length(), assign.column_item_->column_name_.ptr(),
            static_cast<int>(strlen(schema_type_str)), schema_type_str, static_cast<int>(strlen(obj_type_str)), obj_type_str);
        LOG_WARN("can only append string type", K(ret), K(delta));
      } else {
        const ObString &column_name = assign.column_item_->column_name_;
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
          assign.column_item_->generated_expr_str_ = generated_expr_str;
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

  if (OB_FAIL(init_insert_up(false))) {
    LOG_WARN("fail to init insert up", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns_.count(); i++) {
      ObTableAssignment &assign = assigns_.at(i);
      const ObObj &delta = assign.assign_value_;
      if (OB_ISNULL(assign.column_item_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret), K(assign));
      } else if (assign.column_item_->is_auto_increment_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "increment auto increment column");
        LOG_WARN("not support increment auto increment column", K(ret), K(assign));
      } else if (assign.column_item_->auto_filled_timestamp_) {
        // do nothing
      } else if (OB_UNLIKELY(!ob_is_int_tc(assign.column_item_->column_type_))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-integer types for increment operation");
        LOG_WARN("invalid type for increment", K(ret), K(assign));
      } else if (!ob_is_int_tc(delta.get_type())) {
        ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
        const char *schema_type_str = "intTc";
        const char *obj_type_str = ob_obj_type_str(delta.get_type());
        LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH, assign.column_item_->column_name_.length(), assign.column_item_->column_name_.ptr(),
            static_cast<int>(strlen(schema_type_str)), schema_type_str, static_cast<int>(strlen(obj_type_str)), obj_type_str);
        LOG_WARN("delta should only be signed integer type", K(ret), K(delta));
      } else {
        const ObString &column_name = assign.column_item_->column_name_;
        const int64_t total_len = strlen("IFNULL(``, 0) + ``") + 1
            + column_name.length() + column_name.length();
        int64_t actual_len = -1;
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(total_len));
        } else if ((actual_len = snprintf(buf, total_len, "IFNULL(`%s`, 0) + `%s`",
            column_name.ptr(), column_name.ptr())) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to construct increment expr string", K(ret), K(total_len), K(delta));
        } else {
          ObString generated_expr_str(actual_len, buf);
          assign.column_item_->generated_expr_str_ = generated_expr_str;
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
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
      const ObTableColumnItem &tmp_item = column_items_.at(i);
      if (has_exist_in_array(select_col_ids_, tmp_item.column_id_)) {
        if (OB_ISNULL(tmp_item.expr_)) {
          // use column ref exprs, cause select exprs no need to calculate
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column ref expr is NULL", K(ret), K(i));
        } else if (OB_FAIL(select_exprs_.push_back(tmp_item.expr_))) {
          LOG_WARN("fail to push back select expr", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_schema_->get_rowkey_column_ids(rowkey_column_ids))) {
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
    if (OB_SUCC(ret) && is_index_scan_) {
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


// get primary tablet location and all index tablet location, add them to das context.
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
        if (OB_FAIL(get_related_tablet_id(*index_schema, index_tablet_id_))) {
          LOG_WARN("fail to get index tablet id", K(ret));
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
        if (ObTableOperationType::Type::UPDATE == operation_type_) { // check index column is updated or not
          bool found = false;
          for (int64_t j = 0; OB_SUCC(ret) && j < assigns_.count() && !found; j++) {
            const ObTableAssignment &assign = assigns_.at(j);
            if (OB_ISNULL(assign.column_item_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("assign column item is null", K(ret), K(assign));
            } else if (OB_NOT_NULL(index_schema->get_column_schema(assign.column_item_->column_id_))) {
              found = true;
            }
          }
          if (OB_SUCC(ret) && found && OB_FAIL(related_index_ids_.push_back(index_schema->get_table_id()))) {
            LOG_WARN("fail to add related index ids", K(ret), K(index_schema->get_table_id()));
          }
        } else if (OB_FAIL(related_index_ids_.push_back(index_schema->get_table_id()))) {
          LOG_WARN("fail to add related index ids", K(ret), K(index_schema->get_table_id()));
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
int ObTableCtx::add_auto_inc_param(const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObIArray<AutoincParam> &auto_params = exec_ctx_.get_physical_plan_ctx()->get_autoinc_params();
  int64_t auto_increment_cache_size = -1;

  if (!auto_params.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected auto params count, should be empty", K(ret));
  } else if (OB_FAIL(get_session_info().get_auto_increment_cache_size(auto_increment_cache_size))) {
    LOG_WARN("fail to get increment factor", K(ret));
  } else {
    AutoincParam param;
    param.tenant_id_ = tenant_id_;
    param.autoinc_table_id_ = table_schema_->get_table_id();
    param.autoinc_first_part_num_ = table_schema_->get_first_part_num();
    param.autoinc_table_part_num_ = table_schema_->get_all_part_num();
    param.autoinc_col_id_ = column_schema.get_column_id();
    param.auto_increment_cache_size_ = get_auto_increment_cache_size(
      table_schema_->get_auto_increment_cache_size(), auto_increment_cache_size);
    param.part_level_ = table_schema_->get_part_level();
    ObObjType column_type = column_schema.get_data_type();
    param.autoinc_col_type_ = column_type;
    param.autoinc_desired_count_ = 0;
    param.autoinc_mode_is_order_ = table_schema_->is_order_auto_increment_mode();
    param.autoinc_version_ = table_schema_->get_truncate_version();
    param.total_value_count_ = 1;
    param.autoinc_increment_ = 1;
    param.autoinc_offset_ = 1;
    param.part_value_no_order_ = true;
    if (column_schema.is_tbl_part_key_column()) {
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

  return ret;
}

/*
  update auto increment local and gobal value.
  - we need to update local and gobal value after user set specific value.
*/
int ObTableCtx::update_auto_inc_value()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = get_physical_plan_ctx();
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_plan_ctx is null", K(ret));
  } else if (OB_FAIL(phy_plan_ctx->sync_last_value_local())) {
    LOG_WARN("fail to sync last value local", K(ret));
  } else if (OB_FAIL(phy_plan_ctx->sync_last_value_global())) {
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
    if (OB_ISNULL(table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(table_schema_->get_part_idx_by_tablet(tablet_id_, part_idx, subpart_idx))) {
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
  use_put = false;
  bool can_use_put = true;

  if (is_inc_or_append()) { // increment or append operarion need old value to calculate, can not use put
    can_use_put = false;
  } else if (ObTableOperationType::INSERT_OR_UPDATE != operation_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation type", K(ret), K_(operation_type));
  } else if (has_lob_column()) {
    // has lob column cannot use put: may cause lob storeage leak when put row to lob meta table
    can_use_put = false;
  } else if (is_htable()) { // htable has no index and alway full filled.
    can_use_put = true;
  } else if (is_client_set_put_ && !related_index_ids_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with index use put");
    LOG_WARN("client set use_put flag, but has local index is not support", K(ret), K_(related_index_ids));
  } else if (!related_index_ids_.empty()) { // has index, can not use put
    can_use_put = false;
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", K(ret));
  } else {
    bool is_all_columns_filled = false;
    if (OB_ISNULL(entity_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("entity is null", K(ret));
    } else if (FALSE_IT(is_all_columns_filled = table_schema_->get_column_count()
        - table_schema_->get_rowkey_column_num() <= entity_->get_properties_count())) { // all columns are filled
    } else if (is_client_set_put_ && !is_all_columns_filled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "all columns not filled but use put");
      LOG_WARN("client set use_put flag, but not fill all columns is not support", K(ret), KPC_(table_schema), KPC_(entity));
    } else if (is_client_set_put_ || is_all_columns_filled) {
      can_use_put = true;
    } else { // some columns are missing
      can_use_put = false;
    }
  }

  if (OB_SUCC(ret) && can_use_put) {
    use_put = true;
    if (!is_htable() && is_total_quantity_log()) { // tableapi with full binlog image can not use put
      use_put = false;
    }
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
