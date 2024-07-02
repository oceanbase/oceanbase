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

#include "share/vector_index/ob_ivfflat_index_search_helper.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_param.h"
#include "share/ob_ddl_common.h"
#include "share/object/ob_obj_cast.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace share
{

int ObIvfflatIndexSearchHelper::init(
    const int64_t tenant_id,
    const int64_t index_table_id,
    const int64_t ann_k,
    const int64_t probes,
    const ObTypeVector &qvector,
    const common::ObIArray<int32_t> &output_projector,
    common::sqlclient::ObISQLConnection *conn,
    sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(index_table_id), K(qvector));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == index_table_id || 0 == ann_k || !qvector.is_valid()
      || output_projector.empty() || nullptr == conn || nullptr == session) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(index_table_id), K(output_projector), K(conn), K(session));
  } else if (OB_FAIL(session->get_vector_ivfflat_probes(n_probes_))) {
    LOG_WARN("failed to get vector ivfflat probes", K(ret), K_(n_probes));
  } else {
    n_probes_ = probes > 0 ? probes : n_probes_; // hint first
    tenant_id_ = tenant_id;
    index_table_id_ = index_table_id;
    ann_k_ = ann_k;
    projector_cnt_ = output_projector.count();
    qvector_ = qvector;
    conn_ = conn;
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "IvfflatScan"));
    const schema::ObTableSchema *index_table_schema = nullptr;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("fail to check formal guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id, index_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id), K(index_table_id));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(index_table_schema), K_(tenant_id), K(index_table_id));
    } else {
      distance_type_ = static_cast<ObVectorDistanceType>(index_table_schema->get_vector_distance_func());
      if (n_probes_ == 0) {
        // TODO:(@wangmiao) don't use ivfflat_lists any more.
        n_probes_ = sqrt(index_table_schema->get_vector_ivfflat_lists());
        n_probes_ = n_probes_ > 0 ? n_probes_ : 1;
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::alloc_rows(const bool need_objs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rows_ = static_cast<ObIvfflatRow*>(allocator_.alloc(ann_k_ * sizeof(ObIvfflatRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator rows", K(ret));
  } else if (need_objs) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ann_k_; ++i) {
      if (OB_ISNULL(rows_[i].projector_objs_ = static_cast<ObObj*>(allocator_.alloc(projector_cnt_ * sizeof(ObObj))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator projector_objs_", K(ret));
      } else {
        for (int64_t j = 0; j < projector_cnt_; ++j) {
          rows_[i].projector_objs_[j].reset();
        }
      }
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::reset_centers()
{
  int ret = OB_SUCCESS;
  centers_ = nullptr; // reset
  LOG_INFO("######ivfflatindexcache###### reset centers", K(index_table_id_), K(partition_idx_));
  if (OB_FAIL(MTL(ObTenantIvfflatCenterCache*)->get(index_table_id_, partition_idx_, centers_))) {
    LOG_WARN("failed to get center cache", K(ret), K(index_table_id_), K(partition_idx_));
  }
  if (OB_HASH_NOT_EXIST == ret) {
    const schema::ObTableSchema *index_table_schema = nullptr;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("fail to check formal guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id_, index_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id), K(index_table_id_));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(index_table_schema), K_(tenant_id), K(index_table_id_));
    } else if (OB_FAIL(MTL(ObTenantIvfflatCenterCache*)->put(*index_table_schema))) {
      LOG_WARN("failed to push center", K(ret));
    } else if (OB_FAIL(MTL(ObTenantIvfflatCenterCache*)->get(index_table_id_, partition_idx_, centers_))) {
      LOG_WARN("failed to get center cache", K(ret), K(index_table_id_), K(partition_idx_));
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::set_partition_name(common::ObTabletID &tablet_id)
{
  return ObTenantIvfflatCenterCache::set_partition_name(
      tenant_id_, index_table_id_, tablet_id, allocator_, partition_name_, partition_idx_);
}

void ObIvfflatIndexSearchHelper::reuse()
{
  center_heap_.reset();
  row_cnt_ = 0;
}

void ObIvfflatIndexSearchHelper::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  index_table_id_ = OB_INVALID_ID;
  ann_k_ = 0;
  row_cnt_ = 0;
  projector_cnt_ = 0;
  n_probes_ = 0;
  distance_type_ = INVALID_DISTANCE_TYPE;
  rows_ = nullptr;
  centers_ = nullptr;
  center_heap_.reset();
  row_heap_.reset();
  row_item_pool_.reset();
  allocator_.clear();
}

int ObIvfflatIndexSearchHelper::get_centers_by_sql(
    schema::ObSchemaGetterGuard &schema_guard,
    const schema::ObTableSchema &container_table_schema,
    common::sqlclient::ObISQLConnection &conn)
{
  int ret = OB_SUCCESS;
  ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
  ObSqlString sql_str;
  const ObString &table_name = container_table_schema.get_table_name_str();
  const uint64_t database_id = container_table_schema.get_database_id();
  ObString database_name;
  const schema::ObDatabaseSchema *db_schema = nullptr;
  if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K_(tenant_id), K(database_id), K(container_table_schema));
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
    if (OB_FAIL(container_table_schema.get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        if (FALSE_IT(col_id = column_ids.at(i).col_id_)) {
        } else if (OB_ISNULL(column_schema = container_table_schema.get_column_schema(col_id))) {
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
    } else if (!partition_name_.empty() && OB_FAIL(sql_str.assign_fmt("SELECT /*+DYNAMIC_SAMPLING(0)*/ %.*s from `%.*s`.`%.*s` partition(%.*s)",
        static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(table_name.length()), table_name.ptr(),
        static_cast<int>(partition_name_.length()), partition_name_.ptr()))) {
      LOG_WARN("fail to assign select sql string", K(ret));
    } else if (partition_name_.empty() && OB_FAIL(sql_str.assign_fmt("SELECT /*+DYNAMIC_SAMPLING(0)*/ %.*s from `%.*s`.`%.*s`",
        static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(table_name.length()), table_name.ptr()))) {
      LOG_WARN("fail to assign select sql string", K(ret));
    } else {
      LOG_TRACE("start to get centers", K(sql_str));
      ObTypeVector tmp_vec;
      int64_t column_id = 0;
      int64_t center_idx = 0;
      double distance = 0;
      SMART_VAR(ObMySQLProxy::MySQLResult, read_res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(conn.execute_read(tenant_id_, sql_str.ptr(), read_res))) {
          LOG_WARN("fail to scan index table", K(ret), K(tenant_id_));
        } else if (OB_ISNULL(result = read_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret), KP(result));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next row", K(ret));
              }
            } else if (FALSE_IT(column_id = 0)) {
            } else if (OB_FAIL(result->get_int(column_id, center_idx))) {
              LOG_WARN("fail to get int", K(ret), K(column_id));
            } else if (OB_FAIL(result->get_vector(result->get_row()->get_count() - 1, tmp_vec))) {
              LOG_WARN("fail to get vector", K(ret), K(column_id));
            } else {
              if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(qvector_, distance))) {
                LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
              } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, qvector_, distance))) {
                LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
              } else if (center_heap_.count() < n_probes_) {
                if (OB_FAIL(center_heap_.push(HeapCenterItem(distance, center_idx)))) {
                  LOG_WARN("failed to push into heap", K(ret), K(distance), K(center_idx));
                }
              } else { // update heap
                const HeapCenterItem &top = center_heap_.top();
                if (distance < top.distance_) {
                  HeapCenterItem tmp(distance, center_idx);
                  if (OB_FAIL(center_heap_.replace_top(tmp))) {
                    LOG_WARN("failed to replace top", K(ret), K(tmp));
                  }
                }
              }
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObIvfflatIndexSearchHelper::get_centers_by_cache()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(centers_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null centers", K(ret));
  } else {
    LOG_TRACE("start to get centers by cache");
    ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
    double distance = 0;
    for (int64_t i = 0; i < centers_->count(); ++i) {
      const ObTypeVector &tmp_vec = centers_->at(i);
      if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(qvector_, distance))) {
        LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
      } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, qvector_, distance))) {
        LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
      } else if (center_heap_.count() < n_probes_) {
        if (OB_FAIL(center_heap_.push(HeapCenterItem(distance, i)))) {
          LOG_WARN("failed to push into heap", K(ret), K(distance), K(i));
        }
      } else { // update heap
        const HeapCenterItem &top = center_heap_.top();
        if (distance < top.distance_) {
          HeapCenterItem tmp(distance, i);
          if (OB_FAIL(center_heap_.replace_top(tmp))) {
            LOG_WARN("failed to replace top", K(ret), K(tmp));
          }
        }
      }
    }
    LOG_TRACE("success to get centers by cache", K(ret), K(center_heap_.count()));
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::generate_centers_by_storage(
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(output_exprs) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exprs", K(ret), K(output_exprs), K(eval_ctx));
  } else if (output_exprs->count() != 2) { // froce defence
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(output_exprs));
  } else {
    LOG_TRACE("start to get centers");
    ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
    const int64_t column_cnt = output_exprs->count();
    ObObj tmp_obj;
    ObTypeVector tmp_vec;
    int64_t center_idx = -1;
    double distance = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      sql::ObExpr *expr = output_exprs->at(i);
      ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (tmp_obj.get_type() == ObIntType) { // center_idx
        if (OB_FAIL(tmp_obj.get_int(center_idx))) {
          LOG_WARN("failed to get int", K(ret), K(tmp_obj));
        }
      } else if (tmp_obj.get_type() == ObVectorType) { // vector
        if (OB_FAIL(tmp_obj.get_vector(tmp_vec))) {
          LOG_WARN("failed to get vector", K(ret), K(tmp_obj));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(ret), K(tmp_obj));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(qvector_, distance))) {
      LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
    } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, qvector_, distance))) {
      LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
    } else if (center_heap_.count() < n_probes_) {
      if (OB_FAIL(center_heap_.push(HeapCenterItem(distance, center_idx)))) {
        LOG_WARN("failed to push into heap", K(ret), K(distance), K(center_idx));
      }
    } else { // update heap
      const HeapCenterItem &top = center_heap_.top();
      if (distance < top.distance_) {
        HeapCenterItem tmp(distance, center_idx);
        if (OB_FAIL(center_heap_.replace_top(tmp))) {
          LOG_WARN("failed to replace top", K(ret), K(tmp));
        }
      }
    }
    LOG_TRACE("success to get center", K(ret), K(tmp_vec), K(distance), K(center_heap_.top().distance_));
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::prepare_rowkey_ranges(
    ObIArray<ObNewRange> &range_array,
    const int64_t rowkey_cnt,
    const uint64_t table_id,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < center_heap_.count(); ++i) {
    const int64_t center_idx = center_heap_.at(i).center_idx_;
    ObNewRange key_range;
    if (OB_FAIL(prepare_rowkey_range(key_range, rowkey_cnt, center_idx, table_id, allocator))) {
      LOG_WARN("failed to prepare rowkey range", K(ret));
    } else if (OB_FAIL(range_array.push_back(key_range))) {
       LOG_WARN("faield to push back array", K(ret));
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::prepare_rowkey_range(
    ObNewRange &key_range,
    const int64_t rowkey_cnt,
    const int64_t center_idx,
    const uint64_t table_id,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObj *min_ptr = nullptr;
  ObObj *max_ptr = nullptr;
  void *min_buf = nullptr;
  void *max_buf = nullptr;
  if (OB_ISNULL(min_buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else if (OB_ISNULL(max_buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    min_ptr = new(min_buf) ObObj[rowkey_cnt];
    max_ptr = new(max_buf) ObObj[rowkey_cnt];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    if (i == 0) {
      min_ptr[i].set_int(center_idx);
      max_ptr[i].set_int(center_idx);
    } else {
      min_ptr[i].set_min_value();
      max_ptr[i].set_max_value();
    }
  }
  if (OB_SUCC(ret)) {
    key_range.table_id_ = table_id;
    ObRowkey min_key(min_ptr, rowkey_cnt);
    key_range.start_key_ = min_key;
    ObRowkey max_key(max_ptr, rowkey_cnt);
    key_range.end_key_ = max_key;
    key_range.border_flag_.unset_inclusive_start();
    key_range.border_flag_.unset_inclusive_end();
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::init_item_pool(const HeapRowItem &src_item)
{
  int ret = OB_SUCCESS;
  HeapRowItem dst_item;
  for (int64_t i = 0; OB_SUCC(ret) && i < ann_k_ ; ++i) {
    if (FALSE_IT(dst_item.projector_objs_ = nullptr)) {
    } else if (OB_ISNULL(dst_item.projector_objs_ = static_cast<ObObj*>(allocator_.alloc(projector_cnt_ * sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator projector_objs_", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < projector_cnt_; ++j) {
        ObObj &src = src_item.projector_objs_[j];
        ObObj &dst = dst_item.projector_objs_[j];
        if (FALSE_IT(dst.reset())) {
        } else if (OB_UNLIKELY(src.need_deep_copy())) {
          int64_t deep_copy_size = src.get_deep_copy_size();
          char *buf = static_cast<char*>(allocator_.alloc(deep_copy_size));
          int64_t pos = 0;
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(deep_copy_size));
          } else if (OB_FAIL(dst.deep_copy(src, buf, deep_copy_size, pos))) {
            LOG_WARN("deep copy src obj failed", K(ret), K(deep_copy_size), K(pos));
          }
        } else {
          dst = src;
        }
      }
      if (FAILEDx(row_item_pool_.push_back(dst_item))) {
        LOG_WARN("failed to push back", K(ret), K(dst_item));
      }
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::alloc_item_from_pool(HeapRowItem &item)
{
  int ret = OB_SUCCESS;
  if (row_item_pool_.empty()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(row_item_pool_.pop_back(item))) {
    LOG_WARN("failed to pop back", K(ret));
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::alloc_row_item(
    const sql::ExprFixedArray *output_exprs,
    const double distance,
    sql::ObEvalCtx *eval_ctx,
    HeapRowItem &row_item)
{
  int ret = OB_SUCCESS;
  ObObj src_obj;
  sql::ObExpr *expr = nullptr;
  if (row_heap_.count() == 0) {
    if (OB_ISNULL(row_item.projector_objs_ = static_cast<ObObj*>(allocator_.alloc(projector_cnt_ * sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator projector_objs_", K(ret));
    } else {
      row_item.distance_ = distance;
      for (int64_t i = 0; OB_SUCC(ret) && i < projector_cnt_; ++i) {
        if (FALSE_IT(row_item.projector_objs_[i].reset())) {
        } else {
          expr = output_exprs->at(i);
          ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
          if (OB_FAIL(col_datum.to_obj(src_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("convert datum to obj failed", K(ret));
          } else if (OB_FAIL(ob_write_obj(allocator_, src_obj, row_item.projector_objs_[i]))) {
            LOG_WARN("failed to deep copy obj", K(ret), K(src_obj));
          }
        }
      }
      if (FAILEDx(init_item_pool(row_item))) {
        LOG_WARN("failed to init item pool", K(ret));
      }
    }
  } else if (OB_FAIL(alloc_item_from_pool(row_item))) {
    LOG_WARN("failed to alloc item from pool", K(ret));
  } else {
    row_item.distance_ = distance;
    for (int64_t i = 0; OB_SUCC(ret) && i < projector_cnt_; ++i) {
      ObObj &dst = row_item.projector_objs_[i];
      expr = output_exprs->at(i);
      ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
      // 为实现简单，如果pool中的obj有足够大的buf，则复用原地址进行deep_copy，否则分配新空间，原空间等到helper回收时一并回收
      if (OB_FAIL(col_datum.to_obj(src_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_UNLIKELY(src_obj.need_deep_copy())) {
        int64_t deep_copy_size = src_obj.get_deep_copy_size();
        char *buf = nullptr;
        int64_t pos = 0;
        if (deep_copy_size <= dst.val_len_) {
          buf = (char*)dst.v_.ptr_;
        } else {
          buf = static_cast<char*>(allocator_.alloc(deep_copy_size));
        }
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(deep_copy_size));
        } else if (OB_FAIL(dst.deep_copy(src_obj, buf, deep_copy_size, pos))) {
          LOG_WARN("deep copy src obj failed", K(ret), K(deep_copy_size), K(pos));
        }
      } else {
        dst = src_obj;
      }
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::update_row_heap(const HeapRowItem &row_item, const double distance)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (row_heap_.count() < ann_k_) {
      if (OB_FAIL(row_heap_.push(row_item))) {
        LOG_WARN("failed to push into heap", K(ret), K(distance));
      }
    } else if (distance < row_heap_.top().distance_) { // update heap
      HeapRowItem &top_item = row_heap_.top();
      if (OB_FAIL(row_item_pool_.push_back(top_item))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(row_heap_.replace_top(row_item))) {
        LOG_WARN("failed to replace top", K(ret));
      }
    }
  }
  return ret;
}

int ObIvfflatIndexSearchHelper::get_rows_by_storage(
    const sql::ExprFixedArray *extra_access_exprs,
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra_access_exprs) || OB_ISNULL(output_exprs) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exprs", K(ret), K(extra_access_exprs), K(output_exprs), K(eval_ctx));
  } else if (extra_access_exprs->count() != 2) { // froce defence
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(extra_access_exprs));
  } else if (projector_cnt_ != output_exprs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(output_exprs));
  } else {
    double distance = 0;
    int64_t column_cnt = extra_access_exprs->count();
    ObObj tmp_obj;
    HeapRowItem row_item;
    ObTypeVector tmp_vec;
    ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
    sql::ObExpr *expr = extra_access_exprs->at(1); // center_idx | vector
    ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
    if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (OB_FAIL(tmp_obj.get_vector(tmp_vec))) {
      LOG_WARN("failed to get vector", K(ret), K(tmp_obj));
    } else if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(qvector_, distance))) {
      LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
    } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, qvector_, distance))) {
      LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_));
    } else if (row_heap_.count() < ann_k_ || distance < row_heap_.top().distance_) {
      if (OB_FAIL(alloc_row_item(output_exprs, distance, eval_ctx, row_item))) {
        LOG_WARN("failed to alloc row item", K(ret));
      }
    }
    if (FAILEDx(update_row_heap(row_item, distance))) {
      LOG_WARN("failed to update row heap");
    }
    LOG_TRACE("get row by storage", K(ret), K(tmp_vec), K(distance), K(row_heap_.top().distance_), K(row_heap_.count()));
  }
  LOG_TRACE("finish to get one row by storage", K(ret));
  return ret;
}

int ObIvfflatIndexSearchHelper::get_rows_by_storage()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_rows(false/*need_objs*/))) {
    LOG_WARN("failed to alloc rows", K(ret));
  } else {
    for (int64_t i = row_heap_.count() - 1; OB_SUCC(ret) && 0 < row_heap_.count() && 0 <= i; --i) { // shallow copy
      const HeapRowItem &item = row_heap_.top();
      rows_[i].projector_objs_ = item.projector_objs_;
      row_cnt_++;
      if (OB_FAIL(row_heap_.pop())) {
        LOG_WARN("failed to pop row heap", K(ret));
      }
    }
    LOG_TRACE("finish to get rows by storage", K(ret), K(row_cnt_));
  }
  return ret;
}
int ObIvfflatIndexSearchHelper::get_row(const int64_t idx, ObIvfflatRow *&row)
{
  int ret = OB_SUCCESS;
  if (idx >= row_cnt_) {
    ret = OB_ITER_END;
  } else {
    row = &rows_[idx];
  }
  return ret;
}

} // share
} // oceanbase