/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SERVER
#include "ob_hbase_column_family_service.h"
#include "observer/table/adapters/ob_hbase_adapter_factory.h"
#include "share/table/ob_table_util.h"
#include "observer/table/ob_table_query_common.h"
#include "observer/table/cf_service/ob_hbase_multi_cf_iterator.h"
#include "observer/table/part_calc/ob_table_part_calc.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

ObHbaseCfServiceGuard::~ObHbaseCfServiceGuard()
{
  if (OB_NOT_NULL(cf_service_)) {
    cf_service_->~ObHbaseColumnFamilyService();
    allocator_.free(cf_service_);
  }
}

int ObHbaseCfServiceGuard::get_cf_service(ObHbaseColumnFamilyService *&cf_service)
{
  int ret = OB_SUCCESS;
  cf_service = nullptr;
  if (OB_ISNULL(cf_service_)) {
    if (OB_FAIL(ObHbaseColumnFamilyService::alloc_family_sevice(allocator_, 
                                                                is_multi_cf_req_,
                                                                cf_service_))) {
      LOG_WARN("failed to alloc column family service", K(ret), K_(is_multi_cf_req));
    } else if (OB_ISNULL(cf_service_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("created null column family service", K(ret), K_(is_multi_cf_req));
    }
  }

  if (OB_SUCC(ret)) {
    cf_service = cf_service_;
  }
  return ret;
}

int ObHbaseColumnFamilyService::alloc_family_sevice(ObIAllocator &alloc, bool is_multi_cf_req, ObHbaseColumnFamilyService *&cf_service)
{
  int ret = OB_SUCCESS;
  if (is_multi_cf_req) {
    cf_service = OB_NEWx(ObHbaseMultiCFService, &alloc);
  } else {
    cf_service = OB_NEWx(ObHbaseColumnFamilyService, &alloc);
  }

  if (OB_ISNULL(cf_service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(is_multi_cf_req));
  }

  return ret;
}

int ObHbaseColumnFamilyService::put(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  exec_ctx.set_table_id(table_cells.get_table_id());
  const ObIArray<ObHbaseTabletCells *> &tablet_cells_arr = table_cells.get_tablet_cells_array();
  ObSEArray<ObITableEntity *, 4> all_cells;
  all_cells.set_attr(ObMemAttr(MTL_ID(), "HbaseCFAllCells"));

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cells_arr.count(); i++) {
    const ObHbaseTabletCells *tablet_cells = tablet_cells_arr.at(i);
    if (OB_ISNULL(tablet_cells)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet cells", K(ret));
    } else {
      const ObTabletID &tablet_id = tablet_cells->get_tablet_id(); 
      const ObIArray<ObITableEntity *> &cells = tablet_cells->get_cells();
      for (int64_t j = 0; OB_SUCC(ret) && j < cells.count(); j++) {
        ObITableEntity *cell = cells.at(j);
        if (OB_ISNULL(cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null cell", K(ret));
        } else if (OB_FAIL(all_cells.push_back(cell))) {
          LOG_WARN("fail to push back cell", K(ret), K(cell));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObHbaseAdapterGuard adapter_guard(exec_ctx.get_allocator(), exec_ctx);
    ObIHbaseAdapter *adapter = nullptr;
    if (OB_FAIL(adapter_guard.get_hbase_adapter(adapter))) {
      LOG_WARN("fail to get hbase adapter", K(ret));
    } else if (all_cells.count() == 1) {
      if (OB_FAIL(adapter->put(exec_ctx, *all_cells.at(0)))) {
        LOG_WARN("fail to put", K(ret), K(all_cells));
      }
    } else {
      if (OB_FAIL(adapter->multi_put(exec_ctx, all_cells))) {
        LOG_WARN("fail to multi put", K(ret), K(all_cells));
      }
    }
  }

  return ret;
}

int ObHbaseColumnFamilyService::construct_query(const ObITableEntity &cell, ObTableExecCtx &exec_ctx, ObHbaseQuery &hbase_query)
{
  int ret = OB_SUCCESS;
  ObTableQuery &query = hbase_query.get_query();
  ObIAllocator &allocator = exec_ctx.get_allocator();
  if (OB_FAIL(ObHTableUtils::cons_query_by_entity(cell, allocator, query))) {
    LOG_WARN("fail to cons query from del entity");
  } else {}

  return ret;
}

int ObHbaseColumnFamilyService::query(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx, ObHbaseQueryResultIterator *&result_iter)
{
  int ret = OB_SUCCESS;
  ObHbaseCFIterator *tmp_result_iter = nullptr;
  if (OB_ISNULL(tmp_result_iter = OB_NEWx(ObHbaseCFIterator, &exec_ctx.get_allocator(), query, exec_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_result_iter->init())) {
    LOG_WARN("fail to init result iter", K(ret));
  } else {
    result_iter = tmp_result_iter;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_result_iter)) {
    tmp_result_iter->close();
    OB_DELETEx(ObHbaseCFIterator, &exec_ctx.get_allocator(), tmp_result_iter);
  }
  return ret;
}

int ObHbaseMultiCFService::put(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObHbaseTabletCells *> &tablet_cells_arr = table_cells.get_tablet_cells_array();
  const uint64_t table_id = table_cells.get_table_id(); 
  ObSEArray<ObITableEntity *, 4> all_cells;
  all_cells.set_attr(ObMemAttr(MTL_ID(), "HbaseCFAllCells"));
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cells_arr.count(); i++) {
    const ObHbaseTabletCells *tablet_cells = tablet_cells_arr.at(i);
    if (OB_ISNULL(tablet_cells)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet cells", K(ret));
    } else {
      const ObTabletID &tablet_id = tablet_cells->get_tablet_id(); 
      const ObIArray<table::ObITableEntity *> &cells = tablet_cells->get_cells();
      int cur_cell_idx = 0;
      while (OB_SUCC(ret) && cur_cell_idx < cells.count()) {
        ObString first_family_name; 
        ObITableEntity *first_cell = cells.at(cur_cell_idx);
        if (OB_ISNULL(first_cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null cell", K(ret));
        } else if (OB_FAIL(get_family_from_cell(*first_cell, first_family_name))) {
          LOG_WARN("fail to get family from cell", KPC(first_cell));
        } else if (OB_FAIL(remove_family_from_qualifier(*first_cell))) {
          LOG_WARN("fail to remove family from qualifier", K(ret), KPC(first_cell));
        } else if (OB_FAIL(all_cells.push_back(first_cell))) {
          LOG_WARN("fail to push back cell", K(ret), KPC(first_cell));
        } else {
          cur_cell_idx++;
          bool stop = false;
          // aggregate cells which family name equals first_family_name
          while (OB_SUCC(ret) && !stop && cur_cell_idx < cells.count()) {
            ObString cur_family_name;
            ObITableEntity *cur_cell = cells.at(cur_cell_idx);
            if (OB_ISNULL(cur_cell)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null cell", K(ret));
            } else if (OB_FAIL(get_family_from_cell(*cur_cell, cur_family_name))) {
              LOG_WARN("fail to get family from cell", KPC(cur_cell));
            } else if (cur_family_name.case_compare(first_family_name) == 0) {
              if (OB_FAIL(remove_family_from_qualifier(*cur_cell))) {
                LOG_WARN("fail to remove family from qualifier", K(ret), KPC(cur_cell));
              } else if (OB_FAIL(all_cells.push_back(cur_cell))) {
                LOG_WARN("fail to push back cell", K(ret), KPC(cur_cell));
              } else {
                cur_cell_idx++;
              }
            } else {
              stop = true;
            }
          }

          if (OB_SUCC(ret)) {
            uint64_t real_table_id = OB_INVALID_ID;
            ObTabletID real_tablet_id;
            if (OB_FAIL(find_real_table_tablet_id(exec_ctx, table_id, tablet_id,
                first_family_name, real_table_id, real_tablet_id))) {
              LOG_WARN("fail to find real tablet id", K(ret), K(table_id),
                  K(tablet_id), K(first_family_name));
            } else {
              exec_ctx.set_table_id(real_table_id);
              for (int64_t i = 0; i < all_cells.count() && OB_SUCC(ret); i++) {
                all_cells.at(i)->set_tablet_id(real_tablet_id);
              }
              ObHbaseAdapterGuard adapter_guard(exec_ctx.get_allocator(), exec_ctx);
              ObIHbaseAdapter *adapter = nullptr;
              if (OB_FAIL(adapter_guard.get_hbase_adapter(adapter))) {
                LOG_WARN("fail to get hbase adapter", K(ret));
              } else if (OB_FAIL(adapter->multi_put(exec_ctx, all_cells))) {
                LOG_WARN("fail to multi put", K(ret), K(all_cells));
              } else {
                all_cells.reuse();
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHbaseMultiCFService::construct_table_name(ObIAllocator &allocator,
                                                const ObString &table_group_name,
                                                const ObString &family_name,
                                                ObString &table_name)
{
  int ret = OB_SUCCESS;
  const int64_t size = table_group_name.length() + family_name.length() + 1; // "$"
  char *buf = reinterpret_cast<char *>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size));
  } else  {
    int64_t pos = 0;
    MEMCPY(buf + pos, table_group_name.ptr(), table_group_name.length());
    pos += table_group_name.length();
    MEMCPY(buf + pos, "$", 1);
    pos += 1;
    MEMCPY(buf + pos, family_name.ptr(), family_name.length());
    pos += family_name.length();
    table_name.assign(buf, pos);
  }

  return ret;
}

int ObHbaseMultiCFService::find_real_table_tablet_id(ObTableExecCtx &exec_ctx,
                                                     const uint64_t arg_table_id, 
                                                     const ObTabletID arg_tablet_id, 
                                                     const ObString &family_name,
                                                     uint64_t &real_table_id,
                                                     ObTabletID &real_tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t subpart_idx = OB_INVALID_INDEX;
  ObObjectID tmp_object_id = OB_INVALID_ID;
  ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
  ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
  const uint64_t database_id = exec_ctx.get_credential().database_id_;
  const uint64_t tenant_id = MTL_ID();
  const ObSimpleTableSchemaV2 *real_simple_schema = nullptr;
  ObString table_name = ObString::make_empty_string();
  if (OB_FAIL(construct_table_name(exec_ctx.get_allocator(), exec_ctx.get_table_name(),
      family_name, table_name))) {
    LOG_WARN("fail to construct table name", K(ret), K(exec_ctx.get_table_name()), K(family_name));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, database_id, table_name,
      false/*is_index*/, real_simple_schema))) {
    LOG_WARN("failed to get real simple table schema", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_ISNULL(real_simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(find_real_table_tablet_id(exec_ctx, arg_table_id, arg_tablet_id,
      *real_simple_schema, real_table_id, real_tablet_id))) {
    LOG_WARN("failed to find real table tablet id", K(ret), K(arg_table_id), K(arg_tablet_id));
  }

  return ret;
}

int ObHbaseMultiCFService::find_real_table_tablet_id(ObTableExecCtx &exec_ctx,
                                                     const uint64_t arg_table_id, 
                                                     const ObTabletID &arg_tablet_id, 
                                                     const ObSimpleTableSchemaV2 &real_simple_schema,
                                                     uint64_t &real_table_id,
                                                     ObTabletID &real_tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t subpart_idx = OB_INVALID_INDEX;
  ObObjectID tmp_object_id = OB_INVALID_ID;
  ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
  ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
  const uint64_t tenant_id = MTL_ID();
  const ObSimpleTableSchemaV2 *arg_simple_schema = nullptr;
  if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, arg_table_id, arg_simple_schema))) {
    LOG_WARN("failed to get arg simple table schema", K(ret), K(tenant_id), K(arg_table_id));
  } else if (OB_ISNULL(arg_simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(arg_table_id));
  } else if (!arg_simple_schema->is_partitioned_table()) {
    if (real_simple_schema.is_partitioned_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table partition not match", K(ret), K(arg_table_id));
    } else {
      real_tablet_id = real_simple_schema.get_tablet_id();
    }
  } else if (OB_FAIL(arg_simple_schema->get_part_idx_by_tablet(arg_tablet_id, part_idx, subpart_idx))) {
    LOG_WARN("failed to get part idx by tablet", K(ret), K(arg_tablet_id), K(arg_table_id));
  } else if (OB_FAIL(real_simple_schema.get_part_id_and_tablet_id_by_idx(part_idx, subpart_idx,
      tmp_object_id, tmp_first_level_part_id, real_tablet_id))) {
    LOG_WARN("failed to get tablet by part idx", K(ret), K(part_idx), K(subpart_idx));
  }

  if (OB_SUCC(ret)) {
    real_table_id = real_simple_schema.get_table_id();
  }

  return ret;
}


int ObHbaseMultiCFService::get_family_from_cell(const ObITableEntity &entity, ObString &family)
{
  int ret = OB_SUCCESS;
  ObObj qualifier;
  if (OB_FAIL(entity.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier))) {
    LOG_WARN("fail to get qualifier value", K(ret));
  } else if (OB_NOT_NULL(qualifier.get_string().find('.'))) {
    family = qualifier.get_string().split_on('.');
  } 
  return ret;
}

int ObHbaseMultiCFService::get_family_from_cell(const ObNewRow &cell, ObString &family)
{
  int ret = OB_SUCCESS;
  if (cell.count_ < ObHTableConstants::HTABLE_ROWKEY_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cell obj size", K(ret), K(cell.count_));
  } else {
    ObString qualifier_str = cell.get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
    if (OB_NOT_NULL(qualifier_str.find('.'))) {
      family = qualifier_str.split_on('.');
    } 
  }
  return ret;
}

int ObHbaseMultiCFService::remove_family_from_qualifier(const ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObObj qualifier;
  if (OB_FAIL(entity.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier))) {
    LOG_WARN("fail to get qualifier value", K(ret));
  } else if (entity.get_rowkey_size() != ObHTableConstants::HTABLE_ROWKEY_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
  } else {
    ObRowkey rowkey = entity.get_rowkey();
    ObObj *obj_ptr = rowkey.get_obj_ptr();
    if (OB_ISNULL(obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj_ptr is nullptr", K(rowkey));
    } else {
      ObHTableCellEntity3 htable_cell(&entity);
      bool row_is_null = htable_cell.last_get_is_null();
      int64_t timestamp = htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
      } else {
        ObObj &q_obj = const_cast<ObObj &>(obj_ptr[ObHTableConstants::COL_IDX_Q]);  // column Q
        if (qualifier.get_string().after('.').length() == 0) {
          q_obj.set_null();
        } else {
          q_obj.set_string(ObObjType::ObVarcharType, qualifier.get_string().after('.'));
        }
      }
    }
  }
  return ret;
}

int ObHbaseMultiCFService::remove_family_from_qualifier(const ObNewRow &cell)
{
  int ret = OB_SUCCESS;
  if (cell.count_ < ObHTableConstants::HTABLE_ROWKEY_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cell obj size", K(ret), K(cell.count_));
  } else {
    ObObj &qualifier = const_cast<ObObj &>(cell.get_cell(ObHTableConstants::COL_IDX_Q));
    if (qualifier.get_string().after('.').length() == 0) {
      qualifier.set_null();
    } else {
      qualifier.set_string(ObObjType::ObVarcharType, qualifier.get_string().after('.'));
    }
  }
  return ret;
}

int ObHbaseColumnFamilyService::del(const ObHbaseQuery &hbase_query, ObNewRow &cell, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObHbaseAdapterGuard adapter_guard(exec_ctx.get_allocator(), exec_ctx);
  ObIHbaseAdapter *adapter = nullptr;
  if (OB_FAIL(adapter_guard.get_hbase_adapter(adapter))) {
    LOG_WARN("fail to get hbase adapter", K(ret));
  } else if (OB_FAIL(ObHbaseColumnFamilyService::delete_cell(hbase_query, exec_ctx, cell, *adapter))) {
    LOG_WARN("fail to del one cell", K(ret), K(hbase_query), K(cell));
  }
  return ret;
}

int ObHbaseColumnFamilyService::del(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObHbaseQueryResultIterator *hbase_result_iter = nullptr;
  ObHbaseAdapterGuard adapter_guard(exec_ctx.get_allocator(), exec_ctx);
  ObTableQueryIterableResult wide_row;
  wide_row.set_need_append_family(false);
  ObNewRow cell;
  ObIHbaseAdapter *adapter = nullptr;
  const ObTableQuery table_query = hbase_query.get_query();
  int64_t tablet_cnt = table_query.get_tablet_ids().count();
  if (OB_FAIL(query(hbase_query, exec_ctx, hbase_result_iter))) {
    LOG_WARN("fail to query", K(ret), K(hbase_query));
  } else if (OB_FAIL(adapter_guard.get_hbase_adapter(adapter))) {
    LOG_WARN("fail to get hbase adapter", K(ret), K(tablet_cnt));
  } else if (OB_ISNULL(hbase_result_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hbase result iter is null", K(ret));
  } else if (OB_FAIL(hbase_result_iter->get_next_result(wide_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next result", K(ret), K(tablet_cnt), K(wide_row));
    } else {
      ret = OB_SUCCESS;
    }
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(wide_row.get_row(cell))) {
      if (OB_ARRAY_OUT_OF_RANGE != ret) {
        LOG_WARN("fail to get cell from iterable result", K(ret));
      }
    } else {
      int64_t timestamp = 0;
      cell.get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp);
      if (OB_UNLIKELY(timestamp <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid timestamp", K(ret), K(timestamp));
      } else {
        cell.get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_cell(hbase_query, exec_ctx, cell, *adapter))) {
        LOG_WARN("fail to delete one cell", K(ret), K(hbase_query), K(cell), K(tablet_cnt), K(wide_row));
      }
    }
  }
  if (ret == OB_ARRAY_OUT_OF_RANGE) {
    ret = OB_SUCCESS;  // one wide rows iter end
  }

  if (OB_NOT_NULL(hbase_result_iter)) {
    hbase_result_iter->close();
    hbase_result_iter->~ObHbaseQueryResultIterator();
    hbase_result_iter = nullptr;
  }

  return ret;
}

int ObHbaseColumnFamilyService::delete_cell(const ObHbaseQuery &query,
                                            ObTableExecCtx &exec_ctx,
                                            const ObNewRow &cell,
                                            ObIHbaseAdapter &adapter)
{
  int ret = OB_SUCCESS;
  ObTableEntity entity;
  exec_ctx.set_table_id(query.get_table_id());
  ObTabletID real_tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObTablePartCalculator calculator(exec_ctx.get_allocator(),
                                   exec_ctx.get_sess_guard(),
                                   exec_ctx.get_schema_cache_guard(),
                                   exec_ctx.get_schema_guard(),
                                   exec_ctx.get_table_schema());
  if (OB_FAIL(ObHTableUtils::construct_entity_from_row(cell, exec_ctx.get_schema_cache_guard(), entity))) {
    LOG_WARN("fail to construct entity from row", K(ret), K(cell));
  } else if (calculator.calc(exec_ctx.get_table_id(), entity, real_tablet_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to calc tablet id", K(ret), K(exec_ctx.get_table_id()), K(entity));
  } else if (FALSE_IT(entity.set_tablet_id(real_tablet_id))) {
  } else if (OB_FAIL(adapter.del(exec_ctx, entity))) {
    LOG_WARN("fail to del one cell", K(ret), K(entity), K(entity.get_tablet_id()));
  }
  return ret;
}

int ObHbaseColumnFamilyService::del(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObHbaseTabletCells *> &tablet_cells_arr = table_cells.get_tablet_cells_array();
  ObTableQuery table_query;
  ObHbaseQuery query(table_query, false);
  uint64_t table_id = table_cells.get_table_id();
  for (int64_t i = 0; i < tablet_cells_arr.count(); i++) {
    const ObHbaseTabletCells *tablet_cells = tablet_cells_arr.at(i);
    if (OB_ISNULL(tablet_cells)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet cells", K(ret));
    } else {
      const ObTabletID &tablet_id = tablet_cells->get_tablet_id(); 
      const ObIArray<ObITableEntity *> &cells = tablet_cells->get_cells();
      for (int64_t j = 0; OB_SUCC(ret) && j < cells.count(); j++) {
        ObITableEntity *cell = cells.at(j);
        if (OB_ISNULL(cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null cell", K(ret));
        } else if (OB_FAIL(construct_query(*cell, exec_ctx, query))) {
          LOG_WARN("fail to construct query from del entity", K(ret), KPC(cell));
        } else {
          query.set_table_id(table_id);
          ObIArray<ObTabletID> &tablet_ids = table_query.get_tablet_ids();
          tablet_ids.reset();
          if (OB_FAIL(tablet_ids.push_back(cell->get_tablet_id()))) {
            LOG_WARN("fail to add tablet id", K(ret), K(cell->get_tablet_id()));
          } else if (OB_FAIL(ObHbaseColumnFamilyService::del(query, exec_ctx))) {
            LOG_WARN("fail to delete by query", K(ret), K(query));
          } else {}
        }
      }
    }
  } 
  return ret;
}

int ObHbaseMultiCFService::del(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObHbaseTabletCells *> &tablet_cells_arr = table_cells.get_tablet_cells_array();
  uint64_t table_id = table_cells.get_table_id();
  const ObString &table_group_name = table_cells.get_table_name();
  for (int64_t i = 0; i < tablet_cells_arr.count(); i++) {
    const ObHbaseTabletCells *tablet_cells = tablet_cells_arr.at(i);
    if (OB_ISNULL(tablet_cells)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet cells", K(ret));
    } else {
      const ObTabletID &tablet_id = tablet_cells->get_tablet_id(); 
      const ObIArray<ObITableEntity *> &cells = tablet_cells->get_cells();
      for (int64_t j = 0; OB_SUCC(ret) && j < cells.count(); j++) {
        ObITableEntity *cell = cells.at(j);
        ObString family_name;
        if (OB_ISNULL(cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null cell", K(ret));
        } else if (OB_FAIL(get_family_from_cell(*cell, family_name))) {
          LOG_WARN("fail to get family from cell", K(ret), KPC(cell));
        } else if (family_name.empty()) {
          // delete all family
          if (OB_FAIL(delete_all_family(*cell, table_group_name, exec_ctx, table_id, tablet_id))) {
            LOG_WARN("fail to delete all family", K(ret), KPC(cell), K(table_group_name), K(table_id), K(tablet_id));
          } else {}
        } else {
          // delete the given family
          if (OB_FAIL(delete_family(*cell, family_name, exec_ctx, table_id, tablet_id))) {
            LOG_WARN("fail to delete one family", K(ret), K(family_name), K(table_id), K(tablet_id));
          }
        }
      }
    }
  } 
  return ret;
}

int ObHbaseMultiCFService::delete_all_family(const ObITableEntity &del_cell, const ObString &table_group_name,
                                             ObTableExecCtx &exec_ctx, const uint64_t table_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard& schema_guard = exec_ctx.get_schema_guard();
  ObTableApiCredential &credential = exec_ctx.get_credential();
  ObSEArray<const ObSimpleTableSchemaV2 *, 4> table_schemas;
  table_schemas.set_attr(ObMemAttr(MTL_ID(), "AllCFSchemas"));
  ObTableQuery table_query;
  ObHbaseQuery query(table_query, false);

  if (OB_FAIL(construct_query(del_cell, exec_ctx, query))) {
    LOG_WARN("fail to cons query from del entity", K(ret), K(del_cell));
  } else if (OB_FAIL(ObTableQueryUtils::get_table_schemas(schema_guard, table_group_name, true,
                  credential.tenant_id_, credential.database_id_, table_schemas))) {
    LOG_WARN("fail to get all column family table schemas", K(ret), K(table_group_name),
      K(credential.tenant_id_), K(credential.database_id_));
  } else {
    uint64_t real_table_id = OB_INVALID_ID;
    ObTabletID real_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table schema", K(ret));
      } else if (OB_FAIL(find_real_table_tablet_id(exec_ctx, table_id, tablet_id, *table_schema,
          real_table_id, real_tablet_id))) {
        LOG_WARN("fail to find real tablet id", K(ret), K(table_id), K(tablet_id));
      } else {
        query.set_table_id(real_table_id);
        query.set_tablet_id(real_tablet_id);
        if (OB_FAIL(ObHbaseColumnFamilyService::del(query, exec_ctx))) {
          LOG_WARN("fail to delete by query", K(ret), K(query));
        } else {}
      }
    }
  }
  return ret;
}

int ObHbaseMultiCFService::delete_family(const ObITableEntity &del_cell, const ObString &family_name,
                                             ObTableExecCtx &exec_ctx, const uint64_t table_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTableQuery table_query;
  ObIArray<ObTabletID> &tablet_ids = table_query.get_tablet_ids();
  ObHbaseQuery query(table_query, false);
  uint64_t real_table_id = OB_INVALID_ID;
  ObTabletID real_tablet_id;
  if (OB_FAIL(find_real_table_tablet_id(exec_ctx, table_id, tablet_id, family_name, real_table_id, real_tablet_id))) {
    LOG_WARN("fail to find real tablet id", K(ret), K(table_id), K(tablet_id), K(family_name));
  } else if (OB_FAIL(tablet_ids.push_back(real_tablet_id))) {
    LOG_WARN("fail to add real tablet id", K(ret), K(real_table_id));
  } else if (OB_FAIL(remove_family_from_qualifier(del_cell))) {
    LOG_WARN("fail to remove family from qualifier", K(ret), K(del_cell));
  } else if (OB_FAIL(construct_query(del_cell, exec_ctx, query))) {
    LOG_WARN("fail to cons query from del entity", K(ret));
  } else {
    query.set_table_id(real_table_id);
    query.set_tablet_id(real_tablet_id);
    if (OB_FAIL(ObHbaseColumnFamilyService::del(query, exec_ctx))) {
      LOG_WARN("fail to delete by query", K(ret), K(query));
    } else {}
  }

  return ret;
}

int ObHbaseMultiCFService::query(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx, ObHbaseQueryResultIterator *&result_iter)
{
  int ret = OB_SUCCESS;
  ObHbaseMultiCFIterator *tmp_result_iter = nullptr;
  if (OB_ISNULL(tmp_result_iter = OB_NEWx(ObHbaseMultiCFIterator, &exec_ctx.get_allocator(), query, exec_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_result_iter->init())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to init result iter", K(ret));
    }
  } else {
    result_iter = tmp_result_iter;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_result_iter)) {
    tmp_result_iter->close();
    tmp_result_iter->~ObHbaseMultiCFIterator();
    exec_ctx.get_allocator().free(tmp_result_iter);
  }
  return ret;
}


} // end of namespace table
} // end of namespace oceanbase
