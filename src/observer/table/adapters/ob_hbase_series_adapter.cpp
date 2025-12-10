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
#include "ob_hbase_series_adapter.h"
#include "observer/table/tableapi/ob_table_api_service.h"
#include "lib/xml/ob_binary_aggregate.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_json.h"

namespace oceanbase
{
namespace table
{

bool ObHSeriesAdapter::can_use_put(ObTableCtx &ctx, bool is_inrow_series)
{
  bool bret = false;
  if (ctx.is_total_quantity_log() || ctx.has_secondary_index()) {
    bret = false;
  } else if (is_inrow_series) {
    bret = true;
  }
  return bret;
}

int ObHSeriesAdapter::put(ObTableExecCtx &ctx, const ObITableEntity &cell)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult result;
  ObTableEntity series_cell;
  SMART_VAR(ObTableCtx, tb_ctx, allocator_)
  {
    bool is_inrow_series = true;
    if (OB_FAIL(init_table_ctx(ctx, cell, ObTableOperationType::INSERT_OR_UPDATE, tb_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (FALSE_IT(lob_inrow_threshold_ = tb_ctx.get_lob_inrow_threshold())) {
    } else if (OB_FAIL(convert_normal_to_series(cell, series_cell, is_inrow_series))) {
      LOG_WARN("fail to convert normal to series", K(ret), K(cell));
    } else if (can_use_put(tb_ctx, is_inrow_series)) {
      tb_ctx.set_client_use_put(true);
      if (OB_FAIL(ObTableApiService::put(tb_ctx, series_cell, result))) { // opt: inrow lob can use put
        LOG_WARN("fail to insert in hbase series adapter", K(ret), K(cell), K(series_cell));
      } else {
        LOG_DEBUG("put success", K(ret), K(ctx.get_table_name()), K(series_cell));
      }
    } else if (OB_FAIL(ObTableApiService::insert(tb_ctx, series_cell, result))) {  // outrowlob can not use put
      LOG_WARN("fail to insert in hbase series adapter", K(ret), K(cell), K(series_cell));
    } else {
      LOG_DEBUG("insert success", K(ret), K(ctx.get_table_name()), K(series_cell));
    }
  }
  return ret;
}

int ObHSeriesAdapter::put(ObTableCtx &ctx, const ObHCfRows &rows)
{
  int ret = OB_SUCCESS;
  bool is_inrow_series = true;
  ObFixedArray<ObTabletID, ObIAllocator> tablet_ids;
  ObFixedArray<const ObITableEntity*, ObIAllocator> normal_cells;
  ObFixedArray<const ObITableEntity*, ObIAllocator> series_cells;
  tablet_ids.set_allocator(&ctx.get_allocator());
  normal_cells.set_allocator(&ctx.get_allocator());
  series_cells.set_allocator(&ctx.get_allocator());
  ctx.set_batch_tablet_ids(&tablet_ids);
  ctx.set_batch_entities(&series_cells);

  if (OB_FAIL(tablet_ids.init(rows.get_cell_count()))) {
    LOG_WARN("fail to init tablet ids", K(ret), K(rows.get_cell_count()));
  } else if (OB_FAIL(normal_cells.init(rows.get_cell_count()))) {
    LOG_WARN("fail to init normal cells", K(ret), K(rows.get_cell_count()));
  } else if (OB_FAIL(series_cells.init(rows.get_cell_count()))) {
    LOG_WARN("fail to init series cells", K(ret), K(rows.get_cell_count()));
  }

  for (int64_t i = 0; i < rows.count() && OB_SUCC(ret); i++) {
    const ObHCfRow &row = rows.get_cf_row(i);
    for (int64_t j = 0; j < row.cells_.count() && OB_SUCC(ret); j++) {
      const ObHCell &cell = row.cells_.at(j);
      if (OB_FAIL(normal_cells.push_back(&cell))) {
        LOG_WARN("fail to push back normal cell", K(ret), K(cell));
      }
    }
  }

  if (OB_SUCC(ret)) {
    lob_inrow_threshold_ = ctx.get_lob_inrow_threshold();
    if (OB_FAIL(convert_normal_to_series(normal_cells, series_cells, tablet_ids, is_inrow_series))) {
      LOG_WARN("fail to convert normal to series", K(ret), K(normal_cells));
    } else if (series_cells.count() != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("series cells count is not equal to tablet ids count", K(ret), K(series_cells), K(tablet_ids));
    } else if (series_cells.count() == 1) {
      ObTableOperationResult result;
      ctx.set_tablet_id(tablet_ids.at(0));
      if (can_use_put(ctx, is_inrow_series)) {
        ctx.set_client_use_put(true);
        if (OB_FAIL(ObTableApiService::put(ctx, *series_cells.at(0), result))) {
          LOG_WARN("fail to put", K(ret), K(series_cells.at(0)));
        } else {
          LOG_DEBUG("put success", K(ret), K(ctx.get_table_name()), K(*series_cells.at(0)));
        }
      } else {
        if (OB_FAIL(ObTableApiService::insert(ctx, *series_cells.at(0), result))) {
          LOG_WARN("fail to insert", K(ret), K(series_cells.at(0)));
        } else {
          LOG_DEBUG("insert success", K(ret), K(ctx.get_table_name()), K(*series_cells.at(0)));
        }
      }
    } else {
      if (can_use_put(ctx, is_inrow_series)) {
        ctx.set_client_use_put(true);
        if (OB_FAIL(ObTableApiService::multi_put(ctx, series_cells))) {
          LOG_WARN("fail to multi put", K(ret), K(ctx), K(series_cells));
        } else {
          LOG_DEBUG("do multi_put success", K(ret), K(ctx), K(series_cells));
        }
      } else {
        if (OB_FAIL(ObTableApiService::multi_insert(ctx, series_cells))) {
          LOG_WARN("fail to multi insert", K(ret), K(ctx), K(series_cells));
        } else {
          LOG_DEBUG("do multi insert success", K(ret), K(ctx), K(series_cells));
        }
      }
    }
  }

  return ret;
}

int ObHSeriesAdapter::multi_put(ObTableExecCtx &ctx, const ObIArray<const ObITableEntity *> &cells)
{
  int ret = OB_SUCCESS;
  uint64_t cells_count = cells.count();
  uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(ret)) {
  } else if (cells_count <= 0) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("multy put cells is empty", K(ret), K(cells));
  } else {
    ObSEArray<const ObITableEntity *, 8> series_cells;
    ObSEArray<ObTabletID, 8> real_tablet_ids;
    series_cells.set_attr(ObMemAttr(tenant_id, "MulPutSerCel"));
    real_tablet_ids.set_attr(ObMemAttr(tenant_id, "MulPutSertblt"));
    SMART_VAR(ObTableCtx, tb_ctx, allocator_)
    {
      bool is_inrow_series = true;
      // tablet_ids需要调整
      if (OB_ISNULL(cells.at(0))) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("first cell is null", K(ret), K(cells));
      } else if (OB_FAIL(init_table_ctx(ctx, *series_cells.at(0), ObTableOperationType::INSERT_OR_UPDATE, tb_ctx))) {
        LOG_WARN("fail to init table ctx", K(ret));
      } else if (FALSE_IT(lob_inrow_threshold_ = tb_ctx.get_lob_inrow_threshold())) {
      } else if (OB_FAIL(convert_normal_to_series(cells, series_cells, real_tablet_ids, is_inrow_series))) {
        LOG_WARN("fail to convert normal to series", K(ret), K(cells));
      } else if (FALSE_IT(tb_ctx.set_batch_tablet_ids(&real_tablet_ids))) {
        LOG_WARN("fail to set batch tablet ids", K(ret));
      } else if (can_use_put(tb_ctx, is_inrow_series)) {
        tb_ctx.set_client_use_put(true);
        if (OB_FAIL(ObTableApiService::multi_put(tb_ctx, series_cells))) { // opt:inrow lob can use put
          LOG_WARN("fail to multi put", K(ret), K(tb_ctx), KPC(tb_ctx.get_batch_tablet_ids()), K(series_cells));
        } else {
          LOG_DEBUG("do multi_put success", K(ret), K(tb_ctx), KPC(tb_ctx.get_batch_tablet_ids()), K(series_cells));
        }
      } else if (OB_FAIL(ObTableApiService::multi_insert(tb_ctx, series_cells))) { // outrowlob can not use put
        LOG_WARN("fail to multi insert in hbase series adapter", K(ret),
                                                                 K(series_cells),
                                                                 KPC(tb_ctx.get_batch_tablet_ids()),
                                                                 K(real_tablet_ids));
      } else {
        LOG_DEBUG("do multi insert success", K(ret), K(tb_ctx), KPC(tb_ctx.get_batch_tablet_ids()), K(series_cells));
      }
    }
  }
  return ret;
}

int ObHSeriesAdapter::construct_query(ObTableExecCtx &ctx,
                                      const ObITableEntity &entity,
                                      ObTableQuery &table_query)
{
  int ret = OB_SUCCESS;
  table_query.clear_scan_range();
  ObNewRange scan_range;
  ObObj *start_obj = nullptr;
  ObObj *end_obj = nullptr;
  table_query.set_limit(-1);
  table_query.set_offset(0);
  table_query.set_scan_order(common::ObQueryFlag::Forward);
  ObObj k_obj, t_obj;
  if (OB_ISNULL(start_obj = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end_obj = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for end_obj failed", K(ret));
  } else if (OB_FAIL(entity.get_rowkey_value(ObHTableConstants::COL_IDX_K, k_obj))) {
    LOG_WARN("fail to get rowkey", K(ret), K(entity));
  } else if (OB_FAIL(entity.get_rowkey_value(ObHTableConstants::COL_IDX_T, t_obj))) {
    LOG_WARN("fail to get rowkey", K(ret), K(entity));
  } else if (OB_FAIL(ob_write_obj(allocator_, k_obj, start_obj[ObHTableConstants::COL_IDX_K]))) {
    LOG_WARN("fail to write start k obj", K(ret), K(entity));
  } else if (OB_FAIL(ob_write_obj(allocator_, t_obj, start_obj[ObHTableConstants::COL_IDX_SER_T]))) {
    LOG_WARN("fail to write start t obj", K(ret), K(entity));
  } else if (OB_FAIL(ob_write_obj(allocator_, k_obj, end_obj[ObHTableConstants::COL_IDX_K]))) {
    LOG_WARN("fail to write end k obj", K(ret), K(entity));
  } else if (OB_FAIL(ob_write_obj(allocator_, t_obj, end_obj[ObHTableConstants::COL_IDX_SER_T]))) {
    LOG_WARN("fail to write end t obj", K(ret), K(entity));
  } else {
    start_obj[ObHTableConstants::COL_IDX_S].set_min_value();
    end_obj[ObHTableConstants::COL_IDX_S].set_max_value();
    scan_range.start_key_.assign(start_obj, ObHTableConstants::HTABLE_ROWKEY_SIZE);
    scan_range.end_key_.assign(end_obj, ObHTableConstants::HTABLE_ROWKEY_SIZE);
    if (OB_FAIL(table_query.add_scan_range(scan_range))) {
      LOG_WARN("fail to add scan range", K(ret), K(scan_range), K(table_query));
    } else if (OB_FAIL(table_query.get_tablet_ids().push_back(entity.get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret));
    }
  }
  return ret;
}

int ObHSeriesAdapter::get_query_iter(ObTableExecCtx &ctx,
                                     const ObITableEntity &entity,
                                     ObTableCtx &scan_ctx,
                                     ObTableApiRowIterator &tb_row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table query should be null", K(ret));
  } else if (OB_ISNULL(table_query_ = OB_NEWx(ObTableQuery, (&allocator_)))) { // will be freed in del
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new table query", K(ret));
  } else if (OB_FAIL(construct_query(ctx, entity, *table_query_))) {
    LOG_WARN("fail to construct query", K(ret), K(ctx), K(entity));
  } else if (OB_FAIL(init_scan(ctx, *table_query_, scan_ctx))) {
    LOG_WARN("fail to init scan table ctx", K(ret));
  } else if (FALSE_IT(scan_ctx.set_batch_tablet_ids(table_query_->get_tablet_ids_ptr()))) {
  } else if (OB_FAIL(ObTableApiService::query(scan_ctx, tb_row_iter))) {
    LOG_WARN("fail to query in hbase series adapter", K(ret), KPC(table_query_));
  } else if (OB_FAIL(tb_row_iter.open())) {
    LOG_WARN("fail to open table api row iter", K(ret), KPC(table_query_));
  }

  return ret;
}

int ObHSeriesAdapter::del_and_insert(ObIAllocator &alloc,
                                     ObJsonNode &json_node,
                                     ObTableCtx &del_ctx,
                                     ObTableCtx &ins_ctx,
                                     ObNewRow &json_cell)
{
  int ret = OB_SUCCESS;
  // 1. delete
  ObTableEntity del_entity;
  ObTableOperationResult result;
  if (OB_FAIL(ObHTableUtils::construct_entity_from_row(json_cell, *del_ctx.get_schema_cache_guard(), del_entity))) {
    LOG_WARN("fail to construct entity from row", K(ret), K(del_entity), K(json_cell));
  } else if (OB_FAIL(ObTableApiService::del(del_ctx, del_entity, result))) {
    LOG_WARN("fail to del in hbase series adapter", K(ret), K(json_cell));
  } else if (json_node.element_count() != 0) {
    // 2. construct new entity
    // 注意，如果删除这个q，没有其他node的话，就不需要insert了
    ObObj value_obj;
    result.reset();
    ObTableEntity series_cell;
    if (OB_FAIL(add_series_rowkey(series_cell,
                                  json_cell.get_cell(ObHTableConstants::COL_IDX_K),
                                  json_cell.get_cell(ObHTableConstants::COL_IDX_SER_T),
                                  json_cell.get_cell(ObHTableConstants::COL_IDX_S)))) {
      LOG_WARN("fail to add series rowkey", K(ret), K(json_cell));
    } else if (OB_FAIL(construct_series_value(alloc, json_node, value_obj))) {
      LOG_WARN("fail to construct series value", K(ret), K(json_node));
    } else if (OB_FAIL(series_cell.set_property(ObHTableConstants::VALUE_CNAME_STR, value_obj))) {
      LOG_WARN("fail to set property", K(ret), K(json_cell));
    } else if (OB_FAIL(ObTableApiService::insert(ins_ctx, series_cell, result))) {
      LOG_WARN("fail to insert in hbase series adapter", K(ret), K(json_cell));
    }
  }
  return ret;
}

int ObHSeriesAdapter::del(ObTableExecCtx &ctx, const ObITableEntity &cell)
{
  int ret = OB_SUCCESS;
  ObTableCtx *scan_ctx = nullptr;
  ObTableCtx *del_ctx = nullptr;
  ObTableCtx *ins_ctx = nullptr;
  ObTableApiRowIterator tb_row_iter;
  if (OB_ISNULL(scan_ctx = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new scan table ctx", K(ret));
  } else if (OB_ISNULL(del_ctx = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new del table ctx", K(ret));
  } else if (OB_ISNULL(ins_ctx = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new insert table ctx", K(ret));
  } else if (OB_FAIL(init_table_ctx(ctx, cell, ObTableOperationType::DEL, *del_ctx))) {
    LOG_WARN("fail to init del table ctx", K(ret));
  } else if (FALSE_IT(del_ctx->set_skip_scan(true))) {
  } else if (OB_FAIL(init_table_ctx(ctx, cell, ObTableOperationType::INSERT_OR_UPDATE, *ins_ctx))) {
    LOG_WARN("fail to init insert table ctx", K(ret));
  } else if (OB_FAIL(get_query_iter(ctx, cell, *scan_ctx, tb_row_iter))) {
    LOG_WARN("fail to get table query iter", K(ret), KPC(scan_ctx));
  } else {
    ObNewRow *json_cell = nullptr;
    ObObj qualifier_obj;
    ObArenaAllocator alloc("SerAdapTreAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    while (OB_SUCC(ret) && OB_SUCC(tb_row_iter.get_next_row(json_cell))) {
      if (OB_ISNULL(json_cell) || json_cell->get_count() < 4) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("json cell is null or less than 4", K(ret), KPC(json_cell));
      } else if (cell.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get qualifier", K(ret), K(json_cell));
      } else {
        const ObString &qualifier = qualifier_obj.get_varchar();
        ObObj &json_obj = json_cell->get_cell(ObHTableConstants::COL_IDX_V);
        if (!json_obj.has_lob_header() || !is_lob_storage(json_obj.get_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json obj is not lob", K(ret), K(json_obj));
        } else if (OB_FAIL(ObTableCtx::read_real_lob(alloc, json_obj))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to read real lob", K(ret), K(json_obj));
        } else {
          ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_val_len(), &alloc);
          ObJsonNode *json_tree = NULL;
          uint64_t origin_count = 0;
          if (OB_FAIL(bin.reset_iter())) {
            LOG_WARN("fail to reset iter", K(ret), K(json_obj));
          } else if (OB_FAIL(bin.to_tree(json_tree))) {
            LOG_WARN("fail to to tree", K(ret), K(json_obj));
          } else if (OB_ISNULL(json_tree)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json tree is null", K(ret), K(json_obj));
          } else if (FALSE_IT(origin_count = json_tree->element_count())) {
          } else if (origin_count == 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("series value origin count is 0", K(ret), K(json_obj));
          } else if (OB_FAIL(json_tree->object_remove(qualifier))) {
            LOG_WARN("fail to remove", K(ret), K(qualifier));
          } else if (json_tree->element_count() != origin_count) {
            if (OB_FAIL(del_and_insert(alloc, *json_tree, *del_ctx, *ins_ctx, *json_cell))) {
              LOG_WARN("fail to del and insert", K(ret), K(qualifier));
            }
          }
          OB_DELETEx(ObJsonNode, &alloc, json_tree);
        }
      }
      json_cell = nullptr;
      alloc.reuse();
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;  // ret overwrite
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret), KPC(scan_ctx));
    }
  }
  tb_row_iter.close();
  OB_DELETEx(ObTableCtx, &allocator_, scan_ctx);
  OB_DELETEx(ObTableCtx, &allocator_, del_ctx);
  OB_DELETEx(ObTableCtx, &allocator_, ins_ctx);
  OB_DELETEx(ObTableQuery, &allocator_, table_query_); // allocated in get_query_iter

  return ret;
}

int ObHSeriesAdapter::save_and_adjust_range(ObHbaseSeriesCellIter *&iter,
                                            ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObHbaseSeriesCellIter *series_iter = static_cast<ObHbaseSeriesCellIter *>(iter);
  if (OB_ISNULL(series_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("series iter is null", K(ret), K(iter));
  } else {
    ObTableCtx &tb_ctx = series_iter->get_tb_ctx();
    ObNewRange &origin_range = series_iter->get_origin_range();
    if (tb_ctx.get_key_ranges().count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid key ranges", K(ret), K(tb_ctx));
    } else {
      ObNewRange &scan_range = tb_ctx.get_key_ranges().at(0);
      origin_range.border_flag_ = scan_range.border_flag_;
      if (OB_FAIL(scan_range.start_key_.deep_copy(origin_range.start_key_, alloc))) {
        LOG_WARN("fail to deep copy start key", K(ret), K(scan_range));
      } else if (OB_FAIL(scan_range.end_key_.deep_copy(origin_range.end_key_, alloc))) {
        LOG_WARN("fail to deep copy end key", K(ret), K(scan_range));
      } else if (!scan_range.start_key_.is_min_row()) {
        if (scan_range.start_key_.get_obj_cnt() < ObHTableConstants::HTABLE_ROWKEY_SIZE) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid hbase start key", K(ret), K(scan_range));
        } else {
          scan_range.start_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_SER_T].set_min_value();
          scan_range.start_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_S].set_min_value();
        }
      } else if (!scan_range.end_key_.is_max_row()) {
        if (scan_range.end_key_.get_obj_cnt() < ObHTableConstants::HTABLE_ROWKEY_SIZE) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid hbase end key", K(ret), K(scan_range));
        } else {
          scan_range.end_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_SER_T].set_max_value();
          scan_range.end_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_S].set_max_value();
        }
      }
      if (scan_range.start_key_.get_obj_cnt() > tb_ctx.get_column_info_array().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start key obj cnt is greater than column info array count", K(ret), K(scan_range));
      } else if (scan_range.end_key_.get_obj_cnt() > tb_ctx.get_column_info_array().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("end key obj cnt is greater than column info array count", K(ret), K(scan_range));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < scan_range.start_key_.get_obj_cnt(); i++) {
          ObObj &obj = const_cast<ObObj&>(scan_range.start_key_.get_obj_ptr()[i]);
          if (obj.is_min_value() || obj.is_max_value()) {
          // do nothing
        } else if (OB_ISNULL(tb_ctx.get_column_info_array().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is null", K(ret), K(i));
        } else if (OB_FAIL(tb_ctx.adjust_column_type(*tb_ctx.get_column_info_array().at(i), obj))) {
          LOG_WARN("fail to adjust column type", K(ret), K(i), K(obj));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < scan_range.end_key_.get_obj_cnt(); i++) {
          ObObj &obj = const_cast<ObObj&>(scan_range.end_key_.get_obj_ptr()[i]);
          if (obj.is_min_value() || obj.is_max_value()) {
          // do nothing
          } else if (OB_ISNULL(tb_ctx.get_column_info_array().at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column info is null", K(ret), K(i));
          } else if (OB_FAIL(tb_ctx.adjust_column_type(*tb_ctx.get_column_info_array().at(i), obj))) {
            LOG_WARN("fail to adjust column type", K(ret), K(i), K(obj));
          }
        }
      }
    }
  }
  return ret;
}

int ObHSeriesAdapter::scan(ObIAllocator &alloc,
                           ObTableExecCtx &ctx,
                           const ObTableQuery &query,
                           ObHbaseICellIter *&iter)
{
  int ret = OB_SUCCESS;
  ObHbaseSeriesCellIter *tmp_iter = nullptr;
  if (OB_ISNULL(tmp_iter = OB_NEWx(ObHbaseSeriesCellIter, (&alloc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new hbase series cell iter", K(ret), K(query));
  } else {
    ObTableCtx &tb_ctx = tmp_iter->get_tb_ctx();
    ObTableApiRowIterator &tb_row_iter = tmp_iter->get_table_api_scan_iter();
    if (OB_FAIL(init_scan(ctx, query, tb_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (FALSE_IT(tb_ctx.set_batch_tablet_ids(&query.get_tablet_ids()))) {
    } else if (OB_FAIL(save_and_adjust_range(tmp_iter, alloc))) {
      LOG_WARN("fail to adjust range", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableApiService::query(tb_ctx, tb_row_iter))) {
      LOG_WARN("fail to query in hbase series adapter", K(ret), K(query));
    } else {
      iter = tmp_iter;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_iter)) {
    tmp_iter->~ObHbaseSeriesCellIter();
    tmp_iter = nullptr;
  }
  LOG_DEBUG("ObHSeriesAdapter::scan",
    K(ret), K(query), K(ctx), K(tmp_iter->get_tb_ctx()));
  return ret;
}

int ObHSeriesAdapter::get_normal_rowkey(const ObITableEntity &normal_cell,
                                        ObObj &rowkey_obj,
                                        ObObj &qualifier_obj,
                                        ObObj &timestamp_obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(normal_cell.get_rowkey_value(ObHTableConstants::COL_IDX_K, rowkey_obj))) {
    LOG_WARN("failed to get rowkey", K(ret));
  } else if (OB_FAIL(normal_cell.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier_obj))) {
    LOG_WARN("failed to get qualifier", K(ret));
  } else if (OB_FAIL(normal_cell.get_rowkey_value(ObHTableConstants::COL_IDX_T, timestamp_obj))) {
    LOG_WARN("failed to get timestamp", K(ret));
  }
  return ret;
}

int ObHSeriesAdapter::add_series_rowkey(ObITableEntity &series_cell,
                                        const ObObj &rowkey_obj,
                                        const ObObj &timestamp_obj,
                                        const ObObj &seq_obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(series_cell.add_rowkey_name(ObHTableConstants::ROWKEY_CNAME_STR))) {
    LOG_WARN("failed to push back rowkey name", K(ret));
  } else if (OB_FAIL(series_cell.add_rowkey_name(ObHTableConstants::VERSION_CNAME_STR))) {
    LOG_WARN("failed to push back version name", K(ret));
  } else if (OB_FAIL(series_cell.add_rowkey_name(ObHTableConstants::SEQ_CNAME_STR))) {
    LOG_WARN("failed to push back seq name", K(ret));
  } else if (OB_FAIL(series_cell.add_rowkey_value(rowkey_obj))) {
    LOG_WARN("failed to add rowkey value", K(ret), K(rowkey_obj));
  } else if (OB_FAIL(series_cell.add_rowkey_value(timestamp_obj))) {
    LOG_WARN("failed to add timestamp value", K(ret), K(timestamp_obj));
  } else if (OB_FAIL(series_cell.add_rowkey_value(seq_obj))) {
    LOG_WARN("failed to add seq value", K(ret), K(seq_obj));
  }
  return ret;
}

int ObHSeriesAdapter::convert_normal_to_series(const ObITableEntity &cell,
                                               ObITableEntity &series_cell,
                                               bool &is_inrow_series)
{
  int ret = OB_SUCCESS;
  int64_t curr_time = -ObTimeUtility::current_time() / 1000; // ms
  ObObj seq_obj(curr_time), rowkey_obj, qualifier_obj, timestamp_obj, value_obj, new_value;
  // construct series cell
  if (OB_FAIL(get_normal_rowkey(cell, rowkey_obj, qualifier_obj, timestamp_obj))) {
    LOG_WARN("failed to get normal cell rowkey", K(ret));
  } else if (OB_FAIL(cell.get_property(ObHTableConstants::VALUE_CNAME_STR, value_obj))) {
    LOG_WARN("failed to get value", K(ret));
  } else if (OB_FAIL(add_series_rowkey(series_cell, rowkey_obj, timestamp_obj, seq_obj))) {
    LOG_WARN("failed to push back series rowkey", K(ret));
  } else {
    // construct series value
    ObJsonBinSerializer bin_serializer(&allocator_);
    ObJsonObject json_obj(&allocator_);
    json_obj.set_use_lexicographical_order();
    ObJsonString json_value(value_obj.get_string_ptr(), value_obj.get_string_len());
    ObStringBuffer str_buf(&allocator_);
    if (OB_FAIL(json_obj.add(qualifier_obj.get_varchar(), &json_value))) {
      LOG_WARN("failed to add value", K(ret), K(qualifier_obj), K(json_value));
    } else if (OB_FAIL(bin_serializer.serialize_json_object(&json_obj, str_buf))) {
      LOG_WARN("failed to append value", K(ret), K(value_obj));
    } else if (FALSE_IT(new_value.set_lob_value(ObObjType::ObJsonType, str_buf.ptr(), str_buf.length()))) {
    } else if (OB_FAIL(series_cell.set_property(ObHTableConstants::VALUE_CNAME_STR, new_value))) {
      LOG_WARN("failed to set property", K(ret), K(new_value));
    } else {
      if (str_buf.length() > lob_inrow_threshold_) {
        is_inrow_series = false;
      } else {
        is_inrow_series = true;
      }
      LOG_DEBUG("convert_normal_to_series", K(ret), K(is_inrow_series), K(str_buf.length()), K(lob_inrow_threshold_));
    }
  }
  return ret;
}

int ObHSeriesAdapter::convert_normal_to_series(const ObIArray<const ObITableEntity *> &cells,
                                               ObIArray<const ObITableEntity *> &series_cells,
                                               ObIArray<common::ObTabletID> &real_tablet_ids,
                                               bool &is_inrow_series)
{
  int ret = OB_SUCCESS;
  int64_t curr_time = -ObTimeUtility::current_time() / 1000; // ms
  ObObj seq_obj(curr_time);
  is_inrow_series = true;
  if (!kt_agg_map_.created()) {
    if (OB_FAIL(kt_agg_map_.create(cells.count(), "SeriKTAggMap"))) {
      LOG_WARN("failed to create json obj map", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(release_map())) {
    LOG_WARN("failed to reuse json obj map", K(ret));
  } else {
    // 1. agg same kt entity
    for (int i = 0; OB_SUCC(ret) && i < cells.count(); i++) {
      ObObj rowkey_obj, qualifier_obj, timestamp_obj, value_obj;
      const ObITableEntity *entity = cells.at(i);
      if (OB_ISNULL(entity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("entity is null", K(ret));
      } else if (OB_FAIL(get_normal_rowkey(*entity, rowkey_obj, qualifier_obj, timestamp_obj))) {
        LOG_WARN("failed to get normal cell rowkey", K(ret));
      } else if (OB_FAIL(entity->get_property(ObHTableConstants::VALUE_CNAME_STR, value_obj))) {
        LOG_WARN("failed to get value", K(ret));
      } else {
        KTNode kt_node(rowkey_obj.get_varchar(), timestamp_obj.get_int(), entity->get_tablet_id());
        // get old val
        QVMap *qv_map = nullptr;
        if (OB_FAIL(kt_agg_map_.get_refactored(kt_node, qv_map))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            if (OB_ISNULL(qv_map = OB_NEWx(QVMap, (&allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to new q_t_map", K(ret));
            } else if (!qv_map->created() && OB_FAIL(qv_map->create(cells.count(), "SeriQVMap"))) {
              LOG_WARN("failed to create q_t_map", K(ret));
            } else if (OB_FAIL(kt_agg_map_.set_refactored(kt_node, qv_map))) {
              LOG_WARN("failed to set kt_agg_map_", K(ret), K(kt_node));
            }
          } else {
            LOG_WARN("failed to get json obj map", K(ret), K(kt_node));
          }
        }
        if (OB_SUCC(ret)) {
          ObString str_val;
          if (value_obj.is_null()) {
            // To ensure compatibility with native HBase
            value_obj.set_varbinary(ObString());
          }
          if (OB_FAIL(qv_map->get_refactored(qualifier_obj.get_varchar(), str_val))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              if (OB_FAIL(qv_map->set_refactored(qualifier_obj.get_varchar(), value_obj.get_varchar()))) {
                LOG_WARN("failed to set q_t_map", K(ret), K(kt_node));
              }
            } else {
              LOG_WARN("failed to get json obj map", K(ret), K(kt_node));
            }
          } else {
            QVUpdateOp update_op(value_obj.get_varchar());
            if (OB_FAIL(qv_map->set_or_update(qualifier_obj.get_varchar(), value_obj.get_varchar(), update_op))) {
              LOG_WARN("failed to erase q_t_map", K(ret), K(kt_node));
            }
          }
        }
      }
    }

    // 2. construct series cells
    for (KTAggMap::const_iterator i = kt_agg_map_.begin(); OB_SUCC(ret) && i != kt_agg_map_.end(); ++i) {
      ObTableEntity *series_cell = NULL;
      KTNode node = i->first;
      ObObj rowkey_obj, timestamp_obj(node.time_), value_obj;
      rowkey_obj.set_varchar(node.key_);
      bool is_cell_inrow = true;
      if (OB_ISNULL(series_cell = OB_NEWx(ObTableEntity, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to new table entity", K(ret));
      } else if (OB_FAIL(add_series_rowkey(*series_cell, rowkey_obj, timestamp_obj, seq_obj))) {
        LOG_WARN("failed to push back series rowkey", K(ret));
      } else {
        QVMap *qv_map = i->second;
        ObJsonObject json_obj(&allocator_);
        json_obj.set_use_lexicographical_order();
        for (QVMap::const_iterator j = qv_map->begin(); OB_SUCC(ret) && j != qv_map->end(); ++j) {
          ObJsonString *json_node = nullptr;
          if (OB_ISNULL(json_node = OB_NEWx(ObJsonString, (&allocator_), j->second))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to new json string", K(ret));
          } else if (OB_FAIL(json_obj.add(j->first, json_node))) {
            LOG_WARN("failed to add value", K(ret), K(j->first), KPC(json_node));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(construct_series_value(allocator_, json_obj, value_obj, is_cell_inrow))) {
          LOG_WARN("failed to construct json obj", K(ret), K(qv_map));
        } else if (OB_FAIL(series_cell->set_property(ObHTableConstants::VALUE_CNAME_STR, value_obj))) {
          LOG_WARN("failed to set property", K(ret), K(value_obj));
        } else if (FALSE_IT(series_cell->set_tablet_id(node.tablet_id_))) {
          LOG_WARN("failed to set tablet_id", K(ret), K(node.tablet_id_));
        } else if (OB_FAIL(real_tablet_ids.push_back(node.tablet_id_))) {
          LOG_WARN("fail to push back tablet_id", K(ret), K(node.tablet_id_));
        } else if (OB_FAIL(series_cells.push_back(const_cast<ObTableEntity*>(series_cell)))) {
          LOG_WARN("fail to push back series cell", K(ret), KPC(series_cell));
        } else if (!is_cell_inrow) {
          is_inrow_series = false;
        }
      }
    }
  }
  return ret;
}

int ObHSeriesAdapter::construct_series_value(ObIAllocator &allocator, ObJsonNode &json_obj, ObObj &value_obj, bool &is_inrow_series)
{
  int ret = OB_SUCCESS;
  // construct series value
  ObJsonBuffer json_buf(&allocator_);
  ObString str_out, json_data;
  if (OB_FAIL(ObJsonReassembler::prepare_lob_common(json_buf))) {
    LOG_WARN("prepare_lob_common fail", K(ret));
  } else if (OB_FAIL(ObJsonBin::add_doc_header_v0(json_buf))) {
    LOG_WARN("add_doc_header_v0 fail", K(ret));
  } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(&json_obj, json_buf, false /*enable_reserialize*/))) {
    LOG_WARN("serialize json binary fail", K(ret));
  } else if (OB_FAIL(json_buf.get_result_string(str_out))) {
    LOG_WARN("get_result_string fail", K(ret), K(json_buf));
  } else if (OB_FALSE_IT(json_data.assign_ptr(str_out.ptr() + sizeof(ObLobCommon),
                                              str_out.length() - sizeof(ObLobCommon)))) {
  } else if (OB_FAIL(ObJsonBin::set_doc_header_v0(json_data,
                                                  json_data.length(),
                                                  json_obj.use_lexicographical_order()))) {
    LOG_WARN("set_doc_header_v0 fail", K(ret));
  } else {
    str_out.length() > lob_inrow_threshold_ ? is_inrow_series = false : is_inrow_series = true;
    value_obj.set_lob_value(ObObjType::ObJsonType, str_out.ptr(), str_out.length());
    value_obj.set_has_lob_header();
  }
  return ret;
}

int ObHSeriesAdapter::construct_series_value(ObIAllocator &allocator, ObJsonNode &json_obj, ObObj &value_obj)
{
  int ret = OB_SUCCESS;
  bool is_inrow_series = true; // unused
  if (OB_FAIL(construct_series_value(allocator, json_obj, value_obj, is_inrow_series))) {
    LOG_WARN("failed to construct series value", K(ret), K(json_obj));
  }
  return ret;
}

int ObHSeriesAdapter::release_map()
{
  int ret = OB_SUCCESS;
  if (kt_agg_map_.created()) {
    for (KTAggMap::const_iterator i = kt_agg_map_.begin(); i != kt_agg_map_.end(); ++i) {
      QVMap *qv_map = i->second;
      if (OB_NOT_NULL(qv_map)) {
        qv_map->~QVMap();
      }
    }
    if (OB_FAIL(kt_agg_map_.reuse())) {
      LOG_WARN("failed to reuse json obj map", K(ret));
    }
    allocator_.reuse();
  }
  return ret;
}

/**
 * ---------------------------------------- KTNode ----------------------------------------
 */

int KTNode::hash(uint64_t &res) const
{
  res = 0;
  res = key_.hash(res);
  res = murmurhash(&time_, sizeof(int64_t), res);
  return OB_SUCCESS;
}

} // end of namespace table
} // end of namespace oceanbase
