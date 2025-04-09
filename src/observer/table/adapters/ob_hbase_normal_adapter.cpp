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

#include "ob_hbase_normal_adapter.h"
#include "observer/table/tableapi/ob_table_api_service.h"
#include "ob_normal_adapter_iter.h"

namespace oceanbase
{
namespace table
{

int ObHNormalAdapter::put(ObTableExecCtx &ctx, const ObITableEntity &cell)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult result;
  SMART_VAR(ObTableCtx, tb_ctx, allocator_)
  {
    if (OB_FAIL(init_table_ctx(ctx, cell, ObTableOperationType::INSERT_OR_UPDATE, tb_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret), K(ctx), K(cell));
    } else if (tb_ctx.is_client_use_put()) {
      if (OB_FAIL(ObTableApiService::put(tb_ctx, cell, result))) {
        LOG_WARN("fail to put in hbase normal adapter", K(ret), K(cell));
      } else {
        LOG_DEBUG("put success", K(ret), K(ctx.get_table_name()), K(tb_ctx.get_tablet_id()), K(cell));
      }
    } else if (OB_FAIL(ObTableApiService::insert_or_update(tb_ctx, cell, result))) {
      LOG_WARN("fail to insert or update in hbase normal adapter", K(ret), K(cell));
    } else {
      LOG_DEBUG("put success", K(ret), K(ctx.get_table_name()), K(tb_ctx.get_tablet_id()), K(cell));
    }
  }
  return ret;
}

int ObHNormalAdapter::multi_put(ObTableExecCtx &ctx, const ObIArray<ObITableEntity *> &cells)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObSEArray<ObTabletID, 16> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(tenant_id, "TmpTbltIds"));
  for (int64_t i = 0; i < cells.count() && OB_SUCC(ret); i++) {
    ObITableEntity *cell = cells.at(i);
    if (OB_ISNULL(cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cell is null", K(ret));
    } else if (OB_FAIL(tablet_ids.push_back(cell->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (cells.count() <= 0) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("multi put cells is empty", K(ret), K(cells));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, allocator_)
    {
      if (OB_ISNULL(cells.at(0))) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("first cell is null", K(ret), K(cells));
      } else if (OB_FAIL(init_table_ctx(ctx, *cells.at(0), ObTableOperationType::INSERT_OR_UPDATE, tb_ctx))) {
        LOG_WARN("fail to init table ctx", K(ret), K(ctx), KPC(cells.at(0)));
      } else if (FALSE_IT(tb_ctx.set_batch_tablet_ids(&tablet_ids))) {
      } else if (tb_ctx.is_client_use_put()) {
        if (OB_FAIL(ObTableApiService::multi_put(tb_ctx, cells))) {
          LOG_WARN("fail to multi put in hbase normal adapter", K(ret), K(cells));
        } else {
          LOG_DEBUG("multi put success", K(ret), K(ctx.get_table_name()),
            KPC(tb_ctx.get_batch_tablet_ids()), K(cells));
        }
      } else if (OB_FAIL(ObTableApiService::multi_insert_or_update(tb_ctx, cells))) {
        LOG_WARN("fail to multi insert or update in hbase normal adapter", K(ret), K(cells));
      } else {
        LOG_DEBUG("multi put success", K(ret), K(ctx.get_table_name()),
            KPC(tb_ctx.get_batch_tablet_ids()), K(cells));
      }
    }
  }
  return ret;
}

int ObHNormalAdapter::del(ObTableExecCtx &ctx, const ObTabletID &tablet_id, const ObNewRow &cell)
{
  int ret = OB_SUCCESS;
  ObTableEntity entity;
  UNUSED(tablet_id);
  ObTabletID invalid_tablet_id(ObTabletID::INVALID_TABLET_ID);
  entity.set_tablet_id(invalid_tablet_id);
  if (OB_FAIL(ObHTableUtils::construct_entity_from_row(cell, ctx.get_schema_cache_guard(), entity))) {
    LOG_WARN("fail to construct entity from row", K(ret), K(cell));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, allocator_)
    {
      ObTableOperationResult result;
      ObSEArray<ObITableEntity *, 1> cells;
      cells.set_attr(ObMemAttr(MTL_ID(), "DelCells"));
      if (OB_FAIL(cells.push_back(&entity))) {
        LOG_WARN("fail to push back entity", K(ret), K(entity));
      } else if (OB_FAIL(init_table_ctx(ctx, entity, ObTableOperationType::DEL, tb_ctx))) {
        LOG_WARN("fail to init table ctx", K(ret), K(ctx), K(entity));
      } else if (FALSE_IT(tb_ctx.set_skip_scan(true))) {
      } else if (OB_FAIL(ObTableApiService::multi_delete(tb_ctx, cells))) {
        LOG_WARN("fail to multi del in hbase normal adapter", K(ret), K(cells));
      } else {
        LOG_DEBUG("delete success", K(ret), K(ctx.get_table_name()),
            K(tb_ctx.get_tablet_id()), K(entity));
      }
    }
  }
  return ret;
}

int ObHNormalAdapter::scan(ObIAllocator &alloc, ObTableExecCtx &ctx, const ObTableQuery &query, ObHbaseICellIter *&iter)
{
  int ret = OB_SUCCESS;
  ObHbaseNormalCellIter *tmp_iter = nullptr;
  if (OB_ISNULL(tmp_iter = OB_NEWx(ObHbaseNormalCellIter, (&alloc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new hbase normal cell iter", K(ret), K(query));
  } else {
    ObTableCtx &tb_ctx = tmp_iter->get_tb_ctx();
    ObTableApiRowIterator &tb_row_iter = tmp_iter->get_table_api_scan_iter();
    if (OB_FAIL(init_scan(ctx, query, tb_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (FALSE_IT(tb_ctx.set_batch_tablet_ids(&query.get_tablet_ids()))) {
    } else if (OB_FAIL(ObTableApiService::query(tb_ctx, tb_row_iter))) {
      LOG_WARN("fail to query in hbase normal adapter", K(ret), K(query));
    } else {
      LOG_DEBUG("query success", K(ret), K(ctx.get_table_name()),
          KPC(tb_ctx.get_batch_tablet_ids()), K(query));
      iter = tmp_iter;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_iter)) {
    tmp_iter->~ObHbaseNormalCellIter();
    tmp_iter = nullptr;
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase