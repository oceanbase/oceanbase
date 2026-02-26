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
#include "ob_i_adapter.h"
#include "observer/table/ob_table_cg_service.h"

namespace oceanbase
{
namespace table
{

int ObIHbaseAdapter::init_table_ctx(ObTableExecCtx &exec_ctx,
                                    const ObITableEntity &cell,
                                    ObTableOperationType::Type op_type,
                                    ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  bool can_use_put = true;
  ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
  const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
  tb_ctx.set_operation_type(op_type);
  tb_ctx.set_entity(&cell);
  tb_ctx.set_entity_type(ObTableEntityType::ET_HKV);
  tb_ctx.set_schema_guard(&schema_guard);
  if (OB_NOT_NULL(exec_ctx.get_simple_schema()) && exec_ctx.get_table_id() == exec_ctx.get_simple_schema()->get_table_id()) {
    simple_table_schema = exec_ctx.get_simple_schema();
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(), exec_ctx.get_table_id(), simple_table_schema))) {
    LOG_WARN("fail to get simple table schema", K(ret), K(exec_ctx.get_table_id()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(simple_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("simple table schema is null", K(ret));
  } else {
    tb_ctx.set_simple_table_schema(simple_table_schema);
    tb_ctx.set_schema_cache_guard(&exec_ctx.get_schema_cache_guard());
    tb_ctx.set_sess_guard(&exec_ctx.get_sess_guard());
    tb_ctx.set_audit_ctx(exec_ctx.get_audit_ctx());
    if (tb_ctx.is_init()) {
      LOG_INFO("tb ctx has been inited", K(tb_ctx));
    } else if (OB_FAIL(tb_ctx.init_common_without_check(exec_ctx.get_credential(), cell.get_tablet_id(), exec_ctx.get_timeout_ts()))) {
      LOG_WARN("fail to init table ctx common", K(ret), K(cell.get_tablet_id()));
    } else if (OB_FAIL(tb_ctx.check_tablet_id_valid())) {
      LOG_WARN("fail to check tablet_id", K(ret), K(tb_ctx.get_tablet_id()));
    } else if (op_type == ObTableOperationType::INSERT_OR_UPDATE && OB_FAIL(tb_ctx.check_insert_up_can_use_put(can_use_put))) {
        LOG_WARN("fail to check htable put can use table api put", K(ret));
    } else {
      switch (op_type) {
        case ObTableOperationType::DEL: {
          if (OB_FAIL(tb_ctx.init_delete())) {
            LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx));
          }
          break;
        }
        case ObTableOperationType::INSERT_OR_UPDATE: {
          if (can_use_put) {
            if (OB_FAIL(tb_ctx.init_put())) {
              LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx));
            }
          } else if (OB_FAIL(tb_ctx.init_insert_up(false))) {
            LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected operation type", "type", op_type);
          break;
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
    } else if (OB_FAIL(tb_ctx.init_trans(exec_ctx.get_trans_param().trans_desc_,
                                         exec_ctx.get_trans_param().tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret));
    } else {
      tb_ctx.set_init_flag(true);
    }
  }
  return ret;
}

int ObIHbaseAdapter::init_scan(ObTableExecCtx &exec_ctx,
                               const ObTableQuery &query,
                               ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTabletID> &tablet_ids = query.get_tablet_ids();
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  bool is_weak_read = exec_ctx.get_trans_param().consistency_level_ == ObTableConsistencyLevel::EVENTUAL;
  tb_ctx.set_scan(true);
  tb_ctx.set_entity_type(ObTableEntityType::ET_HKV);
  tb_ctx.set_schema_cache_guard(&exec_ctx.get_schema_cache_guard());
  ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
  tb_ctx.set_schema_guard(&schema_guard);
  tb_ctx.set_sess_guard(&exec_ctx.get_sess_guard());
  tb_ctx.set_audit_ctx(exec_ctx.get_audit_ctx());
  if (tb_ctx.is_init()) {
    ret = OB_INIT_TWICE;
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_NOT_NULL(exec_ctx.get_simple_schema()) &&
             exec_ctx.get_table_id() == exec_ctx.get_simple_schema()->get_table_id()) {
    table_schema = exec_ctx.get_simple_schema();
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(), exec_ctx.get_table_id(), table_schema))) {
    LOG_WARN("fail to get simple table schema", K(ret), K(exec_ctx.get_table_id()));
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tb_ctx.set_simple_table_schema(table_schema))){
  } else if (tablet_ids.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ids is empty", K(ret));
  } else if (OB_FAIL(tb_ctx.init_common_without_check(exec_ctx.get_credential(), tablet_ids.at(0), exec_ctx.get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret));
  } else if (OB_FAIL(tb_ctx.init_scan(query, is_weak_read, exec_ctx.get_table_id(), true /* skip_get_ls */))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(is_weak_read), K(exec_ctx.get_table_id()));
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(exec_ctx.get_trans_param().trans_desc_,
                                       exec_ctx.get_trans_param().tx_snapshot_))) {
    LOG_WARN("fail to init trans", K(ret));
  } else {
    tb_ctx.set_read_latest(!exec_ctx.get_trans_param().is_readonly_);
    tb_ctx.set_init_flag(true);
  }

  return ret;
}

void ObIHbaseAdapter::reuse()
{
  allocator_.reuse();
}

} // end of namespace table
} // end of namespace oceanbase