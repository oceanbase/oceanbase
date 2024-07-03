/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_optimizer_stats_gathering_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "share/stat/ob_opt_stat_sql_service.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_dbms_stats_executor.h"
#include "pl/sys_package/ob_dbms_stats.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObOptimizerStatsGatheringSpec::ObOptimizerStatsGatheringSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      part_level_(schema::PARTITION_LEVEL_ZERO),
      calc_part_id_expr_(NULL),
      table_id_(OB_INVALID_ID),
      type_(OSG_TYPE::GATHER_OSG),
      target_osg_id_(OB_INVALID_ID),
      generated_column_exprs_(alloc),
      col_conv_exprs_(alloc),
      column_ids_(alloc),
      online_sample_rate_(1.)
{
}

int ObOptimizerStatsGatheringSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (type_ == OSG_TYPE::GATHER_OSG) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void *buf = ctx.get_allocator().alloc(sizeof(ObOptStatsGatherWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocated memory", K(ret));
      } else {
        ObOptStatsGatherWholeMsg::WholeMsgProvider *provider =
          new (buf)ObOptStatsGatherWholeMsg::WholeMsgProvider();
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_OPT_STATS_GATHER_WHOLE_MSG, *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObOptimizerStatsGatheringSpec, ObOpSpec),
                    part_level_,
                    calc_part_id_expr_,
                    table_id_,
                    type_,
                    target_osg_id_,
                    generated_column_exprs_,
                    col_conv_exprs_,
                    column_ids_,
                    online_sample_rate_);

ObOptimizerStatsGatheringOp::ObOptimizerStatsGatheringOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    tenant_id_(OB_INVALID_ID),
    table_stats_map_(),
    osg_col_stats_map_(),
    part_map_(),
    piece_msg_(),
    arena_("ObOptStatGather")
{
}

void ObOptimizerStatsGatheringOp::destroy()
{
  reset();
  ObOperator::destroy();
}

void ObOptimizerStatsGatheringOp::reset()
{
  FOREACH(it, osg_col_stats_map_) {
    if (OB_NOT_NULL(it->second)) {
      it->second->~ObOptOSGColumnStat();
      it->second = NULL;
    }
  }
  table_stats_map_.destroy();
  osg_col_stats_map_.destroy();
  part_map_.destroy();
  piece_msg_.reset();
  arena_.reset();
  sample_helper_.reset();
}

int ObOptimizerStatsGatheringOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  FOREACH(it, osg_col_stats_map_) {
    if (OB_NOT_NULL(it->second)) {
      it->second->~ObOptOSGColumnStat();
      it->second = NULL;
    }
  }
  table_stats_map_.reuse();
  osg_col_stats_map_.reuse();
  arena_.reset();
  sample_helper_.reset();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan");
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::inner_open()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *tab_schema = nullptr;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_virtual_table_ctx().schema_guard_;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(tenant_id_ = ctx_.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id_, MY_SPEC.table_id_, tab_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(MY_SPEC.table_id_));
  } else if (OB_ISNULL(tab_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret));
  } else {
    arena_.set_tenant_id(tenant_id_);
    piece_msg_.set_tenant_id(tenant_id_);
    sample_helper_.init(MY_SPEC.online_sample_rate_);
    int64_t map_size = MY_SPEC.column_ids_.count();
    if (OB_FAIL(table_stats_map_.create(map_size,
        "TabStatBucket",
        "TabStatNode"))) {
      LOG_WARN("fail to create table stats map", K(ret));
    } else if (OB_FAIL(osg_col_stats_map_.create(map_size,
        "ColStatBucket",
        "ColStatNode"))) {
      LOG_WARN("fail to create column stats map", K(ret));
    }
    LOG_TRACE("succeed to open optimizer_stats_gathering op",
              K(ret), K(map_size), K(MY_SPEC.column_ids_.count()), K(MY_SPEC.table_id_));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  } else {
    reset();
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END == ret) {
      if (MY_SPEC.type_ == OSG_TYPE::GATHER_OSG) {
        if (OB_FAIL(send_stats())) {
          LOG_WARN("failed to send stats", K(ret));
        }
      } else if (MY_SPEC.type_ != OSG_TYPE::GATHER_OSG) {
        if (OB_FAIL(msg_end())) {
          LOG_WARN("failed to call msg end", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ret = OB_ITER_END;
      }
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (MY_SPEC.type_ != OSG_TYPE::MERGE_OSG && OB_FAIL(calc_stats())) {
    LOG_WARN("fail to calc stats", K(ret));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (child_brs->end_ && 0 == child_brs->size_) {
  } else if (MY_SPEC.type_ != OSG_TYPE::MERGE_OSG) {
    // set the index of output.
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
      if (child_brs->skip_->exist(i)) {
      } else {
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(calc_stats())) {
          LOG_WARN("fail to calc stats", K(ret), K(i), K(child_brs->size_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(brs_.copy(child_brs))) {
      LOG_WARN("copy child_brs to brs_ failed", K(ret));
    } else if (brs_.end_) {
      if (MY_SPEC.type_ == OSG_TYPE::GATHER_OSG) {
        if (OB_FAIL(send_stats())) {
          LOG_WARN("failed to send stats", K(ret));
        }
      } else if (MY_SPEC.type_ != OSG_TYPE::GATHER_OSG) {
        if (OB_FAIL(msg_end())) {
          LOG_WARN("failed to call msg end", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::send_stats()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  const ObOptStatsGatherWholeMsg *whole_msg = NULL;
  if (OB_ISNULL(handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("do not need sqc in serial mode");
  } else if (OB_FAIL(build_piece_msg(piece_msg_, handler->get_sqc_proxy()))) {
    LOG_WARN("failed to build piece msg", K(ret));
  } else if (OB_FAIL(handler->get_sqc_proxy().get_dh_msg(
                MY_SPEC.id_, dtl::DH_OPT_STATS_GATHER_WHOLE_MSG, piece_msg_, whole_msg,
                ctx_.get_physical_plan_ctx()->get_timeout_timestamp(), true, false))) {
    LOG_WARN("get msg failed", K(ret), K(MY_SPEC.id_), K(piece_msg_));
  } else {
    LOG_DEBUG("SUCCESS to send piece msg", K(ret), K(piece_msg_));
    //after send we need to reset table_stat_map and column_stat_map
    piece_msg_.reset();
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::build_piece_msg(ObOptStatsGatherPieceMsg &piece,
                                                 ObPxSQCProxy &proxy)
{
  int ret = OB_SUCCESS;
  piece.op_id_ = MY_SPEC.id_;
  piece.thread_id_ = GETTID();
  piece.source_dfo_id_ = proxy.get_dfo_id();
  piece.target_dfo_id_ =  proxy.get_dfo_id();
  piece.target_osg_id_ = MY_SPEC.target_osg_id_;
  if (OB_FAIL(get_tab_stats(piece.table_stats_))) {
    LOG_WARN("fail to get table stats", K(ret));
  } else if (OB_FAIL(get_col_stats(piece.column_stats_))) {
    LOG_WARN("fail to get column stats", K(ret));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::get_tab_stat_by_key(ObOptTableStat::Key &key, ObOptTableStat *&tab_stat)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  if (OB_FAIL(table_stats_map_.get_refactored(key, tab_stat))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("failed to find in hashmap", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(ptr = arena_.alloc(sizeof(ObOptTableStat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(ptr));
      } else {
        tab_stat = new (ptr) ObOptTableStat();
        tab_stat->set_table_id(MY_SPEC.table_id_);
        tab_stat->set_partition_id(key.partition_id_);
        if (OB_FAIL(table_stats_map_.set_refactored(key, tab_stat))) {
          LOG_WARN("fail to insert into hash map", K(key), KPC(tab_stat));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("GET tab_stat", KPC(tab_stat));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::get_col_stat_by_key(ObOptColumnStat::Key &key, ObOptOSGColumnStat *&osg_col_stat)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(osg_col_stats_map_.get_refactored(key, osg_col_stat))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("failed to find in hashmap", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(osg_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(arena_)) ||
                OB_ISNULL(osg_col_stat->col_stat_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create new col stat");
      } else {
        osg_col_stat->col_stat_->set_table_id(MY_SPEC.table_id_);
        osg_col_stat->col_stat_->set_partition_id(key.partition_id_);
        osg_col_stat->col_stat_->set_column_id(key.column_id_);
        if (OB_FAIL(osg_col_stats_map_.set_refactored(key, osg_col_stat))) {
          LOG_WARN("fail to insert into hash map", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("GET col_stat", K(key), KPC(osg_col_stat));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::calc_column_stats(ObExpr *expr, uint64_t column_id, int64_t &row_len)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  int64_t col_len  = 0;
  ObOptOSGColumnStat *global_col_stat = NULL;
  ObOptColumnStat::Key global_col_stats_key(tenant_id_, MY_SPEC.table_id_, MY_SPEC.table_id_, column_id);
  if (MY_SPEC.is_part_table()) {
    global_col_stats_key.partition_id_ = -1;
  }
  if (OB_ISNULL(expr) || OB_ISNULL(expr->basic_funcs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (OB_FAIL(get_col_stat_by_key(global_col_stats_key, global_col_stat))) {
    LOG_WARN("fail to get global table stat", K(ret));
  } else if (OB_ISNULL(global_col_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!ObColumnStatParam::is_valid_opt_col_type(expr->obj_meta_.get_type())) {
    // do nothing yet, should use the plain stats.
  } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
    LOG_WARN("failed to eval expr", K(*expr));
  } else if (OB_ISNULL(datum) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(ObExprSysOpOpnsize::calc_sys_op_opnsize(expr, datum, col_len))) {
    LOG_WARN("fail to calc sys op opnsize", K(ret));
  } else if (OB_FALSE_IT(global_col_stat->col_stat_->set_stat_level(StatLevel::TABLE_LEVEL))) {
  } else if (OB_FAIL(global_col_stat->update_column_stat_info(datum, expr->obj_meta_,
                                                              expr->basic_funcs_->null_first_cmp_))) {
    LOG_WARN("fail to set global column stat", K(ret));
  } else {
    row_len += col_len;
    LOG_TRACE("succed to calc column stat", KPC(expr), K(row_len), KPC(datum));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::calc_columns_stats(int64_t &row_len)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.column_ids_.count() != MY_SPEC.col_conv_exprs_.count() + MY_SPEC.generated_column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids doesn't match the output", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.col_conv_exprs_.count(); i++) {
      uint64_t column_id = MY_SPEC.column_ids_.at(i);
      if (OB_FAIL(calc_column_stats(MY_SPEC.col_conv_exprs_.at(i), column_id, row_len))) {
        LOG_WARN("fail to calc column stats", K(ret));
      }
    }
    //generated column
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.generated_column_exprs_.count(); i++) {
      uint64_t column_id = MY_SPEC.column_ids_.at(i + MY_SPEC.col_conv_exprs_.count());
      if (OB_FAIL(calc_column_stats(MY_SPEC.generated_column_exprs_.at(i), column_id, row_len))) {
        LOG_WARN("fail to calc column stats", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::calc_table_stats(int64_t &row_len, bool is_sample_row)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *global_tab_stat = NULL;
  ObOptTableStat::Key global_key(tenant_id_, MY_SPEC.table_id_, (int64_t)MY_SPEC.table_id_);
  if (MY_SPEC.is_part_table()) {
    global_key.partition_id_ = -1;
  }
  if (OB_FAIL(get_tab_stat_by_key(global_key, global_tab_stat))) {
    LOG_WARN("fail to get global table stat", K(ret));
  } else if (OB_ISNULL(global_tab_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    if (!is_sample_row) {
      global_tab_stat->add_avg_row_size(row_len);
      global_tab_stat->add_sample_size(1);
    }
    global_tab_stat->add_row_count(1);
    global_tab_stat->set_object_type(StatLevel::TABLE_LEVEL);
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::calc_stats()
{
  int ret = OB_SUCCESS;
  int64_t row_len = 0;
  bool ignore = false;
  if (OB_FAIL(sample_helper_.sample_row(ignore))) {
    LOG_WARN("failed to sample row", K(ret));
  } else if (!ignore &&
             OB_FAIL(calc_columns_stats(row_len))) {
    LOG_WARN("failed to calc column stats", K(ret));
  } else if (OB_FAIL(calc_table_stats(row_len, ignore))) {
    LOG_WARN("failed to calc table stats", K(ret));
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::merge_tab_stat(ObOptTableStat *src_tab_stat)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *tab_stat = NULL;
  if (OB_ISNULL(src_tab_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else {
    ObOptTableStat::Key stat_key(tenant_id_, src_tab_stat->get_table_id(), src_tab_stat->get_partition_id());
    if (OB_FAIL(table_stats_map_.get_refactored(stat_key, tab_stat))) {
      void *ptr = NULL;
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("failed to find in hashmap", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_ISNULL(ptr = arena_.alloc(sizeof(ObOptTableStat)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(ptr));
        } else if (OB_FAIL(src_tab_stat->deep_copy((char*)ptr, sizeof(ObOptTableStat), tab_stat))) {
          LOG_WARN("fail to copy tab_stat", K(ret));
        } else if (OB_ISNULL(tab_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to copy tab_stat", K(ret));
        } else if (OB_FAIL(table_stats_map_.set_refactored(stat_key, tab_stat))) {
          LOG_WARN("fail to insert stats idx to map", K(ret));
        }
      }
    } else {
      if (OB_FAIL(tab_stat->merge_table_stat(*src_tab_stat))) {
        LOG_WARN("fail to merge two table stats", K(ret), K(tab_stat), K(src_tab_stat));
      }
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::merge_col_stat(ObOptColumnStat *src_col_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_col_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else {
    ObOptColumnStat *col_stat = NULL;
    ObOptOSGColumnStat *osg_col_stat = NULL;
    ObOptColumnStat::Key stat_key(tenant_id_,
                                  src_col_stat->get_table_id(),
                                  src_col_stat->get_partition_id(),
                                  src_col_stat->get_column_id());
    if (OB_FAIL(osg_col_stats_map_.get_refactored(stat_key, osg_col_stat))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("failed to find in hashmap", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_ISNULL(osg_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(arena_)) ||
            OB_ISNULL(osg_col_stat->col_stat_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create osg col stat");
        } else if (OB_FAIL(osg_col_stat->col_stat_->deep_copy(*src_col_stat))) {
          LOG_WARN("fail to copy tab_stat", K(ret));
        } else if (OB_FAIL(osg_col_stats_map_.set_refactored(stat_key, osg_col_stat))) {
          LOG_WARN("fail to insert stats idx to map", K(ret));
        }
      }
    } else if (OB_ISNULL(osg_col_stat) || OB_ISNULL(osg_col_stat->col_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null");
    } else if (OB_FAIL(osg_col_stat->col_stat_->merge_column_stat(*src_col_stat))) {
      LOG_WARN("failed to merge column stat");
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::on_piece_msg(const ObOptStatsGatherPieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  // merge table_stats and column stats.
  if (MY_SPEC.type_ != OSG_TYPE::MERGE_OSG) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only MERGE_OSG support on_piece_msg");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_msg.table_stats_.count(); i++) {
      if (OB_FAIL(merge_tab_stat(piece_msg.table_stats_.at(i)))) {
        LOG_WARN("fail to merge table stat", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < piece_msg.column_stats_.count(); i++) {
      if (OB_FAIL(merge_col_stat(piece_msg.column_stats_.at(i)))) {
        LOG_WARN("fail to merge column stat", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("OSG merge piece msg", K(piece_msg));
      // why reset piece_msg_?
      piece_msg_.reset();
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::msg_end()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.type_ == OSG_TYPE::GATHER_OSG) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gather osg shouln't reach here", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
    ObTableStatParam param;
    ColStatIndMap col_stat_map;
    if (OB_ISNULL(schema_guard = ctx_.get_virtual_table_ctx().schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (OB_FAIL(generate_stat_param(param)))  {
      LOG_WARN("fail to generate param", K(ret));
    } else if (OB_FAIL(get_col_stat_map(col_stat_map))) {
      LOG_WARN("failed to get col stat map");
    } else if (OB_FAIL(ObDbmsStatsExecutor::update_online_stat(ctx_,
                                                              param,
                                                              schema_guard,
                                                              get_tab_stat_map(),
                                                              col_stat_map))) {
      LOG_WARN("fail to update tab/col stats", K(ret));
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::get_col_stat_map(ColStatIndMap &col_stat_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_stat_map.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has been created");
  } else if (OB_FAIL(col_stat_map.create(osg_col_stats_map_.size() == 0 ?
                                         1 : osg_col_stats_map_.size(),
                                         "ColStatMap"))) {
    LOG_WARN("failed to create col stat map");
  } else {
    FOREACH_X(it, osg_col_stats_map_, OB_SUCC(ret)) {
      ObOptOSGColumnStat *osg_col_stat = NULL;
      if (OB_ISNULL(osg_col_stat = it->second) || OB_ISNULL(osg_col_stat->col_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
        LOG_WARN("failed to persistence min max");
      } else if (OB_FAIL(col_stat_map.set_refactored(it->first, osg_col_stat->col_stat_))) {
        LOG_WARN("failed to set col stat");
      }
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::generate_stat_param(ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_virtual_table_ctx().schema_guard_;

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    param.tenant_id_ = tenant_id_;
    param.table_id_ = MY_SPEC.table_id_;
    param.global_stat_param_.need_modify_ = true;
    param.part_level_ = MY_SPEC.part_level_;
    param.allocator_ = &ctx_.get_allocator();
    if (!MY_SPEC.is_part_table()) {
      param.global_part_id_ = MY_SPEC.table_id_;
      param.global_tablet_id_ = MY_SPEC.table_id_;
      param.part_stat_param_.need_modify_ = false;
      param.subpart_stat_param_.need_modify_ = false;
    } else {
      param.part_stat_param_.need_modify_ = false;
      param.subpart_stat_param_.need_modify_ = false;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_ids_.count(); i++) {
      ObColumnStatParam col_param;
      col_param.column_id_ = MY_SPEC.column_ids_.at(i);
      const ObColumnSchemaV2 *col_schema =  nullptr;
      if (OB_FAIL(schema_guard->get_column_schema(tenant_id_, MY_SPEC.table_id_, col_param.column_id_, col_schema))) {
        LOG_WARN("can't get column schema", K(ret), K(tenant_id_), K(MY_SPEC.table_id_), K(col_param.column_id_));
      } else if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't get column schema", K(ret), K(tenant_id_), K(MY_SPEC.table_id_), K(col_param.column_id_));
      } else {
        col_param.cs_type_ = col_schema->get_collation_type();
      }
      if (OB_SUCC(ret) && OB_FAIL(param.column_params_.push_back(col_param))) {
        LOG_WARN("fail to push back column param", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::get_col_stats(common::ObIArray<ObOptColumnStat*>& col_stats)
{
  int ret = OB_SUCCESS;
  FOREACH_X(it, osg_col_stats_map_, OB_SUCC(ret)) {
    ObOptOSGColumnStat *osg_col_stat = NULL;
    if (OB_ISNULL(osg_col_stat = it->second) || OB_ISNULL(osg_col_stat->col_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
      LOG_WARN("failed to persistence min max");
    } else if (OB_FAIL(col_stats.push_back(osg_col_stat->col_stat_))) {
      LOG_WARN("failed to push back col stat");
    }
  }
  return ret;
}

int ObOptimizerStatsGatheringOp::get_tab_stats(common::ObIArray<ObOptTableStat*>& tab_stats)
{
  int ret = OB_SUCCESS;
  FOREACH_X(it, table_stats_map_, OB_SUCC(ret)) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (OB_FAIL(tab_stats.push_back(it->second))) {
      LOG_WARN("fail to push back col stats", K(ret));
    }
  }
  return ret;
}

}
}
