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

#include "share/datum/ob_datum_util.h"

#include "ob_operator_factory.h"
#include "ob_operator_reg.h"
#include "ob_operator.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_link.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/engine/basic/ob_limit_op.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_link_scan.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_log_stat_collector.h"
#include "sql/optimizer/ob_log_optimizer_stats_gathering.h"
#include "sql/engine/aggregate/ob_merge_distinct_op.h"
#include "sql/engine/aggregate/ob_hash_distinct_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/basic/ob_topk_op.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/basic/ob_count_op.h"
#include "sql/engine/basic/ob_values_op.h"
#include "sql/engine/set/ob_hash_union_op.h"
#include "sql/engine/set/ob_hash_intersect_op.h"
#include "sql/engine/set/ob_hash_except_op.h"
#include "sql/engine/set/ob_merge_union_op.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all_op.h"
#include "sql/engine/recursive_cte/ob_fake_cte_table_op.h"
#include "sql/engine/set/ob_merge_intersect_op.h"
#include "sql/engine/set/ob_merge_except_op.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"
#include "sql/engine/dml/ob_table_merge_op.h"
#include "sql/engine/dml/ob_table_delete_op.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/dml/ob_table_lock_op.h"
#include "sql/engine/dml/ob_table_insert_up_op.h"
#include "sql/engine/dml/ob_table_replace_op.h"
#include "sql/engine/dml/ob_link_dml_op.h"
#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "sql/engine/subquery/ob_subplan_scan_op.h"
#include "sql/engine/subquery/ob_unpivot_op.h"
#include "sql/engine/join/ob_merge_join_op.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/basic/ob_monitoring_dump_op.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/sequence/ob_sequence_op.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/px/exchange/ob_px_ms_receive_op.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_reduce_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_fifo_coord_op.h"
#include "sql/engine/px/exchange/ob_px_ordered_coord_op.h"
#include "sql/engine/px/exchange/ob_px_ms_coord_op.h"
#include "sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/table/ob_table_row_store_op.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/table/ob_row_sample_scan_op.h"
#include "sql/engine/table/ob_block_sample_scan_op.h"
#include "sql/engine/table/ob_table_scan_with_index_back_op.h"
#include "sql/executor/ob_direct_receive_op.h"
#include "sql/executor/ob_direct_transmit_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_delete_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_insert_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_update_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/basic/ob_temp_table_transformation_op.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/dml/ob_err_log_op.h"
#include "sql/engine/basic/ob_select_into_op.h"
#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/basic/ob_json_table_op.h"
#include "sql/engine/table/ob_link_scan_op.h"
#include "sql/engine/dml/ob_table_insert_all_op.h"
#include "sql/engine/basic/ob_stat_collector_op.h"
#include "sql/engine/opt_statistics/ob_optimizer_stats_gathering_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

template <int TYPE>
    struct AllocSpecHelper;
template <int TYPE>
    struct AllocOpHelper;
template <int TYPE>
    struct AllocInputHelper;
template <int TYPE>
    struct GenSpecHelper;

int report_not_registered()
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("not registered", K(ret));
  return ret;
}

// alloc functions for unregistered operator, always return OB_ERR_UNEXPECTED;
// should never be called.
template <>
struct AllocSpecHelper<0>
{
  static int alloc(ObIAllocator &, const ObPhyOperatorType, const int64_t, ObOpSpec *&)
  { return report_not_registered(); }
};

template <>
struct AllocOpHelper<0>
{
  static int alloc(ObIAllocator &, ObExecContext &,
                   const ObOpSpec &, ObOpInput *, const int64_t , ObOperator *&)
  { return report_not_registered(); }

};

template <>
struct AllocInputHelper<0>
{
  static int alloc(ObIAllocator &, ObExecContext &,
                   const ObOpSpec &, ObOpInput *&)
  { return report_not_registered(); }
};

template <>
struct GenSpecHelper<0>
{
  static int generate(ObStaticEngineCG &, ObLogicalOperator &, ObOpSpec &, const bool)
  { return report_not_registered(); }
};

// alloc functions for registered operator
template <int TYPE>
struct AllocSpecHelper
{
  static int alloc(ObIAllocator &alloc, const ObPhyOperatorType type,
                   const int64_t child_cnt, ObOpSpec *&spec)
  {
    int ret = OB_SUCCESS;
    typedef typename op_reg::ObOpTypeTraits<TYPE>::Spec SpecType;
    if (type != TYPE || child_cnt < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(type), LITERAL_K(TYPE), K(child_cnt));
    } else {
      const int64_t alloc_size = child_cnt * sizeof(SpecType *) + sizeof(SpecType);
      ObOpSpec **mem = static_cast<ObOpSpec **>(alloc.alloc(alloc_size));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
      } else {
        memset(mem, 0, sizeof(SpecType *) * child_cnt);
        spec = new (&mem[child_cnt]) SpecType(alloc, type);
        if (OB_FAIL(spec->set_children_pointer(mem, child_cnt))) {
          LOG_WARN("set children pointer failed", K(ret));
          spec->~ObOpSpec();
          spec = NULL;
          alloc.free(mem);
        }
      }
    }
    return ret;
  }
};

template <int TYPE>
struct AllocOpHelper
{
  static int alloc(ObIAllocator &alloc, ObExecContext &exec_ctx,
                   const ObOpSpec &spec, ObOpInput *input,
                   const int64_t child_cnt, ObOperator *&op)
  {
    int ret = OB_SUCCESS;
    typedef typename op_reg::ObOpTypeTraits<TYPE> Traints;
    typedef typename Traints::Op OpType;
    if ((Traints::has_input_ && NULL == input)
        || (!Traints::has_input_ && NULL != input)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("input argument mismatch with registered status",
               K(ret), KP(input), LITERAL_K(Traints::has_input_));
    } else if (child_cnt < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), LITERAL_K(TYPE), K(child_cnt));
    } else {
      const int64_t alloc_size = child_cnt * sizeof(OpType *) + sizeof(OpType);
      ObOperator **mem = static_cast<ObOperator **>(alloc.alloc(alloc_size));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
      } else {
        memset(mem, 0, sizeof(OpType *) * child_cnt);
        op = new (&mem[child_cnt]) OpType(exec_ctx, spec, input);
        if (OB_FAIL(op->set_children_pointer(mem, child_cnt))
            || OB_FAIL(op->init())) {
          LOG_WARN("set children pointer or init failed", K(ret));
          op->~ObOperator();
          op = NULL;
          alloc.free(mem);
        }
      }
    }
    return ret;
  }
};

template <int TYPE>
struct AllocInputHelper
{
  static int alloc(ObIAllocator &alloc, ObExecContext &exec_ctx,
                   const ObOpSpec &spec, ObOpInput *&input)
  {
    int ret = OB_SUCCESS;
    typedef typename op_reg::ObOpTypeTraits<TYPE>::Input InputType;
    input = static_cast<InputType *>(alloc.alloc(sizeof(InputType)));
    if (OB_ISNULL(input)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      input = new (input) InputType(exec_ctx, spec);
    }
    return ret;
  }

};

template <int TYPE>
struct GenSpecHelper
{
  static int generate(ObStaticEngineCG &cg, ObLogicalOperator &log_op, ObOpSpec &spec,
                      const bool in_root_job)
  {
    int ret = OB_SUCCESS;
    typedef typename op_reg::ObOpTypeTraits<TYPE>::LogOp LogType;
    typedef typename op_reg::ObOpTypeTraits<TYPE>::Spec SpecType;
    OB_ASSERT(spec.type_ == TYPE);
    // Check type again, to make sure the registered type is correct.
    // In code generation, performance loss of dynamic cast is acceptable.
    LogType *derived_op = dynamic_cast<LogType *>(&log_op);
    SpecType *derived_spec = dynamic_cast<SpecType *>(&spec);
    if (OB_ISNULL(derived_op) || OB_ISNULL(derived_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("class type mismatch, "
               "please check the registered type is correct",
               K(ret), KP(derived_op), KP(derived_spec),
               K(log_op.get_name()), K(ob_phy_operator_type_str(spec.type_)));
    } else if (OB_FAIL(cg.generate_spec(*derived_op, *derived_spec, in_root_job))) {
      LOG_WARN("generate operator specification failed",
               K(ret), K(log_op.get_name()), K(ob_phy_operator_type_str(spec.type_)));
    }

    return ret;
  }
};

// define && init alloc function array
static ObOperatorFactory::AllocFun G_ALLOC_FUNCTION_ARRAY[PHY_END];
static_assert(PHY_END == ARRAYSIZEOF(G_ALLOC_FUNCTION_ARRAY),
              "alloc function array is too small");

ObOperatorFactory::AllocFun *ObOperatorFactory::G_ALL_ALLOC_FUNS_ = G_ALLOC_FUNCTION_ARRAY;

static bool G_VECTORIZED_OP_ARRAY[PHY_END];
bool *ObOperatorFactory::G_VECTORIZED_OP_ARRAY_ = G_VECTORIZED_OP_ARRAY;
static uint64_t G_OB_VERSION_ARRAY[PHY_END];
uint64_t *ObOperatorFactory::G_OB_VERSION_ARRAY_ = G_OB_VERSION_ARRAY;

template <int N>
struct InitAllocFunc
{
  static void init_array()
  {
    static constexpr int registered = op_reg::ObOpTypeTraits<N>::registered_;
    static constexpr int has_input = op_reg::ObOpTypeTraits<N>::has_input_;
    G_ALLOC_FUNCTION_ARRAY[N] = ObOperatorFactory::AllocFun {
          (registered ? &AllocSpecHelper<N * registered>::alloc : NULL),
          (registered ? &AllocOpHelper<N * registered>::alloc : NULL),
          (has_input ? &AllocInputHelper<N * has_input>::alloc : NULL),
          (registered ? &GenSpecHelper<N * registered>::generate : NULL)
    };

    G_VECTORIZED_OP_ARRAY[N] = op_reg::ObOpTypeTraits<N>::vectorized_;
    G_OB_VERSION_ARRAY[N] = op_reg::ObOpTypeTraits<N>::ob_version_;
  }
};



bool G_ALLOC_FUNC_SET = common::ObArrayConstIniter<PHY_END, InitAllocFunc>::init();

int ObOperatorFactory::alloc_op_spec(ObIAllocator &alloc, const ObPhyOperatorType type,
                                     const int64_t child_cnt, ObOpSpec *&spec)
{
  int ret = OB_SUCCESS;
  if (!is_registered(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("static engine not implement", K(type), K(ret));
  } else if (child_cnt < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child cnt", K(ret), K(child_cnt), K(type));
  } else if (OB_FAIL(G_ALLOC_FUNCTION_ARRAY[type].spec_func_(
              alloc, type, child_cnt, spec))) {
    LOG_WARN("allocate operator specification failed", K(ret));
  }
  return ret;
}

int ObOperatorFactory::alloc_operator(ObIAllocator &alloc, ObExecContext &exec_ctx,
                                      const ObOpSpec &spec, ObOpInput *input,
                                      const int64_t child_cnt, ObOperator *&op)
{
  const ObPhyOperatorType type = spec.type_;
  int ret = OB_SUCCESS;
  if (child_cnt < 0 || !is_registered(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child cnt or operator not registered, "
             "please register in ob_operator_reg.h with REGISTER_OPERATOR",
             K(ret), K(child_cnt), K(type));
  } else if (OB_FAIL(G_ALLOC_FUNCTION_ARRAY[type].op_func_(
              alloc, exec_ctx, spec, input, child_cnt, op))) {
    LOG_WARN("allocate operator failed", K(ret));
  }

  return ret;
}

int ObOperatorFactory::alloc_op_input(ObIAllocator &alloc, ObExecContext &exec_ctx,
                                      const ObOpSpec &spec,
                                      ObOpInput *&input)
{
  int ret = OB_SUCCESS;
  const ObPhyOperatorType type = spec.type_;
  if (!has_op_input(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator input not registered, "
             "please register in ob_operator_reg.h with REGISTER_OPERATOR",
             K(ret), K(type));
  } else if (OB_FAIL(G_ALLOC_FUNCTION_ARRAY[type].input_func_(
              alloc, exec_ctx, spec, input))) {
    LOG_WARN("allocate operator input failed", K(ret));
  }
  return ret;
}

int ObOperatorFactory::generate_spec(ObStaticEngineCG &cg,
                                     ObLogicalOperator &log_op, ObOpSpec &spec,
                                     const bool in_root_job)
{
  const ObPhyOperatorType type = spec.type_;
  int ret = OB_SUCCESS;
  if (!is_registered(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child cnt or operator not registered, "
             "please register in ob_operator_reg.h with REGISTER_OPERATOR",
             K(ret), K(type));
  } else if (OB_FAIL(G_ALLOC_FUNCTION_ARRAY[type].gen_spec_func_(
              cg, log_op, spec, in_root_job))) {
    LOG_WARN("generate operator spec failed", K(type), K(ret));
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
