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

#ifndef OCEANBASE_ENGINE_OB_OPERATOR_REG_H_
#define OCEANBASE_ENGINE_OB_OPERATOR_REG_H_

#include "sql/engine/ob_phy_operator_type.h"
#include <type_traits>

namespace oceanbase {
namespace sql {

namespace op_reg {

template <int>
struct ObOpTypeTraits {

  constexpr static int registered_ = false;
  constexpr static int has_input_ = false;
  typedef char LogOp;
  typedef char Spec;
  typedef char Op;
  typedef char Input;
};

template <typename T>
struct ObOpTraits {
  constexpr static int type_ = 0;
};

}  // end namespace op_reg

#define DEF_OP_TRAITS_true(type, op)   \
  template <>                          \
  struct ObOpTraits<op> {              \
    constexpr static int type_ = type; \
  };

#define DEF_OP_TRAITS_false(type, op)
#define DEF_OP_INPUT_TRAITS(has, type, input) DEF_OP_TRAITS_##has(type, input)

#define REGISTER_OPERATOR_FULL(log_op, type, op_spec, op, has_input, input)                                  \
  namespace op_reg {                                                                                         \
  template <>                                                                                                \
  struct ObOpTypeTraits<type> {                                                                              \
    constexpr static bool registered_ = true;                                                                \
    constexpr static bool has_input_ = has_input;                                                            \
    typedef log_op LogOp;                                                                                    \
    typedef op_spec Spec;                                                                                    \
    typedef op Op;                                                                                           \
    typedef input Input;                                                                                     \
  };                                                                                                         \
  DEF_OP_TRAITS_true(type, op_spec) DEF_OP_TRAITS_true(type, op) DEF_OP_INPUT_TRAITS(has_input, type, input) \
  }

#define REGISTER_OPERATOR_4(log_op, type, spec, op) REGISTER_OPERATOR_FULL(log_op, type, spec, op, false, char)
#define REGISTER_OPERATOR_5(log_op, type, spec, op, input) REGISTER_OPERATOR_FULL(log_op, type, spec, op, true, input)

#define REGISTER_OPERATOR___(n, ...) REGISTER_OPERATOR_##n(__VA_ARGS__)
#define REGISTER_OPERATOR__(...) REGISTER_OPERATOR___(__VA_ARGS__)
#define REGISTER_OPERATOR(...) REGISTER_OPERATOR__(ARGS_NUM(__VA_ARGS__), __VA_ARGS__)

template <typename T>
const typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_>::Spec& get_my_spec(const T& op)
{
  typedef typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_> Traits;
  static_assert(Traits::registered_, "only registered operator can call this function");
  return static_cast<const typename Traits::Spec&>(op.get_spec());
}

#define MY_SPEC get_my_spec(*this)
// to use MY_SPEC base operator, you need to override get_my_spec() like this:
// OB_INLINE const ObBaseSpec &get_my_spec(const ObBaseOp &op)
// {
//   return static_cast<ObBaseSpec &>(op.get_spec());
// }

template <typename T>
typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_>::Input& get_my_input(const T& op)
{
  typedef typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_> Traits;
  static_assert(Traits::has_input_, "only registered operator with operator input can call this function");
  // Dereferencing op.get_input() here is safe, because input pointer validity is checked in
  // ObOperatorFactory::alloc_operator if operator registered with input.
  return static_cast<typename Traits::Input&>(*op.get_input());
}

#define MY_INPUT get_my_input(*this)

// All operators should been registered here with REGISTER_OPERATOR macro:
//    REGISTER_OPERATOR(logic_operator, type, operator_spec, operator)
//  or:
//    REGISTER_OPERATOR(logic_operator, type, operator_spec, operator, operator_input)
//
// Forward declarations are sufficient, do not need to include definitions.

class ObLogLimit;
class ObLimitSpec;
class ObLimitOp;
REGISTER_OPERATOR(ObLogLimit, PHY_LIMIT, ObLimitSpec, ObLimitOp);

class ObLogDistinct;
class ObMergeDistinctSpec;
class ObMergeDistinctOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_MERGE_DISTINCT, ObMergeDistinctSpec, ObMergeDistinctOp);

class ObHashDistinctSpec;
class ObHashDistinctOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_HASH_DISTINCT, ObHashDistinctSpec, ObHashDistinctOp);

class ObLogMaterial;
class ObMaterialSpec;
class ObMaterialOp;
REGISTER_OPERATOR(ObLogMaterial, PHY_MATERIAL, ObMaterialSpec, ObMaterialOp);

class ObLogSort;
class ObSortSpec;
class ObSortOp;
REGISTER_OPERATOR(ObLogSort, PHY_SORT, ObSortSpec, ObSortOp);

class ObLogSet;
class ObHashUnionSpec;
class ObHashUnionOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_UNION, ObHashUnionSpec, ObHashUnionOp);

class ObLogSet;
class ObHashIntersectSpec;
class ObHashIntersectOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_INTERSECT, ObHashIntersectSpec, ObHashIntersectOp);

class ObLogSet;
class ObHashExceptSpec;
class ObHashExceptOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_EXCEPT, ObHashExceptSpec, ObHashExceptOp);

class ObLogSet;
class ObMergeUnionSpec;
class ObMergeUnionOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_UNION, ObMergeUnionSpec, ObMergeUnionOp);

class ObLogSet;
class ObRecursiveUnionAllSpec;
class ObRecursiveUnionAllOp;
REGISTER_OPERATOR(ObLogSet, PHY_RECURSIVE_UNION_ALL, ObRecursiveUnionAllSpec, ObRecursiveUnionAllOp);

class ObLogTableScan;
class ObFakeCTETableSpec;
class ObFakeCTETableOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_FAKE_CTE_TABLE, ObFakeCTETableSpec, ObFakeCTETableOp);

class ObLogTempTableAccess;
class ObTempTableAccessOpInput;
class ObTempTableAccessOpSpec;
class ObTempTableAccessOp;
REGISTER_OPERATOR(ObLogTempTableAccess, PHY_TEMP_TABLE_ACCESS, ObTempTableAccessOpSpec, ObTempTableAccessOp,
    ObTempTableAccessOpInput);

class ObLogTempTableInsert;
class ObTempTableInsertOpInput;
class ObTempTableInsertOpSpec;
class ObTempTableInsertOp;
REGISTER_OPERATOR(ObLogTempTableInsert, PHY_TEMP_TABLE_INSERT, ObTempTableInsertOpSpec, ObTempTableInsertOp,
    ObTempTableInsertOpInput);

class ObLogTempTableTransformation;
class ObTempTableTransformationOpSpec;
class ObTempTableTransformationOp;
REGISTER_OPERATOR(ObLogTempTableTransformation, PHY_TEMP_TABLE_TRANSFORMATION, ObTempTableTransformationOpSpec,
    ObTempTableTransformationOp);

class ObLogSet;
class ObMergeIntersectSpec;
class ObMergeIntersectOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_INTERSECT, ObMergeIntersectSpec, ObMergeIntersectOp);

class ObLogSet;
class ObMergeExceptSpec;
class ObMergeExceptOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_EXCEPT, ObMergeExceptSpec, ObMergeExceptOp);

class ObLogCount;
class ObCountSpec;
class ObCountOp;
REGISTER_OPERATOR(ObLogCount, PHY_COUNT, ObCountSpec, ObCountOp);

class ObLogValues;
class ObValuesSpec;
class ObValuesOp;
REGISTER_OPERATOR(ObLogValues, PHY_VALUES, ObValuesSpec, ObValuesOp);

class ObLogTableLookup;
class ObTableLookupSpec;
class ObTableLookupOp;
REGISTER_OPERATOR(ObLogTableLookup, PHY_TABLE_LOOKUP, ObTableLookupSpec, ObTableLookupOp);

class ObLogTableScan;
class ObTableScanOpInput;
class ObTableScanSpec;
class ObTableScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_TABLE_SCAN, ObTableScanSpec, ObTableScanOp, ObTableScanOpInput);

class ObLogTableScan;
class ObMultiPartTableScanOpInput;
class ObMultiPartTableScanSpec;
class ObMultiPartTableScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_MULTI_PART_TABLE_SCAN, ObMultiPartTableScanSpec, ObMultiPartTableScanOp,
    ObMultiPartTableScanOpInput);

class ObLogTableScan;
class ObRowSampleScanOpInput;
class ObRowSampleScanSpec;
class ObRowSampleScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_ROW_SAMPLE_SCAN, ObRowSampleScanSpec, ObRowSampleScanOp, ObRowSampleScanOpInput);

class ObLogTableScan;
class ObBlockSampleScanOpInput;
class ObBlockSampleScanSpec;
class ObBlockSampleScanOp;
REGISTER_OPERATOR(
    ObLogTableScan, PHY_BLOCK_SAMPLE_SCAN, ObBlockSampleScanSpec, ObBlockSampleScanOp, ObBlockSampleScanOpInput);

class ObLogMerge;
class ObTableMergeSpec;
class ObTableMergeOp;
class ObTableMergeOpInput;
REGISTER_OPERATOR(ObLogMerge, PHY_MERGE, ObTableMergeSpec, ObTableMergeOp, ObTableMergeOpInput);

class ObLogMerge;
class ObMultiTableMergeSpec;
class ObMultiTableMergeOp;
REGISTER_OPERATOR(ObLogMerge, PHY_MULTI_TABLE_MERGE, ObMultiTableMergeSpec, ObMultiTableMergeOp);

class ObLogInsert;
class ObTableInsertSpec;
class ObTableInsertOp;
class ObTableInsertOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_INSERT, ObTableInsertSpec, ObTableInsertOp, ObTableInsertOpInput);

class ObLogInsert;
class ObMultiPartInsertSpec;
class ObMultiPartInsertOp;
REGISTER_OPERATOR(ObLogInsert, PHY_MULTI_PART_INSERT, ObMultiPartInsertSpec, ObMultiPartInsertOp);

class ObTableInsertReturningSpec;
class ObTableInsertReturningOp;
class ObTableInsertReturningOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_INSERT_RETURNING, ObTableInsertReturningSpec, ObTableInsertReturningOp,
    ObTableInsertReturningOpInput);

class ObLogDelete;
class ObMultiPartDeleteSpec;
class ObMultiPartDeleteOp;
REGISTER_OPERATOR(ObLogDelete, PHY_MULTI_PART_DELETE, ObMultiPartDeleteSpec, ObMultiPartDeleteOp);

// PDML-delete
class ObLogDelete;
class ObPxMultiPartDeleteSpec;
class ObPxMultiPartDeleteOp;
class ObPxMultiPartDeleteOpInput;
REGISTER_OPERATOR(
    ObLogDelete, PHY_PX_MULTI_PART_DELETE, ObPxMultiPartDeleteSpec, ObPxMultiPartDeleteOp, ObPxMultiPartDeleteOpInput);

// PDML-insert
class ObLogInsert;
class ObPxMultiPartInsertSpec;
class ObPxMultiPartInsertOp;
class ObPxMultiPartInsertOpInput;
REGISTER_OPERATOR(
    ObLogInsert, PHY_PX_MULTI_PART_INSERT, ObPxMultiPartInsertSpec, ObPxMultiPartInsertOp, ObPxMultiPartInsertOpInput);

// PDML-update
class ObLogUpdate;
class ObPxMultiPartUpdateSpec;
class ObPxMultiPartUpdateOp;
class ObPxMultiPartUpdateOpInput;
REGISTER_OPERATOR(
    ObLogUpdate, PHY_PX_MULTI_PART_UPDATE, ObPxMultiPartUpdateSpec, ObPxMultiPartUpdateOp, ObPxMultiPartUpdateOpInput);

class ObLogicalOperator;
class ObAppendSpec;
class ObAppendOp;
REGISTER_OPERATOR(ObLogicalOperator, PHY_APPEND, ObAppendSpec, ObAppendOp);

class ObLogicalOperator;
class ObTableRowStoreSpec;
class ObTableRowStoreOp;
class ObTableRowStoreOpInput;
REGISTER_OPERATOR(
    ObLogicalOperator, PHY_TABLE_ROW_STORE, ObTableRowStoreSpec, ObTableRowStoreOp, ObTableRowStoreOpInput);

class ObLogExprValues;
class ObExprValuesSpec;
class ObExprValuesOp;
class ObExprValuesOpInput;
REGISTER_OPERATOR(ObLogExprValues, PHY_EXPR_VALUES, ObExprValuesSpec, ObExprValuesOp, ObExprValuesOpInput);

class ObLogDelete;
class ObTableDeleteSpec;
class ObTableDeleteOp;
class ObTableDeleteOpInput;
REGISTER_OPERATOR(ObLogDelete, PHY_DELETE, ObTableDeleteSpec, ObTableDeleteOp, ObTableDeleteOpInput);

class ObLogDelete;
class ObTableDeleteReturningSpec;
class ObTableDeleteReturningOp;
class ObTableDeleteReturningOpInput;
REGISTER_OPERATOR(ObLogDelete, PHY_DELETE_RETURNING, ObTableDeleteReturningSpec, ObTableDeleteReturningOp,
    ObTableDeleteReturningOpInput);

class ObLogInsert;
class ObTableReplaceSpec;
class ObTableReplaceOp;
class ObTableReplaceOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_REPLACE, ObTableReplaceSpec, ObTableReplaceOp, ObTableReplaceOpInput);

class ObLogInsert;
class ObMultiTableReplaceSpec;
class ObMultiTableReplaceOp;
REGISTER_OPERATOR(ObLogInsert, PHY_MULTI_TABLE_REPLACE, ObMultiTableReplaceSpec, ObMultiTableReplaceOp);

class ObLogJoin;
class ObNLConnectByWithIndexSpec;
class ObNLConnectByWithIndexOp;
REGISTER_OPERATOR(
    ObLogJoin, PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX, ObNLConnectByWithIndexSpec, ObNLConnectByWithIndexOp);

class ObLogJoin;
class ObNLConnectBySpec;
class ObNLConnectByOp;
REGISTER_OPERATOR(ObLogJoin, PHY_NESTED_LOOP_CONNECT_BY, ObNLConnectBySpec, ObNLConnectByOp);

class ObLogJoin;
class ObHashJoinSpec;
class ObHashJoinOp;
REGISTER_OPERATOR(ObLogJoin, PHY_HASH_JOIN, ObHashJoinSpec, ObHashJoinOp);

class ObNestedLoopJoinSpec;
class ObNestedLoopJoinOp;
REGISTER_OPERATOR(ObLogJoin, PHY_NESTED_LOOP_JOIN, ObNestedLoopJoinSpec, ObNestedLoopJoinOp);

class ObLogSubPlanFilter;
class ObSubPlanFilterSpec;
class ObSubPlanFilterOp;
REGISTER_OPERATOR(ObLogSubPlanFilter, PHY_SUBPLAN_FILTER, ObSubPlanFilterSpec, ObSubPlanFilterOp);

class ObLogSubPlanScan;
class ObSubPlanScanSpec;
class ObSubPlanScanOp;
REGISTER_OPERATOR(ObLogSubPlanScan, PHY_SUBPLAN_SCAN, ObSubPlanScanSpec, ObSubPlanScanOp);

class ObLogForUpdate;
class ObTableLockOpInput;
class ObTableLockSpec;
class ObTableLockOp;
REGISTER_OPERATOR(ObLogForUpdate, PHY_LOCK, ObTableLockSpec, ObTableLockOp, ObTableLockOpInput);

class ObLogUpdate;
class ObTableUpdateOpInput;
class ObTableUpdateSpec;
class ObTableUpdateOp;
REGISTER_OPERATOR(ObLogUpdate, PHY_UPDATE, ObTableUpdateSpec, ObTableUpdateOp, ObTableUpdateOpInput);

class ObTableUpdateReturningOpInput;
class ObTableUpdateReturningSpec;
class ObTableUpdateReturningOp;
REGISTER_OPERATOR(ObLogUpdate, PHY_UPDATE_RETURNING, ObTableUpdateReturningSpec, ObTableUpdateReturningOp,
    ObTableUpdateReturningOpInput);

class ObLogInsert;
class ObTableInsertUpOpInput;
class ObTableInsertUpSpec;
class ObTableInsertUpOp;
REGISTER_OPERATOR(ObLogInsert, PHY_INSERT_ON_DUP, ObTableInsertUpSpec, ObTableInsertUpOp, ObTableInsertUpOpInput);

class ObLogInsert;
class ObMultiTableInsertUpSpec;
class ObMultiTableInsertUpOp;
REGISTER_OPERATOR(ObLogInsert, PHY_MULTI_TABLE_INSERT_UP, ObMultiTableInsertUpSpec, ObMultiTableInsertUpOp);

class ObLogForUpdate;
class ObMultiPartLockOp;
class ObMultiPartLockSpec;
REGISTER_OPERATOR(ObLogForUpdate, PHY_MULTI_LOCK, ObMultiPartLockSpec, ObMultiPartLockOp);

class ObLogUpdate;
class ObMultiPartUpdateOp;
class ObMultiPartUpdateSpec;
REGISTER_OPERATOR(ObLogUpdate, PHY_MULTI_PART_UPDATE, ObMultiPartUpdateSpec, ObMultiPartUpdateOp);

class ObLogGroupBy;
class ObScalarAggregateSpec;
class ObScalarAggregateOp;
REGISTER_OPERATOR(ObLogGroupBy, PHY_SCALAR_AGGREGATE, ObScalarAggregateSpec, ObScalarAggregateOp);

class ObLogGroupBy;
class ObMergeGroupBySpec;
class ObMergeGroupByOp;
REGISTER_OPERATOR(ObLogGroupBy, PHY_MERGE_GROUP_BY, ObMergeGroupBySpec, ObMergeGroupByOp);

class ObLogGroupBy;
class ObHashGroupBySpec;
class ObHashGroupByOp;
REGISTER_OPERATOR(ObLogGroupBy, PHY_HASH_GROUP_BY, ObHashGroupBySpec, ObHashGroupByOp);

class ObLogWindowFunction;
class ObWindowFunctionSpec;
class ObWindowFunctionOp;
REGISTER_OPERATOR(ObLogWindowFunction, PHY_WINDOW_FUNCTION, ObWindowFunctionSpec, ObWindowFunctionOp);

class ObLogJoin;
class ObMergeJoinSpec;
class ObMergeJoinOp;
REGISTER_OPERATOR(ObLogJoin, PHY_MERGE_JOIN, ObMergeJoinSpec, ObMergeJoinOp);

class ObLogTopk;
class ObTopKSpec;
class ObTopKOp;
REGISTER_OPERATOR(ObLogTopk, PHY_TOPK, ObTopKSpec, ObTopKOp);

class ObLogMonitoringDump;
class ObMonitoringDumpSpec;
class ObMonitoringDumpOp;
REGISTER_OPERATOR(ObLogMonitoringDump, PHY_MONITORING_DUMP, ObMonitoringDumpSpec, ObMonitoringDumpOp);

class ObLogSequence;
class ObSequenceSpec;
class ObSequenceOp;
REGISTER_OPERATOR(ObLogSequence, PHY_SEQUENCE, ObSequenceSpec, ObSequenceOp);

// PX Operator
// PHY_GRANULE_ITERATOR,
// PHY_PX_FIFO_RECEIVE,
// PHY_PX_MERGE_SORT_RECEIVE,
// PHY_PX_DIST_TRANSMIT,
// PHY_PX_REPART_TRANSMIT,
// PHY_PX_REDUCE_TRANSMIT,
// PHY_PX_FIFO_COORD,
// PHY_PX_MERGE_SORT_COORD,
class ObLogGranuleIterator;
class ObGranuleIteratorSpec;
class ObGranuleIteratorOp;
class ObGIOpInput;
REGISTER_OPERATOR(ObLogGranuleIterator, PHY_GRANULE_ITERATOR, ObGranuleIteratorSpec, ObGranuleIteratorOp, ObGIOpInput);

class ObLogExchange;
class ObPxFifoReceiveSpec;
class ObPxFifoReceiveOp;
class ObPxFifoReceiveOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_FIFO_RECEIVE, ObPxFifoReceiveSpec, ObPxFifoReceiveOp, ObPxFifoReceiveOpInput);

class ObLogExchange;
class ObPxMSReceiveSpec;
class ObPxMSReceiveOp;
class ObPxMSReceiveOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_MERGE_SORT_RECEIVE, ObPxMSReceiveSpec, ObPxMSReceiveOp, ObPxMSReceiveOpInput);

class ObLogExchange;
class ObPxDistTransmitSpec;
class ObPxDistTransmitOp;
class ObPxDistTransmitOpInput;
REGISTER_OPERATOR(
    ObLogExchange, PHY_PX_DIST_TRANSMIT, ObPxDistTransmitSpec, ObPxDistTransmitOp, ObPxDistTransmitOpInput);

class ObLogExchange;
class ObPxRepartTransmitSpec;
class ObPxRepartTransmitOp;
class ObPxRepartTransmitOpInput;
REGISTER_OPERATOR(
    ObLogExchange, PHY_PX_REPART_TRANSMIT, ObPxRepartTransmitSpec, ObPxRepartTransmitOp, ObPxRepartTransmitOpInput);

class ObLogExchange;
class ObPxReduceTransmitSpec;
class ObPxReduceTransmitOp;
class ObPxReduceTransmitOpInput;
REGISTER_OPERATOR(
    ObLogExchange, PHY_PX_REDUCE_TRANSMIT, ObPxReduceTransmitSpec, ObPxReduceTransmitOp, ObPxReduceTransmitOpInput);

class ObLogExchange;
class ObPxFifoCoordSpec;
class ObPxFifoCoordOp;
class ObPxFifoCoordOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_FIFO_COORD, ObPxFifoCoordSpec, ObPxFifoCoordOp, ObPxFifoCoordOpInput);

class ObLogExchange;
class ObPxMSCoordSpec;
class ObPxMSCoordOp;
class ObPxMSCoordOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_MERGE_SORT_COORD, ObPxMSCoordSpec, ObPxMSCoordOp, ObPxMSCoordOpInput);

class ObLogExchange;
class ObDirectReceiveSpec;
class ObDirectReceiveOp;
REGISTER_OPERATOR(ObLogExchange, PHY_DIRECT_RECEIVE, ObDirectReceiveSpec, ObDirectReceiveOp);

class ObLogExchange;
class ObDirectTransmitSpec;
class ObDirectTransmitOp;
class ObDirectTransmitOpInput;
REGISTER_OPERATOR(
    ObLogExchange, PHY_DIRECT_TRANSMIT, ObDirectTransmitSpec, ObDirectTransmitOp, ObDirectTransmitOpInput);

class ObLogConflictRowFetcher;
class ObTableConflictRowFetcherSpec;
class ObTableConflictRowFetcherOp;
class ObTCRFetcherOpInput;
REGISTER_OPERATOR(ObLogConflictRowFetcher, PHY_TABLE_CONFLICT_ROW_FETCHER, ObTableConflictRowFetcherSpec,
    ObTableConflictRowFetcherOp, ObTCRFetcherOpInput);

#undef REGISTER_OPERATOR
#undef REGISTER_OPERATOR_FULL
#undef REGISTER_OPERATOR_4
#undef REGISTER_OPERATOR_5
#undef DEF_OP_TRAITS_true
#undef DEF_OP_TRAITS_false
#undef DEF_OP_INPUT_TRAITS

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_OB_OPERATOR_REG_H_
