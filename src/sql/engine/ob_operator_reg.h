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
#include "share/ob_cluster_version.h"
#include <type_traits>

namespace oceanbase
{
namespace sql
{

namespace op_reg
{

template <int>
struct ObOpTypeTraits
{

  constexpr static bool registered_ = false;
  constexpr static bool has_input_ = false;
  constexpr static bool vectorized_ = false;
  constexpr static uint64_t ob_version_ = 0;
  constexpr static bool support_rich_format_ = false;
  constexpr static const char *vec_op_name_ = "";
  typedef char LogOp;
  typedef char Spec;
  typedef char Op;
  typedef char Input;
};

template <typename T>
struct ObOpTraits
{
  constexpr static int type_ = 0;
};

} // end namespace op_reg

struct VECTORIZED_OP {};
struct SUPPORT_RICH_FORMAT {};
#define NOINPUT char


// check the input is char, return 0 or 1
#define CHECK_IS_CHAR__char 1, 1
#define CHECK_IS_CHAR(x) SELECT(2, CHECK_IS_CHAR__ ## x, 0)

// define operator input type traits if input is not char.
#define DEF_OP_INPUT_TRAITS(input, type) \
    CONCAT(DEF_OP_INPUT_TRAITS_, CHECK_IS_CHAR(input))(input, type)
#define DEF_OP_INPUT_TRAITS_0(input, type) \
  template <> struct ObOpTraits<input> { constexpr static int type_ = type; };
#define DEF_OP_INPUT_TRAITS_1(input, type)


#define REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, y, z, rich_fmt, vec_op_name) \
  REGISTER_OPERATOR_FULL_(log_op, type, spec, op, x, y, z, rich_fmt, vec_op_name)
#define REGISTER_OPERATOR_FULL_(...) REGISTER_OPERATOR_FULL__(__VA_ARGS__)

#define REGISTER_OPERATOR_FULL__(log_op, type, op_spec, op, input_type,        \
                                 vec_type, ob_version, rich_fmt, vec_op_name)  \
  namespace op_reg {                                                           \
  template <> struct ObOpTypeTraits<type> {                                    \
    constexpr static bool registered_ = true;                                  \
    constexpr static bool has_input_ = !std::is_same<char, input_type>::value; \
    constexpr static bool vectorized_ =                                        \
        std::is_same<VECTORIZED_OP, vec_type>::value;                          \
    constexpr static uint64_t ob_version_ =                                    \
        (ob_version == 0 ? CLUSTER_VERSION_4_0_0_0 : ob_version);              \
    constexpr static bool support_rich_format_ =                               \
        std::is_same<SUPPORT_RICH_FORMAT, rich_fmt>::value;                    \
    constexpr static const char *vec_op_name_ = support_rich_format_ ? vec_op_name : #type;\
    typedef log_op LogOp;                                                      \
    typedef op_spec Spec;                                                      \
    typedef op Op;                                                             \
    typedef input_type Input;                                                  \
  };                                                                           \
  template <> struct ObOpTraits<op_spec> {                                     \
    constexpr static int type_ = type;                                         \
  };                                                                           \
  template <> struct ObOpTraits<op> { constexpr static int type_ = type; };    \
  DEF_OP_INPUT_TRAITS(input_type, type)                                        \
  }

#define REGISTER_OPERATOR_5(log_op, type, spec, op, x) \
    REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, char, 0, char, "")
#define REGISTER_OPERATOR_6(log_op, type, spec, op, x, y) \
    REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, y, 0, char, "")
#define REGISTER_OPERATOR_7(log_op, type, spec, op, x, y, z) \
    REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, y, z, char, "")
#define REGISTER_OPERATOR_8(log_op, type, spec, op, x, y, z, rich_fmt) \
    REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, y, z, rich_fmt, #type)
#define REGISTER_OPERATOR_9(log_op, type, spec, op, x, y, z, rich_format, new_vec_op)\
    REGISTER_OPERATOR_FULL(log_op, type, spec, op, x, y, z, rich_format, new_vec_op)

#define REGISTER_OPERATOR___(n, ...) REGISTER_OPERATOR_ ##n(__VA_ARGS__)
#define REGISTER_OPERATOR__(...) REGISTER_OPERATOR___(__VA_ARGS__)
#define REGISTER_OPERATOR(...) \
    REGISTER_OPERATOR__(ARGS_NUM(__VA_ARGS__), __VA_ARGS__)

template <typename T>
const typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_>::Spec &get_my_spec(const T &op)
{
  typedef typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_> Traits;
  static_assert(Traits::registered_,
                "only registered operator can call this function");
  return static_cast<const typename Traits::Spec &>(op.get_spec());
}

#define MY_SPEC get_my_spec(*this)
// to use MY_SPEC base operator, you need to override get_my_spec() like this:
// OB_INLINE const ObBaseSpec &get_my_spec(const ObBaseOp &op)
// {
//   return static_cast<ObBaseSpec &>(op.get_spec());
// }

template <typename T>
typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_>::Input &get_my_input(const T &op)
{
  typedef typename op_reg::ObOpTypeTraits<op_reg::ObOpTraits<T>::type_> Traits;
  static_assert(Traits::has_input_,
                "only registered operator with operator input can call this function");
  // Dereferencing op.get_input() here is safe, because input pointer validity is checked in
  // ObOperatorFactory::alloc_operator if operator registered with input.
  return static_cast<typename Traits::Input &>(*op.get_input());
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
REGISTER_OPERATOR(ObLogLimit, PHY_LIMIT, ObLimitSpec, ObLimitOp, NOINPUT,
                  VECTORIZED_OP);

class ObLogLimit;
class ObLimitVecSpec;
class ObLimitVecOp;
REGISTER_OPERATOR(ObLogLimit, PHY_VEC_LIMIT, ObLimitVecSpec, ObLimitVecOp, NOINPUT,
                  VECTORIZED_OP, 0, SUPPORT_RICH_FORMAT);

class ObLogDistinct;
class ObMergeDistinctSpec;
class ObMergeDistinctOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_MERGE_DISTINCT, ObMergeDistinctSpec,
                  ObMergeDistinctOp, NOINPUT, VECTORIZED_OP);

class ObLogDistinct;
class ObMergeDistinctVecSpec;
class ObMergeDistinctVecOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_VEC_MERGE_DISTINCT, ObMergeDistinctVecSpec,
                  ObMergeDistinctVecOp, NOINPUT, VECTORIZED_OP, 0, SUPPORT_RICH_FORMAT);

class ObHashDistinctSpec;
class ObHashDistinctOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_HASH_DISTINCT, ObHashDistinctSpec,
                  ObHashDistinctOp, NOINPUT, VECTORIZED_OP);

class ObHashDistinctVecSpec;
class ObHashDistinctVecOp;
REGISTER_OPERATOR(ObLogDistinct, PHY_VEC_HASH_DISTINCT, ObHashDistinctVecSpec,
                  ObHashDistinctVecOp, NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogMaterial;
class ObMaterialSpec;
class ObMaterialOp;
class ObMaterialOpInput;
REGISTER_OPERATOR(ObLogMaterial, PHY_MATERIAL, ObMaterialSpec, ObMaterialOp,
                  ObMaterialOpInput, VECTORIZED_OP);

class ObLogMaterial;
class ObMaterialVecSpec;
class ObMaterialVecOp;
class ObMaterialVecOpInput;
REGISTER_OPERATOR(ObLogMaterial, PHY_VEC_MATERIAL, ObMaterialVecSpec, ObMaterialVecOp,
                  ObMaterialVecOpInput, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogTempTableInsert;
class ObTempTableInsertVecOpSpec;
class ObTempTableInsertVecOp;
class ObTempTableInsertVecOpInput;
REGISTER_OPERATOR(ObLogTempTableInsert, PHY_VEC_TEMP_TABLE_INSERT, ObTempTableInsertVecOpSpec, ObTempTableInsertVecOp,
                  ObTempTableInsertVecOpInput, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogTempTableAccess;
class ObTempTableAccessVecOpSpec;
class ObTempTableAccessVecOp;
class ObTempTableAccessVecOpInput;
REGISTER_OPERATOR(ObLogTempTableAccess, PHY_VEC_TEMP_TABLE_ACCESS, ObTempTableAccessVecOpSpec, ObTempTableAccessVecOp,
                  ObTempTableAccessVecOpInput, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogTempTableTransformation;
class ObTempTableTransformationVecOpSpec;
class ObTempTableTransformationVecOp;
REGISTER_OPERATOR(ObLogTempTableTransformation, PHY_VEC_TEMP_TABLE_TRANSFORMATION, ObTempTableTransformationVecOpSpec, ObTempTableTransformationVecOp,
                  NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogSort;
class ObSortSpec;
class ObSortOp;
REGISTER_OPERATOR(ObLogSort, PHY_SORT, ObSortSpec, ObSortOp, NOINPUT,
                  VECTORIZED_OP);
class ObSortVecSpec;
class ObSortVecOp;
REGISTER_OPERATOR(ObLogSort, PHY_VEC_SORT, ObSortVecSpec,
                  ObSortVecOp, NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogSet;
class ObHashUnionSpec;
class ObHashUnionOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_UNION, ObHashUnionSpec, ObHashUnionOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObHashUnionVecSpec;
class ObHashUnionVecOp;
REGISTER_OPERATOR(ObLogSet, PHY_VEC_HASH_UNION, ObHashUnionVecSpec, ObHashUnionVecOp,
                  NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogSet;
class ObHashIntersectSpec;
class ObHashIntersectOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_INTERSECT, ObHashIntersectSpec,
                  ObHashIntersectOp, NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObHashIntersectVecSpec;
class ObHashIntersectVecOp;
REGISTER_OPERATOR(ObLogSet, PHY_VEC_HASH_INTERSECT, ObHashIntersectVecSpec, ObHashIntersectVecOp,
                  NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogSet;
class ObHashExceptSpec;
class ObHashExceptOp;
REGISTER_OPERATOR(ObLogSet, PHY_HASH_EXCEPT, ObHashExceptSpec, ObHashExceptOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObHashExceptVecSpec;
class ObHashExceptVecOp;
REGISTER_OPERATOR(ObLogSet, PHY_VEC_HASH_EXCEPT, ObHashExceptVecSpec, ObHashExceptVecOp,
                  NOINPUT, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);

class ObLogSet;
class ObMergeUnionSpec;
class ObMergeUnionOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_UNION, ObMergeUnionSpec, ObMergeUnionOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObRecursiveUnionAllSpec;
class ObRecursiveUnionAllOp;
REGISTER_OPERATOR(ObLogSet, PHY_RECURSIVE_UNION_ALL, ObRecursiveUnionAllSpec,
                  ObRecursiveUnionAllOp, NOINPUT, VECTORIZED_OP);

class ObLogTableScan;
class ObFakeCTETableSpec;
class ObFakeCTETableOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_FAKE_CTE_TABLE, ObFakeCTETableSpec,
                  ObFakeCTETableOp, NOINPUT, VECTORIZED_OP);

class ObLogTempTableAccess;
class ObTempTableAccessOpInput;
class ObTempTableAccessOpSpec;
class ObTempTableAccessOp;
REGISTER_OPERATOR(ObLogTempTableAccess, PHY_TEMP_TABLE_ACCESS,
                  ObTempTableAccessOpSpec, ObTempTableAccessOp, ObTempTableAccessOpInput, VECTORIZED_OP);

class ObLogTempTableInsert;
class ObTempTableInsertOpInput;
class ObTempTableInsertOpSpec;
class ObTempTableInsertOp;
REGISTER_OPERATOR(ObLogTempTableInsert, PHY_TEMP_TABLE_INSERT,
                  ObTempTableInsertOpSpec, ObTempTableInsertOp, ObTempTableInsertOpInput, VECTORIZED_OP);

class ObLogTempTableTransformation;
class ObTempTableTransformationOpSpec;
class ObTempTableTransformationOp;
REGISTER_OPERATOR(ObLogTempTableTransformation, PHY_TEMP_TABLE_TRANSFORMATION,
                  ObTempTableTransformationOpSpec, ObTempTableTransformationOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObMergeIntersectSpec;
class ObMergeIntersectOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_INTERSECT, ObMergeIntersectSpec,
                  ObMergeIntersectOp, NOINPUT, VECTORIZED_OP);

class ObLogSet;
class ObMergeExceptSpec;
class ObMergeExceptOp;
REGISTER_OPERATOR(ObLogSet, PHY_MERGE_EXCEPT, ObMergeExceptSpec,
                  ObMergeExceptOp, NOINPUT, VECTORIZED_OP);

class ObLogCount;
class ObCountSpec;
class ObCountOp;
REGISTER_OPERATOR(ObLogCount, PHY_COUNT, ObCountSpec, ObCountOp, NOINPUT);

class ObLogValues;
class ObValuesSpec;
class ObValuesOp;
REGISTER_OPERATOR(ObLogValues, PHY_VALUES, ObValuesSpec, ObValuesOp, NOINPUT);

class ObLogTableScan;
class ObTableScanOpInput;
class ObTableScanSpec;
class ObTableScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_TABLE_SCAN,
                  ObTableScanSpec, ObTableScanOp, ObTableScanOpInput,
                  VECTORIZED_OP, 0 /*version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_TABLE_SCAN");

class ObLogTableScan;
class ObRowSampleScanOpInput;
class ObRowSampleScanSpec;
class ObRowSampleScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_ROW_SAMPLE_SCAN,
                  ObRowSampleScanSpec, ObRowSampleScanOp, ObRowSampleScanOpInput,
                  VECTORIZED_OP);

class ObLogTableScan;
class ObBlockSampleScanOpInput;
class ObBlockSampleScanSpec;
class ObBlockSampleScanOp;
REGISTER_OPERATOR(ObLogTableScan, PHY_BLOCK_SAMPLE_SCAN,
                  ObBlockSampleScanSpec, ObBlockSampleScanOp, ObBlockSampleScanOpInput,
                  VECTORIZED_OP);

class ObLogMerge;
class ObTableMergeSpec;
class ObTableMergeOp;
class ObTableMergeOpInput;
REGISTER_OPERATOR(ObLogMerge, PHY_MERGE, ObTableMergeSpec, ObTableMergeOp, ObTableMergeOpInput);

class ObLogInsert;
class ObTableInsertSpec;
class ObTableInsertOp;
class ObTableInsertOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_INSERT, ObTableInsertSpec,
                  ObTableInsertOp, ObTableInsertOpInput);

// PDML-delete
class ObLogDelete;
class ObPxMultiPartDeleteSpec;
class ObPxMultiPartDeleteOp;
class ObPxMultiPartDeleteOpInput;
REGISTER_OPERATOR(ObLogDelete, PHY_PX_MULTI_PART_DELETE, ObPxMultiPartDeleteSpec, ObPxMultiPartDeleteOp, ObPxMultiPartDeleteOpInput);

// PDML-insert
class ObLogInsert;
class ObPxMultiPartInsertSpec;
class ObPxMultiPartInsertOp;
class ObPxMultiPartInsertOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_PX_MULTI_PART_INSERT, ObPxMultiPartInsertSpec, ObPxMultiPartInsertOp, ObPxMultiPartInsertOpInput);

// PDML-update
class ObLogUpdate;
class ObPxMultiPartUpdateSpec;
class ObPxMultiPartUpdateOp;
class ObPxMultiPartUpdateOpInput;
REGISTER_OPERATOR(ObLogUpdate, PHY_PX_MULTI_PART_UPDATE, ObPxMultiPartUpdateSpec, ObPxMultiPartUpdateOp, ObPxMultiPartUpdateOpInput);

// ddl insert sstable
class ObLogInsert;
class ObPxMultiPartSSTableInsertSpec;
class ObPxMultiPartSSTableInsertOp;
class ObPxMultiPartSSTableInsertOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_PX_MULTI_PART_SSTABLE_INSERT, ObPxMultiPartSSTableInsertSpec, ObPxMultiPartSSTableInsertOp, ObPxMultiPartSSTableInsertOpInput);

class ObLogicalOperator;
class ObTableRowStoreSpec;
class ObTableRowStoreOp;
class ObTableRowStoreOpInput;
REGISTER_OPERATOR(ObLogicalOperator, PHY_TABLE_ROW_STORE, ObTableRowStoreSpec,
                  ObTableRowStoreOp, ObTableRowStoreOpInput);

class ObLogExprValues;
class ObExprValuesSpec;
class ObExprValuesOp;
REGISTER_OPERATOR(ObLogExprValues, PHY_EXPR_VALUES, ObExprValuesSpec, ObExprValuesOp, NOINPUT);

class ObLogValuesTableAccess;
class ObValuesTableAccessSpec;
class ObValuesTableAccessOp;
REGISTER_OPERATOR(ObLogValuesTableAccess, PHY_VALUES_TABLE_ACCESS, ObValuesTableAccessSpec, ObValuesTableAccessOp, NOINPUT);

class ObLogDelete;
class ObTableDeleteSpec;
class ObTableDeleteOp;
class ObTableDeleteOpInput;
REGISTER_OPERATOR(ObLogDelete, PHY_DELETE, ObTableDeleteSpec,
                  ObTableDeleteOp, ObTableDeleteOpInput);

class ObLogInsert;
class ObTableReplaceSpec;
class ObTableReplaceOp;
class ObTableReplaceOpInput;
REGISTER_OPERATOR(ObLogInsert, PHY_REPLACE, ObTableReplaceSpec, ObTableReplaceOp,
                  ObTableReplaceOpInput);

class ObLogJoin;
class ObNLConnectByWithIndexSpec;
class ObNLConnectByWithIndexOp;
REGISTER_OPERATOR(ObLogJoin, PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX,
                  ObNLConnectByWithIndexSpec, ObNLConnectByWithIndexOp,
                  NOINPUT);

class ObLogJoin;
class ObNLConnectBySpec;
class ObNLConnectByOp;
REGISTER_OPERATOR(ObLogJoin, PHY_NESTED_LOOP_CONNECT_BY, ObNLConnectBySpec,
                  ObNLConnectByOp, NOINPUT);

class ObLogJoin;
class ObHashJoinSpec;
class ObHashJoinOp;
class ObHashJoinInput;
REGISTER_OPERATOR(ObLogJoin, PHY_HASH_JOIN, ObHashJoinSpec, ObHashJoinOp,
                  ObHashJoinInput, VECTORIZED_OP);

class ObLogJoin;
class ObHashJoinVecSpec;
class ObHashJoinVecOp;
class ObHashJoinVecInput;
REGISTER_OPERATOR(ObLogJoin, PHY_VEC_HASH_JOIN, ObHashJoinVecSpec, ObHashJoinVecOp,
                  ObHashJoinVecInput, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT);


class ObNestedLoopJoinSpec;
class ObNestedLoopJoinOp;
REGISTER_OPERATOR(ObLogJoin, PHY_NESTED_LOOP_JOIN, ObNestedLoopJoinSpec,
                  ObNestedLoopJoinOp, NOINPUT, VECTORIZED_OP);

class ObLogSubPlanFilter;
class ObSubPlanFilterSpec;
class ObSubPlanFilterOp;
REGISTER_OPERATOR(ObLogSubPlanFilter, PHY_SUBPLAN_FILTER,
                  ObSubPlanFilterSpec, ObSubPlanFilterOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogSubPlanScan;
class ObSubPlanScanSpec;
class ObSubPlanScanOp;
REGISTER_OPERATOR(ObLogSubPlanScan, PHY_SUBPLAN_SCAN, ObSubPlanScanSpec,
                  ObSubPlanScanOp, NOINPUT, VECTORIZED_OP,  0 /*+version*/,
                  SUPPORT_RICH_FORMAT, "PHY_VEC_SUBPLAN_SCAN");

class ObLogUnpivot;
class ObUnpivotSpec;
class ObUnpivotOp;
REGISTER_OPERATOR(ObLogUnpivot, PHY_UNPIVOT, ObUnpivotSpec,
                  ObUnpivotOp, NOINPUT, VECTORIZED_OP);

class ObLogForUpdate;
class ObTableLockOpInput;
class ObTableLockSpec;
class ObTableLockOp;
REGISTER_OPERATOR(ObLogForUpdate, PHY_LOCK, ObTableLockSpec, ObTableLockOp,
                  ObTableLockOpInput, VECTORIZED_OP);

class ObLogUpdate;
class ObTableUpdateOpInput;
class ObTableUpdateSpec;
class ObTableUpdateOp;
REGISTER_OPERATOR(ObLogUpdate, PHY_UPDATE, ObTableUpdateSpec,
                  ObTableUpdateOp, ObTableUpdateOpInput);

class ObLogInsert;
class ObTableInsertUpOpInput;
class ObTableInsertUpSpec;
class ObTableInsertUpOp;
REGISTER_OPERATOR(ObLogInsert, PHY_INSERT_ON_DUP, ObTableInsertUpSpec,
                  ObTableInsertUpOp, ObTableInsertUpOpInput);

class ObLogGroupBy;
class ObScalarAggregateSpec;
class ObScalarAggregateOp;
class ObScalarAggregateVecOp;
class ObScalarAggregateVecSpec;
REGISTER_OPERATOR(ObLogGroupBy, PHY_SCALAR_AGGREGATE, ObScalarAggregateSpec,
                  ObScalarAggregateOp, NOINPUT, VECTORIZED_OP);
REGISTER_OPERATOR(ObLogGroupBy, PHY_VEC_SCALAR_AGGREGATE, ObScalarAggregateVecSpec,
                  ObScalarAggregateVecOp, NOINPUT, VECTORIZED_OP, 0, SUPPORT_RICH_FORMAT);

class ObLogGroupBy;
class ObMergeGroupBySpec;
class ObMergeGroupByOp;
REGISTER_OPERATOR(ObLogGroupBy, PHY_MERGE_GROUP_BY, ObMergeGroupBySpec,
                  ObMergeGroupByOp, NOINPUT, VECTORIZED_OP);

class ObLogGroupBy;
class ObHashGroupBySpec;
class ObHashGroupByOp;
class ObHashGroupByVecSpec;
class ObHashGroupByVecOp;
REGISTER_OPERATOR(ObLogGroupBy, PHY_HASH_GROUP_BY, ObHashGroupBySpec,
                  ObHashGroupByOp, NOINPUT, VECTORIZED_OP);

REGISTER_OPERATOR(ObLogGroupBy, PHY_VEC_HASH_GROUP_BY, ObHashGroupByVecSpec,
                  ObHashGroupByVecOp, NOINPUT, VECTORIZED_OP, 0, SUPPORT_RICH_FORMAT);

class ObLogWindowFunction;
class ObWindowFunctionSpec;
class ObWindowFunctionOp;
class ObWindowFunctionOpInput;
class ObWindowFunctionVecOpInput;
class ObWindowFunctionVecOp;
class ObWindowFunctionVecSpec;
REGISTER_OPERATOR(ObLogWindowFunction, PHY_WINDOW_FUNCTION, ObWindowFunctionSpec,
                  ObWindowFunctionOp, ObWindowFunctionOpInput, VECTORIZED_OP);

REGISTER_OPERATOR(ObLogWindowFunction, PHY_VEC_WINDOW_FUNCTION, ObWindowFunctionVecSpec,
                  ObWindowFunctionVecOp, ObWindowFunctionVecOpInput, VECTORIZED_OP, 0,
                  SUPPORT_RICH_FORMAT);

class ObLogJoin;
class ObMergeJoinSpec;
class ObMergeJoinOp;
REGISTER_OPERATOR(ObLogJoin, PHY_MERGE_JOIN, ObMergeJoinSpec, ObMergeJoinOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogTopk;
class ObTopKSpec;
class ObTopKOp;
REGISTER_OPERATOR(ObLogTopk, PHY_TOPK, ObTopKSpec, ObTopKOp, NOINPUT);

class ObLogMonitoringDump;
class ObMonitoringDumpSpec;
class ObMonitoringDumpOp;
REGISTER_OPERATOR(ObLogMonitoringDump, PHY_MONITORING_DUMP, ObMonitoringDumpSpec,
                  ObMonitoringDumpOp, NOINPUT, VECTORIZED_OP);

class ObLogSequence;
class ObSequenceSpec;
class ObSequenceOp;
REGISTER_OPERATOR(ObLogSequence, PHY_SEQUENCE, ObSequenceSpec, ObSequenceOp,
                  NOINPUT);

class ObLogJoinFilter;
class ObJoinFilterSpec;
class ObJoinFilterOp;
class ObJoinFilterOpInput;
REGISTER_OPERATOR(ObLogJoinFilter, PHY_JOIN_FILTER, ObJoinFilterSpec,
                  ObJoinFilterOp, ObJoinFilterOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_JOIN_FILTER");

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
REGISTER_OPERATOR(ObLogGranuleIterator, PHY_GRANULE_ITERATOR, ObGranuleIteratorSpec,
                  ObGranuleIteratorOp, ObGIOpInput, VECTORIZED_OP, 0 /*+version*/,
                  SUPPORT_RICH_FORMAT, "PHY_VEC_GRANULE_ITERATOR");

class ObLogExchange;
class ObPxFifoReceiveSpec;
class ObPxFifoReceiveOp;
class ObPxFifoReceiveOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_FIFO_RECEIVE, ObPxFifoReceiveSpec,
                  ObPxFifoReceiveOp, ObPxFifoReceiveOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_FIFO_RECEIVE");

class ObLogExchange;
class ObPxMSReceiveSpec;
class ObPxMSReceiveOp;
class ObPxMSReceiveOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_MERGE_SORT_RECEIVE, ObPxMSReceiveSpec,
                  ObPxMSReceiveOp, ObPxMSReceiveOpInput, VECTORIZED_OP);

class ObLogExchange;
class ObPxMSReceiveVecSpec;
class ObPxMSReceiveVecOp;
class ObPxMSReceiveVecOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_VEC_PX_MERGE_SORT_RECEIVE, ObPxMSReceiveVecSpec,
                  ObPxMSReceiveVecOp, ObPxMSReceiveVecOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT);

class ObLogExchange;
class ObPxDistTransmitSpec;
class ObPxDistTransmitOp;
class ObPxDistTransmitOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_DIST_TRANSMIT, ObPxDistTransmitSpec,
                  ObPxDistTransmitOp, ObPxDistTransmitOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_DIST_TRANSMIT");

class ObLogExchange;
class ObPxRepartTransmitSpec;
class ObPxRepartTransmitOp;
class ObPxRepartTransmitOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_REPART_TRANSMIT, ObPxRepartTransmitSpec,
                  ObPxRepartTransmitOp, ObPxRepartTransmitOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_REPART_TRANSMIT");

class ObLogExchange;
class ObPxReduceTransmitSpec;
class ObPxReduceTransmitOp;
class ObPxReduceTransmitOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_REDUCE_TRANSMIT, ObPxReduceTransmitSpec,
                  ObPxReduceTransmitOp, ObPxReduceTransmitOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_REDUCE_TRANSMIT");

class ObLogExchange;
class ObPxFifoCoordSpec;
class ObPxFifoCoordOp;
class ObPxFifoCoordOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_FIFO_COORD, ObPxFifoCoordSpec,
                  ObPxFifoCoordOp, ObPxFifoCoordOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_FIFO_COORD");

class ObPxOrderedCoordSpec;
class ObPxOrderedCoordOp;
class ObPxOrderedCoordOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_ORDERED_COORD, ObPxOrderedCoordSpec,
                  ObPxOrderedCoordOp, ObPxOrderedCoordOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT, "PHY_VEC_PX_ORDERED_COORD");
class ObLogExchange;
class ObPxMSCoordSpec;
class ObPxMSCoordOp;
class ObPxMSCoordOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_PX_MERGE_SORT_COORD, ObPxMSCoordSpec,
                  ObPxMSCoordOp, ObPxMSCoordOpInput, VECTORIZED_OP);

class ObLogExchange;
class ObPxMSCoordVecSpec;
class ObPxMSCoordVecOp;
class ObPxMSCoordVecOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_VEC_PX_MERGE_SORT_COORD, ObPxMSCoordVecSpec,
                  ObPxMSCoordVecOp, ObPxMSCoordVecOpInput, VECTORIZED_OP,
                  0 /*+version*/, SUPPORT_RICH_FORMAT);

class ObLogExchange;
class ObDirectReceiveSpec;
class ObDirectReceiveOp;
REGISTER_OPERATOR(ObLogExchange, PHY_DIRECT_RECEIVE, ObDirectReceiveSpec,
                  ObDirectReceiveOp, NOINPUT);

class ObLogExchange;
class ObDirectTransmitSpec;
class ObDirectTransmitOp;
class ObDirectTransmitOpInput;
REGISTER_OPERATOR(ObLogExchange, PHY_DIRECT_TRANSMIT, ObDirectTransmitSpec,
                  ObDirectTransmitOp, ObDirectTransmitOpInput);

class ObLogErrLog;
class ObErrLogSpec;
class ObErrLogOp;
REGISTER_OPERATOR(ObLogErrLog, PHY_ERR_LOG, ObErrLogSpec, ObErrLogOp, NOINPUT);

class ObLogSelectInto;
class ObSelectIntoSpec;
class ObSelectIntoOp;
class ObSelectIntoOpInput;
REGISTER_OPERATOR(ObLogSelectInto, PHY_SELECT_INTO, ObSelectIntoSpec, ObSelectIntoOp,
                  ObSelectIntoOpInput, VECTORIZED_OP);
class ObLogLinkScan;
class ObLinkScanSpec;
class ObLinkScanOp;
REGISTER_OPERATOR(ObLogLinkScan, PHY_LINK_SCAN, ObLinkScanSpec, ObLinkScanOp,
                  NOINPUT, VECTORIZED_OP);

class ObLogLinkDml;
class ObLinkDmlSpec;
class ObLinkDmlOp;
REGISTER_OPERATOR(ObLogLinkDml, PHY_LINK_DML, ObLinkDmlSpec, ObLinkDmlOp, NOINPUT);

class ObLogFunctionTable;
class ObFunctionTableSpec;
class ObFunctionTableOp;
REGISTER_OPERATOR(ObLogFunctionTable, PHY_FUNCTION_TABLE, ObFunctionTableSpec,
                  ObFunctionTableOp, NOINPUT);

class ObLogInsertAll;
class ObTableInsertAllSpec;
class ObTableInsertAllOp;
class ObTableInsertAllOpInput;
REGISTER_OPERATOR(ObLogInsertAll, PHY_MULTI_TABLE_INSERT,
                  ObTableInsertAllSpec, ObTableInsertAllOp,
                  ObTableInsertAllOpInput);

class ObLogStatCollector;
class ObStatCollectorSpec;
class ObStatCollectorOp;
REGISTER_OPERATOR(ObLogStatCollector, PHY_STAT_COLLECTOR, ObStatCollectorSpec, ObStatCollectorOp, NOINPUT,
                  VECTORIZED_OP);

class ObLogJsonTable;
class ObJsonTableSpec;
class ObJsonTableOp;
REGISTER_OPERATOR(ObLogJsonTable, PHY_JSON_TABLE, ObJsonTableSpec,
                  ObJsonTableOp, NOINPUT);

class ObLogOptimizerStatsGathering;
class ObOptimizerStatsGatheringSpec;
class ObOptimizerStatsGatheringOp;
REGISTER_OPERATOR(ObLogOptimizerStatsGathering, PHY_OPTIMIZER_STATS_GATHERING,
                  ObOptimizerStatsGatheringSpec, ObOptimizerStatsGatheringOp, NOINPUT, VECTORIZED_OP);

#undef REGISTER_OPERATOR
#undef REGISTER_OPERATOR_FULL
#undef CHECK_IS_CHAR
#undef DEF_OP_INPUT_TRAITS
#undef REGISTER_OPERATOR_4
#undef REGISTER_OPERATOR_5
#undef REGISTER_OPERATOR_6

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_OPERATOR_REG_H_
