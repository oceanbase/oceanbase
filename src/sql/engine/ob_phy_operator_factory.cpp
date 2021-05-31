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

#include "sql/engine/ob_phy_operator_factory.h"
#include "sql/engine/join/ob_nested_loop_join.h"
#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/connect_by/ob_nested_loop_connect_by_with_index.h"
#include "sql/engine/join/ob_block_based_nested_loop_join.h"
#include "sql/engine/join/ob_hash_join.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_link_scan.h"
#include "sql/engine/table/ob_row_sample_scan.h"
#include "sql/engine/table/ob_block_sample_scan.h"
#include "sql/engine/table/ob_mv_table_scan.h"
#include "sql/engine/table/ob_domain_index.h"
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "sql/engine/table/ob_table_scan_with_checksum.h"
#include "sql/engine/table/ob_table_scan_create_domain_index.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/table/ob_table_row_store.h"
#include "sql/engine/join/ob_merge_join.h"
#include "sql/engine/aggregate/ob_groupby.h"
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/basic/ob_limit.h"
#include "sql/engine/sequence/ob_sequence.h"
#include "sql/engine/basic/ob_topk.h"
#include "sql/engine/dml/ob_table_delete.h"
#include "sql/engine/dml/ob_table_delete_returning.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/dml/ob_table_insert_returning.h"
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/dml/ob_table_update_returning.h"
#include "sql/engine/dml/ob_table_replace.h"
#include "sql/engine/dml/ob_table_append_local_sort_data.h"
#include "sql/engine/dml/ob_table_append_sstable.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_merge.h"
#include "sql/engine/dml/ob_multi_part_insert.h"
#include "sql/engine/dml/ob_multi_part_update.h"
#include "sql/engine/dml/ob_multi_part_delete.h"
#include "sql/engine/pdml/ob_px_multi_part_delete.h"
#include "sql/engine/pdml/ob_px_multi_part_update.h"
#include "sql/engine/pdml/ob_px_multi_part_insert.h"
#include "sql/engine/dml/ob_multi_table_replace.h"
#include "sql/engine/dml/ob_multi_table_insert_up.h"
#include "sql/engine/dml/ob_multi_table_merge.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/basic/ob_expr_values_with_child.h"
#include "sql/engine/basic/ob_values.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/engine/basic/ob_select_into.h"
#include "sql/engine/basic/ob_count.h"
#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/engine/basic/ob_temp_table_transformation.h"
#include "sql/engine/aggregate/ob_merge_distinct.h"
#include "sql/engine/aggregate/ob_hash_distinct.h"
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "sql/engine/set/ob_merge_union.h"
#include "sql/engine/set/ob_merge_intersect.h"
#include "sql/engine/set/ob_merge_except.h"
#include "sql/engine/set/ob_append.h"
#include "sql/engine/set/ob_hash_union.h"
#include "sql/engine/set/ob_hash_intersect.h"
#include "sql/engine/set/ob_hash_except.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all.h"
#include "sql/engine/recursive_cte/ob_fake_cte_table.h"
#include "sql/engine/subquery/ob_subplan_filter.h"
#include "sql/engine/subquery/ob_subplan_scan.h"
#include "sql/engine/subquery/ob_unpivot.h"
#include "sql/engine/window_function/ob_window_function.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit.h"
#include "sql/engine/px/exchange/ob_px_reduce_transmit.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_px_merge_sort_receive.h"
#include "sql/engine/px/ob_px_fifo_coord.h"
#include "sql/engine/px/ob_px_merge_sort_coord.h"
#include "sql/engine/px/ob_light_granule_iterator.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_direct_transmit.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/engine/table/ob_uk_row_transform.h"
#include "sql/executor/ob_determinate_task_transmit.h"
#include "sql/engine/basic/ob_monitoring_dump.h"
#include "sql/engine/dml/ob_table_lock.h"
#include "sql/engine/dml/ob_multi_part_lock.h"
#include "sql/engine/dml/ob_table_insert_all.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
ObPhyOperatorFactory::AllocFunc ObPhyOperatorFactory::PHY_OPERATOR_ALLOC[PHY_END] = {
    NULL,
    ObPhyOperatorFactory::alloc<ObLimit, PHY_LIMIT>,
    ObPhyOperatorFactory::alloc<ObSort, PHY_SORT>,
    ObPhyOperatorFactory::alloc<ObTableScan, PHY_TABLE_SCAN>,
    NULL,                                                     // PHY_VIRTUAL_TABLE_SCAN
    ObPhyOperatorFactory::alloc<ObMergeJoin, PHY_MERGE_JOIN>, /*5*/
    ObPhyOperatorFactory::alloc<ObNestedLoopJoin, PHY_NESTED_LOOP_JOIN>,
    ObPhyOperatorFactory::alloc<ObHashJoin, PHY_HASH_JOIN>,  // hash join
    ObPhyOperatorFactory::alloc<ObMergeGroupBy, PHY_MERGE_GROUP_BY>,
    ObPhyOperatorFactory::alloc<ObHashGroupBy, PHY_HASH_GROUP_BY>,
    ObPhyOperatorFactory::alloc<ObScalarAggregate, PHY_SCALAR_AGGREGATE>, /*10*/
    ObPhyOperatorFactory::alloc<ObMergeDistinct, PHY_MERGE_DISTINCT>,
    ObPhyOperatorFactory::alloc<ObHashDistinct, PHY_HASH_DISTINCT>,
    ObPhyOperatorFactory::alloc<ObRootTransmit, PHY_ROOT_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObDirectTransmit, PHY_DIRECT_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObDirectReceive, PHY_DIRECT_RECEIVE>, /*15*/
    ObPhyOperatorFactory::alloc<ObDistributedTransmit, PHY_DISTRIBUTED_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObFifoReceive, PHY_FIFO_RECEIVE>,
    ObPhyOperatorFactory::alloc<ObMergeUnion, PHY_MERGE_UNION>,
    ObPhyOperatorFactory::alloc<ObHashUnion, PHY_HASH_UNION>,           // PHY_HASH_UNION
    ObPhyOperatorFactory::alloc<ObMergeIntersect, PHY_MERGE_INTERSECT>, /*20*/
    ObPhyOperatorFactory::alloc<ObHashIntersect, PHY_HASH_INTERSECT>,
    ObPhyOperatorFactory::alloc<ObMergeExcept, PHY_MERGE_EXCEPT>,
    ObPhyOperatorFactory::alloc<ObHashExcept, PHY_HASH_EXCEPT>,
    ObPhyOperatorFactory::alloc<ObTableInsert, PHY_INSERT>,
    ObPhyOperatorFactory::alloc<ObTableUpdate, PHY_UPDATE>, /*25*/
    ObPhyOperatorFactory::alloc<ObTableDelete, PHY_DELETE>,
    ObPhyOperatorFactory::alloc<ObTableReplace, PHY_REPLACE>,
    ObPhyOperatorFactory::alloc<ObTableInsertUp, PHY_INSERT_ON_DUP>,
    ObPhyOperatorFactory::alloc<ObValues, PHY_VALUES>,
    ObPhyOperatorFactory::alloc<ObExprValues, PHY_EXPR_VALUES>, /*30*/
    NULL,                                                       // PHY_AUTOINCREMENT
    ObPhyOperatorFactory::alloc<ObSubPlanScan, PHY_SUBPLAN_SCAN>,
    ObPhyOperatorFactory::alloc<ObSubPlanFilter, PHY_SUBPLAN_FILTER>,
    ObPhyOperatorFactory::alloc<ObMaterial, PHY_MATERIAL>,
    ObPhyOperatorFactory::alloc<ObBLKNestedLoopJoin, PHY_BLOCK_BASED_NESTED_LOOP_JOIN>, /*35*/
    ObPhyOperatorFactory::alloc<ObDomainIndex, PHY_DOMAIN_INDEX>,
    ObPhyOperatorFactory::alloc<ObTableScanWithIndexBack, PHY_TABLE_SCAN_WITH_DOMAIN_INDEX>,
    ObPhyOperatorFactory::alloc<ObWindowFunction, PHY_WINDOW_FUNCTION>,
    ObPhyOperatorFactory::alloc<ObSelectInto, PHY_SELECT_INTO>,
    ObPhyOperatorFactory::alloc<ObTopK, PHY_TOPK>, /*40*/
    ObPhyOperatorFactory::alloc<ObMVTableScan, PHY_MV_TABLE_SCAN>,
    ObPhyOperatorFactory::alloc<ObAppend, PHY_APPEND>,
    NULL,                                                              // PHY_ROOT_RECEIVE
    NULL,                                                              // PHY_DISTRIBUTED_RECEIVE
    ObPhyOperatorFactory::alloc<ObFifoReceiveV2, PHY_FIFO_RECEIVE_V2>, /*45*/
    ObPhyOperatorFactory::alloc<ObTaskOrderReceive, PHY_TASK_ORDER_RECEIVE>,
    ObPhyOperatorFactory::alloc<ObMergeSortReceive, PHY_MERGE_SORT_RECEIVE>,
    ObPhyOperatorFactory::alloc<ObConnectByWithIndex, PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX>,
    ObPhyOperatorFactory::alloc<ObCount, PHY_COUNT>,
    ObPhyOperatorFactory::alloc<ObRecursiveUnionAll, PHY_RECURSIVE_UNION_ALL>, /*50*/
    ObPhyOperatorFactory::alloc<ObFakeCTETable, PHY_FAKE_CTE_TABLE>,
    ObPhyOperatorFactory::alloc<ObTableMerge, PHY_MERGE>,
    ObPhyOperatorFactory::alloc<ObRowSampleScan, PHY_ROW_SAMPLE_SCAN>,
    ObPhyOperatorFactory::alloc<ObBlockSampleScan, PHY_BLOCK_SAMPLE_SCAN>,
    ObPhyOperatorFactory::alloc<ObTableInsertReturning, PHY_INSERT_RETURNING>, /*55*/
    NULL,                                                                      // PHY_REPLACE_RETURNING
    NULL,                                                                      // PHY_INSERT_ON_DUP_RETURNING
    ObPhyOperatorFactory::alloc<ObTableDeleteReturning, PHY_DELETE_RETURNING>,
    ObPhyOperatorFactory::alloc<ObTableUpdateReturning, PHY_UPDATE_RETURNING>,
    ObPhyOperatorFactory::alloc<ObTableScanWithChecksum, PHY_TABLE_SCAN_WITH_CHECKSUM>, /*60*/
    ObPhyOperatorFactory::alloc<ObTableScanCreateDomainIndex, PHY_TABLE_SCAN_CREATE_DOMAIN_INDEX>,
    ObPhyOperatorFactory::alloc<ObTableAppendLocalSortData, PHY_APPEND_LOCAL_SORT_DATA>,
    ObPhyOperatorFactory::alloc<ObTableAppendSSTable, PHY_APPEND_SSTABLE>,
    ObPhyOperatorFactory::alloc<ObMultiPartInsert, PHY_MULTI_PART_INSERT>,
    ObPhyOperatorFactory::alloc<ObMultiPartUpdate, PHY_MULTI_PART_UPDATE>, /*65*/
    ObPhyOperatorFactory::alloc<ObMultiPartDelete, PHY_MULTI_PART_DELETE>,
    ObPhyOperatorFactory::alloc<ObUKRowTransform, PHY_UK_ROW_TRANSFORM>,
    ObPhyOperatorFactory::alloc<ObDeterminateTaskTransmit, PHY_DETERMINATE_TASK_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObMultiPartTableScan, PHY_MULTI_PART_TABLE_SCAN>,
    ObPhyOperatorFactory::alloc<ObTableLookup, PHY_TABLE_LOOKUP>, /*70*/
    ObPhyOperatorFactory::alloc<ObGranuleIterator, PHY_GRANULE_ITERATOR>,
    ObPhyOperatorFactory::alloc<ObPxFifoReceive, PHY_PX_FIFO_RECEIVE>,
    ObPhyOperatorFactory::alloc<ObPxMergeSortReceive, PHY_PX_MERGE_SORT_RECEIVE>,
    ObPhyOperatorFactory::alloc<ObPxDistTransmit, PHY_PX_DIST_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObPxRepartTransmit, PHY_PX_REPART_TRANSMIT>, /*75*/
    ObPhyOperatorFactory::alloc<ObPxReduceTransmit, PHY_PX_REDUCE_TRANSMIT>,
    ObPhyOperatorFactory::alloc<ObPxFifoCoord, PHY_PX_FIFO_COORD>,
    ObPhyOperatorFactory::alloc<ObPxMergeSortCoord, PHY_PX_MERGE_SORT_COORD>,
    ObPhyOperatorFactory::alloc<ObTableRowStore, PHY_TABLE_ROW_STORE>,
    NULL,  // ObPhyOperatorFactory::alloc<ObJoinFilter, PHY_JOIN_FILTER>, /*80*/
    ObPhyOperatorFactory::alloc<ObTableConflictRowFetcher, PHY_TABLE_CONFLICT_ROW_FETCHER>,
    ObPhyOperatorFactory::alloc<ObMultiTableReplace, PHY_MULTI_TABLE_REPLACE>,
    ObPhyOperatorFactory::alloc<ObMultiTableInsertUp, PHY_MULTI_TABLE_INSERT_UP>,
    ObPhyOperatorFactory::alloc<ObSequence, PHY_SEQUENCE>,
    ObPhyOperatorFactory::alloc<ObExprValuesWithChild, PHY_EXPR_VALUES_WITH_CHILD>, /*85*/
    NULL,  // ObPhyOperatorFactory::alloc<ObFunctionTable, PHY_FUNCTION_TABLE>,
    ObPhyOperatorFactory::alloc<ObMonitoringDump, PHY_MONITORING_DUMP>,
    ObPhyOperatorFactory::alloc<ObMultiTableMerge, PHY_MULTI_TABLE_MERGE>,
    ObPhyOperatorFactory::alloc<ObLightGranuleIterator, PHY_LIGHT_GRANULE_ITERATOR>,
    ObPhyOperatorFactory::alloc<ObPxMultiPartDelete, PHY_PX_MULTI_PART_DELETE>, /*90*/
    ObPhyOperatorFactory::alloc<ObPxMultiPartUpdate, PHY_PX_MULTI_PART_UPDATE>,
    ObPhyOperatorFactory::alloc<ObPxMultiPartInsert, PHY_PX_MULTI_PART_INSERT>,
    ObPhyOperatorFactory::alloc<ObUnpivot, PHY_UNPIVOT>,
    ObPhyOperatorFactory::alloc<ObConnectBy, PHY_NESTED_LOOP_CONNECT_BY>,
    ObPhyOperatorFactory::alloc<ObLinkScan, PHY_LINK>,
    ObPhyOperatorFactory::alloc<ObTableLock, PHY_LOCK>,
    ObPhyOperatorFactory::alloc<ObMultiPartLock, PHY_MULTI_LOCK>,
    ObPhyOperatorFactory::alloc<ObTempTableInsert, PHY_TEMP_TABLE_INSERT>,
    ObPhyOperatorFactory::alloc<ObTempTableAccess, PHY_TEMP_TABLE_ACCESS>,
    ObPhyOperatorFactory::alloc<ObTempTableTransformation, PHY_TEMP_TABLE_TRANSFORMATION>,
    ObPhyOperatorFactory::alloc<ObMultiTableInsert, PHY_MULTI_TABLE_INSERT>,
    NULL  // ObPhyOperatorFactory::alloc<, PHY_FAKE_TABLE>
};

int ObPhyOperatorFactory::alloc(ObPhyOperatorType type, ObPhyOperator*& phy_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_phy_op_type_valid(type))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(type));
  } else if (NULL == ObPhyOperatorFactory::PHY_OPERATOR_ALLOC[type]) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid op_type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPhyOperatorFactory::PHY_OPERATOR_ALLOC[type](alloc_, phy_op))) {
    OB_LOG(WARN, "fail to alloc phy_op", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, ObPhyOperatorType type>
int ObPhyOperatorFactory::alloc(common::ObIAllocator& alloc, ObPhyOperator*& phy_op)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc phy_operator", K(ret));
  } else {
    phy_op = new (buf) ClassT(alloc);
    phy_op->set_type(type);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
