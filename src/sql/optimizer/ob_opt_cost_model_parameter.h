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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_PARAMETER_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_PARAMETER_
#include "common/object/ob_obj_type.h"
#define DEFAULT_CPU_SPEED 2500
#define DEFAULT_DISK_SEQ_READ_SPEED 1024
#define DEFAULT_DISK_RND_READ_SPEED 512
#define DEFAULT_NETWORK_SPEED 156
#define DEFAULT_MACR_BLOCK_SIZE  (16 * 1024)

namespace oceanbase
{
namespace sql
{
class OptSystemStat;
enum PROJECT_TYPE {
  PROJECT_INT = 0,
  PROJECT_NUMBER,
  PROJECT_CHAR,
  MAX_PROJECT_TYPE
};
class ObOptCostModelParameter {
public:
  explicit ObOptCostModelParameter(
    const double DEFAULT_CPU_TUPLE_COST,
    const double DEFAULT_TABLE_SCAN_CPU_TUPLE_COST,
    const double DEFAULT_MICRO_BLOCK_SEQ_COST,
    const double DEFAULT_MICRO_BLOCK_RND_COST,
    const double DEFAULT_FETCH_ROW_RND_COST,
    const double DEFAULT_CMP_GEO_COST,
    const double DEFAULT_MATERIALIZE_PER_BYTE_WRITE_COST,
    const double DEFAULT_READ_MATERIALIZED_PER_ROW_COST,
    const double DEFAULT_PER_AGGR_FUNC_COST,
    const double DEFAULT_PER_WIN_FUNC_COST,
    const double DEFAULT_CPU_OPERATOR_COST,
    const double DEFAULT_JOIN_PER_ROW_COST,
    const double DEFAULT_BUILD_HASH_PER_ROW_COST,
    const double DEFAULT_PROBE_HASH_PER_ROW_COST,
    const double DEFAULT_RESCAN_COST,
    const double DEFAULT_NETWORK_SER_PER_BYTE_COST,
    const double DEFAULT_NETWORK_DESER_PER_BYTE_COST,
    const double DEFAULT_NETWORK_TRANS_PER_BYTE_COST,
    const double DEFAULT_PX_RESCAN_PER_ROW_COST,
    const double DEFAULT_PX_BATCH_RESCAN_PER_ROW_COST,
    const double DEFAULT_NL_SCAN_COST,
    const double DEFAULT_BATCH_NL_SCAN_COST,
    const double DEFAULT_NL_GET_COST,
    const double DEFAULT_BATCH_NL_GET_COST,
    const double DEFAULT_TABLE_LOOPUP_PER_ROW_RPC_COST,
    const double DEFAULT_INSERT_PER_ROW_COST,
    const double DEFAULT_INSERT_INDEX_PER_ROW_COST,
    const double DEFAULT_INSERT_CHECK_PER_ROW_COST,
    const double DEFAULT_UPDATE_PER_ROW_COST,
    const double DEFAULT_UPDATE_INDEX_PER_ROW_COST,
    const double DEFAULT_UPDATE_CHECK_PER_ROW_COST,
    const double DEFAULT_DELETE_PER_ROW_COST,
    const double DEFAULT_DELETE_INDEX_PER_ROW_COST,
    const double DEFAULT_DELETE_CHECK_PER_ROW_COST,
    const double DEFAULT_SPATIAL_PER_ROW_COST,
    const double DEFAULT_RANGE_COST,
    const double DEFAULT_CMP_UDF_COST,
    const double DEFAULT_CMP_LOB_COST,
    const double DEFAULT_CMP_ERR_HANDLE_EXPR_COST,
    const double (&comparison_params)[common::ObMaxTC + 1],
		const double (&hash_params)[common::ObMaxTC + 1],
		const double (&project_params)[2][2][MAX_PROJECT_TYPE]
    )
    : CPU_TUPLE_COST(DEFAULT_CPU_TUPLE_COST),
      TABLE_SCAN_CPU_TUPLE_COST(DEFAULT_TABLE_SCAN_CPU_TUPLE_COST),
      MICRO_BLOCK_SEQ_COST(DEFAULT_MICRO_BLOCK_SEQ_COST),
      MICRO_BLOCK_RND_COST(DEFAULT_MICRO_BLOCK_RND_COST),
      FETCH_ROW_RND_COST(DEFAULT_FETCH_ROW_RND_COST),
      CMP_SPATIAL_COST(DEFAULT_CMP_GEO_COST),
      MATERIALIZE_PER_BYTE_WRITE_COST(DEFAULT_MATERIALIZE_PER_BYTE_WRITE_COST),
      READ_MATERIALIZED_PER_ROW_COST(DEFAULT_READ_MATERIALIZED_PER_ROW_COST),
      PER_AGGR_FUNC_COST(DEFAULT_PER_AGGR_FUNC_COST),
      PER_WIN_FUNC_COST(DEFAULT_PER_WIN_FUNC_COST),
      CPU_OPERATOR_COST(DEFAULT_CPU_OPERATOR_COST),
      JOIN_PER_ROW_COST(DEFAULT_JOIN_PER_ROW_COST),
      BUILD_HASH_PER_ROW_COST(DEFAULT_BUILD_HASH_PER_ROW_COST),
      PROBE_HASH_PER_ROW_COST(DEFAULT_PROBE_HASH_PER_ROW_COST),
      RESCAN_COST(DEFAULT_RESCAN_COST),
      NETWORK_SER_PER_BYTE_COST(DEFAULT_NETWORK_SER_PER_BYTE_COST),
      NETWORK_DESER_PER_BYTE_COST(DEFAULT_NETWORK_DESER_PER_BYTE_COST),
      NETWORK_TRANS_PER_BYTE_COST(DEFAULT_NETWORK_TRANS_PER_BYTE_COST),
      PX_RESCAN_PER_ROW_COST(DEFAULT_PX_RESCAN_PER_ROW_COST),
      PX_BATCH_RESCAN_PER_ROW_COST(DEFAULT_PX_BATCH_RESCAN_PER_ROW_COST),
      NL_SCAN_COST(DEFAULT_NL_SCAN_COST),
      BATCH_NL_SCAN_COST(DEFAULT_BATCH_NL_SCAN_COST),
      NL_GET_COST(DEFAULT_NL_GET_COST),
      BATCH_NL_GET_COST(DEFAULT_BATCH_NL_GET_COST),
      TABLE_LOOPUP_PER_ROW_RPC_COST(DEFAULT_TABLE_LOOPUP_PER_ROW_RPC_COST),
      INSERT_PER_ROW_COST(DEFAULT_INSERT_PER_ROW_COST),
      INSERT_INDEX_PER_ROW_COST(DEFAULT_INSERT_INDEX_PER_ROW_COST),
      INSERT_CHECK_PER_ROW_COST(DEFAULT_INSERT_CHECK_PER_ROW_COST),
      UPDATE_PER_ROW_COST(DEFAULT_UPDATE_PER_ROW_COST),
      UPDATE_INDEX_PER_ROW_COST(DEFAULT_UPDATE_INDEX_PER_ROW_COST),
      UPDATE_CHECK_PER_ROW_COST(DEFAULT_UPDATE_CHECK_PER_ROW_COST),
      DELETE_PER_ROW_COST(DEFAULT_DELETE_PER_ROW_COST),
      DELETE_INDEX_PER_ROW_COST(DEFAULT_DELETE_INDEX_PER_ROW_COST),
      DELETE_CHECK_PER_ROW_COST(DEFAULT_DELETE_CHECK_PER_ROW_COST),
      SPATIAL_PER_ROW_COST(DEFAULT_SPATIAL_PER_ROW_COST),
      RANGE_COST(DEFAULT_RANGE_COST),
      CMP_UDF_COST(DEFAULT_CMP_UDF_COST),
      CMP_LOB_COST(DEFAULT_CMP_LOB_COST),
      CMP_ERR_HANDLE_EXPR_COST(DEFAULT_CMP_ERR_HANDLE_EXPR_COST),
      comparison_params_(comparison_params),
		  hash_params_(hash_params),
			project_params_(project_params)
    {
    }

  double get_cpu_tuple_cost(const OptSystemStat& stat) const;
  double get_table_scan_cpu_tuple_cost(const OptSystemStat& stat) const;
  double get_micro_block_seq_cost(const OptSystemStat& stat) const;
  double get_micro_block_rnd_cost(const OptSystemStat& stat) const;
  double get_project_column_cost(const OptSystemStat& stat,
                                 PROJECT_TYPE type,
                                 bool is_rnd,
                                 bool use_column_store) const;
  double get_fetch_row_rnd_cost(const OptSystemStat& stat) const;
  double get_cmp_spatial_cost(const OptSystemStat& stat) const;
  double get_materialize_per_byte_write_cost(const OptSystemStat& stat) const;
  double get_read_materialized_per_row_cost(const OptSystemStat& stat) const;
  double get_per_aggr_func_cost(const OptSystemStat& stat) const;
  double get_per_win_func_cost(const OptSystemStat& stat) const;
  double get_cpu_operator_cost(const OptSystemStat& stat) const;
  double get_join_per_row_cost(const OptSystemStat& stat) const;
  double get_build_hash_per_row_cost(const OptSystemStat& stat) const;
  double get_probe_hash_per_row_cost(const OptSystemStat& stat) const;
  double get_rescan_cost(const OptSystemStat& stat) const;
  double get_network_ser_per_byte_cost(const OptSystemStat& stat) const;
  double get_network_deser_per_byte_cost(const OptSystemStat& stat) const;
  double get_network_trans_per_byte_cost(const OptSystemStat& stat) const;
  double get_px_rescan_per_row_cost(const OptSystemStat& stat) const;
  double get_px_batch_rescan_per_row_cost(const OptSystemStat& stat) const;
  double get_nl_scan_cost(const OptSystemStat& stat) const;
  double get_batch_nl_scan_cost(const OptSystemStat& stat) const;
  double get_nl_get_cost(const OptSystemStat& stat) const;
  double get_batch_nl_get_cost(const OptSystemStat& stat) const;
  double get_table_loopup_per_row_rpc_cost(const OptSystemStat& stat) const;
  double get_insert_per_row_cost(const OptSystemStat& stat) const;
  double get_insert_index_per_row_cost(const OptSystemStat& stat) const;
  double get_insert_check_per_row_cost(const OptSystemStat& stat) const;
  double get_update_per_row_cost(const OptSystemStat& stat) const;
  double get_update_index_per_row_cost(const OptSystemStat& stat) const;
  double get_update_check_per_row_cost(const OptSystemStat& stat) const;
  double get_delete_per_row_cost(const OptSystemStat& stat) const;
  double get_delete_index_per_row_cost(const OptSystemStat& stat) const;
  double get_delete_check_per_row_cost(const OptSystemStat& stat) const;
  double get_spatial_per_row_cost(const OptSystemStat& stat) const;
  double get_range_cost(const OptSystemStat& stat) const;
  double get_comparison_cost(const OptSystemStat& stat, int64_t type) const;
  double get_hash_cost(const OptSystemStat& stat, int64_t type) const;
  double get_cmp_lob_cost(const OptSystemStat& stat) const;
  double get_cmp_udf_cost(const OptSystemStat& stat) const;
  double get_cmp_err_handle_expr_cost(const OptSystemStat& stat) const;

protected:
  /** 读取一行的CPU开销，基本上只包括get_next_row()操作 */
  double CPU_TUPLE_COST;
  /** 存储层吐出一行的代价 **/
  double TABLE_SCAN_CPU_TUPLE_COST;
  /** 顺序读取一个微块并反序列化的开销 */
  double MICRO_BLOCK_SEQ_COST;
  /** 随机读取一个微块并反序列化的开销 */
  double MICRO_BLOCK_RND_COST;
  /** 随机读取中定位某一行所在位置的开销 */
  double FETCH_ROW_RND_COST;
  /** 比较一次空间数据的代价 */
  double CMP_SPATIAL_COST;
  /** 物化一个字节的代价 */
  double MATERIALIZE_PER_BYTE_WRITE_COST;
  /** 读取物化后的行的代价，即对物化后数据结构的get_next_row() */
  double READ_MATERIALIZED_PER_ROW_COST;
  /** 一次聚集函数计算的代价 */
  double PER_AGGR_FUNC_COST;
  /** 一次窗口函数计算的代价 */
  double PER_WIN_FUNC_COST;
  /** 一次操作的基本代价 */
  double CPU_OPERATOR_COST;
  /** 连接两表的一行的基本代价 */
  double JOIN_PER_ROW_COST;
  /** 构建hash table时每行的均摊代价 */
  double BUILD_HASH_PER_ROW_COST;
  /** 查询hash table时每行的均摊代价 */
  double PROBE_HASH_PER_ROW_COST;
  double RESCAN_COST;
  /*network serialization cost for one byte*/
  double NETWORK_SER_PER_BYTE_COST;
  /*network de-serialization cost for one byte*/
  double NETWORK_DESER_PER_BYTE_COST;
  /** 网络传输1个字节的代价 */
  double NETWORK_TRANS_PER_BYTE_COST;
  /*additional px-rescan cost*/
  double PX_RESCAN_PER_ROW_COST;
  double PX_BATCH_RESCAN_PER_ROW_COST;
  //条件下压nestloop join右表扫一次的代价
  double NL_SCAN_COST;
  //条件下压batch nestloop join右表扫一次的代价
  double BATCH_NL_SCAN_COST;
  //条件下压nestloop join右表GET一次的代价
  double NL_GET_COST;
  //条件下压batch nestloop join右表GET一次的代价
  double BATCH_NL_GET_COST;
  //table look up一行的rpc代价
  double TABLE_LOOPUP_PER_ROW_RPC_COST;
  //insert一行主表的代价
  double INSERT_PER_ROW_COST;
  //insert一行索引表的代价
  double INSERT_INDEX_PER_ROW_COST;
  //insert单个约束检查代价
  double INSERT_CHECK_PER_ROW_COST;
  //update一行主表的代价
  double UPDATE_PER_ROW_COST;
  //update一行索引表的代价
  double UPDATE_INDEX_PER_ROW_COST;
  //update单个约束检查代价
  double UPDATE_CHECK_PER_ROW_COST;
  //delete一行主表的代价
  double DELETE_PER_ROW_COST;
  //delete一行索引表的代价
  double DELETE_INDEX_PER_ROW_COST;
  //delete单个约束检查代价
  double DELETE_CHECK_PER_ROW_COST;
  //空间索引扫描的线性参数
  double SPATIAL_PER_ROW_COST;
  //存储层切换一次range的代价
  double RANGE_COST;
  //计算一个UDF的代价
  double CMP_UDF_COST;
  //计算一个返回值为LOB的表达式的代价
  double CMP_LOB_COST;
  //计算一个需处理异常的表达式的代价
  double CMP_ERR_HANDLE_EXPR_COST;

  const double (&comparison_params_)[common::ObMaxTC + 1];
  const double (&hash_params_)[common::ObMaxTC + 1];  /*
   *                             +-sequence access project
   *              +-row store----+
   *              |              +-random access project
   * project cost-+
   *              |              +-sequence access project
   *              +-column store-+
   *                             +-random access project
   */
  const double (&project_params_)[2][2][MAX_PROJECT_TYPE];
};

}
}
#endif /*OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_PARAMETER_*/