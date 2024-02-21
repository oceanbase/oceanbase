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

#define USING_LOG_PREFIX SQL_OPT

#include "ob_opt_cost_model_parameter.h"
#include "src/sql/optimizer/ob_optimizer_context.h"

using namespace oceanbase;
using namespace sql;

double ObOptCostModelParameter::get_cpu_tuple_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CPU_TUPLE_COST;
    } else {
        return CPU_TUPLE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_table_scan_cpu_tuple_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return TABLE_SCAN_CPU_TUPLE_COST;
    } else {
        return TABLE_SCAN_CPU_TUPLE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_micro_block_seq_cost(const OptSystemStat& stat) const
{
    if (stat.get_disk_seq_read_speed() <= 0 || stat.get_cpu_speed() <= 0) {
        return MICRO_BLOCK_SEQ_COST;
    } else {
        return MICRO_BLOCK_SEQ_COST * 16 * 1024.0 / stat.get_disk_seq_read_speed();
    }
}

double ObOptCostModelParameter::get_micro_block_rnd_cost(const OptSystemStat& stat) const
{
    if (stat.get_disk_rnd_read_speed() <= 0 || stat.get_cpu_speed() <= 0) {
        return MICRO_BLOCK_RND_COST;
    } else {
        return MICRO_BLOCK_RND_COST * 16 * 1024.0 / stat.get_disk_rnd_read_speed();
    }
}

double ObOptCostModelParameter::get_project_column_cost(const OptSystemStat& stat,
                                                        PROJECT_TYPE type,
                                                        bool is_rnd,
                                                        bool use_column_store) const
{
    if (stat.get_cpu_speed() <= 0) {
        return project_params_[use_column_store][is_rnd][type];
    } else {
        return project_params_[use_column_store][is_rnd][type] / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_fetch_row_rnd_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return FETCH_ROW_RND_COST;
    } else {
        return FETCH_ROW_RND_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_cmp_spatial_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CMP_SPATIAL_COST;
    } else {
        return CMP_SPATIAL_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_materialize_per_byte_write_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return MATERIALIZE_PER_BYTE_WRITE_COST;
    } else {
        return MATERIALIZE_PER_BYTE_WRITE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_read_materialized_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return READ_MATERIALIZED_PER_ROW_COST;
    } else {
        return READ_MATERIALIZED_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_per_aggr_func_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return PER_AGGR_FUNC_COST;
    } else {
        return PER_AGGR_FUNC_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_per_win_func_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return PER_WIN_FUNC_COST;
    } else {
        return PER_WIN_FUNC_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_cpu_operator_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CPU_OPERATOR_COST;
    } else {
        return CPU_OPERATOR_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_join_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return JOIN_PER_ROW_COST;
    } else {
        return JOIN_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_build_hash_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return BUILD_HASH_PER_ROW_COST;
    } else {
        return BUILD_HASH_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_probe_hash_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return PROBE_HASH_PER_ROW_COST;
    } else {
        return PROBE_HASH_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_rescan_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return RESCAN_COST;
    } else {
        return RESCAN_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_network_ser_per_byte_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return NETWORK_SER_PER_BYTE_COST;
    } else {
        return NETWORK_SER_PER_BYTE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_network_deser_per_byte_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return NETWORK_DESER_PER_BYTE_COST;
    } else {
        return NETWORK_DESER_PER_BYTE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_network_trans_per_byte_cost(const OptSystemStat& stat) const
{
    if (stat.get_network_speed() <= 0) {
        return NETWORK_TRANS_PER_BYTE_COST;
    } else {
        return NETWORK_TRANS_PER_BYTE_COST / stat.get_network_speed();
    }
}

double ObOptCostModelParameter::get_px_rescan_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return PX_RESCAN_PER_ROW_COST;
    } else {
        return PX_RESCAN_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_px_batch_rescan_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return PX_BATCH_RESCAN_PER_ROW_COST;
    } else {
        return PX_BATCH_RESCAN_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_nl_scan_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return NL_SCAN_COST;
    } else {
        return NL_SCAN_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_batch_nl_scan_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return BATCH_NL_SCAN_COST;
    } else {
        return BATCH_NL_SCAN_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_nl_get_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return NL_GET_COST;
    } else {
        return NL_GET_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_batch_nl_get_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return BATCH_NL_GET_COST;
    } else {
        return BATCH_NL_GET_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_table_loopup_per_row_rpc_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return TABLE_LOOPUP_PER_ROW_RPC_COST;
    } else {
        return TABLE_LOOPUP_PER_ROW_RPC_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_insert_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return INSERT_PER_ROW_COST;
    } else {
        return INSERT_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_insert_index_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return INSERT_INDEX_PER_ROW_COST;
    } else {
        return INSERT_INDEX_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_insert_check_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return INSERT_CHECK_PER_ROW_COST;
    } else {
        return INSERT_CHECK_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_update_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return UPDATE_PER_ROW_COST;
    } else {
        return UPDATE_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_update_index_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return UPDATE_INDEX_PER_ROW_COST;
    } else {
        return UPDATE_INDEX_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_update_check_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return UPDATE_CHECK_PER_ROW_COST;
    } else {
        return UPDATE_CHECK_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_delete_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return DELETE_PER_ROW_COST;
    } else {
        return DELETE_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_delete_index_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return DELETE_INDEX_PER_ROW_COST;
    } else {
        return DELETE_INDEX_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_delete_check_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return DELETE_CHECK_PER_ROW_COST;
    } else {
        return DELETE_CHECK_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_spatial_per_row_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return SPATIAL_PER_ROW_COST;
    } else {
        return SPATIAL_PER_ROW_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_range_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return RANGE_COST;
    } else {
        return RANGE_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_comparison_cost(const OptSystemStat& stat, int64_t type) const
{
    double cost = 0;
    if (type >=0 && type <= ObMaxTC) {
        cost = comparison_params_[type];
    } else {
        cost = comparison_params_[0];
    }
    if (stat.get_cpu_speed() <= 0) {
        return cost;
    } else {
        return cost / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_hash_cost(const OptSystemStat& stat, int64_t type) const
{
    double cost = 0;
    if (type >=0 && type <= ObMaxTC) {
        cost = hash_params_[type];
    } else {
        cost = hash_params_[0];
    }
    if (stat.get_cpu_speed() <= 0) {
        return cost;
    } else {
        return cost / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_cmp_lob_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CMP_LOB_COST;
    } else {
        return CMP_LOB_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_cmp_udf_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CMP_UDF_COST;
    } else {
        return CMP_UDF_COST / stat.get_cpu_speed();
    }
}

double ObOptCostModelParameter::get_cmp_err_handle_expr_cost(const OptSystemStat& stat) const
{
    if (stat.get_cpu_speed() <= 0) {
        return CMP_ERR_HANDLE_EXPR_COST;
    } else {
        return CMP_ERR_HANDLE_EXPR_COST / stat.get_cpu_speed();
    }
}
