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
#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "sql/optimizer/ob_storage_estimator.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_topk_hist_estimator.h"
namespace oceanbase
{
namespace common
{

ObBasicStatsEstimator::ObBasicStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObStatsEstimator(ctx, allocator)
{}

template<class T>
int ObBasicStatsEstimator::add_stat_item(const T &item)
{
  int ret = OB_SUCCESS;
  ObStatItem *cpy = NULL;
  if (!item.is_needed()) {
    // do nothing
  } else if (OB_ISNULL(cpy = copy_stat_item(allocator_, item))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to copy stat item", K(ret));
  } else if (OB_FAIL(stat_items_.push_back(cpy))) {
    LOG_WARN("failed to push back stat item", K(ret));
  }
  return ret;
}

int ObBasicStatsEstimator::estimate(const ObOptStatGatherParam &param,
                                    ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnStatParam> &column_params = param.column_params_;
  ObString calc_part_id_str;
  ObOptTableStat tab_stat;
  ObOptStat src_opt_stat;
  src_opt_stat.table_stat_ = &tab_stat;
  ObOptTableStat *src_tab_stat = src_opt_stat.table_stat_;
  ObIArray<ObOptColumnStat*> &src_col_stats = src_opt_stat.column_stats_;
  ObArenaAllocator allocator("ObBasicStatsEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  // Note that there are dependences between different kinds of statistics
  //            1. RowCount should be added at the first
  //            2. NumDistinct should be estimated before TopKHist
  //            3. AvgRowLen should be added at the last
  if (OB_UNLIKELY(dst_opt_stats.empty()) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(dst_opt_stats.empty()), K(param.allocator_));
  } else if (OB_FAIL(ObDbmsStatsUtils::init_col_stats(allocator,
                                                      column_params.count(),
                                                      src_col_stats))) {
    LOG_WARN("failed init col stats", K(ret));
  } else if (OB_FAIL(fill_hints(allocator, param.tab_name_, param.gather_vectorize_,
                                param.use_column_store_ && dst_opt_stats.count() == 1))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(add_from_table(param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to add query sql parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(fill_sample_info(allocator, param.sample_info_))) {
    LOG_WARN("failed to fill sample info", K(ret));
  } else if (OB_FAIL(fill_specify_scn_info(allocator, param.sepcify_scn_))) {
    LOG_WARN("failed to fill specify scn info", K(ret));
  } else if (OB_FAIL(add_stat_item(ObStatRowCount(src_tab_stat)))) {
    LOG_WARN("failed to add row count", K(ret));
  } else if (!param.is_split_gather_) {
    if (dst_opt_stats.count() > 1) {
      if (OB_FAIL(fill_group_by_info(allocator, param, calc_part_id_str))) {
        LOG_WARN("failed to add group by info", K(ret));
      } else if (OB_FAIL(add_stat_item(ObPartitionId(src_tab_stat, calc_part_id_str, -1)))) {
        LOG_WARN("failed to add partition id", K(ret));
      } else if (param.is_specify_partition_ &&
                 OB_FAIL(fill_partition_info(allocator, param.partition_infos_))) {
        LOG_WARN("failed to add partition info", K(ret));
      }
    } else if (OB_UNLIKELY(param.partition_infos_.count() > 1) ||
               OB_ISNULL(dst_opt_stats.at(0).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.partition_infos_));
    } else if (!param.partition_infos_.empty() &&
               OB_FAIL(fill_partition_info(allocator, param.partition_infos_.at(0).part_name_))) {
      LOG_WARN("failed to add partition info", K(ret));
    } else {
      src_tab_stat->set_partition_id(dst_opt_stats.at(0).table_stat_->get_partition_id());
    }
  } else {//table has been split gather because the system resource limit
    if (dst_opt_stats.count() > 1) {
      if (OB_FAIL(fill_group_by_info(allocator, param, calc_part_id_str))) {
        LOG_WARN("failed to add group by info", K(ret));
      } else if (OB_FAIL(add_stat_item(ObPartitionId(src_tab_stat, calc_part_id_str, -1)))) {
        LOG_WARN("failed to add partition id", K(ret));
      } else if (OB_FAIL(fill_partition_info(allocator, param.partition_infos_))) {
        LOG_WARN("failed to add partition info", K(ret));
      }
    } else if (OB_UNLIKELY(param.partition_infos_.count() > 1) ||
               OB_ISNULL(dst_opt_stats.at(0).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.partition_infos_));
    } else if (!param.partition_infos_.empty() &&
               OB_FAIL(fill_partition_info(allocator, param.partition_infos_.at(0).part_name_))) {
      LOG_WARN("failed to add partition info", K(ret));
    } else {
      src_tab_stat->set_partition_id(dst_opt_stats.at(0).table_stat_->get_partition_id());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    const ObColumnStatParam *col_param = &column_params.at(i);
    if (OB_FAIL(add_stat_item(ObStatMaxValue(col_param, src_col_stats.at(i)))) ||
        OB_FAIL(add_stat_item(ObStatMinValue(col_param, src_col_stats.at(i)))) ||
        OB_FAIL(add_stat_item(ObStatNumNull(col_param, src_tab_stat, src_col_stats.at(i)))) ||
        OB_FAIL(add_stat_item(ObStatNumDistinct(col_param, src_col_stats.at(i), param.need_approx_ndv_))) ||
        OB_FAIL(add_stat_item(ObStatAvgLen(col_param, src_col_stats.at(i)))) ||
        OB_FAIL(add_stat_item(ObStatLlcBitmap(col_param, src_col_stats.at(i))))) {
      LOG_WARN("failed to add statistic item", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_stat_item(ObStatAvgRowLen(src_tab_stat, src_col_stats)))) {
      LOG_WARN("failed to add avg row size estimator", K(ret));
    } else if (OB_FAIL(pack(raw_sql))) {
      LOG_WARN("failed to pack raw sql", K(ret));
    } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql.string(), true, src_opt_stat, dst_opt_stats))) {
      LOG_WARN("failed to evaluate basic stats", K(ret));
    } else if (OB_FAIL(refine_basic_stats(param, dst_opt_stats))) {
      LOG_WARN("failed to refine basic stats", K(ret));
    } else {
      LOG_TRACE("basic stats is collected", K(dst_opt_stats.count()));
    }
  }
  return ret;
}

int ObBasicStatsEstimator::estimate_block_count(ObExecContext &ctx,
                                                const ObTableStatParam &param,
                                                PartitionIdBlockMap &id_block_map,
                                                bool &use_column_store,
                                                bool &use_split_part)
{
  int ret = OB_SUCCESS;
  ObGlobalTableStat global_tab_stat;
  ObSEArray<ObGlobalTableStat, 4> first_part_tab_stats;
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<ObObjectID, 4> partition_ids;
  ObSEArray<EstimateBlockRes, 4> estimate_result;
  hash::ObHashMap<int64_t, int64_t> first_part_idx_map;
  ObArray<uint64_t> column_group_ids;
  uint64_t table_id = share::is_oracle_mapping_real_virtual_table(param.table_id_) ?
                              share::get_real_table_mappings_tid(param.table_id_) : param.table_id_;
  use_column_store = false;
  use_split_part = false;
  if (is_virtual_table(table_id)) {//virtual table no need estimate block count
    //do nothing
  } else if (OB_FAIL(get_all_tablet_id_and_object_id(param, tablet_ids, partition_ids))) {
    LOG_WARN("failed to get all tablet id and object id", K(ret));
  } else if (param.part_level_ == share::schema::PARTITION_LEVEL_TWO &&
             OB_FAIL(first_part_tab_stats.prepare_allocate(param.all_part_infos_.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else if (param.part_level_ == share::schema::PARTITION_LEVEL_TWO &&
             OB_FAIL(generate_first_part_idx_map(param.all_part_infos_, first_part_idx_map))) {
    LOG_WARN("failed to generate first part idx map", K(ret));
  } else if (OB_FAIL(generate_column_group_ids(param, column_group_ids))) {
    LOG_WARN("failed to generate column group ids", K(ret), K(param));
  } else if (OB_FAIL(do_estimate_block_count(ctx, param.tenant_id_, table_id, tablet_ids,
                                             partition_ids, column_group_ids, estimate_result))) {
    LOG_WARN("failed to do estimate block count", K(ret));
  } else {
    int64_t total_sstable_row_cnt = 0;
    int64_t total_memtable_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < estimate_result.count(); ++i) {
      BlockNumStat *block_num_stat = NULL;
      void *buf = NULL;
      if (OB_ISNULL(param.allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(param.allocator_));
      } else if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(BlockNumStat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(buf));
      } else {
        block_num_stat = new (buf) BlockNumStat();
        block_num_stat->tab_macro_cnt_ = estimate_result.at(i).macro_block_count_;
        block_num_stat->tab_micro_cnt_ = estimate_result.at(i).micro_block_count_;
        block_num_stat->sstable_row_cnt_ = estimate_result.at(i).sstable_row_count_;
        block_num_stat->memtable_row_cnt_ = estimate_result.at(i).memtable_row_count_;
        total_sstable_row_cnt += estimate_result.at(i).sstable_row_count_;
        total_memtable_row_cnt += estimate_result.at(i).memtable_row_count_;
        int64_t partition_id = static_cast<int64_t>(estimate_result.at(i).part_id_);
        if (OB_FAIL(block_num_stat->cg_macro_cnt_arr_.assign(estimate_result.at(i).cg_macro_cnt_arr_)) ||
            OB_FAIL(block_num_stat->cg_micro_cnt_arr_.assign(estimate_result.at(i).cg_micro_cnt_arr_))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(id_block_map.set_refactored(partition_id, block_num_stat))) {
          LOG_WARN("failed to set refactored", K(ret));
        } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ONE) {
          if (OB_FAIL(global_tab_stat.add(1, 0, 0,
                                          block_num_stat->tab_macro_cnt_,
                                          block_num_stat->tab_micro_cnt_,
                                          block_num_stat->cg_macro_cnt_arr_,
                                          block_num_stat->cg_micro_cnt_arr_,
                                          block_num_stat->sstable_row_cnt_,
                                          block_num_stat->memtable_row_cnt_))) {
            LOG_WARN("faild to add", K(ret));
          }
        } else if (param.part_level_ == share::schema::PARTITION_LEVEL_TWO) {
          int64_t cur_part_id = -1;
          if (OB_UNLIKELY(!ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_, partition_id, cur_part_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(partition_id), K(cur_part_id), K(param));
          } else {
            if (OB_FAIL(global_tab_stat.add(1, 0, 0,
                                            block_num_stat->tab_macro_cnt_,
                                            block_num_stat->tab_micro_cnt_,
                                            block_num_stat->cg_macro_cnt_arr_,
                                            block_num_stat->cg_micro_cnt_arr_,
                                            block_num_stat->sstable_row_cnt_,
                                            block_num_stat->memtable_row_cnt_))) {
              LOG_WARN("faild to add", K(ret));
            } else {
              int64_t idx = 0;
              if (OB_FAIL(first_part_idx_map.get_refactored(cur_part_id, idx))) {
                LOG_WARN("failed to set refactored", K(ret));
              } else if (OB_UNLIKELY(idx < 0 || idx >= first_part_tab_stats.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get invalid part id", K(ret), K(idx), K(partition_id), K(cur_part_id),
                                                K(first_part_tab_stats.count()));
              } else if (OB_FAIL(first_part_tab_stats.at(idx).add(1, 0, 0,
                                                                  block_num_stat->tab_macro_cnt_,
                                                                  block_num_stat->tab_micro_cnt_,
                                                                  block_num_stat->cg_macro_cnt_arr_,
                                                                  block_num_stat->cg_micro_cnt_arr_,
                                                                  block_num_stat->sstable_row_cnt_,
                                                                  block_num_stat->memtable_row_cnt_))) {
                LOG_WARN("faild to add", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_can_use_column_store_and_split_part_gather(total_sstable_row_cnt,
                                                                   total_memtable_row_cnt,
                                                                   column_group_ids.count(),
                                                                   partition_ids.count(),
                                                                   param.degree_,
                                                                   use_column_store,
                                                                   use_split_part))) {
        LOG_WARN("failed to check can use column table and split part gather", K(ret));
      } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ONE ||
                 param.part_level_ == share::schema::PARTITION_LEVEL_TWO) {
        BlockNumStat *block_num_stat = NULL;
        void *buf = NULL;
        if (OB_ISNULL(param.allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(param.allocator_));
        } else if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(BlockNumStat)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret), K(buf));
        } else {
          block_num_stat = new (buf) BlockNumStat();
          block_num_stat->tab_macro_cnt_ = global_tab_stat.get_macro_block_count();
          block_num_stat->tab_micro_cnt_ = global_tab_stat.get_micro_block_count();
          block_num_stat->sstable_row_cnt_ = global_tab_stat.get_sstable_row_cnt();
          block_num_stat->memtable_row_cnt_ = global_tab_stat.get_memtable_row_cnt();
          if (OB_FAIL(block_num_stat->cg_macro_cnt_arr_.assign(global_tab_stat.get_cg_macro_arr())) ||
              OB_FAIL(block_num_stat->cg_micro_cnt_arr_.assign(global_tab_stat.get_cg_micro_arr()))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (OB_FAIL(id_block_map.set_refactored(-1, block_num_stat))) {
            LOG_WARN("failed to set refactored", K(ret));
          } else if (param.part_level_ == share::schema::PARTITION_LEVEL_TWO &&
                    OB_UNLIKELY(first_part_tab_stats.count() != param.all_part_infos_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(first_part_tab_stats), K(param.all_part_infos_));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < first_part_tab_stats.count(); ++i) {
              if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(BlockNumStat)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc memory", K(ret), K(buf));
              } else {
                block_num_stat = new (buf) BlockNumStat();
                block_num_stat->tab_macro_cnt_ = first_part_tab_stats.at(i).get_macro_block_count();
                block_num_stat->tab_micro_cnt_ = first_part_tab_stats.at(i).get_micro_block_count();
                block_num_stat->sstable_row_cnt_ = first_part_tab_stats.at(i).get_sstable_row_cnt();
                block_num_stat->memtable_row_cnt_ = first_part_tab_stats.at(i).get_memtable_row_cnt();
                if (OB_FAIL(block_num_stat->cg_macro_cnt_arr_.assign(first_part_tab_stats.at(i).get_cg_macro_arr())) ||
                    OB_FAIL(block_num_stat->cg_micro_cnt_arr_.assign(first_part_tab_stats.at(i).get_cg_micro_arr()))) {
                  LOG_WARN("failed to assign", K(ret));
                } else if (OB_FAIL(id_block_map.set_refactored(param.all_part_infos_.at(i).part_id_, block_num_stat))) {
                  LOG_WARN("failed to set refactored", K(ret));
                } else {/*do nothing*/}
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::do_estimate_block_count(ObExecContext &ctx,
                                                   const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   const ObIArray<ObTabletID> &tablet_ids,
                                                   const ObIArray<ObObjectID> &partition_ids,
                                                   const ObIArray<uint64_t> &column_group_ids,
                                                   ObIArray<EstimateBlockRes> &estimate_res)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  const int64_t MAX_RETRY_CNT = 10;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("failed to check status", K(ret));
      retry_cnt = MAX_RETRY_CNT;
    } else if (OB_FAIL(do_estimate_block_count_and_row_count(ctx, tenant_id, table_id, tablet_ids,
                                                             partition_ids, column_group_ids, estimate_res))) {
      LOG_WARN("failed to do estimate block count and row count", K(ret));
      if (DAS_CTX(ctx).get_location_router().is_refresh_location_error(ret)) {
        DAS_CTX(ctx).get_location_router().refresh_location_cache_by_errno(true, ret);
        ++ retry_cnt;
        ob_usleep(1000L * 1000L); // retry interval 1s
      } else {
        retry_cnt = MAX_RETRY_CNT;
      }
    }
  } while (OB_FAIL(ret) && retry_cnt < MAX_RETRY_CNT);
  return ret;
}

int ObBasicStatsEstimator::do_estimate_block_count_and_row_count(ObExecContext &ctx,
                                                                 const uint64_t tenant_id,
                                                                 const uint64_t table_id,
                                                                 const ObIArray<ObTabletID> &tablet_ids,
                                                                 const ObIArray<ObObjectID> &partition_ids,
                                                                 const ObIArray<uint64_t> &column_group_ids,
                                                                 ObIArray<EstimateBlockRes> &estimate_res)
{
  int ret = OB_SUCCESS;
  typedef common::ObSEArray<ObCandiTabletLoc, 4> ObCandiTabletLocArray;
  SMART_VAR(ObCandiTabletLocArray, candi_tablet_locs) {
    if (OB_FAIL(get_tablet_locations(ctx, table_id, tablet_ids, partition_ids, candi_tablet_locs))) {
      LOG_WARN("failed to get tablet locations", K(ret));
    } else if (OB_UNLIKELY(candi_tablet_locs.count() != tablet_ids.count() ||
                          candi_tablet_locs.count() != partition_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(candi_tablet_locs.count()), K(tablet_ids.count()),
                                      K(partition_ids.count()), K(ret));
    } else if (OB_FAIL(estimate_res.prepare_allocate(partition_ids.count()))) {
      LOG_WARN("Partitoin location list prepare error", K(ret));
    } else {
      ObSEArray<ObAddr, 4> all_selected_addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs.count(); ++i) {
        ObAddr selected_addr;
        if (OB_FAIL(ObSQLUtils::choose_best_partition_replica_addr(ctx.get_addr(),
                                                                  candi_tablet_locs.at(i),
                                                                  true,
                                                                  selected_addr))) {
          LOG_WARN("failed to get best partition replica addr", K(ret), K(candi_tablet_locs), K(i),
                                                                K(ctx.get_addr()));
        } else if (OB_FAIL(all_selected_addr.push_back(selected_addr))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
      ObSqlBitSet<> skip_idx_set;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_selected_addr.count(); ++i) {
        if (skip_idx_set.has_member(i)) {//have been estimate
          //do nothing
        } else {
          ObAddr &cur_selected_addr = all_selected_addr.at(i);
          obrpc::ObEstBlockArg arg;
          obrpc::ObEstBlockRes result;
          ObSEArray<int64_t, 4> selected_tablet_idx;
          for (int64_t j = i ; OB_SUCC(ret) && j < all_selected_addr.count(); ++j) {
            if (skip_idx_set.has_member(j)) {//have been estimate
              //do nothing
            } else if (all_selected_addr.at(j) == cur_selected_addr) {
              if (OB_UNLIKELY(tablet_ids.at(j) !=
                              candi_tablet_locs.at(j).get_partition_location().get_tablet_id())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected error", K(ret), K(tablet_ids), K(j),
                                K(candi_tablet_locs.at(j).get_partition_location().get_tablet_id()));
              } else {
                obrpc::ObEstBlockArgElement arg_element;
                arg_element.tenant_id_ = tenant_id;
                arg_element.tablet_id_ = candi_tablet_locs.at(j).get_partition_location().get_tablet_id();
                arg_element.ls_id_ = candi_tablet_locs.at(j).get_partition_location().get_ls_id();
                if (OB_FAIL(arg_element.column_group_ids_.assign(column_group_ids))) {
                  LOG_WARN("failed to assign", K(ret));
                } else if (OB_FAIL(arg.tablet_params_arg_.push_back(arg_element))) {
                  LOG_WARN("failed to push back", K(ret));
                } else if (OB_FAIL(skip_idx_set.add_member(j))) {//record
                  LOG_WARN("failed to add members", K(ret));
                } else if (OB_FAIL(selected_tablet_idx.push_back(j))) {
                  LOG_WARN("failed to push back", K(ret));
                } else {/*do nothing*/}
              }
            } else {/*do nothing*/}
          }
          if (OB_SUCC(ret)) {//begin storage estimate block count
            if (OB_FAIL(stroage_estimate_block_count_and_row_count(ctx, cur_selected_addr, arg, result))) {
              LOG_WARN("failed to stroage estimate block count", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < selected_tablet_idx.count(); ++i) {
                int64_t idx = selected_tablet_idx.at(i);
                if (OB_UNLIKELY(idx >= estimate_res.count() || idx >= partition_ids.count() ||
                                selected_tablet_idx.count() != result.tablet_params_res_.count())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get unexpected error", K(idx), K(estimate_res), K(result), K(partition_ids),
                                                  K(selected_tablet_idx), K(ret));
                } else {
                  estimate_res.at(idx).part_id_ = partition_ids.at(idx);
                  estimate_res.at(idx).macro_block_count_ = result.tablet_params_res_.at(i).macro_block_count_;
                  estimate_res.at(idx).micro_block_count_ = result.tablet_params_res_.at(i).micro_block_count_;
                  estimate_res.at(idx).sstable_row_count_ = result.tablet_params_res_.at(i).sstable_row_count_;
                  estimate_res.at(idx).memtable_row_count_ = result.tablet_params_res_.at(i).memtable_row_count_;
                  if (OB_FAIL(estimate_res.at(idx).cg_macro_cnt_arr_.assign(result.tablet_params_res_.at(i).cg_macro_cnt_arr_))) {
                    LOG_WARN("failed to assign", K(ret));
                  } else if (OB_FAIL(estimate_res.at(idx).cg_micro_cnt_arr_.assign(result.tablet_params_res_.at(i).cg_micro_cnt_arr_))) {
                    LOG_WARN("failed to assign", K(ret));
                  }
                }
              }
              LOG_TRACE("succeed to estimate block count", K(selected_tablet_idx), K(partition_ids),
                                                  K(tablet_ids), K(arg), K(result), K(estimate_res));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::stroage_estimate_block_count_and_row_count(ObExecContext &ctx,
                                                                      const ObAddr &addr,
                                                                      const obrpc::ObEstBlockArg &arg,
                                                                      obrpc::ObEstBlockRes &result)
{
  int ret = OB_SUCCESS;
  if (addr == ctx.get_addr()) {
    if (OB_FAIL(ObStorageEstimator::estimate_block_count_and_row_count(arg, result))) {
      LOG_WARN("failed to estimate partition rows", K(ret));
    } else {
      LOG_TRACE("succeed to stroage estimate block count and row count", K(addr), K(arg), K(result));
    }
  } else {
    obrpc::ObSrvRpcProxy *rpc_proxy = NULL;
    const ObSQLSessionInfo *session_info = NULL;
    int64_t timeout = std::min(MAX_OPT_STATS_PROCESS_RPC_TIMEOUT, THIS_WORKER.get_timeout_remain());
    if (OB_ISNULL(session_info = ctx.get_my_session()) ||
        OB_ISNULL(rpc_proxy = GCTX.srv_rpc_proxy_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rpc_proxy or session is null", K(ret), K(rpc_proxy), K(session_info));
    } else if (0 >= timeout) {
      ret = OB_TIMEOUT;
      LOG_WARN("query timeout is reached", K(ret), K(timeout));
    } else if (OB_FAIL(rpc_proxy->to(addr)
                       .timeout(timeout)
                       .by(session_info->get_rpc_tenant_id())
                       .estimate_tablet_block_count(arg, result))) {
      LOG_WARN("failed to remote storage est failed", K(ret));
    } else {
      LOG_TRACE("succeed to stroage estimate block count", K(addr), K(arg), K(result));
    }
  }
  return ret;
}

int ObBasicStatsEstimator::get_tablet_locations(ObExecContext &ctx,
                                                const uint64_t ref_table_id,
                                                const ObIArray<ObTabletID> &tablet_ids,
                                                const ObIArray<ObObjectID> &partition_ids,
                                                ObCandiTabletLocIArray &candi_tablet_locs)
{
  int ret = OB_SUCCESS;
  ObDASLocationRouter &loc_router = ctx.get_das_ctx().get_location_router();
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session) || OB_UNLIKELY(tablet_ids.count() != partition_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session), K(tablet_ids.count()), K(partition_ids.count()));
  } else {
    candi_tablet_locs.reset();
    if (OB_FAIL(candi_tablet_locs.prepare_allocate(tablet_ids.count()))) {
      LOG_WARN("Partitoin location list prepare error", K(ret));
    } else {
      ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
      //This interface does not require the first_level_part_ids, so construct an empty array.
      ObSEArray<ObObjectID, 8> first_level_part_ids;
      ObDASTableLocMeta loc_meta(allocator);
      loc_meta.ref_table_id_ = ref_table_id;
      loc_meta.table_loc_id_ = ref_table_id;
      loc_meta.select_leader_ = 0;
      if (OB_FAIL(loc_router.nonblock_get_candi_tablet_locations(loc_meta,
                                                                 tablet_ids,
                                                                 partition_ids,
                                                                 first_level_part_ids,
                                                                 candi_tablet_locs))) {
        LOG_WARN("nonblock get candi tablet location failed", K(ret), K(loc_meta), K(partition_ids), K(tablet_ids));
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::estimate_modified_count(ObExecContext &ctx,
                                                   const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   int64_t &result,
                                                   const bool need_inc_modified_count/*default true*/)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  const int64_t obj_pos = 0;
  ObObj result_obj;
  bool is_valid = true;
  if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(tenant_id, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (need_inc_modified_count &&
             OB_FAIL(select_sql.append_fmt(
        "select cast(sum(inserts + updates + deletes) - sum(last_inserts + last_updates + " \
        "last_deletes) as signed) as inc_mod_count " \
        "from %s where tenant_id = %lu and table_id = %lu;",
        share::OB_ALL_MONITOR_MODIFIED_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (!need_inc_modified_count &&
             OB_FAIL(select_sql.append_fmt(
        "select cast(sum(inserts + updates + deletes) as signed) as modified_count " \
        "from %s where tenant_id = %lu and table_id = %lu;",
        share::OB_ALL_MONITOR_MODIFIED_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else if (OB_FAIL(client_result->next())) {
        LOG_WARN("failed to get next result", K(ret));
      } else if (OB_FAIL(client_result->get_obj(obj_pos, result_obj))) {
        LOG_WARN("failed to get object", K(ret));
      } else if (result_obj.is_null()) {
        result = 0;
      } else if (OB_UNLIKELY(!result_obj.is_integer_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(result_obj.get_type()));
      } else {
        result = result_obj.get_int();
        LOG_TRACE("succeed to get estimate modified count", K(table_id), K(result),
                                                            K(need_inc_modified_count));
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::estimate_row_count(ObExecContext &ctx,
                                              const uint64_t tenant_id,
                                              const uint64_t table_id,
                                              int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  row_cnt = 0;
  ObSqlString select_sql;
  bool is_valid = true;
  if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(tenant_id, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(select_sql.append_fmt(
        "select cast(sum(inserts) - sum(deletes) as signed) as row_cnt " \
        "from %s where tenant_id = %lu and table_id = %lu;",
        share::OB_ALL_MONITOR_MODIFIED_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      ObObj row_cnt_obj;
      const int64_t obj_pos = 0;
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else if (OB_FAIL(client_result->next())) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          row_cnt = 0;
        } else {
          LOG_WARN("failed to get next result", K(ret));
        }
      } else if (OB_FAIL(client_result->get_obj(obj_pos, row_cnt_obj))) {
        LOG_WARN("failed to get object", K(ret));
      } else if (row_cnt_obj.is_null()) {
        row_cnt = 0;
      } else if (OB_UNLIKELY(!row_cnt_obj.is_integer_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(row_cnt_obj.get_type()));
      } else {
        row_cnt = row_cnt_obj.get_int();
      }
      LOG_TRACE("succeed to get table row count", K(table_id), K(row_cnt));
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::get_gather_table_duration(ObExecContext &ctx,
                                                     const uint64_t tenant_id,
                                                     const uint64_t table_id,
                                                     int64_t &last_gather_duration)
{
  int ret = OB_SUCCESS;
  last_gather_duration = 0;
  ObSqlString select_sql;
  bool is_valid = true;
  if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(tenant_id, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(select_sql.append_fmt(
        "select cast((time_to_usec(end_time) - time_to_usec(start_time)) as signed) as last_gather_duration" \
        " from %s where tenant_id = %lu and table_id = %lu and ret_code = 0 order by start_time desc limit 1;",
        share::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      ObObj obj;
      const int64_t obj_pos = 0;
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, gen_meta_tenant_id(tenant_id), select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else if (OB_FAIL(client_result->next())) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          last_gather_duration = 0;
        } else {
          LOG_WARN("failed to get result");
        }
      } else if (OB_FAIL(client_result->get_obj(obj_pos, obj))) {
        LOG_WARN("failed to get object", K(ret));
      } else if (obj.is_null()) {
        last_gather_duration = 0;
      } else if (OB_UNLIKELY(!obj.is_integer_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(obj.get_type()));
      } else {
        last_gather_duration = obj.get_int();
      }
      LOG_TRACE("succeed to get last gather table duration", K(table_id), K(last_gather_duration));
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::estimate_stale_partition(ObExecContext &ctx,
                                                    const uint64_t tenant_id,
                                                    const uint64_t table_id,
                                                    const int64_t global_part_id,
                                                    const ObIArray<PartInfo> &partition_infos,
                                                    const double stale_percent_threshold,
                                                    ObIArray<ObPartitionStatInfo> &partition_stat_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  bool is_valid = true;
  ObSEArray<int64_t, 4> monitor_modified_part_ids;
  bool is_check_global = false;
  int64_t table_inc_modified = 0;
  bool has_part_invalid_inc = false;
  if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(tenant_id, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(select_sql.append_fmt(
          "select tablet_id, (inserts + updates + deletes - last_inserts - " \
          "last_updates - last_deletes) as inc_mod_count "\
          "from %s where tenant_id = %lu and table_id = %lu order by 1;",
        share::OB_ALL_MONITOR_MODIFIED_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        int64_t cur_part_id = -1; //current partition for first part
        int64_t cur_inc_mod_count = 0;//current inc_mod_count for first part
        bool has_subpart_invalid_inc = false;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx1 = 0;
          int64_t idx2 = 1;
          ObObj tablet_id_obj;
          ObObj inc_mod_count_obj;
          int64_t dst_tablet_id = 0;
          int64_t dst_partition = -1;
          int64_t inc_mod_count = 0;
          int64_t dst_part_id = -1;
          if (OB_FAIL(client_result->get_obj(idx1, tablet_id_obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tablet_id_obj.get_int(dst_tablet_id))) {
            LOG_WARN("failed to get int", K(ret), K(tablet_id_obj));
          } else if (OB_FAIL(client_result->get_obj(idx2, inc_mod_count_obj))) {
            LOG_WARN("failed to get object", K(ret), K(inc_mod_count_obj));
          } else if (!inc_mod_count_obj.is_null() &&
                     OB_FAIL(inc_mod_count_obj.get_int(inc_mod_count))) {
            LOG_WARN("failed to get int", K(ret), K(inc_mod_count_obj));
          } else if (OB_FAIL(ObDbmsStatsUtils::get_dst_partition_by_tablet_id(ctx,
                                                                              dst_tablet_id,
                                                                              partition_infos,
                                                                              dst_partition))) {
            LOG_WARN("failed to get dst partition by tablet id", K(ret));
          } else if (OB_FAIL(check_partition_stat_state(dst_partition,
                                                        inc_mod_count,
                                                        stale_percent_threshold,
                                                        partition_stat_infos))) {
            LOG_WARN("failed to check partition stat state", K(ret));
          } else if (OB_FAIL(monitor_modified_part_ids.push_back(dst_partition))) {
            LOG_WARN("failed to push back part ids occurred in monitor_modified", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(monitor_modified_part_ids, cur_part_id))) {
            LOG_WARN("failed to push back part ids occurred in monitor_modified", K(ret));
          } else if (ObDbmsStatsUtils::is_subpart_id(partition_infos, dst_partition, dst_part_id)) {
            has_subpart_invalid_inc |= inc_mod_count < 0;
            if (cur_part_id == dst_part_id) {
              cur_inc_mod_count += inc_mod_count;
            } else if (cur_part_id == -1) {
              cur_part_id = dst_part_id;
              cur_inc_mod_count = inc_mod_count;
            } else if (OB_FAIL(check_partition_stat_state(cur_part_id,
                                                          has_subpart_invalid_inc ? -1 : cur_inc_mod_count,
                                                          stale_percent_threshold,
                                                          partition_stat_infos))) {
              LOG_WARN("failed to check partition stat state", K(ret));
            } else {
              cur_part_id = dst_part_id;
              cur_inc_mod_count = inc_mod_count;
              has_subpart_invalid_inc = false;
            }
          }
          has_part_invalid_inc |= inc_mod_count < 0;
          is_check_global = true;
          table_inc_modified += inc_mod_count;
        }
        if (OB_FAIL(ret)) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get result", K(ret));
          } else {
            ret = OB_SUCCESS;
            if (cur_part_id != -1 &&
                OB_FAIL(check_partition_stat_state(cur_part_id,
                                                   has_subpart_invalid_inc ? -1 : cur_inc_mod_count,
                                                   stale_percent_threshold,
                                                   partition_stat_infos))) {
              LOG_WARN("failed to check partition stat state", K(ret));
            } else if (is_check_global &&
                       OB_FAIL(check_partition_stat_state(global_part_id,
                                                          has_part_invalid_inc ? -1 : table_inc_modified,
                                                          stale_percent_threshold,
                                                          partition_stat_infos))) {
              LOG_WARN("failed to check partition stat state", K(ret));
            } else {/*do nothing*/}
          }
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    ObSEArray<int64_t, 4> record_first_part_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
      int64_t partition_id = partition_infos.at(i).part_id_;
      int64_t first_part_id = partition_infos.at(i).first_part_id_;
      // Partitions who not have dml infos are no need to regather stats
      if (!is_contain(monitor_modified_part_ids, partition_id)) {
        ObPartitionStatInfo partition_stat_info(partition_id, 0, false, true);
        if (OB_FAIL(partition_stat_infos.push_back(partition_stat_info))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret) &&
          first_part_id != OB_INVALID_ID &&
          !is_contain(monitor_modified_part_ids, first_part_id) &&
          !is_contain(record_first_part_ids, first_part_id)) {
        ObPartitionStatInfo partition_stat_info(first_part_id, 0, false, true);
        if (OB_FAIL(partition_stat_infos.push_back(partition_stat_info)) ||
            OB_FAIL(record_first_part_ids.push_back(first_part_id))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  LOG_TRACE("succeed to estimate stale partition", K(stale_percent_threshold),
                                                   K(partition_stat_infos),
                                                   K(partition_infos),
                                                   K(monitor_modified_part_ids));
  return ret;
}

int ObBasicStatsEstimator::update_last_modified_count(ObExecContext &ctx,
                                                      const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(update_last_modified_count(trans.get_connection(), param))) {
    LOG_WARN("failed to update last modified count", K(ret));
  }
  //end gather trans
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObBasicStatsEstimator::update_last_modified_count(sqlclient::ObISQLConnection *conn,
                                                      const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString udpate_sql;
  ObSqlString tablet_list;
  int64_t affected_rows = 0;
  bool is_valid = true;
  bool is_all_update = false;
  //if this is virtual table real agent, we need update the real table id modifed count
  uint64_t table_id = share::is_oracle_mapping_real_virtual_table(param.table_id_) ?
                              share::get_real_table_mappings_tid(param.table_id_) : param.table_id_;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(conn));
  } else if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(param.tenant_id_, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(gen_tablet_list(param, tablet_list, is_all_update))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (tablet_list.empty() && !is_all_update) {
    /*do nothing*/
  } else if (OB_FAIL(udpate_sql.append_fmt(
        "update %s set last_inserts = inserts, last_updates = updates, last_deletes = deletes " \
        "where tenant_id = %lu and table_id = %lu %s %s;",
        share::OB_ALL_MONITOR_MODIFIED_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(param.tenant_id_, param.tenant_id_),
        share::schema::ObSchemaUtils::get_extract_schema_id(param.tenant_id_, table_id),
        !tablet_list.empty() ? "and tablet_id in" : " ",
        !tablet_list.empty() ? tablet_list.ptr() : " "))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(conn->execute_write(param.tenant_id_, udpate_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(udpate_sql));
  } else {
    LOG_TRACE("succeed to update last modified count", K(udpate_sql), K(affected_rows));
  }
  return ret;
}

int ObBasicStatsEstimator::check_table_statistics_state(ObExecContext &ctx,
                                                        const uint64_t tenant_id,
                                                        const uint64_t table_id,
                                                        const int64_t global_part_id,
                                                        bool &is_locked,
                                                        ObIArray<ObPartitionStatInfo> &partition_stat_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  bool is_valid = true;
  is_locked = false;
  if (OB_FAIL(ObDbmsStatsUtils::check_table_read_write_valid(tenant_id, is_valid))) {
    LOG_WARN("failed to check table read write valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(select_sql.append_fmt(
        "select partition_id, stattype_locked, row_cnt from %s where tenant_id = %lu and " \
        "table_id = %lu order by 1;",
        share::OB_ALL_TABLE_STAT_TNAME,
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && !is_locked && OB_SUCC(client_result->next())) {
          ObObj tmp;
          int64_t part_val = -1;
          int64_t lock_val = -1;
          int64_t row_cnt = 0;
          int64_t idx1 = 0;
          int64_t idx2 = 1;
          int64_t idx3 = 2;
          if (OB_FAIL(client_result->get_obj(idx1, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(part_val))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(client_result->get_obj(idx2, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(lock_val))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(client_result->get_obj(idx3, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (!tmp.is_null() && OB_FAIL(tmp.get_int(row_cnt))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (global_part_id == part_val && lock_val > 0) {
            is_locked = true;
          } else {
            ObPartitionStatInfo partition_stat_info(part_val, row_cnt, lock_val > 0, false);
            if (OB_FAIL(partition_stat_infos.push_back(partition_stat_info))) {
              LOG_WARN("failed to push back", K(ret));
            } else {/*do nothing*/}
          }
        }
        if (OB_FAIL(ret)) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get result", K(ret));
          } else {
           ret = OB_SUCCESS;
          }
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    LOG_TRACE("Succeed check table has any statistics", K(is_locked), K(partition_stat_infos));
  }
  return ret;
}

int ObBasicStatsEstimator::check_partition_stat_state(const int64_t partition_id,
                                                      const int64_t inc_mod_count,
                                                      const double stale_percent_threshold,
                                                      ObIArray<ObPartitionStatInfo> &partition_stat_infos)
{
  int ret = OB_SUCCESS;
  bool find_it = false;
  for (int64_t i = 0; !find_it && i < partition_stat_infos.count(); ++i) {
    if (partition_stat_infos.at(i).partition_id_ == partition_id) {
      //locked partition id or no arrived stale percent threshold no need regather stats.
      double stale_percent = 0.0;
      if (inc_mod_count < 0 || partition_stat_infos.at(i).row_cnt_ <= 0) {
        stale_percent = inc_mod_count == 0 ? 0.0 : 1.0;
      } else {
        stale_percent = 1.0 * inc_mod_count / partition_stat_infos.at(i).row_cnt_;
      }
      partition_stat_infos.at(i).is_no_stale_ = stale_percent <= stale_percent_threshold;
      find_it = true;
    }
  }
  if (!find_it) {
    ObPartitionStatInfo partition_stat_info(partition_id, 0, false, false);
    partition_stat_info.is_no_stale_ = false;
    ret = partition_stat_infos.push_back(partition_stat_info);
  }
  return ret;
}

int ObBasicStatsEstimator::gen_tablet_list(const ObTableStatParam &param,
                                           ObSqlString &tablet_list,
                                           bool &is_all_update)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> tablet_ids;
  is_all_update = false;
  if (param.global_stat_param_.need_modify_) {
    if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO ||
        !param.global_stat_param_.gather_approx_) {
      is_all_update = true;
    }
  }
  if (OB_SUCC(ret) && !is_all_update && param.part_stat_param_.need_modify_) {
    if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
        if (OB_FAIL(tablet_ids.push_back(param.part_infos_.at(i).tablet_id_.id()))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    } else if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < param.subpart_infos_.count(); ++j) {
          if (param.part_infos_.at(i).part_id_ == param.subpart_infos_.at(j).first_part_id_) {
            if (OB_FAIL(tablet_ids.push_back(param.subpart_infos_.at(j).tablet_id_.id()))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_all_update && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(tablet_ids.push_back(param.subpart_infos_.at(i).tablet_id_.id()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      char prefix = (i == 0 ? '(' : ' ');
      char suffix = (i == tablet_ids.count() - 1 ? ')' : ',');
      if (OB_FAIL(tablet_list.append_fmt("%c%lu%c", prefix, tablet_ids.at(i), suffix))) {
        LOG_WARN("failed to append sql", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObBasicStatsEstimator::get_all_tablet_id_and_object_id(const ObTableStatParam &param,
                                                           ObIArray<ObTabletID> &tablet_ids,
                                                           ObIArray<ObObjectID> &partition_ids)
{
  int ret = OB_SUCCESS;
  if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
    ObTabletID global_tablet_id(param.global_tablet_id_);
    if (OB_FAIL(tablet_ids.push_back(global_tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(partition_ids.push_back(static_cast<ObObjectID>(param.global_part_id_)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ONE) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.all_part_infos_.count(); ++i) {
      if (OB_FAIL(tablet_ids.push_back(param.all_part_infos_.at(i).tablet_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(partition_ids.push_back(static_cast<ObObjectID>(param.all_part_infos_.at(i).part_id_)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  } else if (param.part_level_ == share::schema::PARTITION_LEVEL_TWO) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.all_subpart_infos_.count(); ++i) {
      if (OB_FAIL(tablet_ids.push_back(param.all_subpart_infos_.at(i).tablet_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(partition_ids.push_back(static_cast<ObObjectID>(param.all_subpart_infos_.at(i).part_id_)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObBasicStatsEstimator::get_need_stats_tables(ObExecContext &ctx,
                                                 const int64_t tenant_id,
                                                 const int64_t last_table_id,
                                                 const int64_t slice_cnt,
                                                 ObIArray<int64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString gather_table_type_list;
  ObSqlString select_sql;
  if (OB_FAIL(get_gather_table_type_list(gather_table_type_list))) {
    LOG_WARN("failed to get gather table type list", K(ret));
  } else if (OB_FAIL(select_sql.append_fmt("SELECT /*+no_rewrite*/table_id "\
                                           "FROM   %s t "\
                                           "WHERE  table_id > %ld"
                                           "  AND  table_type IN %s"\
                                           "  AND  (table_type = %u "\
                                           "       OR EXISTS(SELECT 1 "\
                                           "                 FROM   %s m"\
                                           "                 WHERE  t.table_id = m.table_id"\
                                           "                        AND t.tenant_id = m.tenant_id"\
                                           "                        AND inserts + deletes + updates > 0"\
                                           "                 limit 1)) "
                                           "ORDER  BY tenant_id, table_id "\
                                           "LIMIT  %ld;",
                                           share::OB_ALL_TABLE_TNAME,
                                           last_table_id,
                                           gather_table_type_list.ptr(),
                                           share::schema::ObTableType::VIRTUAL_TABLE,
                                           share::OB_ALL_MONITOR_MODIFIED_TNAME,
                                           slice_cnt))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx = 0;
          ObObj obj;
          int64_t table_id = -1;
          if (OB_FAIL(client_result->get_obj(idx, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_int(table_id))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_FAIL(table_ids.push_back(table_id))) {
            LOG_WARN("failed to push back table id", K(ret));
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    LOG_TRACE("succeed to get table ids that need gathering table stats",
                K(select_sql), K(last_table_id), K(slice_cnt), K(table_ids.count()), K(table_ids));
  }
  return ret;
}

int ObBasicStatsEstimator::generate_first_part_idx_map(const ObIArray<PartInfo> &all_part_infos,
                                                       hash::ObHashMap<int64_t, int64_t> &first_part_idx_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(all_part_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(all_part_infos.empty()));
  } else if (OB_FAIL(first_part_idx_map.create(all_part_infos.count(), "ObStatsEst"))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_part_infos.count(); ++i) {
      if (OB_FAIL(first_part_idx_map.set_refactored(all_part_infos.at(i).part_id_, i))) {
        LOG_WARN("failed to set refactored", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

/**
 * @brief ObBasicStatsEstimator::refine_basic_stats
 *   when the user specify estimate_percent is too small, the sample data isn't enough to describe the
 * overall data distribution, So we need consider refine it, and reset the appropriate estimate_percent
 * to regather basic stats.
 */
int ObBasicStatsEstimator::refine_basic_stats(const ObOptStatGatherParam &param,
                                              ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (sample_value_ >= 0.000001 && sample_value_ < 100.0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_opt_stats.count(); ++i) {
      bool need_re_estimate = false;
      ObOptStatGatherParam new_param;
      ObSEArray<ObOptStat, 1> tmp_opt_stats;
      ObBasicStatsEstimator basic_re_est(ctx_, *param.allocator_);
      if (OB_FAIL(check_stat_need_re_estimate(param, dst_opt_stats.at(i), need_re_estimate, new_param))) {
        LOG_WARN("failed to check stat need re-estimate", K(ret));
      } else if (!need_re_estimate) {
        //do nothing
      } else if (OB_FAIL(tmp_opt_stats.push_back(dst_opt_stats.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(basic_re_est.estimate(new_param, tmp_opt_stats))) {
        LOG_WARN("failed to estimate basic statistics", K(ret));
      } else {
        LOG_TRACE("Suceed to re-estimate stats", K(new_param), K(param));
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::check_stat_need_re_estimate(const ObOptStatGatherParam &origin_param,
                                                       ObOptStat &opt_stat,
                                                       bool &need_re_estimate,
                                                       ObOptStatGatherParam &new_param)
{
  int ret = OB_SUCCESS;
  need_re_estimate = false;
  if (OB_ISNULL(opt_stat.table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(opt_stat.table_stat_));
  } else if (opt_stat.table_stat_->get_row_count() * sample_value_ / 100 >= MAGIC_MIN_SAMPLE_SIZE) {
    //do nothing
  } else if (OB_FAIL(new_param.assign(origin_param))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    need_re_estimate = true;
    int64_t total_row_count = opt_stat.table_stat_->get_row_count();
    //1.set sample ratio
    if (total_row_count <= MAGIC_SAMPLE_SIZE) {
      new_param.sample_info_.is_sample_ = false;
      new_param.sample_info_.sample_value_ = 0.0;
      new_param.sample_info_.is_block_sample_ = false;
    } else {
      new_param.sample_info_.is_sample_ = true;
      new_param.sample_info_.is_block_sample_ = false;
      new_param.sample_info_.sample_value_ = (MAGIC_SAMPLE_SIZE * 100.0) / total_row_count;
      new_param.sample_info_.sample_type_ = PercentSample;
    }
    //2.set partition info
    if (new_param.stat_level_ != TABLE_LEVEL) {
      if (OB_FAIL(ObDbmsStatsUtils::remove_stat_gather_param_partition_info(opt_stat.table_stat_->get_partition_id(),
                                                                            new_param))) {
        LOG_WARN("failed to remove stat gather param partition info", K(ret));
      }
    }
    //3.reset opt stat
    if (OB_SUCC(ret)) {
      opt_stat.table_stat_->set_row_count(0);
      opt_stat.table_stat_->set_avg_row_size(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_stat.column_stats_.count(); ++i) {
        if (OB_ISNULL(opt_stat.column_stats_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else {
          ObObj null_val;
          null_val.set_null();
          opt_stat.column_stats_.at(i)->set_max_value(null_val);
          opt_stat.column_stats_.at(i)->set_min_value(null_val);
          opt_stat.column_stats_.at(i)->set_num_not_null(0);
          opt_stat.column_stats_.at(i)->set_num_null(0);
          opt_stat.column_stats_.at(i)->set_num_distinct(0);
          opt_stat.column_stats_.at(i)->set_avg_len(0);
          opt_stat.column_stats_.at(i)->set_llc_bitmap_size(ObOptColumnStat::NUM_LLC_BUCKET);
          MEMSET(opt_stat.column_stats_.at(i)->get_llc_bitmap(), 0, ObOptColumnStat::NUM_LLC_BUCKET);
          opt_stat.column_stats_.at(i)->get_histogram().reset();
        }
      }
    }
  }
  return ret;
}

int ObBasicStatsEstimator::fill_hints(common::ObIAllocator &alloc,
                                      const ObString &table_name,
                                      int64_t gather_vectorize,
                                      bool use_column_store)
{
  int ret = OB_SUCCESS;
  ObSqlString default_hints;
  const char *use_col_tab_hint = lib::is_oracle_mode() ? " USE_COLUMN_TABLE(\"%.*s\")" : " USE_COLUMN_TABLE(`%.*s`) ";
  const char *use_full_table_hint = lib::is_oracle_mode() ? " FULL(\"%.*s\") " : " FULL(`%.*s`) ";
  if (OB_UNLIKELY(table_name.empty() || gather_vectorize < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_name), K(gather_vectorize));
  } else if (OB_FAIL(default_hints.append_fmt("NO_REWRITE USE_PLAN_CACHE(NONE) DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', %ld)",
                                               gather_vectorize))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(default_hints.append_fmt(use_full_table_hint,
                                              table_name.length(),
                                              table_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (use_column_store && OB_FAIL(default_hints.append_fmt(use_col_tab_hint,
                                                                  table_name.length(),
                                                                  table_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(add_hint(default_hints.string(), alloc))) {
    LOG_WARN("failed to add hint", K(ret));
  } else {
    LOG_TRACE("succeed to fill index info", K(default_hints));
  }
  return ret;
}

int ObBasicStatsEstimator::generate_column_group_ids(const ObTableStatParam &param,
                                                     ObIArray<uint64_t> &column_group_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.column_group_params_.count(); ++i) {
    if (OB_FAIL(column_group_ids.push_back(param.column_group_params_.at(i).column_group_id_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObBasicStatsEstimator::check_can_use_column_store_and_split_part_gather(const int64_t sstable_row_cnt,
                                                                            const int64_t memtable_row_cnt,
                                                                            const int64_t cg_cnt,
                                                                            const int64_t part_cnt,
                                                                            const int64_t degree,
                                                                            bool &use_column_store,
                                                                            bool &use_split_part)
{
  int ret = OB_SUCCESS;
  use_split_part = false;
  use_column_store = false;
  int64_t total_rowcnt = sstable_row_cnt + memtable_row_cnt;
  const int64_t SPLIT_PART_MINIMUM_ROW_COUNT = 1000000;
  const int64_t COST_SCHEDULER_GATHER_PER_PART = 10000;//10ms
  const int64_t COST_GATHER_MINIMUM_ROW_COUNT_PER_COLUMN = 100000;//100ms
  const double RATIO_OF_IMPROVEMENT = 0.3;
  if (cg_cnt > 0 && sstable_row_cnt > memtable_row_cnt * 10) {
    use_column_store = true;
    if (use_column_store && part_cnt > 1 && degree > 0 && total_rowcnt >= SPLIT_PART_MINIMUM_ROW_COUNT) {
      double cost_scheduler_split_part = 1.0 * COST_SCHEDULER_GATHER_PER_PART * part_cnt;
      double cost_of_no_split_gather_part = 1.0 * total_rowcnt / SPLIT_PART_MINIMUM_ROW_COUNT * (cg_cnt / 2 + 1) * COST_GATHER_MINIMUM_ROW_COUNT_PER_COLUMN / degree;
      double cost_of_improvement = cost_of_no_split_gather_part * RATIO_OF_IMPROVEMENT;
      use_split_part = cost_of_improvement > cost_scheduler_split_part;
    }
  }
  LOG_TRACE("check_can_use_column_store_and_split_part_gather", K(use_split_part), K(use_column_store),
                                                                K(cg_cnt), K(part_cnt), K(degree),
                                                                K(sstable_row_cnt), K(memtable_row_cnt));
  return ret;
}

int ObBasicStatsEstimator::get_gather_table_type_list(ObSqlString &gather_table_type_list)
{
  int ret = OB_SUCCESS;
  int64_t table_type_arr[] = {share::schema::ObTableType::SYSTEM_TABLE,
                              share::schema::ObTableType::VIRTUAL_TABLE,
                              share::schema::ObTableType::USER_TABLE,
                              share::schema::ObTableType::EXTERNAL_TABLE};
  int64_t table_type_cnt = sizeof(table_type_arr)/sizeof(table_type_arr[0]);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_type_cnt; ++i) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == table_type_cnt - 1 ? ')' : ',');
    if (OB_FAIL(gather_table_type_list.append_fmt("%c%lu%c", prefix, table_type_arr[i], suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

} // end of common
} // end of oceanbase
