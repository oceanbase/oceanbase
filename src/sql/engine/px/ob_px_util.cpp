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

#include "ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "share/schema/ob_part_mgr_util.h"
#include "storage/ob_locality_manager.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/dml/ob_table_merge_op.h"
#include "share/stat/ob_opt_stat_manager.h"
#ifdef OB_BUILD_CPP_ODPS
#include "sql/engine/table/ob_odps_table_row_iter.h"
#endif


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::share;

#define CASE_IGNORE_ERR_HELPER(ERR_CODE)                        \
case ERR_CODE: {                                                \
  should_ignore = true;                                         \
  LOG_USER_WARN(OB_IGNORE_ERR_ACCESS_VIRTUAL_TABLE, ERR_CODE);  \
  break;                                                        \
}                                                               \

OB_SERIALIZE_MEMBER(ObExprExtraSerializeInfo, *current_time_, *last_trace_id_, *mview_ids_, *last_refresh_scns_);

ObBaseOrderMap::~ObBaseOrderMap()
{
  int ret = OB_SUCCESS;
  ClearMapFunc clear_func;
  if (OB_FAIL(map_.foreach_refactored(clear_func))) {
    LOG_WARN("failed to clear");
  }
  map_.destroy();
  allocator_.reset();
}

int ObBaseOrderMap::init(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.create(count, ObModIds::OB_SQL_PX))) {
    SQL_LOG(WARN, "Failed to create hash table", K(count));
  }
  return ret;
}

int ObBaseOrderMap::add_base_partition_order(int64_t pwj_group_id,
                                             const TabletIdArray &tablet_id_array,
                                             const DASTabletLocIArray &dst_locations, bool asc)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTMArray<int64_t> *base_order = nullptr;
  if (OB_ISNULL(buf = reinterpret_cast<ObTMArray<int64_t> *>(
                    allocator_.alloc(sizeof(ObTMArray<int64_t>))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else if (FALSE_IT(base_order = new(buf) ObTMArray<int64_t>())) {
  } else if (OB_FAIL(base_order->reserve(dst_locations.count()))) {
    LOG_WARN("fail reserve base order", K(ret), K(dst_locations.count()));
  } else if (OB_FAIL(map_.set_refactored(pwj_group_id, std::make_pair(base_order, asc)))) {
    base_order->destroy();
    LOG_WARN("failed to set", K(pwj_group_id));
  } else {
    for (int i = 0; i < dst_locations.count() && OB_SUCC(ret); ++i) {
      for (int j = 0; j < tablet_id_array.count() && OB_SUCC(ret); ++j) {
        if (dst_locations.at(i)->tablet_id_.id() == tablet_id_array.at(j)) {
          if (OB_FAIL(base_order->push_back(j))) {
            LOG_WARN("fail to push idx into base order", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseOrderMap::reorder_partition_as_base_order(int64_t pwj_group_id,
                                                    const TabletIdArray &tablet_id_array,
                                                    DASTabletLocIArray &dst_locations)
{
  int ret = OB_SUCCESS;
  std::pair<ObIArray<int64_t> *, bool> base_order;
  ObIArray<int64_t> *base_order_arr = nullptr;
  if (OB_FAIL(map_.get_refactored(pwj_group_id, base_order))) {
    LOG_WARN("hash not found", K(pwj_group_id));
  } else if (FALSE_IT(base_order_arr = base_order.first)) {
  } else {
    int index = 0;
    for (int i = 0; i < base_order_arr->count() && OB_SUCC(ret); ++i) {
      for (int j = 0; j < dst_locations.count() && OB_SUCC(ret); ++j) {
        if (dst_locations.at(j)->tablet_id_.id() == tablet_id_array.at(base_order_arr->at(i))) {
          std::swap(dst_locations.at(j), dst_locations.at(index++));
          break;
        }
      }
    }
  }
  return ret;
}

// 物理分布策略：对于叶子节点，dfo 分布一般直接按照数据分布来
// Note：如果 dfo 中有两个及以上的 scan，仅仅考虑第一个。并且，要求其余 scan
//       的副本分布和第一个 scan 完全一致，否则报错。
int ObPXServerAddrUtil::alloc_by_data_distribution(const ObIArray<ObTableLocation> *table_locations,
                                                   ObExecContext &ctx,
                                                   ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  if (nullptr != dfo.get_root_op_spec()) {
    if (OB_FAIL(alloc_by_data_distribution_inner(table_locations, ctx, dfo))) {
      LOG_WARN("failed to alloc data distribution", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  return ret;
}


int ObPXServerAddrUtil::build_dynamic_partition_table_location(common::ObIArray<const ObTableScanSpec *> &scan_ops,
      const ObIArray<ObTableLocation> *table_locations, ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  uint64_t table_location_key = OB_INVALID_INDEX;
  for (int i = 0; i < scan_ops.count() && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(scan_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan ops is null", K(ret));
    } else {
      table_location_key = scan_ops.at(i)->get_table_loc_id();
      for (int j = 0; j < table_locations->count() && OB_SUCC(ret); ++j) {
        if (table_location_key == table_locations->at(j).get_table_id()) {
          ObIArray<ObPxSqcMeta> &sqcs = dfo.get_sqcs();
          for (int k = 0; k < sqcs.count() && OB_SUCC(ret); ++k) {
            if (OB_FAIL(sqcs.at(k).get_pruning_table_locations().push_back(table_locations->at(j)))) {
              LOG_WARN("fail to push back pruning table locations", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::sort_and_collect_local_file_distribution(
    ObIArray<ObExternalFileInfo> &files,
    ObIArray<ObAddr> &dst_addrs)
{
  int ret = OB_SUCCESS;
  ObAddr pre_addr;
  if (OB_SUCC(ret)) {
    auto addrcmp = [](const ObExternalFileInfo &l, const ObExternalFileInfo &r) -> bool {
      return l.file_addr_ < r.file_addr_;
    };
    lib::ob_sort(files.get_data(), files.get_data() + files.count(), addrcmp);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); i++) {
    ObAddr &cur_addr = files.at(i).file_addr_;
    if (cur_addr != pre_addr) {
      pre_addr = cur_addr;
      OZ (dst_addrs.push_back(files.at(i).file_addr_));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_external_table_loc(
    ObExecContext &ctx,
    uint64_t table_id,
    uint64_t ref_table_id,
    const ObQueryRangeProvider &pre_query_range,
    ObDfo &dfo,
    ObDASTableLoc *&table_loc)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *local_loc = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  bool is_external_files_on_disk = false;
  ObIArray<ObExternalFileInfo> &ext_file_urls = dfo.get_external_table_files();
  ObSEArray<ObAddr, 16> all_locations;
  ObQueryRangeArray ranges;
  if (OB_ISNULL(local_loc = DAS_CTX(ctx).get_table_loc_by_id(table_id, ref_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table loc", K(ret), K(table_id), K(ref_table_id));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null location service", K(ret));
  } else {
    tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    is_external_files_on_disk = local_loc->loc_meta_->is_external_files_on_disk_;
    if (OB_FAIL(GCTX.location_service_->external_table_get(tenant_id, all_locations))) {
      LOG_WARN("fail to get external table location", K(ret));
    }
  }

  if (OB_SUCC(ret) && ext_file_urls.empty()) {
    // TODO EXTARNAL TABLE
    // if (pre_query_range.has_exec_param() || 0 == pre_query_range.get_column_count()) {
    //   ret = OB_NOT_SUPPORTED;
    //   LOG_WARN("Has dynamic params in external table or empty range is not supported", K(ret),
    //            K(pre_query_range.has_exec_param()), K(pre_query_range.get_column_count()));
    ObSEArray<int64_t, 16> part_ids;
    for (DASTabletLocListIter iter = local_loc->tablet_locs_begin(); OB_SUCC(ret)
               && iter != local_loc->tablet_locs_end(); ++iter) {
      if ((*iter) != nullptr) {
        ret = part_ids.push_back((*iter)->partition_id_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("uninited table loc", K(ret));
      }
    }

    OZ (ObSQLUtils::extract_pre_query_range(pre_query_range, ctx.get_allocator(), ctx, ranges,
                                    ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_external_object_id(ref_table_id)) {
      ObSEArray<const ObTableScanSpec *, 2> scan_ops;
      const ObOpSpec *root_op = NULL;
      dfo.get_root(root_op);
      if (OB_ISNULL(root_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
        LOG_WARN("failed to find scan_ops", K(ret), KP(root_op));
      } else if (scan_ops.count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty scan_ops", K(ret));
      } else if (OB_FAIL(ObExternalTableFileManager::get_instance().get_mocked_external_table_files(
                                                            tenant_id, part_ids, ctx,
                                                            scan_ops.at(0)->tsc_ctdef_.scan_ctdef_,
                                                            ext_file_urls))) {
        LOG_WARN("fail to get mocked external table files", K(ret));
      }
    } else {
      OZ (ObExternalTableFileManager::get_instance().get_external_files_by_part_ids(tenant_id,
                                                                                    ref_table_id,
                                                                                    part_ids,
                                                                                    is_external_files_on_disk,
                                                                                    ctx.get_allocator(),
                                                                                    ext_file_urls,
                                                                                    ranges.empty() ? NULL : &ranges));
    }

    if (is_external_files_on_disk) {
      OZ (ObExternalTableUtils::filter_files_in_locations(ext_file_urls, all_locations));
    }

    if (OB_SUCC(ret) && ext_file_urls.empty()) {
      ObExternalFileInfo dummy_file;
      dummy_file.file_url_ = ObExternalTableUtils::dummy_file_name();
      dummy_file.file_id_ = INT64_MAX;
      dummy_file.part_id_ = ref_table_id;
      if (is_external_files_on_disk) {
        dummy_file.file_addr_ = GCTX.self_addr();
      }
      OZ (dfo.get_external_table_files().push_back(dummy_file));
    }
  }

  if (OB_SUCC(ret)
      && NULL == (table_loc = DAS_CTX(ctx).get_external_table_loc_by_id(table_id, ref_table_id))) {
    //generate locations
    ObSEArray<ObAddr, 16> target_locations;
    if (is_external_files_on_disk) {
      // locations are the collection of file's ip
      if (OB_FAIL(sort_and_collect_local_file_distribution(dfo.get_external_table_files(),
                                                           target_locations))) {
        LOG_WARN("fail to collect local file distribution", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < target_locations.count(); ++i) {
        if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(
                          target_locations.at(i), ctx.get_my_session()->get_process_query_time()))) {
          ret = OB_SERVER_NOT_ALIVE;
          LOG_WARN("observer is not alive", K(target_locations.at(i)));
        }
      }
    } else {
      // bool is_odps_external_table = false;
      ObExternalFileFormat::FormatType external_table_type = ObExternalFileFormat::INVALID_FORMAT;
      ObSEArray<const ObTableScanSpec *, 2> scan_ops;
      const ObTableScanSpec *scan_op = nullptr;
      const ObOpSpec *root_op = NULL;
      ObString external_table_format_str;
      dfo.get_root(root_op);
      if (OB_ISNULL(root_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
        LOG_WARN("failed to find scan_ops", K(ret), KP(root_op));
      } else if (scan_ops.count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty scan_ops", K(ret));
      } else if (FALSE_IT(external_table_format_str = scan_ops.at(0)->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_)) {
      } else if (OB_FAIL(ObSQLUtils::get_external_table_type(external_table_format_str, external_table_type))) {
        LOG_WARN("failed to get external table type", K(ret), K(external_table_format_str));
      } else if (ObExternalFileFormat::ODPS_FORMAT == external_table_type ||
                 ObExternalFileFormat::PLUGIN_FORMAT == external_table_type) {
        int64_t expected_location_cnt = std::min(dfo.get_dop(), all_locations.count());
        if (1 == expected_location_cnt) {
          if (OB_FAIL(target_locations.push_back(GCTX.self_addr()))) {
            LOG_WARN("fail to push push back", K(ret));
          }
        } else if (expected_location_cnt >= all_locations.count() ?
                     OB_FAIL(target_locations.assign(all_locations)) :
                     OB_FAIL(ObPXServerAddrUtil::do_random_dfo_distribution(
                       all_locations, expected_location_cnt, target_locations))) {
          LOG_WARN("fail to calc random dfo distribution", K(ret), K(all_locations),
                   K(expected_location_cnt));
        }
      } else {
        if (OB_FAIL(ObExternalTableUtils::select_external_table_loc_by_load_balancer(
              ext_file_urls, all_locations, target_locations))) {
          LOG_WARN("failed to select external table location", K(ret));
        }
      }
    }
    LOG_TRACE("calc external table location", K(target_locations));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(DAS_CTX(ctx).build_external_table_location(table_id, ref_table_id, target_locations))) {
        LOG_WARN("fail to build external table locations", K(ret));
      } else if (OB_ISNULL(table_loc = DAS_CTX(ctx).get_external_table_loc_by_id(table_id, ref_table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected location", K(ret));
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::assign_external_files_to_sqc(
    ObDfo &dfo,
    ObExecContext &exec_ctx,
    bool is_file_on_disk,
    ObIArray<ObPxSqcMeta> &sqcs,
    int64_t parallel)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::ObExternalFileInfo> &files = dfo.get_external_table_files();
  if (is_file_on_disk) {
    ObAddr pre_addr;
    ObPxSqcMeta *target_sqc = NULL;
    for (int i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      if (pre_addr != files.at(i).file_addr_) {
        // TODO [External Table] OPT this
        target_sqc = NULL;
        for (int j = 0; j < sqcs.count(); j++) {
          if (sqcs.at(j).get_exec_addr() == files.at(i).file_addr_) {
            target_sqc = &sqcs.at(j);
            break;
          }
        }
        pre_addr = files.at(i).file_addr_;
      }
      if (OB_ISNULL(target_sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find sqc", K(files.at(i).file_addr_));
      } else if (OB_FAIL(target_sqc->get_access_external_table_files().push_back(files.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  } else {
    // bool is_odps_external_table = false;
    ObExternalFileFormat::FormatType external_table_type = ObExternalFileFormat::INVALID_FORMAT;
    bool is_odps_external_table = false;
    ObODPSGeneralFormat::ApiMode odps_api_mode;
    ObSEArray<const ObTableScanSpec *, 2> scan_ops;
    const ObTableScanSpec *scan_op = nullptr;
    const ObOpSpec *root_op = NULL;
    ObString external_table_format_str;
    dfo.get_root(root_op);
    if (OB_ISNULL(root_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
      LOG_WARN("failed to find scan_ops", K(ret), KP(root_op));
    } else if (scan_ops.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty scan_ops", K(ret));
    } else if (FALSE_IT(external_table_format_str = scan_ops.at(0)->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_)) {
    } else if (OB_FAIL(ObSQLUtils::get_external_table_type(external_table_format_str, external_table_type))) {
      LOG_WARN("failed to get external table type", K(ret), K(external_table_format_str));
    } else if (FALSE_IT(is_odps_external_table = (ObExternalFileFormat::ODPS_FORMAT == external_table_type))) {
    } else if (OB_FAIL(ObSQLUtils::get_odps_api_mode(scan_ops.at(0)->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_,
                                                     is_odps_external_table,
                                                     odps_api_mode))) {
      LOG_WARN("failed to check is odps external table or not", K(ret));
    } else if (is_odps_external_table) {
      if (OB_FAIL(ObExternalTableUtils::assign_odps_file_to_sqcs(
              dfo, exec_ctx, sqcs, parallel, odps_api_mode))) {
        LOG_WARN("failed to assisn odps file to sqcs", K(files), K(ret));
      }
    } else if (ObExternalFileFormat::PLUGIN_FORMAT == external_table_type) {
      if (OB_FAIL(ObExternalTableUtils::plugin_split_tasks(
              exec_ctx.get_allocator(), external_table_format_str, dfo, sqcs, parallel))) {
        LOG_WARN("failed to do plugin_split_tasks", K(ret));
      }
    } else {
      ObArray<int64_t> file_assigned_sqc_ids;
      OZ(ObExternalTableUtils::assigned_files_to_sqcs_by_load_balancer(files, sqcs,
                                                                       file_assigned_sqc_ids));
      if (OB_SUCC(ret) && file_assigned_sqc_ids.count() != files.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid result of assigned sqc", K(file_assigned_sqc_ids.count()), K(files.count()));
      }
      for (int i = 0; OB_SUCC(ret) && i < file_assigned_sqc_ids.count(); i++) {
        int64_t assign_sqc_idx = file_assigned_sqc_ids.at(i);
        if (OB_UNLIKELY(assign_sqc_idx >= sqcs.count() || assign_sqc_idx < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected file idx", K(file_assigned_sqc_ids.at(i)));
        } else {
          OZ (sqcs.at(assign_sqc_idx).get_access_external_table_files().push_back(files.at(i)));
        }
      }
    }
  }
  LOG_TRACE("check dfo external files", K(files));
  for (int64_t i = 0; i < sqcs.count(); ++i) {
    LOG_TRACE("check sqc external files", K(sqcs.at(i).get_access_external_table_files()));
  }
  return ret;
}

int ObPXServerAddrUtil::assign_lake_table_files_to_sqc(const DASTabletLocList &locations,
                                                       ObExecContext &exec_ctx,
                                                       ObIArray<ObPxSqcMeta> &sqcs)
{
  int ret = OB_SUCCESS;
  ObLakeTableFileMap *lake_table_file_map = nullptr;
  if (OB_FAIL(exec_ctx.get_lake_table_file_map(lake_table_file_map))) {
    LOG_WARN("failed to get lake table file map");
  } else if (OB_ISNULL(lake_table_file_map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < sqcs.count(); i++) {
      ObAddr addr = sqcs.at(i).get_exec_addr();
      ObLakeTableFileDesc &lake_table_file_desc = sqcs.at(i).get_lake_table_file_desc();
      for (ConstDASTabletLocListIter iter = locations.begin(); OB_SUCC(ret) && iter != locations.end(); ++iter) {
        if (addr == (*iter)->server_) {
          ObLakeTableFileMapKey key;
          key.table_loc_id_ = (*iter)->loc_meta_->table_loc_id_;
          key.tablet_id_ = (*iter)->tablet_id_;
          ObLakeTableFileArray *lake_table_file_array = nullptr;
          if (OB_FAIL(lake_table_file_map->get_refactored(key, lake_table_file_array))) {
            LOG_WARN("failed to get lake table file array", K(key));
          } else if (OB_FAIL(lake_table_file_desc.add_lake_table_file_desc(key, lake_table_file_array))) {
            LOG_WARN("failed to add lake table file desc");
          }
        }
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_data_distribution_inner(
    const ObIArray<ObTableLocation> *table_locations,
    ObExecContext &ctx,
    ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScanSpec *, 2> scan_ops;
  ObSEArray<const ObTableModifySpec *, 1> dml_ops;
  // INSERT, REPLACE算子
  const ObTableModifySpec *dml_op = nullptr;
  const ObTableScanSpec *scan_op = nullptr;
  const ObOpSpec *root_op = NULL;
  dfo.get_root(root_op);
  // PDML的逻辑中将会被去除
  if (OB_ISNULL(root_op) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(dfo));
  } else if (0 != dfo.get_sqcs_count()) {
    /**
     * this dfo has been built. do nothing.
     */
    LOG_TRACE("this dfo has been built", K(dfo.get_dfo_id()));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("fail find scan ops in dfo", K(dfo), K(ret));
  } else if (OB_FAIL(ObPXServerAddrUtil::find_dml_ops(dml_ops, *root_op))) {
    LOG_WARN("failed find insert op in dfo", K(ret), K(dfo));
  } else if (1 < dml_ops.count()) { // 目前一个dfo中最多只有一个dml算子
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of insert ops is not right", K(ret), K(dml_ops.count()));
  } else if (0 == scan_ops.count() && 0 == dml_ops.count()) {
    /**
     * some dfo may not contain tsc and dml. for example, select 8 from union all select t1.c1 from t1.
     */
    if (OB_FAIL(alloc_by_local_distribution(ctx, dfo))) {
      LOG_WARN("alloc SQC on local failed", K(ret));
    }
  } else {
    ObDASTableLoc *table_loc = NULL;
    ObDASTableLoc *dml_full_loc = NULL;
    uint64_t table_location_key = OB_INVALID_INDEX;
    uint64_t ref_table_id = OB_INVALID_ID;
    // Prioritize using DML operators to build SQC.
    if (dml_ops.count() > 0) {
      dml_op = dml_ops.at(0);
      if (OB_FAIL(dml_op->get_single_table_loc_id(table_location_key, ref_table_id))) {
        LOG_WARN("get single table loc id failed", K(ret));
      }
    } else if (scan_ops.count() > 0) {
      scan_op = scan_ops.at(0);
      if (OB_ISNULL(scan_op)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan op is null in scan ops", K(ret));
      } else {
        table_location_key = scan_op->get_table_loc_id();
        ref_table_id = scan_op->get_loc_ref_table_id();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (dml_op && dml_op->is_table_location_uncertain()) {
      OZ(ObTableLocation::get_full_leader_table_loc(DAS_CTX(ctx).get_location_router(),
                                                    ctx.get_allocator(),
                                                    ctx.get_my_session()->get_effective_tenant_id(),
                                                    table_location_key,
                                                    ref_table_id,
                                                    table_loc));
      if (OB_SUCC(ret)) {
        dml_full_loc = table_loc;
      }
    } else {
      // 通过TSC或者DML获得当前的DFO的partition对应的location信息
      // 后续利用location信息构建对应的SQC meta
      if (OB_ISNULL(scan_op) || !scan_op->is_ob_external_table()) {
        // scan_ops is empty
        if (OB_ISNULL(table_loc = DAS_CTX(ctx).get_table_loc_by_id(table_location_key, ref_table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get table loc", K(ret), K(table_location_key), K(ref_table_id), K(DAS_CTX(ctx).get_table_loc_list()));
        }
      } else {
        // create new table loc for a random dfo distribution for external table
        OZ (get_external_table_loc(ctx, table_location_key, ref_table_id, scan_op->get_query_range_provider(), dfo, table_loc));
      }
    }

    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get phy table location", K(ret));
    } else {
      const DASTabletLocList &locations = table_loc->get_tablet_locs();
      if (locations.size() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the location array is empty", K(locations.size()), K(ret));
      } else if (OB_FAIL(build_dfo_sqc(ctx, locations, dfo, scan_ops))) {
        LOG_WARN("fail fill dfo with sqc infos", K(dfo), K(ret));
      } else if (OB_FAIL(set_dfo_accessed_location(ctx, table_location_key, dfo, scan_ops, dml_op, dml_full_loc))) {
        LOG_WARN("fail to set all table partition for tsc", K(ret), K(scan_ops.count()), K(dml_op),
                 K(table_location_key), K(ref_table_id), K(locations));
      } else if (OB_NOT_NULL(table_locations) && !table_locations->empty() &&
            OB_FAIL(build_dynamic_partition_table_location(scan_ops, table_locations, dfo))) {
        LOG_WARN("fail to build dynamic partition pruning table", K(ret));
      } else if (NULL != dml_op && OB_FAIL(add_pdml_merge_gindex_locations(*dml_op, ctx, dfo))) {
        LOG_WARN("add pdml merge global index locations failed", K(ret));
      }
      LOG_TRACE("allocate sqc by data distribution", K(dfo), K(locations));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_NOT_ADD_PDML_MERGE_GINDEX_LOCATION);
int ObPXServerAddrUtil::add_pdml_merge_gindex_locations(const ObTableModifySpec &dml_op,
                                                        ObExecContext &ctx,
                                                        ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  if (dml_op.get_type() != PHY_MERGE) {
    // do nothing
  } else if (ERRSIM_NOT_ADD_PDML_MERGE_GINDEX_LOCATION) {
    // do nothing
  } else {
    ObSEArray<const ObDMLBaseCtDef *, 8> dml_ctdefs;
    if (OB_FAIL(static_cast<const ObTableMergeSpec &>(dml_op).get_global_index_ctdefs(dml_ctdefs))) {
      LOG_WARN("get global index ctdefs failed", K(ret));
    } else {
      for (int64_t i = 0; i < dml_ctdefs.count() && OB_SUCC(ret); i++) {
        uint64_t idx_table_location_key = dml_ctdefs.at(i)->das_base_ctdef_.table_id_;
        uint64_t idx_ref_table_id = dml_ctdefs.at(i)->das_base_ctdef_.index_tid_;
        ObDASTableLoc *idx_table_loc = NULL;
        if (OB_FAIL(ObTableLocation::get_full_leader_table_loc(DAS_CTX(ctx).get_location_router(),
                                              ctx.get_allocator(),
                                              ctx.get_my_session()->get_effective_tenant_id(),
                                              idx_table_location_key,
                                              idx_ref_table_id,
                                              idx_table_loc))) {
          LOG_WARN("get full leader table location failed", K(ret), K(idx_table_location_key), K(idx_ref_table_id));
        } else {
          ObIArray<ObPxSqcMeta> &sqcs = dfo.get_sqcs();
          for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); i++) {
            DASTabletLocIArray &sqc_locations = sqcs.at(i).get_extra_access_table_locations_for_update();
            for (DASTabletLocListIter iter = idx_table_loc->tablet_locs_begin();
                 iter != idx_table_loc->tablet_locs_end() && OB_SUCC(ret); ++iter) {
              OZ (sqc_locations.push_back(*iter));
            }
          }
          LOG_TRACE("add sqc extra access table locations", K(idx_table_location_key),
                    K(idx_ref_table_id), KPC(idx_table_loc));
        }
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::find_dml_ops(common::ObIArray<const ObTableModifySpec *> &insert_ops,
                             const ObOpSpec &op)
{
  return find_dml_ops_inner(insert_ops, op);
}

bool ObPXServerAddrUtil::check_build_dfo_with_dml(const ObOpSpec &op)
{
  bool b_ret = false;
  if (static_cast<const ObTableModifySpec &>(op).use_dist_das() && PHY_MERGE != op.get_type()
      && PHY_INSERT_ON_DUP != op.get_type()) {
    // px no need schedule das except merge
  } else if (PHY_LOCK == op.get_type()) {
    // no need lock op
  } else {
    b_ret = true;
  }
  return b_ret;
}

int ObPXServerAddrUtil::find_dml_ops_inner(common::ObIArray<const ObTableModifySpec *> &insert_ops,
                             const ObOpSpec &op)
{
  int ret = OB_SUCCESS;
  if (IS_DML(op.get_type())) {
    if (!check_build_dfo_with_dml(op)) {
    } else if (OB_FAIL(insert_ops.push_back(static_cast<const ObTableModifySpec *>(&op)))) {
      LOG_WARN("fail to push back table insert op", K(ret));
    }
  }
  if (OB_SUCC(ret) && !IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ObOpSpec *child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(find_dml_ops(insert_ops, *child_op))) {
        LOG_WARN("fail to find child insert ops",
                 K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  return ret;
}


int ObPXServerAddrUtil::generate_dh_map_info(ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  ObP2PDhMapInfo &p2p_map_info = dfo.get_p2p_dh_map_info();
  if (OB_ISNULL(dfo.get_coord_info_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected coord info ptr", K(ret));
  } else if (!dfo.get_p2p_dh_ids().empty() && p2p_map_info.is_empty()) {
    ObP2PDfoMapNode node;
    for (int i = 0; i < dfo.get_p2p_dh_ids().count() && OB_SUCC(ret); ++i) {
      node.reset();
      if (OB_FAIL(dfo.get_coord_info_ptr()->p2p_dfo_map_.get_refactored(
          dfo.get_p2p_dh_ids().at(i), node))) {
        LOG_WARN("fail to get target dfo id", K(ret));
      } else if (node.addrs_.empty()) {
        ObDfo *target_dfo_ptr = nullptr;
        if (OB_FAIL(dfo.get_coord_info_ptr()->dfo_mgr_.find_dfo_edge(
            node.target_dfo_id_, target_dfo_ptr))) {
          LOG_WARN("fail to find dfo edge", K(ret));
        } else if (target_dfo_ptr->get_p2p_dh_addrs().empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get p2p dh addrs", K(ret), K(dfo.get_p2p_dh_ids().at(i)));
        } else if (OB_FAIL(node.addrs_.assign(target_dfo_ptr->get_p2p_dh_addrs()))) {
          LOG_WARN("fail to assign p2p dh addrs", K(ret));
        } else if (OB_FAIL(dfo.get_coord_info_ptr()->p2p_dfo_map_.set_refactored(
              dfo.get_p2p_dh_ids().at(i), node, 1/*over_write*/))) {
          LOG_WARN("fail to set p2p dh addrs", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(p2p_map_info.p2p_sequence_ids_.push_back(dfo.get_p2p_dh_ids().at(i)))) {
          LOG_WARN("fail to push back p2p map info", K(ret));
        } else if (OB_FAIL(p2p_map_info.target_addrs_.push_back(node.addrs_))) {
          LOG_WARN("fail to push back addrs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::build_dfo_sqc(ObExecContext &ctx,
                                      const DASTabletLocList &locations,
                                      ObDfo &dfo,
                                      const ObIArray<const ObTableScanSpec *> &scan_ops)
{
  int ret = OB_SUCCESS;
  ObTMArray<ObAddr> addrs;
  ObTMArray<int64_t> sqc_max_task_count;
  ObTMArray<double> sqc_part_count;
  ObTMArray<double> sqc_row_count;
  ObTMArray<ObPxTabletInfo> px_tablets_info;
  bool is_opt_stat_valid = false;
  //for pkey, we send tablets row info to sqc to help decide the affinity rule
  bool send_px_tablets_info_to_sqc = false;
  int64_t parallel = dfo.get_assigned_worker_count();
  if (0 >= parallel) {
    parallel = 1;
    LOG_TRACE("parallel not set in query hint. set default to 1");
  }
  if (OB_FAIL(get_location_addrs<DASTabletLocList>(locations, addrs))) {
    LOG_WARN("fail get location addrs", K(ret));
  } else if (FALSE_IT(lib::ob_sort(addrs.begin(), addrs.end()))) {
  } else if (OB_FAIL(sqc_part_count.prepare_allocate(addrs.count()))) {
    LOG_WARN("fail to pre allocate sqc part count");
  } else if (OB_FAIL(sqc_row_count.prepare_allocate(addrs.count()))) {
    LOG_WARN("fail to pre allocate sqc row count");
  } else if (OB_FAIL(generate_dh_map_info(dfo))) {
    LOG_WARN("fail to generate dh map info", K(ret));
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_statistic_of_dfo_tables(
                 parallel, ctx, dfo, addrs, scan_ops, px_tablets_info, is_opt_stat_valid,
                 send_px_tablets_info_to_sqc))) {
    LOG_WARN("failed to get statistic of dfo tables");
  }

  if (OB_SUCC(ret) && addrs.count() > 0) {
    ObIArray<ObPxSqcMeta> &sqcs = dfo.get_sqcs();
    int64_t total_part_cnt = locations.size();
    int64_t total_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); ++i) {
      SMART_VAR(ObPxSqcMeta, sqc) {
        sqc.set_dfo_id(dfo.get_dfo_id());
        sqc.set_sqc_id(i);
        sqc.set_exec_addr(addrs.at(i));
        sqc.set_qc_addr(GCTX.self_addr());
        sqc.set_execution_id(dfo.get_execution_id());
        sqc.set_px_sequence_id(dfo.get_px_sequence_id());
        sqc.set_qc_id(dfo.get_qc_id());
        sqc.set_interrupt_id(dfo.get_interrupt_id());
        sqc.set_px_detectable_ids(dfo.get_px_detectable_ids());
        sqc.set_fulltree(dfo.is_fulltree());
        sqc.set_qc_server_id(dfo.get_qc_server_id());
        sqc.set_parent_dfo_id(dfo.get_parent_dfo_id());
        sqc.set_single_tsc_leaf_dfo(dfo.is_single_tsc_leaf_dfo());
        sqc.get_monitoring_info().init(dfo);
        sqc.set_partition_random_affinitize(dfo.partition_random_affinitize());
        if (OB_SUCC(ret)) {
          if (!dfo.get_p2p_dh_map_info().is_empty()) {
            if (OB_FAIL(sqc.get_p2p_dh_map_info().assign(dfo.get_p2p_dh_map_info()))) {
              LOG_WARN("fail to assign p2p dh map info", K(ret));
            }
          }
        }
        if (is_opt_stat_valid) {
          for (int64_t k = 0; k < px_tablets_info.count() && OB_SUCC(ret); ++k) {
            const ObPxTabletInfo &table_info = px_tablets_info.at(k);
            if (addrs.at(i) == table_info.tablet_loc_->server_) {
              sqc_row_count.at(i) += table_info.estimated_row_count_;
              if (!send_px_tablets_info_to_sqc) {
              } else if (OB_FAIL(sqc.get_px_tablets_info().push_back(table_info))) {
                LOG_WARN("failed to push back");
              }
            }
          }
          total_row_cnt += sqc_row_count.at(i);
        } else {
          for (DASTabletLocList::const_iterator iter = locations.begin();
               OB_SUCC(ret) && iter != locations.end(); ++iter) {
            if (addrs.at(i) == (*iter)->server_) {
              sqc_part_count.at(i)++;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(dfo.add_sqc(sqc))) {
            LOG_WARN("Failed to add sqc", K(ret), K(sqc));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_parallel_into_task(parallel,
                                           is_opt_stat_valid ? sqc_row_count : sqc_part_count,
                                           sqc_max_task_count))) {
        LOG_WARN("Failed to get sqc max task count", K(ret), K(is_opt_stat_valid), K(sqc_row_count),
                 K(sqc_part_count));
      } else if (OB_UNLIKELY(sqcs.count() != sqc_max_task_count.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected sqcs count and sqc max task count", K(ret), K(sqcs.count()), K(sqc_max_task_count.count()));
      }
    }
    int64_t total_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i).set_min_task_count(1);
      sqcs.at(i).set_max_task_count(sqc_max_task_count.at(i));
      total_task_count += sqc_max_task_count.at(i);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i).set_total_task_count(total_task_count);
      sqcs.at(i).set_total_part_count(total_part_cnt);
    }
    if (OB_SUCC(ret) && !locations.empty() && (*locations.begin())->loc_meta_->is_external_table_) {
      if (!(*locations.begin())->loc_meta_->is_lake_table_) {
        if (OB_FAIL(assign_external_files_to_sqc(dfo, ctx,
                      (*locations.begin())->loc_meta_->is_external_files_on_disk_, sqcs, parallel))) {
          LOG_WARN("fail to assign external files to sqc", K(ret));
        }
      } else {
        if (OB_FAIL(assign_lake_table_files_to_sqc(locations, ctx, sqcs))) {
          LOG_WARN("fail to assign lake table files to sqc", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_extra_lake_table_files_to_sqc(ctx, dfo))) {
        LOG_WARN("failed to assign extra lake table files to sqc");
      }
    }
  }
  return ret;
}


int ObPXServerAddrUtil::alloc_by_temp_child_distribution(ObExecContext &exec_ctx,
                                                         ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  if (nullptr != dfo.get_root_op_spec()) {
    if (OB_FAIL(alloc_by_temp_child_distribution_inner(exec_ctx, dfo))) {
      LOG_WARN("failed to alloc temp child distribution", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_temp_child_distribution_inner(ObExecContext &exec_ctx,
                                                               ObDfo &child)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScanSpec *, 2> scan_ops;
  const ObTableScanSpec *scan_op = nullptr;
  const ObOpSpec *root_op = NULL;
  child.get_root(root_op);
  ObIArray<ObSqlTempTableCtx>& temp_ctx = exec_ctx.get_temp_table_ctx();
  ObSqlTempTableCtx *ctx = NULL;
  int64_t parallel = child.get_assigned_worker_count();
  if (0 >= parallel) {
    parallel = 1;
    LOG_TRACE("parallel not set in query hint. set default to 1");
  }
  for (int64_t i = 0; NULL == ctx && i < temp_ctx.count(); i++) {
    if (child.get_temp_table_id() == temp_ctx.at(i).temp_table_id_) {
      ctx = &temp_ctx.at(i);
    }
  }
  if (OB_NOT_NULL(ctx) && !ctx->interm_result_infos_.empty()) {
    ObIArray<ObTempTableResultInfo> &interm_result_infos = ctx->interm_result_infos_;
    ObTMArray<std::pair<ObAddr, int64_t>> addr_to_idx_map;
    ObArray<int64_t> sqc_max_task_count;
    ObArray<double> sqc_result_count;
    if (OB_FAIL(addr_to_idx_map.reserve(interm_result_infos.count()))) {
      LOG_WARN("Failed to pre allocate addr to idx map", K(interm_result_infos.count()));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < interm_result_infos.count(); j++) {
      OZ(addr_to_idx_map.push_back(std::make_pair(interm_result_infos.at(j).addr_, j)));
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(lib::ob_sort(addr_to_idx_map.begin(), addr_to_idx_map.end()))) {
    } else if (OB_FAIL(sqc_result_count.prepare_allocate(interm_result_infos.count()))) {
      LOG_WARN("Failed to pre allocate sqc part count");
    } else if (OB_FAIL(generate_dh_map_info(child))) {
      LOG_WARN("fail to generate dh map info", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < interm_result_infos.count(); j++) {
      SMART_VAR(ObPxSqcMeta, sqc) {
        ObTempTableResultInfo &info = interm_result_infos.at(addr_to_idx_map.at(j).second);
        sqc.set_exec_addr(info.addr_);
        sqc.set_qc_addr(GCTX.self_addr());
        sqc.set_dfo_id(child.get_dfo_id());
        sqc.set_sqc_id(j);
        sqc.set_execution_id(child.get_execution_id());
        sqc.set_px_sequence_id(child.get_px_sequence_id());
        sqc.set_qc_id(child.get_qc_id());
        sqc.set_interrupt_id(child.get_interrupt_id());
        sqc.set_px_detectable_ids(child.get_px_detectable_ids());
        sqc.set_fulltree(child.is_fulltree());
        sqc.set_qc_server_id(child.get_qc_server_id());
        sqc.set_parent_dfo_id(child.get_parent_dfo_id());
        sqc.get_monitoring_info().init(child);
        sqc.set_partition_random_affinitize(child.partition_random_affinitize());
        if (OB_SUCC(ret)) {
          if (!child.get_p2p_dh_map_info().is_empty()) {
            if (OB_FAIL(sqc.get_p2p_dh_map_info().assign(child.get_p2p_dh_map_info()))) {
              LOG_WARN("fail to assign p2p dh map info", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(child.add_sqc(sqc))) {
          LOG_WARN("fail add sqc", K(sqc), K(ret));
        }
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < interm_result_infos.count(); j++) {
      sqc_result_count.at(j) = interm_result_infos.at(j).interm_result_ids_.count();
      if (0 >= sqc_result_count.at(j)) {
        sqc_result_count.at(j) = 1;
      }
    }
    ObIArray<ObPxSqcMeta> &sqcs = child.get_sqcs();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_parallel_into_task(parallel, sqc_result_count, sqc_max_task_count))) {
        LOG_WARN("Failed to split parallel into task", K(ret));
      } else if (OB_UNLIKELY(sqcs.count() != sqc_max_task_count.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected sqcs count and sqc max task count", K(ret));
      }
    }
    int64_t total_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i).set_min_task_count(1);
      sqcs.at(i).set_max_task_count(sqc_max_task_count.at(i));
      total_task_count += sqc_max_task_count.at(i);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i).set_total_task_count(total_task_count);
    }
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect temp table in dfo", K(child.get_temp_table_id()), K(temp_ctx));
  }
  if (OB_SUCC(ret)) {
    int64_t base_table_location_key = OB_INVALID_ID;
    if (OB_ISNULL(root_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr or sqc is not empty", K(ret), K(child));
    } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
      LOG_WARN("fail find scan ops in dfo", K(child), K(ret));
    } else if (scan_ops.empty()) {
    } else if (FALSE_IT(base_table_location_key = scan_ops.at(0)->get_table_loc_id())) {
    } else if (OB_FAIL(set_dfo_accessed_location(exec_ctx,
          base_table_location_key, child, scan_ops, NULL, NULL))) {
      LOG_WARN("fail to set all table partition for tsc", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(assign_extra_lake_table_files_to_sqc(exec_ctx, child))) {
      LOG_WARN("failed to assign extra lake table files to sqc");
    }
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_child_distribution(ObExecContext &exec_ctx,
                                                    const ObDfo &child,
                                                    ObDfo &parent)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObPxSqcMeta> &sqcs = child.get_sqcs();
  if (OB_FAIL(generate_dh_map_info(parent))) {
    LOG_WARN("fail to generate dh map info", K(ret));
  } else {
    for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); ++i) {
      const ObPxSqcMeta &child_sqc = sqcs.at(i);
      SMART_VAR(ObPxSqcMeta, sqc) {
        sqc.set_exec_addr(child_sqc.get_exec_addr());
        sqc.set_qc_addr(GCTX.self_addr());
        sqc.set_dfo_id(parent.get_dfo_id());
        sqc.set_min_task_count(child_sqc.get_min_task_count());
        sqc.set_max_task_count(child_sqc.get_max_task_count());
        sqc.set_total_task_count(child_sqc.get_total_task_count());
        sqc.set_total_part_count(child_sqc.get_total_part_count());
        sqc.set_sqc_id(i);
        sqc.set_execution_id(parent.get_execution_id());
        sqc.set_px_sequence_id(parent.get_px_sequence_id());
        sqc.set_qc_id(parent.get_qc_id());
        sqc.set_interrupt_id(parent.get_interrupt_id());
        sqc.set_px_detectable_ids(parent.get_px_detectable_ids());
        sqc.set_fulltree(parent.is_fulltree());
        sqc.set_qc_server_id(parent.get_qc_server_id());
        sqc.set_parent_dfo_id(parent.get_parent_dfo_id());
        sqc.get_monitoring_info().assign(child_sqc.get_monitoring_info());
        sqc.set_partition_random_affinitize(child.partition_random_affinitize());
        if (!parent.get_p2p_dh_map_info().is_empty()) {
          if (OB_FAIL(sqc.get_p2p_dh_map_info().assign(parent.get_p2p_dh_map_info()))) {
            LOG_WARN("fail to assign p2p dh map info", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(parent.add_sqc(sqc))) {
          LOG_WARN("fail add sqc", K(sqc), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_extra_lake_table_files_to_sqc(exec_ctx, parent))) {
        LOG_WARN("failed to assign extra lake table files to sqc");
      }
    }
    LOG_TRACE("allocate by child distribution", K(sqcs));
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_random_distribution(ObExecContext &exec_ctx,
    const ObDfo &child, ObDfo &parent, ObPxNodePool &px_node_pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx.get_physical_plan_ctx()) ||
      OB_ISNULL(exec_ctx.get_physical_plan_ctx()->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL phy plan ctx", K(ret), K(exec_ctx.get_physical_plan_ctx()));
  } else if (0 != parent.get_sqcs_count()) {
    /**
     * this dfo has been built. do nothing.
     */
    LOG_TRACE("this dfo has been built", K(parent.get_dfo_id()));
  } else if (OB_FAIL(px_node_pool.init(exec_ctx))) {
    LOG_WARN("Fail to init mpp node info", K(ret));
  } else if (px_node_pool.data_node_empty()) {
    // a defensive code, if this SQL does not have a location, still alloc by child
    // this kind of plan is not common
    if (OB_FAIL(alloc_by_child_distribution(exec_ctx, child, parent))) {
      LOG_WARN("fail to alloc by child distribution", K(ret));
    }
  } else {
    const ObAddrFixedArray &px_candidate_node_pool = px_node_pool.get_candidate_node_pool();
    sql::ObTMArray<ObAddr> addrs;
    int64_t parallel = std::max(parent.get_assigned_worker_count(), (int64_t)(1));
    ObTMArray<int64_t> sqc_max_task_counts;
    ObTMArray<double> sqc_part_counts;
    int64_t total_task_count = 0;
    ObPxNodeSelectionMode selection_mode = px_node_pool.get_px_node_selection_mode();
    LOG_TRACE("px candidate node pool", K(parallel),
        K(px_candidate_node_pool.count()), K(px_candidate_node_pool));
    switch (selection_mode) {
      case ObPxNodeSelectionMode::SPECIFY_NODE: {
        if (OB_FAIL(addrs.assign(px_candidate_node_pool))) {
          LOG_WARN("Fail to assign", K(ret));
        }
        break;
      }
      case ObPxNodeSelectionMode::SPECIFY_COUNT: {
        int64_t actual_node_num = std::min(
              exec_ctx.get_physical_plan_ctx()->get_phy_plan()->get_px_node_count(),
              px_candidate_node_pool.count());
        for (int i = 0; OB_SUCC(ret) && i < actual_node_num; ++i) {
          if (OB_FAIL(addrs.push_back(px_candidate_node_pool.at(i)))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        break;
      }
      case ObPxNodeSelectionMode::DEFAULT: {
        int64_t actual_node_num = std::min(parallel, px_candidate_node_pool.count());
        for (int i = 0; OB_SUCC(ret) && i < actual_node_num; ++i) {
          if (OB_FAIL(addrs.push_back(px_candidate_node_pool.at(i)))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected mpp node selection mode", K(selection_mode));
        break;
      }
    }
    for (int i = 0; i < addrs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(sqc_part_counts.push_back(1))) {
        LOG_WARN("fail to push back sqc part count", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(split_parallel_into_task(parallel, sqc_part_counts, sqc_max_task_counts))) {
      LOG_WARN("fail to split parallel task", K(ret));
    } else {
      CK(sqc_max_task_counts.count() == addrs.count());
      for (int i = 0; i < sqc_max_task_counts.count() && OB_SUCC(ret); ++i) {
        total_task_count += sqc_max_task_counts.at(i);
      }
      LOG_TRACE("sqc task count", K(total_task_count),
          K(addrs.count()), K(addrs), K(sqc_max_task_counts));
    }
    // generate dh map info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_dh_map_info(parent))) {
        LOG_WARN("fail to generate dh map info", K(ret));
      }
    }
    for (int64_t i = 0; i < addrs.count() && OB_SUCC(ret); ++i) {
      SMART_VAR(ObPxSqcMeta, sqc) {
        sqc.set_exec_addr(addrs.at(i));
        sqc.set_qc_addr(GCTX.self_addr());
        sqc.set_dfo_id(parent.get_dfo_id());
        sqc.set_min_task_count(1);
        sqc.set_max_task_count(sqc_max_task_counts.at(i));
        sqc.set_total_task_count(total_task_count);
        sqc.set_total_part_count(sqc_part_counts.count());
        sqc.set_sqc_id(i);
        sqc.set_execution_id(parent.get_execution_id());
        sqc.set_px_sequence_id(parent.get_px_sequence_id());
        sqc.set_qc_id(parent.get_qc_id());
        sqc.set_interrupt_id(parent.get_interrupt_id());
        sqc.set_px_detectable_ids(parent.get_px_detectable_ids());
        sqc.set_fulltree(parent.is_fulltree());
        sqc.set_qc_server_id(parent.get_qc_server_id());
        sqc.set_parent_dfo_id(parent.get_parent_dfo_id());
        sqc.get_monitoring_info().init(parent);
        sqc.set_partition_random_affinitize(parent.partition_random_affinitize());
        if (OB_SUCC(ret)) {
          if (!parent.get_p2p_dh_map_info().is_empty()) {
            if (OB_FAIL(sqc.get_p2p_dh_map_info().assign(parent.get_p2p_dh_map_info()))) {
              LOG_WARN("fail to assign p2p dh map info", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(parent.add_sqc(sqc))) {
          LOG_WARN("fail add sqc", K(sqc), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_extra_lake_table_files_to_sqc(exec_ctx, parent))) {
        LOG_WARN("failed to assign extra lake table files to sqc");
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_local_distribution(ObExecContext &exec_ctx,
                                                    ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  // generate dh map info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_dh_map_info(dfo))) {
      LOG_WARN("fail to generate dh map info", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL phy plan ctx", K(ret));
  } else {
    SMART_VAR(ObPxSqcMeta, sqc) {
      sqc.set_exec_addr(GCTX.self_addr());
      sqc.set_qc_addr(GCTX.self_addr());
      sqc.set_dfo_id(dfo.get_dfo_id());
      sqc.set_min_task_count(1);
      sqc.set_max_task_count(dfo.get_assigned_worker_count());
      sqc.set_sqc_id(0);
      sqc.set_execution_id(dfo.get_execution_id());
      sqc.set_px_sequence_id(dfo.get_px_sequence_id());
      sqc.set_qc_id(dfo.get_qc_id());
      sqc.set_interrupt_id(dfo.get_interrupt_id());
      sqc.set_px_detectable_ids(dfo.get_px_detectable_ids());
      sqc.set_fulltree(dfo.is_fulltree());
      sqc.set_parent_dfo_id(dfo.get_parent_dfo_id());
      sqc.set_qc_server_id(dfo.get_qc_server_id());
      sqc.get_monitoring_info().init(dfo);
      sqc.set_partition_random_affinitize(dfo.partition_random_affinitize());
      if (!dfo.get_p2p_dh_map_info().is_empty()) {
        OZ(sqc.get_p2p_dh_map_info().assign(dfo.get_p2p_dh_map_info()));
      }
      OZ(dfo.add_sqc(sqc));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_extra_lake_table_files_to_sqc(exec_ctx, dfo))) {
        LOG_WARN("failed to assign extra lake table files to sqc");
      }
    }
  }
  return ret;
}

/**
 *  hash-local的ppwj slave mapping计划又两种，第一种是：
 *                  |
 *               hash join (dfo3)
 *                  |
 * ----------------------------------
 * |                                |
 * |                                |
 * |                                |
 * TSC(dfo1:partition-hash)         TSC(dfo2:hash-local)
 * 在遇到这种类型的调度的树时，我们调度dfo1和dfo3的时候，dfo2还没背调度起来。
 * 在pkey的计划中，dfo2才是reference table，slave mapping的构建中，parent（也就是dfo3）的构建
 * 依赖于reference的端，所以在alloc parent的时候，我们提前将dfo2也alloc出来，然后parent按照
 * dfo2的sqc来进行构建。
 *
 */
int ObPXServerAddrUtil::alloc_by_reference_child_distribution(
    ObExecContext &exec_ctx, ObDfo &parent)
{
  int ret = OB_SUCCESS;
  ObDfo *reference_child = nullptr;
  if (OB_FAIL(find_reference_child(parent, reference_child))) {
    LOG_WARN("find reference child failed", K(ret));
  } else if (OB_ISNULL(reference_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret));
  } else if (OB_FAIL(alloc_by_child_distribution(exec_ctx, *reference_child, parent))) {
    LOG_WARN("failed to alloc by child distribution", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_distribution_of_reference_child(
                                          const ObIArray<ObTableLocation> *table_locations,
                                          ObExecContext &exec_ctx,
                                          ObDfo &parent)
{
  int ret = OB_SUCCESS;
  ObDfo *reference_child = nullptr;
  if (OB_FAIL(find_reference_child(parent, reference_child))) {
    LOG_WARN("find reference child failed", K(ret));
  } else if (OB_ISNULL(reference_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret));
  } else if (OB_FAIL(alloc_by_data_distribution(table_locations, exec_ctx, *reference_child))) {
    LOG_WARN("failed to alloc by data distribution", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::find_reference_child(ObDfo &parent, ObDfo *&reference_child)
{
  int ret = OB_SUCCESS;
  reference_child = nullptr;
  for (int64_t i = 0;
       OB_SUCC(ret) && nullptr == reference_child && i < parent.get_child_count();
       ++i) {
    ObDfo *candi_child = nullptr;
    if (OB_FAIL(parent.get_child_dfo(i, candi_child))) {
      LOG_WARN("failed to get dfo", K(ret));
    } else if (OB_ISNULL(candi_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret));
    } else if (ObPQDistributeMethod::HASH == candi_child->get_dist_method() &&
               candi_child->is_out_slave_mapping()) {
      reference_child = candi_child;
    }
  }
  return ret;
}

int ObPXServerAddrUtil::check_partition_wise_location_valid(DASTabletLocIArray &tsc_locations)
{
  int ret = OB_SUCCESS;
  common::ObAddr exec_addr;
  if (tsc_locations.count() <= 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsc locatition ", K(ret));
  } else {
    exec_addr = tsc_locations.at(0)->server_;
  }
  ARRAY_FOREACH_X(tsc_locations, idx, cnt, OB_SUCC(ret)) {
    ObDASTabletLoc &partition_rep_location = *tsc_locations.at(idx);
    const common::ObAddr &addr = partition_rep_location.server_;
    if (exec_addr != addr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some partition not belong to this server", K(ret),
          K(exec_addr), K(addr));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_access_partition_order(
  ObDfo &dfo,
  const ObOpSpec *phy_op,
  bool &asc_order)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *root = NULL;
  dfo.get_root(root);
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get root phy op", K(ret));
  } else if (OB_FAIL(get_access_partition_order_recursively(root, phy_op, asc_order))) {
    LOG_WARN("fail to get table scan partition", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::get_access_partition_order_recursively (
  const ObOpSpec *root,
  const ObOpSpec *phy_op,
  bool &asc_order)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the root or phy op is null", K(ret), K(root), K(phy_op));
  } else if (root == phy_op) { // 没有GI的情况下，默认ASC访问
    asc_order = true;
    LOG_DEBUG("No GI in this dfo");
  } else if (PHY_GRANULE_ITERATOR == phy_op->get_type()) {
    const ObGranuleIteratorSpec *gi = static_cast<const ObGranuleIteratorSpec*>(phy_op);
    asc_order = !ObGranuleUtil::desc_order(gi->gi_attri_flag_);
  } else if (OB_FAIL(get_access_partition_order_recursively(root, phy_op->get_parent(), asc_order))) {
    LOG_WARN("fail to access partition order", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::set_dfo_accessed_location(ObExecContext &ctx,
                                                  int64_t base_table_location_key,
                                                  ObDfo &dfo,
                                                  ObIArray<const ObTableScanSpec *> &scan_ops,
                                                  const ObTableModifySpec *dml_op,
                                                  ObDASTableLoc *dml_loc)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *dml_table_loc = nullptr;
  ObTableID dml_table_location_key = OB_INVALID_ID;
  ObTableID dml_ref_table_id = OB_INVALID_ID;
  ObBaseOrderMap base_order_map;
  ObSEArray<std::pair<int64_t, bool>, 18> locations_order;
  if (OB_FAIL(base_order_map.init(max(1, scan_ops.count())))) {
    LOG_WARN("Failed to init base_order_map");
  }
  // 处理insert op 对应的partition location信息
  if (OB_FAIL(ret) || OB_ISNULL(dml_op)) {
    // pass
  } else {
    ObDASTableLoc *table_loc = nullptr;
    ObTableID table_location_key = OB_INVALID_ID;
    ObTableID ref_table_id = OB_INVALID_ID;
    if (OB_FAIL(dml_op->get_single_table_loc_id(table_location_key, ref_table_id))) {
      LOG_WARN("get single table location id failed", K(ret));
    } else {
      if (dml_op->is_table_location_uncertain()) {
        if (OB_ISNULL(dml_loc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected dml loc", K(ret));
        } else {
          table_loc = dml_loc;
        }
      } else {
        // 通过TSC或者DML获得当前的DFO的partition对应的location信息
        // 后续利用location信息构建对应的SQC meta
        if (OB_ISNULL(table_loc = DAS_CTX(ctx).get_table_loc_by_id(table_location_key, ref_table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get table loc id", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table loc is null", K(ret));
    } else if (OB_FAIL(set_sqcs_accessed_location(ctx, base_table_location_key,
        dfo, base_order_map, table_loc, dml_op, locations_order))) {
      LOG_WARN("failed to set sqc accessed location", K(ret));
    }
    dml_table_loc = table_loc;
    dml_table_location_key = table_location_key;
    dml_ref_table_id = ref_table_id;
  }
  // 处理tsc对应的partition location信息
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
    ObDASTableLoc *table_loc = nullptr;
    const ObTableScanSpec *scan_op = nullptr;
    uint64_t table_location_key = common::OB_INVALID_ID;
    uint64_t ref_table_id = common::OB_INVALID_ID;
    // 物理表还是虚拟表?
    if (OB_ISNULL(scan_op = scan_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan op can't be null", K(ret));
    } else if (FALSE_IT(table_location_key = scan_op->get_table_loc_id())) {
    } else if (FALSE_IT(ref_table_id = scan_op->get_loc_ref_table_id())) {
    } else {
      if (!scan_op->is_ob_external_table()) {
        if (OB_ISNULL(table_loc = DAS_CTX(ctx).get_table_loc_by_id(table_location_key, ref_table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get phy table location", K(ret));
        }
      } else {
        if (OB_FAIL(get_external_table_loc(ctx, table_location_key, ref_table_id,
                                          scan_op->get_query_range_provider(), dfo, table_loc))) {
          LOG_WARN("fail to get external table loc", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(set_sqcs_accessed_location(ctx,
            // dml op has already set sqc.get_location information,
            // table scan does not need to be set again
            OB_ISNULL(dml_op) ? base_table_location_key : OB_INVALID_ID,
            dfo, base_order_map, table_loc, scan_op, locations_order))) {
        LOG_WARN("failed to set sqc accessed location", K(ret), K(table_location_key),
                K(ref_table_id), KPC(table_loc));
      }
    }
  } // end for
  if (OB_FAIL(ret)) {
    if (NULL == dml_op) {
      LOG_WARN("set dfo accessed location failed, dml op is null", K(base_table_location_key), K(dfo));
    } else {
      LOG_WARN("set dfo accessed location failed, dml op is not null", K(base_table_location_key),
               K(dml_op), K(dml_op->is_table_location_uncertain()),  K(dml_table_location_key),
               K(dml_ref_table_id), KPC(dml_table_loc));
    }
  } else {
    ARRAY_FOREACH_X(dfo.get_sqcs(), sqc_idx, sqc_cnt, OB_SUCC(ret)) {
      ObPxSqcMeta &sqc_meta = dfo.get_sqcs().at(sqc_idx);
      if (OB_FAIL(sqc_meta.get_locations_order().assign(locations_order))) {
        LOG_WARN("assign failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::set_sqcs_accessed_location(
    ObExecContext &ctx, int64_t base_table_location_key, ObDfo &dfo,
    ObBaseOrderMap &base_order_map,
    const ObDASTableLoc *table_loc, const ObOpSpec *phy_op,
    ObIArray<std::pair<int64_t, bool>> &locations_order)
{
  int ret = OB_SUCCESS;
  ObIArray<ObPxSqcMeta> &sqcs = dfo.get_sqcs();
  int n_locations = 0;
  const DASTabletLocList &locations = table_loc->get_tablet_locs();
  DASTabletLocSEArray temp_locations;
  if (OB_ISNULL(table_loc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table_loc or phy_op", K(phy_op), K(table_loc));
  } else if (table_loc->loc_meta_->is_external_table_) {
    //just copy locations, do not need reorder
    OZ (temp_locations.reserve(locations.size()));
    for (auto iter = locations.begin(); iter != locations.end() && OB_SUCC(ret); ++iter) {
      OZ (temp_locations.push_back(*iter));
    }
  } else {
    int64_t table_location_key = table_loc->get_table_location_key();
    bool asc_order = true;
    if (locations.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the locations can not be zero", K(ret), K(locations.size()));
    } else if (OB_FAIL(get_access_partition_order(dfo, phy_op, asc_order))) {
      LOG_WARN("fail to get table scan partition order", K(ret));
    } else if (OB_FAIL(ObPXServerAddrUtil::reorder_all_partitions(table_location_key,
        table_loc->get_ref_table_id(), locations,
        temp_locations, asc_order, ctx, base_order_map, phy_op->get_id(), locations_order))) {
      // 按照GI要求的访问顺序对当前SQC涉及到的分区进行排序
      // 如果是partition wise join场景, 需要根据partition_wise_join要求结合GI要求做asc/desc排序
      LOG_WARN("fail to reorder all partitions", K(ret));
    } else {
      LOG_TRACE("sqc partition order is", K(asc_order), K(locations), K(temp_locations), KPC(table_loc->loc_meta_));
    }
  }

  // 将一个表涉及到的所有partition按照server addr划分到对应的sqc中
  ARRAY_FOREACH_X(sqcs, sqc_idx, sqc_cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc_meta = sqcs.at(sqc_idx);
    DASTabletLocIArray &sqc_locations = sqc_meta.get_access_table_locations_for_update();
    ObIArray<ObSqcTableLocationKey> &sqc_location_keys = sqc_meta.get_access_table_location_keys();
    ObIArray<ObSqcTableLocationIndex> &sqc_location_indexes = sqc_meta.get_access_table_location_indexes();
    int64_t location_start_pos = sqc_locations.count();
    int64_t location_end_pos = sqc_locations.count();
    const common::ObAddr &sqc_server = sqc_meta.get_exec_addr();
    ARRAY_FOREACH_X(temp_locations, idx, cnt, OB_SUCC(ret)) {
      const common::ObAddr &server = temp_locations.at(idx)->server_;
      if (server == sqc_server) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sqc_locations.push_back(temp_locations.at(idx)))) {
          LOG_WARN("sqc push back table location failed", K(ret));
        } else if (OB_FAIL(sqc_location_keys.push_back(ObSqcTableLocationKey(
              table_loc->get_table_location_key(),
              table_loc->get_ref_table_id(),
              temp_locations.at(idx)->tablet_id_,
              IS_DML(phy_op->get_type()),
              IS_DML(phy_op->get_type()) ?
                static_cast<const ObTableModifySpec *>(phy_op)->is_table_location_uncertain() :
                false)))) {
        } else {
          ++n_locations;
          ++location_end_pos;
        }
      }
    }
    if (OB_SUCC(ret) && location_start_pos < location_end_pos) {
      if (OB_FAIL(sqc_location_indexes.push_back(ObSqcTableLocationIndex(
          table_loc->get_table_location_key(),
          location_start_pos,
          location_end_pos - 1)))) {
        LOG_WARN("fail to push back table location index", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && n_locations != locations.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we do not find this addr's execution sqc", K(ret), K(n_locations),
             K(locations.size()), K(sqcs), K(locations));
  }
  return ret;
}


// used to fast lookup from phy partition id to partition order(index)
// for a range partition, the greater the range, the greater the partition_index
// for a hash partition, the index means nothing
int ObPXServerAddrUtil::build_tablet_idx_map(ObTaskExecutorCtx &task_exec_ctx,
                                              int64_t tenant_id,
                                              uint64_t ref_table_id,
                                              ObTabletIdxMap &idx_map)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(task_exec_ctx.schema_service_)) {
  } else if (OB_FAIL(task_exec_ctx.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema))) {
    LOG_WARN("fail get table schema", K(tenant_id), K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail get schema", K(ref_table_id), K(ret));
  } else if (OB_FAIL(build_tablet_idx_map(table_schema, idx_map))) {
    LOG_WARN("fail create index map", K(ret), "cnt", table_schema->get_all_part_num());
  }
  return ret;
}


class ObPXTabletOrderIndexCmp
{
public:
  ObPXTabletOrderIndexCmp(bool asc, ObTabletIdxMap *map)
      : asc_(asc), map_(map)
  {}
  bool operator() (const ObDASTabletLoc *left, const ObDASTabletLoc *right)
  {
    int ret = OB_SUCCESS;
    bool bret = false;
    int64_t lv, rv;
    if (OB_FAIL(map_->get_refactored(left->tablet_id_.id(), lv))) {
      LOG_WARN("fail get partition index", K(asc_), K(left), K(right), K(ret));
      throw OB_EXCEPTION<OB_HASH_NOT_EXIST>();
    } else if (OB_FAIL(map_->get_refactored(right->tablet_id_.id(), rv))) {
      LOG_WARN("fail get partition index", K(asc_), K(left), K(right), K(ret));
      throw OB_EXCEPTION<OB_HASH_NOT_EXIST>();
    } else {
      bret = asc_ ? (lv < rv) : (lv > rv);
    }
    return bret;
  }
private:
  bool asc_;
  ObTabletIdxMap *map_;
};

int ObPXServerAddrUtil::reorder_all_partitions(
    int64_t table_location_key, int64_t ref_table_id, const DASTabletLocList &src_locations,
    DASTabletLocIArray &dst_locations, bool asc, ObExecContext &exec_ctx,
    ObBaseOrderMap &base_order_map, int64_t op_id,
    ObIArray<std::pair<int64_t, bool>> &locations_order)
{
  int ret = OB_SUCCESS;
  dst_locations.reset();
  if (src_locations.size() > 1) {
    ObTabletIdxMap tablet_order_map;
    if (OB_FAIL(dst_locations.reserve(src_locations.size()))) {
      LOG_WARN("fail reserve locations", K(ret), K(src_locations.size()));
    // virtual table is list partition now,
    // no actual partition define, can't traverse
    // table schema for partition info
    } else if (!is_virtual_table(ref_table_id) &&
        OB_FAIL(build_tablet_idx_map(exec_ctx.get_task_exec_ctx(),
                                     GET_MY_SESSION(exec_ctx)->get_effective_tenant_id(),
                                     ref_table_id, tablet_order_map))) {
      LOG_WARN("fail build index lookup map", K(ret));
    }
    for (auto iter = src_locations.begin(); iter != src_locations.end() && OB_SUCC(ret); ++iter) {
        if (OB_FAIL(dst_locations.push_back(*iter))) {
        LOG_WARN("fail to push dst locations", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      try {
        if (!is_virtual_table(ref_table_id)) {
          lib::ob_sort(&dst_locations.at(0),
                    &dst_locations.at(0) + dst_locations.count(),
                    ObPXTabletOrderIndexCmp(asc, &tablet_order_map));
        }
      } catch (OB_BASE_EXCEPTION &except) {
        if (OB_HASH_NOT_EXIST == (ret = except.get_errno())) {
          // schema changed during execution, notify to retry
          ret = OB_SCHEMA_ERROR;
        }
      }
      GroupPWJTabletIdMap *group_pwj_map = nullptr;
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to sort  locations", K(ret));
      } else if (OB_NOT_NULL(group_pwj_map = exec_ctx.get_group_pwj_map())) {
        GroupPWJTabletIdInfo group_pwj_tablet_id_info;
        TabletIdArray &tablet_id_array = group_pwj_tablet_id_info.tablet_id_array_;
        if (OB_FAIL(group_pwj_map->get_refactored(table_location_key, group_pwj_tablet_id_info))) {
          if (OB_HASH_NOT_EXIST == ret) {
            // means this is not a partition wise join table, do not need to reorder partition
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get_refactored", K(table_location_key));
          }
        } else {
          // set base order or reorder partition as base order
          uint64_t pwj_group_id = group_pwj_tablet_id_info.group_id_;
          std::pair<ObIArray<int64_t> *, bool> base_order;
          if (OB_FAIL(base_order_map.get_map().get_refactored(pwj_group_id, base_order))) {
            if (ret == OB_HASH_NOT_EXIST) {
              ret = base_order_map.add_base_partition_order(pwj_group_id, tablet_id_array,
                                                            dst_locations, asc);
              if (ret != OB_SUCCESS) {
                LOG_WARN("failed to add_base_partition_order");
              } else {
                LOG_TRACE("succ to add_base_partition_order", K(pwj_group_id), K(table_location_key));
              }
            } else {
              LOG_WARN("failed to get_refactored");
            }
          } else if (OB_FAIL(base_order_map.reorder_partition_as_base_order(
                  pwj_group_id, tablet_id_array, dst_locations))) {
            LOG_WARN("failed to reorder_partition_as_base_order");
          } else {
            asc = base_order.second;
            LOG_TRACE("succ to reorder_partition_as_base_order", K(pwj_group_id), K(table_location_key));
          }
        }
      }
    }
  } else if (1 == src_locations.size() &&
             OB_FAIL(dst_locations.push_back(*src_locations.begin()))) {
    LOG_WARN("fail to push dst locations", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(locations_order.push_back(std::make_pair(op_id, asc)))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

/**
 * 算法文档：
 * 大致思路：
 * n为总线程数，p为涉及总的partition数，ni为第i个sqc被计算分的线程数，pi为第i个sqc的partition数量。
 * a. 一个adjust函数，递归的调整sqc的线程数。求得ni ＝ n*pi/p的值，保证每个都是大于等于1。
 * b. 计算sqc执行时间，并按照其进行排序。
 * c. 剩下线程从执行时间长到时间短挨个加入到sqc中。
 */
int ObPXServerAddrUtil::split_parallel_into_task(const int64_t parallel,
                                                 const common::ObIArray<double> &sqc_part_count,
                                                 common::ObIArray<int64_t> &results) {
  int ret = OB_SUCCESS;
  common::ObArray<ObPxSqcTaskCountMeta> sqc_task_metas;
  double total_part_count = 0;
  int64_t total_thread_count = 0;
  int64_t thread_remain = 0;
  results.reset();
  if (parallel <= 0 || sqc_part_count.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid input argument", K(ret), K(parallel), K(sqc_part_count.count()));
  } else if (OB_FAIL(results.prepare_allocate(sqc_part_count.count()))) {
    LOG_WARN("Failed to prepare allocate array", K(ret));
  }
  // prepare
  ARRAY_FOREACH(sqc_part_count, idx) {
    ObPxSqcTaskCountMeta meta;
    meta.idx_ = idx;
    meta.partition_count_ = sqc_part_count.at(idx);
    meta.thread_count_ = 0;
    meta.time_ = 0;
    if (OB_FAIL(sqc_task_metas.push_back(meta))) {
      LOG_WARN("Failed to push back sqc partition count", K(ret));
    } else if (sqc_part_count.at(idx) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid partition count", K(ret));
    } else {
      total_part_count += sqc_part_count.at(idx);
    }
  }
  if (OB_SUCC(ret)) {
    // 为什么需要调整，因为极端情况下可能有的sqc只能拿到不足一个线程；算法必须保证每个sqc至少
    // 有一个线程。
    if (OB_FAIL(adjust_sqc_task_count(sqc_task_metas, parallel, total_part_count))) {
      LOG_WARN("Failed to adjust sqc task count", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 算出每个sqc的执行时间
    for (int64_t i = 0; i < sqc_task_metas.count(); ++i) {
      ObPxSqcTaskCountMeta &meta = sqc_task_metas.at(i);
      total_thread_count += meta.thread_count_;
      meta.time_ = static_cast<double>(meta.partition_count_) / static_cast<double>(meta.thread_count_);
    }
    // 排序，执行时间长的排在前面
    auto compare_fun_long_time_first = [](ObPxSqcTaskCountMeta a, ObPxSqcTaskCountMeta b) -> bool { return a.time_ > b.time_; };
    lib::ob_sort(sqc_task_metas.begin(),
              sqc_task_metas.end(),
              compare_fun_long_time_first);
    /// 把剩下的线程安排出去
    thread_remain = parallel - total_thread_count;
    if (thread_remain <= 0) {
      // 这种情况是正常的，parallel < sqc count的时候就会出现这种情况。
    } else if (thread_remain > sqc_task_metas.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Thread remain is invalid", K(ret), K(thread_remain), K(sqc_task_metas.count()),
               K(sqc_part_count), K(total_part_count));
    } else {
      for (int64_t i = 0; i < thread_remain; ++i) {
        ObPxSqcTaskCountMeta &meta = sqc_task_metas.at(i);
        meta.thread_count_ += 1;
        total_thread_count += 1;
        meta.time_ = static_cast<double>(meta.partition_count_) / static_cast<double>(meta.thread_count_);
      }
    }
  }
  // 将结果记录下来
  if (OB_SUCC(ret)) {
    int64_t idx = 0;
    for (int64_t i = 0; i < sqc_task_metas.count(); ++i) {
      ObPxSqcTaskCountMeta meta = sqc_task_metas.at(i);
      idx = meta.idx_;
      results.at(idx) = meta.thread_count_;
    }
  }
  // 检验，指定的并行度大于机器数，理论上分配不超过parallel个线程。
  if (OB_SUCC(ret) && parallel > sqc_task_metas.count() && total_thread_count > parallel) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate expected parallel", K(ret), K(parallel), K(sqc_task_metas.count()),
             K(total_thread_count));
  }
  LOG_TRACE("Sqc max task count", K(results), K(sqc_task_metas), K(sqc_part_count));
  return ret;
}

int ObPXServerAddrUtil::adjust_sqc_task_count(common::ObIArray<ObPxSqcTaskCountMeta> &sqc_tasks,
                                              int64_t parallel,
                                              double partition)
{
  int ret = OB_SUCCESS;
  int64_t thread_used = 0;
  int64_t partition_remain = partition;
  // 存在partition总数为0 的情况，例如，在gi任务划分中，所有partition都没有宏块的情况。
  double real_partition = NON_ZERO_VALUE(partition);
  ARRAY_FOREACH(sqc_tasks, idx) {
    ObPxSqcTaskCountMeta &meta = sqc_tasks.at(idx);
    if (!meta.finish_) {
      meta.thread_count_ = meta.partition_count_ * parallel / real_partition;
      if (0 >= meta.thread_count_) {
        // 出现小数个线程或者负数个线程，调整改线程为1，标记为finish，后续不再调整它。
        thread_used++;
        partition_remain -= meta.partition_count_;
        meta.finish_ = true;
        meta.thread_count_ = 1;
      }
    }
  }
  if (thread_used != 0) {
    if (OB_FAIL(adjust_sqc_task_count(sqc_tasks, parallel - thread_used, partition_remain))) {
      LOG_WARN("Failed to adjust sqc task count", K(ret), K(sqc_tasks));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::do_random_dfo_distribution(
    const common::ObIArray<common::ObAddr> &src_addrs,
    int64_t dst_addrs_count,
    common::ObIArray<common::ObAddr> &dst_addrs)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> new_addrs;
  CK(src_addrs.count() > dst_addrs_count && dst_addrs_count > 0);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_addrs.assign(src_addrs))) {
    LOG_WARN("fail to assign src addrs", K(ret));
  } else {
    int64_t rand_num = 0;
    int64_t m = dst_addrs_count;
    // reservoir sampling
    // https://en.wikipedia.org/wiki/Reservoir_sampling
    for (int i = m; i < new_addrs.count() && OB_SUCC(ret); ++i) {
      rand_num = ObRandom::rand(0, i); //thread local random, seed_[0] is thread id.
      if (rand_num < m) {
        std::swap(new_addrs.at(i), new_addrs.at(rand_num));
      }
    }
    if (OB_SUCC(ret)) {
      dst_addrs.reset();
      for (int i = 0; i < m && OB_SUCC(ret); ++i) {
        if (OB_FAIL(dst_addrs.push_back(new_addrs.at(i)))) {
          LOG_WARN("fail to push back dst addrs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxOperatorVisitor::visit(ObExecContext &ctx, const ObOpSpec &root, ApplyFunc &func)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(func.apply(ctx, root))) {
    LOG_WARN("fail apply func to input", "op_id", root.id_, K(ret));
  } else if (!IS_PX_RECEIVE(root.type_)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_cnt(); ++i) {
      const ObOpSpec *child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(root.type_));
      } else if (OB_FAIL(visit(ctx, *child_op, func))) {
        LOG_WARN("fail to apply func", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(func.reset(root))) {
    LOG_WARN("Failed to reset", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::build_tablet_idx_map(
      const share::schema::ObTableSchema *table_schema,
      ObTabletIdxMap &idx_map)
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = 0;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(idx_map.create(table_schema->get_all_part_num(), "TabletOrderIdx"))) {
    LOG_WARN("fail create index map", K(ret), "cnt", table_schema->get_all_part_num());
  } else if (is_virtual_table(table_schema->get_table_id())) {
    // In observer 4.2, the table schema of a distributed virtual table will show all_part_num as 1,
    // whereas in lower versions it would display as 65536.
    // For a distributed virtual table, we may encounter a situation where part_id is 1 in sqc1,
    // part_id is 2 in sqc2 and so on, but the idx_map only contains one item with key=1.
    // Hence, if we seek with part_id=2, the idx_map will return -4201 (OB_HASH_NOT_EXIST)
    // will return -4201(OB_HASH_NOT_EXIST). In such cases, we can directly obtain the value that equals part_id + 1.
    for (int i = 0; OB_SUCC(ret) && i < table_schema->get_all_part_num(); ++i) {
      if (OB_FAIL(idx_map.set_refactored(i + 1, tablet_idx++))) {
        LOG_WARN("fail set value to hashmap", K(ret));
      }
    }
  } else {
    ObPartitionSchemaIter iter(*table_schema, CHECK_PARTITION_MODE_NORMAL);
    ObPartitionSchemaIter::Info info;
    do {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail get next partition item from iterator", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(idx_map.set_refactored(info.tablet_id_.id(), tablet_idx++))) {
        LOG_WARN("fail set value to hashmap", K(ret));
      }
      LOG_DEBUG("table item info", K(info));
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObPxPartitionLocationUtil::get_all_tables_tablets(
    const common::ObIArray<const ObTableScanSpec*> &scan_ops,
    const DASTabletLocIArray &all_locations,
    const common::ObIArray<ObSqcTableLocationKey> &table_location_keys,
    ObSqcTableLocationKey dml_location_key,
    common::ObIArray<DASTabletLocArray> &all_tablets)
{
  int ret = common::OB_SUCCESS;
  DASTabletLocArray tablets;
  int64_t idx = 0;
  CK(all_locations.count() == table_location_keys.count());
  if (OB_SUCC(ret) && dml_location_key.table_location_key_ != OB_INVALID_ID) {
    for ( ; idx < all_locations.count() && OB_SUCC(ret); ++idx) {
      if (dml_location_key.table_location_key_ == table_location_keys.at(idx).table_location_key_
          && table_location_keys.at(idx).is_dml_) {
        if (OB_FAIL(tablets.push_back(all_locations.at(idx)))) {
          LOG_WARN("fail to push back pkey", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !tablets.empty()) {
      if (OB_FAIL(all_tablets.push_back(tablets))) {
        LOG_WARN("push back tsc partitions failed", K(ret));
      }
    }
  }
  for (int i = 0; i < scan_ops.count() && OB_SUCC(ret); ++i) {
    tablets.reset();
    for (int j = 0; j < all_locations.count() && OB_SUCC(ret); ++j) {
      if (scan_ops.at(i)->get_table_loc_id() == table_location_keys.at(j).table_location_key_
          && !table_location_keys.at(j).is_dml_) {
        if (OB_FAIL(tablets.push_back(all_locations.at(j)))) {
          LOG_WARN("fail to push back pkey", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !tablets.empty()) {
      if (OB_FAIL(all_tablets.push_back(tablets))) {
        LOG_WARN("push back tsc partitions failed", K(ret));
      }
    }
  }
  LOG_TRACE("add partition in table by tscs", K(ret), K(all_locations), K(all_tablets));
  return ret;
}

//serialize plan tree for engine3.0
template <bool SERIALIZE_PLAN_PART>
int ObPxTreeSerializer::serialize_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       char **frames,
                                       const int64_t frame_cnt,
                                       bool no_ser_data/* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t need_extra_mem_size = 0;
  if (SERIALIZE_PLAN_PART) {
    OB_UNIS_ENCODE(all_frames.count());
    for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
      OB_UNIS_ENCODE(all_frames.at(i));
    }
  }
  int64_t item_size = 0;
  if ((all_frames.count() > 0) && all_frames.at(0).use_rich_format_) {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
  } else {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    //TODO shengle seri can opt, only serialize: sizeof(ObDatum) + sizeof(ObEvalInfo)
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      char *frame_buf = frames[frame_info.frame_idx_];
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * item_size;
      OB_UNIS_ENCODE(expr_mem_size);
      if (pos + expr_mem_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_mem_size), K(buf_len));
      } else if (!no_ser_data && 0 < expr_mem_size) {
        MEMCPY(buf + pos, frame_buf, expr_mem_size);
        pos += expr_mem_size;
      }
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                  (frame_buf + j * item_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ENCODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ENCODE(expr_datum_size);
      if (pos + expr_datum_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_datum_size), K(buf_len));
      } else if (0 < expr_datum_size) {
        // TODO: longzhong.wlz 这里可能有兼容性问题，暂时这样，后面看着改
        MEMCPY(buf + pos, expr_datum->ptr_, expr_datum_size);
        pos += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret),
      K(ser_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

template <bool SERIALIZE_PLAN_PART>
int ObPxTreeSerializer::serialize_expr_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("trace start ser expr frame info", K(ret), K(buf_len), K(pos));
  if (SERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = expr_frame_info.need_ctx_cnt_;
    OB_UNIS_ENCODE(need_ctx_cnt);
  }
  // expr extra info
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_SUCC(ret) && OB_FAIL(expr_info.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize expr extra info", K(ret));
  }
  // rt exprs
  if (SERIALIZE_PLAN_PART) {
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    const int32_t expr_cnt = expr_frame_info.is_mark_serialize()
        ? expr_frame_info.ser_expr_marks_.count()
        : exprs.count();
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, expr_cnt))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (nullptr == ObExpr::get_serialize_array()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialize array is null", K(ret), K(pos), K(expr_cnt));
    } else {
      if (!expr_frame_info.is_mark_serialize()) {
        LOG_TRACE("exprs normal serialization", K(expr_cnt));
        for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
          if (OB_FAIL(exprs.at(i).serialize(buf, buf_len, pos))) {
            LOG_WARN("failed to serialize expr", K(ret), K(i), K(exprs.at(i)));
          }
        }
      } else {
        LOG_TRACE("exprs mark serialization", K(expr_cnt), K(exprs.count()));
        for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
          if (expr_frame_info.ser_expr_marks_.at(i)) {
            if (OB_FAIL(exprs.at(i).serialize(buf, buf_len, pos))) {
              LOG_WARN("failed to serialize expr", K(ret), K(i), K(exprs.at(i)));
            }
          } else if (OB_FAIL(ObEmptyExpr::instance().serialize(buf, buf_len, pos))) {
            LOG_WARN("serialize empty expr failed", K(ret), K(i));
          }
        }
      }
    }
  }
  // frames
  int64_t frame_count = 0;
  char **frames = nullptr;
  if (ctx.get_ori_frame_cnt() != 0 && ctx.get_ori_frames() != nullptr) {
    frame_count = ctx.get_ori_frame_cnt();
    frames = ctx.get_ori_frames();
  } else {
    frame_count = ctx.get_frame_cnt();
    frames = ctx.get_frames();
  }
  OB_UNIS_ENCODE(frame_count);
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.const_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.param_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.dynamic_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.datum_frame_, frames, frame_count, true));
  LOG_DEBUG("trace end ser expr frame info", K(ret), K(buf_len), K(pos));
  return ret;
}

template <bool DESERIALIZE_PLAN_PART>
int ObPxTreeSerializer::deserialize_frame_info(const char *buf,
                                       int64_t data_len,
                                       int64_t &pos,
                                       ObIAllocator &allocator,
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       ObIArray<char *> *char_ptrs,
                                       char **&frames,
                                       int64_t &frame_cnt,
                                       bool no_deser_data)
{
  int ret = OB_SUCCESS;
  int64_t frame_info_cnt = all_frames.count();
  if (DESERIALIZE_PLAN_PART) {
    OB_UNIS_DECODE(frame_info_cnt);
    ObIArray<ObFrameInfo> &non_const_all_frames = const_cast<ObIArray<ObFrameInfo> &>(all_frames);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(non_const_all_frames.reserve(frame_info_cnt))) {
      LOG_WARN("failed to reserve const frame", K(ret));
    } else {
      ObFrameInfo frame_info;
      for (int64_t i = 0; i < frame_info_cnt && OB_SUCC(ret); ++i) {
        OB_UNIS_DECODE(frame_info);
        if (OB_FAIL(non_const_all_frames.push_back(frame_info))) {
          LOG_WARN("failed to push back frame", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != char_ptrs && OB_FAIL(char_ptrs->reserve(frame_info_cnt))) {
    LOG_WARN("failed to reserve const frame", K(ret));
  }
  int64_t need_extra_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    int64_t expr_mem_size = 0;
    OB_UNIS_DECODE(expr_mem_size);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else if (0 < frame_info.expr_cnt_) {
      char *frame_buf = nullptr;
      if (nullptr == (frame_buf = static_cast<char*>(allocator.alloc(frame_info.frame_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate frame buf", K(ret));
      } else if (pos + expr_mem_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(frame_info.frame_size_), K(data_len));
      } else {
        MEMSET(frame_buf, 0, frame_info.frame_size_);
        frames[frame_info.frame_idx_] = frame_buf;
        if (nullptr != char_ptrs && OB_FAIL(char_ptrs->push_back(frame_buf))) {
          LOG_WARN("failed to push back frame buf", K(ret));
        }
        if (!no_deser_data && 0 < expr_mem_size) {
          MEMCPY(frame_buf, buf + pos, expr_mem_size);
          pos += expr_mem_size;
        }
      }
    }
  }
  OB_UNIS_DECODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t des_mem_size = 0;
  char *expr_datum_buf = nullptr;
  if (0 < need_extra_mem_size
      && nullptr == (expr_datum_buf = static_cast<char*>(allocator.alloc(need_extra_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    int64_t item_size = 0;
    if (frame_info.use_rich_format_) {
      item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
    } else {
      item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    }
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      OB_UNIS_DECODE(expr_datum_size);
      if (pos + expr_datum_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_datum_size), K(data_len));
      } else if (0 == expr_datum_size) {
        // 对于该序列化数据，datum的len_为0， 前面已将ObDatum反序列化, 不需要再做其他处理
      } else {
        // TODO: longzhong.wlz 之前说这里有兼容性问题，后续一并处理，暂时先这样
        MEMCPY(expr_datum_buf, buf + pos, expr_datum_size);
        expr_datum->ptr_ = expr_datum_buf;
        pos += expr_datum_size;
        des_mem_size += expr_datum_size;
        expr_datum_buf += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && des_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret),
      K(des_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

template <bool DESERIALIZE_PLAN_PART>
int ObPxTreeSerializer::deserialize_expr_frame_info(const char *buf,
                                       int64_t data_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  int32_t expr_cnt = 0;
  if (DESERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = 0;
    OB_UNIS_DECODE(need_ctx_cnt);
    const_cast<ObExprFrameInfo &>(expr_frame_info).need_ctx_cnt_ = need_ctx_cnt;
  }
  // deserialize expr extra info.
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_SUCC(ret) && OB_FAIL(expr_info.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize expr extra info", K(ret));
  }

  if (DESERIALIZE_PLAN_PART) {
    ObIArray<ObExpr> &exprs = const_cast<ObArray<ObExpr> &>(expr_frame_info.rt_exprs_);
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (OB_FAIL(exprs.prepare_allocate(expr_cnt))) {
      LOG_WARN("failed to prepare allocator expr", K(ret));
    } else {
      for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
        ObExpr &expr = exprs.at(i);
        if (OB_FAIL(expr.deserialize(buf, data_len, pos))) {
          LOG_WARN("failed to serialize expr", K(ret));
        }
      }
    }
  }

  // frames
  int64_t frame_cnt = 0;
  char **frames = nullptr;
  const ObIArray<char*> *param_frame_ptrs = &plan_ctx->get_param_frame_ptrs();
  OB_UNIS_DECODE(frame_cnt);
  ObIArray<char *> *const_char_ptrs = DESERIALIZE_PLAN_PART ?
                              &(const_cast<ObExprFrameInfo &>(expr_frame_info)).const_frame_ptrs_ :
                              NULL;
  if (OB_FAIL(ret)) {
  } else if (nullptr == (frames = static_cast<char**>(
              ctx.get_allocator().alloc(sizeof(char*) * frame_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate frames", K(ret));
  } else if (FALSE_IT(MEMSET(frames, 0, sizeof(char*) * frame_cnt))) {
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(), expr_frame_info.const_frame_,
      const_char_ptrs, frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(), expr_frame_info.param_frame_,
      const_cast<ObIArray<char*>*>(param_frame_ptrs), frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(),expr_frame_info.dynamic_frame_,
      nullptr, frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(),expr_frame_info.datum_frame_,
      nullptr, frames, frame_cnt, true))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else {
    ctx.set_frames(frames);
    ctx.set_frame_cnt(frame_cnt);
    // init const vector
    ObEvalCtx eval_ctx(ctx);
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      const ObExpr &expr = exprs.at(i);
      if (expr.is_const_expr()
          && UINT32_MAX != expr.vector_header_off_
          && T_OP_ROW != expr.type_) {
        ret = expr.init_vector(eval_ctx, VEC_UNIFORM_CONST, 1/*size*/);
      }
    }
  }
  return ret;
}

template <bool SERIALIZE_PLAN_PART>
int64_t ObPxTreeSerializer::get_serialize_frame_info_size(
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       char **frames,
                                       int64_t frame_cnt,
                                       bool no_ser_data/* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t need_extra_mem_size = 0;
  int64_t item_size = 0;
  if ((all_frames.count() > 0) && all_frames.at(0).use_rich_format_) {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
  } else {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  if (SERIALIZE_PLAN_PART) {
    OB_UNIS_ADD_LEN(all_frames.count());
    for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
      OB_UNIS_ADD_LEN(all_frames.at(i));
    }
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * item_size;
      OB_UNIS_ADD_LEN(expr_mem_size);
      len += expr_mem_size;
      char *frame_buf = frames[frame_info.frame_idx_];
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                  (frame_buf + j * item_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ADD_LEN(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ADD_LEN(expr_datum_size);
      if (0 < expr_datum_size) {
        // 这里可能有兼容性问题，暂时这样，后面看着改
        len += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    LOG_ERROR("unexpected status: serialize size is not match", K(ret),
      K(ser_mem_size), K(need_extra_mem_size));
  }
  return len;
}

template <bool SERIALIZE_PLAN_PART>
int64_t ObPxTreeSerializer::get_serialize_expr_frame_info_size(
  ObExecContext &ctx,
  const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (SERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = expr_frame_info.need_ctx_cnt_;
    OB_UNIS_ADD_LEN(need_ctx_cnt);
  }
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  len += expr_info.get_serialize_size();

  if (SERIALIZE_PLAN_PART) {
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    int32_t expr_cnt = expr_frame_info.is_mark_serialize()
        ? expr_frame_info.ser_expr_marks_.count()
        : exprs.count();
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    len += serialization::encoded_length_i32(expr_cnt);
    if (!expr_frame_info.is_mark_serialize()) {
      for (int64_t i = 0; i < expr_cnt; ++i) {
        len += exprs.at(i).get_serialize_size();
      }
    } else {
      for (int64_t i = 0; i < expr_cnt; ++i) {
        if (expr_frame_info.ser_expr_marks_.at(i)) {
          len += exprs.at(i).get_serialize_size();
        } else {
          len += ObEmptyExpr::instance().get_serialize_size();
        }
      }
    }
  }
  int64_t frame_count = 0;
  char **frames = nullptr;
  if (ctx.get_ori_frame_cnt() != 0 && ctx.get_ori_frames() != nullptr) {
    frame_count = ctx.get_ori_frame_cnt();
    frames = ctx.get_ori_frames();
  } else {
    frame_count = ctx.get_frame_cnt();
    frames = ctx.get_frames();
  }
  OB_UNIS_ADD_LEN(frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.const_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.param_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.dynamic_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.datum_frame_, frames, frame_count, true);
  LOG_DEBUG("trace end get ser expr frame info size", K(ret), K(len));
  return len;
}

/* Previously, QC set sqc.is_fulltree_ and serialize it.
 * QC find all dfos which contain QC and mark sqcs in this and all descedant dfos as fulltree.
 * The problem is that all child dfos of the dfo containing QC will be marked as full tree, including
 * dfos that are not below this nested QC.
 * Now we ignore sqc.is_fulltree_ and set is_fulltree = true when meet a QC.
*/
int ObPxTreeSerializer::serialize_tree(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       const ObOpSpec &root,
                                       bool is_fulltree,
                                       const ObAddr &run_svr,
                                       ObPhyOpSeriCtx *seri_ctx)
{
  int ret = OB_SUCCESS;
  is_fulltree = is_fulltree || IS_PX_COORD(root.type_);
  int32_t child_cnt = (!is_fulltree && IS_RECEIVE(root.type_)) ? 0 : root.get_child_cnt();
  if (OB_FAIL(common::check_stack_overflow())) {
    LOG_WARN("failed to check_stack_overflow");
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, root.type_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL((seri_ctx == NULL ? root.serialize(buf, buf_len, pos) :
      root.serialize(buf, buf_len, pos, *seri_ctx)))) {
    ObCStringHelper helper;
    LOG_WARN("fail to serialize root", K(ret), "type", root.type_, "root", helper.convert(root));
  } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.type_)
             && OB_FAIL(serialize_sub_plan(buf, buf_len, pos, root))) {
    LOG_WARN("fail to serialize sub plan", K(ret));
  }
  if (OB_SUCC(ret)
      && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_1_0_0
      && root.is_table_scan()
      && static_cast<const ObTableScanSpec&>(root).is_global_index_back()) {
    bool is_same_zone = false;
    if (OB_ISNULL(GCTX.locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret), K(GCTX.locality_manager_));
    } else if (OB_FAIL(GCTX.locality_manager_->is_same_zone(run_svr, is_same_zone))) {
      LOG_WARN("check same zone failed", K(ret), K(run_svr));
    } else if (!is_same_zone) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cross-zone global index lookup during 4.0 upgrade");
    }
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    const ObOpSpec *child_op = root.get_child(i);
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child operator", K(i), K(root.type_));
    } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *child_op, is_fulltree, run_svr, seri_ctx))) {
      LOG_WARN("fail to serialize tree", K(ret));
    }
  }
  LOG_DEBUG("end trace serialize tree", K(pos), K(buf_len));
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(const char *buf,
                                         int64_t data_len,
                                         int64_t &pos,
                                         ObPhysicalPlan &phy_plan,
                                         ObOpSpec *&root,
                                         ObIArray<const ObTableScanSpec *> &tsc_ops)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  uint32_t child_cnt = 0;
  ObOpSpec *op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator", K(phy_operator_type),
              "type_str", ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)), K(pos));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_op_spec(
        static_cast<ObPhyOperatorType>(phy_operator_type), child_cnt, op, 0))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else {
      if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type, K(op->id_));
      } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->type_)
                 && OB_FAIL(deserialize_sub_plan(buf, data_len, pos, phy_plan, op))) {
        LOG_WARN("fail to deserialize sub plan", K(ret));
      } else {
        LOG_DEBUG("deserialize phy operator",
                  K(op->type_), "serialize_size",
                  op->get_serialize_size(), "data_len",
                  data_len, "pos",
                  pos, "takes", data_len-pos);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ((op->type_ <= PHY_INVALID || op->type_ >= PHY_END)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid operator type", N_TYPE, op->type_);
    } else if (PHY_TABLE_SCAN == op->type_) {
      ObTableScanSpec *tsc_spec = static_cast<ObTableScanSpec *>(op);
      if (OB_FAIL(tsc_ops.push_back(tsc_spec))) {
        LOG_WARN("Failed to push back table scan operator", K(ret));
      }
    }
  }

  // Terminate serialization when meet ObReceive, as this op indicates
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
      ObOpSpec *child = NULL;
      if (OB_FAIL(deserialize_tree(buf, data_len, pos, phy_plan, child, tsc_ops))) {
        LOG_WARN("fail to deserialize tree", K(ret));
      } else if (OB_FAIL(op->set_child(i, child))) {
        LOG_WARN("fail to set child", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    root = op;
  }
  return ret;
}

// 用于全文索引回表和全局索引回表使用，场景为op内含有子计划。
// 序列化反序列化的时候需要一同序列化/反序列化
int ObPxTreeSerializer::deserialize_sub_plan(const char *buf,
                                             int64_t data_len,
                                             int64_t &pos,
                                             ObPhysicalPlan &phy_plan,
                                             ObOpSpec *&op)
{
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  UNUSED(op);
  UNUSED(phy_plan);
  int ret = OB_SUCCESS;
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->get_type()) {
    // FIXME: TODO: 完善TableScanWithIdexBack op
    // ObOpSpec *index_scan_tree = nullptr;
    // ObTableScanWithIndexBack *table_scan = static_cast<ObTableScanWithIndexBack*>(op);
    // if (OB_FAIL(deserialize_op(buf, data_len, pos, phy_plan, index_scan_tree))) {
    //   LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
    // } else {
    //   table_scan->set_index_scan_tree(index_scan_tree);
    // }
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "deserialize TableScanWithIdexBack");
  }
  return ret;
}


int ObPxTreeSerializer::deserialize_op(const char *buf,
                                       int64_t data_len,
                                       int64_t &pos,
                                       ObPhysicalPlan &phy_plan,
                                       ObOpSpec *&root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  uint32_t child_cnt = 0;
  ObOpSpec *op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator", K(phy_operator_type),
              "type_str",
              ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)), K(pos));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_op_spec(static_cast<ObPhyOperatorType>(phy_operator_type),
        child_cnt, op, 0))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
    } else {
      LOG_DEBUG("deserialize phy operator",
                K(op->get_type()), "serialize_size",
                op->get_serialize_size(), "data_len",
                data_len, "pos",
                pos, "takes", data_len-pos);
      root = op;
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(const char *buf,
                                         int64_t data_len,
                                         int64_t &pos,
                                         ObPhysicalPlan &phy_plan,
                                         ObOpSpec *&root)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScanSpec*, 8> tsc_ops;
  ret = deserialize_tree(buf, data_len, pos, phy_plan, root, tsc_ops);
  return ret;
}

// 用于全文索引回表和全局索引回表使用，场景为op内含有子计划。
// 序列化反序列化的时候需要一同序列化/反序列化
int64_t ObPxTreeSerializer::get_tree_serialize_size(const ObOpSpec &root, bool is_fulltree,
    ObPhyOpSeriCtx *seri_ctx)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  is_fulltree = is_fulltree || IS_PX_COORD(root.type_);
  int32_t child_cnt = (!is_fulltree && IS_RECEIVE(root.type_)) ? 0 : root.get_child_cnt();
  len += serialization::encoded_length_vi32(root.get_type());
  len += serialization::encoded_length(child_cnt);
  len += (NULL == seri_ctx ? root.get_serialize_size() :
         root.get_serialize_size(*seri_ctx));
  len += get_sub_plan_serialize_size(root);

  // Terminate serialization when meet ObReceive, as this op indicates
  for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    const ObOpSpec *child_op = root.get_child(i);
    if (OB_ISNULL(child_op)) {
      // ignore ret
      // 这里无法抛出错误，不过在serialize阶段会再次检测是否有null child。
      // 所以是安全的
      LOG_ERROR("null child op", K(i), K(root.get_child_cnt()), K(root.get_type()));
    } else {
      len += get_tree_serialize_size(*child_op, is_fulltree, seri_ctx);
    }
  }
  return len;
}

// 用于全文索引回表和全局索引回表使用，场景为op内含有子计划。
// 序列化反序列化的时候需要一同序列化/反序列化
int ObPxTreeSerializer::serialize_sub_plan(char *buf,
                                           int64_t buf_len,
                                           int64_t &pos,
                                           const ObOpSpec &root)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    // FIXME: TODO:
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "deserialize TableScanWithIdexBack");

  }
  return ret;
}

int64_t ObPxTreeSerializer::get_sub_plan_serialize_size(const ObOpSpec &root)
{
  int64_t len = 0;
  // some op contain inner mini plan
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
  }
  return len;
}

// 这里说明下，任务每个ObOpSpen序列化都是一样，所以这里没有采用递归处理
// 而是采用扁平化处理，本质上是ok的，如果有特殊逻辑需要这里改下，如设置某些变量等
int ObPxTreeSerializer::deserialize_op_input(
  const char *buf,
  int64_t data_len,
  int64_t &pos,
  ObOpKitStore &op_kit_store)
{
  int ret = OB_SUCCESS;
  int32_t real_input_count = 0;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &real_input_count))) {
    LOG_WARN("decode int32_t", K(ret), K(data_len), K(pos));
  }
  ObOperatorKit *kit = nullptr;
  ObPhyOperatorType phy_op_type;
  int64_t tmp_phy_op_type = 0;
  int64_t index = 0;
  int64_t ser_input_cnt = 0;
  for (int32_t i = 0; OB_SUCC(ret) && i < real_input_count; ++i) {
    OB_UNIS_DECODE(index);
    OB_UNIS_DECODE(tmp_phy_op_type);
    phy_op_type = static_cast<ObPhyOperatorType>(tmp_phy_op_type);
    kit = op_kit_store.get_operator_kit(index);
    if (nullptr == kit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op input is NULL", K(ret), K(index), K(phy_op_type), K(kit), K(real_input_count));
    } else if (nullptr == kit->input_) {
      LOG_WARN("op input is NULL", K(ret), K(index), K(phy_op_type), K(real_input_count));
    } else if (phy_op_type != kit->input_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the type of op input is not match", K(ret), K(index),
        K(phy_op_type), K(kit->input_->get_type()));
    } else {
      OB_UNIS_DECODE(*kit->input_);
      ++ser_input_cnt;
    }
  }
  if (OB_SUCC(ret) && ser_input_cnt != real_input_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ser input is not match", K(ret),
      K(ser_input_cnt), K(real_input_count));
  }
  return ret;
}

int64_t ObPxTreeSerializer::get_serialize_op_input_size(
  const ObOpSpec &op_spec, ObOpKitStore &op_kit_store)
{
  int64_t len = 0;
  int32_t real_input_count = 0;
  len += serialization::encoded_length_i32(real_input_count);
  len += get_serialize_op_input_tree_size(op_spec, op_kit_store, false /* is_fulltree */);
  return len;
}


int64_t ObPxTreeSerializer::get_serialize_op_input_subplan_size(
  const ObOpSpec &op_spec,
  ObOpKitStore &op_kit_store,
  bool is_fulltree)
{
  int ret = OB_SUCCESS;
  UNUSED(is_fulltree);
  UNUSED(op_spec);
  UNUSED(op_kit_store);
  int64_t len = 0;
  if (OB_SUCC(ret) && PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_) {
    // do nothing
  }
  return len;
}

int64_t ObPxTreeSerializer::get_serialize_op_input_tree_size(
  const ObOpSpec &op_spec,
  ObOpKitStore &op_kit_store,
  bool is_fulltree)
{
  int ret = OB_SUCCESS;
  ObOpInput *op_spec_input = NULL;
  int64_t index = op_spec.id_;
  ObOperatorKit *kit = op_kit_store.get_operator_kit(index);
  int64_t len = 0;
  if (OB_FAIL(common::check_stack_overflow())) {
    LOG_ERROR("failed to check_stack_overflow");
    len = 0;
  } else if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op input is NULL", K(ret), K(index));
  } else if (nullptr != (op_spec_input = kit->input_)) {
    OB_UNIS_ADD_LEN(index); //serialize index
    OB_UNIS_ADD_LEN(static_cast<int64_t>(op_spec_input->get_type())); //serialize operator type
    OB_UNIS_ADD_LEN(*op_spec_input); //serialize input parameter
  }
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_) {
    len += get_serialize_op_input_subplan_size(op_spec, op_kit_store, true/*is_fulltree*/);
    // do nothing
  }
  is_fulltree = is_fulltree || IS_PX_COORD(op_spec.type_);
  // Terminate serialization when meet ObReceive, as this op indicates
  bool serialize_child = is_fulltree || (!IS_RECEIVE(op_spec.type_));
  if (OB_SUCC(ret) && serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op_spec.get_child_cnt(); ++i) {
      const ObOpSpec *child_op = op_spec.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null child operator", K(i), K(op_spec.type_));
      } else {
        len += get_serialize_op_input_tree_size(*child_op, op_kit_store, is_fulltree);
      }
    }
  }
  return len;
}

int ObPxTreeSerializer::serialize_op_input_subplan(
  char *buf,
  int64_t buf_len,
  int64_t &pos,
  const ObOpSpec &op_spec,
  ObOpKitStore &op_kit_store,
  bool is_fulltree,
  int32_t &real_input_count)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  UNUSED(op_spec);
  UNUSED(op_kit_store);
  UNUSED(is_fulltree);
  UNUSED(real_input_count);
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_) {
    // do nothing
  }
  return ret;
}

int ObPxTreeSerializer::serialize_op_input_tree(
  char *buf,
  int64_t buf_len,
  int64_t &pos,
  const ObOpSpec &op_spec,
  ObOpKitStore &op_kit_store,
  bool is_fulltree,
  int32_t &real_input_count)
{
  int ret = OB_SUCCESS;
  ObOpInput *op_spec_input = NULL;
  int64_t index = op_spec.id_;
  ObOperatorKit *kit = op_kit_store.get_operator_kit(index);
  if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op input is NULL", K(ret), K(index));
  } else if (nullptr != (op_spec_input = kit->input_)) {
    OB_UNIS_ENCODE(index); //serialize index
    OB_UNIS_ENCODE(static_cast<int64_t>(op_spec_input->get_type())); //serialize operator type
    OB_UNIS_ENCODE(*op_spec_input); //serialize input parameter
    if (OB_SUCC(ret)) {
      ++real_input_count;
    }
  }
  if (OB_SUCC(ret) && (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_)
             && OB_FAIL(serialize_op_input_subplan(
               buf, buf_len, pos, op_spec, op_kit_store, true/*is_fulltree*/, real_input_count))) {
    LOG_WARN("fail to serialize sub plan", K(ret));
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  is_fulltree = is_fulltree || IS_PX_COORD(op_spec.type_);
  bool serialize_child = is_fulltree || (!IS_RECEIVE(op_spec.type_));
  if (OB_SUCC(ret) && serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op_spec.get_child_cnt(); ++i) {
      const ObOpSpec *child_op = op_spec.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(op_spec.type_));
      } else if (OB_FAIL(serialize_op_input_tree(
          buf, buf_len, pos, *child_op, op_kit_store, is_fulltree, real_input_count))) {
        LOG_WARN("fail to serialize tree", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTreeSerializer::serialize_op_input(
  char *buf,
  int64_t buf_len,
  int64_t &pos,
  const ObOpSpec &op_spec,
  ObOpKitStore &op_kit_store)
{
  int ret = OB_SUCCESS;
  int64_t input_start_pos = pos;
  int32_t real_input_count = 0;
  pos += serialization::encoded_length_i32(real_input_count);
  // 这里不序列化input，后面通过copy方式进行，因为反序列化依赖的input没有，这里与老的方式不同
  if (OB_FAIL(serialize_op_input_tree(
      buf, buf_len, pos, op_spec, op_kit_store, false /* is_fulltree */, real_input_count))) {
    LOG_WARN("failed to serialize spec tree", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i32(buf, buf_len, input_start_pos, real_input_count))) {
      LOG_WARN("encode int32_t", K(buf_len), K(input_start_pos), K(real_input_count));
    }
    LOG_TRACE("trace end ser input cnt", K(ret), K(real_input_count), K(op_kit_store.size_),
      K(pos), K(input_start_pos));
  }
  LOG_DEBUG("end trace ser kit store", K(buf_len), K(pos));
  return ret;
}

//------ end serialize plan tree for engine3.0

// first out timestamp of channel is the first in timestamp of receive
// last out timestamp of channel is the last in timestamp of receive
int ObPxChannelUtil::set_receive_metric(
  ObIArray<ObDtlChannel*> &channels,
  ObOpMetric &metric)
{
  int ret = OB_SUCCESS;
  int64_t first_in_ts = INT64_MAX;
  int64_t last_in_ts = 0;
  for (int64_t nth_ch = 0; nth_ch < channels.count(); ++nth_ch) {
    ObDtlChannel *channel = channels.at(nth_ch);
    ObOpMetric &ch_metric = channel->get_op_metric();
    if (first_in_ts > ch_metric.get_first_out_ts()) {
      first_in_ts = ch_metric.get_first_out_ts();
    }
    if (last_in_ts < ch_metric.get_last_out_ts()) {
      last_in_ts = ch_metric.get_last_out_ts();
    }
  }
  if (INT64_MAX != first_in_ts) {
    metric.set_first_in_ts(first_in_ts);
  }
  if (0 != last_in_ts) {
    metric.set_last_in_ts(last_in_ts);
  }
  return ret;
}

// transmit is same as channel
// eg: first in timestamp of channel is same as first in timestamp of transmit
int ObPxChannelUtil::set_transmit_metric(
  ObIArray<ObDtlChannel*> &channels,
  ObOpMetric &metric)
{
  int ret = OB_SUCCESS;
  int64_t first_in_ts = INT64_MAX;
  int64_t first_out_ts = INT64_MAX;
  int64_t last_in_ts = 0;
  int64_t last_out_ts = 0;
  for (int64_t nth_ch = 0; nth_ch < channels.count(); ++nth_ch) {
    ObDtlChannel *channel = channels.at(nth_ch);
    ObOpMetric &ch_metric = channel->get_op_metric();
    if (first_in_ts > ch_metric.get_first_in_ts()) {
      first_in_ts = ch_metric.get_first_in_ts();
    }
    if (first_out_ts > ch_metric.get_first_out_ts()) {
      first_out_ts = ch_metric.get_first_out_ts();
    }
    if (last_in_ts < ch_metric.get_last_in_ts()) {
      last_in_ts = ch_metric.get_last_in_ts();
    }
    if (last_out_ts < ch_metric.get_last_out_ts()) {
      last_out_ts = ch_metric.get_last_out_ts();
    }
  }
  if (INT64_MAX != first_in_ts) {
    metric.set_first_in_ts(first_in_ts);
  }
  if (INT64_MAX != first_out_ts) {
    metric.set_first_out_ts(first_out_ts);
  }
  if (0 != last_in_ts) {
    metric.set_last_in_ts(last_in_ts);
  }
  if (0 != last_out_ts) {
    metric.set_last_out_ts(last_out_ts);
  }
  return ret;
}

int ObPxChannelUtil::unlink_ch_set(dtl::ObDtlChSet &ch_set, ObDtlFlowControl *dfc, bool batch_free_ch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  /* ignore error, try to relase all channels */
  if (nullptr == dfc) {
    for (int64_t idx = 0; idx < ch_set.count(); ++idx) {
      if (OB_SUCCESS != (tmp_ret = ch_set.get_channel_info(idx, ci))) {
        ret = tmp_ret;
        LOG_ERROR("fail get channel info", K(idx), K(ret));
      }
      if (OB_SUCCESS != (tmp_ret = dtl::ObDtlChannelGroup::unlink_channel(ci))) {
        ret = tmp_ret;
        LOG_WARN("fail unlink channel", K(ci), K(ret));
      }
    }
  } else {
    // 这里将channel从dtl service中移除和释放分开，主要是为了解决rpc processor和px worker会并发拿channel，进行不同处理问题
    // 如果不移除，rpc processor会继续插入msg，这个时候对channel的清理操作完，还是会有msg来，这样channel就不是一个干净的msg，很多统计无法进行
    // 所以先移除，后清理再释放，这样就不会有并发导致channel还会收msg问题
    ObSEArray<ObDtlChannel*, 16> chans;
    ObDtlChannel *ch = nullptr;
    ObDtlChannel *first_ch = nullptr;
    for (int64_t idx = 0; idx < ch_set.count(); ++idx) {
      if (OB_SUCCESS != (tmp_ret = ch_set.get_channel_info(idx, ci))) {
        ret = tmp_ret;
        LOG_ERROR("fail get channel info", K(idx), K(ret));
      }
      if (OB_SUCCESS != (tmp_ret = dtl::ObDtlChannelGroup::remove_channel(ci, ch))) {
        ret = tmp_ret;
        LOG_WARN("fail unlink channel", K(ci), K(ret));
      } else if (0 == idx) {
        first_ch = ch;
      }
      if (OB_SUCCESS != tmp_ret) {
        // do nothing
      } else if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channel is null", K(ci), K(ret));
      } else if (OB_SUCCESS != (tmp_ret = chans.push_back(ch))) {
        ret = tmp_ret;
        // 如果push back失败了，只能一个一个channel来处理
        if (OB_SUCCESS != (tmp_ret = DTL.get_dfc_server().unregister_dfc_channel(*dfc, ch))) {
          ret = tmp_ret;
          LOG_ERROR("failed to unregister dfc channel", K(ci), K(ret), K(ret));
        }
        if (batch_free_ch) {
          if (NULL != ch) {
            ch->~ObDtlChannel();
            ch = NULL;
          }
        } else {
          ob_delete(ch);
        }
        LOG_WARN("failed to push back channels", K(ci), K(ret));
      }
    }
    if (OB_SUCCESS != (tmp_ret = DTL.get_dfc_server().deregister_dfc(*dfc))) {
      ret = tmp_ret;
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail deregister all channel from dfc server", KR(tmp_ret));
    }
    if (0 < chans.count()) {
      for (int64_t idx = chans.count() - 1; 0 <= idx; --idx) {
        if (OB_SUCCESS != (tmp_ret = chans.pop_back(ch))) {
          ret = tmp_ret;
          LOG_ERROR("failed to unregister channel", K(ret));
        } else if (batch_free_ch) {
          if (NULL != ch) {
            ch->~ObDtlChannel();
            ch = NULL;
          }
        } else {
          ob_delete(ch);
        }
      }
    }
    if (batch_free_ch && nullptr != first_ch) {
      ob_free((void *)first_ch);
    }
  }
  return ret;
}


int ObPxChannelUtil::flush_rows(common::ObIArray<dtl::ObDtlChannel*> &channels)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel *ch = NULL;
  /* ignore error, try to flush all channels */
  for (int64_t slice_idx = 0; slice_idx < channels.count(); ++slice_idx) {
    if (NULL == (ch = channels.at(slice_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected NULL ptr", K(ret));
    } else {
      if (OB_FAIL(ch->flush())) {
        LOG_WARN("Fail to flush row to slice channel."
                 "The peer side may not set up as expected.",
                 K(slice_idx), "peer", ch->get_peer(), K(ret));
      }
    }
  }
  return ret;
}

int ObPxChannelUtil::dtl_channles_asyn_wait(common::ObIArray<dtl::ObDtlChannel*> &channels, bool ignore_error)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(channels, idx, cnt, OB_SUCC(ret)) {
    ObDtlChannel *ch = channels.at(idx);
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel is null", K(ret), K(idx));
    } else if (OB_FAIL(ch->flush())) {
      if (ignore_error) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to wait for channel", K(ret), K(idx),
          "peer", ch->get_peer());
      }
    }
  }
  return ret;
}

int ObPxChannelUtil::sqcs_channles_asyn_wait(ObIArray<ObPxSqcMeta> &sqcs)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObDtlChannel *ch = sqcs.at(idx).get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel is null", K(ret), K(idx));
    } else if (OB_FAIL(ch->flush())) {
      LOG_WARN("failed to wait for channel", K(ret), K(idx),
          "peer", ch->get_peer());
    }
  }
  return ret;
}

int ObPxAffinityByRandom::add_partition(int64_t tablet_id,
                                        int64_t tablet_idx,
                                        int64_t worker_cnt,
                                        uint64_t tenant_id,
                                        ObPxTabletInfo &tablet_row_info)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("add partition", K(tablet_id), K(tablet_idx), K(worker_cnt), K(this), K(order_partitions_));
  if (0 >= worker_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The worker cnt is invalid", K(ret), K(worker_cnt));
  } else {
    TabletHashValue tablet_hash_value;
    if (order_partitions_) {
      tablet_hash_value.hash_value_ = 0;
    } else {
      uint64_t value = (tenant_id << 32 | tablet_idx);
      tablet_hash_value.hash_value_ = common::murmurhash(&value, sizeof(value), worker_cnt);
    }
    tablet_hash_value.tablet_idx_ = tablet_idx;
    tablet_hash_value.tablet_id_ = tablet_id;
    tablet_hash_value.tablet_info_ = tablet_row_info;
    worker_cnt_ = worker_cnt;
    if (OB_FAIL(tablet_hash_values_.push_back(tablet_hash_value))) {
      LOG_WARN("Failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObPxAffinityByRandom::do_random(bool use_partition_info, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_hash_values_.empty()) {
    /**
     * 为什么需要保持相同的worker里面涉及的partition的序呢？
     * 因为在global order的情况下，在qc端已经对parition进行
     * 了排序，如果partition在单个worker里面的序乱了，则不符合
     * global order的预期，也就是单个worker里面的数据无序了。
     */
    bool asc_order = true;
    int array_order_now = 0; // -1: desc, 1: asc, 0: random
    if (tablet_hash_values_.count() > 1
        && (tablet_hash_values_.at(0).tablet_idx_ > tablet_hash_values_.at(1).tablet_idx_)) {
      asc_order = false;
    }

    if (use_partition_info) { // if version < 4.4.1, use_partition_info alwary be false
      /*
         1. sort tablet by load desc
         2. assign tablet with zigzag round-robin
      */
      lib::ob_sort(tablet_hash_values_.begin(), tablet_hash_values_.end(),
                   TabletHashValue::cmp_by_row_count);
      LOG_TRACE("Dop Assign]after sort partition_hash_values by row count", K(tablet_hash_values_));
      int64_t worker_id = 0;
      int direction = 1;
      ARRAY_FOREACH(tablet_hash_values_, idx) {
        tablet_hash_values_.at(idx).worker_id_ = worker_id;
        worker_id += direction;
        if (worker_id == worker_cnt_ || worker_id == -1) {
          // reach zigzag bound, reverse
          direction = -direction;
          worker_id += direction;
        }
      }
    } else {
      if (order_partitions_) {
      // in partition wise affinity scenario, partition_idx of a pair of partitions may be different.
      // for example, T1 consists of p0, p1, p2 and T2 consists of p1, p2
      // T1.p1 <===> T2.p1  and T1.p2 <===> T2.p2
      // The partition_idx of T1.p1 is 1 and the partition_idx of T2.p1 is 0.
      // If we calculate hash value of partition_idx and sort partitions by the hash value,
      // T1.p1 and T2.p1 may be assigned to different worker.
      // So we sort partitions by partition_idx and generate a relative_idx which starts from zero.
      // Then calculate hash value with the relative_idx
        lib::ob_sort(tablet_hash_values_.begin(), tablet_hash_values_.end(),
                     TabletHashValue::desc_order_cmp);
        array_order_now = -1;
        if (partition_random_affinitize_) {// only random scene need hash value
          int64_t relative_idx = 0;
          for (int64_t i = 0; i < tablet_hash_values_.count(); i++) {
            uint64_t value = ((tenant_id << 32) | relative_idx);
            tablet_hash_values_.at(i).hash_value_ =
                common::murmurhash(&value, sizeof(value), worker_cnt_);
            relative_idx++;
          }
        }
      }
      if (partition_random_affinitize_) {
        lib::ob_sort(tablet_hash_values_.begin(), tablet_hash_values_.end(),
                     TabletHashValue::cmp_by_hash_value);
        array_order_now = 0;
      }
      // assign tablet to worker with round-robin
      int64_t worker_id = 0;
      for (int64_t idx = 0; idx < tablet_hash_values_.count(); ++idx) {
        if (worker_id > worker_cnt_ - 1) {
          worker_id = 0;
        }
        tablet_hash_values_.at(idx).worker_id_ = worker_id++;
      }
    }

    // keep global order
    if (asc_order && array_order_now != 1) {
      lib::ob_sort(tablet_hash_values_.begin(), tablet_hash_values_.end(),
                   TabletHashValue::asc_order_cmp);
    } else if (!asc_order && array_order_now != -1) {
      lib::ob_sort(tablet_hash_values_.begin(), tablet_hash_values_.end(),
                   TabletHashValue::desc_order_cmp);
    }
  }
  return ret;
}

int ObPxAffinityByRandom::get_tablet_info(int64_t tablet_id, ObIArray<ObPxTabletInfo> &tablets_info, ObPxTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  bool find = false;
  if (!tablets_info.empty()) {
    ARRAY_FOREACH(tablets_info, idx) {
      if (tablets_info.at(idx).tablet_id_ == tablet_id) {
        find = true;
        tablet_info.assign(tablets_info.at(idx));
      }
    }
    if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to find partition in tablets_info", K(ret), K(tablet_id), K(tablets_info));
    }
  }
  return ret;
}

double ObPxSqcUtil::get_sqc_partition_ratio(ObExecContext *exec_ctx)
{
  double ratio = 1.0;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t total_part_cnt = sqc_args.sqc_.get_total_part_count();
    int64_t sqc_part_cnt = sqc_args.sqc_.get_access_table_locations().count();
    if (0 < total_part_cnt && 0 < sqc_part_cnt) {
      ratio = sqc_part_cnt * 1.0 / total_part_cnt;
    }
    LOG_TRACE("trace sqc partition ratio", K(total_part_cnt), K(sqc_part_cnt));
  }
  return ratio;
}

double ObPxSqcUtil::get_sqc_est_worker_ratio(ObExecContext *exec_ctx)
{
  double ratio = 1.0;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t est_total_task_cnt = sqc_args.sqc_.get_total_task_count();
    int64_t est_sqc_task_cnt = sqc_args.sqc_.get_max_task_count();
    if (0 < est_total_task_cnt && 0 < est_sqc_task_cnt) {
      ratio = est_sqc_task_cnt * 1.0 / est_total_task_cnt;
    }
    LOG_TRACE("trace sqc estimate worker ratio", K(est_sqc_task_cnt), K(est_total_task_cnt));
  }
  return ratio;
}

int64_t ObPxSqcUtil::get_est_total_worker_count(ObExecContext *exec_ctx)
{
  int64_t worker_cnt = 1;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    worker_cnt = sqc_args.sqc_.get_total_task_count();
  }
  return worker_cnt;
}

int64_t ObPxSqcUtil::get_est_sqc_worker_count(ObExecContext *exec_ctx)
{
  int64_t worker_cnt = 1;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    worker_cnt = sqc_args.sqc_.get_max_task_count();
  }
  return worker_cnt;
}

int64_t ObPxSqcUtil::get_total_partition_count(ObExecContext *exec_ctx)
{
  int64_t total_part_cnt = 1;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t tmp_total_part_cnt = sqc_args.sqc_.get_total_part_count();
    if (0 < tmp_total_part_cnt) {
      total_part_cnt = tmp_total_part_cnt;
    }
  }
  return total_part_cnt;
}

// SQC that belongs to leaf dfo should estimate size by GI or Tablescan
int64_t ObPxSqcUtil::get_actual_worker_count(ObExecContext *exec_ctx)
{
  int64_t sqc_actual_worker_cnt = 1;
  ObPxSqcHandler *sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    sqc_actual_worker_cnt = sqc_args.sqc_.get_task_count();
  }
  return sqc_actual_worker_cnt;
}

uint64_t ObPxSqcUtil::get_plan_id(ObExecContext *exec_ctx)
{
  uint64_t plan_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    ObPhysicalPlanCtx *plan_ctx = exec_ctx->get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
      plan_id = plan_ctx->get_phy_plan()->get_plan_id();
    }
  }
  return plan_id;
}

const char* ObPxSqcUtil::get_sql_id(ObExecContext *exec_ctx)
{
  const char *sql_id = nullptr;
  if (OB_NOT_NULL(exec_ctx)) {
    ObPhysicalPlanCtx *plan_ctx = exec_ctx->get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
      sql_id = plan_ctx->get_phy_plan()->get_sql_id();
    }
  }
  return sql_id;
}

uint64_t ObPxSqcUtil::get_exec_id(ObExecContext *exec_ctx)
{
  uint64_t exec_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    exec_id = exec_ctx->get_execution_id();
  }
  return exec_id;
}

uint64_t ObPxSqcUtil::get_session_id(ObExecContext *exec_ctx)
{
  uint64_t session_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    if (OB_NOT_NULL(exec_ctx->get_my_session())) {
      session_id = exec_ctx->get_my_session()->get_sessid_for_table();
    }
  }
  return session_id;
}

int ObPxEstimateSizeUtil::get_px_size(
  ObExecContext *exec_ctx,
  const PxOpSizeFactor factor,
  const int64_t total_size,
  int64_t &ret_size)
{
  int ret = OB_SUCCESS;
  int64_t actual_worker = 0;
  double sqc_part_ratio = 0;
  int64_t total_part_cnt = 0;
  double sqc_est_worker_ratio = 0;
  int64_t total_actual_worker_cnt = 0;
  ret_size = total_size;
  if (factor.has_leaf_granule()) {
    // leaf
    if (!factor.has_exchange()) {
      if (factor.has_block_granule()) {
        actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
        sqc_est_worker_ratio = ObPxSqcUtil::get_sqc_est_worker_ratio(exec_ctx);
        ret_size =  sqc_est_worker_ratio * total_size / actual_worker;
      } else if (factor.partition_granule_child_) {
        total_part_cnt = ObPxSqcUtil::get_total_partition_count(exec_ctx);
        ret_size = total_size / total_part_cnt;
      } else if (factor.partition_granule_parent_) {
        // total_size / total_part_cnt * sqc_part_cnt
        actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
        sqc_part_ratio = ObPxSqcUtil::get_sqc_partition_ratio(exec_ctx);
        ret_size = sqc_part_ratio * ret_size / actual_worker;
      }
    } else {
      if (factor.pk_exchange_) {
        if (factor.has_block_granule()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect status: block granule with PK");
        } else {
          // total_size / total_part_cnt * sqc_part_cnt / sqc_autual_worker_count
          actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
          sqc_part_ratio = ObPxSqcUtil::get_sqc_partition_ratio(exec_ctx);
          ret_size = sqc_part_ratio * ret_size / actual_worker;
        }
      }
    }
  } else {
    if (factor.broadcast_exchange_) {
    } else {
      // 这里目前无法根据实际线程数来分配，因为worker启动后就开始执行，QC还没有发送总共的worker数
      // 这里假设都可以拿到
      actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
      sqc_est_worker_ratio = ObPxSqcUtil::get_sqc_est_worker_ratio(exec_ctx);
      ret_size = total_size * sqc_est_worker_ratio / actual_worker;
    }
  }
  if (ret_size > total_size || OB_FAIL(ret)) {
    LOG_WARN("unexpect status: estimate size is greater than total size",
      K(ret_size), K(total_size), K(ret));
    ret_size = total_size;
    ret = OB_SUCCESS;
  }
  if (0 >= ret_size) {
    ret_size = 1;
  }
  LOG_DEBUG("trace get px size", K(ret_size), K(total_size), K(factor), K(actual_worker),
    K(sqc_part_ratio), K(total_part_cnt), K(sqc_est_worker_ratio), K(total_actual_worker_cnt));
  return ret;
}

struct PWAffinityAlignInfo
{
  TO_STRING_KV(K(pw_group_id_), K(start_idx_), K(end_idx_));
  int64_t pw_group_id_{-1};
  // start idx and end idx in px_tablets_info array [start_idx_, end_idx_)
  int64_t start_idx_{0};
  int64_t end_idx_{0};
};

static int prepare_align_info(GroupPWJTabletIdMap *group_pwj_map,
                              const ObIArray<const ObTableScanSpec *> &scan_ops,
                              ObIArray<PWAffinityAlignInfo> &align_infos)
{
  int ret = OB_SUCCESS;
  PWAffinityAlignInfo align_info;
  if (OB_FAIL(align_infos.reserve(scan_ops.count()))) {
    LOG_WARN("failed to reserve");
  }
  GroupPWJTabletIdInfo *tablet_id_info = nullptr;
  for (int64_t i = 0; i < scan_ops.count() && OB_SUCC(ret); ++i) {
    tablet_id_info = group_pwj_map->get(scan_ops.at(i)->get_table_loc_id());
    if (OB_NOT_NULL(tablet_id_info)) {
      align_info.pw_group_id_ = tablet_id_info->group_id_;
    } else {
      align_info.pw_group_id_ = -1;
    }
    if (OB_FAIL(align_infos.push_back(align_info))) {
      LOG_WARN("failed to push back");
    }
  }
  return ret;
}

static int
align_pw_affinity_tablets_info(ObIArray<ObPxTabletInfo> &tablets_info,
                               const ObIArray<PWAffinityAlignInfo> &align_infos)
{
  // for pw affinity gi, the tablets_info is consists as
  // [p0: 1000, p1: 2000, p2: 500, p3: 5000, p0: 3000, p1: 500, p2:1000,  p3: 200]
  //  |___________________________________|  |__________________________________|
  //                   t0                                     t1
  // in order to keep affinity after worker load assignment, we summarize row count of t0 and t1,
  // ensure t0p0 and t1p1 will assign to same worker
  int ret = OB_SUCCESS;
  ObSEArray<ObPxTabletInfo, 8> &se_tablets_info =
      static_cast<ObSEArray<ObPxTabletInfo, 8> &>(tablets_info);

  int64_t max_pw_group = -1;
  for (int64_t i = 0; i < align_infos.count(); ++i) {
    max_pw_group = max(max_pw_group, align_infos.at(i).pw_group_id_);
  }
  LOG_TRACE("[Dop Assign] try align pw affinity tablets info", K(tablets_info.count()), K(max_pw_group));
  // for each pw affinity gi, align tablets info of tsc below it
  ObTMArray<PWAffinityAlignInfo> pw_group;
  for (int64_t cur_group_id = 0; cur_group_id <= max_pw_group && OB_SUCC(ret); ++cur_group_id) {
    int64_t tablet_count = -1;
    pw_group.reset();
    for (int64_t j = 0; j < align_infos.count() && OB_SUCC(ret); ++j) {
      int64_t start_idx = align_infos.at(j).start_idx_;
      int64_t end_idx = align_infos.at(j).end_idx_;
      if (align_infos.at(j).pw_group_id_ != cur_group_id) {
        // not this pw group
      } else if (OB_FAIL(pw_group.push_back(align_infos.at(j)))) {
        LOG_WARN("failed to push back");
      } else {
        int64_t delta = end_idx - start_idx;
        if (tablet_count == -1) {
          tablet_count = delta;
        } else if (OB_UNLIKELY(tablet_count != delta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unmatch partition wise tablet count", K(delta), K(tablet_count));
        }
        if (OB_SUCC(ret)) {
          // reorder each tsc's tablets info by tablet_idx to keep tablets affinity
          oceanbase::lib::ob_sort(se_tablets_info.begin() + start_idx,
                                  se_tablets_info.begin() + end_idx,
                                  ObPxTabletInfo::cmp_by_tablet_idx);
        }
        LOG_TRACE("[Dop Assign] sort pw affinity", K(cur_group_id), K(start_idx), K(end_idx));
      }
    }

    for (int64_t offset = 0; offset < tablet_count && OB_SUCC(ret); ++offset) {
      int64_t pw_affinity_row_count = 0;
      // summarize same tablet row count of a partition wise group
      for (int64_t j = 0; j < pw_group.count() && OB_SUCC(ret); ++j) {
        int64_t array_idx = pw_group.at(j).start_idx_ + offset;
        pw_affinity_row_count += tablets_info.at(array_idx).estimated_row_count_;
      }
      // cover with affinity row count
      for (int64_t j = 0; j < pw_group.count() && OB_SUCC(ret); ++j) {
        int64_t array_idx = pw_group.at(j).start_idx_ + offset;
        tablets_info.at(array_idx).estimated_row_count_ = pw_affinity_row_count;
        LOG_TRACE("[Dop Assign] align pw affinity row count ",
                  K(tablets_info.at(array_idx).tablet_id_),
                  K(tablets_info.at(array_idx).tablet_idx_), K(pw_affinity_row_count));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ForceDopAssignOpt);
int ObPxEstimateSizeUtil::get_statistic_of_dfo_tables(
    int64_t parallel, ObExecContext &ctx, ObDfo &dfo, const ObIArray<ObAddr> &addrs,
    const ObIArray<const ObTableScanSpec *> &scan_ops, ObIArray<ObPxTabletInfo> &px_tablets_info,
    bool &is_opt_stat_valid, bool &send_px_tablets_info_to_sqc)
{
  /*
    In 2 scene we need to get statistic:
    1. sqc dispatch to mulitple observers
    2. for pkey affinity, binding partitions to workers
  */
  int ret = OB_SUCCESS;
  ScopedTimer timer(ObMetricId::GET_STAT_OF_DFO_TABLES);
  static constexpr int64_t max_get_stat_tables = 3;
  bool feature_enable = false;
  bool dop_assign_among_sqc = addrs.count() > 1;
  ObTMArray<PWAffinityAlignInfo> align_infos;
  bool has_affinity_gi = false; // for affinity, send tablet info to sqc to bind pkey channel
  bool has_pw_affinity_gi = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (scan_ops.empty()) {
    // only for scan scene, not support for dml
    LOG_TRACE("[Dop Assign] no scan op");
  } else if (!tenant_config.is_valid() || !tenant_config->_enable_data_based_sqc_dop
             || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
    // config disabled
    LOG_TRACE("[Dop Assign] config disabled");
  } else if (ForceDopAssignOpt) {
    // if tracepoint enabled, force to get statistic of tables
    feature_enable = true;
    dop_assign_among_sqc = true;
    LOG_TRACE("[Dop Assign] enable by tracepoint");
  } else if (scan_ops.count() > max_get_stat_tables) {
    // if too many tables in dfo, we can not sure the the real calculation expense
    LOG_TRACE("[Dop Assign] scan op > 3, disable");
  } else if (parallel <= addrs.count() * 2) {
    // at least 2 workers in one sqc
    LOG_TRACE("[Dop Assign] parallel to small", K(parallel), K(addrs.count()));
  } else {
    feature_enable = true;
  }

  if (!feature_enable) {
  } else if (OB_FAIL(get_affinity_gi_ops(dfo.get_root_op_spec(), has_affinity_gi,
                                         has_pw_affinity_gi))) {
    LOG_WARN("failed to get affinity gi");
  } else if (FALSE_IT(send_px_tablets_info_to_sqc = has_affinity_gi)) {
  } else if (has_pw_affinity_gi && OB_ISNULL(ctx.get_group_pwj_map())) {
    // has partition wise join table, but group_pwj_map is null, do not use stat info
    LOG_TRACE("[Dop Assign] pw_affinity but null group_pwj_map");
  } else if (has_pw_affinity_gi
             && OB_FAIL(prepare_align_info(ctx.get_group_pwj_map(), scan_ops, align_infos))) {
    LOG_WARN("failed to prepare align info");
  } else if (dop_assign_among_sqc || send_px_tablets_info_to_sqc) {
    is_opt_stat_valid = true;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("Failed to get schema guard");
    }
    for (int64_t i = 0; i < scan_ops.count() && OB_SUCC(ret) && is_opt_stat_valid; ++i) {
      const ObTableScanSpec *scan_op = scan_ops.at(i);
      ObDASTableLoc *table_loc = nullptr;
      uint64_t table_location_key = scan_op->get_table_loc_id();
      int64_t start_idx = px_tablets_info.count();
      if (scan_op->is_external_table_) {
        // not support for external table
        is_opt_stat_valid = false;
        break;
      } else if (OB_ISNULL(table_loc = DAS_CTX(ctx).get_table_loc_by_id(
                               table_location_key, scan_op->get_loc_ref_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table loc", K(scan_op->id_));
      } else if (OB_FAIL(prepare_px_tablets_info(ctx, schema_guard, scan_op,
                                                 table_loc->get_tablet_locs(), px_tablets_info,
                                                 is_opt_stat_valid))) {
        LOG_WARN("failed to prepare px tablets info", K(scan_op->id_));
      } else {
        LOG_TRACE("[Dop Assign]get partition info of table", K(scan_op->get_loc_ref_table_id()),
                  K(is_opt_stat_valid));
      }
      if (has_pw_affinity_gi) {
        align_infos.at(i).start_idx_ = start_idx;
        align_infos.at(i).end_idx_ = px_tablets_info.count();
      }
    }
  }

  if (OB_FAIL(ret)) {
  // binding the pw tablet pairs
  } else if (is_opt_stat_valid && has_pw_affinity_gi
             && OB_FAIL(align_pw_affinity_tablets_info(px_tablets_info, align_infos))) {
    LOG_WARN("failed to align pw affinity tablets info");
  }
  return ret;
}

int ObPxEstimateSizeUtil::get_affinity_gi_ops(
    const ObOpSpec *root, bool &has_affinity_gi,
    bool &has_pw_affinity_gi)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr");
  } else if (IS_PX_GI(root->get_type())) {
    const ObGranuleIteratorSpec *gi = static_cast<const ObGranuleIteratorSpec *>(root);
    if (gi->affinitize_) {
      has_affinity_gi = true;
      if (gi->partition_wise_join_) {
        has_pw_affinity_gi = true;
      }
    }
  } else if (IS_PX_RECEIVE(root->get_type())) {
  } else {
    for (int i = 0; i < root->get_child_cnt() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(root->get_child(i))) {
      } else if (OB_FAIL(SMART_CALL(get_affinity_gi_ops(root->get_child(i), has_affinity_gi,
                                                        has_pw_affinity_gi)))) {
        LOG_WARN("failed to check has affinity gi");
      }
    }
  }
  return ret;
}

int ObPxEstimateSizeUtil::prepare_px_tablets_info(ObExecContext &ctx,
                                                  ObSchemaGetterGuard &schema_guard,
                                                  const ObTableScanSpec *scan_op,
                                                  const DASTabletLocList &locations,
                                                  ObIArray<ObPxTabletInfo> &px_tablets_info,
                                                  bool &is_opt_stat_valid)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  int64_t ref_table_id = scan_op->get_loc_ref_table_id();
  const ObTableSchema *table_schema = nullptr;
  ObTabletIdxMap idx_map;
  if (OB_FAIL(px_tablets_info.reserve(px_tablets_info.count() + locations.size()))) {
    LOG_WARN("failed to prepare allocate");
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema))) {
    LOG_WARN("Failed to get table schema", K(tenant_id), K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("null schema, may be deleted", K(tenant_id), K(ref_table_id));
  } else if (OB_FAIL(ObPXServerAddrUtil::build_tablet_idx_map(table_schema, idx_map))) {
    LOG_WARN("fail to build tablet idx map");
  } else if (OB_FAIL(fill_px_tablets_info_with_stat(ctx, scan_op, locations, idx_map,
                                                    px_tablets_info, is_opt_stat_valid))) {
    LOG_WARN("failed to fill px tablets info");
  }
  return ret;
}

int ObPxEstimateSizeUtil::fill_px_tablets_info_with_stat(ObExecContext &ctx,
                                                         const ObTableScanSpec *scan_op,
                                                         const DASTabletLocList &locations,
                                                         const ObTabletIdxMap &idx_map,
                                                         ObIArray<ObPxTabletInfo> &px_tablets_info,
                                                         bool &is_opt_stat_valid)
{
  int ret = OB_SUCCESS;
  // 1. get partition statistic of table
  int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  int64_t ref_table_id = scan_op->get_loc_ref_table_id();
  ObOptTableStat::Key key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = ref_table_id;
  ObPxTabletInfo tablet_info;
  int64_t tablets_info_offset = px_tablets_info.count();
  for (DASTabletLocList::const_iterator iter = locations.begin();
       OB_SUCC(ret) && iter != locations.end(); ++iter) {
    key.partition_id_ = (*iter)->partition_id_;
    tablet_info.tablet_id_ = (*iter)->tablet_id_.id();
    tablet_info.tablet_loc_ = (*iter);
    ObOptTableStat tstat;
    if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(tenant_id, key, tstat))) {
      LOG_WARN("failed to get table stat", K(ret));
    } else if (tstat.get_last_analyzed() <= 0) {
      is_opt_stat_valid = false;
      break;
    } else if (OB_FAIL(idx_map.get_refactored(tablet_info.tablet_id_, tablet_info.tablet_idx_))) {
      ret = OB_HASH_NOT_EXIST == ret ? OB_SCHEMA_ERROR : ret;
      LOG_WARN("fail to get tablet idx", K(ret));
    } else if (FALSE_IT(tablet_info.estimated_row_count_ = max(1, tstat.get_row_count()))) {
    } else if (OB_FAIL(px_tablets_info.push_back(tablet_info))) {
      LOG_WARN("Failed to push back tablet info");
    } else {
      LOG_TRACE("[Dop Assign]get parttion row count", K(tablet_info.tablet_id_),
                K(tablet_info.estimated_row_count_));
    }
  }

  // 2. use column statistic scale row count
  if (OB_FAIL(ret)) {
  } else if (!is_opt_stat_valid) {
  } else if (OB_FAIL(scale_partition_row_count_with_column_stat(tenant_id, ctx, scan_op, locations,
                                                                ref_table_id, px_tablets_info,
                                                                tablets_info_offset))) {
    LOG_WARN("failed to scale partition row count with column stat");
  }
  return ret;
}

/*
  if query has range, we use range + column stat to scale the row count
*/
int ObPxEstimateSizeUtil::scale_partition_row_count_with_column_stat(
    int64_t tenant_id, ObExecContext &ctx, const ObTableScanSpec *scan_op,
    const DASTabletLocList &locations, int64_t ref_table_id,
    ObIArray<ObPxTabletInfo> &px_tablets_info, int64_t tablets_info_offset)
{
/*
suppose bound is [a, b] and range is [c, d], do ajust:
if c < a, then set c = a
if d > b, then set d =b

for equal range, we use 1/ndv as selectivity
for inprecise range, skip the post columns
for normal range, use range / bound as selectivity
for example:
  bound:        [1, 5]    [1, 5]    [1, 5]   [1, 5]    [1, 5]
  range:        [-5, -1]  [-5, 2]   [-5, 7]  [3, 3]    [-5, 1]
  adjust range: [1, -1]   [1, 2]    [1, 5]   [3, 3]    [1, 1]
  selectivity:   0         0.25      1        1/dnv     1/ndv
*/
#define get_range_scale(data_type, get_data_func)                                                  \
  data_type bound_min = min.get_data_func();                                                       \
  data_type bound_max = max.get_data_func();                                                       \
  uint64_t bound = bound_max - bound_min;                                                          \
  data_type range_min;                                                                             \
  data_type range_max;                                                                             \
  uint64_t range;                                                                                  \
  double selectivity = 1;                                                                          \
  for (int64_t j = 0; j < scan_ranges.count() && OB_SUCC(ret); ++j) {                              \
    selectivity = 1;                                                                               \
    if (range_selectivity.at(j) == 0) {                                                            \
      continue;                                                                                    \
    }                                                                                              \
    const common::ObObj &range_obj_min = scan_ranges.at(j)->start_key_.get_obj_ptr()[col_idx];     \
    const common::ObObj &range_obj_max = scan_ranges.at(j)->end_key_.get_obj_ptr()[col_idx];       \
    if (range_obj_min.is_max_value() || range_obj_max.is_min_value()) {                            \
      /*means inprecise range, skip it*/                                                           \
      continue;                                                                                    \
    }                                                                                              \
    if (is_oracle && range_obj_min.is_null()) {                                                    \
      range_min = bound_max; /*null last in oracle*/                                               \
    } else if ((!is_oracle && range_obj_min.is_null()) || range_obj_min.is_min_value()             \
               || range_obj_min.get_data_func() < bound_min) {                                     \
      range_min = bound_min;                                                                       \
    } else {                                                                                       \
      range_min = range_obj_min.get_data_func();                                                   \
    }                                                                                              \
    if ((!is_oracle && range_obj_max.is_null())) {                                                 \
      range_max = bound_min; /*null first in mysql*/                                               \
    } else if ((is_oracle && range_obj_max.is_null()) || range_obj_max.is_max_value()              \
               || range_obj_max.get_data_func() > bound_max) {                                     \
      range_max = bound_max;                                                                       \
    } else {                                                                                       \
      range_max = range_obj_max.get_data_func();                                                   \
    }                                                                                              \
    if (range_max < range_min) {                                                                   \
      selectivity = 0;                                                                             \
    } else if (range_max == range_min) {                                                           \
      /*equal range, use ndv*/                                                                     \
      selectivity = double(1) / ndv;                                                               \
    } else if (FALSE_IT(range = range_max - range_min)) {                                          \
    } else if (bound != range) {                                                                   \
      /* bound >= range > 0 here*/                                                                 \
      selectivity = double(range) / bound;                                                         \
    }                                                                                              \
    if (selectivity != 0 && selectivity != 1) {                                                    \
      /*sel1 * sel2^1/2 * sel3 ^ 1/2 ^ 1/2 */                                                      \
      for (int s = 0; s < col_idx; ++s) { selectivity = sqrt(selectivity); }                       \
    }                                                                                              \
    range_selectivity.at(j) *= selectivity;                                                        \
    LOG_TRACE("[Dop Assign]print selectivity", K(location_idx), K(col_idx), K(j),                  \
              K(range_selectivity.at(j)), K(range_min), K(range_max), K(bound_min), K(bound_max)); \
  }

  int ret = OB_SUCCESS;
  bool need_get_column_stat = true;
  int64_t most_range_column_cnt = 0;
  ObQueryRangeArray scan_ranges;
  ObTMArray<double> range_selectivity;
  ObTMArray<double> tablet_selectivity;
  bool is_oracle = lib::is_oracle_mode();
  const ObQueryRangeProvider &tsc_pre_query_range = scan_op->get_query_range_provider();
  if (0 == tsc_pre_query_range.get_column_count() || tsc_pre_query_range.is_precise_whole_range()
      || tsc_pre_query_range.has_exec_param() || tsc_pre_query_range.is_ss_range()
      || !tsc_pre_query_range.is_new_query_range()) {
    need_get_column_stat = false;
  } else {
    for (int64_t i = 0; i < tsc_pre_query_range.get_column_count() && OB_SUCC(ret); ++i) {
      const ObExprResType &result_type =
          static_cast<const ObPreRangeGraph &>(tsc_pre_query_range).get_column_meta(i)->column_type_;
      if (!(result_type.is_unsigned_integer() || result_type.is_signed_integer())) {
        break;
      } else {
        // only for int type column, we can use column statistic
        most_range_column_cnt++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (most_range_column_cnt == 0) {
      need_get_column_stat = false;
    } else if (OB_FAIL(ObSQLUtils::extract_pre_query_range(
                   tsc_pre_query_range, ctx.get_allocator(), ctx, scan_ranges,
                   ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
      LOG_WARN("failed to get scan ranges", K(ret));
    } else if (scan_ranges.count() > 10) {
      need_get_column_stat = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!need_get_column_stat) {
  } else {
    LOG_TRACE("[Dop Assign]use column_stat", K(most_range_column_cnt), K(scan_ranges.count()));
    ObOptColumnStatHandle handle;
    int64_t location_idx = 0;
    bool is_column_stat_valied = true;
    if (OB_FAIL(range_selectivity.prepare_allocate(scan_ranges.count()))) {
      LOG_WARN("failed to prepare allocate", K(scan_ranges.count()));
    } else if (OB_FAIL(tablet_selectivity.prepare_allocate(locations.size()))) {
      LOG_WARN("failed to prepare allocate", K(locations.size()));
    }

    for (DASTabletLocList::const_iterator iter = locations.begin();
         OB_SUCC(ret) && is_column_stat_valied && iter != locations.end(); ++iter, ++location_idx) {
      int64_t partition_id = (*iter)->partition_id_;
      // init each range's scale selectivity as 1
      for (int64_t j = 0; j < scan_ranges.count(); ++j) {
        range_selectivity.at(j) = 1;
      }
      for (int64_t col_idx = 0; col_idx < most_range_column_cnt && OB_SUCC(ret); ++col_idx) {
        handle.reset();
        uint64_t column_id = static_cast<const ObPreRangeGraph &>(tsc_pre_query_range)
                                 .get_column_meta(col_idx)->column_id_;
        if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(
                tenant_id, ref_table_id, partition_id, column_id, handle))) {
          LOG_WARN("failed to get_column_stat");
        } else if (OB_ISNULL(handle.stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null");
        } else if (0 >= handle.stat_->get_last_analyzed()) {
          is_column_stat_valied = false;
          break;
        } else {
          const common::ObObj &min = handle.stat_->get_min_value();
          const common::ObObj &max = handle.stat_->get_max_value();
          int64_t ndv = std::max(handle.stat_->get_num_distinct(), 1L); // avoid div 0
          if (min.is_unsigned_integer()) {
            get_range_scale(uint64_t, get_uint64);
          } else {
            get_range_scale(int64_t, get_int);
          }
        }
      }
      tablet_selectivity.at(location_idx) = 0;
      for (int64_t j = 0; j < scan_ranges.count(); ++j) {
        tablet_selectivity.at(location_idx) += range_selectivity.at(j);
      }
    }
    // if column stat valied, use it to scale row count, otherwise directly using partition stat
    if (OB_SUCC(ret) && is_column_stat_valied) {
      int64_t location_idx = 0;
      int64_t tablet_arr_idx = tablets_info_offset;
      for (DASTabletLocList::const_iterator iter = locations.begin();
           OB_SUCC(ret) && is_column_stat_valied && iter != locations.end();
           ++iter, ++location_idx, ++tablet_arr_idx) {
        px_tablets_info.at(tablet_arr_idx).estimated_row_count_ *= tablet_selectivity.at(location_idx);
        px_tablets_info.at(tablet_arr_idx).estimated_row_count_ =
            max(1, px_tablets_info.at(tablet_arr_idx).estimated_row_count_);
        LOG_TRACE("[Dop Assign]print partition row count after scale",
                  K(px_tablets_info.at(tablet_arr_idx).tablet_id_),
                  K(tablet_selectivity.at(location_idx)),
                  K(px_tablets_info.at(tablet_arr_idx).estimated_row_count_));
      }
    }
  }
#undef get_range_scale
  return ret;
}

int ObSlaveMapUtil::build_mn_channel(
  ObPxChTotalInfos *dfo_ch_total_infos,
  ObDfo &child,
  ObDfo &parent,
  const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // 设置 [M, N, start_ch_id_, ch_count_, sqc_addrs_, prefix_sqc_task_counts_]
  if (OB_ISNULL(dfo_ch_total_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit or receive mn channel info is null", KP(dfo_ch_total_infos));
  } else {
    OZ(dfo_ch_total_infos->prepare_allocate(1));
    if (OB_SUCC(ret)) {
      ObDtlChTotalInfo &transmit_ch_info = dfo_ch_total_infos->at(0);
      OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.transmit_exec_server_, child.get_sqcs()));
      OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.receive_exec_server_, parent.get_sqcs()));
      transmit_ch_info.channel_count_ = transmit_ch_info.transmit_exec_server_.total_task_cnt_
                                      * transmit_ch_info.receive_exec_server_.total_task_cnt_;
      transmit_ch_info.start_channel_id_ = ObDtlChannel::generate_id(transmit_ch_info.channel_count_)
                                         - transmit_ch_info.channel_count_ + 1;
      transmit_ch_info.tenant_id_ = tenant_id;
      if (transmit_ch_info.transmit_exec_server_.exec_addrs_.count() >
          transmit_ch_info.transmit_exec_server_.total_task_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ch info", K(transmit_ch_info), K(child));
      }
    }
  }
  return ret;
}

int ObSlaveMapUtil::build_mn_channel_per_sqcs(
  ObPxChTotalInfos *dfo_ch_total_infos,
  ObDfo &child,
  ObDfo &parent,
  int64_t sqc_count,
  uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // 设置 [M, N, start_ch_id_, ch_count_, sqc_addrs_, prefix_sqc_task_counts_]
  if (OB_ISNULL(dfo_ch_total_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit or receive mn channel info is null", KP(dfo_ch_total_infos));
  } else if (OB_UNLIKELY(child.get_sqcs_count() != parent.get_sqcs_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc count not match in slave mapping plan", K(ret), K(parent), K(child));
  } else {
    OZ(dfo_ch_total_infos->prepare_allocate(sqc_count));
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < sqc_count && OB_SUCC(ret); ++i) {
        ObDtlChTotalInfo &transmit_ch_info = dfo_ch_total_infos->at(i);
        transmit_ch_info.is_local_shuffle_ = true;
        ObPxSqcMeta *child_sqc = &child.get_sqcs().at(i);
        ObPxSqcMeta *parent_sqc = nullptr;
        if (parent.need_access_store() && parent.is_in_slave_mapping()
            && ObPQDistributeMethod::HASH == child.get_dist_method()
            && child.is_out_slave_mapping()) {
          // for slave mapping under union all, the parent dfo may contain scan ops
          // the sqc addr's sequence for each union branch may be different
          // we should map the sqc pair of parent and child for slave mapping
          for (int64_t j = 0; j < sqc_count && OB_SUCC(ret); ++j) {
            if (child_sqc->get_exec_addr() == parent.get_sqcs().at(j).get_exec_addr()) {
              parent_sqc = &parent.get_sqcs().at(j);
              break;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(parent_sqc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("addr not match", K(ret), K(child.get_dfo_id()), K(parent.get_dfo_id()));
          }
        } else if (OB_UNLIKELY(parent.get_sqcs().at(i).get_exec_addr()
                               != child.get_sqcs().at(i).get_exec_addr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("addr not match", K(ret), K(child.get_dfo_id()), K(parent.get_dfo_id()));
        } else {
          parent_sqc = &parent.get_sqcs().at(i);
        }
        if (OB_SUCC(ret)) {
          OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.transmit_exec_server_, *child_sqc));
          OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.receive_exec_server_, *parent_sqc));
          transmit_ch_info.channel_count_ = transmit_ch_info.transmit_exec_server_.total_task_cnt_
                                          * transmit_ch_info.receive_exec_server_.total_task_cnt_;
          transmit_ch_info.start_channel_id_ = ObDtlChannel::generate_id(transmit_ch_info.channel_count_)
                                            - transmit_ch_info.channel_count_ + 1;
          transmit_ch_info.tenant_id_ = tenant_id;
        }
      }
    }
  }
  LOG_DEBUG("build mn channel per sqcs", K(parent), K(child), KPC(dfo_ch_total_infos));
  return ret;
}

// 对应的Plan是
// GI(Partition)
//    Hash Join           Hash Join
//      hash   ->>          Exchange(hash local)
//      hash                Exchange(hash local)
// Hash local指仅仅在server内部做hash hash，不会跨server进行shuffle
int ObSlaveMapUtil::build_pwj_slave_map_mn_group(ObDfo &parent, ObDfo &child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  int64_t child_dfo_idx = -1;
  /**
   * 根据slave mapping的设计，现在我们是按照一台机器内所有的线程为一个组。
   * 这里我们会根据ch set的具体执行server来build组。
   */
  if (parent.get_sqcs_count() != child.get_sqcs_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pwj must have the same sqc count", K(ret), K(parent.get_sqcs_count()), K(child.get_sqcs_count()));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel_per_sqcs(
      dfo_ch_total_infos, child, parent, child.get_sqcs_count(), tenant_id))) {
    LOG_WARN("failed to build mn channel per sqc", K(ret));
  } else {
    LOG_DEBUG("build pwj slave map group", K(child.get_dfo_id()));
  }
  return ret;
}

int ObSlaveMapUtil::build_partition_map_by_sqcs(
  common::ObIArray<ObPxSqcMeta> &sqcs,
  ObDfo &child,
  ObIArray<int64_t> &prefix_task_counts,
  ObPxPartChMapArray &map)
{
  int ret = OB_SUCCESS;
  UNUSED(prefix_task_counts);
  DASTabletLocArray locations;
  if (prefix_task_counts.count() != sqcs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix task counts is not match sqcs count", K(ret));
  }
  for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); i++) {
    ObPxSqcMeta &sqc = sqcs.at(i);
    locations.reset();
    if (OB_FAIL(get_pkey_table_locations(child.get_pkey_table_loc_id(),
        sqc, locations))) {
      LOG_WARN("fail to get pkey table locations", K(ret));
    }
    ARRAY_FOREACH_X(locations, loc_idx, loc_cnt, OB_SUCC(ret)) {
      const ObDASTabletLoc &location = *locations.at(loc_idx);
      int64_t tablet_id = location.tablet_id_.id();
      // int64_t prefix_task_count = prefix_task_counts.at(i);
      OZ(map.push_back(ObPxPartChMapItem(tablet_id, i)));
      LOG_DEBUG("debug push partition map", K(tablet_id), K(i), K(sqc.get_sqc_id()));
    }
  }
  LOG_DEBUG("debug push partition map", K(map));
  return ret;
}

int ObSlaveMapUtil::build_affinitized_partition_map_by_sqcs(
  common::ObIArray<ObPxSqcMeta> &sqcs,
  ObDfo &child,
  ObIArray<int64_t> &prefix_task_counts,
  int64_t total_task_cnt,
  ObPxPartChMapArray &map)
{
  int ret = OB_SUCCESS;
  DASTabletLocArray locations;
  if (OB_UNLIKELY(sqcs.count() <= 0 || prefix_task_counts.count() != sqcs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix task counts is not match sqcs count", K(sqcs.count()), K(ret));
  }
  for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); i++) {
    // 目标 sqc 上有 locations.count() 个分区
    // 目标 sqc 上有 sqc_task_count 个线程来处理这些分区
    // 可以采用 Round-Robin 混编这些分区到线程上
    // 不过，由于使用map端做了一个假设同一个 part 的处理线程，一定是相邻的
    // 这边比较容易做 hash 运算。
    //
    // [p0][p1][p2][p3][p4]
    // [t0][t1][t0][t1][t0]
    //
    // [p0][p0][p1][p1][p2]
    // [t0][t1][t2][t3][t4]
    //
    // [p0][p1][p2][p3][p4]
    // [t0][t0][t1][t1][t2]
    //
    // task_per_part: 每个分区有多少个线程服务
    // double task_per_part = (double)sqc_task_count / locations.count();
    // for (int64_t idx = 0; idx < locations.count(); ++idx) {
    //  for (int j = 0; j < task_per_part; j++) {
    //    int64_t p = idx;
    //    int64_t t = idx * task_per_part + j;
    //    if (t >= sqc_task_count) {
    //      break;
    //    }
    //    emit(idx, t);
    //  }
    // }
    int64_t sqc_task_count = 0;
    int64_t prefix_task_count = prefix_task_counts.at(i);
    if (i + 1 == prefix_task_counts.count()) {
      sqc_task_count = total_task_cnt - prefix_task_counts.at(i);
    } else {
      sqc_task_count = prefix_task_counts.at(i + 1) - prefix_task_counts.at(i);
    }
    ObPxSqcMeta &sqc = sqcs.at(i);
    locations.reset();
    if (OB_FAIL(get_pkey_table_locations(child.get_pkey_table_loc_id(),
        sqc, locations))) {
      LOG_WARN("fail to get pkey table locations", K(ret));
    }
    if (locations.count() <= 0 || sqc_task_count <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of location is zero in one sqc", K(ret), K(sqc_task_count), K(sqc));
      break;
    }

    // task_per_part: 每个分区有多少个线程服务，
    // rest_task: 这些task每个再多负责一个分区，别浪费 cpu
    // 例如：
    // 13 个线程，3个分区， task_per_part = 4， rest_task = 1 去做第一个分区
    // 3 个线程，5个分区，task_per_part = 0， rest_task = 3 去做1,2,3三个分区
    int64_t task_per_part = sqc_task_count / locations.count();
    int64_t rest_task = sqc_task_count % locations.count();
    if (task_per_part > 0) {
      // 场景1： 线程数 > 分区数
      // 要点：每个分区分配的线程数要连续
      int64_t t = 0;
      for (int64_t p = 0; OB_SUCC(ret) && p < locations.count(); ++p) {
        int64_t tablet_id = locations.at(p)->tablet_id_.id();
        int64_t next = (p >= rest_task) ? task_per_part : task_per_part + 1;
        for (int64_t loop = 0; OB_SUCC(ret) && loop < next; ++loop) {
          // first：tablet id, second: prefix_task_count, third: sqc_task_idx
          OZ(map.push_back(ObPxPartChMapItem(tablet_id, prefix_task_count, t)));
          LOG_DEBUG("t>p: push partition map", K(tablet_id), "sqc", i, "g_t", prefix_task_count + t, K(t));
          t++;
        }
      }
    } else {
      // 场景2：线程数 < 分区数
      // 要点：保证每个分区至少有一个线程处理
      for (int64_t p = 0; OB_SUCC(ret) && p < locations.count(); ++p) {
        int64_t t = p % rest_task;
        int64_t tablet_id = locations.at(p)->tablet_id_.id();
        // 具体含义，参考 ObPxPartChMapItem:
        // first：tablet_id, second: prefix_task_count, third: sqc_task_idx
        OZ(map.push_back(ObPxPartChMapItem(tablet_id, prefix_task_count, t)));
        LOG_DEBUG("t<=p: push partition map", K(tablet_id), "sqc", i, "g_t", prefix_task_count + t, K(t));
      }
    }
  }
  LOG_DEBUG("debug push partition map", K(map));
  return ret;
}

int ObSlaveMapUtil::build_ppwj_bcast_slave_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t child_dfo_idx = -1;
  ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  ObPxPartChMapArray &map = child.get_part_ch_map();
  LOG_DEBUG("build ppwj bcast slave map", K(parent.get_dfo_id()), K(parent.get_sqcs_count()),
      K(parent.get_tasks()));
  ObIArray<ObPxSqcMeta> &sqcs = parent.get_sqcs();
  if (OB_UNLIKELY(sqcs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
    LOG_WARN("failed to build mn channels", K(ret));
  } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
  } else if (OB_FAIL(build_partition_map_by_sqcs(
      sqcs, child,
      dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
    LOG_WARN("failed to build channel map by sqc", K(ret));
  } else if (map.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of channel map is unexpected", K(ret), K(map.count()));
  }
  return ret;
}

int ObSlaveMapUtil::build_ppwj_slave_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (ObPQDistributeMethod::PARTITION_HASH == child.get_dist_method()) {
    ObDfo *reference_child = nullptr;
    int64_t child_dfo_idx = -1;
    ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
    ObPxPartChMapArray &map = child.get_part_ch_map();
    if (OB_FAIL(ObPXServerAddrUtil::find_reference_child(parent, reference_child))) {
      LOG_WARN("find reference child", K(ret));
    } else if (OB_ISNULL(reference_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret));
    } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
      LOG_WARN("failed to check dfo pair", K(ret));
    } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
      LOG_WARN("failed to build mn channels", K(ret));
    } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
    } else if (OB_UNLIKELY(reference_child->get_sqcs_count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of sqc is unexpected", K(ret));
    } else if (OB_FAIL(build_partition_map_by_sqcs(
        reference_child->get_sqcs(), child,
        dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
      LOG_WARN("failed to build channel map by sqc", K(ret));
    }
  } else if (OB_FAIL(build_pwj_slave_map_mn_group(parent, child, tenant_id))) {
    LOG_WARN("failed to build ppwj slave map", K(ret));
  }
  return ret;
}

// 本函数用于 pdml 场景，支持：
//  1. 将 pkey 映射到一个或多个线程上
//  2. 将 多个 pkey 映射到一个线程上
// 并且保证：pkey 最小化分布（在充分运用算力的前提下，让尽可能少的线程并发处理同一个分区）
// pkey-hash, pkey-range 等都可以用这个 map
int ObSlaveMapUtil::build_pkey_affinitized_ch_mn_map(ObDfo &parent,
                                                     ObDfo &child,
                                                     uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (1 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dfo", K(ret), K(parent));
  } else if (ObPQDistributeMethod::PARTITION_HASH == child.get_dist_method()
      || ObPQDistributeMethod::PARTITION_RANGE == child.get_dist_method()) {
    LOG_TRACE("build pkey affinitized channel map",
      K(parent.get_dfo_id()), K(parent.get_sqcs_count()),
      K(child.get_dfo_id()), K(child.get_sqcs_count()));
      //  .....
      //    PDML
      //      EX (pkey hash)
      //  .....
      // 为child dfo建立其发送数据的channel map，在pkey random模型下，parent dfo的每一个SQC中的worker都可以
      // 处理其对应SQC中所包含的所有partition，所以直接采用`build_ch_map_by_sqcs`
      ObPxPartChMapArray &map = child.get_part_ch_map();
      ObIArray<ObPxSqcMeta> &sqcs = parent.get_sqcs();
      ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
      int64_t child_dfo_idx = -1;
      if (OB_UNLIKELY(sqcs.count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
      } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
        LOG_WARN("failed to check dfo pair", K(ret));
      } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
        LOG_WARN("failed to build mn channels", K(ret));
      } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
      } else if (OB_FAIL(build_affinitized_partition_map_by_sqcs(
          sqcs,
          child,
          dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_,
          dfo_ch_total_infos->at(0).receive_exec_server_.total_task_cnt_,
          map))) {
        LOG_WARN("failed to build channel map by sqc", K(ret));
      } else if (map.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the size of channel map is unexpected",
          K(ret), K(map.count()), K(parent.get_tasks()), K(sqcs));
      }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child dfo", K(ret), K(child.get_dist_method()));
  }
  return ret;
}


int ObSlaveMapUtil::build_pkey_random_ch_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (1 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dfo", K(ret), K(parent));
  } else if (ObPQDistributeMethod::PARTITION_RANDOM == child.get_dist_method()) {
    LOG_TRACE("build pkey random channel map",
      K(parent.get_dfo_id()), K(parent.get_sqcs_count()),
      K(child.get_dfo_id()), K(child.get_sqcs_count()));
      //  .....
      //    PDML
      //      EX (pkey random)
      //  .....
      // 为child dfo建立其发送数据的channel map，在pkey random模型下，parent dfo的每一个SQC中的worker都可以
      // 处理其对应SQC中所包含的所有partition，所以直接采用`build_ch_map_by_sqcs`
      ObPxPartChMapArray &map = child.get_part_ch_map();
      ObIArray<ObPxSqcMeta> &sqcs = parent.get_sqcs();
      ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
      int64_t child_dfo_idx = -1;
      if (OB_UNLIKELY(sqcs.count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
      } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
        LOG_WARN("failed to check dfo pair", K(ret));
      } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
        LOG_WARN("failed to build mn channels", K(ret));
      } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
      } else if (OB_FAIL(build_partition_map_by_sqcs(
          sqcs, child,
          dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
        LOG_WARN("failed to build channel map by sqc", K(ret));
      } else if (map.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the size of channel map is unexpected",
          K(ret), K(map.count()), K(parent.get_tasks()), K(sqcs));
      }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child dfo", K(ret), K(child.get_dist_method()));
  }
  return ret;
}

int ObSlaveMapUtil::build_ppwj_ch_mn_map(ObExecContext &ctx, ObDfo &parent, ObDfo &child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // dfo 中可以取到所有 dfo 相关的 partition
  // 从中可以计算出所有相关的 partition id
  // 根据 partition id 用确定的 hash 算法计算 id 与 channel 的映射关系

  // 1. 遍历 dfo 中所有 sqc，里面包含了 partition id
  // 2. 将 partition id 做 hash，算出 task_id
  // 3. 遍历 tasks，找到 i, 满足:
  //    tasks[i].server = sqc.server && tasks[i].task_id = hash(tablet_id)
  // 4. 将 (tablet_id, i) 记录到 map 中
  ObIArray<ObPxSqcMeta> &sqcs = parent.get_sqcs();
  ObPxPartChMapArray &map = child.get_part_ch_map();
  int64_t child_dfo_idx = -1;
  ObPxChTotalInfos *dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  if (OB_FAIL(map.reserve(parent.get_tasks().count()))) {
    LOG_WARN("fail reserve memory for map", K(ret), K(parent.get_tasks().count()));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
    LOG_WARN("failed to build mn channels", K(ret));
  } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *table_schema = NULL;
    ObTabletIdxMap idx_map;
    common::ObIArray<int64_t> &prefix_task_counts =
      dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_;
    if (prefix_task_counts.count() != sqcs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: prefix task count is not match sqcs count", K(ret),
        KP(prefix_task_counts.count()), K(sqcs.count()));
    }
    DASTabletLocArray locations;
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      // 所有的affinitize计算都是SQC局部，不是全局的。
      ObPxSqcMeta &sqc = sqcs.at(idx);
      ObPxAffinityByRandom affinitize_rule(sqc.sqc_order_gi_tasks(),
                                           sqc.partition_random_affinitize());
      LOG_TRACE("build ppwj_ch_mn_map", K(sqc));
      ObPxTabletInfo tablet_row_info;
      locations.reset();
      if (OB_FAIL(get_pkey_table_locations(child.get_pkey_table_loc_id(), sqc, locations))) {
        LOG_WARN("fail to get pkey table locations", K(ret));
      } else if (OB_FAIL(affinitize_rule.reserve(locations.count()))) {
        LOG_WARN("fail reserve memory", K(ret), K(locations.count()));
      }
      int64_t tablet_idx = OB_INVALID_ID;
      ARRAY_FOREACH_X(locations, loc_idx, loc_cnt, OB_SUCC(ret)) {
        const ObDASTabletLoc &location = *locations.at(loc_idx);
        if (NULL == table_schema) {
          uint64_t table_id = location.loc_meta_->ref_table_id_;
          if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                      ctx.get_my_session()->get_effective_tenant_id(),
                      schema_guard))) {
            LOG_WARN("faile to get schema guard", K(ret));
          } else if (OB_FAIL(schema_guard.get_table_schema(
                     ctx.get_my_session()->get_effective_tenant_id(),
                     table_id, table_schema))) {
            LOG_WARN("faile to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("table schema is null", K(ret), K(table_id));
          } else if (OB_FAIL(ObPXServerAddrUtil::build_tablet_idx_map(table_schema, idx_map))) {
            LOG_WARN("fail to build tablet idx map", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          // pass
        } else if (OB_FAIL(idx_map.get_refactored(location.tablet_id_.id(), tablet_idx))) {
          ret = OB_HASH_NOT_EXIST == ret ? OB_SCHEMA_ERROR : ret;
          LOG_WARN("fail to get tablet idx", K(ret));
        } else if (OB_FAIL(ObPxAffinityByRandom::get_tablet_info(location.tablet_id_.id(),
                                                                 sqc.get_px_tablets_info(),
                                                                 tablet_row_info))) {
          LOG_WARN("Failed to get partition info", K(ret));
        } else if (OB_FAIL(affinitize_rule.add_partition(location.tablet_id_.id(),
                tablet_idx,
                sqc.get_task_count(),
                ctx.get_my_session()->get_effective_tenant_id(),
                tablet_row_info))) {
          LOG_WARN("fail calc task_id", K(location.tablet_id_), K(sqc), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(affinitize_rule.do_random(!sqc.get_px_tablets_info().empty(),
                         ctx.get_my_session()->get_effective_tenant_id()))) {
        LOG_WARN("failed to do random", K(ret));
      } else {
        const ObIArray<ObPxAffinityByRandom::TabletHashValue> &partition_worker_pairs =
          affinitize_rule.get_result();
        int64_t prefix_task_count = prefix_task_counts.at(idx);
        ARRAY_FOREACH(partition_worker_pairs, idx) {
          int64_t tablet_id = partition_worker_pairs.at(idx).tablet_id_;
          int64_t task_id = partition_worker_pairs.at(idx).worker_id_;
          OZ(map.push_back(ObPxPartChMapItem(tablet_id, prefix_task_count, task_id)));
          LOG_DEBUG("debug push partition map", K(tablet_id), K(task_id));
        }
        LOG_DEBUG("Get all tablets rows info", K(ret), K(sqc.get_px_tablets_info()));
      }
    }
  }
  return ret;
}

int ObSlaveMapUtil::build_slave_mapping_mn_ch_map(ObExecContext &ctx, ObDfo &child, ObDfo &parent,
                                                  uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SlaveMappingType slave_type = parent.get_in_slave_mapping_type();
  switch(slave_type) {
  case SlaveMappingType::SM_PWJ_HASH_HASH : {
    if (OB_FAIL(build_pwj_slave_map_mn_group(parent, child, tenant_id))) {
      LOG_WARN("fail to build pwj slave map", K(ret));
    }
    break;
  }
  case SlaveMappingType::SM_PPWJ_BCAST_NONE :
  case SlaveMappingType::SM_PPWJ_NONE_BCAST : {
    if (OB_FAIL(build_ppwj_bcast_slave_mn_map(parent, child, tenant_id))) {
      LOG_WARN("fail to build pwj slave map", K(ret));
    }
    break;
  }
  case SlaveMappingType::SM_PPWJ_HASH_HASH : {
    if (OB_FAIL(build_ppwj_slave_mn_map(parent, child, tenant_id))) {
      LOG_WARN("fail to build pwj slave map", K(ret));
    }
    break;
  }
  case SlaveMappingType::SM_NONE : {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected slave mapping type", K(child.get_dfo_id()), K(parent.get_dfo_id()));
    break;
  }
  }
  LOG_DEBUG("debug distribute type", K(child.get_dfo_id()), K(parent.get_dfo_id()), K(slave_type));
  return ret;
}

int ObSlaveMapUtil::build_pkey_mn_ch_map(ObExecContext &ctx, ObDfo &child, ObDfo &parent,
                                         uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObPQDistributeMethod::Type child_dist_method = child.get_dist_method();
  switch(child_dist_method) {
  case ObPQDistributeMethod::Type::PARTITION : {
    // for normal pkey
    if (OB_FAIL(build_ppwj_ch_mn_map(ctx, parent, child, tenant_id))) {
      LOG_WARN("failed to build partial partition wise join channel map", K(ret));
    }
    break;
  }
  case ObPQDistributeMethod::Type::PARTITION_HASH:
  case ObPQDistributeMethod::Type::PARTITION_RANGE: {
    if (OB_FAIL(build_pkey_affinitized_ch_mn_map(parent, child, tenant_id))) {
      LOG_WARN("failed to build pkey random channel map", K(ret));
    }
    break;
  }
  case ObPQDistributeMethod::Type::PARTITION_RANDOM: {
    // PDML: shuffle to any worker in parent dfo
    if (OB_FAIL(build_pkey_random_ch_mn_map(parent, child, tenant_id))) {
      LOG_WARN("failed to build pkey random channel map", K(ret));
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child dist method type", K(child.get_dfo_id()), K(parent.get_dfo_id()),
             K(child_dist_method));
    break;
  }
  }
  LOG_DEBUG("debug distribute type", K(child.get_dfo_id()), K(parent.get_dfo_id()),
            K(child.get_dist_method()));
  return ret;
}

int ObSlaveMapUtil::get_pkey_table_locations(int64_t table_location_key,
    ObPxSqcMeta &sqc,
    DASTabletLocIArray &pkey_locations)
{
  int ret = OB_SUCCESS;
  pkey_locations.reset();
  DASTabletLocIArray &access_table_locations = sqc.get_access_table_locations_for_update();
  ObIArray<ObSqcTableLocationIndex> &location_indexes = sqc.get_access_table_location_indexes();
  int64_t cnt = location_indexes.count();
  // from end to start is necessary!
  for (int i = cnt - 1; i >= 0 && OB_SUCC(ret); --i) {
    if (table_location_key == location_indexes.at(i).table_location_key_) {
      int64_t start = location_indexes.at(i).location_start_pos_;
      int64_t end = location_indexes.at(i).location_end_pos_;
      for (int j = start;j <= end && OB_SUCC(ret); ++j) {
        if (j >= 0 && j < access_table_locations.count()) {
          OZ(pkey_locations.push_back(access_table_locations.at(j)));
        }
      }
      break;
    }
  }
  if (OB_SUCC(ret) && pkey_locations.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected locations", K(location_indexes), K(access_table_locations), K(table_location_key), K(ret));
  }
  return ret;
}

int ObDtlChannelUtil::get_mn_receive_dtl_channel_set(
  const int64_t sqc_id,
  const int64_t task_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  // receive
  int64_t ch_cnt = 0;
  if (0 > sqc_id || sqc_id >= ch_total_info.receive_exec_server_.prefix_task_counts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret),
      K(ch_total_info.receive_exec_server_.prefix_task_counts_.count()));
  } else if (ch_total_info.transmit_exec_server_.prefix_task_counts_.count() != ch_total_info.transmit_exec_server_.exec_addrs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: prefix task count is not match with execute address", K(ret),
      "prefix task count", ch_total_info.transmit_exec_server_.prefix_task_counts_.count(),
      "execute address count", ch_total_info.transmit_exec_server_.exec_addrs_.count());
  } else {
    int64_t start_task_id = ch_total_info.receive_exec_server_.prefix_task_counts_.at(sqc_id);
    int64_t receive_task_cnt = ch_total_info.receive_exec_server_.total_task_cnt_;
    int64_t base_chid = ch_total_info.start_channel_id_ + (start_task_id + task_id);
    int64_t chid = 0;
    ObIArray<int64_t> &prefix_task_counts = ch_total_info.transmit_exec_server_.prefix_task_counts_;
    int64_t pre_prefix_task_count = 0;
    if (OB_FAIL(ch_set.reserve(ch_total_info.transmit_exec_server_.total_task_cnt_))) {
      LOG_WARN("fail reserve memory for channels", K(ret),
               "channels", ch_total_info.transmit_exec_server_.total_task_cnt_);
    }
    // 遍历transmit的所有server，逐个构建当前这个receive task和它们的channel
    for (int64_t i = 0; i < prefix_task_counts.count() && OB_SUCC(ret); ++i) {
      int64_t prefix_task_count = 0;
      if (i + 1 == prefix_task_counts.count()) {
        prefix_task_count = ch_total_info.transmit_exec_server_.total_task_cnt_;
      } else {
        prefix_task_count = prefix_task_counts.at(i + 1);
      }
      ObAddr &dst_addr = ch_total_info.transmit_exec_server_.exec_addrs_.at(i);
      bool is_local = dst_addr == GCONF.self_addr_;
      // [pre_prefix_task_count, prefix_task_count)表示transmit的第i个sqc中的transmit tasks，
      // 在所有sqcs的transmit tasks中的编号
      for (int64_t j = pre_prefix_task_count; j < prefix_task_count && OB_SUCC(ret); ++j) {
        ObDtlChannelInfo ch_info;
        chid = base_chid + receive_task_cnt * j;
        ObDtlChannelGroup::make_receive_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
        OZ(ch_set.add_channel_info(ch_info));
        LOG_DEBUG("debug receive channel", KP(chid), K(ch_info), K(sqc_id), K(task_id),
          K(ch_total_info.start_channel_id_));
        ++ch_cnt;
      }
      pre_prefix_task_count = prefix_task_count;
    }
    if (OB_SUCC(ret) && ch_cnt != ch_total_info.transmit_exec_server_.total_task_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match", K(ch_cnt),
        K(ch_total_info.channel_count_), K(receive_task_cnt), K(prefix_task_counts),
        K(ch_total_info.transmit_exec_server_.total_task_cnt_), K(sqc_id), K(task_id));
    }
  }
  LOG_DEBUG("get mn receive dtl channel set", K(sqc_id), K(task_id), K(ch_total_info), K(ch_set));
  return ret;
}

int ObDtlChannelUtil::get_sm_receive_dtl_channel_set(
  const int64_t sqc_id,
  const int64_t task_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  UNUSED(sqc_id);
  int64_t receive_task_cnt = ch_total_info.receive_exec_server_.total_task_cnt_;
  int64_t transmit_task_cnt = ch_total_info.transmit_exec_server_.total_task_cnt_;
  if (OB_UNLIKELY(1 != ch_total_info.receive_exec_server_.exec_addrs_.count()
                  || 1 != ch_total_info.transmit_exec_server_.exec_addrs_.count()
                  || ch_total_info.receive_exec_server_.exec_addrs_.at(0) !=
                      ch_total_info.transmit_exec_server_.exec_addrs_.at(0)
                  || ch_total_info.receive_exec_server_.exec_addrs_.at(0) != GCONF.self_addr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exec addrs count", K(ret), K(ch_total_info), K(GCONF.self_addr_));
  } else if (OB_FAIL(ch_set.reserve(transmit_task_cnt))) {
    LOG_WARN("fail reserve memory for channels", K(ret), K(transmit_task_cnt));
  } else {
    ObAddr &dst_addr = ch_total_info.transmit_exec_server_.exec_addrs_.at(0);
    bool is_local = true;
    int64_t chid = 0;
    for (int64_t i = 0; i < transmit_task_cnt && OB_SUCC(ret); ++i) {
      ObDtlChannelInfo ch_info;
      chid = ch_total_info.start_channel_id_ + task_id + receive_task_cnt * i;
      ObDtlChannelGroup::make_receive_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
      OZ(ch_set.add_channel_info(ch_info));
    }
  }
  LOG_DEBUG("get sm receive dtl channel set", K(sqc_id), K(task_id), K(ch_total_info), K(ch_set));
  return ret;
}

int ObDtlChannelUtil::get_mn_transmit_dtl_channel_set(
  const int64_t sqc_id,
  const int64_t task_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  if (0 > sqc_id || sqc_id >= ch_total_info.transmit_exec_server_.prefix_task_counts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret),
      K(ch_total_info.transmit_exec_server_.prefix_task_counts_.count()));
  } else {
    int64_t start_task_id = ch_total_info.transmit_exec_server_.prefix_task_counts_.at(sqc_id);
    int64_t base_chid = ch_total_info.start_channel_id_
          + (start_task_id + task_id) * ch_total_info.receive_exec_server_.total_task_cnt_;
    ObIArray<int64_t> &prefix_task_counts = ch_total_info.receive_exec_server_.prefix_task_counts_;
    int64_t ch_cnt = 0;
    int64_t pre_prefix_task_count = 0;
    int64_t chid = 0;
    if (OB_FAIL(ch_set.reserve(ch_total_info.receive_exec_server_.total_task_cnt_))) {
      LOG_WARN("fail reserve memory for channels", K(ret),
               "channels", ch_total_info.receive_exec_server_.total_task_cnt_);
    }
    for (int64_t i = 0; i < prefix_task_counts.count() && OB_SUCC(ret); ++i) {
      int64_t prefix_task_count = 0;
      if (i + 1 == prefix_task_counts.count()) {
        prefix_task_count = ch_total_info.receive_exec_server_.total_task_cnt_;
      } else {
        prefix_task_count = prefix_task_counts.at(i + 1);
      }
      ObAddr &dst_addr = ch_total_info.receive_exec_server_.exec_addrs_.at(i);
      bool is_local = dst_addr == GCONF.self_addr_;
      for (int64_t j = pre_prefix_task_count; j < prefix_task_count && OB_SUCC(ret); ++j) {
        ObDtlChannelInfo ch_info;
        chid = base_chid + j;
        ObDtlChannelGroup::make_transmit_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
        OZ(ch_set.add_channel_info(ch_info));
        ++ch_cnt;
        LOG_DEBUG("debug transmit channel", KP(chid), K(ch_info), K(sqc_id), K(task_id),
          K(ch_total_info.start_channel_id_));
      }
      pre_prefix_task_count = prefix_task_count;
    }
    if (OB_SUCC(ret) && ch_cnt != ch_total_info.receive_exec_server_.total_task_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match", K(ch_cnt),
        K(ch_total_info.transmit_exec_server_.total_task_cnt_));
    }
  }
  LOG_DEBUG("get transmit dtl channel set", K(sqc_id), K(task_id), K(ch_total_info), K(ch_set));
  return ret;
}

int ObDtlChannelUtil::get_sm_transmit_dtl_channel_set(
  const int64_t sqc_id,
  const int64_t task_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  UNUSED(sqc_id);
  int64_t receive_task_cnt = ch_total_info.receive_exec_server_.total_task_cnt_;
  int64_t transmit_task_cnt = ch_total_info.transmit_exec_server_.total_task_cnt_;
  if (OB_UNLIKELY(1 != ch_total_info.receive_exec_server_.exec_addrs_.count()
                  || 1 != ch_total_info.transmit_exec_server_.exec_addrs_.count()
                  || ch_total_info.receive_exec_server_.exec_addrs_.at(0) !=
                      ch_total_info.transmit_exec_server_.exec_addrs_.at(0)
                  || ch_total_info.receive_exec_server_.exec_addrs_.at(0) != GCONF.self_addr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exec addrs count", K(ret), K(ch_total_info), K(GCONF.self_addr_));
  } else if (OB_FAIL(ch_set.reserve(receive_task_cnt))) {
    LOG_WARN("fail reserve memory for channels", K(ret), K(receive_task_cnt));
  } else {
    ObAddr &dst_addr = ch_total_info.receive_exec_server_.exec_addrs_.at(0);
    bool is_local = true;
    int64_t chid = 0;
    for (int64_t i = 0; i < receive_task_cnt && OB_SUCC(ret); ++i) {
      ObDtlChannelInfo ch_info;
      chid = ch_total_info.start_channel_id_ + receive_task_cnt * task_id + i;
      ObDtlChannelGroup::make_transmit_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
      OZ(ch_set.add_channel_info(ch_info));
    }
  }
  LOG_DEBUG("get sm receive dtl channel set", K(sqc_id), K(task_id), K(transmit_task_cnt),
           K(receive_task_cnt), K(ch_total_info), K(ch_set));
  return ret;
}

// for bloom filter
int ObDtlChannelUtil::get_receive_bf_dtl_channel_set(
  const int64_t sqc_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  // receive
  if (0 > sqc_id || sqc_id >= ch_total_info.receive_exec_server_.prefix_task_counts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret),
      K(ch_total_info.receive_exec_server_.prefix_task_counts_.count()));
  } else {
    int64_t ch_cnt = 0;
    int64_t base_chid = ch_total_info.start_channel_id_ + sqc_id;
    int64_t receive_server_cnt = ch_total_info.receive_exec_server_.exec_addrs_.count();
    int64_t transmit_server_cnt = ch_total_info.transmit_exec_server_.exec_addrs_.count();
    ObIArray<ObAddr> &exec_addrs = ch_total_info.transmit_exec_server_.exec_addrs_;
    int64_t chid = 0;
    if (OB_FAIL(ch_set.reserve(exec_addrs.count()))) {
      LOG_WARN("fail reserve dtl channel memory", K(ret), "channels", exec_addrs.count());
    }
    for (int64_t i = 0; i < exec_addrs.count() && OB_SUCC(ret); ++i) {
      chid = base_chid + receive_server_cnt * i;
      ObAddr &dst_addr = exec_addrs.at(i);
      ObDtlChannelInfo ch_info;
      bool is_local = dst_addr == GCONF.self_addr_;
      ObDtlChannelGroup::make_receive_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
      OZ(ch_set.add_channel_info(ch_info));
      LOG_DEBUG("debug receive bloom filter channel", KP(chid), K(ch_info), K(sqc_id));
      ++ch_cnt;
    }
    if (OB_SUCC(ret) && ch_cnt != transmit_server_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match", K(ch_cnt),
        K(ch_total_info.channel_count_));
    }
  }
  return ret;
}

int ObDtlChannelUtil::get_transmit_bf_dtl_channel_set(
  const int64_t sqc_id,
  ObDtlChTotalInfo &ch_total_info,
  ObDtlChSet &ch_set)
{
  int ret = OB_SUCCESS;
  // receive
  if (0 > sqc_id || sqc_id >= ch_total_info.transmit_exec_server_.exec_addrs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret),
      K(ch_total_info.transmit_exec_server_.exec_addrs_.count()));
  } else {
    int64_t ch_cnt = 0;
    int64_t receive_server_cnt = ch_total_info.receive_exec_server_.exec_addrs_.count();
    int64_t base_chid = ch_total_info.start_channel_id_ + sqc_id * receive_server_cnt;
    int64_t chid = 0;
    ObIArray<ObAddr> &exec_addrs = ch_total_info.receive_exec_server_.exec_addrs_;
    if (OB_FAIL(ch_set.reserve(exec_addrs.count()))) {
      LOG_WARN("fail reserve dtl channel memory", K(ret), "channels", exec_addrs.count());
    }
    for (int64_t i = 0; i < exec_addrs.count() && OB_SUCC(ret); ++i) {
      chid = base_chid + i;
      ObAddr &dst_addr = exec_addrs.at(i);
      ObDtlChannelInfo ch_info;
      bool is_local = dst_addr == GCONF.self_addr_;
      ObDtlChannelGroup::make_transmit_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
      OZ(ch_set.add_channel_info(ch_info));
      LOG_DEBUG("debug transmit bloom filter channel", KP(chid), K(ch_info), K(sqc_id));
      ++ch_cnt;
    }
    if (OB_SUCC(ret) && ch_cnt != receive_server_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match", K(ch_cnt),
        K(ch_total_info.channel_count_));
    }
  }
  return ret;
}

int ObDtlChannelUtil::link_ch_set(
  ObDtlChSet &ch_set,
  common::ObIArray<dtl::ObDtlChannel*> &channels,
  DTLChannelPredFunc pred,
  ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  if (OB_FAIL(ch_set.reserve(ch_set.count()))) {
    LOG_WARN("fail reserve dtl channel memory", K(ret), "channels", ch_set.count());
  }
  for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel *ch = NULL;
    if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
      LOG_WARN("fail get channel info", K(idx), K(ret));
    } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail add qc channel", K(ret));
    } else if (OB_FAIL(channels.push_back(ch))) {
      LOG_WARN("fail push back channel ptr", K(ci), K(ret));
    } else if (nullptr != pred) {
      pred(ch);
    }
  }
  return ret;
}

int ObExtraServerAliveCheck::check() const
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL = 10000000; // 10 second
  int64_t cur_time  = ObTimeUtil::fast_current_time();
  if (cur_time - last_check_time_ > CHECK_INTERVAL) {
    ret = do_check();
    if (OB_SUCC(ret)) {
      last_check_time_ = cur_time;
    }
  }
  return ret;
}

int ObExtraServerAliveCheck::do_check() const
{
  int ret = OB_SUCCESS;
  if (NULL != dfo_mgr_) {
    ObSEArray<ObDfo *, 32> dfos;
    if (OB_FAIL(dfo_mgr_->get_running_dfos(dfos))) {
      LOG_WARN("fail find dfo", K(ret));
    } else {
      // need check all sqc because we set sqc need_report = false here and don't need wait sqc finish msg.
      for (int64_t i = 0; i < dfos.count(); i++) {
        ObIArray<ObPxSqcMeta> &sqcs = dfos.at(i)->get_sqcs();
        for (int64_t j = 0; j < sqcs.count(); j++) {
          if (sqcs.at(j).need_report()) {
            if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(sqcs.at(j).get_exec_addr(),
                query_start_time_))) {
              sqcs.at(j).set_need_report(false);
              sqcs.at(j).set_thread_finish(true);
              sqcs.at(j).set_server_not_alive(true);
              if (!sqcs.at(j).is_ignore_vtable_error()) {
                ret = OB_RPC_CONNECT_ERROR;
                LOG_WARN("server not in communication, maybe crashed.", K(ret),
                          KPC(dfos.at(i)), K(sqcs.at(j)));
              }
            }
          }
        }
      }
    }
  } else if (OB_LIKELY(qc_addr_.is_valid())) {
    if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(qc_addr_, query_start_time_))) {
      ret = OB_RPC_CONNECT_ERROR;
      LOG_WARN("qc not in communication, maybe crashed", K(ret), K(qc_addr_));
    }
  }
  LOG_DEBUG("server alive do check", K(ret), K(qc_addr_), K(cluster_id_), K(dfo_mgr_));
  return ret;
}

bool ObVirtualTableErrorWhitelist::should_ignore_vtable_error(int error_code)
{
  bool should_ignore = false;
  switch (error_code) {
    CASE_IGNORE_ERR_HELPER(OB_ALLOCATE_MEMORY_FAILED)
    CASE_IGNORE_ERR_HELPER(OB_RPC_CONNECT_ERROR)
    CASE_IGNORE_ERR_HELPER(OB_RPC_SEND_ERROR)
    CASE_IGNORE_ERR_HELPER(OB_RPC_POST_ERROR)
    CASE_IGNORE_ERR_HELPER(OB_TENANT_NOT_IN_SERVER)
    default: {
      if (is_schema_error(error_code)) {
        should_ignore = true;
        const int ret = error_code;
        LOG_WARN("ignore schema error", KR(ret));
      }
      break;
    }
  }
  return should_ignore;
}

bool ObPxCheckAlive::is_in_blacklist(const common::ObAddr &addr, int64_t server_start_time)
{
  int ret = OB_SUCCESS;
  bool in_blacklist = false;
  obrpc::ObNetKeepAliveData alive_data;
  if (OB_FAIL(ObNetKeepAlive::get_instance().in_black(addr, in_blacklist, &alive_data))) {
    ret = OB_SUCCESS;
    in_blacklist = false;
  } else if (!in_blacklist && server_start_time > 0) {
    in_blacklist = alive_data.start_service_time_ >= server_start_time;
  }
  if (in_blacklist) {
    LOG_WARN("server in blacklist", K(addr), K(server_start_time), K(alive_data.start_service_time_));
  }
  return in_blacklist;
}

int LowestCommonAncestorFinder::find_op_common_ancestor(
    const ObOpSpec *left, const ObOpSpec *right, const ObOpSpec *&ancestor)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObOpSpec *, 32> ancestors;

  const ObOpSpec *parent = left;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret)) {
    if (OB_FAIL(ancestors.push_back(parent))) {
      LOG_WARN("failed to push back");
    } else {
      parent = parent->get_parent();
    }
  }

  parent = right;
  bool find = false;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret) && !find) {
    for (int64_t i = 0; i < ancestors.count() && OB_SUCC(ret); ++i) {
      if (parent == ancestors.at(i)) {
        find = true;
        ancestor = parent;
        break;
      }
    }
    parent = parent->get_parent();
  }
  return ret;
}

int LowestCommonAncestorFinder::get_op_dfo(const ObOpSpec *op, ObDfo *root_dfo, ObDfo *&op_dfo)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *parent = op;
  const ObOpSpec *dfo_root_op = nullptr;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret)) {
    if (IS_PX_COORD(parent->type_) || IS_PX_TRANSMIT(parent->type_)) {
      dfo_root_op = parent;
      break;
    } else {
      parent = parent->get_parent();
    }
  }
  ObDfo *dfo = nullptr;
  bool find = false;

  ObSEArray<ObDfo *, 16> dfo_queue;
  int64_t cur_que_front = 0;
  if (OB_FAIL(dfo_queue.push_back(root_dfo))) {
    LOG_WARN("failed to push back");
  }

  while (cur_que_front < dfo_queue.count() && !find && OB_SUCC(ret)) {
    int64_t cur_que_size = dfo_queue.count() - cur_que_front;
    for (int64_t i = 0; i < cur_que_size && OB_SUCC(ret); ++i) {
      dfo = dfo_queue.at(cur_que_front);
      if (dfo->get_root_op_spec() == dfo_root_op) {
        op_dfo = dfo;
        find = true;
        break;
      } else {
        // push child into the queue
        for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < dfo->get_child_count(); ++child_idx) {
          if (OB_FAIL(dfo_queue.push_back(dfo->get_child_dfos().at(child_idx)))) {
            LOG_WARN("failed to push back child dfo");
          }
        }
      }
      if (OB_SUCC(ret)) {
        cur_que_front++;
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_data_servers(ObExecContext &exec_ctx,
                                         sql::ObTMArray<ObAddr> &addrs,
                                         bool &is_empty,
                                         int64_t &data_node_cnt)
{
  int ret = OB_SUCCESS;
  DASTableLocList &table_locs = DAS_CTX(exec_ctx).get_table_loc_list();
  DASTabletLocArray locations;
  FOREACH_X(tmp_node, table_locs, OB_SUCC(ret)) {
    ObDASTableLoc *table_loc = *tmp_node;
    for (DASTabletLocListIter tablet_node = table_loc->tablet_locs_begin();
        OB_SUCC(ret) && tablet_node != table_loc->tablet_locs_end(); ++tablet_node) {
      OZ(locations.push_back(*tablet_node));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (locations.empty()) {
    is_empty = true;
  } else if (OB_FAIL(get_location_addrs<DASTabletLocArray>(locations, addrs))) {
    LOG_WARN("fail get location addrs", K(ret));
  } else {
    data_node_cnt = addrs.count();
    if (OB_FAIL(shuffle_px_node_pool(addrs, data_node_cnt))) {
      LOG_WARN("Fail to shuffle calc node pool", K(ret),
                K(data_node_cnt), K(addrs));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_data_servers(ObExecContext &exec_ctx,
                                         ObAddrSet &addr_set,
                                         bool &is_empty)
{
  int ret = OB_SUCCESS;
  DASTableLocList &table_locs = DAS_CTX(exec_ctx).get_table_loc_list();
  DASTabletLocArray locations;
  FOREACH_X(tmp_node, table_locs, OB_SUCC(ret)) {
    ObDASTableLoc *table_loc = *tmp_node;
    for (DASTabletLocListIter tablet_node = table_loc->tablet_locs_begin();
        OB_SUCC(ret) && tablet_node != table_loc->tablet_locs_end(); ++tablet_node) {
      OZ(locations.push_back(*tablet_node));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (locations.empty()) {
    is_empty = true;
  } else {
    addr_set.reuse();
    if (OB_FAIL(addr_set.create(locations.size()))) {
      LOG_WARN("fail create addr set", K(locations.size()), K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
      ret = addr_set.exist_refactored((locations.at(i))->server_);
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(addr_set.set_refactored((locations.at(i))->server_))) {
          LOG_WARN("fail set addr to addr_set", K(ret));
        }
      } else {
        LOG_WARN("fail check server exist in addr_set", K(ret));
      }
    }
  }
  return ret;
}

struct ObGetZonesOfServersCall
{
  ObGetZonesOfServersCall(
    const ObAddrSet &server_set,
    ObZoneSet &zone_set)
    :  server_set_(server_set),
        zone_set_(zone_set) {}
  int operator()(const ObServerInfoInTable &server_info);

  const ObAddrSet &server_set_;
  ObZoneSet &zone_set_;
};

int ObGetZonesOfServersCall::operator()(const ObServerInfoInTable &server_info)
{
  int ret = OB_SUCCESS;
  ret = server_set_.exist_refactored(server_info.get_server());
  if (OB_HASH_EXIST == ret) {
    ret = zone_set_.exist_refactored(server_info.get_zone());
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(zone_set_.set_refactored(server_info.get_zone()))) {
        LOG_WARN("fail set zone to zone_set", K(ret));
      }
    } else {
      LOG_WARN("fail check zone exist in zone_set", K(ret));
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail check server exist in addr_set", K(ret));
  }
  return ret;
}

struct ObGetServersOfZonesCall
{
  ObGetServersOfZonesCall(
    ObIArray<ObAddr> &servers,
    const ObZoneSet &zone_set,
    const ObAddrSet &data_server_set,
    const ObAddrSet &tenant_server_set)
    :  servers_(servers),
        zone_set_(zone_set),
        data_server_set_(data_server_set),
        tenant_server_set_(tenant_server_set) {}
  int operator()(const ObServerInfoInTable &server_info);

  ObIArray<ObAddr> &servers_;
  const ObZoneSet &zone_set_;
  const ObAddrSet &data_server_set_;
  const ObAddrSet &tenant_server_set_;
};

int ObGetServersOfZonesCall::operator()(const ObServerInfoInTable &server_info)
{
  int ret = OB_SUCCESS;
  ret = zone_set_.exist_refactored(server_info.get_zone());
  if (OB_HASH_EXIST == ret) {
    // Not a data node and belongs to the current tenant.
    ret = data_server_set_.exist_refactored(server_info.get_server());
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = tenant_server_set_.exist_refactored(server_info.get_server());
      if (OB_HASH_EXIST == ret) {
        if (OB_FAIL(servers_.push_back(server_info.get_server()))) {
          LOG_WARN("Fail to push back server", K(ret));
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail check server exist in tenant addrset", K(ret));
      }
    } else {
      LOG_WARN("fail check server exist in data addr_set", K(ret));
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail check server exist in addr_set", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::inner_get_zone_servers(const ObAddrSet &data_addr_set,
                                               ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObZoneSet zone_set;
  ObAddrSet tenant_addr_set;
  ObGetZonesOfServersCall get_zones_call(data_addr_set, zone_set);
  if (OB_FAIL(zone_set.create(data_addr_set.size()))) {
    LOG_WARN("zone_set failed to create", K(ret), K(data_addr_set.size()));
  } else if (OB_FAIL(SVR_TRACER.for_each_server_info(get_zones_call))) {
    LOG_WARN("Failed to for_each_server_info", K(ret));
  } else if (OB_FAIL(get_tenant_server_set(MTL_ID(), tenant_addr_set))) {
    LOG_WARN("Fail to get tenant server set", K(ret));
  }
  FOREACH_X(addr_iter, data_addr_set, OB_SUCC(ret)) {
    // Data nodes are placed at the front of the candidate node pool.
    if (OB_FAIL(addrs.push_back(addr_iter->first))) {
      LOG_WARN("addrs failed to push_back", K(ret));
    }
  }
  if (OB_SUCC(ret) && !tenant_addr_set.empty()) {
    ObGetServersOfZonesCall get_servers_call(addrs,
              zone_set, data_addr_set, tenant_addr_set);
    if (OB_FAIL(SVR_TRACER.for_each_server_info(get_servers_call))) {
      LOG_WARN("Failed to for_each_server_info", K(ret));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_zone_servers(ObExecContext &exec_ctx,
                                         sql::ObTMArray<ObAddr> &addrs,
                                         bool &is_empty,
                                         int64_t &data_node_cnt)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObAddrSet data_addr_set;
  if (OB_FAIL(get_data_servers(exec_ctx, data_addr_set, is_empty))) {
    LOG_WARN("Fail to get_data_servers", K(ret));
  } else if (is_empty) {
    // do nothing
  } else {
    data_node_cnt = data_addr_set.size();
    if (OB_FAIL(inner_get_zone_servers(data_addr_set, addrs))) {
      LOG_WARN("Failed to inner_get_zone_servers", K(ret));
    } else if (OB_FAIL(shuffle_px_node_pool(addrs, data_node_cnt))) {
      LOG_WARN("Fail to shuffle calc node pool", K(ret),
                K(data_node_cnt), K(addrs));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_tenant_server_set(const int64_t &tenant_id,
                                              ObAddrSet &tenant_server_set)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_op;
  ObTMArray<ObUnit> tenant_units;
  tenant_server_set.reuse();
  sql::ObTMArray<common::ObAddr> tenant_servers;
  int64_t renew_time = 0;
  if (OB_FAIL(SVR_TRACER.get_alive_tenant_servers(MTL_ID(),
                          tenant_servers, renew_time))) {
    LOG_WARN("Fail to get alive tenant servers", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(tenant_servers.empty())) {
    LOG_WARN("Unable to retrieve the machine list for the current tenant, "
               "reverting to PX_NODE_POLICY = DATA mode.", K(ret));
  } else if (OB_FAIL(tenant_server_set.create(tenant_servers.size()))) {
    LOG_WARN("fail create tenant_server_set", K(tenant_servers.size()), K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < tenant_servers.count(); ++i) {
      ret = tenant_server_set.exist_refactored(tenant_servers.at(i));
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(tenant_server_set.set_refactored(tenant_servers.at(i)))) {
          LOG_WARN("fail set addr to tenant_server_set", K(ret));
        }
      } else {
        LOG_WARN("fail check server exist in tenant_server_set", K(ret));
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_tenant_servers(const int64_t &tenant_id,
                                          ObIArray<ObAddr> &tenant_servers)
{
  int ret = OB_SUCCESS;
  int64_t renew_time = 0;
  if (OB_FAIL(SVR_TRACER.get_alive_tenant_servers(MTL_ID(),
                              tenant_servers, renew_time))) {
    LOG_WARN("Fail to get alive tenant servers", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(tenant_servers.empty())) {
    LOG_WARN("Unable to retrieve the machine list for the current tenant, "
               "reverting to PX_NODE_POLICY = DATA mode.", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::get_cluster_servers(ObExecContext &exec_ctx,
                                            sql::ObTMArray<ObAddr> &addrs,
                                            bool &is_empty,
                                            int64_t &data_node_cnt)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObAddrSet addr_set;
  ObTMArray<ObAddr> tenant_servers;
  if (OB_FAIL(get_data_servers(exec_ctx, addr_set, is_empty))) {
    LOG_WARN("Fail to get_data_servers", K(ret));
  } else if (is_empty) {
    // do nothing
  } else if (OB_FAIL(get_tenant_servers(MTL_ID(), tenant_servers))) {
    LOG_WARN("fail to get_units_by_tenant from inner_table",
            K(ret), K(MTL_ID()));
  } else {
    data_node_cnt = addr_set.size();
    FOREACH_X(addr_iter, addr_set, OB_SUCC(ret)) {
      // Data nodes are placed at the front of the candidate node pool.
      if (OB_FAIL(addrs.push_back(addr_iter->first))) {
        LOG_WARN("addrs failed to push_back", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < tenant_servers.count(); ++i) {
      ret = addr_set.exist_refactored(tenant_servers.at(i));
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(addrs.push_back(tenant_servers.at(i)))) {
          LOG_WARN("fail push back addr", K(ret));
        }
      } else {
        LOG_WARN("fail check server exist in addr_set", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(shuffle_px_node_pool(addrs, data_node_cnt))) {
        LOG_WARN("Fail to shuffle calc node pool", K(ret),
                  K(data_node_cnt), K(addrs));
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_specified_servers(ObExecContext &exec_ctx,
                                              sql::ObTMArray<ObAddr> &addrs,
                                              bool &is_empty,
                                              int64_t &data_node_cnt)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObAddrSet tenant_addr_set;
  const common::ObFixedArray<common::ObAddr, common::ObIAllocator> &px_node_addrs =
            exec_ctx.get_physical_plan_ctx()->get_phy_plan()->get_px_node_addrs();
  if (OB_FAIL(ObPXServerAddrUtil::get_tenant_server_set(MTL_ID(), tenant_addr_set))) {
    LOG_WARN("Fail to get tenant server set", K(ret));
  } else if (tenant_addr_set.empty()) {
    is_empty = true;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < px_node_addrs.count(); ++i) {
      ret = tenant_addr_set.exist_refactored(px_node_addrs.at(i));
      if (OB_HASH_EXIST == ret) {
        if (OB_FAIL(addrs.push_back(px_node_addrs.at(i)))) {
          LOG_WARN("fail push back addr", K(ret));
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        LOG_WARN("unexpected addr during the execution period",
                  K(ret), K(px_node_addrs.at(i)));
      } else {
        LOG_WARN("fail check server exist in tenant addrset", K(ret));
      }
    }
    data_node_cnt = px_node_addrs.count();
  }
  return ret;
}

int ObPXServerAddrUtil::shuffle_px_node_pool(sql::ObTMArray<ObAddr> &addrs,
                                              int64_t data_node_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t seed = ObTimeUtil::current_time();
  // Ensure that data nodes are placed at the beginning of the array
  // by separately shuffling the data nodes and the candidate compute nodes.
  if (data_node_cnt > 1) {
    std::shuffle(addrs.begin(), addrs.begin() + data_node_cnt,
                  std::default_random_engine(seed));
  }
  if (addrs.count() - data_node_cnt > 1) {
    std::shuffle(addrs.begin() + data_node_cnt, addrs.end(),
                  std::default_random_engine(seed));
  }
  return ret;
}

int ObPXServerAddrUtil::get_zone_server_cnt(const ObIArray<ObAddr> &server_list,
                                            int64_t &server_cnt)
{
  int ret = OB_SUCCESS;
  server_cnt = 0;
  ObAddrSet data_addr_set;
  if (OB_FAIL(data_addr_set.create(server_list.count()))) {
    LOG_WARN("data_addr_set failed to create", K(ret), K(server_list.count()));
  } else {
    for (int i = 0; i < server_list.count() && OB_SUCC(ret); ++i) {
      ret = data_addr_set.exist_refactored(server_list.at(i));
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(data_addr_set.set_refactored(server_list.at(i)))) {
          LOG_WARN("fail push back addr", K(ret));
        }
      } else {
        LOG_WARN("fail check server exist in data addrset", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObTMArray<ObAddr> zone_servers;
    if (OB_FAIL(inner_get_zone_servers(data_addr_set, zone_servers))) {
      LOG_WARN("Failed to inner_get_zone_servers", K(ret));
    } else {
      server_cnt = zone_servers.count();
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_cluster_server_cnt(const ObIArray<ObAddr> &server_list,
                                              int64_t &server_cnt)
{
  int ret = OB_SUCCESS;
  ObTMArray<ObAddr> tenant_servers;
  if (OB_FAIL(get_tenant_servers(MTL_ID(), tenant_servers))) {
    LOG_WARN("fail to get_units_by_tenant from inner_table",
            K(ret), K(MTL_ID()));
  } else {
    server_cnt = std::max(server_list.count(), tenant_servers.count());
  }
  return ret;
}

// for slave mapping under union all, the parent dfo may also contain scan ops,
// thus we should check the sqc addr is match
int ObPXServerAddrUtil::check_slave_mapping_location_constraint(ObDfo &child, ObDfo &parent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(child.get_sqcs_count() != parent.get_sqcs_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc count not match for slave_mapping", K(child.get_dfo_id()), K(parent.get_dfo_id()));
  } else {
    common::ObIArray<ObPxSqcMeta> &child_sqcs = child.get_sqcs();
    common::ObIArray<ObPxSqcMeta> &parent_sqcs = parent.get_sqcs();
    for (int64_t i = 0; i < child_sqcs.count() && OB_SUCC(ret); ++i) {
      bool match = false;
      const ObAddr &child_addr = child_sqcs.at(i).get_exec_addr();
      for (int64_t j = 0; j < parent_sqcs.count() && OB_SUCC(ret); ++j) {
        const ObAddr &parent_addr = parent_sqcs.at(j).get_exec_addr();
        if (child_addr == parent_addr) {
          match = true;
          break;
        }
      }
      if (OB_UNLIKELY(!match)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sqc addr not match", K(child.get_dfo_id()), K(parent.get_dfo_id()));
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::assign_extra_lake_table_files_to_sqc(ObExecContext &exec_ctx, ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  // handle nested QC
  if (dfo.get_sqcs_count() == 1 && dfo.is_fulltree() && exec_ctx.has_lake_table_file_map()) {
    ObSEArray<const ObTableScanSpec*, 2> scan_ops;
    const ObOpSpec *root_op = dfo.get_root_op_spec();
    ObPxSqcMeta &sqc = dfo.get_sqcs().at(0);
    ObLakeTableFileMap *lake_table_file_map = nullptr;
    if (OB_ISNULL(root_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null op spec", K(dfo));
    } else if (OB_FAIL(ObTaskSpliter::find_scan_ops_under_qc(scan_ops, *root_op))) {
      LOG_WARN("fail find scan ops in dfo", K(dfo), K(ret));
    } else if (OB_FAIL(exec_ctx.get_lake_table_file_map(lake_table_file_map))) {
      LOG_WARN("failed to get lake table file map");
    } else if (OB_ISNULL(lake_table_file_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else {
      ObLakeTableFileDesc &lake_table_file_desc = sqc.get_lake_table_file_desc();
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
        const ObTableScanSpec *scan_op = scan_ops.at(i);
        ObDASTableLoc *table_loc = NULL;
        if (OB_ISNULL(scan_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null scan op");
        } else if (!scan_op->is_lake_external_table()) {
          // do nothing
        } else if (OB_ISNULL(table_loc = DAS_CTX(exec_ctx).get_table_loc_by_id(scan_op->get_table_loc_id(),
                                                                               scan_op->get_loc_ref_table_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table loc");
        } else {
          const DASTabletLocList &locations = table_loc->get_tablet_locs();
          for (DASTabletLocListIter iter = table_loc->tablet_locs_begin();
               OB_SUCC(ret) && iter != table_loc->tablet_locs_end(); ++iter) {
            ObLakeTableFileMapKey key((*iter)->loc_meta_->table_loc_id_, (*iter)->tablet_id_);
            ObLakeTableFileArray *lake_table_file_array = nullptr;
            if (OB_FAIL(lake_table_file_map->get_refactored(key, lake_table_file_array))) {
              LOG_WARN("failed to get lake table file array", K(key));
            } else if (OB_FAIL(lake_table_file_desc.add_lake_table_file_desc(key, lake_table_file_array))) {
              LOG_WARN("failed to add lake table file desc");
            }
          }
        }
      }
    }
  }
  return ret;
}
