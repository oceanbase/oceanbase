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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_context.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{


int ObDASCtx::init(const ObPhysicalPlan &plan, ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  ObSEArray<ObObjectID, 2> partition_ids;
  ObSEArray<ObObjectID, 2> first_level_part_ids;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_my_session());
  const ObIArray<ObTableLocation> &normal_locations = plan.get_table_locations();
  const ObIArray<ObTableLocation> &das_locations = plan.get_das_table_locations();
  location_router_.set_last_errno(ctx.get_my_session()->get_retry_info().get_last_query_retry_err());
  location_router_.set_history_retry_cnt(ctx.get_my_session()->get_retry_info().get_retry_cnt());
  for (int64_t i = 0; OB_SUCC(ret) && i < das_locations.count(); ++i) {
    const ObTableLocation &das_location = das_locations.at(i);
    ObDASTableLoc *table_loc = nullptr;
    tablet_ids.reuse();
    partition_ids.reuse();
    first_level_part_ids.reuse();
    if (OB_FAIL(das_location.calculate_tablet_ids(ctx,
                                                  plan_ctx->get_param_store(),
                                                  tablet_ids,
                                                  partition_ids,
                                                  first_level_part_ids,
                                                  dtc_params))) {
      LOG_WARN("calculate partition ids failed", K(ret));
    } else if (OB_FAIL(extended_table_loc(das_location.get_loc_meta(), table_loc))) {
      LOG_WARN("extended table location failed", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); ++j) {
      ObDASTabletLoc *tablet_loc = nullptr;
      if (OB_FAIL(extended_tablet_loc(*table_loc, tablet_ids.at(j), tablet_loc, partition_ids.at(j),
                                      first_level_part_ids.empty() ? OB_INVALID_ID : first_level_part_ids.at(j)))) {
        LOG_WARN("extended tablet location failed", K(ret));
      }
    }
  }
  LOG_TRACE("init das context finish", K(ret), K(normal_locations), K(das_locations), K(table_locs_));
  return ret;
}

int ObDASCtx::get_das_tablet_mapper(const uint64_t ref_table_id,
                                    ObDASTabletMapper &tablet_mapper,
                                    const DASTableIDArrayWrap *related_table_ids)
{
  int ret = OB_SUCCESS;

  tablet_mapper.related_info_.related_map_ = &related_tablet_map_;
  tablet_mapper.related_info_.related_tids_ = related_table_ids;

  bool is_vt = is_virtual_table(ref_table_id);
  bool is_mapping_real_vt = is_oracle_mapping_real_virtual_table(ref_table_id);
  uint64_t real_table_id = ref_table_id;
  if (is_mapping_real_vt) {
    is_vt = false;
    real_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(ref_table_id);
  }
  const uint64_t tenant_id = MTL_ID();
  if (tablet_mapper.is_non_partition_optimized()) {
    // table ids has calced for no partition entity table, continue
  } else if (!is_vt) {
    //get ObTableSchema object corresponding to the table_id from ObSchemaGetterGuard
    //record the ObTableSchema into tablet_mapper
    //the tablet and partition info come from ObTableSchema in the real table
    ObSchemaGetterGuard *schema_guard = nullptr;
    if (OB_ISNULL(sql_ctx_) || OB_ISNULL(schema_guard = sql_ctx_->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is nullptr", K(ret), K(sql_ctx_), K(schema_guard));
    } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id,
                                                      real_table_id,
                                                      tablet_mapper.table_schema_))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(real_table_id));
    } else if (OB_ISNULL(tablet_mapper.table_schema_)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is not found", K(ret), K(real_table_id));
    } else {
      tablet_mapper.related_info_.guard_ = schema_guard;
    }
  } else {
    //get all server lists corresponding to the table_id from the tablet location cache
    //and record the server list in tablet_mapper
    //the tablet_id and partition id of the virtual table is the index of server list
    if (OB_FAIL(location_router_.get_vt_svr_pair(real_table_id, tablet_mapper.vt_svr_pair_))) {
      LOG_WARN("get virtual table server pair failed", K(ret), K(real_table_id));
    }
  }
  return ret;
}

ObDASTableLoc *ObDASCtx::get_table_loc_by_id(uint64_t table_loc_id, uint64_t ref_table_id)
{
  ObDASTableLoc *table_loc = nullptr;
  FOREACH(tmp_node, table_locs_) {
    if ((*tmp_node)->loc_meta_->table_loc_id_ == table_loc_id &&
        (*tmp_node)->loc_meta_->ref_table_id_ == ref_table_id) {
      table_loc = *tmp_node;
    }
  }
  return table_loc;
}

ObDASTableLoc *ObDASCtx::get_external_table_loc_by_id(uint64_t table_loc_id, uint64_t ref_table_id)
{
  ObDASTableLoc *table_loc = nullptr;
  FOREACH(tmp_node, external_table_locs_) {
    if ((*tmp_node)->loc_meta_->table_loc_id_ == table_loc_id &&
        (*tmp_node)->loc_meta_->ref_table_id_ == ref_table_id) {
      table_loc = *tmp_node;
    }
  }
  return table_loc;
}

int ObDASCtx::build_external_table_location(
    uint64_t table_loc_id,
    uint64_t ref_table_id,
    ObIArray<ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *local_location = NULL;
  ObDASTabletLoc *local_tablet_loc = NULL;
  if (OB_ISNULL(local_location = get_table_loc_by_id(table_loc_id, ref_table_id))
      || OB_ISNULL(local_tablet_loc = local_location->get_first_tablet_loc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected location", K(ret));
  } else {
    ObDASTableLoc *table_loc = NULL;
    ObDASTableLocMeta *loc_meta = NULL;

    if (OB_ISNULL(table_loc = OB_NEWx(ObDASTableLoc, (&allocator_), (allocator_)))
        || OB_ISNULL(loc_meta = OB_NEWx(ObDASTableLocMeta, (&allocator_), (allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(loc_meta->assign(*(local_tablet_loc->loc_meta_)))) {
      LOG_WARN("fail to assign loc meta", K(ret));
    } else {
      table_loc->loc_meta_ = loc_meta;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
      ObDASTabletLoc *tablet_loc = NULL;
      if (OB_ISNULL(tablet_loc = OB_NEWx(ObDASTabletLoc, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        tablet_loc->loc_meta_ = loc_meta;
        tablet_loc->tablet_id_ = local_tablet_loc->tablet_id_;
        tablet_loc->ls_id_ = local_tablet_loc->ls_id_;
        tablet_loc->server_ = locations.at(i);
        if (OB_FAIL(table_loc->add_tablet_loc(tablet_loc))) {
          LOG_WARN("fail to add tablet location", K(ret));
        }
      }
    }
    OZ (external_table_locs_.push_back(table_loc));
    LOG_DEBUG("external table distribution", K(locations), KPC(table_loc));
  }
  return ret;
}

int ObDASCtx::extended_tablet_loc(ObDASTableLoc &table_loc,
                                  const ObTabletID &tablet_id,
                                  ObDASTabletLoc *&tablet_loc,
                                  const common::ObObjectID &partition_id,
                                  const common::ObObjectID &first_level_part_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_loc.get_tablet_loc_by_id(tablet_id, tablet_loc))) {
    LOG_WARN("get tablet loc failed", KR(ret));
  }
  if (OB_SUCC(ret) && tablet_loc == nullptr) {
    LOG_DEBUG("tablet location is not exists, begin to construct it", K(table_loc), K(tablet_id));
    void *loc_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(loc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc failed", K(ret));
    } else if (OB_ISNULL(tablet_loc = new(loc_buf) ObDASTabletLoc())) {
      //do nothing
    } else if (OB_FAIL(location_router_.get_tablet_loc(*table_loc.loc_meta_,
                                                       tablet_id,
                                                       *tablet_loc))) {
      LOG_WARN("nonblock get tablet location failed", K(ret), KPC(table_loc.loc_meta_), K(tablet_id));
    } else if (OB_FAIL(table_loc.add_tablet_loc(tablet_loc))) {
      LOG_WARN("store tablet location info failed", K(ret));
    } else {
      tablet_loc->loc_meta_ = table_loc.loc_meta_;
      tablet_loc->partition_id_ = partition_id;
      tablet_loc->first_level_part_id_ = first_level_part_id;
    }
    //build related tablet location
    if (OB_SUCC(ret) && OB_FAIL(build_related_tablet_loc(*tablet_loc))) {
      LOG_WARN("build related tablet loc failed", K(ret), KPC(tablet_loc), KPC(tablet_loc->loc_meta_));
    }
    if (OB_SUCC(ret) && need_check_server_ && OB_FAIL(check_same_server(tablet_loc))) {
      LOG_WARN("check same server failed", KR(ret));
    }
  }
  return ret;
}

int ObDASCtx::check_same_server(const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_loc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet location is null", KR(ret), KP(tablet_loc));
  } else if (same_server_) {
    ObDASTabletLoc *first_tablet = NULL;
    FOREACH_X(table_node, table_locs_, NULL == first_tablet) {
      ObDASTableLoc *cur_table_loc = *table_node;
      for (DASTabletLocListIter tablet_node = cur_table_loc->tablet_locs_begin();
           NULL == first_tablet && tablet_node != cur_table_loc->tablet_locs_end();
           ++tablet_node) {
        first_tablet = *tablet_node;
      }
    }
    if (OB_ISNULL(first_tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first tablet location is null", KR(ret), KP(first_tablet));
    } else if (tablet_loc->server_ != first_tablet->server_) {
      same_server_ = false;
    }
  }
  return ret;
}

int ObDASCtx::extended_tablet_loc(ObDASTableLoc &table_loc,
                                  const ObCandiTabletLoc &candi_tablet_loc,
                                  ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  const ObOptTabletLoc &opt_tablet_loc = candi_tablet_loc.get_partition_location();
  if (OB_FAIL(table_loc.get_tablet_loc_by_id(opt_tablet_loc.get_tablet_id(), tablet_loc))) {
    LOG_WARN("get tablet loc failed", KR(ret), K(opt_tablet_loc.get_tablet_id()));
  }
  if (OB_SUCC(ret) && tablet_loc == nullptr) {
    ObLSReplicaLocation replica_loc;
    void *tablet_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(tablet_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc buf failed", K(ret), K(sizeof(ObDASTabletLoc)));
    } else if (OB_FAIL(candi_tablet_loc.get_selected_replica(replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(candi_tablet_loc));
    } else {
      tablet_loc = new(tablet_buf) ObDASTabletLoc();
      tablet_loc->server_ = replica_loc.get_server();
      tablet_loc->tablet_id_ = opt_tablet_loc.get_tablet_id();
      tablet_loc->ls_id_ = opt_tablet_loc.get_ls_id();
      tablet_loc->partition_id_ = opt_tablet_loc.get_partition_id();
      tablet_loc->first_level_part_id_ = opt_tablet_loc.get_first_level_part_id();
      tablet_loc->loc_meta_ = table_loc.loc_meta_;
      if (OB_FAIL(table_loc.add_tablet_loc(tablet_loc))) {
        LOG_WARN("store tablet loc failed", K(ret), K(tablet_loc));
      }
    }
    //build related tablet location
    if (OB_SUCC(ret) && OB_FAIL(build_related_tablet_loc(*tablet_loc))) {
      LOG_WARN("build related tablet loc failed", K(ret), KPC(tablet_loc), KPC(tablet_loc->loc_meta_));
    }
    if (OB_SUCC(ret) && need_check_server_ && OB_FAIL(check_same_server(tablet_loc))) {
      LOG_WARN("check same server failed", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObDASCtx::build_related_tablet_loc(ObDASTabletLoc &tablet_loc)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_loc.loc_meta_->related_table_ids_.count(); ++i) {
    ObTableID related_table_id = tablet_loc.loc_meta_->related_table_ids_.at(i);
    ObDASTableLoc *related_table_loc = nullptr;
    ObDASTabletLoc *related_tablet_loc = nullptr;
    const DASRelatedTabletMap::Value *rv = nullptr;
    void *related_loc_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(related_loc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc failed", K(ret));
    } else if (OB_ISNULL(related_table_loc = get_table_loc_by_id(tablet_loc.loc_meta_->table_loc_id_,
                                                                 related_table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get table loc by id failed", K(ret), KPC(tablet_loc.loc_meta_),
               K(related_table_id), K(table_locs_));
    } else if (OB_ISNULL(rv = related_tablet_map_.get_related_tablet_id(tablet_loc.tablet_id_,
                                                                        related_table_id))) {
      //related local index tablet_id pruning only can be used in local plan or remote plan(all operator
      //use the same das context),
      //because the distributed plan will transfer tablet_id through exchange operator,
      //but the related tablet_id map can not be transfered by exchange operator,
      //unused related pruning in distributed plan's dml operator,
      //we will use get_all_tablet_and_object_id() to build the related tablet_id map when
      //dml operator's table loc was inited
      if (OB_FAIL(build_related_tablet_map(*tablet_loc.loc_meta_))) {
        LOG_WARN("build related tablet map failed", K(ret), KPC(tablet_loc.loc_meta_));
      } else if (OB_ISNULL(rv = related_tablet_map_.get_related_tablet_id(tablet_loc.tablet_id_,
                                                                          related_table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get related tablet id failed", K(ret),
                 K(tablet_loc.tablet_id_), K(related_table_id), K(related_tablet_map_));
      }
    }
    if (OB_SUCC(ret)) {
      related_tablet_loc = new(related_loc_buf) ObDASTabletLoc();
      related_tablet_loc->tablet_id_ = rv->tablet_id_;
      related_tablet_loc->ls_id_ = tablet_loc.ls_id_;
      related_tablet_loc->server_ = tablet_loc.server_;
      related_tablet_loc->loc_meta_ = related_table_loc->loc_meta_;
      related_tablet_loc->next_ = tablet_loc.next_;
      related_tablet_loc->partition_id_ = rv->part_id_;
      related_tablet_loc->first_level_part_id_ = rv->first_level_part_id_;
      tablet_loc.next_ = related_tablet_loc;
      if (OB_FAIL(location_router_.save_touched_tablet_id(related_tablet_loc->tablet_id_))) {
        LOG_WARN("save touched tablet id failed", K(ret), KPC(related_tablet_loc));
      } else if (OB_FAIL(related_table_loc->add_tablet_loc(related_tablet_loc))) {
        LOG_WARN("add related tablet location failed", K(ret));
      }
    }
    LOG_DEBUG("build related tablet loc", K(ret), K(tablet_loc), KPC(related_tablet_loc),
              KPC(tablet_loc.loc_meta_), K(related_table_id));
  }
  return ret;
}

OB_INLINE int ObDASCtx::build_related_table_loc(ObDASTableLoc &table_loc)
{
  int ret = OB_SUCCESS;
  if (!table_loc.loc_meta_->related_table_ids_.empty()) {
    for (DASTabletLocListIter node = table_loc.tablet_locs_begin();
         OB_SUCC(ret) && node != table_loc.tablet_locs_end(); ++node) {
      ObDASTabletLoc *tablet_loc = *node;
      if (OB_FAIL(build_related_tablet_loc(*tablet_loc))) {
        LOG_WARN("build related tablet loc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASCtx::extended_table_loc(const ObDASTableLocMeta &loc_meta, ObDASTableLoc *&table_loc)
{
  int ret = OB_SUCCESS;
  table_loc = get_table_loc_by_id(loc_meta.table_loc_id_, loc_meta.ref_table_id_);
  if (nullptr == table_loc) {
    void *loc_buf = nullptr;
    if (OB_ISNULL(loc_buf = allocator_.alloc(sizeof(ObDASTableLoc)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate table loc failed", K(ret), K(sizeof(ObDASTableLoc)));
    } else if (OB_ISNULL(table_loc = new(loc_buf) ObDASTableLoc(allocator_))) {
      //do nothing
    } else if (OB_FAIL(table_locs_.push_back(table_loc))) {
      LOG_WARN("extended table location failed", K(ret));
    } else {
      table_loc->loc_meta_ = &loc_meta;
      LOG_DEBUG("extended table loc", K(loc_meta));
    }
    //to extended related table location
    for (int64_t i = 0; OB_SUCC(ret) && i < loc_meta.related_table_ids_.count(); ++i) {
      ObTableID related_table_id = loc_meta.related_table_ids_.at(i);
      ObDASTableLoc *related_table_loc = nullptr;
      void *related_loc_buf = allocator_.alloc(sizeof(ObDASTableLoc));
      void *loc_meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
      if (OB_ISNULL(related_loc_buf) || OB_ISNULL(loc_meta_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table loc failed", K(ret), K(related_loc_buf), K(loc_meta_buf));
      } else if (OB_ISNULL(related_table_loc = new(related_loc_buf) ObDASTableLoc(allocator_))) {
        //do nothing
      } else if (OB_FAIL(table_locs_.push_back(related_table_loc))) {
        LOG_WARN("extended table location failed", K(ret));
      } else {
        ObDASTableLocMeta *related_loc_meta = new(loc_meta_buf) ObDASTableLocMeta(allocator_);
        if (OB_FAIL(loc_meta.init_related_meta(related_table_id, *related_loc_meta))) {
          LOG_WARN("init related meta failed", K(ret), K(related_table_id));
        } else {
          related_table_loc->loc_meta_ = related_loc_meta;
        }
      }
    }
  }
  return ret;
}

int ObDASCtx::add_candi_table_loc(const ObDASTableLocMeta &loc_meta,
                                  const ObCandiTableLoc &candi_table_loc)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *table_loc = nullptr;
  ObDASTableLocMeta *final_meta = nullptr;
  LOG_DEBUG("das table loc assign begin", K(loc_meta));
  const ObCandiTabletLocIArray &candi_tablet_locs = candi_table_loc.get_phy_part_loc_info_list();
  if (OB_FAIL(ObDASUtils::build_table_loc_meta(allocator_, loc_meta, final_meta))) {
    LOG_WARN("build table loc meta failed", K(ret));
  } else if (OB_FAIL(extended_table_loc(*final_meta, table_loc))) {
    LOG_WARN("extended table loc failed", K(ret), K(loc_meta));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs.count(); ++i) {
    const ObCandiTabletLoc &candi_tablet_loc = candi_tablet_locs.at(i);
    ObDASTabletLoc *tablet_loc = nullptr;
    if (OB_FAIL(extended_tablet_loc(*table_loc, candi_tablet_loc, tablet_loc))) {
      LOG_WARN("extended tablet loc failed", K(ret));
    }
  }
  LOG_TRACE("das table loc assign finish", K(candi_table_loc), K(loc_meta), K(table_loc->get_tablet_locs()));
  return ret;
}

int ObDASCtx::get_all_lsid(share::ObLSArray &ls_ids)
{
  int ret = OB_SUCCESS;
  FOREACH_X(table_node, table_locs_, OB_SUCC(ret)) {
    ObDASTableLoc *table_loc = *table_node;
    for (DASTabletLocListIter tablet_node = table_loc->tablet_locs_begin();
         OB_SUCC(ret) && tablet_node != table_loc->tablet_locs_end(); ++tablet_node) {
      ObDASTabletLoc *tablet_loc = *tablet_node;
      if (!is_contain(ls_ids, tablet_loc->ls_id_)) {
        ret = ls_ids.push_back(tablet_loc->ls_id_);
      }
    }
  }
  return ret;
}

int64_t ObDASCtx::get_related_tablet_cnt() const
{
  int64_t total_cnt = 0;
  FOREACH(table_node, table_locs_) {
    ObDASTableLoc *table_loc = *table_node;
    total_cnt += table_loc->get_tablet_locs().size();
  }

  return total_cnt;
}

int ObDASCtx::rebuild_tablet_loc_reference()
{
  int ret = OB_SUCCESS;
  FOREACH_X(table_node, table_locs_, OB_SUCC(ret)) {
    ObDASTableLoc *table_loc = *table_node;
    ObTableID table_loc_id = table_loc->loc_meta_->table_loc_id_;
    if (table_loc->rebuild_reference_) {
      //has been rebuild the related table reference, ignore it
      continue;
    } else {
      table_loc->rebuild_reference_ = 1;
    }
    for (int64_t i = 0; i < table_loc->loc_meta_->related_table_ids_.count(); ++i) {
      ObTableID related_table_id = table_loc->loc_meta_->related_table_ids_.at(i);
      ObDASTableLoc *related_table_loc = get_table_loc_by_id(table_loc_id, related_table_id);
      related_table_loc->rebuild_reference_ = 1;
      if (table_loc->get_tablet_locs().size() != related_table_loc->get_tablet_locs().size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet location count not matched", K(ret),
                 KPC(table_loc), KPC(related_table_loc));
      }
      DASTabletLocList::iterator tablet_iter = table_loc->tablet_locs_begin();
      DASTabletLocList::iterator related_tablet_iter = related_table_loc->tablet_locs_begin();
      for (; OB_SUCC(ret) && tablet_iter != table_loc->tablet_locs_end();
          ++tablet_iter, ++related_tablet_iter) {
        ObDASTabletLoc *tablet_loc = *tablet_iter;
        ObDASTabletLoc *related_tablet_loc = *related_tablet_iter;
        related_tablet_loc->next_ = tablet_loc->next_;
        tablet_loc->next_ = related_tablet_loc;
        LOG_DEBUG("build related reference", KPC(related_tablet_loc), K(related_tablet_loc->next_),
                  KPC(tablet_loc), KPC(table_loc->loc_meta_), KPC(related_table_loc->loc_meta_));
      }
    }
  }
  return ret;
}

// For TP queries, we would like proxy to route tasks to servers where data
// is located. If partition_hit=false, proxy will refresh its location cache
// and route future tasks elsewhere. If partition_hit=true, proxy will continue
// route tasks here.
//
// In the following, we call an operator that starts a data flow a "driver table"
// and an operator that accepts input from a data flow a "driven table". For instance,
// in the query plan below, t1 is a "driver table" and t2 is a "driven table".
//
//    NLJ
//   /   \
//  t1   t2
//
// There are 4 cases:
// 1. there exists a driver table, and driven tables' partitions are located
//    on a single remote server.
// 2. there exists a driver table, and driven tables' partitions are located
//    across at least 2 servers or on local server.
// 3. there doesn't exist any driver tables, and partitions are located on a
//    single remote server.
// 4. there doesn't exist any driver tables, and partitions are located across
//    at least 2 servers or on local server.
//
// We set partition_hit and reroute as following:
// case           1  2  3  4
// partition_hit  F  T  F  T
// reroute        Y  N  N  N
bool ObDASCtx::is_partition_hit()
{
  bool bret = true;
  if (same_server_) {
    if (!table_locs_.empty() && !table_locs_.get_first()->get_tablet_locs().empty()) {
      if (MYADDR == table_locs_.get_first()->get_first_tablet_loc()->server_) {
        // all local partitions
        bret = true;
      } else {
        // all partitions are located on a single remote server
        bret = false;
      }
    }
  }
  return bret;
}

// For background, please see comments for ObDASCtx::is_partition_hit().
void ObDASCtx::unmark_need_check_server()
{
  if (!table_locs_.empty() && !table_locs_.get_first()->get_tablet_locs().empty()) {
    need_check_server_ = false;
  }
}

int ObDASCtx::build_related_tablet_map(const ObDASTableLocMeta &loc_meta)
{
  int ret = OB_SUCCESS;
  ObDASTabletMapper tablet_mapper;
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObObjectID> partition_ids;
  if (OB_FAIL(get_das_tablet_mapper(loc_meta.ref_table_id_, tablet_mapper, &loc_meta.related_table_ids_))) {
    LOG_WARN("get das tablet mapper failed", K(ret));
  } else if (OB_FAIL(tablet_mapper.get_all_tablet_and_object_id(tablet_ids, partition_ids))) {
    LOG_WARN("build related tablet_id map failed", K(ret), K(loc_meta));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDASCtx)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(table_locs_.size());
  FOREACH_X(tmp_node, table_locs_, OB_SUCC(ret)) {
    ObDASTableLoc *table_loc = *tmp_node;
    OB_UNIS_ENCODE(*table_loc);
    LOG_DEBUG("serialize das table location", K(ret), KPC(table_loc));
  }
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(snapshot_);
  OB_UNIS_ENCODE(location_router_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASCtx)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  OB_UNIS_DECODE(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObDASTableLoc *table_loc = nullptr;
    void *table_buf = allocator_.alloc(sizeof(ObDASTableLoc));
    if (OB_ISNULL(table_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate table loc buf failed", K(ret));
    } else {
      table_loc = new(table_buf) ObDASTableLoc(allocator_);
      if (OB_FAIL(table_locs_.push_back(table_loc))) {
        LOG_WARN("store table locs failed", K(ret));
      }
    }
    OB_UNIS_DECODE(*table_loc);
    OX(table_loc->rebuild_reference_ = 0);
    LOG_DEBUG("deserialized das table location", K(ret), KPC(table_loc));
  }
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(snapshot_);
  if (OB_SUCC(ret) && OB_FAIL(rebuild_tablet_loc_reference())) {
    LOG_WARN("rebuild tablet loc reference failed", K(ret));
  }
  OB_UNIS_DECODE(location_router_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASCtx)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(table_locs_.size());
  FOREACH(tmp_node, table_locs_) {
    ObDASTableLoc *table_loc = *tmp_node;
    OB_UNIS_ADD_LEN(*table_loc);
  }
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(snapshot_);
  OB_UNIS_ADD_LEN(location_router_);
  return len;
}
}  // namespace sql
}  // namespace oceanbase
