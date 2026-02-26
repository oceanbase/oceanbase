/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_i_model.h"
#include "observer/table/common/ob_table_query_session_mgr.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;

namespace oceanbase
{
namespace table
{

int ObIModel::get_ls_id(const ObTabletID &tablet_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;

  if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                          tablet_id,
                                          0, /* expire_renew_time */
                                          is_cache_hit,
                                          ls_id))) {
    LOG_WARN("fail to get ls id", K(ret), K(MTL_ID()), K(tablet_id));
  }

  return ret;
}

int ObIModel::check_same_ls(const ObIArray<ObTabletID> &tablet_ids,
                            bool &is_same,
                            ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSID first_ls_id(ObLSID::INVALID_LS_ID);
  ObLSID curr_ls_id(ObLSID::INVALID_LS_ID);
  bool only_one_ls = true;
  for (int64_t i = 0; i < tablet_ids.count() && OB_SUCC(ret) && only_one_ls; i++) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    if (OB_FAIL(get_ls_id(tablet_id, curr_ls_id))) {
      LOG_WARN("fail to get ls id", K(ret), K(tablet_id));
    } else if (i == 0) {
      first_ls_id = curr_ls_id;
    }
    only_one_ls = (curr_ls_id == first_ls_id);
  }

  if (OB_SUCC(ret)) {
    if (only_one_ls && first_ls_id.is_valid()) {
      is_same = true;
      ls_id = first_ls_id;
    }
  }

  return ret;
}

////////////////////////////////// ObTableQueryRequest ////////////////////////////////////////////
int ObIModel::prepare(ObTableExecCtx &ctx,
                      const ObTableQueryRequest &req,
                      ObTableQueryResult &res)
{
  int ret = OB_SUCCESS;
  bool is_same_ls = false;
  ObLSID ls_id(ObLSID::INVALID_LS_ID);
  ObTablePartClipType clip_type = req.query_.is_hot_only() ? ObTablePartClipType::HOT_ONLY : ObTablePartClipType::NONE;
  ObIArray<ObTabletID> &tablet_ids = const_cast<ObIArray<ObTabletID>&>(req.query_.get_tablet_ids());
  ObTablePartCalculator calculator(ctx.get_sess_guard(),
                                   ctx.get_schema_cache_guard(),
                                   ctx.get_schema_guard(),
                                   ctx.get_table_schema(),
                                   clip_type);

  if (OB_FAIL(calculator.calc(ctx.get_table_id(), req.query_.get_scan_ranges(), tablet_ids))) {
    LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(req.query_.get_scan_ranges()));
  } else if (tablet_ids.empty()) {
    ret = OB_ITER_END;
    LOG_DEBUG("all partitions are clipped", K(ret), K(req.query_));
  } else if (OB_FAIL(check_same_ls(tablet_ids, is_same_ls, ls_id))) {
    LOG_WARN("fail to check same ls", K(ret), K(tablet_ids));
  } else {
    if (is_same_ls) {
      ctx.set_ls_id(ls_id);
    }
  }
  LOG_DEBUG("ObIModel::prepare", K(ret), K(ctx), K(req.query_), K(tablet_ids));

  return ret;
}

////////////////////////////////// ObTableLSOpRequest /////////////////////////////////////////////
int ObIModel::prepare_allocate_result(const ObTableLSOpRequest &req,
                                      ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (1 != req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(req.ls_op_->count()));
  } else if (OB_FAIL(res.prepare_allocate(1))) {
    LOG_WARN("fail to prepare_allocate ls result", K(ret));
  } else {
    const int64_t op_count = req.ls_op_->at(0).count();
    ObTableTabletOpResult &tablet_result = res.at(0);
    if (OB_FAIL(tablet_result.prepare_allocate(op_count))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret), K(op_count));
    }
  }

  return ret;
}

int ObIModel::init_ls_id_tablet_op_map(const TabletIdOpsMap &tablet_map,
                                       LsIdTabletOpsMap &ls_map)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id(ObLSID::INVALID_LS_ID);
  bool is_cache_hit = false;
  TabletIdOpsMap::const_iterator it = tablet_map.begin();
  for (; it != tablet_map.end() && OB_SUCC(ret); ++it) {
    ObTabletID tablet_id(it->first);
    if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                            tablet_id,
                                            0, /* expire_renew_time */
                                            is_cache_hit,
                                            ls_id))) {
      LOG_WARN("fail to get ls id", K(ret), K(MTL_ID()), K(tablet_id));
    } else if (OB_FAIL(ls_map[ls_id.id()].push_back(&it->second))) {
      LOG_WARN("fail to push back tablet op", K(ret));
    }
  }

  return ret;
}

ObTablePartClipType ObIModel::get_clip_type(ObTableExecCtx &ctx, bool hot_only)
{
  ObTablePartClipType clip_type = ObTablePartClipType::NONE;
  bool is_table_group = ObHTableUtils::is_tablegroup_req(ctx.get_table_name(),  ObTableEntityType::ET_HKV);
  // table group request clip in cf service
  if (!is_table_group && hot_only) {
    clip_type = ObTablePartClipType::HOT_ONLY;
  }
  return clip_type;
}

int ObIModel::init_tablet_id_ops_map(ObTableExecCtx &ctx,
                                     const ObTableLSOpRequest &req,
                                     TabletIdOpsMap &map)
{
  int ret = OB_SUCCESS;
  bool is_batch_get = req.is_hbase_batch_get();
  bool is_query_and_mutate = req.is_hbase_query_and_mutate();

  if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (1 != req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(req.ls_op_->count()));
  } else {
    bool all_parts_are_clipped = true;
    bool only_one_ls = true;
    ObLSID first_ls_id(ObLSID::INVALID_LS_ID);
    ObLSID curr_ls_id(ObLSID::INVALID_LS_ID);
    const uint64_t table_id = req.ls_op_->get_table_id();
    ObTablePartCalculator calculator(ctx.get_sess_guard(),
                                     ctx.get_schema_cache_guard(),
                                     ctx.get_schema_guard(),
                                     ctx.get_table_schema());
    const ObTableTabletOp &tablet_op = req.ls_op_->at(0);
    const int64_t op_count = tablet_op.count();
    for (int64_t i = 0; i < op_count && OB_SUCC(ret); i++) {
      const ObTableSingleOp *op = &tablet_op.at(i);
      bool is_scan = op->get_op_type() == ObTableOperationType::SCAN;
      ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
      if (is_batch_get || is_query_and_mutate || is_scan) {
        const ObTableQuery *query = op->get_query();
        if (OB_ISNULL(query)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query is null", K(ret));
        } else {
          ObTablePartClipType clip_type = get_clip_type(ctx, query->is_hot_only());
          calculator.set_clip_type(clip_type);
          ObIArray<ObTabletID> &tablet_ids = const_cast<ObIArray<ObTabletID>&>(query->get_tablet_ids());
          if (OB_FAIL(calculator.calc(ctx.get_table_id(), query->get_scan_ranges(), tablet_ids))) {
            LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(query->get_scan_ranges()));
          } else if (tablet_ids.empty()) {
            // maybe all partitions are clipped
            LOG_DEBUG("tablet ids is empty", KPC(query));
          } else {
            all_parts_are_clipped = false;
            tablet_id = tablet_ids.at(0);
            bool is_same_ls = false;
            if (only_one_ls && OB_FAIL(check_same_ls(tablet_ids, is_same_ls, curr_ls_id))) {
              LOG_WARN("fail to check same ls", K(ret), K(tablet_ids));
            }
            LOG_DEBUG("chek same ls", K(is_batch_get), K(is_query_and_mutate), K(is_scan),
              K(only_one_ls), K(first_ls_id), K(is_same_ls), K(curr_ls_id), K(tablet_ids));
          }
        }
      } else {
        const ObITableEntity &entity = op->get_entities().at(0);
        if (OB_FAIL(calculator.calc(table_id, entity, tablet_id))) {
          LOG_WARN("fail to calc tablet id", K(ret), K(table_id), K(entity));
        } else {
          all_parts_are_clipped = false;
          const_cast<ObITableEntity&>(entity).set_tablet_id(tablet_id);
          bool is_same_ls = false;
          if (only_one_ls && OB_FAIL(get_ls_id(tablet_id, curr_ls_id))) {
            LOG_WARN("fail to get ls id", K(ret), K(tablet_id));
          }
        }
      }

      if (OB_SUCC(ret) && tablet_id.is_valid()) {
        if (i == 0) {
          first_ls_id = curr_ls_id;
        }
        map[tablet_id.id()].first = tablet_id.id();
        if (OB_FAIL(map[tablet_id.id()].second.push_back(op))) {
          LOG_WARN("fail to push back single op", K(ret));
        } else {
          only_one_ls = (curr_ls_id == first_ls_id) && curr_ls_id.is_valid();
        }
      }
    } // end for

    if (OB_SUCC(ret) && all_parts_are_clipped) {
      ret = OB_ITER_END;
      LOG_DEBUG("all paritions are clipped", K(req));
    }

    if (OB_SUCC(ret)) {
      if (only_one_ls && first_ls_id.is_valid()) {
        ctx.set_ls_id(first_ls_id); // local snapshot is enough
      }
    }

    LOG_DEBUG("init tablet id map", K(is_batch_get), K(is_query_and_mutate),
        K(only_one_ls), K(first_ls_id), K(tablet_op));
  }

  return ret;
}

int ObIModel::calc_single_op_tablet_id(ObTableExecCtx &ctx,
                                       ObTablePartCalculator &calculator,
                                       ObTableSingleOp &single_op,
                                       ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (single_op.get_op_type() == ObTableOperationType::SCAN ||
      single_op.get_op_type() == ObTableOperationType::QUERY_AND_MUTATE) {
    const ObTableQuery *query = single_op.get_query();
    if (OB_ISNULL(query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query is null", K(ret));
    } else {
      ObTablePartClipType clip_type = get_clip_type(ctx, query->is_hot_only());
      calculator.set_clip_type(clip_type);
      ObIArray<ObTabletID> &tablet_ids = const_cast<ObIArray<ObTabletID>&>(query->get_tablet_ids());
      if (OB_FAIL(calculator.calc(ctx.get_table_id(), query->get_scan_ranges(), tablet_ids))) {
        LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(query->get_scan_ranges()));
      } else if (tablet_ids.empty()) {
        // maybe all partitions are clipped
        LOG_DEBUG("tablet ids is empty", KPC(query));
      } else {
        tablet_id = tablet_ids.at(0);
      }
    }
  } else {
    const ObITableEntity &entity = single_op.get_entities().at(0);
    if (OB_FAIL(calculator.calc(ctx.get_table_id(), entity, tablet_id))) {
      LOG_WARN("fail to calc tablet id", K(ret), K(ctx.get_table_id()), K(entity));
    } else {
      const_cast<ObITableEntity&>(entity).set_tablet_id(tablet_id);
    }
  }
  return ret;
}

int ObIModel::init_request(const ObTableLSOpRequest &src_req,
                           const LsIdTabletOpsMap &ls_map,
                           ObIArray<ObTableLSOpRequest*> &new_reqs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(src_req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (1 != src_req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(src_req.ls_op_->count()));
  } else {
    const ObTableTabletOp &src_tablet_op = src_req.ls_op_->at(0);
    LsIdTabletOpsMap::const_iterator it = ls_map.begin();
    for (int64_t i = 0; it != ls_map.end() && OB_SUCC(ret); ++it, ++i) {
      int64_t ls_id = it->first;
      const TabletOps &map_tablet_ops = it->second;
      ObTableLSOpRequest *new_req = new_reqs.at(i);
      if (OB_ISNULL(new_req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new request is null", K(ret));
      } else {
        new_req->shaddow_copy_without_op(src_req);
        new_req->ls_op_->set_ls_id(ObLSID(ls_id));
        ObIArray<ObTableTabletOp> &new_tablet_ops = new_req->ls_op_->get_tablet_ops();
        const int64_t tablet_op_count = map_tablet_ops.count();
        if (OB_FAIL(new_tablet_ops.prepare_allocate(tablet_op_count))) {
          LOG_WARN("fail to prepare allocate tablet op", K(ret), K(tablet_op_count));
        }
        for (int64_t j = 0; j < tablet_op_count && OB_SUCC(ret); ++j) {
          const TabletOp *map_tablet_op = map_tablet_ops.at(j);
          ObTableTabletOp &new_tablet_op = new_tablet_ops.at(j);
          new_tablet_op.shaddow_copy_without_op(src_tablet_op);
          if (OB_ISNULL(map_tablet_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("map_tablet_op is null", K(ret));
          } else {
            ObTabletID tablet_id(map_tablet_op->first);
            new_tablet_op.set_tablet_id(tablet_id);
            const int64_t single_op_count = map_tablet_op->second.count();
            ObIArray<ObTableSingleOp> &new_single_ops = new_tablet_op.get_single_ops();
            if (OB_FAIL(new_single_ops.reserve(single_op_count))) {
              LOG_WARN("fail to reserve single op", K(ret), K(single_op_count));
            } else {
              for (int64_t k = 0; k < single_op_count && OB_SUCC(ret); k++) {
                const ObTableSingleOp *map_single_op = map_tablet_op->second.at(k);
                if (OB_ISNULL(map_single_op)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("single_op is null", K(ret));
                } else if (OB_FAIL(new_single_ops.push_back(*map_single_op))) {
                  LOG_WARN("fail to push back single op", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObIModel::alloc_requests_and_results(ObIAllocator &allocator,
                                         const int64_t count,
                                         ObIArray<ObTableLSOpRequest*> &reqs,
                                         ObIArray<ObTableLSOpResult*> &results)
{
  int ret = OB_SUCCESS;
  char *req_buf = nullptr;

  if (OB_FAIL(reqs.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate requests", K(ret), K(count));
  } else if (OB_FAIL(results.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate results", K(ret), K(count));
  } else if (OB_ISNULL(req_buf = static_cast<char *>(allocator.alloc(sizeof(ObTableLSOpRequest) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableLSOpRequest", K(ret), K(sizeof(ObTableLSOpRequest)), K(count));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      reqs.at(i) = new (req_buf + sizeof(ObTableLSOpRequest) * i) ObTableLSOpRequest();
      if (OB_FAIL(TABLEAPI_OBJECT_POOL_MGR->alloc_ls_op(reqs.at(i)->ls_op_))) {
        LOG_WARN("fail to alloc ls op", K(ret), K(i), K(count));
      } else if (OB_ISNULL(reqs.at(i)->ls_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls op object", K(ret), K(i), K(count));
      } else if (OB_FAIL(TABLEAPI_OBJECT_POOL_MGR->alloc_res(results.at(i)))) {
        LOG_WARN("fail to alloc ls result", K(ret), K(i), K(count));
      } else if (OB_ISNULL(results.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls result object", K(ret), K(i), K(count));
      }
    }
  }

  return ret;
}

int ObIModel::alloc_requests_and_results_for_mix_batch(ObTableExecCtx &ctx,
                                                       const int64_t count,
                                                       ObIArray<ObTableLSOpRequest*> &reqs,
                                                       ObIArray<ObTableLSOpResult*> &results)
{
  int ret = OB_SUCCESS;
  char *req_buf = nullptr;
  ObIAllocator *cb_allocator = ctx.get_cb_allocator();

  if (OB_FAIL(reqs.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate requests", K(ret), K(count));
  } else if (OB_FAIL(results.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate results", K(ret), K(count));
  } else if (OB_ISNULL(req_buf = static_cast<char *>(allocator_.alloc(sizeof(ObTableLSOpRequest) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableLSOpRequest", K(ret), K(sizeof(ObTableLSOpRequest)), K(count));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      reqs.at(i) = new (req_buf + sizeof(ObTableLSOpRequest) * i) ObTableLSOpRequest();
      ObTableLSOp *ls_op = nullptr;
      ObTableLSOpResult *ls_result = nullptr;
      if (OB_ISNULL(ls_op = OB_NEWx(ObTableLSOp, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ls op", K(ret));
      } else if (OB_ISNULL(cb_allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cb_allocator is NULL", K(ret));
      } else if (OB_ISNULL(ls_result = OB_NEWx(ObTableLSOpResult, cb_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ls result", K(ret));
      } else {
        reqs.at(i)->ls_op_ = ls_op;
        results.at(i) = ls_result;
      }
      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(ls_op)) {
          ls_op->~ObTableLSOp();
        }
        if (OB_NOT_NULL(ls_result)) {
          ls_result->~ObTableLSOpResult();
        }
      }
    } // end for
  }
  return ret;
}

int ObIModel::pre_init_results(ObTableExecCtx &ctx,
                               const ObTableLSOpRequest &src_req,
                               ObTableLSOpResult &src_res,
                               ObIArray<ObTableLSOpResult*> &results)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(src_req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (src_req.ls_op_->need_all_prop_bitmap()) {
    ObSEArray<ObString, 8> all_prop_name;
    if (OB_FAIL(ctx.get_schema_cache_guard().get_all_column_name(all_prop_name))) {
      LOG_WARN("fail to get all column name", K(ret));
    } else {
      for (int64_t i = 0; i < results.count() && OB_SUCC(ret); i++) {
        ObTableLSOpResult *res = results.at(i);
        if (OB_ISNULL(res)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new result is null", K(ret));
        } else if (OB_FAIL(res->assign_properties_names(all_prop_name))) {
          LOG_WARN("fail to assign property names to new result", K(ret));
        } else if (OB_FAIL(src_res.assign_properties_names(all_prop_name))) {
          LOG_WARN("fail to assign property names to result", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObIModel::alloc_and_init_request_result(ObTableExecCtx &ctx,
                                            const ObTableLSOpRequest &src_req,
                                            ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  TabletIdOpsMap tablet_map;
  LsIdTabletOpsMap ls_map;

  if (OB_FAIL(init_tablet_id_ops_map(ctx, src_req, tablet_map))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to init TabletIdOpsMap", K(ret));
    }
  } else if (OB_FAIL(init_ls_id_tablet_op_map(tablet_map, ls_map))) {
    LOG_WARN("fail to init LsIdTabletOpsMap", K(ret));
  } else if (OB_FAIL(alloc_requests_and_results(ctx.get_allocator(), ls_map.size(), new_reqs_, new_results_))) {
    LOG_WARN("fail to alloc request and results", K(ret));
  } else if (OB_FAIL(init_request(src_req, ls_map, new_reqs_))) {
    LOG_WARN("fail to init new requests", K(ret), K(src_req));
  } else if (OB_FAIL(pre_init_results(ctx, src_req, res, new_results_))) {
    LOG_WARN("fail to pre init results", K(ret), K(ctx), K(src_req));
  }

  return ret;
}

/*
To ensure the execute order in mix batch, we generate a new ls req for each single op:
case like :
- src req: put -> delete -> get
if aggreate src req by tablet_id, we may generate following new ls req:
  new_req_ls1:
    tablet_op_1: delete (may across tablet, and it will be added in the first tablet)
                 get (may across tablet, the same as delete)
    tablet_op_2: put
then:
  the execution order is out of expection: delete -> get -> put
so:
  we generate a new ls req for each sinle op and execute by its order
*/
int ObIModel::alloc_and_init_request_result_for_mix_batch(ObTableExecCtx &ctx,
                                                          const ObTableLSOpRequest &src_req,
                                                          ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  // use allocator to alloc reuqests and results
  is_alloc_from_pool_ = false;
  if (OB_ISNULL(src_req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (1 != src_req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(src_req.ls_op_->count()));
  } else {
    const ObTableTabletOp &tablet_op = src_req.ls_op_->at(0);
    const int64_t op_count = tablet_op.count();
    if (OB_FAIL(alloc_requests_and_results_for_mix_batch(ctx, op_count, new_reqs_, new_results_))) {
      LOG_WARN("fail to alloc request and results", K(ret), K(op_count));
    } else if (OB_FAIL(init_request_result_for_mix_batch(ctx, src_req, res, new_reqs_, new_results_))) {
      LOG_WARN("fail to alloc request and results for hyper batch", K(ret), K(src_req), K(op_count));
    }
  }
  return ret;
}

int ObIModel::init_request_result_for_mix_batch(ObTableExecCtx &ctx,
                                                const ObTableLSOpRequest &src_req,
                                                ObTableLSOpResult &src_res,
                                                ObIArray<ObTableLSOpRequest*> &new_reqs,
                                                ObIArray<ObTableLSOpResult*> &new_results)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_req.ls_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls op is null", K(ret));
  } else if (1 != src_req.ls_op_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should only has one tablet op", K(ret), K(src_req.ls_op_->count()));
  } else if (src_req.ls_op_->at(0).count() != new_reqs.count() ||
             src_req.ls_op_->at(0).count() != new_results.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new ls requests nums is not equal to single ops count", K(new_reqs.count()));
  } else {
    bool is_same_ls = false;
    ObLSID first_ls_id(ObLSID::INVALID_LS_ID);
    const ObTableTabletOp &src_tablet_op = src_req.ls_op_->at(0);
    ObTablePartCalculator calculator(ctx.get_sess_guard(),
                                     ctx.get_schema_cache_guard(),
                                     ctx.get_schema_guard(),
                                     ctx.get_table_schema());
    ObSEArray<ObString, 8> all_prop_name;
    all_prop_name.set_attr(ObMemAttr(MTL_ID(), "AllPropNames"));
    if (src_req.ls_op_->need_all_prop_bitmap()) {
      if (OB_FAIL(ctx.get_schema_cache_guard().get_all_column_name(all_prop_name))) {
        LOG_WARN("fail to get all column name", K(ret));
      } else if (OB_FAIL(src_res.assign_properties_names(all_prop_name))) {
        LOG_WARN("fail to assign property names to result", K(ret));
      }
    }
    for (int64_t i = 0; i < src_tablet_op.count() && OB_SUCC(ret); i++) {
      ObTableSingleOp &single_op = const_cast<ObTableSingleOp &>(src_tablet_op.at(i));
      ObTableLSOpRequest *new_req = new_reqs.at(i);
      ObTableLSOpResult *new_result = new_results.at(i);
      if (OB_ISNULL(new_req) || OB_ISNULL(new_result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new request or new result is null", K(ret), KP(new_req), KP(new_result),
            K(new_reqs), K(new_results), K(src_tablet_op.count()), K(i));
      } else {
        // init new request
        new_req->shaddow_copy_without_op(src_req);
        ObIArray<ObTableTabletOp> &new_tablet_ops = new_req->ls_op_->get_tablet_ops();
        if (OB_FAIL(new_tablet_ops.prepare_allocate(1))) {
          LOG_WARN("fail to prepare allocate tablet op", K(ret));
        } else {
          ObLSID ls_id(ObLSID::INVALID_LS_ID);
          ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
          ObTableTabletOp &new_tablet_op = new_tablet_ops.at(0);
          new_tablet_op.shaddow_copy_without_op(src_tablet_op);
          bool is_cache_hit = false;
          ObIArray<ObTableSingleOp> &new_single_ops = new_tablet_op.get_single_ops();
          if (OB_FAIL(calc_single_op_tablet_id(ctx, calculator, single_op , tablet_id))) {
            LOG_WARN("fail to calcat tablet id", K(ret), K(i), K(single_op));
          } else if (OB_FAIL(new_single_ops.reserve(1))) {
            LOG_WARN("fail to reserve single op", K(ret));
          } else if (OB_FAIL(new_single_ops.push_back(single_op))) {
            LOG_WARN("fail to push back single op", K(ret), K(i));
          } else if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                            tablet_id,
                                            0, /* expire_renew_time */
                                            is_cache_hit,
                                            ls_id))) {
            LOG_WARN("fail to get ls id", K(ret), K(MTL_ID()), K(tablet_id));
          } else if (i == 0 && FALSE_IT(first_ls_id = ls_id)) {
          } else {
            new_tablet_op.set_tablet_id(tablet_id);
            new_req->ls_op_->set_ls_id(ls_id);
            if (is_same_ls) {
              is_same_ls = first_ls_id == ls_id;
            }
          }
        }
      }
      // init new ls result
      if (OB_SUCC(ret) && src_req.ls_op_->need_all_prop_bitmap()) {
        if (OB_FAIL(new_result->assign_properties_names(all_prop_name))) {
          LOG_WARN("fail to assign property names to new result", K(ret));
        }
      }
    } // end for
    if (OB_SUCC(ret) && is_same_ls) {
      ctx.set_ls_id(first_ls_id);
    }
  }

  return ret;
}

/*
src_req: [0][1][2][...][n]
new_req_ls1:
    tablet_id0: [1][5][9][...]
    tablet_id1: [2][6][10][...]
    ...
new_req_ls2:
    tablet_id0: [3][7][11][...]
    tablet_id1: [4][8][12][...]
    ...
new_res_ls1:
    tablet_id0: [1][5][9][...]
    tablet_id1: [2][6][10][...]
    ...
new_res_ls2:
    tablet_id0: [3][7][11][...]
    tablet_id1: [4][8][12][...]
    ...
src_res: [0][1][2][...][n]
*/
int ObIModel::init_result(ObTableExecCtx &ctx,
                          const ObTableLSOpRequest &src_req,
                          ObTableLSOpResult &src_res)
{
  int ret = OB_SUCCESS;

  if (new_reqs_.count() == 0 || new_results_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty request or result", K(ret), K(new_reqs_.count()), K(new_results_.count()));
  } else if (new_reqs_.count() != new_results_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid request or result count", K(ret), K(new_reqs_.count()), K(new_results_.count()));
  } else if (1 != src_res.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet result", K(ret), K(src_res.count()));
  } else if (OB_ISNULL(src_req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (src_res.at(0).count() != src_req.ls_op_->at(0).count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid result count", K(ret), K(src_res.count()), K(src_req.ls_op_->at(0).count()));
  } else {
    ObTableTabletOpResult &src_tablet_res = src_res.at(0);
    const int64_t ls_count = new_reqs_.count();
    for (int64_t i = 0; i < ls_count && OB_SUCC(ret); i++) {
      const ObTableLSOpRequest *ls_req = new_reqs_.at(i);
      const ObTableLSOpResult *ls_res = new_results_.at(i);
      if (OB_ISNULL(ls_req) || OB_ISNULL(ls_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("req or res is null", K(ret), KPC(ls_req), KPC(ls_res));
      } else if (ls_req->ls_op_->count() != ls_res->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", K(ret), K(ls_req->ls_op_->count()), K(ls_res->count()));
      } else {
        const int64_t tablet_cnt = ls_req->ls_op_->count();
        for (int64_t j = 0; j < tablet_cnt && OB_SUCC(ret); j++) {
          const ObTableTabletOp &tablet_op = ls_req->ls_op_->at(j);
          const ObTableTabletOpResult &new_tablet_res = ls_res->at(j);
          for (int64_t k = 0; k < tablet_op.count() && OB_SUCC(ret); k++) {
            const int64_t index = tablet_op.at(k).get_index();
            const ObTableSingleOpResult &single_res = new_tablet_res.at(k);
            if (src_tablet_res.count() <= index) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("src_tablet_res count is unexpected", K(ret), K(src_tablet_res.count()), K(index));
            } else {
              src_tablet_res.at(index) = single_res;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_result(ctx, src_req, src_res))) {
      LOG_WARN("fail to check final result", K(ret), K(src_res), K(ctx));
    }
  }

  return ret;
}

int ObIModel::check_result(ObTableExecCtx &ctx,
                           const ObTableLSOpRequest &src_req,
                           ObTableLSOpResult &src_res)
{
  int ret = OB_SUCCESS;
  ObITableEntityFactory *entity_factory = ctx.get_entity_factory();

  if (OB_ISNULL(entity_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity_factory is null", K(ret));
  } else if (OB_ISNULL(src_req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls op is null", K(ret));
  } else if (1 != src_res.count() || 1 != src_req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet result", K(ret), K(src_res.count()), K(src_req.ls_op_->count()));
  } else if (src_res.at(0).count() != src_req.ls_op_->at(0).count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid result count", K(ret), K(src_res.count()), K(src_req.ls_op_->at(0).count()));
  } else {
    const ObTableTabletOp &src_tablet_op = src_req.ls_op_->at(0);
    ObTableTabletOpResult &src_tablet_res = src_res.at(0);
    for (int64_t i = 0; i < src_tablet_res.count() && OB_SUCC(ret); i++) {
      ObTableSingleOpResult &single_res = src_tablet_res.at(i);
      if (OB_ISNULL(single_res.get_entity())) { // maybe some keys not exist when do batch get, need fill empty entity.
        if (src_tablet_op.get_single_ops().empty() || src_tablet_op.get_single_ops().at(0).get_entities().empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid count", K(src_tablet_op));
        } else {
          const ObTableSingleOpEntity &req_entity = src_tablet_op.get_single_ops().at(0).get_entities().at(0);
          ObTableSingleOpEntity *entity = static_cast<ObTableSingleOpEntity *>(entity_factory->alloc());
          if (OB_ISNULL(entity)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc entity", K(ret));
          } else {
            entity->set_dictionary(&src_res.get_rowkey_names(), &src_res.get_properties_names());
            if (OB_FAIL(entity->construct_names_bitmap(req_entity))) { // directly use request bitmap as result bitmap
              LOG_WARN("fail to construct name bitmap", K(ret), K(req_entity));
            } else {
              single_res.set_entity(entity);
              single_res.set_err(OB_SUCCESS);
            }
          }
        }
      }
    }
  }

  return ret;
}

void ObIModel::free_requests_and_results(ObTableExecCtx &ctx)
{
  for (int64_t i = 0; i < new_reqs_.count() && i < new_results_.count(); i++) {
    if (is_alloc_from_pool_) {
      if (OB_NOT_NULL(new_reqs_.at(i))) {
        TABLEAPI_OBJECT_POOL_MGR->free_ls_op(new_reqs_.at(i)->ls_op_);
        new_reqs_.at(i)->ls_op_ = nullptr;
      }
      if (!ctx.is_async_commit()) { // async commit free in async callback
        TABLEAPI_OBJECT_POOL_MGR->free_res(new_results_.at(i));
        new_results_.at(i) = nullptr;
      }
    } else {
      if (OB_NOT_NULL(new_reqs_.at(i)) && OB_NOT_NULL(new_reqs_.at(i)->ls_op_)) {
        new_reqs_.at(i)->ls_op_->~ObTableLSOp();
        new_reqs_.at(i)->ls_op_ = nullptr;
      }
      if (!ctx.is_async_commit() && OB_NOT_NULL(new_results_.at(i))) { // async commit free in async callback
        new_results_.at(i)->~ObTableLSOpResult();
        new_results_.at(i) = nullptr;
      }
    }
  }
}

int ObIModel::prepare_allocate_and_init_result(ObTableExecCtx &ctx,
                                               const ObTableLSOpRequest &req,
                                               ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(prepare_allocate_result(req, res))) {
    LOG_WARN("fail to prepate allocate result", K(ret), K(req));
  } else if (OB_FAIL(init_result(ctx, req, res))) {
    LOG_WARN("fail to init result", K(ret), K(req));
  }

  return ret;
}

////////////////////////////////// ObTableQueryAsyncRequest ////////////////////////////////////////
int ObIModel::prepare(ObTableExecCtx &arg_ctx,
                      const ObTableQueryAsyncRequest &req,
                      ObTableQueryAsyncResult &res,
                      ObTableExecCtx *&ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = req.table_id_;
  const ObTabletID &tablet_id = req.tablet_id_;
  const ObString &table_name = req.table_name_;
  const ObQueryOperationType query_type = req.query_type_;
  uint64_t arg_sessid = req.query_session_id_;
  uint64_t real_sessid = ObTableQueryASyncMgr::INVALID_SESSION_ID;

  if (OB_FAIL(ObTableQueryASyncMgr::check_query_type(query_type))) {
    LOG_WARN("query type is invalid", K(ret), K(query_type));
  } else if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_session_id(real_sessid, arg_sessid, query_type))) {
    LOG_WARN("fail to get query session id", K(ret), K(arg_sessid), K(query_type));
  } else if (FALSE_IT(res.query_session_id_ = real_sessid)) {
  } else if (OB_FAIL(get_query_session(real_sessid, query_type, query_session_))) {
    LOG_WARN("fail to get query session", K(ret), K(real_sessid), K(query_type));
  } else if (ObQueryOperationType::QUERY_START == query_type) {
    if (OB_FAIL(query_session_->init_query_ctx(table_id, tablet_id, table_name,
                                               ObHTableUtils::is_tablegroup_req(table_name,
                                               query_session_->get_session_type()),
                                               arg_ctx.get_credential()))) {
      LOG_WARN("fail to init query ctx", K(ret), K(tablet_id), K(table_name));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(query_session_->get_or_create_exec_ctx(ctx))) {
      LOG_WARN("fail to get exec ctx from session", K(ret));
    } else if (OB_ISNULL(arg_ctx.get_audit_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("audit ctx is null", K(ret));
    } else if (FALSE_IT(ctx->set_audit_ctx(*arg_ctx.get_audit_ctx()))) {
    } else if (ObQueryOperationType::QUERY_START == query_type) {
      ObString tmp_table_name;
      if (OB_FAIL(ob_write_string(ctx->get_allocator(), table_name, tmp_table_name))) {
        LOG_WARN("fail to deep copy table name", K(ret), K(table_name));
      } else {
        bool is_same_ls = false;
        ObLSID ls_id(ObLSID::INVALID_LS_ID);
        ObIArray<ObTabletID> &tablet_ids = const_cast<ObIArray<ObTabletID>&>(req.query_.get_tablet_ids());
        ctx->set_table_id(table_id);
        ctx->set_table_name(tmp_table_name);
        ctx->set_timeout_ts(arg_ctx.get_timeout_ts());
        ObTablePartClipType clip_type = get_clip_type(arg_ctx, req.query_.is_hot_only());
        ObTablePartCalculator calculator(ctx->get_sess_guard(),
                                         ctx->get_schema_cache_guard(),
                                         ctx->get_schema_guard(),
                                         ctx->get_table_schema(),
                                         clip_type);
        if (OB_FAIL(calculator.calc(table_id, req.query_.get_scan_ranges(), tablet_ids))) {
          LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(req.query_.get_scan_ranges()));
        } else if (tablet_ids.empty()) {
          ret = OB_ITER_END;
          LOG_DEBUG("all paritions are clipped", K(ret), K(req.query_));
        } else if (OB_FAIL(check_same_ls(tablet_ids, is_same_ls, ls_id))) {
          LOG_WARN("fail to check same ls", K(ret), K(tablet_ids));
        } else {
          if (is_same_ls) {
            ctx->set_ls_id(ls_id);
          }
        }
      }
    }
  }
  return ret;
}

////////////////////////////////// ObTableQueryAsyncRequest ////////////////////////////////////////
int ObIModel::work(ObTableExecCtx &ctx,
                   const ObTableQueryAsyncRequest &req,
                   ObTableQueryAsyncResult &res)
{
  int ret = OB_SUCCESS;
  ObIAsyncQueryIter *query_iter = nullptr;
  const ObQueryOperationType query_type = req.query_type_;
  if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session should be initialized in prepare", K(ret));
  } else if (OB_FAIL(query_session_->get_or_create_query_iter(query_session_->get_session_type(), query_iter))) {
    LOG_WARN("fail to get or create async query iter", K(ret));
  } else {
    int64_t session_id = query_session_->get_session_id();
    WITH_CONTEXT(query_session_->get_memory_ctx()) {
      if (ObQueryOperationType::QUERY_START == query_type) {
        lease_timeout_period_ = query_iter->get_lease_timeout_period();
        query_session_->set_req_start_time(ObTimeUtility::current_monotonic_time());
        ret = query_iter->start(req, ctx, res);
      } else if (ObQueryOperationType::QUERY_NEXT == query_type) {
        ret = query_iter->next(ctx, res);
      } else if (ObQueryOperationType::QUERY_RENEW == query_type) {
        ret = query_iter->renew(res);
      } else if (ObQueryOperationType::QUERY_END == query_type) {
        ret = query_iter->end(res);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("query execution failed, need rollback", K(ret), K(query_type));
      } else if (res.is_end_) {
        // no more result, no need to set time info
      } else {
        if (ObQueryOperationType::QUERY_START == req.query_type_) {
          if (OB_FAIL(query_session_->alloc_req_timeinfo())) {
            LOG_WARN("fail to allocate req time info", K(ret));
          } else {
            // start time > end time, end time == 0, reentrant cnt == 1
            query_session_->update_req_timeinfo_start_time();
          }
        }
        query_session_->set_timeout_ts(); // update session timeout
        query_session_->set_in_use(false);
      }
    }
  }

  return ret;
}

} // end of namespace table
} // end of namespace oceanbase
