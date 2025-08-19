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
#include "ob_hbase_model.h"
#include "share/table/ob_table.h"
#include "observer/table/ob_table_context.h"
#include "observer/table/ob_table_op_wrapper.h"
#include "observer/table/ob_table_batch_service.h"
#include "observer/table/common/ob_table_sequential_grouper.h"
#include "observer/table/part_calc/ob_table_part_calc.h"
#include "observer/table/common/ob_table_query_session_mgr.h"
#include "observer/table/cf_service/ob_hbase_column_family_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::table;

int ObHBaseModel::check_mode_defense(ObTableExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  is_multi_cf_req_ = ObHTableUtils::is_tablegroup_req(ctx.get_table_name(), ObTableEntityType::ET_HKV);
  bool is_series_mode = ctx.get_schema_cache_guard().get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data version failed", K(ret));
  } else if (is_series_mode && data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support series model with data version less than 4_3_5_2", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "series model with data version less than 4_3_5_2");
  } else if (is_multi_cf_req_ && is_series_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("hbase series mode is not supported multi cf", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with multi column family");
  }

  return ret;
}
int ObHBaseModel::check_ls_op_defense(ObTableExecCtx &ctx, const ObTableLSOpRequest &req)
{
  int ret = OB_SUCCESS;
  bool is_series_mode = ctx.get_schema_cache_guard().get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;

  if (is_series_mode) {
    if (req.is_hbase_put() || (!req.is_hbase_batch() && req.is_hbase_query_and_mutate())) {
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("for timeseries hbase ls op, only put and delete is supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation type except hbase put and single delete");
    }
  }

  return ret;
}

int ObHBaseModel::check_mode_defense(ObTableExecCtx &ctx, const ObTableQueryRequest &req)
{
  int ret = OB_SUCCESS;
  bool is_series_mode = ctx.get_schema_cache_guard().get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;
  if (OB_FAIL(check_mode_defense(ctx))) {
    LOG_WARN("fail to check mode defense", K(ret));
  } else if (is_series_mode) {
    const ObTableQuery &query = req.query_;
    const ObKVParams &kv_params = query.get_ob_params();
    const ObHBaseParams* hbase_params = nullptr;
    const ObHTableFilter &hbase_filter = query.get_htable_filter();
    if (query.get_batch() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported batch query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with batch query");
    } else if (query.get_limit() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported limit query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with limit query");
    } else if (query.get_offset() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported offset query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with offset query");
    } else if (query.get_scan_order() != common::ObQueryFlag::Forward) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported reverse query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with reverse query");
    } else if (!hbase_filter.get_filter().empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported filter query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with filter query");
    } else if (hbase_filter.get_max_results_per_column_family() >= 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported max results per column family query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with max results per column family query");
    } else if (hbase_filter.get_row_offset_per_column_family() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported row offset query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with row offset query");
    } else if (kv_params.is_valid() && OB_FAIL(kv_params.get_hbase_params(hbase_params))) {
      LOG_WARN("get hbase param fail", K(ret), K(kv_params));
    } else if (hbase_params->caching_ > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported caching query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with caching query");
    } else if (hbase_params->check_existence_only_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported check existence only query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with check existence only query");
    } else if (hbase_params->allow_partial_results_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("hbase series mode is not supported allow partial results query", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table with allow partial results query");
    }
  } else if (ctx.get_schema_cache_guard().get_schema_flags().is_secondary_part_ &&
             req.query_.get_scan_order() != common::ObQueryFlag::Forward) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("secondary partitioned hbase table with reverse query is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "secondary partitioned hbase table with reverse query");
  }
  return ret;
}

int ObHBaseModel::replace_timestamp(ObTableExecCtx &ctx,
                                    ObTableLSOpRequest &req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_op_ is null", K(ret));
  } else if (1 != req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(req.ls_op_->count()));
  } else {
    int64_t now_ms = -ObHTableUtils::current_time_millis();
    ObTableTabletOp &tablet_op = req.ls_op_->at(0);
    const int64_t op_count = tablet_op.count();
    for (int64_t i = 0; i < op_count && OB_SUCC(ret); i++) {
      ObTableSingleOp &op = tablet_op.at(i);
      if (op.get_op_type() == ObTableOperationType::Type::SCAN) {
        // hbase_get operation, T_COL in entity is meaningless
      } else if (!op.is_user_specific_T()) {
        for (int64_t j = 0; j < op.get_entities().count() && OB_SUCC(ret); j++) {
          ObITableEntity &entity = op.get_entities().at(j);
          if (entity.get_rowkey_size() != 3) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
          } else {
            ObObj &t_obj = const_cast<ObObj&>(entity.get_rowkey().get_obj_ptr()[ObHTableConstants::COL_IDX_T]);  // column T
            ObHTableCellEntity3 htable_cell(&entity);
            bool row_is_null = htable_cell.last_get_is_null();
            int64_t timestamp = htable_cell.get_timestamp();
            bool timestamp_is_null = htable_cell.last_get_is_null();
            if (row_is_null || timestamp_is_null) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), K(row_is_null), K(timestamp_is_null));
            } else {
              t_obj.set_int(now_ms);
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObHBaseModel::replace_timestamp(ObTableExecCtx &ctx,
                                    ObTableQueryAndMutateRequest &req)
{
  int ret = OB_SUCCESS;

  int64_t now_ms = -ObHTableUtils::current_time_millis();
  bool is_user_specific_T = req.query_and_mutate_.is_user_specific_T();
  if (!is_user_specific_T) {
    ObTableBatchOperation &mutations = req.query_and_mutate_.get_mutations();
    for (int64_t i = 0; i < mutations.count() && OB_SUCC(ret); i++) {
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(mutations.at(i).get_entity(entity))) {
        LOG_WARN("fail to get entity", K(ret), K(mutations));
      } else if (mutations.at(i).type() == ObTableOperationType::DEL) {
        // do nothing
      } else if (OB_ISNULL(entity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("entity is null", K(ret));
      } else if (entity->get_rowkey_size() != 3) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
      } else {
        ObObj &t_obj = const_cast<ObObj&>(entity->get_rowkey().get_obj_ptr()[ObHTableConstants::COL_IDX_T]);  // column T
        ObHTableCellEntity3 htable_cell(entity);
        bool row_is_null = htable_cell.last_get_is_null();
        int64_t timestamp = htable_cell.get_timestamp();
        bool timestamp_is_null = htable_cell.last_get_is_null();
        if (row_is_null || timestamp_is_null) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(row_is_null), K(timestamp_is_null));
        } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) {
          t_obj.set_int(now_ms);
        }
      }
    }
  }

  return ret;
}

int ObHBaseModel::calc_tablets(ObTableExecCtx &ctx,
                               const ObTableQueryAndMutateRequest &req)
{
  int ret = OB_SUCCESS;
  bool is_user_specific_T = req.query_and_mutate_.is_user_specific_T();
  ObIArray<ObTabletID> &tablet_ids = const_cast<ObIArray<ObTabletID>&>(req.query_and_mutate_.get_query().get_tablet_ids());

  if (is_user_specific_T) {
    // do nothing, tablet is correct when T is user specified
  } else {
    const ObTableQuery &query = req.query_and_mutate_.get_query();
    bool is_same_ls = false;
    ObLSID ls_id(ObLSID::INVALID_LS_ID);
    ObTablePartClipType clip_type = query.is_hot_only() ? ObTablePartClipType::HOT_ONLY : ObTablePartClipType::NONE;
    ObTablePartCalculator calculator(ctx.get_allocator(),
                                     ctx.get_sess_guard(),
                                     ctx.get_schema_cache_guard(),
                                     ctx.get_schema_guard(),
                                     ctx.get_table_schema(),
                                     clip_type);
    ObTableBatchOperation &batch_op = const_cast<ObTableBatchOperation&>(req.query_and_mutate_.get_mutations());
    ObTabletID mutation_tablet_id(ObTabletID::INVALID_TABLET_ID);
    if (OB_FAIL(calculator.calc(ctx.get_table_id(), query.get_scan_ranges(), tablet_ids))) {
      LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(query.get_scan_ranges()));
    } else if (OB_FAIL(check_same_ls(tablet_ids, is_same_ls, ls_id))) {
      LOG_WARN("fail to check same ls", K(ret), K(tablet_ids));
    } else {
      if (is_same_ls) {
        ctx.set_ls_id(ls_id);
      }
      ObITableEntity *entity = nullptr;
      for (int i = 0; OB_SUCC(ret) && i < batch_op.count(); i++) {
        if (batch_op.at(i).type() == ObTableOperationType::DEL) {
          // do nothing
        } else if (OB_FAIL(batch_op.at(i).get_entity(entity))) {
          LOG_WARN("fail to get entity", K(ret), K(batch_op));
        } else if (OB_ISNULL(entity)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("entity is null", K(ret));
        } else if (OB_FAIL(calculator.calc(ctx.get_table_id(), *entity, mutation_tablet_id))) {
          LOG_WARN("fail to calc tablet_id", K(ret), K(ctx), K(entity));
        } else {
          entity->set_tablet_id(mutation_tablet_id);
        }
      }
    }
  }
  return ret;
}

int ObHBaseModel::lock_rows(ObTableExecCtx &ctx, const ObTableLSOpRequest &req)
{
  int ret = OB_SUCCESS;
  ObHTableLockHandle *lock_handle = nullptr;

  if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_op_ is null", K(ret));
  } else {
    const uint64_t table_id = req.ls_op_->get_table_id();
    if (1 != req.ls_op_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should only has one tablet op", K(ret), K(req.ls_op_->count()));
    } else {
      const ObIArray<ObTableSingleOp> &ops = req.ls_op_->at(0).get_single_ops();
      if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
        LOG_WARN("fail to get htable lock handle", K(ret));
      } else if (OB_ISNULL(lock_handle)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lock_handle is null", K(ret));
      } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(table_id, ops, *lock_handle, ObHTableLockMode::SHARED))) {
        LOG_WARN("fail to lock rowS", K(ret), K(req));
      } else {
        ctx.get_trans_param().lock_handle_ = lock_handle;
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(lock_handle)) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle);
    lock_handle = nullptr;
  }

  return ret;
}

int ObHBaseModel::lock_rows(ObTableExecCtx &ctx, const ObTableQueryAndMutateRequest &req)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = req.query_and_mutate_.get_query();
  ObHTableLockHandle *lock_handle = nullptr;
  if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (OB_ISNULL(lock_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock_handle is null", K(ret));
  } else if (OB_FAIL(ObHTableUtils::lock_htable_row(req.table_id_, query, *lock_handle, ObHTableLockMode::EXCLUSIVE))) {
    LOG_WARN("fail to lock row", K(ret), K_(req.table_id), K(query));
  } else {
    ctx.get_trans_param().lock_handle_ = lock_handle;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(lock_handle)) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle);
    lock_handle = nullptr;
  }

  return ret;
}

int ObHBaseModel::prepare(ObTableExecCtx &ctx,
                          const ObTableLSOpRequest &req,
                          ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  bool is_batch_get = req.is_hbase_batch_get();
  bool is_mix_batch = req.is_hbase_mix_batch();
  bool is_hbase_put = req.is_hbase_put();
  bool is_same_part_key = false;
  if (OB_FAIL(check_mode_defense(ctx))) {
    LOG_WARN("fail to check mode defense", K(ret), K(ctx));
  } else if (OB_FAIL(check_ls_op_defense(ctx, req))) {
    LOG_WARN("fail to check ls op defense", K(ret), K(ctx), K(req));
  } else if (!is_batch_get && OB_FAIL(replace_timestamp(ctx, const_cast<ObTableLSOpRequest&>(req)))) {
    LOG_WARN("fail to replace timestamp", K(ret), K(req));
  } else if (is_hbase_put && OB_FAIL(check_is_same_part_key(ctx, req, is_same_part_key))) {
    LOG_WARN("failed to check if is same part key", K(ret));
  } else if (is_same_part_key) {
    if (OB_FAIL(init_put_request_result(ctx, const_cast<ObTableLSOpRequest&>(req), res))) {
      LOG_WARN("fail to init put request and result", K(ret), K(ctx), K(req), K(res));
    } else {
      is_alloc_req_res_ = false;
    }
  } else {
    if (!is_mix_batch && OB_FAIL(alloc_and_init_request_result(ctx, req, res))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to alloc and init request and result", K(ret), K(ctx), K(req), K(res));
      }
    } else if (is_mix_batch && OB_FAIL(alloc_and_init_request_result_for_mix_batch(ctx, req, res))) {
      LOG_WARN("fail to alloc and init request and result for hyper batch", K(ret), K(ctx), K(req), K(res));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_batch_get && OB_FAIL(lock_rows(ctx, req))) {
    LOG_WARN("fail to lock rows", K(ret), K(req));
  }
  LOG_DEBUG("hbase mode prepare", K(ret), K(is_same_part_key), K(is_hbase_put), K(is_batch_get), K(is_mix_batch), K(req), K(res));
  return ret;
}

int ObHBaseModel::init_put_request_result(ObTableExecCtx &ctx,
                                          ObTableLSOpRequest &req,
                                          ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id(ObLSID::INVALID_LS_ID);
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObTablePartCalculator calculator(ctx.get_allocator(),
                              ctx.get_sess_guard(),
                              ctx.get_schema_cache_guard(),
                              ctx.get_schema_guard());
  ObTableTabletOp &tablet_op = req.ls_op_->at(0);
  ObTableSingleOp &single_op = tablet_op.at(0);
  bool is_cache_hit = false;
  if (OB_FAIL(calc_single_op_tablet_id(ctx, calculator, single_op , tablet_id))) {
    LOG_WARN("fail to calcat tablet id", K(ret), K(single_op));
  } else if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                                tablet_id,
                                                0, /* expire_renew_time */
                                                is_cache_hit,
                                                ls_id))) {
    LOG_WARN("fail to get ls id", K(ret), K(MTL_ID()), K(tablet_id));
  } else {
    tablet_op.set_tablet_id(tablet_id);
    req.ls_op_->set_ls_id(ls_id);
    ctx.set_ls_id(ls_id);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_reqs_.push_back(&req))) {
      LOG_WARN("fail to push back req", K(ret));
    } else if (req.ls_op_->need_all_prop_bitmap()) {
      ObSEArray<ObString, 8> all_prop_name;
      if (OB_FAIL(ctx.get_schema_cache_guard().get_all_column_name(all_prop_name))) {
        LOG_WARN("fail to get all column name", K(ret));
      } else if (OB_FAIL(res.assign_properties_names(all_prop_name))) {
        LOG_WARN("fail to assign property names to result", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_results_.push_back(&res))) {
        LOG_WARN("fail to push back result", K(ret));
      }
    }
  }
  return ret;
}

int ObHBaseModel::check_is_same_part_key(ObTableExecCtx &ctx,
                                         const ObTableLSOpRequest &req,
                                         bool &is_same)
{
  int ret = OB_SUCCESS;
  bool is_part_table = false;
  bool is_secondary_part = false;
  if (OB_FAIL(ctx.get_schema_cache_guard().is_partitioned_table(is_part_table))) {
    LOG_WARN("fail to get is partitioned table", K(ret));
  } else if (!is_part_table) {
    is_same = true;
  } else if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_op_ is null", K(ret));
  } else if (1 != req.ls_op_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only has one tablet op", K(ret), K(req.ls_op_->count()));
  } else if (OB_FAIL(ctx.get_schema_cache_guard().is_secondary_part_table(is_secondary_part))) {
    LOG_WARN("fail to get is_secondary_part_table", K(ret));
  } else {
    ObTableTabletOp &tablet_op = req.ls_op_->at(0);
    const int64_t op_count = tablet_op.count();
    bool is_same_part_key = true;
    ObITableEntity *first_entity = nullptr;
    for (int64_t i = 0; i < op_count && is_same_part_key; i++) {
      ObTableSingleOp &op = tablet_op.at(i);
      ObITableEntity &entity = op.get_entities().at(0);
      if (i == 0) {
        first_entity = &entity;
      } else {
        is_same_part_key = compare_part_key(*first_entity, entity, is_secondary_part);
      }
    } // end for
    is_same = is_same_part_key;
  }
  LOG_DEBUG("check is same part key", K(is_same), K(is_part_table), K(is_secondary_part));
  return ret;
}

bool ObHBaseModel::compare_part_key(ObITableEntity &first_entity,
                                    ObITableEntity &second_entity,
                                    bool is_secondary_part)
{
  ObHTableCellEntity3 first_cell(&first_entity);
  ObHTableCellEntity3 second_cell(&second_entity);
  bool is_same_part_key = true;
  if (first_cell.get_rowkey().compare(second_cell.get_rowkey()) != 0) {
    is_same_part_key = false;
  } else if (is_secondary_part) {
    if (first_cell.get_timestamp() != second_cell.get_timestamp()) {
      is_same_part_key = false;
    }
  }
  return is_same_part_key;
}

int ObHBaseModel::prepare(ObTableExecCtx &arg_ctx,
                          const ObTableQueryAsyncRequest &req,
                          ObTableQueryAsyncResult &res,
                          ObTableExecCtx *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIModel::prepare(arg_ctx, req, res, ctx))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to prepare", K(ret), K(arg_ctx), K(req), K(res), K(ctx));
    }
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableExecCtx is null", K(ret));
  } else if (OB_FAIL(check_mode_defense(*ctx, req))) {
    LOG_WARN("fail to check mode defense", K(ret), K(arg_ctx), K(req));
  }
  return ret;
}

int ObHBaseModel::prepare(ObTableExecCtx &ctx,
                          const ObTableQueryAndMutateRequest &req,
                          ObTableQueryAndMutateResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_mode_defense(ctx))) {
    LOG_WARN("fail to check mode defense", K(ret), K(ctx));
  } else if (OB_FAIL(replace_timestamp(ctx, const_cast<ObTableQueryAndMutateRequest&>(req)))) {
    LOG_WARN("fail to replace timestamp", K(ret), K(req));
  } else if (OB_FAIL(calc_tablets(ctx, req))) {
    LOG_WARN("fail to calc tablets", K(ret), K(req));
  } else if (OB_FAIL(lock_rows(ctx, req))) {
    LOG_WARN("fail to lock rows", K(ret), K(req));
  }

  return ret;
}

int ObHBaseModel::work(ObTableExecCtx &ctx,
                       const ObIArray<ObTableLSOpRequest*> &reqs,
                       ObIArray<ObTableLSOpResult*> &results)
{
  int ret = OB_SUCCESS;

  if (reqs.count() != results.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid request or result count", K(ret), K(reqs.count()), K(results.count()));
  }

  for (int64_t i = 0; i < reqs.count() && OB_SUCC(ret); i++) {
    const ObTableLSOpRequest *req = reqs.at(i);
    ObTableLSOpResult *res = results.at(i);

    if (OB_ISNULL(req) || OB_ISNULL(res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("req or res is null", K(ret), KPC(req), KPC(res));
    } else if (OB_FAIL(work(ctx, *req, *res))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to work", K(ret), KPC(req), KPC(res), K(i));
      }
    }
  }

  return ret;
}

int ObHBaseModel::work(ObTableExecCtx &ctx, const ObTableLSOpRequest &req, ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(req.ls_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_op_ is null", K(ret));
  } else {
    ObTableLSOp &ls_op = const_cast<ObTableLSOp&>(*req.ls_op_);
    ObHbaseCfServiceGuard cf_service_guard(ctx.get_allocator(), is_multi_cf_req_);
    ObHbaseColumnFamilyService *cf_service = nullptr;
    ctx.set_table_id(ls_op.get_table_id());
    LSOPGrouper grouper;

    // add single ops to grouper, group by operation type
    for (int i = 0; OB_SUCC(ret) && i < ls_op.count(); ++i) {
      ObTableTabletOp &tablet_op = ls_op.at(i);
      for (int j = 0; OB_SUCC(ret) && j < tablet_op.count(); ++j) {
        ObTableSingleOp &single_op = tablet_op.at(j);
        if (OB_FAIL(grouper.add_operation(tablet_op.get_tablet_id(), i, j, single_op))) {
          LOG_WARN("failed to group operation", K(ret), K(single_op), K(grouper));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(prepare_ls_result(*req.ls_op_, res))) {
      LOG_WARN("fail to prepare result", K(ret), K(req));
    } else if (OB_FAIL(cf_service_guard.get_cf_service(cf_service))) {
      LOG_WARN("failed to get column family service", K(ret));
    } else if (OB_ISNULL(cf_service)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create column family service", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < grouper.get_groups().count(); ++i) {
        LSOPGrouper::BatchGroupType *group = grouper.get_groups().at(i);
        if (OB_ISNULL(group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group is null", K(ret));
        } else {
          switch (group->type_) {
            case ObTableOperationType::INSERT_OR_UPDATE:
            case ObTableOperationType::DEL:
              if (OB_FAIL(process_mutation_group(ctx, *group, *cf_service))) {
                LOG_WARN("failed to process mutation group", K(ret), K(group));
              }
              break;
            case ObTableOperationType::QUERY_AND_MUTATE:  // Delete
              if (OB_FAIL(process_query_and_mutate_group(ctx, *group, *cf_service))) {
                LOG_WARN("failed to process query and mutate group", K(ret), K(group));
              }
              break;
            case ObTableOperationType::CHECK_AND_INSERT_UP:  // Check And Mutate
              if (OB_FAIL(process_check_and_mutate_group(ctx, *group, *cf_service))) {
                LOG_WARN("failed to process check and insert up group", K(ret), K(group));
              }
              break;
            case ObTableOperationType::SCAN:
              if (OB_FAIL(process_scan_group(ctx, *group, *cf_service, res))) {
                if (ret != OB_ITER_END) {
                  LOG_WARN("failed to process scan group", K(ret), K(group));
                }
              }
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported operation type", K(ret), K(group->type_));
              LOG_USER_WARN(OB_NOT_SUPPORTED, "this type in the ls operation");
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_ls_op_result(ctx, ls_op, res))) {
      LOG_WARN("failed to process ls op result", K(ret), K(ctx), K(ls_op));
    }
  }

  return ret;
}

int ObHBaseModel::work(ObTableExecCtx &ctx, const ObTableQueryRequest &req, ObTableQueryResult &res)
{
  int ret = OB_SUCCESS;
  ObHbaseCfServiceGuard cf_service_guard(ctx.get_allocator(), false);
  ObHbaseColumnFamilyService *cf_service = nullptr;
  ObHbaseQuery query(req.table_id_, req.tablet_id_, const_cast<ObTableQuery&>(req.query_));
  if (OB_FAIL(cf_service_guard.get_cf_service(cf_service))) {
    LOG_WARN("failed to get column family service", K(ret));
  } else if (OB_ISNULL(cf_service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create column family service", K(ret));
  } else if (OB_FAIL(sync_query(ctx, &query, cf_service, res))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to execute sync query", K(ret), K(query));
    }
  }
  return ret;
}

int ObHBaseModel::work(ObTableExecCtx &ctx,
                       const ObTableQueryAndMutateRequest &req,
                       ObTableQueryAndMutateResult &res)
{
  int ret = OB_SUCCESS;
  res.affected_rows_ = 0;
  ctx.get_trans_param().lock_handle_->set_tx_id(ctx.get_trans_param().trans_desc_->tid());
  const ObTableBatchOperation &mutations = req.query_and_mutate_.get_mutations();
  ObHbaseCfServiceGuard cf_service_guard(ctx.get_allocator(), false);
  ObHbaseColumnFamilyService *cf_service = nullptr;

  if (OB_FAIL(cf_service_guard.get_cf_service(cf_service))) {
    LOG_WARN("failed to get column family service", K(ret));
  } else if (OB_ISNULL(cf_service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create column family service", K(ret));
  } else if (mutations.count() > 0) {
    ObTableOperationType::Type op_type = mutations.at(0).type();
    switch (op_type) {
      case ObTableOperationType::INCREMENT:
      case ObTableOperationType::APPEND:
        ret = process_increment_append(ctx, req, res, *cf_service);
        break;
      case ObTableOperationType::DEL: // checkAndDelete
      case ObTableOperationType::INSERT_OR_UPDATE: // checkAndPut
        ret = process_check_and_mutate(ctx, req, res, *cf_service);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported mutation type", K(ret), K(op_type));
    }
  }

  return ret;
}

int ObHBaseModel::after_work(ObTableExecCtx &ctx, const ObTableLSOpRequest &req, ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (is_alloc_req_res_) {
    if (OB_FAIL(prepare_allocate_and_init_result(ctx, req, res))) {
      LOG_WARN("fail to prepare allocate and init result", K(ret));
    }
  } else {
    // when use origin req and result, reset reqs and results to avoid double free
    new_reqs_.reset();
    new_results_.reset();
  }
  return ret;
}

int ObHBaseModel::before_response(ObTableExecCtx &ctx, const ObTableLSOpRequest &req, ObTableLSOpResult &res)
{
  UNUSEDx(ctx, req, res);
  int ret = OB_SUCCESS;
  if (is_alloc_req_res_) {
    free_requests_and_results(ctx);
  } else {
    // when use origin req and result, reset reqs and results to avoid double free
    new_reqs_.reset();
    new_results_.reset();
  }
  return ret;
}

int ObHBaseModel::sync_query(ObTableExecCtx &ctx,
                             ObHbaseQuery* query,
                             ObHbaseColumnFamilyService* cf_service,
                             ObTableQueryResult &res)
{
  int ret = OB_SUCCESS;
  ObHbaseQueryResultIterator *iter = nullptr;

  if (OB_ISNULL(query) || OB_ISNULL(cf_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(query), KP(cf_service));
  } else if (OB_ISNULL(query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query is null", K(ret));
  } else if (OB_FAIL(cf_service->query(*query, ctx, iter))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to create iterator", K(ret), KP(query));
    }
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query iterator", K(ret), KP(query));
  } else {
    if (OB_FAIL(iter->get_next_result(res))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next results", K(ret));
      } else {
        ret = OB_SUCCESS;  // override ret
      }
    }
    iter->close();
    OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), iter);
  }
  return ret;
}

int ObHBaseModel::check_and_mutate(const ObTableQMParam &request,
                                   const ObTableExecCtx &exec_ctx,
                                   ObTableQueryAndMutateResult &result)
{
    return OB_NOT_IMPLEMENT;
}

int ObHBaseModel::multi_put(ObTableExecCtx &ctx, ObIArray<ObITableOp*> &ops)
{
    return OB_NOT_IMPLEMENT;
}

int ObHBaseModel::modify_htable_timestamp(ObHbaseTabletCells *tablet_cell)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_cell)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet cell is null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < tablet_cell->get_cells().count(); i++) {
      ObITableEntity *entity = tablet_cell->get_cells().at(i);
      if (OB_ISNULL(entity)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cell", K(ret), K(entity));
    } else {
      ObHTableCellEntity3 htable_cell(entity);
      bool row_is_null = htable_cell.last_get_is_null();
      int64_t timestamp = htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
      } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) {
        ObRowkey rowkey = entity->get_rowkey();
        ObObj &t_obj = const_cast<ObObj &>(rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_T]);  // column T
          t_obj.set_int(-ObHTableUtils::current_time_millis());
        }
      }
    }
  }
  return ret;
}

int ObHBaseModel::process_ls_op_result(ObTableExecCtx &ctx, const ObTableLSOp &ls_op, ObTableLSOpResult &ls_result)
{
  int ret = OB_SUCCESS;
  ObITableEntityFactory *entity_factory = ctx.get_entity_factory();
  ObTableSingleOpEntity *entity = nullptr;
  bool return_one_res = ls_op.return_one_result();

  if (OB_ISNULL(entity_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity_factory is null", K(ret));
  } else if (OB_ISNULL(entity = static_cast<ObTableSingleOpEntity *>(entity_factory->alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc entity", K(ret));
  } else if (ls_op.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet op count", K(ret), K(ls_op.count()));
  } else if (ls_op.at(0).count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid single op count", K(ret), K(ls_op.at(0).count()));
  } else {
    const ObTableSingleOp &single_op = ls_op.at(0).at(0);
    const ObTableSingleOpEntity &req_entity = single_op.get_entities().at(0);
    if (OB_FAIL(add_dict_and_bm_to_result_entity(ls_op, ls_result, req_entity, *entity))) {
      LOG_WARN("fail to add dict and bm to result entity", K(ret), K(ls_op));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (return_one_res) {
    ObTableTabletOpResult &tablet_op_result = ls_result.at(0);
    tablet_op_result.at(0).set_entity(entity);
    ObTableSingleOpResult &single_op_result = tablet_op_result.at(0);
    int affeceted_rows = 0;
    for (int i = 0; i < ls_op.count() && OB_SUCC(ret); i++) {
      const ObTableTabletOp &tablet_op = ls_op.at(i);
      for (int j = 0; j < tablet_op.count() && OB_SUCC(ret); j++) {
        const ObTableSingleOp &single_op = tablet_op.at(j);
        affeceted_rows += single_op.get_entities().count();
      }
    }
    single_op_result.set_affected_rows(affeceted_rows);
    single_op_result.set_errno(OB_SUCCESS);
  } else {
    for (int i = 0; i < ls_op.count() && OB_SUCC(ret); i++) {
      const ObTableTabletOp &tablet_op = ls_op.at(i);
      ObTableTabletOpResult &tablet_op_result = ls_result.at(i);
      int affeceted_rows = 0;
      for (int j = 0; j < tablet_op.count() && OB_SUCC(ret); j++) {
        const ObTableSingleOp &single_op = tablet_op.at(j);
        ObTableSingleOpResult &single_res = tablet_op_result.at(j);
        if (single_op.get_op_type() != ObTableOperationType::GET
            && single_op.get_op_type() != ObTableOperationType::SCAN) {
          single_res.set_errno(OB_SUCCESS);
          single_res.set_entity(entity);
          single_res.set_affected_rows(1);
        } else {
          const ObTableSingleOpEntity &req_entity = single_op.get_entities().at(0);
          ObTableSingleOpEntity *result_entity = static_cast<ObTableSingleOpEntity *>(single_res.get_entity());
          if (OB_FAIL(add_dict_and_bm_to_result_entity(ls_op, ls_result, req_entity, *result_entity))) {
            LOG_WARN("fail to add dict and bm to result entity", K(ret), K(ls_op));
          }
        }
      }
    }
  }

  return ret;
}


//----------------------------------- LSOP Operation Helper -----------------------------------

int ObHBaseModel::process_mutation_group(ObTableExecCtx &ctx,
                                         LSOPGrouper::BatchGroupType &group,
                                         ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  ObHbaseTableCells table_cells;
  table_cells.set_table_id(ctx.get_table_id());
  ObSEArray<ObHbaseTabletCells*, 8> tablet_array;

  for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); ++i) {
    const LSOPGrouper::OpInfo &op_info = group.ops_.at(i);
    ObHbaseTabletCells *tablet_cell = nullptr;
    bool found = false;
    for (int64_t j = 0; !found && j < tablet_array.count(); ++j) {
      if (tablet_array.at(j)->get_tablet_id() == op_info.index_) {
        tablet_cell = tablet_array.at(j);
        found = true;
      }
    }
    if (!found) {
      tablet_cell = OB_NEWx(ObHbaseTabletCells, &ctx.get_allocator());
      if (OB_ISNULL(tablet_cell)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate tablet cells", K(ret));
      } else if (OB_FAIL(tablet_array.push_back(tablet_cell))) {
        tablet_cell->~ObHbaseTabletCells();
        ctx.get_allocator().free(tablet_cell);
        LOG_WARN("failed to add tablet cells", K(ret), K(tablet_array));
      } else {
        tablet_cell->set_tablet_id(op_info.index_);
      }
    }

    for (int j = 0; OB_SUCC(ret) && j < op_info.op_->get_entities().count(); j++) {
      ObITableEntity *entity = &op_info.op_->get_entities().at(j);
      if (OB_FAIL(tablet_cell->get_cells().push_back(entity))) {
        LOG_WARN("failed to push back entity", K(ret));
      } else {
        entity->set_tablet_id(op_info.index_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_array.count(); ++i) {
      if (OB_FAIL(table_cells.get_tablet_cells_array().push_back(tablet_array.at(i)))) {
        LOG_WARN("failed to add tablet cells", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      switch (group.type_) {
        case ObTableOperationType::PUT:
        case ObTableOperationType::INSERT_OR_UPDATE:
          ret = cf_service.put(table_cells, ctx);
          break;
        case ObTableOperationType::DEL:
          ret = cf_service.del(table_cells, ctx);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unsupported operation type", K(ret), K(group.type_));
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to execute mutation", K(ret), K(group));
      }
    }
  }
  for (int64_t i = 0; i < tablet_array.count(); ++i) {
    if (OB_NOT_NULL(tablet_array.at(i))) {
      tablet_array.at(i)->~ObHbaseTabletCells();
      ctx.get_allocator().free(tablet_array.at(i));
    }
  }
  return ret;
}


int ObHBaseModel::process_query_and_mutate_group(ObTableExecCtx &ctx,
                                                 LSOPGrouper::BatchGroupType &group,
                                                 ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHbaseQuery*, 8> queries;
  queries.set_attr(ObMemAttr(MTL_ID(), "ObHModQAMut"));
  for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); ++i) {
    const LSOPGrouper::OpInfo &op_info = group.ops_.at(i);
    if (OB_ISNULL(op_info.op_->get_query())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query is null", K(ret), K(op_info));
    } else {
      ObHbaseQuery *query = nullptr;
      if (OB_ISNULL(query = OB_NEWx(ObHbaseQuery, &ctx.get_allocator(),
                                   ctx.get_table_id(),
                                   op_info.index_,
                                   *op_info.op_->get_query(),
                                   is_multi_cf_req_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create query", K(ret));
      } else if (OB_FAIL(queries.push_back(query))) {
        query->~ObHbaseQuery();
        ctx.get_allocator().free(query);
        LOG_WARN("failed to add query", K(ret), K(queries));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < queries.count(); ++i) {
      if (!is_multi_cf_req_) {
        if (OB_FAIL(cf_service.del(*queries.at(i), ctx))) {
          LOG_WARN("failed to process query", K(ret), K(*queries.at(i)));
        }
      } else {
        // Note: This object is a copy of ObHbaseCFIterator::iterable_result_
        // Usage specifications:
        // 1. External result container, cannot set_append_family(false) control append_family
        // 2. Special purpose: Retrieve original physical table name via qualifier's cf name
        ObTableQueryIterableResult wide_row; // copy of ObHbaseCFIterator::iterable_result_
        ObNewRow cell;
        ObHbaseQueryResultIterator *hbase_result_iter = nullptr;
        if (OB_FAIL(cf_service.query(*queries.at(i), ctx, hbase_result_iter))) {
          LOG_WARN("failed to query", K(ret), K(*queries.at(i)));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(hbase_result_iter->get_next_result(wide_row))) {
              if (ret != OB_ITER_END) {
                LOG_WARN("failed to get next result", K(ret), K(*queries.at(i)));
              }
            }
            if (ret == OB_ITER_END && wide_row.rows_.count() > 0) {
              ret = OB_SUCCESS;
            }
            while (OB_SUCC(ret)) {
              if (OB_FAIL(wide_row.get_row(cell))) {
                if (OB_ARRAY_OUT_OF_RANGE != ret) {
                  LOG_WARN("fail to get cell from iterable result", K(ret));
                }
              } else {
                int64_t timestamp = 0;
                cell.get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp);
                if (OB_UNLIKELY(timestamp <= 0)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid timestamp", K(ret), K(timestamp));
                } else {
                  cell.get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
                }
                ObString qualifier = cell.get_cell(ObHTableConstants::COL_IDX_Q).get_string(); // qualifier format: cf\0qualifier
                ObString family = qualifier.split_on('\0'); // now, qualifier is only qualifier
                cell.get_cell(ObHTableConstants::COL_IDX_Q).set_varbinary(qualifier);
                ObString tablegroup = ctx.get_table_name();
                ObSqlString real_table_name;
                if (OB_FAIL(real_table_name.append(tablegroup))) {
                  LOG_WARN("failed to append tablegroup", K(ret), K(tablegroup));
                } else if (OB_FAIL(real_table_name.append("$"))) {
                  LOG_WARN("failed to append $", K(ret), K(tablegroup));
                } else if (OB_FAIL(real_table_name.append(family))) {
                  LOG_WARN("failed to append family", K(ret), K(family));
                }
                const uint64_t database_id = ctx.get_credential().database_id_;
                ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
                const ObSimpleTableSchemaV2 *real_simple_schema = nullptr;
                if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(), database_id, real_table_name.string(), false/*is_index*/, real_simple_schema))) {
                  LOG_WARN("failed to get simple table schema", K(ret), K(real_table_name));
                } else if (OB_ISNULL(real_simple_schema)) {
                  ret = OB_TABLE_NOT_EXIST;
                  LOG_WARN("table not exist", K(ret), K(real_table_name));
                } else {
                  ObHbaseQuery new_query(queries.at(i)->get_query(), false);
                  // set real table id and invalid tablet id to avoid 4377
                  new_query.set_table_id(real_simple_schema->get_table_id());
                  if (OB_FAIL(cf_service.del(new_query, cell, ctx))) {
                    LOG_WARN("failed to delete cell", K(ret), K(new_query), K(cell));
                  }
                }
              }
            }
            if (ret == OB_ARRAY_OUT_OF_RANGE) {
              ret = OB_SUCCESS; // one wide rows iter end
            }
          }
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS; // query result iter end
          }
          if (OB_NOT_NULL(hbase_result_iter)) {
            hbase_result_iter->close();
            OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), hbase_result_iter);
          }
        }
      }
    }
  }
  for (int64_t i = 0; i < queries.count(); ++i) {
    if (OB_NOT_NULL(queries.at(i))) {
      queries.at(i)->~ObHbaseQuery();
      ctx.get_allocator().free(queries.at(i));
    }
  }
  return ret;
}

int ObHBaseModel::process_check_and_mutate_group(ObTableExecCtx &ctx,
                                                 LSOPGrouper::BatchGroupType &group,
                                                 ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); ++i) {
    const LSOPGrouper::OpInfo &op_info = group.ops_.at(i);
    ObTableQueryResult result;
    if (OB_ISNULL(op_info.op_->get_query())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query is null", K(ret), K(op_info));
    } else {
      ObHbaseQueryResultIterator *iter = nullptr;
      ObHbaseQuery *query = nullptr;
      if (OB_ISNULL(query = OB_NEWx(ObHbaseQuery,
                                    &ctx.get_allocator(),
                                    ctx.get_table_id(),
                                    op_info.index_,
                                    *op_info.op_->get_query(),
                                    is_multi_cf_req_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create query", K(ret));
      } else if (OB_FAIL(cf_service.query(*query, ctx, iter))) {
        LOG_WARN("failed to query", K(ret), KP(query));
      } else if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ObHbaseQueryResultIterator", K(ret));
      } else if (OB_FAIL(add_query_columns(ctx, result))) {
        LOG_WARN("failed to add query columns", K(ret));
      } else if (OB_FAIL(iter->get_next_result(result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next results", K(ret));
        } else {
          ret = OB_SUCCESS;  // override ret
        }
      }
      if (OB_SUCC(ret)) {
        bool passed = false;
        if (OB_FAIL(check_passed(ctx, &result, iter, passed))) {
          LOG_WARN("failed to check passed", K(ret), K(result), KP(iter));
        } else if (passed) {
          ObHbaseTableCells table_cells;
          table_cells.set_table_id(ctx.get_table_id());
          ObHbaseTabletCells *tablet_cell = nullptr;
          if (OB_ISNULL(tablet_cell = OB_NEWx(ObHbaseTabletCells, &ctx.get_allocator()))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate tablet cell", K(ret));
          } else {
            tablet_cell->set_tablet_id(op_info.index_);
            for (int64_t j = 0; OB_SUCC(ret) && j < op_info.op_->get_entities().count(); j++) {
              if (OB_ISNULL(op_info.op_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null op in op info", K(ret));
              } else {
                ObITableEntity *entity = &op_info.op_->get_entities().at(j);
                if (OB_FAIL(tablet_cell->get_cells().push_back(entity))) {
                  LOG_WARN("failed to push back entity", K(ret), KP(entity), K(op_info));
                }
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(table_cells.get_tablet_cells_array().push_back(tablet_cell))) {
              LOG_WARN("failed to add tablet cell", K(ret));
            } else if (OB_FAIL(cf_service.put(table_cells, ctx))) {
              LOG_WARN("failed to put", K(ret), K(table_cells));
            }
            OB_DELETEx(ObHbaseTabletCells, &ctx.get_allocator(), tablet_cell);
          }
        }
      }
      if (OB_NOT_NULL(iter)) {
        iter->close();
        OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), iter);
      }
      OB_DELETEx(ObHbaseQuery, &ctx.get_allocator(), query);
    }
  }
  return ret;
}

// LSOP: batch get
int ObHBaseModel::process_scan_group(ObTableExecCtx &ctx,
                                     LSOPGrouper::BatchGroupType &group,
                                     ObHbaseColumnFamilyService &cf_service,
                                     ObTableLSOpResult &res)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHbaseQuery*, 8> queries;
  queries.set_attr(ObMemAttr(MTL_ID(), "ObHModScan"));
  for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); ++i) {
    const LSOPGrouper::OpInfo &op_info = group.ops_.at(i);
    if (OB_ISNULL(op_info.op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op in op info", K(ret));
    } else {
      ObHbaseQuery *query = OB_NEWx(ObHbaseQuery, &ctx.get_allocator(),
                                  ctx.get_table_id(),
                                  op_info.index_,
                                  *op_info.op_->get_query(),
                                  true);
      if (OB_ISNULL(query)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create query", K(ret));
      } else {
        if (ObHTableUtils::is_get_all_qualifier(query->get_query().get_htable_filter().get_columns())) {
          query->set_use_wildcard_column_tracker(true);
        }
        if (OB_FAIL(queries.push_back(query))) {
          query->~ObHbaseQuery();
          ctx.get_allocator().free(query);
          LOG_WARN("failed to add query", K(ret), KP(query), K(queries));
        }
      }

    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < queries.count(); ++i) {
    ObTableSingleOpResult single_op_result;
    ObHbaseQueryResultIterator *iter = nullptr;
    if (OB_FAIL(cf_service.query(*queries.at(i), ctx, iter))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to process query", K(ret), KP(queries.at(i)));
      }
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ObHbaseQueryResultIterator", K(ret));
    } else if (OB_FAIL(aggregate_scan_result(ctx, *iter, single_op_result))) {
      LOG_WARN("failed to aggregate scan result", K(ret), KP(queries.at(i)), KP(iter));
    } else {
      int tablet_idx = group.ops_.at(i).tablet_idx_;
      int single_op_idx = group.ops_.at(i).op_idx_;
      res.at(tablet_idx).at(single_op_idx) = single_op_result;
    }
    if (OB_NOT_NULL(iter)) {
      iter->close();
      OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), iter);
    }
  }
  for (int64_t i = 0; i < queries.count(); ++i) {
    OB_DELETEx(ObHbaseQuery, &ctx.get_allocator(), queries.at(i));
  }
  return ret;
}

int ObHBaseModel::aggregate_scan_result(ObTableExecCtx &ctx,
                                        ObHbaseQueryResultIterator &result_iter,
                                        ObTableSingleOpResult &single_op_result)
{
  int ret = OB_SUCCESS;
  ObTableQueryIterableResult one_result;
  if (OB_FAIL(result_iter.get_next_result(one_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next results", K(ret));
    } else {
      ret = OB_SUCCESS;  // override ret
    }
  }
  if (OB_SUCC(ret)) {
    ObITableEntity *entity = ctx.get_entity_factory()->alloc();
    if (OB_ISNULL(entity)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate entity", K(ret));
    } else {
      ObNewRow new_row, copy_row;
      int64_t affected_rows = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(one_result.get_row(new_row))) {
          if (ret != OB_ARRAY_OUT_OF_RANGE) {
            LOG_WARN("failed to get row", K(ret), K(one_result));
          }
        } else if (OB_ISNULL(ctx.get_cb_allocator())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("callback allocator is null", K(ret));
        } else if (OB_FAIL(ob_write_row(*ctx.get_cb_allocator(), new_row, copy_row))) {
          LOG_WARN("failed to write row", K(ret), K(new_row));
        } else {
          ++affected_rows;
          if (OB_FAIL(entity->set_property(
                  ObHTableConstants::ROWKEY_CNAME_STR, copy_row.get_cell(ObHTableConstants::COL_IDX_K)))) {
            LOG_WARN("fail to add K value", K(ret), K(copy_row));
          } else if (OB_FAIL(entity->set_property(
                         ObHTableConstants::CQ_CNAME_STR, copy_row.get_cell(ObHTableConstants::COL_IDX_Q)))) {
            LOG_WARN("fail to add Q value", K(ret), K(copy_row));
          } else if (OB_FAIL(entity->set_property(
                         ObHTableConstants::VERSION_CNAME_STR, copy_row.get_cell(ObHTableConstants::COL_IDX_T)))) {
            LOG_WARN("fail to add T value", K(ret), K(copy_row));
          } else if (OB_FAIL(entity->set_property(
                         ObHTableConstants::VALUE_CNAME_STR, copy_row.get_cell(ObHTableConstants::COL_IDX_V)))) {
            LOG_WARN("fail to add V value", K(ret), K(copy_row));
          }
        }
      }
      if (OB_ARRAY_OUT_OF_RANGE == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        single_op_result.set_entity(entity);
        single_op_result.set_affected_rows(affected_rows);
        single_op_result.set_err(ret);
        single_op_result.set_type(ObTableOperationType::GET);
      }
    }
  }

  return ret;
}

//----------------------------------- Batch Operation Helper -----------------------------------

int ObHBaseModel::construct_del_query(ObHbaseTableCells &table_cells,
                                      ObTableExecCtx &exec_ctx,
                                      ObHbaseColumnFamilyService &cf_service,
                                      ObHbaseQuery &query)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObHbaseTabletCells *> &tablet_cells_arr = table_cells.get_tablet_cells_array();
  ObTableQuery &table_query = query.get_query();
  ObTablePartClipType clip_type = table_query.is_hot_only() ? ObTablePartClipType::HOT_ONLY : ObTablePartClipType::NONE;
  ObTablePartCalculator calculator(exec_ctx.get_allocator(),
                                   exec_ctx.get_sess_guard(),
                                   exec_ctx.get_schema_cache_guard(),
                                   exec_ctx.get_schema_guard(),
                                   exec_ctx.get_table_schema(),
                                   clip_type);
  uint64_t table_id = exec_ctx.get_table_id();
  query.set_table_id(table_id);
  for (int64_t i = 0; i < tablet_cells_arr.count(); i++) {
    const ObHbaseTabletCells *tablet_cells = tablet_cells_arr.at(i);
    if (OB_ISNULL(tablet_cells)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet cells", K(ret));
    } else {
      const ObIArray<ObITableEntity *> &cells = tablet_cells->get_cells();
      for (int64_t j = 0; OB_SUCC(ret) && j < cells.count(); j++) {
        ObITableEntity *cell = cells.at(j);
        if (OB_ISNULL(cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null cell", K(ret));
        } else if (OB_FAIL(cf_service.construct_query(*cell, exec_ctx, query))) {
          LOG_WARN("fail to construct query from del entity", K(ret), KPC(cell));
        } else if (table_query.get_scan_ranges().count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan ranges is empty", K(ret));
        } else if (OB_FAIL(calculator.calc(table_id,
                                           table_query.get_scan_ranges().at(0),
                                           table_query.get_tablet_ids()))) {
          LOG_WARN("failed to calc tablet id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHBaseModel::process_batch_mutation_group(ObTableExecCtx &ctx,
                                               BatchGrouper::BatchGroupType &group,
                                               ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  ObHbaseTableCells table_cells;
  table_cells.set_table_id(ctx.get_table_id());
  ObHbaseTabletCells *tablet_cell = nullptr;
  if (group.ops_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty group", K(ret));
  } else if (OB_ISNULL(tablet_cell = OB_NEWx(ObHbaseTabletCells, &ctx.get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate tablet cell", K(ret));
  } else {
    tablet_cell->set_tablet_id(group.ops_.at(0).index_);
    for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); ++i) {
      const BatchGrouper::OpInfo *op_info = &group.ops_.at(i);
      if (OB_FAIL(tablet_cell->get_cells().push_back(const_cast<ObITableEntity*>(&op_info->op_->entity())))) {
        LOG_WARN("failed to push back entity", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(table_cells.get_tablet_cells_array().push_back(tablet_cell))) {
    LOG_WARN("failed to add tablet cell", K(ret));
  } else {
    switch (group.type_) {
      case ObTableOperationType::INSERT_OR_UPDATE:
      case ObTableOperationType::PUT:
        if (OB_FAIL(cf_service.put(table_cells, ctx))) {
          LOG_WARN("failed to put", K(ret));
        }
        break;
      case ObTableOperationType::DEL: {
        SMART_VAR(ObTableQuery, table_query)
        {
          ObHbaseQuery query(table_query, false);
          if (OB_FAIL(construct_del_query(table_cells, ctx, cf_service, query))) {
            LOG_WARN("failed to construct del query", K(ret));
          } else if (OB_FAIL(cf_service.del(query, ctx))) {
            LOG_WARN("failed to del", K(ret));
          }
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("mutation type only support del or put ", K(ret), K(group.type_));
        LOG_USER_WARN(OB_NOT_SUPPORTED, "mutation type is not del or put");
    }
  }

  OB_DELETEx(ObHbaseTabletCells, &ctx.get_allocator(), tablet_cell);
  return ret;
}

//----------------------------------- HTable INCREMENT/APPEND Helper -----------------------------------

int ObHBaseModel::process_increment_append(ObTableExecCtx &ctx,
                                           const ObTableQueryAndMutateRequest &req,
                                           ObTableQueryAndMutateResult &res,
                                           ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  ObHbaseQuery query(req.table_id_, req.tablet_id_,
                    const_cast<ObTableQuery&>(req.query_and_mutate_.get_query()));
  ObHbaseQueryResultIterator *query_result_iter = nullptr;
  ObTableQueryResult wide_rows;

  if (OB_FAIL(cf_service.query(query, ctx, query_result_iter))) {
    LOG_WARN("failed to query", K(ret));
  } else if (OB_ISNULL(query_result_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query result iter is null", K(ret));
  } else if (OB_FAIL(add_query_columns(ctx, wide_rows))) {
    LOG_WARN("failed to add query columns", K(ret));
  } else if (OB_FAIL(query_result_iter->get_next_result(wide_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next results", K(ret));
    } else {
      ret = OB_SUCCESS;  // override ret
    }
  }

  if (OB_SUCC(ret)) {
    ObHbaseTableCells table_cells;
    table_cells.set_table_id(req.table_id_);
    ObHbaseTabletCells *tablet_cells = nullptr;
    if (OB_ISNULL(tablet_cells = OB_NEWx(ObHbaseTabletCells, &ctx.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tablet cells", K(ret));
    } else if (OB_FAIL(table_cells.get_tablet_cells_array().push_back(tablet_cells))) {
      tablet_cells->~ObHbaseTabletCells();
      ctx.get_allocator().free(tablet_cells);
      LOG_WARN("failed to push tablet cells", K(ret));
    } else if (OB_FAIL(generate_new_incr_append_table_cells(ctx, req, res, &wide_rows, table_cells))) {
      LOG_WARN("failed to generate new incr append table cells", K(ret));
    } else {
      // calc tablet ids
      ObTablePartCalculator calculator(ctx.get_allocator(),
                                       ctx.get_sess_guard(),
                                       ctx.get_schema_cache_guard(),
                                       ctx.get_schema_guard(),
                                       ctx.get_table_schema());
      ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
      if (tablet_cells->get_cells().count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet cells array is empty", K(ret));
      } else if (OB_FAIL(calculator.calc(table_cells.get_table_id(), *tablet_cells->get_cells().at(0), tablet_id))) {
        LOG_WARN("failed to calc tablet id", K(ret));
      } else {
        for (int i = 0; i < tablet_cells->get_cells().count() && OB_SUCC(ret); i++) {
          if (OB_ISNULL(tablet_cells->get_cells().at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet cells is null", K(ret));
          } else {
            tablet_cells->get_cells().at(i)->set_tablet_id(tablet_id);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cf_service.put(table_cells, ctx))) {
        LOG_WARN("failed to put table cells", K(ret));
      } else {
        res.affected_rows_ = 1;
      }
    }
    OB_DELETEx(ObHbaseTabletCells, &ctx.get_allocator(), tablet_cells);
  }

  if (OB_NOT_NULL(query_result_iter)) {
    query_result_iter->close();
    OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), query_result_iter);
  }
  return ret;
}

int ObHBaseModel::process_check_and_mutate(ObTableExecCtx &ctx,
                                           const ObTableQueryAndMutateRequest &req,
                                           ObTableQueryAndMutateResult &res,
                                           ObHbaseColumnFamilyService &cf_service)
{
  int ret = OB_SUCCESS;
  ObHbaseQuery query(req.table_id_, req.tablet_id_,
                    const_cast<ObTableQuery&>(req.query_and_mutate_.get_query()));
  ObHbaseQueryResultIterator *query_result_iter = nullptr;
  ObTableQueryResult wide_rows;
  bool passed = false;
  if (OB_FAIL(cf_service.query(query, ctx, query_result_iter))) {
    LOG_WARN("failed to query", K(ret));
  } else if (OB_ISNULL(query_result_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query result iter is null", K(ret));
  } else if (OB_FAIL(add_query_columns(ctx, wide_rows))) {
    LOG_WARN("failed to add query columns", K(ret));
  } else if (OB_FAIL(query_result_iter->get_next_result(wide_rows))) {
    if(OB_ITER_END != ret) {
      LOG_WARN("fa (OB_ITER_Eiled to get next results", K(ret));
    } else {
      ret = OB_SUCCESS;  // override ret
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_passed(ctx, &wide_rows, query_result_iter, passed))) {
    LOG_WARN("failed to check passed", K(ret));
  } else if (passed) {
    BatchGrouper grouper;
    const ObTableBatchOperation &mutations = req.query_and_mutate_.get_mutations();

    for (int i = 0; i < mutations.count() && OB_SUCC(ret); i++) {
      ObTableOperation &op = const_cast<ObTableOperation &>(mutations.at(i));
      if (OB_FAIL(grouper.add_operation(req.tablet_id_, 0, i, op))) {
        LOG_WARN("failed to add operation", K(ret), K(op), K(grouper));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      for (int i = 0; i < grouper.get_groups().count() && OB_SUCC(ret); i++) {
        BatchGrouper::BatchGroupType *group = grouper.get_groups().at(i);
        if (OB_ISNULL(group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group is null", K(ret));
        } else {
          switch (group->type_) {
            case ObTableOperationType::INSERT_OR_UPDATE:
            case ObTableOperationType::DEL:
              if (OB_FAIL(process_batch_mutation_group(ctx, *group, cf_service))) {
                LOG_WARN("failed to process batch mutation group", K(ret), K(group));
              }
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("mutation type only support del or put ", K(ret), K(group->type_));
              LOG_USER_WARN(OB_NOT_SUPPORTED, "mutation type is not del or put");
          }
          if (OB_SUCC(ret)) {
            res.affected_rows_++;
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(query_result_iter)) {
    query_result_iter->close();
    OB_DELETEx(ObHbaseQueryResultIterator, &ctx.get_allocator(), query_result_iter);
  }

  return ret;
}

int ObHBaseModel::check_passed(ObTableExecCtx &ctx,
                               ObTableQueryResult *wide_rows,
                               ObHbaseQueryResultIterator *result_iter,
                               bool &passed)
{
  int ret = OB_SUCCESS;
  bool expected_value_is_null = false;
  passed = false;

  if (OB_FAIL(check_expected_value_is_null(ctx, result_iter, expected_value_is_null))) {
    LOG_WARN("failed to check expected value is null", K(ret));
  } else if ((wide_rows->get_row_count() > 0 && !expected_value_is_null) ||
            (wide_rows->get_row_count() == 0 && expected_value_is_null)) {
    passed = true;
  }
  return ret;
}

int ObHBaseModel::generate_new_incr_append_table_cells(ObTableExecCtx &ctx,
                                                       const ObTableQueryAndMutateRequest &req,
                                                       ObTableQueryAndMutateResult &result,
                                                       ObTableQueryResult *wide_rows,
                                                       ObHbaseTableCells &table_cells)
{
  int ret = OB_SUCCESS;
  ObITableEntityFactory *entity_factory = ctx.get_entity_factory();
  ObSEArray<ObITableEntity*, 8> entities;
  ObSEArray<std::pair<common::ObString, int32_t>, OB_DEFAULT_SE_ARRAY_COUNT> columns;
  const ObTableBatchOperation &mutations = req.query_and_mutate_.get_mutations();
  if (OB_ISNULL(entity_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity_factory is null", K(ret));
  } else if (OB_FAIL(sort_qualifier(columns, mutations))) {
    LOG_WARN("failed to sort qualifier", K(ret));
  } else if (OB_FAIL(wide_rows->get_htable_all_entity(entities))) {
    LOG_WARN("failed to get all entity", K(ret));
  }
  ObITableEntity *old_entity = nullptr;
  ObIArray<table::ObHbaseTabletCells *> &tablet_cells_array = table_cells.get_tablet_cells_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    old_entity = nullptr;
    ObTableEntity *new_entity = nullptr;
    const ObTableOperation &op = mutations.at(columns.at(i).second);
    bool is_increment = ObTableOperationType::INCREMENT == op.type();
    const ObRowkey &rowkey = op.entity().get_rowkey();
    const ObObj &q_obj = rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_Q];  // column Q
    ObObj tmp_obj;
    for (int j = 0; OB_SUCC(ret) && j < entities.count(); j++) {
      if (OB_FAIL(entities.at(j)->get_rowkey_value(ObHTableConstants::COL_IDX_Q, tmp_obj))) {
        LOG_WARN("failed to get properties", K(ret));
      } else if (q_obj.get_string().compare(tmp_obj.get_string()) == 0) {
        old_entity = entities.at(j);
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_entity = static_cast<ObTableEntity *>(entity_factory->alloc()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc entity", K(ret));
    } else if (OB_FAIL(generate_new_value(ctx, req, old_entity, op.entity(), is_increment, *new_entity, result))) {
      LOG_WARN("failed to generate new value", K(ret));
    } else if (tablet_cells_array.count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet cells array", K(ret), K(tablet_cells_array.count()));
    } else if (OB_FAIL(tablet_cells_array.at(0)->get_cells().push_back(new_entity))) {
      LOG_WARN("fail to push bach entity", K(ret), K(tablet_cells_array.at(0)));
    }
  }
  return ret;
}

int ObHBaseModel::sort_qualifier(common::ObIArray<std::pair<common::ObString, int32_t>> &columns,
                                 const ObTableBatchOperation &mutations)
{
  int ret = OB_SUCCESS;
  const int64_t N = mutations.count();
  if (N <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty mutations", K(ret));
  }
  ObString htable_row;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    const ObTableOperation &mutation = mutations.at(i);
    const ObITableEntity &entity = mutation.entity();
    if (entity.get_rowkey_size() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
    } else {
      ObHTableCellEntity3 htable_cell(&entity);
      ObString row = htable_cell.get_rowkey();
      bool row_is_null = htable_cell.last_get_is_null();
      ObString qualifier = htable_cell.get_qualifier();
      bool qualifier_is_null = htable_cell.last_get_is_null();
      (void)htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null || qualifier_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null), K(qualifier_is_null));
      } else {
        if (0 == i) {
          htable_row = row;  // shallow copy
        } else if (htable_row != row) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rowkey not the same", K(ret), K(row), K(htable_row));
          break;
        }
        if (OB_FAIL(columns.push_back(std::make_pair(qualifier, i)))) {
          LOG_WARN("failed to push back", K(ret));
          break;
        }
      }
    }
  }

  return ret;
}


int ObHBaseModel::generate_new_value(ObTableExecCtx &ctx,
                                     const ObTableQueryAndMutateRequest &req,
                                     const ObITableEntity *old_entity,
                                     const ObITableEntity &src_entity,
                                     bool is_increment,
                                     ObTableEntity &new_entity,
                                     ObTableQueryAndMutateResult &result)
{
  int ret = OB_SUCCESS;
  ObHTableCellEntity3 htable_cell(&src_entity);
  bool row_is_null = htable_cell.last_get_is_null();
  const ObString delta_str = htable_cell.get_value();
  int64_t delta_int = 0;
  bool v_is_null = htable_cell.last_get_is_null();
  if (row_is_null || v_is_null) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is null or value is null", K(ret), K(row_is_null), K(v_is_null));
  } else if (is_increment && OB_FAIL(ObHTableUtils::java_bytes_to_int64(delta_str, delta_int))) {
    LOG_WARN("fail to convert bytes to integer", K(ret), K(delta_str));
  } else {
    ObString orig_str;
    int64_t new_ts = htable_cell.get_timestamp(); // default insert timestamp
    if (OB_NOT_NULL(old_entity)) { // 旧行存在，构造新值（base + delta）
      ObObj base_obj_v;
      if (OB_FAIL(old_entity->get_property(ObHTableConstants::VALUE_CNAME_STR, base_obj_v))) {
        LOG_WARN("failed to get value", K(ret), K(old_entity));
      } else {
        orig_str = base_obj_v.get_varbinary();
        if (is_increment) {
          int64_t orig_int = 0;
          if (OB_FAIL(ObHTableUtils::java_bytes_to_int64(orig_str, orig_int))) {
            LOG_WARN("fail to convert bytes to integer", K(ret), K(orig_str));
          } else {
            delta_int += orig_int;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObObj new_k, new_q, new_t, new_v, new_ttl;
      // K
      new_k.set_varbinary(htable_cell.get_rowkey());
      // Q
      new_q.set_varbinary(htable_cell.get_qualifier());
      // T
      new_t.set_int(new_ts);
      // V
      if (is_increment) {
        char *bytes = static_cast<char*>(ctx.get_allocator().alloc(sizeof(int64_t)));
        if (OB_ISNULL(bytes)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), KP(bytes));
        } else if (OB_FAIL(ObHTableUtils::int64_to_java_bytes(delta_int, bytes))) {
          LOG_WARN("fail to convert bytes", K(ret), K(delta_int));
        } else {
          ObString v(sizeof(int64_t), bytes);
          new_v.set_varbinary(v);
        }
      } else {
        if (orig_str.empty()) {
          new_v.set_varbinary(delta_str);
        } else {
          int32_t total_len = orig_str.length() + delta_str.length();
          char *bytes = static_cast<char*>(ctx.get_allocator().alloc(total_len));
          if (OB_ISNULL(bytes)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), KP(bytes));
          } else {
            MEMCPY(bytes, orig_str.ptr(), orig_str.length());
            MEMCPY(bytes + orig_str.length(), delta_str.ptr(), delta_str.length());
            ObString new_str(total_len, bytes);
            new_v.set_varbinary(new_str);
          }
        }
      }
      if (OB_SUCC(ret)) { // generate new entity
        bool cell_ttl_exist = false;
        int64_t ttl = INT64_MAX;
        if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::ROWKEY_CNAME_STR, new_k))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_k));
        } else if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::CQ_CNAME_STR, new_q))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_q));
        } else if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::VERSION_CNAME_STR, new_t))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_t));
        } else if (OB_FAIL(new_entity.set_property(ObHTableConstants::VALUE_CNAME_STR, new_v))) {
          LOG_WARN("fail to set value property", K(ret), K(new_v));
        } else if (OB_FAIL(ctx.get_schema_cache_guard().has_hbase_ttl_column(cell_ttl_exist))) {
          LOG_WARN("fail to check cell ttl", K(ret));
        } else if (cell_ttl_exist) {
          // TTL
          if (OB_FAIL(htable_cell.get_ttl(ttl))) {
            if (ret == OB_SEARCH_NOT_FOUND) {
              ret = OB_SUCCESS;
              if (OB_NOT_NULL(old_entity) &&
                  OB_FAIL(old_entity->get_property(ObHTableConstants::TTL_CNAME_STR, new_ttl))) {
                if (ret == OB_SEARCH_NOT_FOUND) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get cell ttl", K(ret), K(old_entity));
                }
              }
            } else {
              LOG_WARN("failed to get cell ttl", K(ret), K(htable_cell));
            }
          } else if (!htable_cell.last_get_is_null()) {
            new_ttl.set_int(ttl);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(new_entity.set_property(ObHTableConstants::TTL_CNAME_STR, new_ttl))) {
            LOG_WARN("fail to set ttl property", K(ret), K(new_ttl));
          }
        }
      }
      if (OB_SUCC(ret) && req.query_and_mutate_.return_affected_entity()) { // set return accected entity
        if (OB_FAIL(add_to_results(new_entity, req, result))) {
          LOG_WARN("fail to add to results", K(ret), K(new_k), K(new_q), K(new_t), K(new_v), K(new_ttl));
        }
      }
    }
  }
  return ret;
}

int ObHBaseModel::add_to_results(const ObTableEntity &new_entity,
                                 const ObTableQueryAndMutateRequest &req,
                                 ObTableQueryAndMutateResult &result)
{
  int ret = OB_SUCCESS;
  ObTableQueryResult &results = result.affected_entity_;
  if (results.get_property_count() <= 0) {
    if (OB_FAIL(results.append_property_names(new_entity.get_rowkey_names()))) {
      LOG_WARN("failed to deep copy name", K(ret), K(new_entity));
    } else if (OB_FAIL(results.append_property_names(new_entity.get_properties_names()))) {
      LOG_WARN("failed to deep copy name", K(ret), K(new_entity));
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObObj> &rowkey_objs = new_entity.get_rowkey_objs();
    const ObIArray<ObObj> &properties_objs = new_entity.get_properties_values();
    int num = rowkey_objs.count() + properties_objs.count();
    ObObj properties_values[num];
    int i = 0;
    for (; i < rowkey_objs.count(); i++) {
      properties_values[i] = rowkey_objs.at(i);
    }
    for (; i < num; i++) {
      properties_values[i] = properties_objs.at(i - rowkey_objs.count());
    }
    common::ObNewRow row(properties_values, num);
    if (OB_FAIL(results.add_row(row))) {
      LOG_WARN("failed to add row to results", K(ret), K(row));
    }
  }
  return ret;
}

//----------------------------------- QueryAndMutate Helper -----------------------------------
int ObHBaseModel::check_expected_value_is_null(ObTableExecCtx &ctx, ObHbaseQueryResultIterator *result_iter, bool &is_null)
{
  int ret = OB_SUCCESS;
  hfilter::Filter *hfilter = nullptr;
  is_null = false;
  if (OB_ISNULL(result_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null result iterator", K(ret));
  } else {
    hfilter = result_iter->get_filter();
    if (OB_ISNULL(hfilter)) {
      // do nothing, filter string is empty, only htable key is specified
    } else {
      hfilter::CheckAndMutateFilter *query_and_mutate_filter = dynamic_cast<hfilter::CheckAndMutateFilter *>(hfilter);
      if (OB_ISNULL(query_and_mutate_filter)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null query and mutate filter", K(ret));
      } else {
        is_null = query_and_mutate_filter->value_is_null();
      }
    }
  }
  return ret;
}

int ObHBaseModel::check_result_value_is_null(ObTableExecCtx &ctx, ObTableQueryResult *wide_rows, bool &is_null)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *entity = nullptr;
  is_null = false;
  if (OB_ISNULL(wide_rows)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null wide rows", K(ret));
  } else if (FALSE_IT(wide_rows->rewind())) {
  } else if (OB_FAIL(wide_rows->get_next_entity(entity))) {
    LOG_WARN("failed to get next entity", K(ret));
  } else {
    ObHTableCellEntity2 htable_cell(entity);
    ObString value_str;
    if (OB_FAIL(htable_cell.get_value(value_str))) {
      LOG_WARN("failed to get value", K(ret));
    } else if (value_str.empty()) {
      is_null = true;
    }
  }
  return ret;
}

int ObHBaseModel::get_query_session(uint64_t sessid, const ObQueryOperationType query_type, ObTableNewQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  ObITableQueryAsyncSession *i_query_session = nullptr;
  if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_query_session<ObTableNewQueryAsyncSession>(sessid, query_type, i_query_session))) {
    LOG_WARN("fail to get query session", K(ret), K(sessid));
  } else if (OB_ISNULL(query_session = dynamic_cast<ObTableNewQueryAsyncSession *>(i_query_session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to cast ObTableNewQueryAsyncSession", KP(i_query_session));
  } else {
    query_session->set_session_type(ObTableEntityType::ET_HKV);
  }

  return ret;
}

int ObHBaseModel::add_query_columns(ObTableExecCtx &ctx, ObTableQueryResult &res)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheGuard &schema_guard = ctx.get_schema_cache_guard();
  const ObIArray<ObTableColumnInfo *> &col_info_array = schema_guard.get_column_info_array();
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_info_array.count(); cell_idx++) {
    ObTableColumnInfo *col_info = col_info_array.at(cell_idx);
    if (OB_ISNULL(col_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(ret), K(cell_idx));
    } else if (OB_FAIL(res.add_property_name(col_info->column_name_))) {
      LOG_WARN("fail to fill column names into query result", K(ret), K(col_info->column_name_));
    }
  }  // end for
  return ret;
}

int ObHBaseModel::prepare(ObTableExecCtx &ctx, const ObTableQueryRequest &req, ObTableQueryResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_mode_defense(ctx, req))) {
    LOG_WARN("fail to check mode defense", K(ret), K(ctx));
  } else if (OB_FAIL(ObIModel::prepare(ctx, req, res))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to prepare", K(ret), K(ctx), K(req));
    }
  } else if (OB_FAIL(add_query_columns(ctx, res))) {
    LOG_WARN("fail to add query columns into result", K(ret), K(ctx));
  }
  return ret;
}

int ObHBaseModel::prepare_ls_result(const ObTableLSOp &ls_op, ObTableLSOpResult &ls_res)
{
  int ret = OB_SUCCESS;
  bool return_one_res = ls_op.return_one_result();

  if (!return_one_res) {
    if (OB_FAIL(ls_res.prepare_allocate(ls_op.count()))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret));
    }
  } else {
    if (OB_FAIL(ls_res.prepare_allocate(1))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (return_one_res) {
    ObTableTabletOpResult &tablet_op_result = ls_res.at(0);
    if (OB_FAIL(tablet_op_result.prepare_allocate(1))) {
      LOG_WARN("fail to prepare_allocate single op result", K(ret), K(1));
    }
  } else {
    for (int i = 0; i < ls_op.count() && OB_SUCC(ret); i++) {
      const ObTableTabletOp &tablet_op = ls_op.at(i);
      ObTableTabletOpResult &tablet_op_result = ls_res.at(i);
      if (OB_FAIL(tablet_op_result.prepare_allocate(tablet_op.count()))) {
        LOG_WARN("fail to prepare_allocate single op result", K(ret), K(tablet_op.count()));
      }
    }
  }

  return ret;
}

int ObHBaseModel::add_dict_and_bm_to_result_entity(const ObTableLSOp &ls_op,
                                                   ObTableLSOpResult &ls_result,
                                                   const ObTableSingleOpEntity &req_entity,
                                                   ObTableSingleOpEntity &result_entity)
{
  int ret = OB_SUCCESS;
  if (ls_op.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet op count", K(ret), K(ls_op.count()));
  } else if (ls_op.at(0).count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid single op count", K(ret), K(ls_op.at(0).count()));
  } else {
    const ObTableSingleOp &single_op = ls_op.at(0).at(0);
    bool need_rebuild_bitmap = ls_op.need_all_prop_bitmap() && single_op.get_op_type() == ObTableOperationType::GET;
    result_entity.set_dictionary(&ls_result.get_rowkey_names(), &ls_result.get_properties_names());
    if (need_rebuild_bitmap) { // construct result entity bitmap based on all columns dict
      if (OB_FAIL(result_entity.construct_names_bitmap_by_dict(req_entity))) {
        LOG_WARN("fail to construct name bitmap by all columns", K(ret), K(req_entity));
      }
    } else if (OB_FAIL(result_entity.construct_names_bitmap(req_entity))) { // directly use request bitmap as result bitmap
      LOG_WARN("fail to construct name bitmap", K(ret), K(req_entity));
    }
  }
  return ret;
}