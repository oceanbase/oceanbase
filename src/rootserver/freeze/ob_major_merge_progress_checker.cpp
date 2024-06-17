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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/scn.h"
#include "rootserver/ob_rs_event_history_table_operator.h" // ROOTSERVICE_EVENT_ADD
#include "storage/compaction/ob_compaction_diagnose.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/compaction/ob_table_ckm_items.h"
#include "storage/compaction/ob_server_compaction_event_history.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace compaction;

const int64_t ObMajorMergeProgressChecker::TABLET_ID_BATCH_CHECK_SIZE;
const int64_t ObMajorMergeProgressChecker::TABLE_MAP_BUCKET_CNT;
const int64_t ObMajorMergeProgressChecker::DEFAULT_ARRAY_CNT;

ObMajorMergeProgressChecker::ObMajorMergeProgressChecker(
    const uint64_t tenant_id, volatile bool &stop)
    : is_inited_(false), first_loop_in_cur_round_(true), stop_(stop),
      loop_cnt_(0), last_errno_(OB_SUCCESS), tenant_id_(tenant_id),
      compaction_scn_(), expected_epoch_(OB_INVALID_ID), sql_proxy_(nullptr),
      schema_service_(nullptr), server_trace_(nullptr), progress_(),
      tablet_status_map_(), table_compaction_map_(), fts_group_array_(),
      ckm_validator_(tenant_id, stop_, tablet_ls_pair_cache_, tablet_status_map_,
                     table_compaction_map_, idx_ckm_validate_array_, validator_statistics_,
                     finish_tablet_ls_pair_array_, finish_tablet_ckm_array_, uncompact_info_, fts_group_array_),
      uncompact_info_(), ls_locality_cache_(), total_time_guard_(), validator_statistics_(), batch_size_mgr_() {}

int ObMajorMergeProgressChecker::init(
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy,
    schema::ObMultiVersionSchemaService &schema_service,
    ObIServerTrace &server_trace,
    ObMajorMergeInfoManager &merge_info_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(tablet_status_map_.create(TABLET_ID_BATCH_CHECK_SIZE, "RSCompStMap", "RSCompStMap", tenant_id_))) {
    LOG_WARN("fail to create tablet compaction status map", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(table_compaction_map_.create(TABLE_MAP_BUCKET_CNT, "RSCompactMap", "RSCompactMap", tenant_id_))) {
    LOG_WARN("fail to create table compaction info map", KR(ret), K_(tenant_id), K(TABLE_MAP_BUCKET_CNT));
  } else if (OB_FAIL(ckm_validator_.init(is_primary_service, sql_proxy))) {
    LOG_WARN("fail to init checksum validator", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_locality_cache_.init(tenant_id_, &merge_info_mgr))) {
    LOG_WARN("failed to init ls locality cache", K(ret));
  } else {
    (void) tablet_ls_pair_cache_.set_tenant_id(tenant_id_);
    idx_ckm_validate_array_.set_attr(ObMemAttr(tenant_id_, "RSCompCkmPair"));
    finish_tablet_ls_pair_array_.set_attr(ObMemAttr(tenant_id_, "RSCompTabPair"));
    finish_tablet_ckm_array_.set_attr(ObMemAttr(tenant_id_, "RSCompCkmArray"));
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    server_trace_ = &server_trace;
    merge_info_mgr_ = &merge_info_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObMajorMergeProgressChecker::rebuild_map_by_tablet_cnt()
{
  int ret = OB_SUCCESS;
  int64_t recommend_map_bucked_cnt = 0;
  if (first_loop_in_cur_round_) {
    const bool need_rebuild_table_map = ObScheduleBatchSizeMgr::need_rebuild_map(
      TABLE_MAP_BUCKET_CNT, table_ids_.count(), table_compaction_map_.bucket_count(), recommend_map_bucked_cnt);
    if (need_rebuild_table_map) {
      table_compaction_map_.destroy();
      if (OB_FAIL(table_compaction_map_.create(recommend_map_bucked_cnt, "RSCompactMap", "RSCompactMap", tenant_id_))) {
        LOG_WARN("fail to create table compaction info map", KR(ret), K_(tenant_id), K(recommend_map_bucked_cnt));
      } else {
        LOG_INFO("success to rebuild table compaction info map", KR(ret), K(recommend_map_bucked_cnt));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t tablet_cnt = MAX(table_ids_.count(), tablet_status_map_.size());
    const bool need_rebuild_tablet_map = ObScheduleBatchSizeMgr::need_rebuild_map(
      TABLET_ID_BATCH_CHECK_SIZE, tablet_cnt, tablet_status_map_.bucket_count(), recommend_map_bucked_cnt);
    if (need_rebuild_tablet_map) {
      tablet_status_map_.destroy();
      if (OB_FAIL(tablet_status_map_.create(recommend_map_bucked_cnt, "RSCompStMap", "RSCompStMap", tenant_id_))) {
        LOG_WARN("fail to create table compaction info map", KR(ret), K_(tenant_id), K(recommend_map_bucked_cnt));
      } else {
        LOG_INFO("success to rebuild tablet status map", KR(ret), K(recommend_map_bucked_cnt));
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::set_basic_info(
    SCN global_broadcast_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!global_broadcast_scn.is_valid() || expected_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(global_broadcast_scn), K(expected_epoch));
  } else if (OB_FAIL(clear_cached_info())) {
    LOG_WARN("fail to clear cached info", KR(ret));
  } else if (OB_FAIL(ckm_validator_.set_basic_info(global_broadcast_scn, expected_epoch))) {
    LOG_WARN("failed to set basic info", KR(ret), K(global_broadcast_scn), K(expected_epoch));
  } else {
    compaction_scn_ = global_broadcast_scn;
    expected_epoch_ = expected_epoch;
    LOG_INFO("success to set basic info", KR(ret), K_(compaction_scn), K_(expected_epoch));
  }
  return ret;
}

int ObMajorMergeProgressChecker::clear_cached_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_status_map_.reuse())) {
    LOG_WARN("fail to reuse tablet_compaction_map", KR(ret));
  } else if (OB_FAIL(table_compaction_map_.reuse())) {
    LOG_WARN("fail to reuse table_compaction_map", KR(ret));
  } else {
    LOG_INFO("success to clear cached info", KR(ret), K_(tenant_id), K_(compaction_scn));
    compaction_scn_.set_min();
    expected_epoch_ = OB_INVALID_ID;
    first_loop_in_cur_round_ = true;
    table_ids_.reset();
    idx_ckm_validate_array_.reset();
    finish_tablet_ls_pair_array_.reset();
    finish_tablet_ckm_array_.reset();
    progress_.reset();
    ckm_validator_.clear_cached_info();
    loop_cnt_ = 0;
    tablet_ls_pair_cache_.reuse();
    reset_uncompacted_tablets();
  }
  return ret;
}

int ObMajorMergeProgressChecker::get_uncompacted_tablets(
    ObArray<ObTabletReplica> &input_tablets,
    common::ObArray<uint64_t> &input_table_ids) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(uncompact_info_.get_uncompact_info(input_tablets, input_table_ids))) {
    LOG_WARN("fail to get uncompacted info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_verification(
    ObSchemaGetterGuard &schema_guard,
    ObIArray<uint64_t> &unfinish_table_id_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTableCompactionInfo *table_compaction_info_ptr = &ckm_validator_.get_table_compaction_info();
  ckm_validator_.clear_array_index();
  int64_t table_batch_size = 0;
  (void) batch_size_mgr_.get_rs_check_batch_size(table_ids_.count(), table_batch_size);
  int64_t index_cnt = 0;
  int64_t table_cnt = 0;
  int64_t idx = table_ids_.batch_start_idx_;
  for ( ; OB_SUCC(ret) && !stop_ && idx < table_ids_.count(); ++idx) {
    const uint64_t table_id = table_ids_.at(idx);
    LOG_TRACE("verify table id", KR(ret), K_(tenant_id), K(table_id));
    if (OB_TMP_FAIL(ckm_validator_.validate_checksum(table_id, schema_guard))) {
      if (can_not_ignore_warning(tmp_ret)) {
        ret = tmp_ret;
        if (OB_CHECKSUM_ERROR == ret) {
          LOG_ERROR("checksum error", KR(ret), K(table_id));
        } else if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
          LOG_INFO("freeze service epoch mismatch", KR(ret));
        }
      } else {
        LOG_WARN("failed to verify table", KR(tmp_ret), K(idx), K(table_id), KPC(table_compaction_info_ptr));
      }
    }
    if (table_compaction_info_ptr->is_index_table()) {
      --index_cnt;
    } else {
      index_cnt = table_compaction_info_ptr->unfinish_index_cnt_;
    }
    // ignore errno, need update progress & unfinish table id array
    LOG_TRACE("check verification", KR(tmp_ret), KPC(table_compaction_info_ptr), K_(progress));
    (void) progress_.update_table_cnt(table_compaction_info_ptr->status_);
    if (!table_compaction_info_ptr->finish_verified()) {
      if (OB_TMP_FAIL(unfinish_table_id_array.push_back(table_id))) {
        LOG_WARN("failed to push table_id into finish_array", KR(tmp_ret), KPC(table_compaction_info_ptr));
      }
      (void) uncompact_info_.add_table(table_id);
    }
    if (0 >= index_cnt // data & index should be in same batch
        && (++table_cnt >= table_batch_size)) {
      break;
    }
  } // end of for
  if (OB_SUCC(ret)) { // record next untouched tablet
    table_ids_.batch_start_idx_ = idx + 1;
  } else {
    // record first failed table, need check in next loop
    table_ids_.batch_start_idx_ = idx;
  }
  return ret;
}

bool ObMajorMergeProgressChecker::should_ignore_cur_table(const ObSimpleTableSchemaV2 *simple_schema)
{
  bool bret = true;
  if (OB_ISNULL(simple_schema)) {
    // table deleted
  } else if (!simple_schema->has_tablet()) {
    // table has not tablet, should not put into table_id_map
  } else if (simple_schema->is_index_table() && !simple_schema->can_read_index()) {
    // not ready index
  } else {
    bret = false;
  }
  return bret;
}

int ObMajorMergeProgressChecker::check_schema_version()
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfo freeze_info;
  int64_t local_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_info_mgr is unexpected null", KR(ret), K_(merge_info_mgr));
  } else if (OB_FAIL(merge_info_mgr_->get_freeze_info_mgr().get_freeze_info(compaction_scn_, freeze_info))) {
    LOG_WARN("failed to get freeze info by snapshot version", KR(ret), K_(tenant_id), K_(compaction_scn));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                    tenant_id_, local_schema_version))) {
    LOG_WARN("fail to get tenant local schema version", KR(ret), K_(tenant_id));
  } else if (!ObSchemaService::is_formal_version(local_schema_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("is not a formal_schema_version", KR(ret), K(local_schema_version));
  } else if (local_schema_version < freeze_info.schema_version_) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not new enough", KR(ret), K(freeze_info), K(local_schema_version));
  }
  return ret;
}

int ObMajorMergeProgressChecker::prepare_unfinish_table_ids()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<uint64_t> table_id_array;
  if (OB_FAIL(check_schema_version())) {
    LOG_WARN("fail to check schema version", KR(ret), K_(tenant_id));
  } else { // get table_id array
    ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK); // temp schema guard to build table_id array
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id_, table_id_array))) {
      LOG_WARN("fail to get table ids in tenant", KR(ret), K_(tenant_id));
    }
  }
  if (OB_SUCC(ret) && table_id_array.count() > 0) {
    if (OB_FAIL(table_ids_.array_.reserve(table_id_array.count() / 4))) {
      LOG_WARN("failed to reserve table_id array", KR(ret), K(table_ids_));
    }
  }
  const ObSimpleTableSchemaV2 *index_simple_schema = nullptr;
  ObTableCompactionInfo table_compaction_info;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_INDEX_PER_TABLE> index_schemas;
  ObSEArray<uint64_t, 50> not_validate_index_ids;
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  int64_t verified_index_cnt[2] = {0, 0};
  while (OB_SUCC(ret) && end_idx < table_id_array.count()) {
    ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK); // temp schema guard to loop table_id array
    start_idx = end_idx;
    end_idx = MIN(table_id_array.count(), start_idx + TABLE_ID_BATCH_CHECK_SIZE);
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
    }
    for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
      const int64_t table_id = table_id_array.at(idx);
      bool is_table_valid = true;
      if (OB_FAIL(get_table_and_index_schema(schema_guard, table_id, is_table_valid, index_schemas))) {
        LOG_WARN("failed to get table & index schemas", KR(ret), K(table_id));
      } else if (is_table_valid) {
        int64_t index_cnt = 0;
        bool need_check_fts = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_schemas.count(); ++j) { // loop index info
          index_simple_schema = index_schemas.at(j);
          if (should_ignore_cur_table(index_simple_schema)) {
            // should ignore cur table
            continue;
          } else if (FALSE_IT(need_check_fts |= index_simple_schema->is_fts_or_multivalue_index())) {
          } else if (index_simple_schema->should_not_validate_data_index_ckm()) {
            if (OB_FAIL(not_validate_index_ids.push_back(index_simple_schema->get_table_id()))) {
              LOG_WARN("failed to push back index id", KR(ret), KPC(index_simple_schema));
            }
          } else if (OB_FAIL(table_ids_.push_back(index_simple_schema->get_table_id()))) {
            LOG_WARN("failed to add table id info", KR(ret), KPC(index_simple_schema));
          } else {
            ++index_cnt;
            ++verified_index_cnt[index_simple_schema->is_global_index_table()];
          }
        } // end of for
        if (OB_SUCC(ret)) { // add table_compaction_info
          if (need_check_fts
              && (!VERIFY_FTS_CHECKSUM || OB_TMP_FAIL(prepare_fts_group(table_id, index_schemas)))) {
            need_check_fts = false;
            LOG_WARN_RET(tmp_ret, "close fts verify or fail to prepare fts group",
              K(table_id), K(need_check_fts), K(table_compaction_info));
          }
          table_compaction_info.table_id_ = table_id;
          table_compaction_info.unfinish_index_cnt_ = index_cnt;
          table_compaction_info.need_check_fts_ = need_check_fts;
          if (OB_FAIL(table_compaction_map_.set_refactored(
                  table_id, table_compaction_info, true /*overwrite*/))) {
            LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
          }
        }
      }
    } // end of for
  } // end of while
  for (int64_t idx = 0; OB_SUCC(ret) && idx < not_validate_index_ids.count(); ++idx) {
    if (OB_FAIL(table_ids_.push_back(not_validate_index_ids.at(idx)))) {
      LOG_WARN("failed to push back index id", KR(ret), K(idx), "index_id", not_validate_index_ids.at(idx));
    }
  }
  if (OB_SUCC(ret)) {
    ADD_RS_COMPACTION_EVENT(
        compaction_scn_.get_val_for_tx(),
        ObServerCompactionEvent::RS_REPAPRE_UNFINISH_TABLE_IDS,
        common::ObTimeUtility::fast_current_time(),
        "tenant_table_cnt", table_id_array.count(),
        "data_table_cnt", table_ids_.count() - verified_index_cnt[0] - verified_index_cnt[1],
        "local_index_cnt", verified_index_cnt[0],
        "global_index_cnt", verified_index_cnt[1]);
    LOG_INFO("success to prepare table_id map", KR(ret), "tenant_table_cnt", table_id_array.count(),
      K_(table_ids), "data_table_cnt", table_ids_.count() - verified_index_cnt[0] - verified_index_cnt[1],
      "local_index_cnt", verified_index_cnt[0], "global_index_cnt", verified_index_cnt[1]);
  }
  return ret;
}

int ObMajorMergeProgressChecker::get_table_and_index_schema(
  ObSchemaGetterGuard &schema_guard,
  const uint64_t table_id,
  bool &is_table_valid,
  ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas)
{
  int ret = OB_SUCCESS;
  is_table_valid = false;
  const ObSimpleTableSchemaV2 *data_simple_schema = nullptr;
  if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, data_simple_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(data_simple_schema) || !data_simple_schema->should_check_major_merge_progress()) {
    // should ignore cur table
  } else if (data_simple_schema->is_index_table()) {
    // index table will be pushed into array by data_table
  } else if (ObChecksumValidator::SPECIAL_TABLE_ID == table_id) {
    // do nothing
  } else if (OB_FAIL(table_ids_.push_back(table_id))) {
    LOG_WARN("failed to add table id info", KR(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(
        tenant_id_, table_id, index_schemas))) {
    LOG_WARN("failed to get index schemas", KR(ret), K_(tenant_id),
      K(table_id), KPC(data_simple_schema));
  } else {
    is_table_valid = true;
  }
  return ret;
}

void ObMajorMergeProgressChecker::deal_with_unfinish_table_ids(
  const int error_no,
  ObIArray<uint64_t> &unfinish_table_id_array)
{
  int ret = OB_SUCCESS;
  bool assgin_flag = true;
  if (OB_SUCCESS != error_no) {
    if (0 == table_ids_.batch_start_idx_) {
      // table_ids_ should not be modified
      assgin_flag = false;
    } else {
      // need reuse all rest table in after start_idx
      for (int64_t idx = table_ids_.batch_start_idx_; OB_SUCC(ret) && idx < table_ids_.count(); ++idx) {
        const uint64_t table_id = table_ids_.at(idx);
        if (OB_FAIL(unfinish_table_id_array.push_back(table_id))) {
          LOG_WARN("failed to push table_id into finish_array", K(table_id));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (assgin_flag) {
    int64_t retry_times = 0;
    do {
      if (OB_FAIL(table_ids_.assign(unfinish_table_id_array))) {
        LOG_WARN("failed to assign table ids", KR(ret), K(unfinish_table_id_array));
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret && (++retry_times < ASSGIN_FAILURE_RETRY_TIMES));
  }
}

int ObMajorMergeProgressChecker::prepare_check_progress(
  ObRSCompactionTimeGuard &tmp_time_guard,
  bool &exist_uncompacted_table)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  exist_uncompacted_table = true;
  table_ids_.start_looping();
  if (OB_TMP_FAIL(ls_locality_cache_.refresh_ls_locality(first_loop_in_cur_round_ /*force_refresh*/))) {
    LOG_WARN("failed to refresh ls locality", K(tmp_ret));
  }
  if (first_loop_in_cur_round_) {
    total_time_guard_.reuse();
    if (OB_FAIL(prepare_unfinish_table_ids())) {
      LOG_WARN("fail to prepare table_id_map", KR(ret), K_(tenant_id));
      table_ids_.reset();
    } else {
      total_time_guard_.click(ObRSCompactionTimeGuard::PREPARE_UNFINISH_TABLE_IDS);
      progress_.total_table_cnt_ = table_ids_.count() + 1/*SPECIAL_TABLE_ID*/;
    }
  }
  if (FAILEDx(rebuild_map_by_tablet_cnt())) {
    LOG_WARN("failed to rebuild map by tablet cnt", K(ret));
  } else if (OB_FAIL(tablet_ls_pair_cache_.try_refresh(first_loop_in_cur_round_ /*force_refresh*/))) {
    LOG_WARN("failed to refresh tablet ls pair", K(ret));
  } else {
    tmp_time_guard.click(ObRSCompactionTimeGuard::GET_TABLET_LS_PAIRS);
    first_loop_in_cur_round_ = false;
    exist_uncompacted_table = progress_.exist_uncompacted_table();
    progress_.clear_before_each_loop();
    reset_uncompacted_tablets();
    if (is_extra_check_round()) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant config is not valid", KR(ret), K_(tenant_id));
      } else {
        batch_size_mgr_.set_tablet_batch_size(tenant_config->compaction_schedule_tablet_batch_cnt);
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_index_and_rest_table()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_index_ckm())) {
    LOG_WARN("failed to validate index checksum", KR(ret), K_(compaction_scn));
  } else if (OB_FAIL(deal_with_rest_data_table())) {
    LOG_WARN("deal with rest data table", KR(ret), K_(compaction_scn));
  } else if (0 == progress_.table_cnt_[INITIAL]
      && fts_group_array_.need_check_fts()
      && OB_FAIL(handle_fts_checksum())) {
    LOG_WARN("failed to handle fts checksum", KR(ret), K_(compaction_scn), K_(progress));
  } else if (progress_.is_merge_finished()) {
    LOG_INFO("progress is check finished", KR(ret), K_(progress));
  } else if (progress_.only_remain_special_table_to_verified() || table_ids_.empty()) {
    bool finish_validate = false;
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_RS_CHECK_SPECIAL_TABLE) ret;
    if (OB_FAIL(ret)) {
      LOG_INFO("ERRSIM EN_RS_CHECK_SPECIAL_TABLE", K(ret));
      ret = OB_SUCCESS; // clear errno
    } else
#endif
    if (OB_FAIL(ckm_validator_.deal_with_special_table_at_last(finish_validate))) {
      LOG_WARN("fail to handle table with first tablet in sys ls", KR(ret), K_(tenant_id),
        K_(compaction_scn), K_(expected_epoch));
    } else if (finish_validate) {
      progress_.deal_with_special_tablet();
    }
  }
  (void) ckm_validator_.batch_update_report_scn();
  (void) ckm_validator_.batch_write_tablet_ckm();
  return ret;
}

int ObMajorMergeProgressChecker::check_progress(
  ObMergeProgress &progress)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  ObRSCompactionTimeGuard tmp_time_guard;
  bool exist_uncompacted_table = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(expected_epoch_ < 0 || !compaction_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached info may be cleared", KR(ret), K_(expected_epoch), K_(compaction_scn));
  } else if (OB_FAIL(prepare_check_progress(tmp_time_guard, exist_uncompacted_table))) {
    LOG_WARN("failed to prepare check progress", KR(ret));
  } else {
    SMART_VAR(ObArray<uint64_t>, unfinish_table_id_array) {
      int64_t tenant_schema_version = 0;
      unfinish_table_id_array.set_attr(ObMemAttr(tenant_id_, "RSCompTableIds"));
      if (OB_FAIL(unfinish_table_id_array.reserve(DEFAULT_ARRAY_CNT))) {
        LOG_WARN("failed to reserve unfinish table id array", KR(ret), "array_cnt", DEFAULT_ARRAY_CNT);
      } else if (exist_uncompacted_table && OB_FAIL(generate_tablet_status_map())) {
        LOG_WARN("failed to generate tablet status map", KR(ret));
      } else {
        tmp_time_guard.click(ObRSCompactionTimeGuard::GET_TABLET_META_TABLE);
      }
      ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
      int64_t last_epoch_check_us = 0;
      while (OB_SUCC(ret) && !table_ids_.loop_finish() && !stop_) { // split batch table_ids
        if (OB_FAIL(ObMajorFreezeUtil::check_epoch_periodically(
                *sql_proxy_, tenant_id_, expected_epoch_, last_epoch_check_us))) {
          LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K_(expected_epoch), K_(compaction_scn));
        } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id_, tenant_schema_version))) {
          LOG_WARN("failed to get schema version", K(ret), K_(tenant_id));
        } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard,
            tenant_schema_version, OB_INVALID_VERSION,
            ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY))) {
          LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
        }
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = OB_E(EventTable::EN_RS_CHECK_MERGE_PROGRESS) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            STORAGE_LOG(INFO, "ERRSIM EN_RS_CHECK_MERGE_PROGRESS", K(ret));
          }
        }
#endif
        if (FAILEDx(check_verification(schema_guard, unfinish_table_id_array))) {
          // check tablet_replica_checksum & table_index_checksum & cross_cluter_checksum
          LOG_WARN("failed to check verification", KR(ret), K_(compaction_scn), K_(expected_epoch));
          // only record OB_CHECKSUM_ERROR, and thus avoid confusing DBA
          if (TC_REACH_TIME_INTERVAL(ADD_RS_EVENT_INTERVAL) && (OB_CHECKSUM_ERROR == ret)) {
            ROOTSERVICE_EVENT_ADD("daily_merge", "verification", K_(tenant_id),
                                  "check verification fail", ret,
                                  "global_broadcast_scn", compaction_scn_.get_val_for_inner_table_field(),
                                  "service_addr", GCONF.self_addr_);
          }
        }
        if (OB_FAIL(ret) && last_errno_ == ret) {
          if (OB_TMP_FAIL(compaction::ADD_COMMON_SUSPECT_INFO(compaction::MAJOR_MERGE, ObDiagnoseTabletType::TYPE_RS_MAJOR_MERGE,
                          ObSuspectInfoType::SUSPECT_RS_SCHEDULE_ERROR,
                          static_cast<int64_t>(compaction_scn_.get_val_for_tx()),
                          static_cast<int64_t>(last_errno_),
                          static_cast<int64_t>(table_ids_.count())))) {
            LOG_WARN("failed to add suspect info", KR(tmp_ret));
          }
        }
        last_errno_ = ret;
        if (!can_not_ignore_warning(ret)) {
          // do not ignore ret, therefore not continue to check next table_schema
          ret = OB_SUCCESS;
        }
      } // end of while
      tmp_time_guard.click(ObRSCompactionTimeGuard::CKM_VERIFICATION);
      total_time_guard_.add_time_guard(tmp_time_guard);
      // deal with finish_table_id_array after loop table_ids_
      (void) deal_with_unfinish_table_ids(ret, unfinish_table_id_array);
    } // SMART_VAR
    if (FAILEDx(check_index_and_rest_table())) {
      LOG_WARN("failed check index ckm and rest table", KR(ret), K_(compaction_scn));
    }
    const int64_t cost_us = ObTimeUtility::fast_current_time() - start_time;
    ++loop_cnt_;
    if (OB_SUCCESS == last_errno_) {
      DEL_SUSPECT_INFO(compaction::MAJOR_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, share::ObDiagnoseTabletType::TYPE_RS_MAJOR_MERGE);
    }
    progress = progress_;
    print_unfinish_info(cost_us);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to check merge progress", KR(ret), K_(last_errno), K_(tenant_id), K_(compaction_scn), K(cost_us), K_(total_time_guard));
      last_errno_ = ret;
    }
  }
  return ret;
}

void ObMajorMergeProgressChecker::print_unfinish_info(const int64_t cost_us)
{
  int ret = OB_SUCCESS;
  const int64_t array_cnt = ObUncompactInfo::DEBUG_INFO_CNT;
  ObSEArray<uint64_t, array_cnt> tmp_table_id_array;
  ObSEArray<uint64_t, array_cnt> tmp_tablets_array;
  ObSEArray<ObTabletReplica, array_cnt> uncompacted_replica_array;
  ObSEArray<uint64_t, array_cnt> uncompacted_table_array;
  (void) uncompact_info_.get_uncompact_info(uncompacted_replica_array, uncompacted_table_array);
  if (table_ids_.count() > 0) {
    const int64_t table_id_cnt = MIN(array_cnt, table_ids_.count());
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_id_cnt; ++idx) {
      if (OB_FAIL(tmp_table_id_array.push_back(table_ids_.at(idx)))) {
        LOG_WARN("failed to push array", KR(ret));
      }
    }
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < uncompacted_replica_array.count(); ++idx) {
    if (OB_FAIL(tmp_tablets_array.push_back(
            uncompacted_replica_array.at(idx).get_tablet_id().id()))) {
      LOG_WARN("failed to push array", KR(ret));
    }
  }
  // table in table_ids_ may finish verified in deal_with_rest_data_table()
  // need next loop to delete from array
  ADD_RS_COMPACTION_EVENT(
    compaction_scn_.get_val_for_tx(),
    ObServerCompactionEvent::RS_FINISH_CUR_LOOP,
    common::ObTimeUtility::fast_current_time(),
    K(cost_us), K_(progress), "remain_table_id_count", table_ids_.count(),
    "remain_table_ids", tmp_table_id_array,
    "remain_tablet_ids", tmp_tablets_array,
    K_(total_time_guard), K_(validator_statistics));
  LOG_INFO("succ to check merge progress", K_(tenant_id), K_(loop_cnt), K_(compaction_scn), K(cost_us),
    K_(progress), "remain_table_id_count", table_ids_.count(),
    "remain_table_ids", tmp_table_id_array,
    "uncompacted_tablets", uncompacted_replica_array,
    "uncompacted_table_ids", uncompacted_table_array,
    K_(total_time_guard), K_(validator_statistics));
}

int ObMajorMergeProgressChecker::deal_with_rest_data_table()
{
  int ret = OB_SUCCESS;
  bool exist_index_table = false;
  bool exist_data_table = false;
  if ((is_extra_check_round() && table_ids_.count() > 0  && table_ids_.count() < DEAL_REST_TABLE_CNT_THRESHOLD)
      || REACH_TENANT_TIME_INTERVAL(DEAL_REST_TABLE_INTERVAL)) {
    ObTableCompactionInfo table_compaction_info;
    for (int64_t idx = 0; idx < table_ids_.count(); ++idx) {
      if (OB_FAIL(table_compaction_map_.get_refactored(table_ids_.at(idx), table_compaction_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get table compaction info", KR(ret), K(idx), "table_id", table_ids_.at(idx));
        }
      } else if (table_compaction_info.is_index_table()) {
        LOG_TRACE("exist index table", K(ret), K(table_compaction_info));
        exist_index_table = true;
      } else {
        LOG_TRACE("exist data table", K(ret), K(table_compaction_info));
        exist_data_table = true;
      }
    } // end of for
    if (OB_SUCC(ret) && (exist_index_table != exist_data_table)) { // rest table are data table/ index table
      int tmp_ret = OB_SUCCESS;
      LOG_INFO("start to deal with rest data table", K(ret), K_(table_ids));
      for (int64_t idx = 0; idx < table_ids_.count(); ++idx) {
        const uint64_t table_id = table_ids_.at(idx);
        if (OB_FAIL(table_compaction_map_.get_refactored(table_id, table_compaction_info))) {
          LOG_WARN("failed to get table compaction info", KR(ret), K_(tenant_id), K(table_id));
        } else if (table_compaction_info.is_compacted()) {
          if (OB_TMP_FAIL(set_table_compaction_info_status(table_id, ObTableCompactionInfo::INDEX_CKM_VERIFIED))) {
            LOG_WARN("failed to update table compaction info", KR(tmp_ret), K(idx), K(table_id));
          } else {
            LOG_TRACE("deal with data table", KR(tmp_ret), K(idx), K(table_id));
          }
        }
      } // end of for
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::set_table_compaction_info_status(
  const uint64_t table_id,
  const ObTableCompactionInfo::Status status)
{
  int ret = OB_SUCCESS;
  ObTableCompactionInfo table_compaction_info;
  if (OB_FAIL(table_compaction_map_.get_refactored(table_id, table_compaction_info))) {
    if (OB_HASH_NOT_EXIST == ret) {  // first initialization
      ret = OB_SUCCESS;
      table_compaction_info.reset();
      table_compaction_info.table_id_ = table_id;
    } else {
      LOG_WARN("fail to get val from hashmap", KR(ret), K(table_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(table_compaction_info.set_status(status))) {
  } else if (OB_FAIL(table_compaction_map_.set_refactored(table_id, table_compaction_info, true /*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
  }
  return ret;
}

int ObMajorMergeProgressChecker::validate_index_ckm()
{
  int ret = OB_SUCCESS;
  if (idx_ckm_validate_array_.count() > 0) {
    if (idx_ckm_validate_array_.count() < 50
      && progress_.get_wait_index_ckm_table_cnt() > 100
      && !is_extra_check_round()) {
      // do nothing
    } else {
      if (OB_FAIL(loop_index_ckm_validate_array())) {
        LOG_WARN("failed to loop index ckm validate array", KR(ret), K_(tenant_id));
      }
    }
    idx_ckm_validate_array_.reuse(); // reuse array
  }
  return ret;
}

int ObMajorMergeProgressChecker::deal_with_validated_table(
  const uint64_t table_id,
  const int64_t finish_index_cnt,
  const ObTableCkmItems &table_ckm)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool validate_finish = (finish_index_cnt == 0);
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (!validate_finish
      && OB_TMP_FAIL(update_finish_index_cnt_for_data_table(
          table_id, finish_index_cnt, validate_finish))) {
    // set table_compaction_info::unfinish_index_cnt
    LOG_WARN("failed to update finish index", KR(tmp_ret), K(table_id), K(finish_index_cnt));
  }
  if (OB_SUCC(ret) && validate_finish) {
    if (table_ckm.is_inited() ) {
      if (OB_TMP_FAIL(ckm_validator_.push_finish_tablet_ls_pairs_with_update(
          table_ckm.get_tablet_ls_pairs()))) {
        LOG_WARN("failed to push back tablet_ls_pair", KR(tmp_ret));
      }
      if (OB_TMP_FAIL(ckm_validator_.push_tablet_ckm_items_with_update(
          table_ckm.get_ckm_items()))) {
        LOG_WARN("failed to push checksum items", KR(tmp_ret), K(table_id));
      }
    }
    if (OB_TMP_FAIL(set_table_compaction_info_status(table_id, ObTableCompactionInfo::VERIFIED))) {
      LOG_WARN("failed to update table compaction info", KR(tmp_ret), K(table_id));
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::loop_index_ckm_validate_array()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t finish_index_cnt = 0;
  uint64_t prev_data_table_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
  ObTableCkmItems data_table_ckm(tenant_id_);
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  }
  for (int64_t idx = 0; idx < idx_ckm_validate_array_.count(); ++idx) {
    const uint64_t data_table_id = idx_ckm_validate_array_.at(idx).data_table_id_;
    const uint64_t index_table_id = idx_ckm_validate_array_.at(idx).index_table_id_;
    if (prev_data_table_id != data_table_id) { // not same table
      if (OB_INVALID_ID != prev_data_table_id) {
        (void) deal_with_validated_table(prev_data_table_id, finish_index_cnt, data_table_ckm);
      }

      finish_index_cnt = 0;
      data_table_ckm.clear();
      prev_data_table_id = data_table_id;
      if (OB_FAIL(data_table_ckm.build(data_table_id, compaction_scn_,
                                       *sql_proxy_, schema_guard, get_tablet_ls_pair_cache()))) {
        LOG_WARN("fail to prepare schema checksum items", KR(ret), K_(tenant_id), K(data_table_id));
      } else {
        ++validator_statistics_.query_ckm_sql_cnt_;
        LOG_TRACE("success to get data table ckm", KR(ret), K(data_table_id), K(data_table_ckm));
      }
    }
    if (FAILEDx(get_idx_ckm_and_validate(index_table_id, schema_guard, data_table_ckm))) {
      if (OB_ITEM_NOT_MATCH == ret || OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get index ckm and validate", KR(ret), K(data_table_id), K(index_table_id));
      }
    }
    if (OB_SUCC(ret)) {
      ++finish_index_cnt;
      ++validator_statistics_.checker_validate_idx_cnt_;
    }
  } // end of for
  if (OB_SUCC(ret) && finish_index_cnt > 0 && OB_INVALID_ID != prev_data_table_id) {
    (void) deal_with_validated_table(prev_data_table_id, finish_index_cnt, data_table_ckm);
  }
  return ret;
}

int ObMajorMergeProgressChecker::get_idx_ckm_and_validate(
  const uint64_t index_table_id,
  ObSchemaGetterGuard &schema_guard,
  ObTableCkmItems &data_table_ckm)
{
  int ret = OB_SUCCESS;
  ObTableCkmItems index_table_ckm(tenant_id_);
  if (OB_FAIL(index_table_ckm.build(index_table_id, compaction_scn_,
                                    *sql_proxy_, schema_guard,
                                    get_tablet_ls_pair_cache()))) {
    LOG_WARN("failed to get checksum items", KR(ret), K(index_table_id), K_(compaction_scn));
  } else if (OB_UNLIKELY(index_table_ckm.get_table_schema()->should_not_validate_data_index_ckm())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not validate spatial index and data table", KR(ret), K(index_table_id), K(index_table_ckm));
  } else {
    const bool is_global_index = index_table_ckm.get_table_schema()->is_global_index_table();
    if (OB_FAIL(ObTableCkmItems::validate_ckm_func[is_global_index](
      compaction_scn_,
      *sql_proxy_,
      data_table_ckm,
      index_table_ckm))) {
      LOG_WARN("failed to validate checksum", KR(ret), "data_table_id", data_table_ckm.get_table_id(),
        K(index_table_id), K(data_table_ckm), K(index_table_ckm));
      if (OB_ITEM_NOT_MATCH == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret) || OB_TABLE_NOT_EXIST == ret) {
    (void) deal_with_validated_table(index_table_id, 0 /*finish_index_cnt*/, index_table_ckm);
  }
  return ret;
}

int ObMajorMergeProgressChecker::update_finish_index_cnt_for_data_table(
  const uint64_t data_table_id,
  const uint64_t finish_index_cnt,
  bool &idx_validate_finish)
{
  int ret = OB_SUCCESS;
  idx_validate_finish = false;
  ObTableCompactionInfo table_compaction_info;
  if (OB_FAIL(table_compaction_map_.get_refactored(data_table_id, table_compaction_info))) {
    LOG_WARN("failed to get table compaction info", KR(ret), K_(tenant_id), K(data_table_id));
  } else {
    if (table_compaction_info.unfinish_index_cnt_ < finish_index_cnt) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "finish index cnt is unexpected", K(table_compaction_info), K(finish_index_cnt));
      table_compaction_info.unfinish_index_cnt_ = 0;
    } else {
      table_compaction_info.unfinish_index_cnt_ -= finish_index_cnt;
    }
    if (0 == table_compaction_info.unfinish_index_cnt_) {
      table_compaction_info.set_index_ckm_verified();
      idx_validate_finish = true;
    }
  }
  if (FAILEDx(table_compaction_map_.set_refactored(data_table_id, table_compaction_info, true /*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K(data_table_id), K(table_compaction_info));
  } else {
    (void) progress_.update_table_cnt(table_compaction_info.status_);
  }
  LOG_TRACE("success to update finish index cnt", K(finish_index_cnt), K(table_compaction_info), K(idx_validate_finish));
  return ret;
}

int ObMajorMergeProgressChecker::generate_tablet_status_map()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletInfo tablet_info;
  int64_t idx = 0;
  bool filter = false;
  ObCompactionTabletMetaIterator iter(!is_extra_check_round(), compaction_scn_.get_val_for_tx());
  if (OB_FAIL(iter.init(tenant_id_, batch_size_mgr_.get_inner_table_scan_batch_size(), *server_trace_))) {
    LOG_WARN("failed to init iter", KR(ret));
  }
  while (OB_SUCC(ret) && !stop_) {
    if (OB_FAIL(iter.next(tablet_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next tablet_info", KR(ret), K_(tenant_id), K_(stop));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(!tablet_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iterate invalid tablet info", KR(ret), K(tablet_info));
    } else {
      ObTabletCompactionStatus status = ObTabletCompactionStatus::COMPACTED;
      SCN replica_snapshot_scn;
      SCN report_scn;
      const ObLSID &ls_id = tablet_info.get_ls_id();
      ObLSInfo ls_info;
      if (OB_FAIL(ls_locality_cache_.get_ls_info(ls_id, ls_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_TRACE("can't find ls_info from ls_locality_cache", KR(ret), K(ls_id), K_(tenant_id));
        } else {
          LOG_WARN("fail to get ls_info from ls_locality_cache", KR(ret), K(ls_id), K_(tenant_id));
        }
      }
      const ObLSReplica *ls_replica = nullptr;
      FOREACH_CNT_X(replica, tablet_info.get_replicas(), OB_SUCC(ret)) {
        filter = false;
        if (!ls_info.is_valid()) {
          // do nothing
          LOG_TRACE("ls info is invalid", KR(ret), K(ls_id), K_(tenant_id), K(ls_info));
        } else if (OB_FAIL(ls_info.find(replica->get_server(), ls_replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // Ignore tablet replicas that are not in ls_info. E.g., after ls replica migration,
            // source ls meta has been deleted, but source tablet meta has not been deleted yet.
            ret = OB_SUCCESS;  // ignore ret
            filter = true;
            if (REACH_TIME_INTERVAL(10_s)) {
              LOG_INFO("ignore this tablet replica, sicne it is not in ls_info", K_(tenant_id),
                    KPC(replica), K(ls_info));
            }
          } else {
            LOG_WARN("fail to find ls replica", KR(ret), KPC(replica), K_(tenant_id));
          }
        } else if (OB_UNLIKELY(nullptr == ls_replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ls replica", KR(ret), KPC(replica));
        } else if ((REPLICA_TYPE_LOGONLY == ls_replica->get_replica_type()
            || REPLICA_TYPE_ENCRYPTION_LOGONLY == ls_replica->get_replica_type())) {
          // logonly replica no need check
          filter = true;
        }
        if (OB_FAIL(ret) || filter) {
          // replica is filtered
          // do nothing
        } else if (OB_FAIL(replica_snapshot_scn.convert_for_tx(replica->get_snapshot_version()))) {
          LOG_WARN("fail to convert val to SCN", KR(ret), KPC(replica));
        } else if (OB_UNLIKELY(ObTabletReplica::ScnStatus::SCN_STATUS_ERROR == replica->get_status())) {
          ret = OB_CHECKSUM_ERROR;
          LOG_ERROR("ERROR! ERROR! ERROR! find error status tablet replica", KR(ret), K(tablet_info));
        } else if (replica_snapshot_scn < compaction_scn_) {
          status = ObTabletCompactionStatus::INITIAL;
          (void) uncompact_info_.add_tablet(*replica);
          LOG_TRACE("unfinish tablet", KR(ret), KPC(replica), K(replica_snapshot_scn), K_(compaction_scn));
          break;
        } else if (OB_FAIL(report_scn.convert_for_tx(replica->get_report_scn()))) { // check report_scn
          LOG_WARN("fail to convert val to SCN", KR(ret), KPC(replica));
        } else if (report_scn >= compaction_scn_
          || replica_snapshot_scn > compaction_scn_) {
          status = ObTabletCompactionStatus::CAN_SKIP_VERIFYING;
          break;
        }
      } // end of FOREACH
      if (OB_SUCC(ret) && ObTabletCompactionStatus::INITIAL != status) {
        ++progress_.merged_tablet_cnt_;
        if (OB_FAIL(tablet_status_map_.set_refactored(tablet_info.get_tablet_id(), status, 1/*overwrite*/))) {
          LOG_WARN("failed to push back status", KR(ret), K(tablet_info), K(status));
        } else {
          LOG_TRACE("success to add tablet status", KR(ret), K(tablet_info), K(status));
        }
      }
    }
  } // end of while
  return ret;
}

int inner_find_doc_word_index(
  const ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas,
  const ObSimpleTableSchemaV2 &input_index_schema,
  const ObSimpleTableSchemaV2 *&doc_word_schema)
{
  int ret = OB_SUCCESS;
  doc_word_schema = NULL;
  const int64_t buf_size = OB_MAX_TABLE_NAME_BUF_LENGTH;
  char buf[buf_size] = {0};
  const ObString &input_index_name = input_index_schema.get_table_name_str();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < index_schemas.count(); ++idx) {
    const ObSimpleTableSchemaV2 *index_schema = index_schemas.at(idx);
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is unexpected null", KR(ret), K(idx), KP(index_schema));
    } else if (is_fts_doc_word_aux(index_schema->get_index_type())) {
      if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s_fts_doc_word", input_index_name.length(), input_index_name.ptr()))) {
        LOG_WARN("fail to printf fts doc word name str", K(ret), K(input_index_name));
      } else if (0 == index_schema->get_table_name_str().case_compare(buf)) {
        doc_word_schema = index_schema;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(doc_word_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObMajorMergeProgressChecker::prepare_fts_group(
  const int64_t table_id,
  const ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas)
{
  int ret = OB_SUCCESS;
  ObFTSGroup fts_group;
  fts_group.data_table_id_ = table_id;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < index_schemas.count(); ++idx) {
    const ObSimpleTableSchemaV2 *index_schema = index_schemas.at(idx);
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is unexpected null", KR(ret), K(idx), KP(index_schema));
    } else if (is_rowkey_doc_aux(index_schema->get_index_type())) {
      if (OB_UNLIKELY(0 != fts_group.rowkey_doc_index_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't have two rowkey_doc_index_id", KR(ret), K(fts_group));
      } else {
        fts_group.rowkey_doc_index_id_ = index_schema->get_table_id();
      }
    } else if (is_doc_rowkey_aux(index_schema->get_index_type())) {
      if (OB_UNLIKELY(0 != fts_group.doc_rowkey_index_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't have two doc_rowkey_index_id", KR(ret), K(fts_group));
      } else {
        fts_group.doc_rowkey_index_id_ = index_schema->get_table_id();
      }
    } else if (is_fts_index_aux(index_schema->get_index_type())) {
      const ObSimpleTableSchemaV2 *doc_word_schema = NULL;
      if (OB_FAIL(inner_find_doc_word_index(index_schemas, *index_schema, doc_word_schema))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("doc word schema of fts index is not found, skip verify", KR(ret), KPC(index_schema));
        } else {
          LOG_WARN("failed to find doc word index", KR(ret), K(idx), KPC(index_schema));
        }
      } else if (OB_FAIL(fts_group.push_back(ObFTSIndexInfo(index_schema->get_table_id(), doc_word_schema->get_table_id())))) {
        LOG_WARN("failed to push doc word index", KR(ret), K(idx), KPC(index_schema), KPC(doc_word_schema));
      }
    }
  }
  if (OB_FAIL(ret) || !fts_group.is_valid()) {
  } else if (OB_FAIL(fts_group_array_.push_back(fts_group))) {
    LOG_WARN("failed to prepare push fts group", KR(ret), K(fts_group));
  } else {
    LOG_INFO("success to prepare fts group", KR(ret), K(fts_group));
  }
  return ret;
}

int ObMajorMergeProgressChecker::handle_fts_checksum()
{
  int ret = OB_SUCCESS;
  int64_t tenant_schema_version = 0;
  ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
  if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id_, tenant_schema_version))) {
    LOG_WARN("failed to get schema version", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
          tenant_id_, schema_guard, tenant_schema_version, OB_INVALID_VERSION,
          ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ckm_validator_.handle_fts_checksum(schema_guard, fts_group_array_))) {
    LOG_WARN("failed to handle fts checksum", KR(ret));
  } else {
    LOG_INFO("success to handle fts checksum", KR(ret), K_(compaction_scn), K_(progress), K_(fts_group_array));
    fts_group_array_.reuse();
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
