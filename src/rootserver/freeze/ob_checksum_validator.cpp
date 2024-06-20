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

#include "rootserver/freeze/ob_checksum_validator.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/ob_tablet_checksum_operator.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_freeze_info_manager.h"
#include "rootserver/freeze/ob_fts_checksum_validate_util.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace schema;
using namespace compaction;

///////////////////////////////////////////////////////////////////////////////

int ObChecksumValidator::init(
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id));
  } else {
    is_primary_service_ = is_primary_service;
    cur_tablet_ls_pair_array_.set_attr(ObMemAttr(tenant_id_, "RSCompPairs"));
    replica_ckm_items_.array_.set_attr(ObMemAttr(tenant_id_, "RSCompCkmItems"));
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObChecksumValidator::set_basic_info(
    const share::SCN &frozen_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid() || expected_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_scn), K(expected_epoch));
  } else if (FALSE_IT(compaction_scn_ = frozen_scn)) {
  } else if (FALSE_IT(major_merge_start_us_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_FAIL(set_need_validate())) { // init compaction_scn_ before call this func
    LOG_WARN("failed to set need_validate", K(ret), K_(tenant_id), K_(is_primary_service));
  } else {
    expected_epoch_ = expected_epoch;
    statistics_.reset();
  }
  return ret;
}

int ObChecksumValidator::deal_with_special_table_at_last(bool &finish_validate)
{
  int ret = OB_SUCCESS;
  finish_validate = false;
  ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
  cur_tablet_ls_pair_array_.reuse();
  ObSEArray<ObTabletID, 1> tmp_tablet_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
    tenant_id_, schema_guard, OB_INVALID_VERSION, OB_INVALID_VERSION,
    ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY))) {
  } else if (FALSE_IT(schema_guard_ = &schema_guard)) {
  } else if (OB_FAIL(check_inner_status())) {
    LOG_WARN("failed to check inner status", K(ret));
  } else if (FALSE_IT(table_id_ = ObChecksumValidator::SPECIAL_TABLE_ID)) {
  } else if (OB_FAIL(get_table_compaction_info(table_id_, table_compaction_info_))) {
    LOG_WARN("failed to get table compaction info", K(ret));
  } else if (OB_FAIL(schema_guard_->get_simple_table_schema(tenant_id_, table_id_, simple_schema_))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K_(table_id));
  } else if (OB_ISNULL(simple_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K_(tenant_id), K_(table_id));
  } else if (OB_FAIL(simple_schema_->get_tablet_ids(tmp_tablet_ids))) {
    LOG_WARN("fail to get tablet_ids from simple table schema", KR(ret), KPC_(simple_schema));
  } else if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_pairs(
      tenant_id_, tmp_tablet_ids, cur_tablet_ls_pair_array_))) {
    LOG_WARN("failed to get tablet ls pairs from cache", KR(ret));
  } else if (OB_UNLIKELY(cur_tablet_ls_pair_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id), K_(table_id),
      K_(cur_tablet_ls_pair_array));
  } else if (OB_FAIL(get_tablet_replica_checksum_and_validate(true /*include_larger_than*/))) {
    if (OB_ITEM_NOT_MATCH == ret) {
      LOG_TRACE("mismatch checksum cnt when deal with special table", KR(ret), K_(cur_tablet_ls_pair_array));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to validate tablet replica checksum", KR(ret), K_(compaction_scn), K_(table_id),
        KPC(simple_schema_), K_(cur_tablet_ls_pair_array));
    }
  } else if (FALSE_IT(table_compaction_info_.set_index_ckm_verified())) {
  } else if (OB_FAIL(validate_cross_cluster_checksum())) {
    LOG_WARN("failed to validate cross cluster checksum", K(ret));
  } else {
    finish_validate = true;
    LOG_INFO("success to deal with special table", KR(ret), K_(table_id), K_(table_compaction_info));
  }
  return ret;
}

// check for every merge round
int ObChecksumValidator::set_need_validate()
{
  int ret = OB_SUCCESS;
  if (is_primary_service_) {
    // need to check index checksum on primary tenant
    need_validate_index_ckm_ = true;
    if (OB_FAIL(check_tablet_checksum_sync_finish(true /*force_check*/))) {
      LOG_WARN("failed to check tablet checksum sync finish", K(ret), K_(is_primary_service));
    } else {
      // for primary service, if cross cluster ckm sync finish, need to validate cur round checksum & inner_table
      // else: write ckm into inner table
      need_validate_cross_cluster_ckm_ = cross_cluster_ckm_sync_finish_;
    }
  } else { // standby tenant
    need_validate_index_ckm_ = false;
    need_validate_cross_cluster_ckm_ = true;
  }
  return ret;
}

int ObChecksumValidator::get_table_compaction_info(
  const uint64_t table_id, compaction::ObTableCompactionInfo &table_compaction_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_compaction_map_.get_refactored(table_id, table_compaction_info))) {
    if (OB_HASH_NOT_EXIST == ret) {  // first initialization
      ret = OB_SUCCESS;
      table_compaction_info.reset();
      table_compaction_info.table_id_ = table_id;
      LOG_TRACE("return init table compaction info", KR(ret));
    } else {
      LOG_WARN("fail to get val from hashmap", KR(ret), K(table_id));
    }
  }
  return ret;
}

int ObChecksumValidator::check_inner_status()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("is not init", KR(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!compaction_scn_.is_valid() || expected_epoch_ < 0 || nullptr == schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compaction_scn/expected_epoch/schema_guard_", KR(ret), K_(tenant_id),
      K_(compaction_scn), K_(expected_epoch), KP_(schema_guard));
  } else if (OB_FAIL(check_tablet_checksum_sync_finish(false /*force_check*/))) {
    LOG_WARN("failed to set need_validate", K(ret), K_(tenant_id));
  }
  return ret;
}

void ObChecksumValidator::clear_cached_info()
{
  cross_cluster_ckm_sync_finish_ = false;
  compaction_scn_.reset();
  major_merge_start_us_ = 0;
  schema_guard_ = nullptr;
  simple_schema_ = nullptr;
  table_compaction_info_.reset();
  cur_tablet_ls_pair_array_.reuse();
  finish_tablet_ls_pair_array_.reuse();
  replica_ckm_items_.reuse();
  last_table_ckm_items_.clear();
}

int ObChecksumValidator::get_tablet_ls_pairs(
  const share::schema::ObSimpleTableSchemaV2 &simple_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
    if (OB_UNLIKELY(!simple_schema.has_tablet())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet schema should have tablet", K(ret), K(simple_schema));
    } else if (OB_FAIL(simple_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet_ids from simple schema", KR(ret), K(simple_schema));
    } else if (OB_UNLIKELY(tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K(simple_schema));
    } else if (OB_FAIL(cur_tablet_ls_pair_array_.reserve(tablet_ids.count()))) {
      LOG_WARN("failed to reserve tablet array", KR(ret), K(tablet_ids.count()));
    } else if (OB_FAIL(finish_tablet_ls_pair_array_.reserve(tablet_ids.count()))) {
      LOG_WARN("failed to reserve finish tablet array", KR(ret));
    } else if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_pairs(table_id_, tablet_ids, cur_tablet_ls_pair_array_))) {
      LOG_WARN("failed to tablet ls pair", KR(ret), K(tablet_ids));
    } else {
#ifdef ERRSIM
        static int64_t enter_cnt = 0;
        if (OB_SUCC(ret) && simple_schema.is_global_index_table()) {
          ret = OB_E(EventTable::EN_GET_TABLET_LS_PAIR_IN_RS) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            if (enter_cnt++ == 0) {
              ret = OB_ITEM_NOT_MATCH;
              STORAGE_LOG(INFO, "ERRSIM EN_GET_TABLET_LS_PAIR_IN_RS", K(ret), K(simple_schema), K_(cur_tablet_ls_pair_array));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
#endif
      LOG_TRACE("success to get tablet ls pairs", KR(ret), K_(cur_tablet_ls_pair_array));
    }
  }
  return ret;
}

int ObChecksumValidator::validate_checksum(
  const uint64_t table_id,
  share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  schema_guard_ = &schema_guard;
  if (OB_UNLIKELY(0 == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (FALSE_IT(table_id_ = table_id)) {
  } else if (OB_FAIL(get_table_compaction_info(table_id_, table_compaction_info_))) {
    LOG_WARN("failed to get table compaction info", KR(ret), K_(table_id));
  } else if (OB_FAIL(check_inner_status())) {
    LOG_WARN("failed to check inner status", K(ret));
  } else if (table_compaction_info_.is_verified()
    || table_compaction_info_.can_skip_verifying()) {
    // do nothing
  } else if (tablet_status_map_.empty()) {
    table_compaction_info_.set_uncompacted();
  } else if (OB_FAIL(schema_guard_->get_simple_table_schema(tenant_id_, table_id_, simple_schema_))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id), K_(table_compaction_info));
  } else if (OB_UNLIKELY(nullptr == simple_schema_ // table deleted
    || !simple_schema_->has_tablet())) {
    // like VIEW, it does not have tablet, treat it as compaction finished and can skip verifying
     table_compaction_info_.set_can_skip_verifying();
  } else if (OB_FAIL(get_tablet_ls_pairs(*simple_schema_))) {
    if (OB_ITEM_NOT_MATCH == ret) {
      ret = OB_SUCCESS;
      table_compaction_info_.set_can_skip_verifying();
    } else {
      LOG_WARN("failed to get table pairs", K(ret), KPC_(simple_schema));
    }
  } else if (OB_UNLIKELY(cur_tablet_ls_pair_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ls pair array is unexpected empty", KR(ret), KPC_(simple_schema), K_(cur_tablet_ls_pair_array));
  } else {
    if (OB_FAIL(validate_tablet_replica_checksum())) {
      LOG_WARN("fail to validate tablet replica checksum", KR(ret), K_(compaction_scn), K_(table_id),
        KPC(simple_schema_), K_(cur_tablet_ls_pair_array));
    } else if (OB_FAIL(validate_index_checksum())) {
      LOG_WARN("failed to validate index checksum", K(ret));
    } else if (OB_FAIL(validate_cross_cluster_checksum())) {
      LOG_WARN("failed to validate cross cluster checksum", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (table_compaction_info_.unfinish_index_cnt_ <= 0
      || table_compaction_info_.is_uncompacted()
      || table_compaction_info_.can_skip_verifying()) {
      // not cache index table/uncompacted or skip_verify data table
    } else if (replica_ckm_items_.count() > 0) {
      int tmp_ret = OB_SUCCESS;
      last_table_ckm_items_.clear();
      if (OB_TMP_FAIL(last_table_ckm_items_.build(*schema_guard_, *simple_schema_, cur_tablet_ls_pair_array_, replica_ckm_items_.array_))) {
        LOG_WARN("failed to build table ckm items", KR(tmp_ret), K_(table_id), K_(cur_tablet_ls_pair_array),
          K_(replica_ckm_items));
      } else {
        LOG_DEBUG("success to build ckm item", KR(tmp_ret), K(last_table_ckm_items_), K_(table_compaction_info));
      }
    } else {
      last_table_ckm_items_.clear();
    }
  }
  cur_tablet_ls_pair_array_.reuse(); // need reuse array when get_tablet_ls_pairs failed

  if (FAILEDx(table_compaction_map_.set_refactored(table_id_, table_compaction_info_, true /*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K_(table_id), K_(table_compaction_info));
  } else {
    LOG_TRACE("success to validate table", KR(ret), K_(table_id), K_(table_compaction_info));
  }
  // do no clear table_compaction_info_ until validate next table
  replica_ckm_items_.reuse();
  schema_guard_ = nullptr;
  simple_schema_ = nullptr;
  return ret;
}

int ObChecksumValidator::validate_tablet_replica_checksum()
{
  int ret = OB_SUCCESS;
  if (table_compaction_info_.is_uncompacted()) {
    if (OB_UNLIKELY(nullptr == simple_schema_ || !simple_schema_->has_tablet())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet schema should have tablet", K(ret), KPC_(simple_schema));
    } else {
      if (OB_FAIL(update_table_compaction_info_by_tablet())) {
        LOG_WARN("failed to check table compaction finish", K(ret));
      } else if (table_compaction_info_.is_compacted()) {
        // verify tablet replica checksum
        if (OB_FAIL(get_tablet_replica_checksum_and_validate(false /*include_larger_than*/))) {
          if (OB_ITEM_NOT_MATCH == ret) {
            ret = OB_SUCCESS;
            table_compaction_info_.set_can_skip_verifying();
          } else {
            LOG_WARN("fail to validate tablet replica checksum", KR(ret), K_(compaction_scn), K_(table_compaction_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObChecksumValidator::update_table_compaction_info_by_tablet()
{
  int ret = OB_SUCCESS;
  // iterate all tablets to check 'compacted/finished status' or not.
  int64_t idx = 0;
  const int64_t end_idx = cur_tablet_ls_pair_array_.count();
  for ( ; OB_SUCC(ret) && (idx < end_idx); ++idx) {
    const ObTabletID &tablet_id = cur_tablet_ls_pair_array_.at(idx).get_tablet_id();
    ObTabletCompactionStatus tablet_status = ObTabletCompactionStatus::INITIAL;
    if (OB_FAIL(tablet_status_map_.get_refactored(tablet_id, tablet_status))) {
      // if tablet not finish compaction, it won't be added into this map
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        table_compaction_info_.set_uncompacted();
        (void) uncompact_info_.add_tablet(tenant_id_, cur_tablet_ls_pair_array_.at(idx).get_ls_id(), tablet_id);
        LOG_TRACE("tablet not exist in tablet status map", KR(ret), K(tablet_id),
          K_(cur_tablet_ls_pair_array), K_(table_compaction_info));
#ifdef ERRSIM
        ret = OB_E(EventTable::EN_SKIP_INDEX_MAJOR) ret;
        if (OB_FAIL(ret)) {
          ret = OB_SUCCESS;
          if (tablet_id.id() > ObTabletID::MIN_USER_TABLET_ID) {
            LOG_INFO("ERRSIM EN_SKIP_INDEX_MAJOR", K(ret), K(tablet_id));
            table_compaction_info_.set_can_skip_verifying();
          }
        }
#endif
        break;
      } else {
        LOG_WARN("fail to get tablet compaction status from map", KR(ret), K(idx));
      }
    } else if (ObTabletCompactionStatus::INITIAL == tablet_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet status", KR(ret), K(tablet_status));
    } else if (ObTabletCompactionStatus::CAN_SKIP_VERIFYING == tablet_status) {
      table_compaction_info_.set_can_skip_verifying();
      break;
    }
  } // end of for
  if (OB_SUCC(ret)) {
    if (idx == end_idx) { // loop finish
      table_compaction_info_.tablet_cnt_ = cur_tablet_ls_pair_array_.count();
      table_compaction_info_.set_compacted();
    }
    LOG_TRACE("update_table_compaction_info_by_tablet", KR(ret), K_(table_compaction_info));
  }

  return ret;
}

int ObChecksumValidator::get_tablet_replica_checksum_and_validate(const bool include_larger_than)
{
  int ret = OB_SUCCESS;
  FREEZE_TIME_GUARD;
  if (OB_FAIL(get_replica_ckm(include_larger_than))) {
    LOG_WARN("fail to check major tablet_replica checksum", KR(ret), K_(tenant_id),
      K_(cur_tablet_ls_pair_array), K_(compaction_scn), K_(table_compaction_info));
  } else if (OB_UNLIKELY(replica_ckm_items_.tablet_cnt_ != cur_tablet_ls_pair_array_.count())) {
    ret = OB_ITEM_NOT_MATCH;
    replica_ckm_items_.reuse();
    LOG_TRACE("checksum cnt is not equal to tablet_ls pairs cnt", KR(ret), K_(tenant_id),
      K_(cur_tablet_ls_pair_array), K_(compaction_scn), K_(table_compaction_info), K(replica_ckm_items_));
  } else if (OB_FAIL(verify_tablet_replica_checksum())) {
    if (OB_CHECKSUM_ERROR == ret) {
      LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in major "
        "tablet_replica_checksum", KR(ret), K_(tenant_id),  K_(compaction_scn), K_(cur_tablet_ls_pair_array));
    } else {
      LOG_WARN("fail to check major tablet_replica checksum", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObChecksumValidator::verify_tablet_replica_checksum()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(replica_ckm_items_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_ckm_items_));
  } else {
    const ObTabletReplicaChecksumItem *prev_item = nullptr;
    ObSEArray<ObTabletLSPair, 64> error_pairs;
    error_pairs.set_attr(ObMemAttr(tenant_id_, "CkmErrPairs"));
    ObLSID prev_error_ls_id;
    ObTabletID prev_error_table_id;
    int64_t affected_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < replica_ckm_items_.count()); ++i) {
      const ObTabletReplicaChecksumItem &curr_item = replica_ckm_items_.at(i);
      if (OB_NOT_NULL(prev_item)
        && curr_item.is_same_tablet(*prev_item)) { // same tablet
        if (OB_FAIL(curr_item.verify_checksum(*prev_item))) {
          if (OB_CHECKSUM_ERROR == ret) {
            LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "checksum error in tablet replica checksum", KR(ret),
                          K(curr_item), KPC(prev_item));
            ret = OB_SUCCESS; // continue checking next checksum
            if (curr_item.ls_id_ != prev_error_ls_id || curr_item.tablet_id_ != prev_error_table_id) {
              prev_error_ls_id = curr_item.ls_id_;
              prev_error_table_id = curr_item.tablet_id_;
              if (OB_TMP_FAIL(error_pairs.push_back(ObTabletLSPair(curr_item.tablet_id_, curr_item.ls_id_)))) {
                LOG_WARN("fail to push back error pair", K(tmp_ret), "tablet_id", curr_item.tablet_id_, "ls_id", curr_item.ls_id_);
              }
            }
          } else {
            LOG_WARN("unexpected error in tablet replica checksum", KR(ret), K(curr_item), KPC(prev_item));
          }
        }
      }
      prev_item = &curr_item;
    }
    if (!error_pairs.empty()) {
      if (OB_TMP_FAIL(ObTabletMetaTableCompactionOperator::batch_set_info_status(MTL_ID(), error_pairs, affected_rows))) {
        LOG_WARN("fail to batch set info status", KR(tmp_ret));
      } else {
        LOG_INFO("succ to batch set info status", K(ret), K(affected_rows), K(error_pairs));
      }
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
/* Cross Cluster Checksum Validator Section */
int ObChecksumValidator::validate_cross_cluster_checksum()
{
  int ret = OB_SUCCESS;
  const bool check_special_table = (ObChecksumValidator::SPECIAL_TABLE_ID == table_id_);
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (table_compaction_info_.is_index_ckm_verified()) {
    // not sync/valid cross cluster before validate index checksum
    if (need_validate_cross_cluster_ckm_) { // need to validate cross-cluster checksum
      if (cross_cluster_ckm_sync_finish_ && OB_FAIL(validate_replica_and_tablet_checksum())) {
        LOG_WARN("fail to validate cross-cluster checksum", KR(ret), K_(stop),
                 K_(compaction_scn), K_(expected_epoch), K_(table_id));
      }
    } else { // no need to validate cross-cluster checksum, write checksum to inner_table
      if (OB_FAIL(try_update_tablet_checksum_items())) {
        LOG_WARN("fail to wrote checksum", KR(ret), K_(tenant_id), K_(compaction_scn), KPC_(simple_schema));
      }
    }
    ret = OB_ITEM_NOT_MATCH == ret ? OB_SUCCESS : ret; // clear errno
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_finish_tablet_ls_pairs_with_update(cur_tablet_ls_pair_array_))) {
        LOG_WARN("failed to push back tablet_ls_pair", KR(ret));
      } else {
        table_compaction_info_.set_verified();
        LOG_TRACE("after cross cluster validate table checksum", K(ret), K_(table_compaction_info));
      }
    }
  } else {  // no need to validate cross-cluster checksum
    // do nothing. index validator should already wrote ckm and updated report_scn
  }
  return ret;
}

int ObChecksumValidator::batch_write_tablet_ckm()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (finish_tablet_ckm_array_.empty()) {
  } else if (!is_primary_service_) {
    // only primary major_freeze_service need to write tablet checksum
  } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch(*sql_proxy_, tenant_id_,
              ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch_, is_match))) {
    LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K_(expected_epoch));
    // Can not select freeze_service_epoch for update and update tablet_checksum_items in one same
    // transaction, since __all_service_epoch and __all_tablet_checksum are not in one same tenant.
    // Therefore, just get and check freeze_service_epoch here. However, this does not impact the
    // correctness of updating tablet_checksum_items.
  } else if (!is_match) {
    ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
    LOG_WARN("no need to update tablet checksum items, cuz freeze_service_epoch mismatch",
             K_(tenant_id), K_(expected_epoch));
  } else {
    const int64_t IMMEDIATE_RETRY_CNT = 5;
    int64_t fail_count = 0;
    int64_t sleep_time_us = 200 * 1000; // 200 ms
    while (OB_SUCC(ret) && !stop_
          && (fail_count < IMMEDIATE_RETRY_CNT)) {
      if (OB_SUCC(ObTabletChecksumOperator::update_tablet_checksum_items(
          *sql_proxy_, tenant_id_, finish_tablet_ckm_array_))) {
        ++statistics_.write_ckm_sql_cnt_;
        break;
      } else if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
        LOG_WARN("freeze_service_epoch mismatch, no need to write tablet checksum items", KR(ret), K_(tenant_id));
        break;
      } else if (OB_ITEM_NOT_MATCH == ret) {
        LOG_INFO("tablet replica checksum item is empty, no need to write tablet checksum items", KR(ret), K_(tenant_id));
        break;
      } else {
        ++fail_count;
        LOG_WARN("fail to write tablet checksum items", KR(ret), K_(tenant_id), K(fail_count), K(sleep_time_us));
        USLEEP(sleep_time_us);
        sleep_time_us *= 2;
      }
    } // end of while
    finish_tablet_ckm_array_.reuse();
  }
  return ret;
}

int ObChecksumValidator::batch_update_report_scn()
{
  int ret = OB_SUCCESS;
  if (finish_tablet_ls_pair_array_.empty()) {
  } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
          tenant_id_, compaction_scn_.get_val_for_tx(),
          finish_tablet_ls_pair_array_,
          ObTabletReplica::ScnStatus::SCN_STATUS_ERROR /*except_status*/,
          expected_epoch_))) {
    LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id),
             K_(finish_tablet_ls_pair_array));
  } else {
    ++statistics_.update_report_scn_sql_cnt_;
    LOG_INFO("success to batch update report_scn", KR(ret), K_(tenant_id),
             "table_cnt", finish_tablet_ls_pair_array_.count());
    finish_tablet_ls_pair_array_.reuse();
  }
  return ret;
}

int ObChecksumValidator::check_tablet_checksum_sync_finish(const bool force_check)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  // need check inner table:
  // 1) force check when first init
  // 2) ckm not sync finish in standby service
  if (!force_check && (is_primary_service_ || cross_cluster_ckm_sync_finish_)) {
  } else if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*sql_proxy_,
    tenant_id_, compaction_scn_, is_exist))) {
    LOG_WARN("fail to check is first tablet in first ls exist", KR(ret), K_(tenant_id),  K_(compaction_scn));
  } else if (is_exist) {
    cross_cluster_ckm_sync_finish_ = true;
  } else if (is_primary_service_) {
    cross_cluster_ckm_sync_finish_ = false;
  } else {
    cross_cluster_ckm_sync_finish_ = check_waiting_tablet_checksum_timeout();
    if (TC_REACH_TIME_INTERVAL(PRINT_CROSS_CLUSTER_LOG_INVERVAL)) {
      LOG_WARN("can not check cross-cluster checksum now, please wait until first tablet"
             "in sys ls exists", K_(tenant_id),  K_(compaction_scn), K_(major_merge_start_us),
             "fast_current_time_us", ObTimeUtil::fast_current_time(), K(is_exist), K_(is_primary_service));
    }
  }
  return ret;
}

int ObChecksumValidator::validate_replica_and_tablet_checksum()
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObArray<ObTabletChecksumItem>, tablet_checksum_items) {
    FREEZE_TIME_GUARD;
    if (replica_ckm_items_.empty() && OB_FAIL(get_replica_ckm())) {
      LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K_(tenant_id), K_(compaction_scn));
    } else if (OB_FAIL(ObTabletChecksumOperator::load_tablet_checksum_items(*sql_proxy_,
                        cur_tablet_ls_pair_array_, tenant_id_, compaction_scn_, tablet_checksum_items))) {
      LOG_WARN("fail to batch get tablet checksum items", KR(ret), K_(tenant_id), K_(compaction_scn));
    } else if (replica_ckm_items_.empty() || tablet_checksum_items.empty()
        || replica_ckm_items_.tablet_cnt_ != tablet_checksum_items.count()) {
      ret = OB_ITEM_NOT_MATCH;
      table_compaction_info_.set_verified();
      LOG_WARN("fail to get checksum items", KR(ret), K_(tenant_id), K_(compaction_scn),
        K(replica_ckm_items_), K(tablet_checksum_items));
    } else if (OB_FAIL(check_column_checksum(replica_ckm_items_.array_, tablet_checksum_items))) {
      if (OB_CHECKSUM_ERROR == ret) {
        LOG_ERROR("ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", KR(ret),
          K_(tenant_id), K_(compaction_scn));
      } else {
        LOG_WARN("fail to check cross-cluster checksum", KR(ret), K_(tenant_id),
          K_(compaction_scn));
      }
    }
  }
  return ret;
}

int ObChecksumValidator::check_column_checksum(
    const ObArray<ObTabletReplicaChecksumItem> &tablet_replica_checksum_items,
    const ObArray<ObTabletChecksumItem> &tablet_checksum_items)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObTabletChecksumItem tablet_checksum_item;
  int64_t i = 0; // tablet_ckm_idx
  int64_t j = 0; // replica_ckm_idx
  int64_t tablet_checksum_item_cnt = tablet_checksum_items.count();
  int64_t tablet_replica_checksum_item_cnt = tablet_replica_checksum_items.count();
  while (OB_SUCC(ret) && (i < tablet_checksum_item_cnt) && (j < tablet_replica_checksum_item_cnt)) {
    cmp_ret = 0;
    const ObTabletChecksumItem &tablet_ckm_item = tablet_checksum_items.at(i);
    do {
      if (cmp_ret >= 0) { // iterator all tablet replica checksum util next different tablet.
        const ObTabletReplicaChecksumItem &replica_ckm_item = tablet_replica_checksum_items.at(j);
        if (0 == (cmp_ret = tablet_ckm_item.compare_tablet(replica_ckm_item))) {
          if (OB_FAIL(tablet_ckm_item.verify_tablet_column_checksum(replica_ckm_item))) {
            if (OB_CHECKSUM_ERROR == ret) {
              LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in "
                            "cross-cluster checksum", K(tablet_ckm_item), K(replica_ckm_item));
            } else {
              LOG_WARN("unexpected error in cross-cluster checksum", KR(ret),
                       K(tablet_ckm_item), K(replica_ckm_item));
            }
          }
        }
      }
      if (cmp_ret >= 0) {
        ++j;
      }
    } while ((cmp_ret >= 0) && (j < tablet_replica_checksum_item_cnt) && OB_SUCC(ret));
    ++i;
  } // end of while
  return ret;
}

bool ObChecksumValidator::check_waiting_tablet_checksum_timeout() const
{

  const int64_t total_wait_time_us = (ObTimeUtil::fast_current_time() - major_merge_start_us_);
  const bool is_timeout = (total_wait_time_us > MAX_TABLET_CHECKSUM_WAIT_TIME_US);
  if (is_timeout) {
    LOG_WARN_RET(OB_TIMEOUT, "check waiting tablet checksum timeout", K_(major_merge_start_us), K(total_wait_time_us));
  }
  return is_timeout;
}

int ObChecksumValidator::try_update_tablet_checksum_items()
{
  int ret = OB_SUCCESS;
  const bool include_lager_than = (table_id_ == SPECIAL_TABLE_ID ? true : false);
  if (replica_ckm_items_.empty() && OB_FAIL(get_replica_ckm(include_lager_than))) {
    LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K_(tenant_id),  K_(compaction_scn));
  } else if (replica_ckm_items_.tablet_cnt_ < cur_tablet_ls_pair_array_.count()) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("fail to get tablet replica checksum items", KR(ret), K_(tenant_id),  K_(compaction_scn),
      K_(cur_tablet_ls_pair_array), K(replica_ckm_items_));
  } else if (OB_FAIL(push_tablet_ckm_items_with_update(replica_ckm_items_.array_))) {
    LOG_WARN("fail to push tablet checksum items", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObChecksumValidator::push_finish_tablet_ls_pairs_with_update(
  const common::ObIArray<share::ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(finish_tablet_ls_pair_array_.push_back(tablet_ls_pairs))) {
    LOG_WARN("failed to push back tablet_ls_pairs", KR(ret), K(tablet_ls_pairs));
  } else {
    bool need_update_report_scn = (finish_tablet_ls_pair_array_.count() >= MAX_BATCH_INSERT_COUNT)
      || table_id_ == SPECIAL_TABLE_ID;
#ifdef ERRSIM
    need_update_report_scn = true;
#endif
    if (need_update_report_scn) {
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(batch_update_report_scn())) {
        LOG_WARN("failed to batch update report scn", KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObChecksumValidator::push_tablet_ckm_items_with_update(
  const ObIArray<ObTabletReplicaChecksumItem> &replica_ckm_items)
{
  int ret = OB_SUCCESS;
  const ObTabletReplicaChecksumItem *prev_replica_item = nullptr;
  ObTabletChecksumItem tmp_checksum_item;
  for (int64_t i = 0; !stop_ && OB_SUCC(ret) && (i < replica_ckm_items.count()); ++i) {
    const ObTabletReplicaChecksumItem &curr_replica_item = replica_ckm_items.at(i);
    if (OB_UNLIKELY(!curr_replica_item.is_key_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet replica checksum is not valid", KR(ret),
               K(curr_replica_item));
    } else {
      if (nullptr != prev_replica_item && curr_replica_item.is_same_tablet( *prev_replica_item)) { // write one checksum_item per tablet
      } else if (OB_FAIL(tmp_checksum_item.assign(curr_replica_item))) {
        // ObTabletReplicaChecksumItem->ObTabletChecksumItem
        LOG_WARN("fail to assign tablet replica checksum item", KR(ret),
                 K(curr_replica_item));
      } else if (OB_FAIL(finish_tablet_ckm_array_.push_back(tmp_checksum_item))) {
        LOG_WARN("fail to push back tablet checksum item", KR(ret),
                 K(curr_replica_item), K(tmp_checksum_item));
      }
      prev_replica_item = &curr_replica_item;
    }
  } // end of for
  if (OB_SUCC(ret)
      && (finish_tablet_ckm_array_.count() >= MAX_BATCH_INSERT_COUNT || table_id_ == SPECIAL_TABLE_ID)) {
    (void) batch_write_tablet_ckm();
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
/* Data Table - Index Table Checksum Validator Section */
int ObChecksumValidator::validate_index_checksum() {
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(simple_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is unexpected null", K(ret), KPC_(simple_schema));
  } else if (!need_validate_index_ckm_) { // no need to validate data-index checksum
    table_compaction_info_.set_index_ckm_verified();
  } else if (simple_schema_->is_index_table()) { // for index table, do not check status
    if (OB_FAIL(handle_index_table(*simple_schema_))) {
      LOG_WARN("fail to handle index table", KR(ret), KPC_(simple_schema));
    }
  } else if (table_compaction_info_.need_check_fts_) {
    LOG_INFO("check fts for data table", KR(ret), K_(table_compaction_info));
  } else if (table_compaction_info_.is_compacted()) { // for data table, check status
    if (0 == table_compaction_info_.unfinish_index_cnt_) { // no unfinish index
      table_compaction_info_.set_index_ckm_verified();
    }
  }
  LOG_TRACE("validate_index_checksum", KR(ret), K_(table_id), K_(table_compaction_info));
  return ret;
}

int ObChecksumValidator::handle_index_table(
  const share::schema::ObSimpleTableSchemaV2 &index_simple_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t index_table_id = index_simple_schema.get_table_id();
  const uint64_t data_table_id = index_simple_schema.get_data_table_id();
  ObTableCompactionInfo &index_compaction_info = table_compaction_info_; // cur table is index
  ObTableCompactionInfo data_compaction_info;
  if (OB_FAIL(get_table_compaction_info(data_table_id, data_compaction_info))) {
    LOG_WARN("fail to get table compaction info", KR(ret), K(data_table_id));
  } else if (!index_simple_schema.can_read_index()) {
    // for index table can not read, directly mark it as VERIFIED
    // do not check compaction_scn and validate checksum of can not read
    // index's tablets. although update_all_tablets_report_scn will update
    // its report_scn. the storage layer may schedule major compaction and
    // increase compaction_scn of this index's tablets later.
    index_compaction_info.set_can_skip_verifying();
  } else if (data_compaction_info.is_index_ckm_verified() || data_compaction_info.is_verified()) {
    // if a data table finished verification, then create index on this data table.
    // we should skip verification for this index table, cuz the data table may already
    // launched another medium compaction.
    LOG_INFO("index table is not verified while data table is already verified, skip"
            " verification for this index table", K(index_table_id), K(data_table_id),
            K(index_compaction_info), K(data_compaction_info));
    if (index_compaction_info.finish_compaction()) {
      index_compaction_info.set_index_ckm_verified();
    }
  } else if (fts_group_array_.need_check_fts() && index_simple_schema.is_fts_or_multivalue_index()) {
    LOG_INFO("skip fts or multivalue index", KR(ret), K(index_simple_schema), K(index_compaction_info));
  } else {
      if (index_compaction_info.is_compacted() && data_compaction_info.is_compacted()) {
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = OB_E(EventTable::EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            if (!is_inner_table(index_table_id)) {
              ret = OB_EAGAIN;
              STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            return ret;
          }
        }
#endif
      // set it to false, if succ to handle_table_can_not_verify
      // both tables' all tablets finished compaction, validate column
      // checksum if need_validate()
      if (OB_UNLIKELY(index_simple_schema.should_not_validate_data_index_ckm())) {
        // do nothing
        // spatial index column is different from data table column
        index_compaction_info.set_index_ckm_verified();
      } else if (1 == data_compaction_info.unfinish_index_cnt_ || last_table_ckm_items_.is_inited()) {
        // only one index
        if (OB_FAIL(verify_table_index(index_simple_schema, data_compaction_info, index_compaction_info))) {
          LOG_WARN("failed to verify table index checksum", K(ret), K(index_simple_schema));
        }
      } else if (OB_FAIL(idx_ckm_validate_array_.push_back(ObIndexCkmValidatePair(data_table_id, index_table_id)))) {
        LOG_WARN("failed to push back table validate info", K(ret), K(data_table_id), K(index_table_id));
      }
    } else if (index_compaction_info.can_skip_verifying()
      || data_compaction_info.can_skip_verifying()) {
        // if one of them can skip verifying, that means we don't need to
        // execute index checksum verification. Mark index table as
        // INDEX_CKM_VERIFIED directly.
      index_compaction_info.set_index_ckm_verified();
    }
  }
  // deal with data table
  if (OB_SUCC(ret) && index_compaction_info.finish_idx_verified() && !data_compaction_info.finish_idx_verified()) {
    if ((0 == (--data_compaction_info.unfinish_index_cnt_)) && !data_compaction_info.need_check_fts_) {
      data_compaction_info.set_index_ckm_verified();
    }
    if (OB_FAIL(table_compaction_map_.set_refactored(
            data_compaction_info.table_id_, data_compaction_info,
            true /*overwrite*/))) {
      LOG_WARN("failed to set", K(ret), K(data_compaction_info));
    }
  }
  LOG_TRACE("handle index table", KR(ret), K_(table_id), K(index_compaction_info), K(data_compaction_info));
  return ret;
}

int ObChecksumValidator::verify_table_index(
    const share::schema::ObSimpleTableSchemaV2 &index_simple_schema,
    compaction::ObTableCompactionInfo &data_compaction_info,
    compaction::ObTableCompactionInfo &index_compaction_info)
{
  int ret = OB_SUCCESS;
  FREEZE_TIME_GUARD;
  const uint64_t index_table_id = index_simple_schema.get_table_id();
  const uint64_t data_table_id = index_simple_schema.get_data_table_id();
  if (replica_ckm_items_.empty() && OB_FAIL(get_replica_ckm())) {
    LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K_(tenant_id),  K_(compaction_scn));
  } else if (replica_ckm_items_.tablet_cnt_ < cur_tablet_ls_pair_array_.count()) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("fail to get tablet replica checksum items", KR(ret), K_(tenant_id),  K_(compaction_scn),
      K_(cur_tablet_ls_pair_array), K(replica_ckm_items_));
  } else {
    ObTableCkmItems data_table_ckm(tenant_id_);
    ObTableCkmItems *data_table_ckm_ptr = nullptr;
    ObTableCkmItems index_table_ckm(tenant_id_);
    if (last_table_ckm_items_.is_inited()) { // use cached data table ckm
      if (OB_UNLIKELY(last_table_ckm_items_.get_table_id() != data_table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cached last table ckm items is invalid", KR(ret), K(data_table_id), K_(last_table_ckm_items));
      } else {
        data_table_ckm_ptr = &last_table_ckm_items_;
        ++statistics_.use_cached_ckm_cnt_;
      }
    }
    if (nullptr != data_table_ckm_ptr || OB_FAIL(ret)) {
    } else if (FALSE_IT(data_table_ckm_ptr = &data_table_ckm)) {
    } else if (OB_FAIL(data_table_ckm.build(data_table_id, compaction_scn_,
                                     *sql_proxy_, *schema_guard_,
                                     tablet_ls_pair_cache_))) {
      LOG_WARN("failed to get checksum items", K(ret), K(data_table_id), K_(compaction_scn));
    } else {
      ++statistics_.query_ckm_sql_cnt_;
    }
    if (FAILEDx(index_table_ckm.build(*schema_guard_, index_simple_schema, cur_tablet_ls_pair_array_,
                                      replica_ckm_items_.array_))) {
      LOG_WARN("failed to assgin checksum items", K(ret), K(replica_ckm_items_));
    } else {
      const bool is_global_index = index_simple_schema.is_global_index_table();
      if (OB_FAIL(ObTableCkmItems::validate_ckm_func[is_global_index](
          compaction_scn_,
          *sql_proxy_,
          *data_table_ckm_ptr,
          index_table_ckm))) {
        LOG_WARN("failed to validate checksum", K(ret), K(data_table_id),
          K(index_table_id), KPC(data_table_ckm_ptr), K(index_table_ckm),
          K_(replica_ckm_items), K_(cur_tablet_ls_pair_array), K_(last_table_ckm_items));
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_ITEM_NOT_MATCH == ret) {
      index_compaction_info.set_can_skip_verifying();
      ret = OB_SUCCESS; // clear errno
    }
  } else {
    index_compaction_info.set_index_ckm_verified();
  }
  return ret;
}

int ObChecksumValidator::get_replica_ckm(const bool include_larger_than/* = false*/)
{
  ++statistics_.query_ckm_sql_cnt_;
  return ObTabletReplicaChecksumOperator::batch_get(tenant_id_, cur_tablet_ls_pair_array_, compaction_scn_,
      *sql_proxy_, replica_ckm_items_.array_, replica_ckm_items_.tablet_cnt_, include_larger_than,
      share::OBCG_DEFAULT, true/*with_order_by_field*/);
}

/***************************************** FTS Checksum Section ******************************************/

int ObChecksumValidator::build_ckm_item_for_fts(const int64_t table_id,
                                                ObTableCkmItems &ckm_item,
                                                ObIArray<int64_t> &finish_table_ids)
{
  int ret = OB_SUCCESS;
  bool skip_verify = false;
  ObTableCompactionInfo table_compaction_info;
  if (OB_FAIL(get_table_compaction_info(table_id, table_compaction_info))) {
    LOG_WARN("failed to get table compaction info", KR(ret));
  } else if (OB_UNLIKELY(!table_compaction_info.is_compacted())) {
    LOG_WARN("exist special status table", KR(ret), K(table_compaction_info));
    skip_verify = true;
  } else if (OB_FAIL(ckm_item.build(table_id, compaction_scn_, *sql_proxy_,
                                    *schema_guard_, tablet_ls_pair_cache_))) {
    if (OB_TABLE_NOT_EXIST == ret || OB_STATE_NOT_MATCH == ret) {
      skip_verify = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to prepare schema checksum items", KR(ret), K_(tenant_id), K(table_id));
    }
  } else if (OB_FAIL(finish_table_ids.push_back(table_id))) {
    LOG_WARN("failed to push index id", KR(ret), K(table_id));
  } else {
    ckm_item.set_is_fts_index(true);
  }
  if (OB_FAIL(ret) || !skip_verify) {
  } else if (OB_FAIL(finish_verify_fts_ckm(table_id))) {
    LOG_WARN("failed to skip verify fts ckm", KR(ret), K(table_id));
  } else {
    LOG_INFO("skip verify fts ckm", KR(ret), K(table_id));
  }
  return ret;
}

int ObChecksumValidator::finish_verify_fts_ckm(const int64_t table_id)
{
  int ret = OB_SUCCESS;
  ObTableCompactionInfo table_compaction_info;
  if (OB_FAIL(get_table_compaction_info(table_id, table_compaction_info))) {
    LOG_WARN("fail to get table compaction info", KR(ret), K(table_id), K(table_compaction_info));
  } else if (FALSE_IT(table_compaction_info.need_check_fts_ = false)) {
  } else if (table_compaction_info.unfinish_index_cnt_ <= 0) {
    // for data table, may exist other index
    table_compaction_info.set_index_ckm_verified();
  }
  if (FAILEDx(table_compaction_map_.set_refactored(table_id, table_compaction_info, true /*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
  }
  return ret;
}

#define VALIDATE_CKM(data_ckm, index_ckm)                                      \
  if (OB_FAIL(ret) || !data_ckm.is_inited() || !index_ckm.is_inited()) {       \
  } else if (OB_FAIL(ObTableCkmItems::validate_ckm_func[0](                    \
                 compaction_scn_, *sql_proxy_, data_ckm, index_ckm))) {        \
    LOG_ERROR("failed to validate ckm func", KR(ret), K(data_ckm),             \
              K(index_ckm));                                                   \
  }

int ObChecksumValidator::handle_fts_checksum(
  share::schema::ObSchemaGetterGuard &schema_guard,
  const ObFTSGroupArray &fts_group_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fts_group_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fts_group_array));
  } else {
    schema_guard_ = &schema_guard;
    ObSEArray<int64_t, 16> finish_table_ids;
    finish_table_ids.set_attr(ObMemAttr(tenant_id_, "FTS_CKM_VER"));
    for (int64_t arr_idx = 0; OB_SUCC(ret) && arr_idx < fts_group_array.count(); ++arr_idx) {
      const ObFTSGroup &fts_group = fts_group_array.at(arr_idx);
      if (OB_FAIL(validate_rowkey_doc_indexs(fts_group, finish_table_ids))) {
        LOG_WARN("failed to validate rowkey doc indexs", KR(ret), K(fts_group));
      }
      for (int64_t idx = 0; OB_SUCC(ret) && idx < fts_group.count(); ++idx) {
        if (OB_FAIL(validate_fts_indexs(fts_group.at(idx), finish_table_ids))) {
          LOG_WARN("failed to validate doc rowkey index", KR(ret), K(idx), K(fts_group));
        } else {
          LOG_INFO("validate index info", K(ret), K(fts_group), K(idx), K(fts_group.at(idx)));
        }
      } // for of fts_group
    } // for of fts_group_array
    for (int64_t idx = 0; OB_SUCC(ret) && idx < finish_table_ids.count(); ++idx) {
      if (OB_FAIL(finish_verify_fts_ckm(finish_table_ids.at(idx)))) {
        LOG_WARN("fail to skip or finish verify fts", KR(ret), K(idx), K(finish_table_ids));
      }
    } // for
    schema_guard_ = NULL;
  }

  return ret;
}

int ObChecksumValidator::validate_rowkey_doc_indexs(const ObFTSGroup &fts_group, ObIArray<int64_t> &finish_table_ids)
{
  int ret = OB_SUCCESS;
  ObTableCkmItems ckm_item[3];
  if (OB_FAIL(build_ckm_item_for_fts(fts_group.data_table_id_, ckm_item[0], finish_table_ids))) {
    LOG_WARN_RET(ret, "failed to build ckm", K(fts_group.data_table_id_));
  } else if (OB_FAIL(build_ckm_item_for_fts(fts_group.rowkey_doc_index_id_, ckm_item[1], finish_table_ids))) {
    LOG_WARN_RET(ret, "failed to build ckm", K(fts_group.rowkey_doc_index_id_));
  } else if (OB_FAIL(build_ckm_item_for_fts(fts_group.doc_rowkey_index_id_, ckm_item[2], finish_table_ids))) {
    LOG_WARN_RET(ret, "failed to build ckm", K(fts_group.doc_rowkey_index_id_));
  }
  // all fts index is local index now
  VALIDATE_CKM(ckm_item[0], ckm_item[1]);
  VALIDATE_CKM(ckm_item[1], ckm_item[2]);
  return ret;
}

int ObChecksumValidator::validate_fts_indexs(const ObFTSIndexInfo &index_info, ObIArray<int64_t> &finish_table_ids)
{
  int ret = OB_SUCCESS;
  ObTableCkmItems ckm_item[2];
  if (OB_FAIL(build_ckm_item_for_fts(index_info.fts_index_id_, ckm_item[0], finish_table_ids))) {
    LOG_WARN_RET(ret, "failed to build ckm", K(index_info.fts_index_id_));
  } else if (OB_FAIL(build_ckm_item_for_fts(index_info.doc_word_index_id_, ckm_item[1], finish_table_ids))) {
    LOG_WARN_RET(ret, "failed to build ckm", K(index_info.doc_word_index_id_));
  }
  VALIDATE_CKM(ckm_item[0], ckm_item[1]);
  return ret;
}
#undef VALIDATE_CKM

} // end namespace rootserver
} // end namespace oceanbase
