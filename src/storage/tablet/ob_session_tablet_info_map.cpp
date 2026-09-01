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

#include "storage/tablet/ob_session_tablet_info_map.h"
#include "storage/tablet/ob_session_tablet_helper.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#include "observer/omt/ob_tenant_config_mgr.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

int ObSessionTabletInfo::init(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id, const uint64_t table_id,
  const int64_t sequence, const uint64_t session_id, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  ls_id_ = ls_id;
  table_id_ = table_id;
  sequence_ = sequence;
  session_id_ = session_id;
  transfer_seq_ = transfer_seq;
  if (is_valid() == false) {
    reset();
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(ls_id), K(table_id),
      K(sequence), K(session_id), K(transfer_seq));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSessionTabletInfo,
                    table_id_,
                    sequence_,
                    session_id_,
                    ls_id_,
                    tablet_id_,
                    transfer_seq_);

ObSessionTabletInfoMap::ObSessionTabletInfoMap()
  : tablet_infos_(),
    mutex_(common::ObLatchIds::SESSION_TABLET_INFO_MAP_LOCK)
{
  tablet_infos_.set_attr(lib::ObMemAttr(MTL_ID(), "SessTblInfoM"));
}

OB_SERIALIZE_MEMBER(ObSessionTabletInfoMap,
                    tablet_infos_);

int ObSessionTabletInfoMap::get_session_tablet_if_not_exist_add(
    const ObSessionTabletInfoKey &key,
    ObSessionTabletInfo &session_tablet_info)
{
  int ret = OB_SUCCESS;
  session_tablet_info.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == key.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(inner_get_session_tablet(key.table_id_,
                                         key.sequence_,
                                         key.session_id_,
                                         session_tablet_info))
            && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to inner get session tablet", KR(ret), K(key));
    }
  }
  if (OB_SUCC(ret)) { // already exist
    LOG_INFO("session tablet already exists", KR(ret), K(key), K(session_tablet_info));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    common::ObArray<uint64_t> table_ids;
    if (OB_FAIL(table_ids.push_back(key.table_id_))) {
      LOG_WARN("failed to push back", KR(ret), K(key));
    } else if (OB_FAIL(add_session_tablet(table_ids, key.sequence_, key.session_id_))) {
      LOG_WARN("failed to add session tablet", KR(ret), K(key));
    } else if (OB_FAIL(get_session_tablet(key, session_tablet_info))) { // tablet info should have been added to tablet_infos_
      LOG_WARN("failed to get session tablet", KR(ret), K(key));
    }
  } else {
    LOG_WARN("failed to get session tablet", KR(ret), K(key));
  }
  return ret;
}

int ObSessionTabletInfoMap::add_session_tablet(
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id)
{
  int ret = OB_SUCCESS;
  ObSessionTabletInfo tablet_info;
  const uint64_t tenant_id = MTL_ID();
  // Dependency direction is map -> helper, one-way:
  //   1. The map drives the reuse classification via its own
  //      try_reuse_truncated_tablets (runs in its own transaction,
  //      committed before the create flow starts).
  //   2. The map plumbs the result into the helper via set_reuse_result.
  //   3. The helper drives the create flow inside its own transaction.
  //   4. The helper does NOT call back into the map.
  ObSessionTabletCreateHelper create_helper(tenant_id, sequence, session_id, *this);
  common::ObSEArray<common::ObTabletID, 4> reused_tablet_ids;
  share::ObLSID resolved_ls_id;
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_ids));
  } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "temporary table access in standby tenant");
    LOG_WARN("temporary table access is not supported in standby tenant", KR(ret), K(table_ids), K(sequence), K(session_id));
  } else if (OB_FAIL(create_helper.set_table_ids(table_ids))) {
    LOG_WARN("failed to set table ids", KR(ret), K(table_ids));
  } else if (OB_FAIL(try_reuse_truncated_tablets(tenant_id, table_ids, sequence, session_id,
                                                 reused_tablet_ids, resolved_ls_id))) {
    LOG_WARN("failed to try reuse truncated tablets", KR(ret),
        K(tenant_id), K(table_ids), K(sequence), K(session_id));
  } else if (OB_FAIL(create_helper.set_reuse_result(reused_tablet_ids, resolved_ls_id))) {
    LOG_WARN("failed to set reuse result", KR(ret));
  } else if (OB_FAIL(create_helper.do_work())) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      // table_ids and exist_table_ids may be different, so we need to get the exist_table_ids from create_helper.
      const common::ObIArray<uint64_t> &exist_table_ids = create_helper.get_table_ids();
      lib::ObMutexGuard guard(mutex_);
      ARRAY_FOREACH(exist_table_ids, idx) {
        const uint64_t table_id = exist_table_ids.at(idx);
        tablet_info.reset();
        if (OB_FAIL(inner_get_session_tablet(table_id, sequence, session_id, tablet_info))) {
          LOG_WARN("failed to inner get session tablet", KR(ret), K(table_id), K(sequence), K(session_id));
        } else {
          LOG_INFO("session tablet already exists, skip create", KR(ret), K(table_id), K(sequence), K(session_id));
        }
      }
    } else {
      LOG_WARN("failed to create session tablet", KR(ret), K(table_ids));
    }
  } else if (OB_UNLIKELY(create_helper.get_tablet_ids().count() < table_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(create_helper.get_tablet_ids().count()), K(table_ids.count()));
  } else {
    const common::ObIArray<uint64_t> &create_table_ids = create_helper.get_table_ids();
    const common::ObIArray<common::ObTabletID> &tablet_ids = create_helper.get_tablet_ids();
    const common::ObIArray<int64_t> &to_create_indices = create_helper.get_to_create_indices();
    const share::ObLSID &ls_id = create_helper.get_ls_id();
    const int64_t total_cnt = create_table_ids.count();
    if(OB_UNLIKELY(total_cnt != tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ids mismatch with tablet ids", K(ret), K(create_table_ids), K(tablet_ids));
    }
    lib::ObMutexGuard guard(mutex_);
    int64_t cur_idx = 0;
    /// @c to_create_indices is guaranteed to be strictly increasing
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      tablet_info.reset();
      const uint64_t table_id = create_table_ids.at(i);
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_FAIL(tablet_info.init(tablet_id, ls_id, table_id, sequence, session_id, 0/*transfer_seq*/))) {
        LOG_WARN("failed to init session tablet info", K(ret), K(tablet_id), K(ls_id), K(table_id), K(sequence), K(session_id));
      } else if (cur_idx < to_create_indices.count() && i == to_create_indices.at(cur_idx)) {
        // tablet is newly created.
        ++cur_idx;
        tablet_info.is_creator_ = true;
        if (OB_FAIL(tablet_infos_.push_back(tablet_info))) {
          LOG_WARN("failed to push back", K(ret), K(tablet_info));
        }
      } else {
        // update sequence only if tablet is not created at this round.
        if (OB_FAIL(update_session_tablet_sequence_without_lock(
              table_id,
              tablet_id,
              session_id,
              sequence))) {
          LOG_WARN("failed to update session tablet sequence", K(ret), K(tablet_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("session tablet added", KR(ret), K(table_ids), K(create_table_ids), K(to_create_indices), K(sequence), K(session_id), K(tablet_ids), K(tablet_infos_));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::get_session_tablet(
    const ObSessionTabletInfoKey &key,
    ObSessionTabletInfo &session_tablet_info,
    const bool local_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == key.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(inner_get_session_tablet(key.table_id_, key.sequence_, key.session_id_,
                                         session_tablet_info, local_only))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to inner get session tablet", KR(ret), K(key), K(local_only));
      }
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::inner_get_session_tablet(
    const uint64_t table_id,
    const int64_t sequence,
    const uint64_t session_id,
    ObSessionTabletInfo &session_tablet_info,
    const bool local_only)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
    if (tablet_infos_.at(i).table_id_ == table_id &&
        tablet_infos_.at(i).sequence_ == sequence &&
        tablet_infos_.at(i).session_id_ == session_id) {
      session_tablet_info = tablet_infos_.at(i);
      break;
    }
  }
  if (OB_SUCC(ret) && i >= tablet_infos_.count()) {
    if (local_only) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sql proxy", K(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::point_get(
                   *GCTX.sql_proxy_, MTL_ID(), table_id, sequence, session_id, session_tablet_info))) {
      // try get from inner table
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get session tablet from inner table", KR(ret), K(table_id), K(sequence), K(session_id));
      }
    } else if (OB_UNLIKELY(!session_tablet_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(session_tablet_info));
    } else if (OB_FAIL(tablet_infos_.push_back(session_tablet_info))) {
      LOG_WARN("failed to push back", KR(ret), K(session_tablet_info));
    } else {
      FLOG_INFO("session tablet get from inner table", KR(ret), K(table_id), K(sequence), K(session_id), K(session_tablet_info), K(tablet_infos_));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::remove_session_tablet(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else {
    lib::ObMutexGuard guard(mutex_);
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).table_id_ == table_id) {
        break;
      }
    }
    if (OB_SUCC(ret) && i >= tablet_infos_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("session tablet not found", KR(ret), K(table_id));
    } else if (OB_FAIL(tablet_infos_.remove(i))) {
      LOG_WARN("failed to remove", KR(ret), K(table_id));
    }
    FLOG_INFO("session tablet removed", KR(ret), K(table_id), K(i), K(tablet_infos_));
  }
  return ret;
}

int ObSessionTabletInfoMap::remove_session_tablet(const ObSessionTabletInfoKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == key.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mutex_);
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).table_id_ == key.table_id_ &&
          tablet_infos_.at(i).sequence_ == key.sequence_ &&
          tablet_infos_.at(i).session_id_ == key.session_id_) {
        break;
      }
    }
    if (OB_SUCC(ret) && i >= tablet_infos_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("session tablet not found", KR(ret), K(key));
    } else if (OB_FAIL(tablet_infos_.remove(i))) {
      LOG_WARN("failed to remove", KR(ret), K(key));
    }
    FLOG_INFO("session tablet removed by key", KR(ret), K(key), K(i), K(tablet_infos_));
  }
  return ret;
}

int ObSessionTabletInfoMap::update_session_tablet_sequence_without_lock(
  const uint64_t table_id,
  const ObTabletID &tablet_id,
  const uint64_t session_id,
  const int64_t new_sequence)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id
                  || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id), K(tablet_id));
  } else {
    bool found_one = false;
    for (int64_t i = 0; i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).table_id_ == table_id
          && tablet_infos_.at(i).tablet_id_ == tablet_id
          && tablet_infos_.at(i).session_id_ == session_id) {
        tablet_infos_.at(i).sequence_ = new_sequence;
        found_one = true;
        FLOG_INFO("session tablet sequence updated", K(table_id),
            K(new_sequence), K(session_id), K(tablet_infos_.at(i)));
      }
    }
    if (!found_one) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("session tablet not found for sequence update", KR(ret),
          K(table_id), K(session_id));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::batch_update_tablet_sequences(
    const uint64_t session_id,
    const common::ObIArray<common::ObTabletID> &updated_tablet_ids,
    const int64_t old_sequence,
    const int64_t new_sequence)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 16> matched_indexes;
  lib::ObMutexGuard guard(mutex_);
  // Phase 1: collect matched indexes and tablet_ids, no modification yet
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
    ObSessionTabletInfo &info = tablet_infos_.at(i);
    if (info.session_id_ == session_id
        && info.sequence_ == old_sequence
        && is_contain(updated_tablet_ids, info.tablet_id_)) {
      if (OB_FAIL(matched_indexes.push_back(i))) {
        LOG_WARN("failed to push back index", KR(ret), K(i), K(info));
      }
    }
  }
  // Phase 2: update sequences only after all push_back succeed
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < matched_indexes.count(); ++i) {
      ObSessionTabletInfo &info = tablet_infos_.at(matched_indexes.at(i));
      info.sequence_ = new_sequence;
      FLOG_INFO("[TRUNCATE TABLET] session tablet sequence updated",
          K(info), K(old_sequence), K(new_sequence));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::get_table_ids_by_session_id_and_sequence(
    const uint64_t session_id,
    const int64_t sequence,
    common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (!tablet_infos_.empty()) {
    lib::ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).session_id_ == session_id &&
          tablet_infos_.at(i).sequence_ == sequence) {
        if (OB_FAIL(table_ids.push_back(tablet_infos_.at(i).table_id_))) {
          LOG_WARN("failed to push back", KR(ret), K(tablet_infos_.at(i)));
        }
      }
    }
  }
  return ret;
}

// Single source of truth for the GTT truncate-tablet enablement gate.
// Both the storage reuse path and ObSQLSessionInfo route their version
// checks through this function. The gate combines a data-version check
// (>= 4.4.2.3) with the tenant parameter _enable_new_trx_gtt_truncate_tablet.
bool ObSessionTabletInfoMap::is_gtt_truncate_tablet_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  uint64_t data_version = 0;
  if (OB_SUCCESS != GET_MIN_DATA_VERSION(tenant_id, data_version)) {
  } else if (data_version >= DATA_VERSION_4_4_2_3) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      enabled = tenant_config->_enable_new_trx_gtt_truncate_tablet;
    }
  }
  return enabled;
}

void ObSessionTabletInfoMap::reset_reuse_outputs(
    common::ObIArray<common::ObTabletID> &reused_tablet_ids,
    share::ObLSID &resolved_ls_id)
{
  resolved_ls_id.reset();
  for (int64_t i = 0; i < reused_tablet_ids.count(); ++i) {
    reused_tablet_ids.at(i).reset();
  }
}

int ObSessionTabletInfoMap::classify_tables_for_reuse(
    const uint64_t tenant_id,
    common::ObMySQLTransaction &trans,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id,
    common::ObIArray<common::ObTabletID> &reused_tablet_ids,
    share::ObLSID &resolved_ls_id,
    common::ObIArray<ObSessionTabletInfo> &infos_to_truncate,
    common::ObIArray<common::ObTabletID> &tablets_to_bump_seq)
{
  int ret = OB_SUCCESS;
  // Step 1: Query inner table — single source of truth, no separate
  // in-memory probe. The inner table carries the authoritative sequence
  // for every (table_id, session_id), so the classification below is
  // driven exclusively by this one batched lookup.
  common::ObSEArray<ObSessionTabletInfo, 4> inner_rows;
  if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_point_get_by_table_ids_and_session_id(
          trans, tenant_id, table_ids, session_id, inner_rows))) {
    LOG_WARN("failed to query inner table", KR(ret), K(tenant_id), K(table_ids), K(session_id));
  } else {
    // Step 2: Sync in-memory map with the authoritative inner-table rows.
    // When the map disagrees (stale sequence from a failed prior commit, or
    // entry missing because it was populated on another node), force-refresh
    // the local entry so subsequent lookups stay consistent.
    {
      lib::ObMutexGuard guard(mutex_);
      for (int64_t k = 0; OB_SUCC(ret) && k < inner_rows.count(); ++k) {
        const ObSessionTabletInfo &row = inner_rows.at(k);
        bool found = false;
        for (int64_t i = 0; i < tablet_infos_.count(); ++i) {
          if (tablet_infos_.at(i).table_id_ == row.get_table_id()
              && tablet_infos_.at(i).session_id_ == row.get_session_id()) {
            ObSessionTabletInfo &cached_info = tablet_infos_.at(i);
            if (cached_info.sequence_ != row.get_sequence()
                || cached_info.ls_id_ != row.get_ls_id()
                || cached_info.tablet_id_ != row.get_tablet_id()
                || cached_info.transfer_seq_ != row.get_transfer_seq()) {
              const bool is_creator = cached_info.is_creator_;
              cached_info = row;
              cached_info.is_creator_ = is_creator;
            }
            found = true;
            break;
          }
        }
        if (!found && OB_FAIL(tablet_infos_.push_back(row))) {
          LOG_WARN("failed to sync map entry from inner table", KR(ret), K(row));
        }
      }
    }
    // Step 3: Classify each table_id against the unified inner-table view.
    // For every input table_id, exactly one outcome:
    //   - No inner-table row          -> needs creation (leave invalid)
    //   - Row exists, seq == current  -> reuse, no work
    //   - Row exists, seq == INACTIVE -> reuse + bump sequence
    //   - Row exists, seq == other    -> reuse + truncate + bump sequence
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      for (int64_t k = 0; OB_SUCC(ret) && k < inner_rows.count(); ++k) {
        if (inner_rows.at(k).get_table_id() == table_ids.at(i)) {
          const ObSessionTabletInfo &row = inner_rows.at(k);
          if (resolved_ls_id.is_valid() && resolved_ls_id != row.get_ls_id()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("reused tablet on different LS within one session",
                KR(ret), K(table_ids.at(i)), K(resolved_ls_id), K(row));
          } else {
            reused_tablet_ids.at(i) = row.get_tablet_id();
            resolved_ls_id = row.get_ls_id();
            if (row.get_sequence() == sequence) { // Already at current transaction sequence: reuse without work.
            } else if (OB_GTT_V2_TRX_TABLET_INACTIVE_SEQUENCE != row.get_sequence() && OB_FAIL(infos_to_truncate.push_back(row))) {
              LOG_WARN("failed to push back info to truncate", KR(ret), K(row));
            } else if (OB_FAIL(tablets_to_bump_seq.push_back(row.get_tablet_id()))) {
              LOG_WARN("failed to push back tablet id to bump", KR(ret), K(row));
            }
          }
          break;
        }
      }
    }
    DEBUG_SYNC(AFTER_CLASSIFY_REUSE_SESSION_TABLETS);
  }
  return ret;
}

int ObSessionTabletInfoMap::run_reuse_path(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id,
    common::ObIArray<common::ObTabletID> &reused_tablet_ids,
    share::ObLSID &resolved_ls_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  common::ObSEArray<ObSessionTabletInfo, 4> infos_to_truncate;
  common::ObSEArray<common::ObTabletID, 4> tablets_to_bump_seq;
  int64_t schema_version = OB_INVALID_VERSION;
  /// NOTE: Should TRUNCATE and CREATE be performed in the same trx?
  if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to begin reuse transaction", KR(ret), K(tenant_id));
  } else if (OB_FAIL(classify_tables_for_reuse(tenant_id, trans, table_ids, sequence,
                                               session_id, reused_tablet_ids,
                                               resolved_ls_id, infos_to_truncate,
                                               tablets_to_bump_seq))) {
    LOG_WARN("failed to classify tables for reuse", KR(ret), K(tenant_id), K(table_ids), K(sequence), K(session_id));
  } else {
    if (!infos_to_truncate.empty()) {
      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.schema_service_->gen_new_schema_version(tenant_id, schema_version))) {
        LOG_WARN("failed to generate schema version for truncate", KR(ret), K(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < infos_to_truncate.count(); ++i) {
        const ObSessionTabletInfo &info = infos_to_truncate.at(i);
        ObSessionTabletTruncateHelper truncate_helper(tenant_id, info, trans, schema_version);
        if (OB_FAIL(truncate_helper.do_work())) {
          LOG_WARN("[TRUNCATE TABLET] failed to compensate truncate", KR(ret), K(info));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!tablets_to_bump_seq.empty() && OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_update_sequence(
                       trans, tenant_id, tablets_to_bump_seq, sequence))) {
      LOG_WARN("failed to bump sequence", KR(ret), K(tablets_to_bump_seq));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    const bool is_commit = (OB_SUCCESS == ret);
    if (OB_TMP_FAIL(trans.end(is_commit))) {
      LOG_WARN("failed to end reuse transaction", KR(ret), KR(tmp_ret));
      ret = is_commit ? tmp_ret : ret;
    }
  }
  if (OB_FAIL(ret)) {
    reset_reuse_outputs(reused_tablet_ids, resolved_ls_id);
  }
  return ret;
}

// Single-probe classifier for the truncate-tablet GTT path.
// Queries the inner table as the authoritative source, syncs the
// in-memory map to match, then classifies each table_id:
//   - reuse, no inner-table writes  (row exists, sequence == current)
//   - reuse + bump sequence         (row exists, sequence == INACTIVE)
//   - reuse + truncate + bump       (row exists, sequence is a stale
//                                    active value from a prior commit)
//   - needs creation                (no inner-table row; caller leaves
//                                    the slot invalid)
// Multi-LS within one session is treated as an invariant violation and
// aborts the entire reuse path with OB_ERR_UNEXPECTED.
//
// When the gate is enabled, the reuse path runs in a transaction owned
// and committed inside run_reuse_path. It is intentionally separate from
// the create transaction (driven later by
// ObSessionTabletCreateHelper::do_work): truncate redo and sequence
// bumps for reused entries are durable independently of whether the
// create flow for the to-create subset succeeds.
//
// Output is sized to table_ids.count():
//   reused_tablet_ids[i]: valid tablet id when reused; reset() (invalid)
//                         when the entry needs creation by the caller.
//   resolved_ls_id:       common LS shared by all reused tablets; invalid
//                         if no entry was reused.
int ObSessionTabletInfoMap::try_reuse_truncated_tablets(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id,
    common::ObIArray<common::ObTabletID> &reused_tablet_ids,
    share::ObLSID &resolved_ls_id)
{
  int ret = OB_SUCCESS;
  resolved_ls_id.reset();
  if (OB_UNLIKELY(table_ids.empty()) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_ids));
  } else if (OB_FAIL(reused_tablet_ids.prepare_allocate(table_ids.count()))) {
    LOG_WARN("failed to pre-size reused_tablet_ids", KR(ret), K(table_ids.count()));
  } else {
    for (int64_t i = 0; i < table_ids.count(); ++i) {
      reused_tablet_ids.at(i).reset();
    }
    if (is_gtt_truncate_tablet_enabled(tenant_id)) {
      if (OB_FAIL(run_reuse_path(tenant_id, table_ids, sequence, session_id, reused_tablet_ids, resolved_ls_id))) {
        LOG_WARN("failed to run reuse path", KR(ret), K(tenant_id), K(table_ids));
      }
    }
  }
  return ret;
}

bool ObSessionTabletInfoMap::has_inactive_trx_session_tablet()
{
  bool b_ret = false;
  lib::ObMutexGuard guard(mutex_);
  if (!is_empty()) {
    for (int64_t i = 0; !b_ret && i < tablet_infos_.count(); ++i) {
      const ObSessionTabletInfo &info = tablet_infos_.at(i);
      if (info.get_sequence() == storage::OB_GTT_V2_TRX_TABLET_INACTIVE_SEQUENCE) {
        b_ret = true;
      }
    }
  }
  return b_ret;
}
} // namespace storage
} // namespace oceanbase
