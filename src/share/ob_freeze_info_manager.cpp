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

#define USING_LOG_PREFIX SHARE

#include "share/ob_freeze_info_manager.h"

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/utility.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_global_stat_proxy.h"
#include "observer/ob_server_struct.h"


namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
using namespace palf;

namespace share
{
/****************************** ObFreezeInfoList ******************************/
int ObFreezeInfoList::assign(const ObFreezeInfoList &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    frozen_statuses_.reset();
    if (OB_FAIL(frozen_statuses_.assign(other.frozen_statuses_))) {
      LOG_WARN("fail to assign", KR(ret), K(other));
    } else {
      latest_snapshot_gc_scn_ = other.latest_snapshot_gc_scn_;
    }
  }
  return ret;
}

int ObFreezeInfoList::get_latest_frozen_scn(SCN &frozen_scn) const
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfo freeze_info;
  if (OB_FAIL(get_latest_freeze_info(freeze_info))) {
    LOG_WARN("fail to get latest frozen status", KR(ret));
  } else {
    frozen_scn =  freeze_info.frozen_scn_;
  }
  return ret;
}

int ObFreezeInfoList::get_latest_freeze_info(
    share::ObFreezeInfo &freeze_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    freeze_info = frozen_statuses_.at(frozen_statuses_.count() - 1);
  }
  return ret;
}

int ObFreezeInfoList::get_min_freeze_info_greater_than(
    const SCN &frozen_scn,
    share::ObFreezeInfo &freeze_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    int64_t idx = -1;
    SCN max_cache_frozen_scn = frozen_statuses_.at(frozen_statuses_.count() - 1).frozen_scn_;
    for (int64_t i = 0; i < frozen_statuses_.count(); i++) {
      if (frozen_statuses_.at(i).frozen_scn_ > frozen_scn) {
        idx = i;
        break;
      }
    }

    if (idx >= 0) {
      freeze_info = frozen_statuses_.at(idx);
    } else { // not found in cache
      if (max_cache_frozen_scn == frozen_scn) {
        LOG_TRACE("no more larger frozen_scn", K(frozen_scn), K_(frozen_statuses));
      } else if (max_cache_frozen_scn < frozen_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max cached frozen_scn should not less than frozen_scn", KR(ret), K(frozen_scn),
          K(max_cache_frozen_scn), K_(frozen_statuses));
      }
    }
  }
  return ret;
}

int ObFreezeInfoList::get_freeze_info(
    const SCN &frozen_scn,
    share::ObFreezeInfo &freeze_info,
    int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = -1;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; i < frozen_statuses_.count(); i++) {
      if (frozen_statuses_.at(i).frozen_scn_ == frozen_scn) {
        idx = i;
        break;
      }
    }

    if (idx >= 0) {
      freeze_info = frozen_statuses_.at(idx);
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found freeze_info", KR(ret), KPC(this), K(frozen_scn));
    }
  }
  return ret;
}


/****************************** ObFreezeInfoManager ******************************/
int ObFreezeInfoManager::init(
    uint64_t tenant_id,
    common::ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &proxy;
    is_inited_ = true;
  }
  return ret;
}

// reload will acquire latest freeze info from __all_freeze_info.
int ObFreezeInfoManager::reload(const share::SCN &min_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFreezeInfo, 8> freeze_infos;
  share::SCN latest_snapshot_gc_scn;

  if (OB_FAIL(fetch_new_freeze_info(tenant_id_, min_frozen_scn, *sql_proxy_, freeze_infos, latest_snapshot_gc_scn))) {
    LOG_WARN("failed to load updated info", K(ret));
  } else if (OB_FAIL(update_freeze_info(freeze_infos, latest_snapshot_gc_scn))) {
    LOG_WARN("failed to update freeze info", K(ret));
  }

  if (OB_FAIL(ret)) {
    reset_freeze_info();
  }
  return ret;
}

int ObFreezeInfoManager::fetch_new_freeze_info(
    const int64_t tenant_id,
    const share::SCN &min_frozen_scn,
    common::ObMySQLProxy &sql_proxy,
    common::ObIArray<ObFreezeInfo> &freeze_infos,
    share::SCN &latest_snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id);

  // 1. get snapshot_gc_scn
  if (OB_FAIL(ObGlobalStatProxy::get_snapshot_gc_scn(
             sql_proxy, tenant_id, latest_snapshot_gc_scn))) {
    LOG_WARN("fail to select for update snapshot_gc_scn", KR(ret), K(tenant_id));
  // 2. acquire freeze info in same trans, ensure we can get the latest freeze info
  } else if (OB_FAIL(freeze_info_proxy.get_freeze_info_larger_or_equal_than(
             sql_proxy, min_frozen_scn, freeze_infos))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(min_frozen_scn));
  } else if (OB_UNLIKELY(freeze_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid frozen status", KR(ret), K(min_frozen_scn));
  }
  return ret;
}

int ObFreezeInfoManager::update_freeze_info(
    const common::ObIArray<ObFreezeInfo> &freeze_infos,
    const share::SCN &latest_snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  const int64_t freeze_info_cnt = freeze_infos.count();

  if (OB_UNLIKELY(freeze_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(freeze_infos), K(latest_snapshot_gc_scn));
  } else if (OB_FAIL(freeze_info_.frozen_statuses_.prepare_allocate(freeze_info_cnt))) {
    LOG_WARN("failed to prepare allocate mem for new freeze info", KR(ret), K(freeze_infos), K(freeze_info_));
  } else if (OB_FAIL(freeze_info_.frozen_statuses_.assign(freeze_infos))) {
    LOG_WARN("fail to assign", KR(ret), K(freeze_infos));
  } else if (freeze_info_.frozen_statuses_.count() > 1) {
    lib::ob_sort(freeze_info_.frozen_statuses_.begin(), freeze_info_.frozen_statuses_.end(),
              [](const ObFreezeInfo &a, const ObFreezeInfo &b)
                { return a.frozen_scn_ < b.frozen_scn_; } );
  }

  if (OB_SUCC(ret)) {
    freeze_info_.latest_snapshot_gc_scn_ = latest_snapshot_gc_scn;
    LOG_INFO("inner load succ", "latest_freeze_info", freeze_info_.frozen_statuses_.at(freeze_info_cnt - 1), K(freeze_info_));
  }
  return ret;
}

int ObFreezeInfoManager::add_freeze_info(const share::ObFreezeInfo &frozen_status)
{
  int ret = OB_SUCCESS;
  const int64_t freeze_info_cnt = freeze_info_.frozen_statuses_.count();

  if (0 == freeze_info_cnt) {
  } else if (OB_UNLIKELY(freeze_info_.frozen_statuses_.at(freeze_info_cnt - 1).frozen_scn_ >= frozen_status.frozen_scn_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(freeze_info_), K(frozen_status));
  }

  if (FAILEDx(freeze_info_.frozen_statuses_.push_back(frozen_status))) {
    LOG_WARN("fail to push back", KR(ret), K(frozen_status));
  }
  return ret;
}

int ObFreezeInfoManager::update_snapshot_gc_scn(const share::SCN &new_snapshot_gc_scn)
{
  int ret = OB_SUCCESS;

  if (!freeze_info_.is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    freeze_info_.latest_snapshot_gc_scn_ = new_snapshot_gc_scn;
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_info(
    const SCN &frozen_scn,
    share::ObFreezeInfo &frozen_status)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (SCN::base_scn() == frozen_scn) {
    frozen_status.frozen_scn_ = SCN::base_scn();
  } else if (OB_FAIL(freeze_info_.get_freeze_info(frozen_scn, frozen_status, idx/*placeholder*/))) {
    LOG_WARN("fail to get frozen status", KR(ret), K(frozen_scn), K_(freeze_info));
  }
  return ret;
}

share::SCN ObFreezeInfoManager::get_latest_frozen_scn()
{
  share::SCN latest_frozen_scn = share::SCN::min_scn();
  if (freeze_info_.empty()) {
    // do nothing
  } else {
    latest_frozen_scn = freeze_info_.frozen_statuses_.at(freeze_info_.count() - 1).frozen_scn_;
  }
  return latest_frozen_scn;
}

int ObFreezeInfoManager::get_latest_freeze_info(share::ObFreezeInfo &frozen_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("freeze info mgr not inited", KR(ret));
  } else if (OB_FAIL(freeze_info_.get_latest_freeze_info(frozen_status))) {
    LOG_WARN("fail to get latest frozen status", KR(ret));
  }
  return ret;
}


int ObFreezeInfoManager::get_freeze_info_by_idx(
    const int64_t idx,
    share::ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("freeze info mgr not inited", KR(ret));
  } else if (freeze_info_.empty() || idx >= freeze_info_.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no freeze info in curr info_list", K(ret), K(freeze_info_));
  } else {
    freeze_info = freeze_info_.frozen_statuses_.at(idx);
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_info_by_major_snapshot(
    const int64_t snapshot_version,
    ObIArray<share::ObFreezeInfo> &info_list,
    const bool need_all_behind_info)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  share::SCN frozen_scn;
  share::ObFreezeInfo freeze_info; //placeholder
  int64_t ret_pos = -1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("freeze info mgr not inited", KR(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(frozen_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert snapshot version to scn", K(ret), K(snapshot_version));
  } else if (OB_FAIL(freeze_info_.get_freeze_info(frozen_scn, freeze_info, ret_pos))) {
    LOG_WARN("failed to get frozen status", K(ret), K(frozen_scn));
  } else if (ret_pos < 0 || ret_pos >= freeze_info_.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("can not find the freeze info", K(ret), K(snapshot_version), K(freeze_info_));
  } else {
    const int64_t end_idx = need_all_behind_info
                          ? freeze_info_.frozen_statuses_.count()
                          : ret_pos + 1;

    for (int64_t i = ret_pos; OB_SUCC(ret) && i < end_idx; ++i) {
      if (OB_FAIL(info_list.push_back(freeze_info_.frozen_statuses_.at(i)))) {
        LOG_WARN("failed to push back frozen status", K(ret));
      }
    }
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_info_behind_major_snapshot(
    const int64_t snapshot_version,
    share::ObFreezeInfo &frozen_status)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("freeze info mgr not inited", KR(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < freeze_info_.frozen_statuses_.count(); ++i) {
      if (snapshot_version < freeze_info_.frozen_statuses_.at(i).frozen_scn_.get_val_for_tx()) {
        frozen_status = freeze_info_.frozen_statuses_.at(i);
        found = true;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_DEBUG("get freeze info", K(ret), K(found), K(snapshot_version), K(frozen_status));
    }
  }
  return ret;
}

int ObFreezeInfoManager::get_neighbour_frozen_status(
    const int64_t snapshot_version,
    share::ObFreezeInfo &prev_frozen_status,
    share::ObFreezeInfo &next_frozen_status)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::ObFreezeInfo> &info_list = freeze_info_.frozen_statuses_;
  bool found = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("freeze info mgr not inited", KR(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no frozen status in curr info list", K(ret), K(snapshot_version));
  } else if (snapshot_version >= info_list.at(info_list.count() - 1).frozen_scn_.get_val_for_tx()) {
    // use found = false setting
  } else {
    for (int64_t i = 0; i < info_list.count() && OB_SUCC(ret) && !found; ++i) {
      const share::ObFreezeInfo &next_info = info_list.at(i);
      if (snapshot_version < next_info.frozen_scn_.get_val_for_tx()) {
        found = true;
        if (0 == i) {
          ret = OB_ENTRY_NOT_EXIST;
          if (REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
            LOG_WARN("cannot get neighbour major freeze before bootstrap", K(ret),
              K(snapshot_version), K(next_info));
          }
        } else {
          next_frozen_status = next_info;
          prev_frozen_status = info_list.at(i- 1);
        }
      }
    }
  }

  if (OB_SUCC(ret) && !found) {
    next_frozen_status.frozen_scn_.set_max();
    prev_frozen_status = info_list.at(info_list.count() - 1);
  }
  return ret;
}

int ObFreezeInfoManager::get_min_freeze_info_greater_than(
    const share::SCN &frozen_scn,
    share::ObFreezeInfo &frozen_status)
{
  return freeze_info_.get_min_freeze_info_greater_than(frozen_scn, frozen_status);
}


} // share
} // oceanbase
