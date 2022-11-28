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
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/backup/ob_tenant_archive_round.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/ob_server_struct.h"                   // GCTX

using namespace oceanbase;
using namespace share;

// round op
int ObTenantArchiveMgr::get_tenant_current_round(const int64_t tenant_id, const int64_t incarnation, ObTenantArchiveRoundAttr &round_attr)
{
  // Only one dest is supported now.
  const int64_t fake_dest_no = 0;
  return get_dest_round_by_dest_no(tenant_id, fake_dest_no, round_attr);
}

int ObTenantArchiveMgr::get_dest_round_by_dest_no(const uint64_t tenant_id, const int64_t dest_no, ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper table_op;
  common::ObMySQLProxy *proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (OB_FAIL(table_op.init(tenant_id))) {
    LOG_WARN("failed to init table op", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_round(*proxy, dest_no, false /* need_lock */, round))) {
    LOG_WARN("failed to get dest round", K(ret), K(tenant_id), K(dest_no));
  }

  return ret;
}

int ObTenantArchiveMgr::is_archive_running(
    common::ObISQLClient &proxy, 
    const uint64_t tenant_id, 
    const int64_t dest_no, 
    bool &is_running)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr round;
  ObArchivePersistHelper table_op;
  if (OB_FAIL(table_op.init(tenant_id))) {
    LOG_WARN("failed to init table op", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_round(proxy, dest_no, false /* need_lock */, round))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_running = false;
    } else {
      LOG_WARN("failed to get dest round", K(ret), K(tenant_id), K(dest_no));
    }
  } else {
    is_running = !round.state_.is_stop();
  }

  return ret;
}

// piece op
int ObTenantArchiveMgr::decide_piece_id(
    const SCN &piece_start_scn,
    const int64_t start_piece_id, 
    const int64_t piece_switch_interval, 
    const SCN &scn,
    int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  if (scn < piece_start_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scn", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(scn));
  } else if (0 >= piece_switch_interval) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece_switch_interval", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(scn));
  } else if (0 >= start_piece_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_piece_id", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(scn));
  } else {
    piece_id = (scn.convert_to_ts() - piece_start_scn.convert_to_ts()) / piece_switch_interval + start_piece_id;
  }

  return ret;
}

int ObTenantArchiveMgr::decide_piece_start_scn(
    const SCN &piece_start_scn,
    const int64_t start_piece_id, 
    const int64_t piece_switch_interval, 
    const int64_t piece_id, 
    SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (piece_id < start_piece_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece id", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(piece_id));
  } else if (SCN::min_scn() >= piece_start_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece_start_scn", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(piece_id));
  } else if (0 >= piece_switch_interval) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece_switch_interval", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(piece_id));
  } else if (OB_FAIL(start_scn.convert_from_ts(piece_start_scn.convert_to_ts() + (piece_id - start_piece_id) * piece_switch_interval))) {
    LOG_WARN("fail to set start scn", K(ret), K(piece_start_scn), K(piece_id), K(start_piece_id), K(piece_switch_interval));
  }

  return ret;
}

int ObTenantArchiveMgr::decide_piece_end_scn(
    const SCN &piece_start_scn,
    const int64_t start_piece_id, 
    const int64_t piece_switch_interval, 
    const int64_t piece_id, 
    SCN &end_scn)
{
  int ret = OB_SUCCESS;
  // piece end scn is the start of next piece.
  if (OB_FAIL(decide_piece_start_scn(piece_start_scn, start_piece_id, piece_switch_interval, piece_id + 1, end_scn))) {
    LOG_WARN("failed to decide piece end scn", K(ret), K(piece_start_scn), K(start_piece_id), K(piece_switch_interval), K(piece_id));
  }

  return ret;
}

int ObTenantArchiveMgr::decide_first_piece_start_scn(
  const SCN &start_scn,
  const int64_t piece_switch_interval,
  SCN &piece_start_scn)
{
  int ret = OB_SUCCESS;

  const int64_t ONE_DAY = 24 * 3600 * 1000000L; // us
  const int64_t ONE_HOUR = 3600 * 1000000L; // us
  const int64_t ONE_MINUTE = 60 * 1000000L; // us
  const int64_t TEN_SECONDS = 10 * 60 * 1000000L; // us
  // If 'piece_switch_interval' is equal or bigger than one day, then it must be an integer multiple of day.
  if (ONE_DAY <= piece_switch_interval) {
    if (0 != piece_switch_interval % ONE_DAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("piece switch interval must be an integer multiple of day", K(ret), K(start_scn), K(piece_switch_interval));
    } else if (OB_FAIL(piece_start_scn.convert_from_ts((start_scn.convert_to_ts() / ONE_DAY) * ONE_DAY))) {
      LOG_WARN("fail to set piece start scn", K(ret));
    }
  }

  // If 'piece_switch_interval' is equal or bigger than one hour, then it must be an integer multiple of hour.
  else if (ONE_HOUR <= piece_switch_interval) {
    if (0 != piece_switch_interval % ONE_HOUR) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("piece switch interval must be an integer multiple of hour", K(ret), K(start_scn), K(piece_switch_interval));
    } else if (OB_FAIL(piece_start_scn.convert_from_ts((start_scn.convert_to_ts() / ONE_HOUR) * ONE_HOUR))) {
      LOG_WARN("fail to set piece start scn", K(ret));
    }
  } 

  // If 'piece_switch_interval' is equal or bigger than one minute, then it must be an integer multiple of minute.
  else if (ONE_MINUTE <= piece_switch_interval) {
    if (0 != piece_switch_interval % ONE_MINUTE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("piece switch interval must be an integer multiple of minute", K(ret), K(start_scn), K(piece_switch_interval));
    } else if (OB_FAIL(piece_start_scn.convert_from_ts((start_scn.convert_to_ts() / ONE_MINUTE) * ONE_MINUTE))) {
      LOG_WARN("fail to set piece start scn", K(ret));
    }
  }

  // If 'piece_switch_interval' is equal or bigger than 10 seconds, then it must be an integer multiple of 10 seconds.
  else if (TEN_SECONDS <= piece_switch_interval) {
    if (0 != piece_switch_interval % TEN_SECONDS) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("piece switch interval must be an integer multiple of 10 seconds", K(ret), K(start_scn), K(piece_switch_interval));
    } else if (OB_FAIL(piece_start_scn.convert_from_ts((start_scn.convert_to_ts() / TEN_SECONDS) * TEN_SECONDS))) {
      LOG_WARN("fail to set piece start scn", K(ret));
    }
  }
  
  else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("piece switch interval is not valid", K(ret), K(start_scn), K(piece_switch_interval));
  }

  return ret;
}

int ObTenantArchiveMgr::timestamp_to_day(const int64_t ts, int64_t &day)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupUtils::convert_timestamp_to_date(ts, day))) {
    LOG_WARN("failed to get day from timestamp", K(ret), K(ts));
  }

  return ret;
}
