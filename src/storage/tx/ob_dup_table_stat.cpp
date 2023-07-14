//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "storage/tx/ob_dup_table_stat.h"
#include "storage/tx/ob_dup_table_tablets.h"

namespace oceanbase
{
namespace transaction
{
constexpr const char DupTableModID::OB_VIRTUAL_DUP_LS_LEASE_MGR[];
constexpr const char DupTableModID::OB_VIRTUAL_DUP_LS_TABLETS[];
constexpr const char DupTableModID::OB_VIRTUAL_DUP_LS_TABLET_SET[];

void ObDupTableLSBaseStat::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  // addr_.reset();
}

void ObDupTableLSLeaseMgrStat::reset()
{
  ObDupTableLSBaseStat::reset();
  follower_addr_.reset();
  grant_ts_ = 0;
  expired_ts_ = 0;
  remain_us_ = 0;
  grant_req_ts_ = 0;
  cached_req_ts_ = 0;
  lease_interval_ = 0;
  max_read_version_ = 0;
  max_commit_version_ = 0;
  max_replayed_scn_.set_invalid();
}

void ObDupTableLSTabletSetStat::reset()
{
  ObDupTableLSBaseStat::reset();
  is_master_ = false;
  unique_id_ = INT64_MAX;
  count_ = 0;
  attr_ = TabletSetAttr::INVALID;
  readable_scn_.set_invalid();
  change_scn_.set_invalid();
  need_confirm_scn_.set_invalid();
  state_ = TabletSetState::INVALID;
  trx_ref_ = 0;
}

void ObDupTableLSTabletsStat::reset()
{
  ObDupTableLSBaseStat::reset();
  is_master_ = false;
  unique_id_ = UINT64_MAX;
  attr_ = TabletSetAttr::INVALID;
  refresh_schema_ts_ = 0;
  // need_gc_ = false;
}

void ObDupTableLSTabletSetStat::set_from_change_status(
     struct DupTabletSetChangeStatus *tmp_status)
{
  if (OB_NOT_NULL(tmp_status)) {
    // remap state
    switch(tmp_status->flag_)
    {
      case DupTabletSetChangeFlag::TEMPORARY:
        set_state(TabletSetState::TMP);
        break;
      case DupTabletSetChangeFlag::CHANGE_LOGGING:
        set_state(TabletSetState::LOGGING);
        break;
      case DupTabletSetChangeFlag::CONFIRMING:
        set_state(TabletSetState::CONFIRMING);
        break;
      case DupTabletSetChangeFlag::CONFIRMED:
        set_state(TabletSetState::CONFIRMED);
        break;
      default:
        set_state(TabletSetState::INVALID);
    }

    set_trx_ref(tmp_status->trx_ref_);
    set_change_scn(tmp_status->tablet_change_scn_);
    set_readable_scn(tmp_status->readable_version_);
    set_need_confirm_scn(tmp_status->need_confirm_scn_);
  }
}

const ObString &get_dup_ls_state_str(const bool is_master)
{
  static const ObString LSStateName[] =
  {
    ObString("LEADER"),
    ObString("FOLLOWER")
  };
  const int state = is_master ? 0 : 1;

  return LSStateName[state];
}

const ObString &get_dup_tablet_set_attr_str(const TabletSetAttr attr)
{
  static const ObString LSTabletSetAttrName[] =
  {
    ObString("INVALID"),
    ObString("DATA_SYNCING"),
    ObString("READABLE"),
    ObString("DELETIGN"),
    ObString("UNKONW") //  invliad argument, return unknow string
  };

  int8_t attr_idx = 0;
  if (attr > TabletSetAttr::MAX || attr <= TabletSetAttr::INVALID) {
    DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED,  "unexpect attr", K(attr));
    attr_idx = static_cast<int8_t>(TabletSetAttr::MAX); // return unkonw
  } else {
    attr_idx = static_cast<int8_t>(attr);
  }

  return LSTabletSetAttrName[attr_idx];
}

const ObString &get_dup_tablet_set_state_str(const TabletSetState state)
{
  static const ObString LSTabletSetStateName[] =
  {
    ObString("INVALID"),
    ObString("TMP"),
    ObString("LOGGING"),
    ObString("CONFIRMING"),
    ObString("CONFIRMED"),
    ObString("UNKONW") //  invliad argument, return unknow string
  };

  int8_t state_idx = 0;
  if (state > TabletSetState::MAX || state <= TabletSetState::INVALID) {
    DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED,  "unexpect state", K(state));
    state_idx = static_cast<int8_t>(TabletSetState::MAX); // return unknow
  } else {
    state_idx = static_cast<int8_t>(state);
  }

  return LSTabletSetStateName[state_idx];
}

} // namespace transaction
} // namespace oceanbase
