/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/tablet/ob_drop_gtt_v2_session_tablet_arg.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/tablet/ob_session_tablet_info_map.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObDropGTTV2SessionTabletArg,
                    tenant_id_,
                    table_ids_,
                    sequence_,
                    session_id_);

OB_SERIALIZE_MEMBER(ObDropGTTV2SessionTabletRes,
                    executed_on_creator_,
                    local_map_hit_,
                    ret_);

int ObDropGTTV2SessionTabletArg::init(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  sequence_ = sequence;
  session_id_ = session_id;
  table_ids_.reset();
  if (OB_FAIL(table_ids_.assign(table_ids))) {
    LOG_WARN("failed to assign table ids", KR(ret), K(table_ids));
  }
  return ret;
}

bool ObDropGTTV2SessionTabletArg::is_matched(const storage::ObSessionTabletInfo &info) const
{
  return info.get_sequence() == sequence_
         && info.get_session_id() == session_id_
         && has_exist_in_array(table_ids_, info.get_table_id());
}

OB_SERIALIZE_MEMBER(ObBatchDropGTTV2SessionTabletArg,
                    tenant_id_,
                    exclude_active_session_trx_tablet_,
                    tablet_infos_);

int ObBatchDropGTTV2SessionTabletArg::init(
    const uint64_t tenant_id,
    const bool exclude_active_trx_session_tablet,
    const common::ObIArray<storage::ObSessionTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  exclude_active_session_trx_tablet_ = exclude_active_trx_session_tablet;
  if (OB_FAIL(tablet_infos_.assign(tablet_infos))) {
    LOG_WARN("failed to assign tablet infos", K(ret), K(tablet_infos));
  }
  return ret;
}

bool ObBatchDropGTTV2SessionTabletArg::is_valid() const
{
  bool b_ret = true;
  if (common::OB_INVALID_TENANT_ID == tenant_id_
      || tablet_infos_.empty()) {
    b_ret = false;
  }
  return b_ret;
}

bool ObBatchDropGTTV2SessionTabletArg::is_matched(const storage::ObSessionTabletInfo &info) const
{
  bool found = false;
  for (int64_t i = 0; !found && i < tablet_infos_.count(); ++i) {
    const ObSessionTabletInfo &info_in_arr = tablet_infos_.at(i);
    if (info.get_table_id() == info_in_arr.get_table_id()
        && info.get_tablet_id() == info_in_arr.get_tablet_id()
        && info.get_sequence() == info_in_arr.get_sequence()
        && info.get_session_id() == info_in_arr.get_session_id()) {
      found = true;
    }
  }
  return found;
}

} // namespace share
} // namespace oceanbase
