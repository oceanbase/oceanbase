/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/tablet/ob_drop_gtt_v2_session_tablet_arg.h"

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

} // namespace share
} // namespace oceanbase
