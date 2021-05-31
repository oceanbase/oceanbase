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

#define USING_LOG_PREFIX SQL

#include "ob_sub_trans_ctrl.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/ob_sql_trans_control.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObSubTransCtrl::start_participants(ObExecContext& ctx, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_participants(sqc, participants_))) {
    LOG_WARN("fail to get participants", K(ret));
  } else if (participants_.count() <= 0) {
    // skip
    LOG_TRACE("sub trans start participant, the number of participants is zero", K(participants_.count()));
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = ObSqlTransControl::start_participant(ctx, participants_, true)))) {
    LOG_WARN("fail to start participant", K(ret));
  }
  trans_state_.set_start_participant_executed(OB_SUCCESS == ret);
  LOG_TRACE("Transaction start:", K_(participants));
  return ret;
}

int ObSubTransCtrl::end_participants(ObExecContext& ctx, bool is_rollback)
{
  int ret = OB_SUCCESS;
  if (trans_state_.is_start_participant_executed()) {
    if (participants_.count() <= 0) {
      // skip
    } else if (OB_FAIL(ObSqlTransControl::end_participant(ctx, is_rollback, participants_))) {
      LOG_WARN("fail to end participant", K(ret));
    }
    LOG_TRACE("Transaction end:", K_(participants));
    trans_state_.clear_start_participant_executed();
  }
  return ret;
}

int ObSubTransCtrl::get_participants(ObPxSqcMeta& sqc, common::ObPartitionArray& participants) const
{
  int ret = OB_SUCCESS;
  // partitions keys
  ObPartitionReplicaLocationIArray* locations;
  locations = &sqc.get_access_table_locations();
  if (OB_SUCC(ret) && OB_NOT_NULL(locations)) {
    ObPartitionKey pkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < locations->count(); ++i) {
      if (OB_FAIL(locations->at(i).get_partition_key(pkey))) {
        LOG_WARN("fail get pkey", K(i), K(ret));
      } else if (is_virtual_table(pkey.get_table_id())) {
        // skip
      } else if (OB_FAIL(add_var_to_array_no_dup(participants, pkey))) {
        LOG_WARN("add var to array no dup failed", K(ret));
      }
    }
  }
  return ret;
}
