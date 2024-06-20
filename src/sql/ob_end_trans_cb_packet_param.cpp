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

#include "sql/ob_end_trans_cb_packet_param.h"
#include "lib/alloc/alloc_assist.h"
#include "sql/ob_result_set.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObEndTransCbPacketParam &ObEndTransCbPacketParam::operator=(const ObEndTransCbPacketParam& other)
{
  MEMCPY(message_, other.message_, MSG_SIZE);
  affected_rows_ = other.affected_rows_;
  last_insert_id_to_client_ = other.last_insert_id_to_client_;
  is_partition_hit_ = other.is_partition_hit_;
  trace_id_.set(other.trace_id_);
  is_valid_ = other.is_valid_;
  return *this;
}

const ObEndTransCbPacketParam &ObEndTransCbPacketParam::fill(ObResultSet &rs,
                                                             ObSQLSessionInfo &session,
                                                             const ObCurTraceId::TraceId &trace_id)
{
  MEMCPY(message_, rs.get_message(), MSG_SIZE); // TODO: optimize out
  // oracle ANONYMOUS_BLOCK affect rows always return 1
  affected_rows_ = stmt::T_ANONYMOUS_BLOCK == rs.get_stmt_type() 
                    ? 1 : rs.get_affected_rows();
  // The commit asynchronous callback logic needs
  // to trigger the update logic of affected row first.
  if (session.is_session_sync_support()) {
    session.set_affected_rows_is_changed(affected_rows_);
  }
  session.set_affected_rows(affected_rows_);
  last_insert_id_to_client_ = rs.get_last_insert_id_to_client();
  is_partition_hit_ = session.partition_hit().get_bool();
  trace_id_.set(trace_id);
  is_valid_ = true;
  return *this;
}


}/* ns sql*/
}/* ns oceanbase */


