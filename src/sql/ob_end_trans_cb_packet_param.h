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

#ifndef __OB_SQL_END_TRANS_CB_PACKET_PARAM_H__
#define __OB_SQL_END_TRANS_CB_PACKET_PARAM_H__

#include "share/ob_define.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase {
namespace sql {
class ObResultSet;
class ObSQLSessionInfo;
class ObEndTransCbPacketParam {
public:
  ObEndTransCbPacketParam()
      : affected_rows_(0), last_insert_id_to_client_(0), is_partition_hit_(true), trace_id_(), is_valid_(false)
  {
    message_[0] = '\0';
  }
  virtual ~ObEndTransCbPacketParam()
  {}

  ObEndTransCbPacketParam& operator=(const ObEndTransCbPacketParam& other);

  void reset()
  {
    message_[0] = '\0';
    affected_rows_ = 0;
    last_insert_id_to_client_ = 0;
    is_partition_hit_ = true;
    trace_id_.reset();
    is_valid_ = false;
  }

  const ObEndTransCbPacketParam& fill(
      ObResultSet& rs, ObSQLSessionInfo& session, const common::ObCurTraceId::TraceId& trace_id);

  bool is_valid() const
  {
    return is_valid_;
  }

  const char* get_message() const
  {
    return message_;
  }
  int64_t get_affected_rows() const
  {
    return affected_rows_;
  }
  uint64_t get_last_insert_id_to_client() const
  {
    return last_insert_id_to_client_;
  }
  bool get_is_partition_hit() const
  {
    return is_partition_hit_;
  }
  const common::ObCurTraceId::TraceId& get_trace_id() const
  {
    return trace_id_;
  }

  TO_STRING_KV(
      K_(message), K_(affected_rows), K_(last_insert_id_to_client), K_(is_partition_hit), K_(trace_id), K_(is_valid));

private:
  char message_[common::MSG_SIZE];  // null terminated message string
  int64_t affected_rows_;           // number of rows affected by INSERT/UPDATE/DELETE
  uint64_t last_insert_id_to_client_;
  bool is_partition_hit_;
  common::ObCurTraceId::TraceId trace_id_;
  bool is_valid_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_END_TRANS_CB_PACKET_PARAM_H__ */
//// end of header file
