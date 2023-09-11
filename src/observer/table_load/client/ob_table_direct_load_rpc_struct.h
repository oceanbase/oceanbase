/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/container/ob_array_serialization.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{

struct ObTableDirectLoadBeginArg
{
  OB_UNIS_VERSION(2);
public:
  ObTableDirectLoadBeginArg()
    : parallel_(0),
      max_error_row_count_(0),
      dup_action_(sql::ObLoadDupActionType::LOAD_INVALID_MODE),
      timeout_(0),
      heartbeat_timeout_(0),
      force_create_(false)
  {
  }
  TO_STRING_KV(K_(table_name), K_(parallel), K_(max_error_row_count), K_(dup_action), K_(timeout),
                K_(heartbeat_timeout), K_(force_create));
public:
  ObString table_name_;
  int64_t parallel_;
  uint64_t max_error_row_count_;
  sql::ObLoadDupActionType dup_action_;
  int64_t timeout_;
  int64_t heartbeat_timeout_;
  bool force_create_;
};

struct ObTableDirectLoadBeginRes
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadBeginRes()
    : table_id_(common::OB_INVALID_ID),
      task_id_(0),
      status_(table::ObTableLoadClientStatus::MAX_STATUS),
      error_code_(0)
  {
  }
  TO_STRING_KV(K_(table_id), K_(task_id), K_(column_names), K_(status), K_(error_code));
public:
  uint64_t table_id_;
  int64_t task_id_;
  common::ObSArray<ObString> column_names_;
  table::ObTableLoadClientStatus status_;
  int32_t error_code_;
};

struct ObTableDirectLoadCommitArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadCommitArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  uint64_t table_id_;
  int64_t task_id_;
};

struct ObTableDirectLoadAbortArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadAbortArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  uint64_t table_id_;
  int64_t task_id_;
};

struct ObTableDirectLoadGetStatusArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadGetStatusArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  uint64_t table_id_;
  int64_t task_id_;
};

struct ObTableDirectLoadGetStatusRes
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadGetStatusRes()
    : status_(table::ObTableLoadClientStatus::MAX_STATUS), error_code_(0)
  {
  }
  TO_STRING_KV(K_(status), K_(error_code));
public:
  table::ObTableLoadClientStatus status_;
  int32_t error_code_;
};

struct ObTableDirectLoadInsertArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadInsertArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), "payload", common::ObHexStringWrap(payload_));
public:
  uint64_t table_id_;
  int64_t task_id_;
  ObString payload_;
};

struct ObTableDirectLoadHeartBeatArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadHeartBeatArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  uint64_t table_id_;
  int64_t task_id_;
};

struct ObTableDirectLoadHeartBeatRes
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadHeartBeatRes()
    : status_(table::ObTableLoadClientStatus::MAX_STATUS), error_code_(0)
  {
  }
  TO_STRING_KV(K_(status), K_(error_code));
public:
  table::ObTableLoadClientStatus status_;
  int32_t error_code_;
};

} // namespace observer
} // namespace oceanbase
