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

#pragma once

#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObTableLoadTableCtx;

struct ObTableLoadRedefTableStartArg
{
public:
  ObTableLoadRedefTableStartArg()
    : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID), parallelism_(0),
      is_load_data_(false), is_insert_overwrite_(false)
  {
  }
  ~ObTableLoadRedefTableStartArg() = default;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    parallelism_ = 0;
    is_load_data_ = false;
    is_insert_overwrite_ = false;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != table_id_ &&
           0 != parallelism_;
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(parallelism), K_(is_load_data), K_(is_insert_overwrite));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t parallelism_;
  bool is_load_data_;
  bool is_insert_overwrite_;
};

struct ObTableLoadRedefTableStartRes
{
public:
  ObTableLoadRedefTableStartRes()
    : dest_table_id_(common::OB_INVALID_ID), task_id_(0), schema_version_(0), snapshot_version_(0), data_format_version_(0)
  {
  }
  ~ObTableLoadRedefTableStartRes() = default;
  void reset()
  {
    dest_table_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
    schema_version_ = 0;
    snapshot_version_ = 0;
    data_format_version_ = 0;
  }
  TO_STRING_KV(K_(dest_table_id), K_(task_id), K_(schema_version), K_(snapshot_version), K_(data_format_version));
public:
  uint64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  uint64_t data_format_version_;
};

struct ObTableLoadRedefTableFinishArg
{
public:
  ObTableLoadRedefTableFinishArg()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      dest_table_id_(common::OB_INVALID_ID),
      task_id_(0),
      schema_version_(0)
  {
  }
  ~ObTableLoadRedefTableFinishArg() = default;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    dest_table_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
    schema_version_ = 0;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != table_id_ &&
           common::OB_INVALID_ID != dest_table_id_ && 0 != task_id_ && 0 != schema_version_;
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(dest_table_id), K_(task_id), K_(schema_version));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
};

struct ObTableLoadRedefTableAbortArg
{
public:
  ObTableLoadRedefTableAbortArg() : tenant_id_(common::OB_INVALID_ID), task_id_(0) {}
  ~ObTableLoadRedefTableAbortArg() = default;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
  }
  bool is_valid() const { return common::OB_INVALID_ID != tenant_id_ && 0 != task_id_; }
  TO_STRING_KV(K_(tenant_id), K_(task_id));
public:
  uint64_t tenant_id_;
  int64_t task_id_;
};

class ObTableLoadRedefTable
{
public:
  static int start(const ObTableLoadRedefTableStartArg &arg, ObTableLoadRedefTableStartRes &res,
                   sql::ObSQLSessionInfo &session_info);
  static int finish(const ObTableLoadRedefTableFinishArg &arg, sql::ObSQLSessionInfo &session_info);
  static int abort(const ObTableLoadRedefTableAbortArg &arg, sql::ObSQLSessionInfo &session_info);
};

} // namespace observer
} // namespace oceanbase
