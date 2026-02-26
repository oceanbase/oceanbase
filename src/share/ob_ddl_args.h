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

#ifndef OCEANBASE_SHARE_OB_DDL_ARGS_H_
#define OCEANBASE_SHARE_OB_DDL_ARGS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
struct ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLArg() :
      ddl_stmt_str_(),
      exec_tenant_id_(common::OB_INVALID_TENANT_ID),
      ddl_id_str_(),
      sync_from_primary_(false),
      based_schema_object_infos_(),
      parallelism_(0),
      task_id_(0),
      consumer_group_id_(0),
      is_parallel_(false)
   { }
  virtual ~ObDDLArg() = default;
  bool is_need_check_based_schema_objects() const
  {
    return 0 < based_schema_object_infos_.count();
  }
  virtual bool is_allow_when_disable_ddl() const { return false; }
  virtual bool is_allow_when_upgrade() const { return false; }
  bool is_sync_from_primary() const
  {
    return sync_from_primary_;
  }
  //user tenant can not ddl in standby
  virtual bool is_allow_in_standby() const
  { return !is_user_tenant(exec_tenant_id_); }
  virtual int assign(const ObDDLArg &other);
  virtual bool contain_sensitive_data() const { return false; }
  void reset()
  {
    ddl_stmt_str_.reset();
    exec_tenant_id_ = common::OB_INVALID_TENANT_ID;
    ddl_id_str_.reset();
    sync_from_primary_ = false;
    based_schema_object_infos_.reset();
    parallelism_ = 0;
    task_id_ = 0;
    consumer_group_id_ = 0;
    is_parallel_ = false;
  }
  DECLARE_TO_STRING;

  common::ObString ddl_stmt_str_;
  uint64_t exec_tenant_id_;
  common::ObString ddl_id_str_;
  bool sync_from_primary_;
  common::ObSArray<share::schema::ObBasedSchemaObjectInfo> based_schema_object_infos_;
  int64_t parallelism_;
  int64_t task_id_;
  int64_t consumer_group_id_;
  //some parallel ddl is effect before 4220, this member is valid after 4220
  bool is_parallel_;
};

} // namespace obrpc
} // namespace oceanbase
#endif
