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

#include "ob_ddl_args.h"

namespace oceanbase
{
namespace obrpc
{
int ObDDLArg::assign(const ObDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(based_schema_object_infos_.assign(other.based_schema_object_infos_))) {
    LOG_WARN("fail to assign based_schema_object_infos", KR(ret));
  } else {
    ddl_stmt_str_ = other.ddl_stmt_str_;
    exec_tenant_id_ = other.exec_tenant_id_;
    ddl_id_str_ = other.ddl_id_str_;
    sync_from_primary_ = other.sync_from_primary_;
    parallelism_ = other.parallelism_;
    task_id_ = other.task_id_;
    consumer_group_id_ = other.consumer_group_id_;
    is_parallel_ = other.is_parallel_;
  }
  return ret;
}

DEF_TO_STRING(ObDDLArg)
{
  int64_t pos = 0;
  J_KV("ddl_stmt_str", contain_sensitive_data() ? ObString(OB_MASKED_STR) : ddl_stmt_str_,
       K_(exec_tenant_id),
       K_(ddl_id_str),
       K_(sync_from_primary),
       K_(based_schema_object_infos),
       K_(parallelism),
       K_(task_id),
       K_(consumer_group_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObDDLArg,
                    ddl_stmt_str_,
                    exec_tenant_id_,
                    ddl_id_str_,
                    sync_from_primary_,
                    based_schema_object_infos_,
                    parallelism_,
                    task_id_,
                    consumer_group_id_,
                    is_parallel_);

} // namespace obrpc
} // namespace oceanbase
