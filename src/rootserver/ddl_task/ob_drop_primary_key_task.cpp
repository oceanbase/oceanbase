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

#define USING_LOG_PREFIX RS
#include "ob_drop_primary_key_task.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObDropPrimaryKeyTask::ObDropPrimaryKeyTask()
  : ObTableRedefinitionTask()
{
  task_type_ = ObDDLType::DDL_DROP_PRIMARY_KEY;
}

ObDropPrimaryKeyTask::~ObDropPrimaryKeyTask()
{
}

int ObDropPrimaryKeyTask::init(const ObTableSchema* src_table_schema, const ObTableSchema* dst_table_schema,
                               const int64_t task_id, const share::ObDDLType &ddl_type, const int64_t parallelism,
                               const int64_t consumer_group_id, const int32_t sub_task_trace_id,
                               const obrpc::ObAlterTableArg &alter_table_arg, const uint64_t tenant_data_version,
                               const int64_t task_status,const int64_t snapshot_version )
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableRedefinitionTask::init(src_table_schema, dst_table_schema, 0, task_id, ddl_type, parallelism, consumer_group_id,
                                            sub_task_trace_id, alter_table_arg, tenant_data_version, task_status, snapshot_version))) {
    LOG_WARN("fail to init ObDropPrimaryKeyTask", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    task_version_ = OB_DROP_PRIMARY_KEY_TASK_VERSION;
    ddl_tracing_.open();
  }
  return ret;
}

int ObDropPrimaryKeyTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropPrimaryKeyTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check task health failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::WAIT_TRANS_END))) {
          LOG_WARN("fail to prepare drop primary key task", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, ObDDLTaskStatus::OBTAIN_SNAPSHOT))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::OBTAIN_SNAPSHOT:
        if (OB_FAIL(obtain_snapshot(ObDDLTaskStatus::REDEFINITION))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::REDEFINITION:
        if (OB_FAIL(table_redefinition(ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS))) {
          LOG_WARN("fail to do table redefinition", K(ret));
        }
        break;
      case ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS:
        if (OB_FAIL(copy_table_dependent_objects(ObDDLTaskStatus::MODIFY_AUTOINC))) {
          LOG_WARN("fail to copy table dependent objects", K(ret));
        }
        break;
      case ObDDLTaskStatus::MODIFY_AUTOINC:
        if (OB_FAIL(modify_autoinc(ObDDLTaskStatus::TAKE_EFFECT))) {
          LOG_WARN("fail to modify autoinc", K(ret));
        }
        break;
      case ObDDLTaskStatus::TAKE_EFFECT:
        if (OB_FAIL(take_effect(ObDDLTaskStatus::SUCCESS))) {
          LOG_WARN("fail to take effect", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("fail to do clean up", K(ret));
        }
        break;
      case share::ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(success())) {
          LOG_WARN("fail to success", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected drop primary key task state", K(task_status_));
        break;
    }
    ddl_tracing_.release_span_hierarchy();
    if (OB_FAIL(ret)) {
      add_event_info("drop primary key task process fail");
      LOG_INFO("drop primary key task process fail", "ddl_event_info", ObDDLEventInfo());
    }
  }
  return ret;
}
