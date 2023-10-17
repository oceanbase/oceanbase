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

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_control_rpc_struct.h"
#include "observer/table_load/ob_table_load_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace sql;
using namespace table;

OB_SERIALIZE_MEMBER(ObDirectLoadControlRequest,
                    command_type_,
                    arg_content_);

OB_UNIS_DEF_SERIALIZE(ObDirectLoadControlResult,
                      command_type_,
                      res_content_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObDirectLoadControlResult,
                           command_type_,
                           res_content_);

OB_DEF_DESERIALIZE(ObDirectLoadControlResult)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator in deserialize", K(ret));
  } else {
    ObString tmp_res_content;
    LST_DO_CODE(OB_UNIS_DECODE,
                command_type_,
                tmp_res_content);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(*allocator_, tmp_res_content, res_content_))) {
      LOG_WARN("fail to copy string", K(ret));
    }
  }
  return ret;
}

// pre_begin
ObDirectLoadControlPreBeginArg::ObDirectLoadControlPreBeginArg()
  : table_id_(common::OB_INVALID_ID),
    column_count_(0),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    px_mode_(false),
    online_opt_stat_gather_(false),
    dest_table_id_(common::OB_INVALID_ID),
    task_id_(0),
    schema_version_(0),
    snapshot_version_(0),
    data_version_(0),
    session_info_(nullptr)
{
  free_session_ctx_.sessid_ = ObSQLSessionInfo::INVALID_SESSID;
}

ObDirectLoadControlPreBeginArg::~ObDirectLoadControlPreBeginArg()
{
  if (nullptr != session_info_) {
    if (free_session_ctx_.sessid_ != ObSQLSessionInfo::INVALID_SESSID) {
      ObTableLoadUtils::free_session_info(session_info_, free_session_ctx_);
    }
    session_info_ = nullptr;
  }
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadControlPreBeginArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              config_,
              column_count_,
              dup_action_,
              px_mode_,
              online_opt_stat_gather_,
              dest_table_id_,
              task_id_,
              schema_version_,
              snapshot_version_,
              data_version_,
              partition_id_array_,
              target_partition_id_array_);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info is null", KR(ret));
    } else {
      OB_UNIS_ENCODE(*session_info_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadControlPreBeginArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              table_id_,
              config_,
              column_count_,
              dup_action_,
              px_mode_,
              online_opt_stat_gather_,
              dest_table_id_,
              task_id_,
              schema_version_,
              snapshot_version_,
              data_version_,
              partition_id_array_,
              target_partition_id_array_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableLoadUtils::create_session_info(session_info_, free_session_ctx_))) {
      LOG_WARN("fail to init session info", KR(ret));
    } else {
      OB_UNIS_DECODE(*session_info_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadControlPreBeginArg)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              config_,
              column_count_,
              dup_action_,
              px_mode_,
              online_opt_stat_gather_,
              dest_table_id_,
              task_id_,
              schema_version_,
              snapshot_version_,
              data_version_,
              partition_id_array_,
              target_partition_id_array_);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info is null", KR(ret));
    } else {
      OB_UNIS_ADD_LEN(*session_info_);
    }
  }
  return len;
}

// confirm_begin
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlConfirmBeginArg,
                           table_id_,
                           task_id_);

// pre_merge
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlPreMergeArg,
                           table_id_,
                           task_id_,
                           committed_trans_id_array_);

// start_merge
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlStartMergeArg,
                           table_id_,
                           task_id_);

// commit
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlCommitArg,
                           table_id_,
                           task_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlCommitRes,
                           result_info_,
                           sql_statistics_);

// abort
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlAbortArg,
                           table_id_,
                           task_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlAbortRes,
                           is_stopped_);

// get_status
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlGetStatusArg,
                           table_id_,
                           task_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlGetStatusRes,
                           status_,
                           error_code_);

// heartbeat
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlHeartBeatArg,
                           table_id_,
                           task_id_);

// pre_start_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlPreStartTransArg,
                           table_id_,
                           task_id_,
                           trans_id_);

// confirm_start_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlConfirmStartTransArg,
                           table_id_,
                           task_id_,
                           trans_id_);

// pre_finish_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlPreFinishTransArg,
                           table_id_,
                           task_id_,
                           trans_id_);

// confirm_finish_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlConfirmFinishTransArg,
                           table_id_,
                           task_id_,
                           trans_id_);

// abandon_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlAbandonTransArg,
                           table_id_,
                           task_id_,
                           trans_id_);

// get_trans_status
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlGetTransStatusArg,
                           table_id_,
                           task_id_,
                           trans_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlGetTransStatusRes,
                           trans_status_,
                           error_code_);

// insert_trans
OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadControlInsertTransArg,
                           table_id_,
                           task_id_,
                           trans_id_,
                           session_id_,
                           sequence_no_,
                           payload_);

} // namespace observer
} // namespace oceanbase
