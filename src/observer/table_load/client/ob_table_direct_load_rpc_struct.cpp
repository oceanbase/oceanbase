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

 #include "ob_table_direct_load_rpc_struct.h"

 namespace oceanbase
{
namespace observer
{

// begin
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadBeginArg,
                           table_name_,
                           parallel_,
                           max_error_row_count_,
                           dup_action_,
                           timeout_,
                           heartbeat_timeout_,
                           force_create_,
                           is_async_,
                           load_method_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadBeginRes,
                           table_id_,
                           task_id_,
                           column_names_,
                           status_,
                           error_code_);

// commit
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadCommitArg,
                           table_id_,
                           task_id_);

// abort
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadAbortArg,
                           table_id_,
                           task_id_);

// get_status
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadGetStatusArg,
                           table_id_,
                           task_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadGetStatusRes,
                           status_,
                           error_code_);

// insert
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadInsertArg,
                           table_id_,
                           task_id_,
                           payload_);

// heart_beat
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadHeartBeatArg,
                           table_id_,
                           task_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadHeartBeatRes,
                           status_,
                           error_code_);

} // namespace observer
} // namespace oceanbase
