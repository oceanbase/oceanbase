// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX CLIENT

#include "ob_table_load_rpc_struct.h"

namespace oceanbase
{
namespace table
{
using namespace common;

/**
 * begin
 */


OB_SERIALIZE_MEMBER(ObTableLoadBeginRequest,
                    credential_,
                    table_name_,
                    config_,
                    timeout_);

OB_SERIALIZE_MEMBER(ObTableLoadBeginResult,
                    table_id_,
                    task_id_,
                    column_names_,
                    status_,
                    error_code_);

OB_SERIALIZE_MEMBER(ObTableLoadPreBeginPeerRequest,
                    credential_,
                    table_id_,
                    config_,
                    column_count_,
                    dup_action_,
                    px_mode_,
                    online_opt_stat_gather_,
                    dest_table_id_,
                    task_id_,
                    schema_version_,
                    data_version_,
                    partition_id_array_,
                    target_partition_id_array_);

OB_SERIALIZE_MEMBER(ObTableLoadPreBeginPeerResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadConfirmBeginPeerRequest,
                    credential_,
                    table_id_,
                    task_id_)

OB_SERIALIZE_MEMBER(ObTableLoadConfirmBeginPeerResult,
                    ret_code_);

/**
 * finish
 */

OB_SERIALIZE_MEMBER(ObTableLoadFinishRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadFinishResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadPreMergePeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    committed_trans_id_array_);

OB_SERIALIZE_MEMBER(ObTableLoadPreMergePeerResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadStartMergePeerRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadStartMergePeerResult,
                    ret_code_);

/**
 * commit
 */

OB_SERIALIZE_MEMBER(ObTableLoadCommitRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadCommitResult,
                    ret_code_,
                    result_info_,
                    sql_statistics_);

OB_SERIALIZE_MEMBER(ObTableLoadCommitPeerRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadCommitPeerResult,
                    ret_code_,
                    result_info_,
                    sql_statistics_);

/**
 * abort
 */

OB_SERIALIZE_MEMBER(ObTableLoadAbortRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadAbortResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadAbortPeerRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadAbortPeerResult,
                    ret_code_);

/**
 * get status
 */

OB_SERIALIZE_MEMBER(ObTableLoadGetStatusRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadGetStatusResult,
                    status_,
                    error_code_);

OB_SERIALIZE_MEMBER(ObTableLoadGetStatusPeerRequest,
                    credential_,
                    table_id_,
                    task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadGetStatusPeerResult,
                    status_,
                    error_code_);

/**
 * load
 */

OB_SERIALIZE_MEMBER(ObTableLoadRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_,
                    session_id_,
                    sequence_no_,
                    payload_);

OB_SERIALIZE_MEMBER(ObTableLoadResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_,
                    session_id_,
                    sequence_no_,
                    payload_);

OB_SERIALIZE_MEMBER(ObTableLoadPeerResult,
                    ret_code_);

/**
 * start trans
 */

OB_SERIALIZE_MEMBER(ObTableLoadStartTransRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    segment_id_);

OB_SERIALIZE_MEMBER(ObTableLoadStartTransResult,
                    trans_id_,
                    trans_status_,
                    error_code_);

OB_SERIALIZE_MEMBER(ObTableLoadPreStartTransPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadPreStartTransPeerResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadConfirmStartTransPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadConfirmStartTransPeerResult,
                    ret_code_);

/**
 * finish trans
 */

OB_SERIALIZE_MEMBER(ObTableLoadFinishTransRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadFinishTransResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadPreFinishTransPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadPreFinishTransPeerResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadConfirmFinishTransPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadConfirmFinishTransPeerResult,
                    ret_code_);

/**
 * abandon trans
 */

OB_SERIALIZE_MEMBER(ObTableLoadAbandonTransRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadAbandonTransResult,
                    ret_code_);

OB_SERIALIZE_MEMBER(ObTableLoadAbandonTransPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadAbandonTransPeerResult,
                    ret_code_);

/**
 * get trans status
 */

OB_SERIALIZE_MEMBER(ObTableLoadGetTransStatusRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadGetTransStatusResult,
                    trans_status_,
                    error_code_);

OB_SERIALIZE_MEMBER(ObTableLoadGetTransStatusPeerRequest,
                    credential_,
                    table_id_,
                    task_id_,
                    trans_id_);

OB_SERIALIZE_MEMBER(ObTableLoadGetTransStatusPeerResult,
                    trans_status_,
                    error_code_);

}  // namespace table
}  // namespace oceanbase
