// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

#include "common/ob_store_range.h"
#include "lib/net/ob_addr.h"
#include "ob_table_load_array.h"
#include "ob_table_load_define.h"
#include "share/table/ob_table_load_row_array.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace table
{
using common::ObString;

/**
 * begin
 */

class ObTableLoadBeginRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadBeginRequest() {}
  TO_STRING_KV(K_(table_name), K_(config), K_(timeout));
public:
  ObString credential_;
  ObString table_name_;
  ObTableLoadConfig config_;
  int64_t timeout_;
};

class ObTableLoadBeginResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadBeginResult()
    : table_id_(common::OB_INVALID_ID),
      task_id_(0),
      status_(ObTableLoadStatusType::NONE),
      error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(table_id), K_(task_id), K_(column_names), K_(status), K_(error_code));
public:
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadArray<ObString> column_names_;
  ObTableLoadStatusType status_;
  int32_t error_code_;
};

class ObTableLoadPreBeginPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreBeginPeerRequest()
    : table_id_(common::OB_INVALID_ID),
      column_count_(0),
      dup_action_(sql::ObLoadDupActionType::LOAD_INVALID_MODE),
      px_mode_(false),
      online_opt_stat_gather_(false),
      dest_table_id_(common::OB_INVALID_ID),
      task_id_(0),
      schema_version_(0),
      data_version_(0)
  {
  }
  TO_STRING_KV(K_(table_id),
               K_(config),
               K_(column_count),
               K_(dup_action),
               K_(px_mode),
               K_(online_opt_stat_gather),
               K_(dest_table_id),
               K_(task_id),
               K_(schema_version),
               K_(data_version),
               K_(partition_id_array),
               K_(target_partition_id_array));
public:
  ObString credential_;
  uint64_t table_id_;
  ObTableLoadConfig config_;
  uint64_t column_count_;
  sql::ObLoadDupActionType dup_action_;
  bool px_mode_;
  bool online_opt_stat_gather_;
  // ddl param
  uint64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
  int64_t data_version_;
  // partition info
  ObTableLoadArray<ObTableLoadLSIdAndPartitionId> partition_id_array_;//orig table
  ObTableLoadArray<ObTableLoadLSIdAndPartitionId> target_partition_id_array_;//FIXME: target table
};

class ObTableLoadPreBeginPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreBeginPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

class ObTableLoadConfirmBeginPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmBeginPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadConfirmBeginPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmBeginPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * finish
 */

class ObTableLoadFinishRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadFinishRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadFinishResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadFinishResult() : ret_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(ret_code));
public:
  int32_t ret_code_;
};

class ObTableLoadPreMergePeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreMergePeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(committed_trans_id_array));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadArray<ObTableLoadTransId> committed_trans_id_array_;
};

class ObTableLoadPreMergePeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreMergePeerResult() : ret_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(ret_code));
public:
  int32_t ret_code_;
};

class ObTableLoadStartMergePeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadStartMergePeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadStartMergePeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadStartMergePeerResult() : ret_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(ret_code));
public:
  int32_t ret_code_;
};

/**
 * commit
 */

class ObTableLoadCommitRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadCommitRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadCommitResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadCommitResult() : ret_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(ret_code), K_(result_info), K_(sql_statistics));
public:
  int32_t ret_code_;
  ObTableLoadResultInfo result_info_;
  ObTableLoadSqlStatistics sql_statistics_;
};

class ObTableLoadCommitPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadCommitPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadCommitPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadCommitPeerResult() : ret_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(ret_code), K_(result_info), K_(sql_statistics));
public:
  int32_t ret_code_;
  ObTableLoadResultInfo result_info_;
  ObTableLoadSqlStatistics sql_statistics_;
};

/**
 * abort
 */

class ObTableLoadAbortRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbortRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadAbortResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbortResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

class ObTableLoadAbortPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbortPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadAbortPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbortPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * get status
 */

class ObTableLoadGetStatusRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetStatusRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadGetStatusResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetStatusResult()
    : status_(ObTableLoadStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(status), K_(error_code));
public:
  ObTableLoadStatusType status_;
  int32_t error_code_;
};

class ObTableLoadGetStatusPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetStatusPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
};

class ObTableLoadGetStatusPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetStatusPeerResult()
    : status_(ObTableLoadStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(status), K_(error_code));
public:
  ObTableLoadStatusType status_;
  int32_t error_code_;
};

/**
 * load
 */

class ObTableLoadRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadRequest()
    : table_id_(common::OB_INVALID_ID),
      task_id_(0),
      session_id_(0),
      sequence_no_(common::OB_INVALID_ID)
  {
  }
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id), K_(session_id), K_(sequence_no),
               K(payload_.length()));
public:
  ObString credential_;  //这个里面会包含tenant_id, database等信息
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
  int32_t session_id_;    // 从1开始
  uint64_t sequence_no_;  // 从1开始
  ObString payload_; //里面包的是ObTableLoadObjArray / ObTableLoadStrArray / Raw String
};

class ObTableLoadResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

class ObTableLoadPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPeerRequest()
    : table_id_(common::OB_INVALID_ID),
      task_id_(0),
      session_id_(0),
      sequence_no_(common::OB_INVALID_ID) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id), K_(session_id), K_(sequence_no));
public:
  ObString credential_;  //这个里面会包含tenant_id, database等信息
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
  int32_t session_id_;    // 从1开始
  uint64_t sequence_no_;  // 从1开始
  ObString payload_; //里面包的是ObTableLoadObjArray
};

class ObTableLoadPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * start trans
 */

class ObTableLoadStartTransRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadStartTransRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(segment_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadSegmentID segment_id_;
};

class ObTableLoadStartTransResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadStartTransResult()
    : trans_status_(ObTableLoadTransStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(trans_id), K_(trans_status), K_(error_code));
public:
  ObTableLoadTransId trans_id_;
  ObTableLoadTransStatusType trans_status_;
  int32_t error_code_;
};

class ObTableLoadPreStartTransPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreStartTransPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadPreStartTransPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreStartTransPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};


class ObTableLoadConfirmStartTransPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmStartTransPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadConfirmStartTransPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmStartTransPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * finish trans
 */

class ObTableLoadFinishTransRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadFinishTransRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadFinishTransResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadFinishTransResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

class ObTableLoadPreFinishTransPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreFinishTransPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadPreFinishTransPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPreFinishTransPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};


class ObTableLoadConfirmFinishTransPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmFinishTransPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadConfirmFinishTransPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfirmFinishTransPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * abandon trans
 */

class ObTableLoadAbandonTransRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbandonTransRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadAbandonTransResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbandonTransResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

class ObTableLoadAbandonTransPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbandonTransPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadAbandonTransPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadAbandonTransPeerResult() : ret_code_(common::OB_SUCCESS) {}

  int32_t ret_code_;

  TO_STRING_KV(K_(ret_code));
};

/**
 * get trans status
 */

class ObTableLoadGetTransStatusRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetTransStatusRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadGetTransStatusResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetTransStatusResult()
    : trans_status_(ObTableLoadTransStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(trans_status), K_(error_code));
public:
  ObTableLoadTransStatusType trans_status_;
  int32_t error_code_;
};

class ObTableLoadGetTransStatusPeerRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetTransStatusPeerRequest() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));
public:
  ObString credential_;
  uint64_t table_id_;
  int64_t task_id_;
  ObTableLoadTransId trans_id_;
};

class ObTableLoadGetTransStatusPeerResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadGetTransStatusPeerResult()
    : trans_status_(ObTableLoadTransStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(trans_status), K_(error_code));
public:
  ObTableLoadTransStatusType trans_status_;
  int32_t error_code_;
};

}  // namespace table
}  // namespace oceanbase
