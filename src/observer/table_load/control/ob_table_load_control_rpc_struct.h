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
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace observer
{
enum class ObDirectLoadControlCommandType
{
  PRE_BEGIN = 0,
  CONFIRM_BEGIN = 1,
  PRE_MERGE = 2,
  START_MERGE = 3,
  COMMIT = 4,
  ABORT = 5,
  GET_STATUS = 6,

  // trans command
  PRE_START_TRANS = 7,
  CONFIRM_START_TRANS = 8,
  PRE_FINISH_TRANS = 9,
  CONFIRM_FINISH_TRANS = 10,
  ABANDON_TRANS = 11,
  GET_TRANS_STATUS = 12,
  INSERT_TRANS = 13,

  HEART_BEAT = 14,

  MAX_TYPE
};

struct ObDirectLoadControlRequest
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlRequest() : command_type_(observer::ObDirectLoadControlCommandType::MAX_TYPE)
  {
  }
  template <class Arg>
  int set_arg(const Arg &arg, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = arg.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory", KR(ret), K(size));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      SERVER_LOG(WARN, "fail to serialize arg", KR(ret), K(arg));
    } else {
      arg_content_.assign_ptr(buf, size);
    }
    return ret;
  }
  template <class Arg>
  int get_arg(Arg &arg) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(arg_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", KR(ret), KPC(this));
    } else if (OB_FAIL(arg.deserialize(arg_content_.ptr(), arg_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize arg content", KR(ret), KPC(this));
    }
    return ret;
  }
  TO_STRING_KV(K_(command_type), "arg_content", common::ObHexStringWrap(arg_content_));

public:
  observer::ObDirectLoadControlCommandType command_type_;
  ObString arg_content_;
};

class ObDirectLoadControlResult
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlResult()
    : allocator_(nullptr), command_type_(observer::ObDirectLoadControlCommandType::MAX_TYPE)
  {
  }
  template <class Res>
  int set_res(const Res &res, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = res.get_serialize_size();
    if (size > 0) {
      char *buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", KR(ret), K(size));
      } else if (OB_FAIL(res.serialize(buf, size, pos))) {
        SERVER_LOG(WARN, "fail to serialize res", KR(ret), K(res));
      } else {
        res_content_.assign_ptr(buf, size);
      }
    }
    return ret;
  }
  template <class Res>
  int get_res(Res &res) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(res_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", KR(ret), KPC(this));
    } else if (OB_FAIL(res.deserialize(res_content_.ptr(), res_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize res content", KR(ret), KPC(this));
    }
    return ret;
  }
  TO_STRING_KV(K_(command_type), "res_content", common::ObHexStringWrap(res_content_));

public:
  common::ObIAllocator *allocator_; // for deserialize
  observer::ObDirectLoadControlCommandType command_type_;
  ObString res_content_;
};

//////////////////////////////////////////////////////////////////////

class ObDirectLoadControlPreBeginArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlPreBeginArg();
  ~ObDirectLoadControlPreBeginArg();
  TO_STRING_KV(K_(table_id), K_(config), K_(column_count), K_(dup_action), K_(px_mode),
               K_(online_opt_stat_gather), K_(dest_table_id), K_(task_id), K_(schema_version),
               K_(snapshot_version), K_(data_version), K_(partition_id_array),
               K_(target_partition_id_array));

public:
  uint64_t table_id_;
  table::ObTableLoadConfig config_;
  uint64_t column_count_;
  sql::ObLoadDupActionType dup_action_;
  bool px_mode_;
  bool online_opt_stat_gather_;
  // ddl param
  uint64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t data_version_;
  // partition info
  table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> partition_id_array_; // origin table
  table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> target_partition_id_array_; // target table
  sql::ObSQLSessionInfo *session_info_;
  sql::ObFreeSessionCtx free_session_ctx_;
};

class ObDirectLoadControlConfirmBeginArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlConfirmBeginArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlPreMergeArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlPreMergeArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(committed_trans_id_array));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadArray<table::ObTableLoadTransId> committed_trans_id_array_;
};

class ObDirectLoadControlStartMergeArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlStartMergeArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlCommitArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlCommitArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlCommitRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlCommitRes() {}
  TO_STRING_KV(K_(result_info), K_(sql_statistics))
public:
  table::ObTableLoadResultInfo result_info_;
  table::ObTableLoadSqlStatistics sql_statistics_;
};

class ObDirectLoadControlAbortArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlAbortArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlAbortRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlAbortRes() : is_stopped_(false) {}
  TO_STRING_KV(K_(is_stopped));

public:
  bool is_stopped_;
};

class ObDirectLoadControlGetStatusArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlGetStatusArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlGetStatusRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlGetStatusRes()
    : status_(table::ObTableLoadStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(status), K_(error_code))
public:
  table::ObTableLoadStatusType status_;
  int32_t error_code_;
};

class ObDirectLoadControlHeartBeatArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlHeartBeatArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
};

class ObDirectLoadControlPreStartTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlPreStartTransArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlConfirmStartTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlConfirmStartTransArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlPreFinishTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlPreFinishTransArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlConfirmFinishTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlConfirmFinishTransArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlAbandonTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlAbandonTransArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlGetTransStatusArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlGetTransStatusArg() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
};

class ObDirectLoadControlGetTransStatusRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlGetTransStatusRes()
    : trans_status_(table::ObTableLoadTransStatusType::NONE), error_code_(common::OB_SUCCESS)
  {
  }
  TO_STRING_KV(K_(trans_status), K_(error_code))
public:
  table::ObTableLoadTransStatusType trans_status_;
  int32_t error_code_;
};

class ObDirectLoadControlInsertTransArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadControlInsertTransArg()
    : table_id_(common::OB_INVALID_ID),
      task_id_(0),
      session_id_(0),
      sequence_no_(common::OB_INVALID_ID)
  {
  }
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trans_id), K_(session_id), K_(sequence_no));

public:
  uint64_t table_id_;
  int64_t task_id_;
  table::ObTableLoadTransId trans_id_;
  int32_t session_id_; // 从1开始
  uint64_t sequence_no_; // 从1开始
  ObString payload_; //里面包的是ObTableLoadObjArray
};

} // namespace observer
} // namespace oceanbase
