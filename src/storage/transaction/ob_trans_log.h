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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_LOG_
#define OCEANBASE_TRANSACTION_OB_TRANS_LOG_

#include <stdint.h>
#include "share/ob_define.h"
#include "share/config/ob_server_config.h"
#include "storage/ob_storage_log_type.h"
#include "ob_trans_define.h"
#include "share/ob_cluster_version.h"
#include "ob_trans_split_adapter.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObDataBuffer;
}  // namespace common
namespace clog {
class ObLogEntry;
}

namespace transaction {

static const int64_t LOG_VERSION_0 = 0;
// for replication group
static const int64_t LOG_VERSION_1 = 1;

class ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransLog() : log_type_(storage::OB_LOG_UNKNOWN), partition_(), trans_id_(), cluster_id_(0)
  {}
  ObTransLog(
      const int64_t log_type, const common::ObPartitionKey& pkey, const ObTransID& trans_id, const uint64_t cluster_id)
      : log_type_(log_type), partition_(pkey), trans_id_(trans_id), cluster_id_(cluster_id)
  {}
  virtual ~ObTransLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const uint64_t cluster_id);

public:
  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  virtual bool is_valid() const;
  uint64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  // Update tenant_id for physical backup and restore
  int inner_replace_tenant_id(const uint64_t tenant_id);
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id));

public:
  virtual uint64_t get_tenant_id() const
  {
    return partition_.get_tenant_id();
  }
  // Update tenant_id for physical backup and restore,
  // the extended class must implement this method
  virtual int replace_tenant_id(const uint64_t new_tenant_id) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransLog);

protected:
  int64_t log_type_;
  common::ObPartitionKey partition_;
  ObTransID trans_id_;
  uint64_t cluster_id_;
};

// log buffer for submitting log
template <int64_t BUFSIZE>
class LogBuf {
public:
  LogBuf()
  {
    reset();
  }
  ~LogBuf()
  {}
  void reset()
  {}
  void destroy()
  {}
  char* get_buf()
  {
    return buf_;
  }
  int64_t get_size() const
  {
    return BUFSIZE;
  }

public:
  // When ob_resource_pool is used to allocate clog_buf and mutator buf, total number is 128 by default,
  // after this value is exceeded, ob_malloc/ob_free will be used for operation
  static const int64_t RP_TOTAL_NUM = 256;
  static const int64_t RP_RESERVE_NUM = 64;

private:
  char buf_[BUFSIZE];
};

typedef LogBuf<common::OB_MAX_LOG_ALLOWED_SIZE> ClogBuf;
typedef LogBuf<common::OB_MAX_LOG_ALLOWED_SIZE> MutatorBuf;

class ObTransMutator : public common::ObDataBuffer {
  OB_UNIS_VERSION(1);

public:
  ObTransMutator() : mutator_buf_(NULL), use_mutator_buf_(true)
  {
    reset();
  }
  ~ObTransMutator()
  {
    destroy();
  }
  int init(const bool use_mutator_buf = true);
  void reset();
  void destroy()
  {
    reset();
  }

public:
  MutatorBuf* get_mutator_buf()
  {
    return mutator_buf_;
  }
  bool get_use_mutator_buf() const
  {
    return use_mutator_buf_;
  }
  int assign(const ObTransMutator& src);
  int assign(const char* data, const int64_t size);

  TO_STRING_KV(KP_(mutator_buf), K_(use_mutator_buf), K_(data), K_(position), K_(capacity));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransMutator);

private:
  MutatorBuf* mutator_buf_;
  // If MutatorBuf is not used to store data, data will not be copied during deserialization,
  // and the data pointer directly points to the deserialized buffer.
  // Whether to use MutatorBuf to store data
  bool use_mutator_buf_;
};

class ObTransMutatorIterator {
public:
  ObTransMutatorIterator() : ptr_(NULL), size_(0), pos_(0)
  {}
  ~ObTransMutatorIterator()
  {}
  int init(const char* ptr, const int64_t size);
  void reset();
  int get_next(common::ObPartitionKey& pkey, const char*& ptr, int64_t& size);
  int get_next(common::ObPartitionKey& pkey, ObTransMutator& mutator, int64_t& size);

private:
  const char* ptr_;
  int64_t size_;
  int64_t pos_;
};

class ObTransRedoLogHelper {
public:
  ObTransRedoLogHelper()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        log_no_(-1),
        scheduler_(),
        coordinator_(),
        participants_(),
        trans_param_(),
        active_memstore_version_(),
        prev_trans_arr_(),
        can_elr_(false),
        xid_(),
        is_last_(false)
  {}
  ~ObTransRedoLogHelper()
  {}

public:
  uint64_t tenant_id_;
  int64_t log_no_;
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  ObStartTransParam trans_param_;
  common::ObVersion active_memstore_version_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  ObXATransID xid_;
  bool is_last_;
};

class ObTransRedoLog : public ObTransLog {
  OB_UNIS_VERSION_V(1);

public:
  ObTransRedoLog(ObTransRedoLogHelper& helper)
      : ObTransLog(),
        tenant_id_(helper.tenant_id_),
        log_no_(helper.log_no_),
        scheduler_(helper.scheduler_),
        coordinator_(helper.coordinator_),
        participants_(helper.participants_),
        trans_param_(helper.trans_param_),
        mutator_(),
        active_memstore_version_(helper.active_memstore_version_),
        prev_trans_arr_(helper.prev_trans_arr_),
        can_elr_(helper.can_elr_),
        xid_(helper.xid_),
        is_last_(helper.is_last_)
  {}

  ObTransRedoLog(const int64_t log_type, const common::ObPartitionKey& partition, ObTransID& trans_id,
      const uint64_t tenant_id, const int64_t log_no, common::ObAddr& scheduler, common::ObPartitionKey& coordinator,
      common::ObPartitionArray& participants, ObStartTransParam& trans_param, const uint64_t cluster_id,
      ObElrTransInfoArray& prev_trans_arr, const bool can_elr, ObXATransID& xid, const bool is_last)
      : ObTransLog(log_type, partition, trans_id, cluster_id),
        tenant_id_(tenant_id),
        log_no_(log_no),
        scheduler_(scheduler),
        coordinator_(coordinator),
        participants_(participants),
        trans_param_(trans_param),
        mutator_(),
        active_memstore_version_(),
        prev_trans_arr_(prev_trans_arr),
        can_elr_(can_elr),
        xid_(xid),
        is_last_(is_last)
  {}
  ~ObTransRedoLog()
  {}
  int init();

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_log_no() const
  {
    return log_no_;
  }
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  const common::ObPartitionKey& get_coordinator() const
  {
    return coordinator_;
  }
  const common::ObPartitionArray& get_participants() const
  {
    return participants_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  ObTransMutator& get_mutator()
  {
    return mutator_;
  }
  const ObTransMutator& get_mutator() const
  {
    return mutator_;
  }
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  void set_last()
  {
    is_last_ = true;
  }
  bool is_last() const
  {
    return is_last_;
  }
  const ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool is_xa_trans() const;

  VIRTUAL_TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(tenant_id), K_(log_no), K_(scheduler),
      K_(coordinator), K_(participants), K_(trans_param), K_(cluster_id), K_(active_memstore_version),
      K_(prev_trans_arr), K_(can_elr), K_(xid), K_(is_last));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransRedoLog);

private:
  uint64_t tenant_id_;
  int64_t log_no_;
  common::ObAddr& scheduler_;
  common::ObPartitionKey& coordinator_;
  common::ObPartitionArray& participants_;
  ObStartTransParam& trans_param_;
  ObTransMutator mutator_;
  common::ObVersion active_memstore_version_;
  ObElrTransInfoArray& prev_trans_arr_;
  bool can_elr_;
  ObXATransID& xid_;
  bool is_last_;
};

class ObTransPrepareLogHelper {
public:
  ObTransPrepareLogHelper()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        scheduler_(),
        coordinator_(),
        participants_(),
        trans_param_(),
        prepare_status_(common::OB_SUCCESS),
        prev_redo_log_ids_(common::ObModIds::OB_TRANS_REDO_LOG_ID_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        local_trans_version_(-1),
        active_memstore_version_(),
        app_trace_id_str_(),
        partition_log_info_arr_(),
        checkpoint_(0),
        prev_trans_arr_(),
        can_elr_(false),
        xid_()
  {}
  ~ObTransPrepareLogHelper()
  {}

public:
  uint64_t tenant_id_;
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  ObStartTransParam trans_param_;
  int prepare_status_;
  // Only record the redo log id before the prepare log.
  // If the prepare log and the redo log are in the same log,
  // then the prepare log id is not recorded
  ObRedoLogIdArray prev_redo_log_ids_;
  // local_trans_version_ is not used anymore.
  // before version 2.2, before submitting prepare_log, prepare_version
  // will be pre-allocated and recorded in the log body for updating
  // part_ctx's prepare_version during follower's replay process.
  // and it's equal to submit_timestamp recorded in log header, to avoid redundency,
  // this field is not used in version 2.2 and later versions.(it's not removed for compatibility reasons)
  int64_t local_trans_version_;
  common::ObVersion active_memstore_version_;
  common::ObString app_trace_id_str_;
  PartitionLogInfoArray partition_log_info_arr_;
  int64_t checkpoint_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  common::ObString app_trace_info_;
  ObXATransID xid_;
};

class ObTransPrepareLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransPrepareLog(const int64_t log_type, common::ObPartitionKey& partition, ObTransID& trans_id,
      const uint64_t tenant_id, common::ObAddr& scheduler, common::ObPartitionKey& coordinator,
      common::ObPartitionArray& participants, ObStartTransParam& trans_param, const int prepare_status,
      ObRedoLogIdArray& prev_redo_log_ids, const uint64_t cluster_id, common::ObString& app_trace_id_str,
      PartitionLogInfoArray& partition_log_info, const int64_t checkpoint, ObElrTransInfoArray& prev_trans_arr,
      const bool can_elr, const common::ObString& app_trace_info, ObXATransID& xid)
      : ObTransLog(log_type, partition, trans_id, cluster_id),
        tenant_id_(tenant_id),
        scheduler_(scheduler),
        coordinator_(coordinator),
        participants_(participants),
        trans_param_(trans_param),
        prepare_status_(prepare_status),
        prev_redo_log_ids_(prev_redo_log_ids),
        local_trans_version_(-1),
        active_memstore_version_(common::ObVersion(2)),
        app_trace_id_str_(app_trace_id_str),
        partition_log_info_arr_(partition_log_info),
        checkpoint_(checkpoint),
        prev_trans_arr_(prev_trans_arr),
        can_elr_(can_elr),
        app_trace_info_(app_trace_info),
        xid_(xid)
  {}

  ObTransPrepareLog(ObTransPrepareLogHelper& helper)
      : ObTransLog(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        scheduler_(helper.scheduler_),
        coordinator_(helper.coordinator_),
        participants_(helper.participants_),
        trans_param_(helper.trans_param_),
        prepare_status_(common::OB_SUCCESS),
        prev_redo_log_ids_(helper.prev_redo_log_ids_),
        local_trans_version_(-1),
        active_memstore_version_(common::ObVersion(2)),
        app_trace_id_str_(helper.app_trace_id_str_),
        partition_log_info_arr_(helper.partition_log_info_arr_),
        checkpoint_(0),
        prev_trans_arr_(helper.prev_trans_arr_),
        can_elr_(false),
        app_trace_info_(helper.app_trace_info_),
        xid_(helper.xid_)
  {}

  ~ObTransPrepareLog()
  {}

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  const common::ObPartitionKey& get_coordinator() const
  {
    return coordinator_;
  }
  const common::ObPartitionArray& get_participants() const
  {
    return participants_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  int get_prepare_status() const
  {
    return prepare_status_;
  }
  const ObRedoLogIdArray& get_redo_log_ids() const
  {
    return prev_redo_log_ids_;
  }
  const common::ObString& get_app_trace_id_str() const
  {
    return app_trace_id_str_;
  }
  const common::ObString& get_app_trace_info() const
  {
    return app_trace_info_;
  }
  const PartitionLogInfoArray& get_partition_log_info_arr() const
  {
    return partition_log_info_arr_;
  }
  bool is_batch_commit_trans() const
  {
    return partition_log_info_arr_.count() > 0 && xid_.empty();
  }
  int64_t get_commit_version() const;
  int64_t get_checkpoint() const
  {
    return checkpoint_;
  }
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  const ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;

  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(tenant_id), K_(scheduler), K_(coordinator),
      K_(participants), K_(trans_param), K_(prepare_status), K_(prev_redo_log_ids), K_(cluster_id),
      K_(active_memstore_version), K_(app_trace_id_str), K_(partition_log_info_arr), K_(checkpoint), K_(prev_trans_arr),
      K_(can_elr), K_(app_trace_info), K_(xid));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransPrepareLog);

private:
  uint64_t tenant_id_;
  common::ObAddr& scheduler_;
  common::ObPartitionKey& coordinator_;
  common::ObPartitionArray& participants_;
  ObStartTransParam& trans_param_;
  int prepare_status_;
  // Only record the redo log id before the prepare log.
  // If the prepare log and the redo log are in the same log,
  // then the prepare log id is not recorded
  ObRedoLogIdArray& prev_redo_log_ids_;
  // local_trans_version_ is not used anymore.
  // before version 2.2, before submitting prepare_log, prepare_version
  // will be pre-allocated and recorded in the log body for updating
  // part_ctx's prepare_version during follower's replay process.
  // and it's equal to submit_timestamp recorded in log header, to avoid redundency,
  // this field is not used in version 2.2 and later versions.(it's not removed for compatability reasons)
  int64_t local_trans_version_;
  common::ObVersion active_memstore_version_;
  common::ObString& app_trace_id_str_;
  PartitionLogInfoArray& partition_log_info_arr_;
  int64_t checkpoint_;
  ObElrTransInfoArray& prev_trans_arr_;
  bool can_elr_;
  common::ObString app_trace_info_;
  ObXATransID& xid_;
};

class ObTransCommitLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransCommitLog(PartitionLogInfoArray& partition_log_info_arr)
      : ObTransLog(), partition_log_info_arr_(partition_log_info_arr), global_trans_version_(-1), checksum_(0)
  {}
  ObTransCommitLog(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      PartitionLogInfoArray& partition_log_info_arr, const int64_t global_trans_version, const uint64_t checksum,
      const uint64_t cluster_id)
      : ObTransLog(log_type, partition, trans_id, cluster_id),
        partition_log_info_arr_(partition_log_info_arr),
        global_trans_version_(global_trans_version),
        checksum_(checksum)
  {}
  ~ObTransCommitLog()
  {}

public:
  int64_t get_global_trans_version() const
  {
    return global_trans_version_;
  }
  const PartitionLogInfoArray& get_partition_log_info_array() const
  {
    return partition_log_info_arr_;
  }
  uint64_t get_checksum() const
  {
    return checksum_;
  }
  const ObTransSplitInfo& get_split_info() const
  {
    return split_info_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(partition_log_info_arr), K_(global_trans_version),
      K_(checksum), K_(cluster_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransCommitLog);

private:
  PartitionLogInfoArray& partition_log_info_arr_;
  int64_t global_trans_version_;
  uint64_t checksum_;
  ObTransSplitInfo split_info_;
};

class ObTransPreCommitLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransPreCommitLog() : ObTransLog(), publish_version_(-1)
  {}
  ~ObTransPreCommitLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const uint64_t cluster_id, const int64_t publish_version);
  int64_t get_publish_version() const
  {
    return publish_version_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;

public:
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id), K_(publish_version));

private:
  int64_t publish_version_;
};

class ObTransAbortLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransAbortLog() : ObTransLog(), partition_log_info_arr_()
  {}
  ~ObTransAbortLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const PartitionLogInfoArray& partition_log_info_arr, const uint64_t cluster_id);
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  const ObTransSplitInfo& get_split_info() const
  {
    return split_info_;
  }
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id), K_(partition_log_info_arr));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransAbortLog);

private:
  PartitionLogInfoArray partition_log_info_arr_;
  ObTransSplitInfo split_info_;
};

class ObTransClearLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransClearLog() : ObTransLog()
  {}
  ~ObTransClearLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const uint64_t cluster_id);
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransClearLog);

public:
  bool is_valid() const;
};

class ObSpTransRedoLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObSpTransRedoLog()
      : ObTransLog(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        log_no_(-1),
        trans_param_(),
        mutator_(),
        active_memstore_version_(),
        prev_trans_arr_(),
        can_elr_(false)
  {}
  ~ObSpTransRedoLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const uint64_t tenant_id, const int64_t log_no, const ObStartTransParam& trans_param, const uint64_t cluster_id,
      const ObElrTransInfoArray& prev_trans_arr, const bool can_elr);

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_log_no() const
  {
    return log_no_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  ObTransMutator& get_mutator()
  {
    return mutator_;
  }
  const ObTransMutator& get_mutator() const
  {
    return mutator_;
  }
  bool is_empty_redo_log() const
  {
    return 0 == mutator_.get_position();
  }
  int set_log_type(const int64_t log_type);
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  int set_prev_trans_arr(const ObElrTransInfoArray& elr_arr);
  bool is_can_elr() const
  {
    return can_elr_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;

  VIRTUAL_TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(tenant_id), K_(log_no), K_(trans_param),
      K_(cluster_id), K_(active_memstore_version), K_(prev_trans_arr), K_(can_elr));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransRedoLog);

protected:
  uint64_t tenant_id_;
  int64_t log_no_;
  ObStartTransParam trans_param_;
  ObTransMutator mutator_;
  common::ObVersion active_memstore_version_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
};

class ObSpTransCommitLog : public ObSpTransRedoLog {
  OB_UNIS_VERSION(1);

public:
  ObSpTransCommitLog()
      : ObSpTransRedoLog(),
        global_trans_version_(-1),
        checksum_(0),
        prev_redo_log_ids_(common::ObModIds::OB_TRANS_REDO_LOG_ID_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        app_trace_id_str_(),
        checkpoint_(0),
        app_trace_info_()
  {}
  ~ObSpTransCommitLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const uint64_t tenant_id,
      const ObTransID& trans_id, const uint64_t checksum, const uint64_t cluster_id,
      const ObRedoLogIdArray& redo_log_id_arr, const ObStartTransParam& trans_param, const int64_t log_no,
      const common::ObString& app_trace_id_str, const int64_t checkpoint, const ObElrTransInfoArray& prev_trans_arr,
      const bool can_elr, const common::ObString& app_trace_info);

public:
  uint64_t get_checksum() const
  {
    return checksum_;
  }
  int set_checksum(const uint64_t checksum)
  {
    checksum_ = checksum;
    return common::OB_SUCCESS;
  }
  const ObRedoLogIdArray& get_redo_log_ids() const
  {
    return prev_redo_log_ids_;
  }
  const common::ObString& get_app_trace_id_str() const
  {
    return app_trace_id_str_;
  }
  const common::ObString& get_app_trace_info() const
  {
    return app_trace_info_;
  }
  int64_t get_checkpoint() const
  {
    return checkpoint_;
  }
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(tenant_id), K_(log_no), K_(trans_param), K_(cluster_id),
      K_(active_memstore_version), K_(checksum), K_(prev_redo_log_ids), K_(app_trace_id_str), K_(checkpoint),
      K_(prev_trans_arr), K_(app_trace_info));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransCommitLog);

private:
  // global_trans_version_ is not used anymore.
  // before version 2.2, before submitting sp commit log, global_trans_version
  // will be pre-allocated and recorded in the log body.
  // and it's equal to submit_timestamp recorded in log header, to avoid redundency,
  // this field is not used in version 2.2 and later versions.(it's not removed for compatability reasons)
  int64_t global_trans_version_;
  uint64_t checksum_;
  // If the commit log and the Redo log are in the same log,
  // the redo log ID is not recorded
  ObRedoLogIdArray prev_redo_log_ids_;
  common::ObString app_trace_id_str_;
  int64_t checkpoint_;
  common::ObString app_trace_info_;
};

class ObSpTransAbortLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObSpTransAbortLog() : ObTransLog()
  {}
  ~ObSpTransAbortLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const uint64_t cluster_id);

public:
  bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransAbortLog);
};

class ObCheckpointLog {
  OB_UNIS_VERSION(1);

public:
  ObCheckpointLog() : checkpoint_(0)
  {}
  ~ObCheckpointLog()
  {}
  int init(const int64_t checkpoint);

public:
  int64_t get_checkpoint() const
  {
    return checkpoint_;
  }
  bool is_valid() const;
  TO_STRING_KV(K_(checkpoint));

private:
  int64_t checkpoint_;
};

class ObTransStateLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransStateLog()
      : create_ts_(0),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        trans_expired_time_(0),
        is_readonly_(false),
        trans_type_(TransType::UNKNOWN),
        session_id_(0),
        proxy_session_id_(0),
        commit_task_count_(0),
        schema_version_(0),
        can_elr_(false),
        cluster_version_(0),
        snapshot_version_(0),
        cur_query_start_time_(0),
        stmt_expired_time_(0)
  {}
  ~ObTransStateLog()
  {}
  int init(const int64_t log_type, const common::ObPartitionKey& pkey, const ObTransID& trans_id,
      const common::ObAddr& scheduler, const uint64_t cluster_id, const int64_t tenant_id,
      const int64_t trans_expired_time, const ObStartTransParam& trans_param, const bool is_readonly,
      const int trans_type, const int session_id, const int64_t proxy_session_id, const int64_t commit_task_count,
      const ObTransStmtInfo& stmt_info, const common::ObString app_trace_id_str, const int64_t schema_version,
      const ObElrTransInfoArray& prev_trans_arr, const bool can_elr, const common::ObAddr& proposal_leader,
      const uint64_t cluster_version, const int64_t snapshot_version, const int64_t cur_query_start_time,
      const int64_t stmt_expired_time, const ObXATransID& xid);

public:
  int64_t get_create_ts() const
  {
    return create_ts_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_trans_expired_time() const
  {
    return trans_expired_time_;
  }
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  bool is_readonly() const
  {
    return is_readonly_;
  }
  int get_trans_type() const
  {
    return trans_type_;
  }
  int get_session_id() const
  {
    return session_id_;
  }
  int64_t get_proxy_session_id() const
  {
    return proxy_session_id_;
  }
  int64_t get_commit_task_count() const
  {
    return commit_task_count_;
  }
  const ObTransStmtInfo& get_stmt_info() const
  {
    return stmt_info_;
  }
  const common::ObString& get_app_trace_id_str() const
  {
    return app_trace_id_str_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  const common::ObAddr& get_proposal_leader() const
  {
    return proposal_leader_;
  }
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  int64_t get_cur_query_start_time() const
  {
    return cur_query_start_time_;
  }
  int64_t get_stmt_expired_time() const
  {
    return stmt_expired_time_;
  }
  const ObXATransID& get_xid() const
  {
    return xid_;
  }

public:
  bool is_valid() const
  {
    return true;
  }
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id), K_(create_ts), K_(tenant_id),
      K_(trans_expired_time), K_(trans_param), K_(is_readonly), K_(trans_type), K_(session_id), K_(proxy_session_id),
      K_(commit_task_count), K_(stmt_info), K_(app_trace_id_str), K_(schema_version), K_(prev_trans_arr), K_(can_elr),
      K_(proposal_leader), K_(cluster_version), K_(snapshot_version), K_(cur_query_start_time), K_(stmt_expired_time),
      K_(xid));

private:
  int64_t create_ts_;
  int64_t tenant_id_;
  int64_t trans_expired_time_;
  common::ObAddr scheduler_;
  ObStartTransParam trans_param_;
  bool is_readonly_;
  int trans_type_;
  int session_id_;
  int64_t proxy_session_id_;
  int64_t commit_task_count_;
  ObTransStmtInfo stmt_info_;
  common::ObString app_trace_id_str_;
  int64_t schema_version_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  common::ObAddr proposal_leader_;
  uint64_t cluster_version_;
  int64_t snapshot_version_;
  int64_t cur_query_start_time_;
  int64_t stmt_expired_time_;
  ObXATransID xid_;
};

class ObTransMutatorLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransMutatorLog()
      : ObTransLog(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        trans_expired_time_(0),
        trans_param_(),
        log_no_(0),
        mutator_(),
        prev_trans_arr_(),
        can_elr_(false),
        cluster_version_(0)
  {}
  ~ObTransMutatorLog()
  {
    destroy();
  }
  int init(const int64_t log_type, const common::ObPartitionKey& pkey, const ObTransID& trans_id,
      const uint64_t cluster_id, const int64_t tenant_id, const int64_t trans_expired_time,
      const ObStartTransParam& trans_param, const int64_t log_no, const ObElrTransInfoArray& prev_trans_arr,
      const bool can_elr, const uint64_t cluster_version, const bool use_mutator_buf = true);
  void destroy()
  {}

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_trans_expired_time() const
  {
    return trans_expired_time_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  int64_t get_log_no() const
  {
    return log_no_;
  }
  const ObTransMutator& get_mutator() const
  {
    return mutator_;
  }
  ObTransMutator& get_mutator()
  {
    return mutator_;
  }
  const ObElrTransInfoArray& get_prev_trans_arr() const
  {
    return prev_trans_arr_;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;

public:
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id), K_(tenant_id), K_(trans_expired_time),
      K_(trans_param), K_(log_no), K_(prev_trans_arr), K_(can_elr), K_(cluster_version));

private:
  int64_t tenant_id_;
  int64_t trans_expired_time_;
  ObStartTransParam trans_param_;
  int64_t log_no_;
  ObTransMutator mutator_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  uint64_t cluster_version_;
};

class ObTransMutatorAbortLog : public ObTransLog {
  OB_UNIS_VERSION(1);

public:
  ObTransMutatorAbortLog()
  {}
  ~ObTransMutatorAbortLog()
  {}
  int init(
      const int64_t log_type, const common::ObPartitionKey& pkey, const ObTransID& trans_id, const uint64_t cluster_id);
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(log_type), K_(partition), K_(trans_id), K_(cluster_id));
};

// Provide related methods for log analysis
class ObTransLogParseUtils {
public:
  static int parse_redo_prepare_log(
      const clog::ObLogEntry& log_entry, uint64_t& log_id, int64_t& log_timestamp, ObTransID& trans_id);
};

}  // namespace transaction
}  // namespace oceanbase
#endif  // OCEANBASE_TRANSACTION_OB_TRANS_LOG_
