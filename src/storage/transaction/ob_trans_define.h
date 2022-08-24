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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_
#define OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_

#include <cstdint>
#include "lib/core_local/ob_core_local_storage.h"
#include "lib/trace/ob_trace_event.h"
#include "share/ob_cluster_version.h"
#include "common/ob_partition_key.h"
#include "common/ob_clock_generator.h"
#include "common/ob_range.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace common {
class ObAddr;
class ObPartitionKey;
class ObPartitionLeaderArray;
template <int64_t N>
class ObFixedLengthString;
class ObString;
class ObStoreRowkey;
}  // namespace common

namespace storage {
class ObWarmUpCtx;
class ObIPartitionGroup;
class ObStoreRowLockState;
}  // namespace storage

namespace sql {
class ObBasicSessionInfo;
class ObAuditRecordData;
}  // namespace sql

namespace clog {
class ObAggreBuffer;
}

namespace memtable {
class ObIMvccCtx;
class ObMemtableCtx;
}  // namespace memtable

namespace transaction {
class ObTransCtx;
class ObPartTransCtx;
class ObScheTransCtx;
class ObMemtableKeyInfo;
class ObITransSubmitLogCb;
class AggreLogTask;
class ObPartTransCtxMgr;
class ObPartitionTransCtxMgr;

// The redo log id can use at least 128KB storage space
static int64_t OB_MIN_REDO_LOG_SERIALIZE_SIZE = 131072;

// redo_log + participants + undo_actions can use MAX_VARCHAR_LENGTH-10KB storage space
static int64_t OB_MAX_TRANS_SERIALIZE_SIZE = common::OB_MAX_VARCHAR_LENGTH - 10 * 1024;

// The participants and undo actions share the last storage space
static int64_t OB_MAX_UNDO_ACTION_SERIALIZE_SIZE = OB_MAX_TRANS_SERIALIZE_SIZE - OB_MIN_REDO_LOG_SERIALIZE_SIZE;

struct UnuseUndoSerializeSize {
  UnuseUndoSerializeSize()
  {
    UNUSED(OB_MAX_UNDO_ACTION_SERIALIZE_SIZE);
  }
};

class ObTransErrsim
{
public:
  static inline bool is_memory_errsim()
  {
    bool ret = false;
#ifdef TRANS_MODULE_TEST
    int per = GCONF.module_test_trx_memory_errsim_percentage;
    if (OB_LIKELY(0 == per)) {
      ret = false;
    } else {
      int rand = common::ObRandom::rand(0, 100);
      if (rand < per) {
        ret = true;
      }
    }
#endif
    return ret;
  }
};

struct TransModulePageAllocator : public common::ModulePageAllocator {
  TransModulePageAllocator(const lib::ObLabel& label = common::ObModIds::OB_MODULE_PAGE_ALLOCATOR,
      int64_t tenant_id = common::OB_SERVER_TENANT_ID, int64_t ctx_id = 0)
      : ModulePageAllocator(label, tenant_id, ctx_id)
  {}
  explicit TransModulePageAllocator(common::ObIAllocator& allocator) : ModulePageAllocator(allocator)
  {}
  virtual ~TransModulePageAllocator()
  {}
  void* alloc(const int64_t sz)
  {
    void* ret = NULL;
    if (OB_UNLIKELY(ObTransErrsim::is_memory_errsim())) {
      ret = NULL;
    } else {
      common::ObMemAttr malloc_attr(tenant_id_, label_, ctx_id_);
      ret = ModulePageAllocator::alloc(sz, malloc_attr);
    }
    return ret;
  }
};

// copy construct, operator=, operator== are needed by ob hashmap
class ObTransID final {
  OB_UNIS_VERSION(1);

public:
  ObTransID()
  {
    reset();
  }
  ObTransID(const ObTransID& trans_id);
  explicit ObTransID(const common::ObAddr& server);
  ObTransID(const common::ObAddr& server, const int64_t inc, const int64_t timestamp);

public:
  bool is_valid() const
  {
    return server_.is_valid() && 0 <= inc_ && 0 < timestamp_;
  }
  int parse(const char* trans_id);
  int64_t get_inc_num() const
  {
    return inc_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  const common::ObAddr& get_server() const
  {
    return server_;
  }
  ObTransID& operator=(const ObTransID& trans_id);
  bool operator==(const ObTransID& trans_id) const;
  bool operator!=(const ObTransID& trans_id) const;
  uint64_t hash() const;
  int compare(const ObTransID& other) const;
  const common::ObAddr& get_addr() const
  {
    return server_;
  }
  bool is_sampling() const
  {
    return 0 < timestamp_ && 0 == (timestamp_ % 1000);
  }
  void reset();
  DECLARE_TO_STRING_AND_YSON;

private:
  static const int64_t SAMPLE_INTERVAL = 3 * 1000 * 1000;

private:
  uint64_t hash_() const;

private:
  // static increase number for generating trans id
  static int64_t s_inc_num;
  // last sample timestamp
  static int64_t s_last_sample_ts;

private:
  uint64_t hv_;
  common::ObAddr server_;
  int64_t inc_;
  int64_t timestamp_;
};

struct ObLockForReadArg {
  ObLockForReadArg(memtable::ObMemtableCtx& read_ctx, const int64_t snapshot_version, ObTransID read_trans_id,
      ObTransID data_trans_id, int32_t read_sql_sequence, int32_t data_sql_sequence, bool read_latest)
      : read_ctx_(read_ctx),
        snapshot_version_(snapshot_version),
        read_trans_id_(read_trans_id),
        data_trans_id_(data_trans_id),
        read_sql_sequence_(read_sql_sequence),
        data_sql_sequence_(data_sql_sequence),
        read_latest_(read_latest)
  {}

  DECLARE_TO_STRING;

  memtable::ObMemtableCtx& read_ctx_;
  int64_t snapshot_version_;
  ObTransID read_trans_id_;
  ObTransID data_trans_id_;
  int32_t read_sql_sequence_;
  int32_t data_sql_sequence_;
  bool read_latest_;
};

class ObTransKey final {
public:
  ObTransKey()
  {
    reset();
  }
  ObTransKey(const ObTransKey& other)
  {
    pkey_ = other.pkey_;
    trans_id_ = other.trans_id_;
    hash_val_ = other.hash_val_;
  }
  explicit ObTransKey(const common::ObPartitionKey& pkey, const ObTransID& trans_id)
      : pkey_(pkey), trans_id_(trans_id), hash_val_(0)
  {
    calc_hash_();
  }

public:
  bool is_valid() const
  {
    return pkey_.is_valid() && trans_id_.is_valid();
  }
  ObTransKey& operator=(const ObTransKey& other)
  {
    pkey_ = other.pkey_;
    trans_id_ = other.trans_id_;
    hash_val_ = other.hash_val_;
    return *this;
  }
  bool operator==(const ObTransKey& other) const
  {
    return hash_val_ == other.hash_val_ && pkey_ == other.pkey_ && trans_id_ == other.trans_id_;
  }
  bool operator!=(const ObTransKey& other) const
  {
    return hash_val_ != other.hash_val_ || pkey_ != other.pkey_ || trans_id_ != other.trans_id_;
  }
  OB_INLINE uint64_t hash() const
  {
    return hash_val_;
  }
  int compare(const ObTransKey& other) const
  {
    int compare_ret = 0;
    compare_ret = pkey_.compare(other.pkey_);
    if (0 == compare_ret) {
      compare_ret = trans_id_.compare(other.trans_id_);
    }
    return compare_ret;
  }
  int compare_trans_table(const ObTransKey& other) const
  {
    int compare_ret = 0;
    if (hash_val_ == other.hash_val_) {
      compare_ret = trans_id_.compare(other.trans_id_);
    } else if (hash_val_ > other.hash_val_) {
      compare_ret = 1;
    } else {
      compare_ret = -1;
    }
    return compare_ret;
  }
  void reset()
  {
    pkey_.reset();
    trans_id_.reset();
    hash_val_ = 0;
  }
  const common::ObPartitionKey get_partition_key() const
  {
    return pkey_;
  }
  const ObTransID get_trans_id() const
  {
    return trans_id_;
  }
  DECLARE_TO_STRING_AND_YSON;

private:
  OB_INLINE uint64_t calc_hash_()
  {
    uint64_t hash_val = 0;
    uint64_t trans_id_hash_val = 0;
    hash_val = pkey_.hash();
    trans_id_hash_val = trans_id_.hash();
    hash_val = common::murmurhash(&trans_id_hash_val, sizeof(trans_id_hash_val), hash_val);
    return hash_val_ = hash_val;
  }

private:
  common::ObPartitionKey pkey_;
  ObTransID trans_id_;
  uint64_t hash_val_;
};

class ObTransAccessMode {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t READ_ONLY = 0;
  static const int32_t READ_WRITE = 1;

public:
  static bool is_valid(const int32_t mode)
  {
    return READ_ONLY == mode || READ_WRITE == mode;
  }

private:
  ObTransAccessMode()
  {}
  ~ObTransAccessMode()
  {}
};

class ObTransSnapshotGeneType {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t CONSULT = 0;
  static const int32_t APPOINT = 1;
  static const int32_t NOTHING = 2;

public:
  static bool is_valid(const int32_t type)
  {
    return CONSULT == type || APPOINT == type || NOTHING == type;
  }
  static const char* cstr(int32_t type)
  {
    const char* ret_str = "UNKNOWN";
    switch (type) {
      case CONSULT:
        ret_str = "CONSULT";
        break;

      case APPOINT:
        ret_str = "APPOINT";
        break;

      case NOTHING:
        ret_str = "NOTHING";
        break;

      default:
        ret_str = "UNKNOWN";
        break;
    }
    return ret_str;
  }

public:
  ObTransSnapshotGeneType()
  {}
  ~ObTransSnapshotGeneType()
  {}
};

class TransType {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t SP_TRANS = 0;
  static const int32_t MINI_SP_TRANS = 1;
  static const int32_t DIST_TRANS = 2;

public:
  static bool is_valid(const int32_t type)
  {
    return SP_TRANS == type || MINI_SP_TRANS == type || DIST_TRANS == type;
  }

private:
  TransType()
  {}
  ~TransType()
  {}
};

class ObTransType {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t TRANS_NORMAL = 0;
  static const int32_t TRANS_MAJOR_FREEZE = 1;
  static const int32_t TRANS_SYSTEM = 2;
  static const int32_t TRANS_USER = 3;

public:
  static bool is_valid(const int32_t type)
  {
    return TRANS_NORMAL == type || TRANS_MAJOR_FREEZE == type || TRANS_SYSTEM == type || TRANS_USER == type;
  }

private:
  ObTransType()
  {}
  ~ObTransType()
  {}
};

/*
 *  +--------------------- +-------------------------------------+---------------------------------+
 *  |                      |            CURRENT_READ             |    BOUNDED_STALENESS_READ       |
 *  +--------------------- +-------------------------------------+---------------------------------+
 *  | TRANSACTION_SNAPSHOT | 1. Isolation Level: SERIALIZABLE    | Not Support                     |
 *  |                      | 2. GTS                              |                                 |
 *  |--------------------- +-------------------------------------+---------------------------------+
 *  | STATEMENT_SNAPSHOT   | 1. Isolation Level: READ-COMMITTED  | 1. READ-COMMITTED               |
 *  |                      | 2. GTS                              | 2. monotonic version            |
 *  |                      |                                     | 3. Statement version            |
 *  |                      |                                     | 4. Specified version            |
 *  +--------------------- +-------------------------------------+---------------------------------+
 *  | PARTICIPANT_SNAPSHOT | 1. Isolation Level: READ-COMMITTED  | 1.  READ-COMMITTED              |
 *  |                      | 2. Participant Snaspthot Version    | 2.  Participant version         |
 *  |--------------------- +-------------------------------------+---------------------------------+
 */

class ObTransConsistencyType {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t CURRENT_READ = 0;
  static const int32_t BOUNDED_STALENESS_READ = 1;

public:
  static bool is_valid(const int64_t consistency_type)
  {
    return CURRENT_READ == consistency_type || BOUNDED_STALENESS_READ == consistency_type;
  }
  static bool is_bounded_staleness_read(const int64_t consistency_type)
  {
    return BOUNDED_STALENESS_READ == consistency_type;
  }
  static bool is_current_read(const int64_t consistency_type)
  {
    return CURRENT_READ == consistency_type;
  }
  static const char* cstr(const int64_t consistency_type)
  {
    const char* str = "UNKNOWN";
    switch (consistency_type) {
      case UNKNOWN:
        str = "UNKNOWN";
        break;
      case CURRENT_READ:
        str = "CURRENT_READ";
        break;
      case BOUNDED_STALENESS_READ:
        str = "BOUNDED_STALENESS_READ";
        break;
      default:
        str = "INVALID";
        break;
    }
    return str;
  }

private:
  ObTransConsistencyType()
  {}
  ~ObTransConsistencyType()
  {}
};

class ObTransReadSnapshotType {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t TRANSACTION_SNAPSHOT = 0;
  static const int32_t STATEMENT_SNAPSHOT = 1;
  static const int32_t PARTICIPANT_SNAPSHOT = 2;

public:
  static bool is_valid(const int32_t type)
  {
    return TRANSACTION_SNAPSHOT == type || STATEMENT_SNAPSHOT == type || PARTICIPANT_SNAPSHOT == type;
  }
  static bool is_consistent_snapshot(const int32_t type)
  {
    return TRANSACTION_SNAPSHOT == type || STATEMENT_SNAPSHOT == type;
  }
  static const char* cstr(const int64_t type)
  {
    const char* str = "UNKNOWN";
    switch (type) {
      case UNKNOWN:
        str = "UNKNOWN";
        break;
      case TRANSACTION_SNAPSHOT:
        str = "TRANSACTION_SNAPSHOT";
        break;
      case STATEMENT_SNAPSHOT:
        str = "STATEMENT_SNAPSHOT";
        break;
      case PARTICIPANT_SNAPSHOT:
        str = "PARTICIPANT_SNAPSHOT";
        break;
      default:
        str = "INVALID";
        break;
    }
    return str;
  }
};

class ObStmtParam {
public:
  ObStmtParam()
  {
    reset();
  }
  ~ObStmtParam()
  {}
  void reset();

public:
  int init(const uint64_t tenant_id, const int64_t stmt_expired_time, const bool is_retry_sql);
  int init(const uint64_t tenant_id, const int64_t stmt_expired_time, const bool is_retry_sql,
      const int64_t safe_weak_read_snapshot, const int64_t weak_read_snapshot_source, const bool trx_elr);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_stmt_expired_time() const
  {
    return stmt_expired_time_;
  }
  bool is_retry_sql() const
  {
    return is_retry_sql_;
  }
  int64_t get_safe_weak_read_snapshot() const
  {
    return safe_weak_read_snapshot_;
  }
  int64_t get_safe_weak_read_snapshot_source() const
  {
    return weak_read_snapshot_source_;
  }
  bool is_trx_elr() const
  {
    return trx_elr_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  uint64_t tenant_id_;
  int64_t stmt_expired_time_;
  bool is_retry_sql_;
  bool trx_elr_;
  int64_t safe_weak_read_snapshot_;
  int64_t weak_read_snapshot_source_;
};

// transaction parameter
class ObStartTransParam {
  OB_UNIS_VERSION(1);

public:
  const static uint64_t INVALID_CLUSTER_VERSION = 0;

public:
  ObStartTransParam()
  {
    reset();
  }
  ~ObStartTransParam()
  {
    magic_ = INVALID_MAGIC_NUM;
  }
  void reset();

public:
  int set_access_mode(const int32_t access_mode);
  int32_t get_access_mode() const
  {
    return access_mode_;
  }
  int set_type(const int32_t type);
  int32_t get_type() const
  {
    return type_;
  }
  int set_isolation(const int32_t isolation);
  int32_t get_isolation() const
  {
    return isolation_;
  }
  void set_autocommit(const bool autocommit)
  {
    autocommit_ = autocommit;
  }
  bool is_autocommit() const
  {
    return autocommit_;
  }
  void set_consistency_type(const int32_t consistency_type)
  {
    consistency_type_ = consistency_type;
  }
  int32_t get_consistency_type() const
  {
    return consistency_type_;
  }
  bool is_bounded_staleness_read() const
  {
    return ObTransConsistencyType::is_bounded_staleness_read(consistency_type_);
  }
  bool is_current_read() const
  {
    return ObTransConsistencyType::is_current_read(consistency_type_);
  }
  void set_read_snapshot_type(const int32_t type)
  {
    read_snapshot_type_ = type;
  }
  int32_t get_read_snapshot_type() const
  {
    return read_snapshot_type_;
  }
  void set_cluster_version(uint64_t cluster_version)
  {
    cluster_version_ = cluster_version;
  }
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  void set_inner_trans(const bool is_inner_trans)
  {
    is_inner_trans_ = is_inner_trans;
  }
  bool is_inner_trans() const
  {
    return is_inner_trans_;
  }
  bool need_consistent_snapshot() const
  {
    return ObTransReadSnapshotType::is_consistent_snapshot(read_snapshot_type_);
  }
  bool need_strong_consistent_snapshot() const
  {
    return ObTransConsistencyType::CURRENT_READ == consistency_type_ && need_consistent_snapshot();
  }
  bool is_serializable_isolation() const;
  int reset_read_snapshot_type_for_isolation();

public:
  bool is_readonly() const
  {
    return ObTransAccessMode::READ_ONLY == access_mode_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  static const uint64_t MAGIC_NUM = 0xF0F0F0F0F0F0F0F0;
  static const uint64_t INVALID_MAGIC_NUM = 0x4348454E4D494E47;

private:
  // READONLY or READ_WRITE
  int32_t access_mode_;
  // transaction type: TRANS_NORMAL
  int32_t type_;
  // isolation level: READ-COMMITTED
  int32_t isolation_;
  uint64_t magic_;
  bool autocommit_;
  int32_t consistency_type_;    // ObTransConsistencyType
  int32_t read_snapshot_type_;  // ObTransReadSnapshotType
  uint64_t cluster_version_;
  bool is_inner_trans_;
};

// record the info from user to db
class ObTraceInfo {
public:
  ObTraceInfo()
  {
    reset();
  }
  ~ObTraceInfo()
  {}
  void reset();
  void reuse();
  int init();
  // app trace info
  int set_app_trace_info(const common::ObString& app_trace_info);
  const common::ObString& get_app_trace_info() const
  {
    return app_trace_info_;
  }
  // app trace id
  int set_app_trace_id(const common::ObString& app_trace_id);
  const common::ObString& get_app_trace_id() const
  {
    return app_trace_id_;
  }
  common::ObString& get_app_trace_id()
  {
    return app_trace_id_;
  }
  TO_STRING_KV(K_(app_trace_info), K_(app_trace_id));

private:
  static const int64_t MAX_TRACE_INFO_BUFFER = 128;

private:
  char app_trace_info_buffer_[MAX_TRACE_INFO_BUFFER + 1];
  common::ObString app_trace_info_;
  char app_trace_id_buffer_[common::OB_MAX_TRACE_ID_BUFFER_SIZE + 1];
  common::ObString app_trace_id_;
};

class ObTransConsistencyLevel {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t STRONG = 0;
  static const int64_t WEAK = 1;

public:
  static bool is_valid(const int64_t type)
  {
    return STRONG == type || WEAK == type;
  }

private:
  ObTransConsistencyLevel()
  {}
  ~ObTransConsistencyLevel()
  {}
};

struct ObStmtDesc {
  OB_UNIS_VERSION(1);

public:
  ObStmtDesc()
  {
    reset();
  }
  ~ObStmtDesc()
  {}
  void reset();

public:
  bool is_valid() const
  {
    // no need to check execution_id and stmt_tenant_id for compatibility
    return (sql::OB_PHY_PLAN_LOCAL == phy_plan_type_ || sql::OB_PHY_PLAN_REMOTE == phy_plan_type_ ||
               sql::OB_PHY_PLAN_DISTRIBUTED == phy_plan_type_ || sql::OB_PHY_PLAN_UNCERTAIN == phy_plan_type_) &&
           (stmt_type_ > sql::stmt::T_NONE && stmt_type_ < sql::stmt::T_STMT_TYPE_MAX) &&
           (ObTransConsistencyLevel::is_valid(consistency_level_));
  }
  bool is_strong_consistency() const
  {
    return ObTransConsistencyLevel::STRONG == consistency_level_;
  }
  bool has_valid_read_snapshot() const
  {
    return cur_stmt_specified_snapshot_version_ > 0;
  }
  bool is_readonly_stmt() const
  {
    return ((sql::stmt::T_SELECT == stmt_type_ && !is_sfu_) || sql::stmt::T_BUILD_INDEX_SSTABLE == stmt_type_);
  }
  bool is_sfu() const
  {
    return is_sfu_;
  }
  sql::stmt::StmtType get_stmt_type() const
  {
    return stmt_type_;
  }
  inline bool is_delete_stmt() const
  {
    return sql::stmt::T_DELETE == stmt_type_;
  }
  const char *get_sql_id() const
  {
    return sql_id_.ptr();
  }

public:
  ObStmtDesc& operator=(const ObStmtDesc& stmt_desc);
  int64_t to_string(char* buf, const int64_t buf_len) const;

public:
  static const int64_t SQL_ID_LENGTH = 16;
  static const int64_t TRACE_ID_LENGTH = 35;

public:
  // local, remote, distributed
  sql::ObPhyPlanType phy_plan_type_;
  // INSERT, UPDATE ...
  sql::stmt::StmtType stmt_type_;
  // STRONG, WEAK
  int64_t consistency_level_;
  int64_t execution_id_;
  common::ObFixedLengthString<SQL_ID_LENGTH> sql_id_;
  common::ObFixedLengthString<TRACE_ID_LENGTH> trace_id_;
  bool inner_sql_;
  char buffer_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  common::ObString app_trace_id_str_;
  bool nested_sql_;
  common::ObTraceIdAdaptor trace_id_adaptor_;
  uint64_t stmt_tenant_id_;
  bool is_sfu_;
  bool is_contain_inner_table_;
  // no need to serialize
  int64_t cur_stmt_specified_snapshot_version_;
  int64_t cur_query_start_time_;
  int64_t trx_lock_timeout_;
};

// no need to serialize
class ObPartitionSchemaInfo {
public:
  ObPartitionSchemaInfo()
  {
    reset();
  }
  ~ObPartitionSchemaInfo()
  {}
  void reset();
  int init(const common::ObPartitionKey& pkey, const int64_t schema_version);
  bool is_valid() const
  {
    return pkey_.is_valid() && schema_version_ > 0;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return pkey_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  TO_STRING_KV(K_(pkey), K_(schema_version));

private:
  common::ObPartitionKey pkey_;
  int64_t schema_version_;
};

typedef common::ObSEArray<ObPartitionSchemaInfo, 10, TransModulePageAllocator> ObPartitionSchemaInfoArray;

class ObTransTraceLog : public common::ObTraceEventRecorderBase<100, 2250> {
public:
  ObTransTraceLog()
      : common::ObTraceEventRecorderBase<100, 2250>::ObTraceEventRecorderBase(
            true, common::ObLatchIds::TRANS_TRACE_RECORDER_LOCK)
  {}
  ~ObTransTraceLog()
  {}
  void destroy()
  {}
};

class ObStmtInfo {
  OB_UNIS_VERSION(1);

public:
  ObStmtInfo()
  {
    reset();
  }
  ~ObStmtInfo()
  {}
  void reset();
  int set_nested_sql()
  {
    nested_sql_ = true;
    return common::OB_SUCCESS;
  }
  bool is_nested_sql() const
  {
    return nested_sql_;
  }
  int inc_start_stmt_cnt()
  {
    start_stmt_cnt_++;
    return common::OB_SUCCESS;
  }
  int64_t get_start_stmt_cnt() const
  {
    return start_stmt_cnt_;
  }
  int inc_end_stmt_cnt()
  {
    end_stmt_cnt_++;
    return common::OB_SUCCESS;
  }
  int64_t get_end_stmt_cnt() const
  {
    return end_stmt_cnt_;
  }
  void reset_stmt_info();
  TO_STRING_KV(K_(nested_sql), K_(start_stmt_cnt), K_(end_stmt_cnt));

private:
  bool nested_sql_;
  int64_t start_stmt_cnt_;
  int64_t end_stmt_cnt_;
};

class ObTaskInfo {
  OB_UNIS_VERSION(1);

public:
  ObTaskInfo() : sql_no_(0), active_task_cnt_(1), snapshot_version_(common::OB_INVALID_VERSION)
  {}
  ObTaskInfo(const int32_t sql_no, const int64_t snapshot_version)
      : sql_no_(sql_no), active_task_cnt_(1), snapshot_version_(snapshot_version)
  {}
  bool is_task_match() const
  {
    return 0 == active_task_cnt_;
  }
  TO_STRING_KV(K_(sql_no), K_(active_task_cnt), K_(snapshot_version));

public:
  int32_t sql_no_;
  int32_t active_task_cnt_;
  int64_t snapshot_version_;
};

class ObTransTaskInfo {
  OB_UNIS_VERSION(1);

public:
  int start_stmt(const int32_t sql_no);
  int start_task(const int32_t sql_no, const int64_t snapshot_version);
  void end_task();
  bool is_task_match(const int32_t sql_no) const;
  void remove_task_after(const int32_t sql_no);
  bool is_stmt_ended() const
  {
    return tasks_.empty();
  }
  int64_t get_top_snapshot_version() const;
  void reset()
  {
    tasks_.reset();
  }
  TO_STRING_KV(K(tasks_));

private:
  common::ObSEArray<ObTaskInfo, 1> tasks_;
};

class ObTransStmtInfo {
  OB_UNIS_VERSION(1);

public:
  ObTransStmtInfo()
  {
    reset();
  }
  ~ObTransStmtInfo()
  {}
  void reset();
  int start_stmt(const int64_t sql_no)
  {
    return task_info_.start_stmt(sql_no);
  }
  void end_stmt(const int64_t sql_no)
  {
    update_sql_no(sql_no);
    need_rollback_ = true;
    task_info_.remove_task_after(0);
  }
  void rollback_to(const int64_t from, const int64_t to)
  {
    update_sql_no(from);
    need_rollback_ = true;
    task_info_.remove_task_after(to);
  }
  int start_task(const int64_t sql_no, const int64_t snapshot_version)
  {
    update_sql_no(sql_no);
    return task_info_.start_task(sql_no, snapshot_version);
  }

  void end_task()
  {
    return task_info_.end_task();
  }
  int64_t get_top_snapshot_version() const
  {
    return task_info_.get_top_snapshot_version();
  }
  bool is_stmt_ended() const
  {
    return task_info_.is_stmt_ended();
  }
  bool is_task_match(const int64_t sql_no = 0) const
  {
    return task_info_.is_task_match(sql_no);
  }
  bool stmt_expired(const int64_t sql_no, const bool is_rollback = false) const
  {
    return sql_no < sql_no_ || (!is_rollback && (need_rollback_ && sql_no == sql_no_));
  }
  bool stmt_matched(const int64_t sql_no) const
  {
    return sql_no == sql_no_ && !need_rollback_;
  }
  bool main_stmt_change(const int64_t sql_no) const
  {
    return (sql_no >> 32) > main_sql_no_;
  }
  bool main_stmt_change_compat(const int64_t sql_no) const
  {
    return sql_no > sql_no_;
  }
  int64_t get_sql_no() const
  {
    return sql_no_;
  }

  TO_STRING_KV(K_(sql_no), K_(start_task_cnt), K_(end_task_cnt), K_(need_rollback), K_(task_info));

private:
  void update_sql_no(const int64_t sql_no)
  {
    if (sql_no > sql_no_) {
      sql_no_ = sql_no;
      need_rollback_ = false;
    }
  }

  union {
    struct {
      int32_t sub_sql_no_;
      int32_t main_sql_no_;
    };
    int64_t sql_no_;
  };
  int64_t start_task_cnt_;  // compat only
  int64_t end_task_cnt_;    // compat only
  bool need_rollback_;
  ObTransTaskInfo task_info_;
};

class ObTransVersion {
public:
  static const int64_t INVALID_TRANS_VERSION = -1;

public:
  static bool is_valid(const int64_t trans_version)
  {
    return trans_version >= 0;
  }

private:
  ObTransVersion()
  {}
  ~ObTransVersion()
  {}
};

typedef struct MonotonicTs {
  OB_UNIS_VERSION(1);

public:
  explicit MonotonicTs(int64_t mts) : mts_(mts)
  {}
  MonotonicTs()
  {
    reset();
  }
  ~MonotonicTs()
  {
    reset();
  }
  void reset()
  {
    mts_ = 0;
  }
  bool is_valid() const
  {
    return mts_ > 0;
  }
  bool operator!=(const struct MonotonicTs other) const
  {
    return mts_ != other.mts_;
  }
  bool operator==(const struct MonotonicTs other) const
  {
    return mts_ == other.mts_;
  }
  bool operator>(const struct MonotonicTs other) const
  {
    return mts_ > other.mts_;
  }
  bool operator>=(const struct MonotonicTs other) const
  {
    return mts_ >= other.mts_;
  }
  bool operator<(const struct MonotonicTs other) const
  {
    return mts_ < other.mts_;
  }
  bool operator<=(const struct MonotonicTs other) const
  {
    return mts_ <= other.mts_;
  }
  struct MonotonicTs operator+(const struct MonotonicTs other) const
  {
    return MonotonicTs(mts_ + other.mts_);
  }
  struct MonotonicTs operator-(const struct MonotonicTs other) const
  {
    return MonotonicTs(mts_ - other.mts_);
  }
  static struct MonotonicTs current_time()
  {
    return MonotonicTs(common::ObTimeUtility::current_time());
  }
  TO_STRING_KV(K_(mts));
  int64_t mts_;
} MonotonicTs;

class ObTransNeedWaitWrap {
public:
  ObTransNeedWaitWrap()
  {
    reset();
  }
  ~ObTransNeedWaitWrap()
  {
    destroy();
  }
  void reset()
  {
    receive_gts_ts_.reset();
    need_wait_interval_us_ = 0;
  }
  void destroy()
  {
    reset();
  }
  int64_t get_remaining_wait_interval_us() const;
  void set_trans_need_wait_wrap(const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us);
  MonotonicTs get_receive_gts_ts() const
  {
    return receive_gts_ts_;
  }
  int64_t get_need_wait_interval_us() const
  {
    return need_wait_interval_us_;
  }
  bool need_wait() const
  {
    return get_remaining_wait_interval_us() > 0;
  }
  TO_STRING_KV(K_(receive_gts_ts), K_(need_wait_interval_us));

private:
  MonotonicTs receive_gts_ts_;
  int64_t need_wait_interval_us_;
};

// savepoint identifier, max length is 128
typedef common::ObFixedLengthString<128> ObSavepointIdString;

class ObSavepointInfo {
public:
  ObSavepointInfo()
  {
    reset();
  }
  ~ObSavepointInfo()
  {
    destroy();
  }
  int init(const common::ObString& id, const int64_t sql_no);
  const ObSavepointIdString& get_id() const
  {
    return id_;
  }
  int64_t get_sql_no() const
  {
    return sql_no_;
  }
  int64_t get_id_len() const
  {
    return id_len_;
  }
  void reset();
  void destroy()
  {
    reset();
  }
  TO_STRING_KV(K_(id), K_(id_len), K_(sql_no));

private:
  int64_t sql_no_;
  ObSavepointIdString id_;
  int64_t id_len_;
};

class ObSavepointPartitionInfo {
public:
  ObSavepointPartitionInfo()
  {
    reset();
  }
  ~ObSavepointPartitionInfo()
  {
    destroy();
  }
  int init(const common::ObPartitionKey& partition, const int64_t max_sql_no);
  void reset()
  {
    partition_.reset();
    max_sql_no_ = -1;
  }
  void destroy()
  {
    reset();
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int64_t get_max_sql_no() const
  {
    return max_sql_no_;
  }
  int update_max_sql_no(const int64_t sql_no);
  TO_STRING_KV(K_(partition), K_(max_sql_no));

private:
  common::ObPartitionKey partition_;
  int64_t max_sql_no_;
};

typedef common::ObSEArray<ObSavepointInfo, 1, TransModulePageAllocator> ObSavepointInfoArray;
typedef common::ObSEArray<ObSavepointPartitionInfo, 1, TransModulePageAllocator> ObSavepointPartitionInfoArray;

class ObSavepointMgr {
public:
  ObSavepointMgr()
  {
    reset();
  }
  ~ObSavepointMgr()
  {
    destroy();
  }
  void reset()
  {
    savepoint_arr_.reset();
    savepoint_partition_arr_.reset();
  }
  void destroy()
  {
    reset();
  }
  int deep_copy(const ObSavepointMgr& sp_mgr);
  int push_savepoint(const common::ObString& id, const int64_t sql_no);
  int truncate_savepoint(const common::ObString& id);
  int del_savepoint(const common::ObString& id);
  int update_savepoint_partition_info(const common::ObPartitionArray& participants, const int64_t sql_no);
  int get_savepoint_rollback_info(const common::ObString& id, int64_t& sql_no, common::ObPartitionArray& partition_arr);
  TO_STRING_KV(K_(savepoint_arr), K_(savepoint_partition_arr));

private:
  int64_t find_savepoint_location_(const common::ObString& id);
  int64_t find_savepoint_partition_location_(const common::ObPartitionKey& partition);
  int find_savepoint_rollback_partitions_(const int64_t sql_no, common::ObPartitionArray& partition_arr);
  int get_savepoint_rollback_partitions_(const int64_t sql_no, common::ObPartitionArray& partition_arr);

protected:
  ObSavepointInfoArray savepoint_arr_;
  ObSavepointPartitionInfoArray savepoint_partition_arr_;
};

class ObTransSnapInfo {
  OB_UNIS_VERSION(1);

public:
  ObTransSnapInfo()
  {
    reset();
  }
  ~ObTransSnapInfo()
  {
    destroy();
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  void set_snapshot_version(const int64_t snapshot_version)
  {
    snapshot_version_ = snapshot_version;
  }
  void set_read_sql_no(const int64_t sql_no)
  {
    read_sql_no_ = sql_no_ = sql_no;
  }
  int64_t get_read_sql_no() const
  {
    return read_sql_no_;
  }
  void set_trans_id(const ObTransID& trans_id)
  {
    trans_id_ = trans_id;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  void set_is_cursor_or_nested(const bool is_cursor_or_nested)
  {
    is_cursor_or_nested_ = is_cursor_or_nested;
  }
  bool is_cursor_or_nested() const
  {
    return is_cursor_or_nested_;
  }
  void reset()
  {
    snapshot_version_ = common::OB_INVALID_VERSION;
    read_sql_no_ = 0;
    sql_no_ = 0;
    trans_id_.reset();
    is_cursor_or_nested_ = false;
  }
  void destroy()
  {
    reset();
  }

public:
  bool is_valid() const;
  TO_STRING_KV(K_(snapshot_version), K_(read_sql_no), K_(sql_no), K_(trans_id), K_(is_cursor_or_nested));

private:
  int64_t snapshot_version_;
  int64_t read_sql_no_;
  int64_t sql_no_;  // deprecated, for compact reason only
  ObTransID trans_id_;
  bool is_cursor_or_nested_;
};

class ObStmtPair {
  OB_UNIS_VERSION(1);

public:
  ObStmtPair() : from_(0), to_(0)
  {}
  ObStmtPair(const int64_t from, const int64_t to) : from_(from), to_(to)
  {}
  ~ObStmtPair()
  {}
  bool is_valid() const
  {
    return (from_ > to_) && (to_ >= 0);
  }
  int64_t get_from() const
  {
    return from_;
  }
  int64_t& get_from()
  {
    return from_;
  }
  int64_t get_to() const
  {
    return to_;
  }
  TO_STRING_KV(K_(from), K_(to));

private:
  int64_t from_;
  int64_t to_;
};

class ObStmtRollbackInfo {
  OB_UNIS_VERSION(1);

public:
  ObStmtRollbackInfo()
  {}
  ~ObStmtRollbackInfo()
  {}
  void reset();

public:
  int push(const ObStmtPair& stmt_pair);
  int search(const int64_t sql_no, ObStmtPair& stmt_pair) const;
  int assign(const ObStmtRollbackInfo& other);
  TO_STRING_KV(K_(stmt_pair_array));

private:
  common::ObSEArray<ObStmtPair, 16> stmt_pair_array_;
};

class ObTransDesc;

class ObStandaloneStmtDesc {
  friend class ObTransDesc;

public:
  ObStandaloneStmtDesc()
  {
    reset();
  }
  ~ObStandaloneStmtDesc()
  {
    destroy();
  }
  int init(const common::ObAddr& addr, const uint64_t tenant_id, const int64_t stmt_expired_time,
      const int64_t trx_lock_timeout, const bool is_local_single_partition, const int32_t consistency_type,
      const int32_t read_snapshot_type, const common::ObPartitionKey& first_pkey);
  void reset();
  void destroy()
  {
    reset();
  }
  bool is_valid() const;
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_stmt_expired_time() const
  {
    return stmt_expired_time_;
  }
  int64_t get_trx_lock_timeout() const
  {
    return trx_lock_timeout_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  void set_snapshot_version(const int64_t snapshot_version)
  {
    snapshot_version_ = snapshot_version;
  }
  bool is_bounded_staleness_read() const
  {
    return consistency_type_ == ObTransConsistencyType::BOUNDED_STALENESS_READ;
  }
  bool is_current_read() const
  {
    return consistency_type_ == ObTransConsistencyType::CURRENT_READ;
  }
  bool is_transaction_snapshot() const
  {
    return read_snapshot_type_ == ObTransReadSnapshotType::TRANSACTION_SNAPSHOT;
  }
  bool is_statement_snapshot() const
  {
    return read_snapshot_type_ == ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
  }
  bool is_stmt_timeout() const
  {
    return common::ObClockGenerator::getClock() >= stmt_expired_time_;
  }
  bool is_snapshot_version_valid() const
  {
    return snapshot_version_ > 0;
  }
  void set_standalone_stmt_end()
  {
    is_standalone_stmt_end_ = true;
  }
  void set_local_single_partition_stmt()
  {
    is_local_single_partition_ = true;
  }
  bool is_local_single_partition_stmt() const
  {
    return is_local_single_partition_;
  }
  int stmt_deep_copy(const ObStandaloneStmtDesc& desc);
  const common::ObPartitionKey& get_first_pkey() const
  {
    return first_pkey_;
  }
  TO_STRING_KV(K_(trans_id), K_(tenant_id), K_(stmt_expired_time), K_(trx_lock_timeout), K_(consistency_type),
      K_(read_snapshot_type), K_(snapshot_version), K_(is_local_single_partition), K_(is_standalone_stmt_end),
      K_(first_pkey));

private:
  // for standalone transaction
  uint64_t magic_;
  ObTransID trans_id_;
  uint64_t tenant_id_;
  int64_t stmt_expired_time_;
  int64_t trx_lock_timeout_;
  // for compatibility
  common::ObPartitionLeaderArray pla_;
  int32_t consistency_type_;
  int32_t read_snapshot_type_;
  int64_t snapshot_version_;
  bool is_local_single_partition_;
  bool is_standalone_stmt_end_;
  // for performance
  common::ObPartitionKey first_pkey_;
};

enum ObXAReqType {
  XA_START = 1,
  XA_END,
  XA_PREPARE,
  XA_COMMIT,
  XA_ROLLBACK,
};

class ObXATransID {
  OB_UNIS_VERSION(1);

public:
  ObXATransID()
  {
    reset();
  }
  ObXATransID(const ObXATransID& xid);
  ~ObXATransID()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int set(const common::ObString& gtrid_str, const common::ObString& bqual_str, const int64_t format_id);
  int set(const ObXATransID& xid);
  const common::ObString& get_gtrid_str() const
  {
    return gtrid_str_;
  }
  const common::ObString& get_bqual_str() const
  {
    return bqual_str_;
  }
  int64_t get_format_id() const
  {
    return format_id_;
  }
  bool empty() const;
  bool is_valid() const;
  ObXATransID& operator=(const ObXATransID& xid);
  bool operator==(const ObXATransID& xid) const;
  bool operator!=(const ObXATransID& xid) const;
  int32_t to_full_xid_string(char* buffer, const int64_t capacity) const;
  bool all_equal_to(const ObXATransID& other) const;
  TO_STRING_KV(K_(gtrid_str), K_(bqual_str), K_(format_id), KPHEX(gtrid_str_.ptr(), gtrid_str_.length()),
      KPHEX(bqual_str_.ptr(), bqual_str_.length()));

public:
  static const int32_t MAX_GTRID_LENGTH = 64;
  static const int32_t MAX_BQUAL_LENGTH = 64;
  static const int32_t MAX_XID_LENGTH = MAX_GTRID_LENGTH + MAX_BQUAL_LENGTH;

private:
  char gtrid_buf_[MAX_GTRID_LENGTH];
  common::ObString gtrid_str_;
  char bqual_buf_[MAX_BQUAL_LENGTH];
  common::ObString bqual_str_;
  int64_t format_id_;
};

class ObTransDesc : public ObSavepointMgr {
  OB_UNIS_VERSION_V(1);

public:
  ObTransDesc();
  ~ObTransDesc()
  {
    destroy();
  }
  void reset();
  void destroy();

public:
  int init(sql::ObBasicSessionInfo* session);
  // for test
  int test_init();
  int set_trans_id(const ObTransID& trans_id);
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int set_snapshot_version(const int64_t version);
  void reset_snapshot_version()
  {
    snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  void set_trans_snapshot_version(const int64_t version)
  {
    trans_snapshot_version_ = version;
  }
  int64_t get_trans_snapshot_version() const
  {
    return trans_snapshot_version_;
  }
  void set_commit_version(const int64_t version)
  {
    commit_version_ = version;
  }
  int64_t get_commit_version() const
  {
    return commit_version_;
  }
  int set_trans_param(const ObStartTransParam& trans_param);
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  ObStartTransParam& get_trans_param()
  {
    return trans_param_;
  }
  int set_scheduler(const common::ObAddr& scheduler);
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  int set_orig_scheduler(const common::ObAddr& scheduler);
  const common::ObAddr& get_orig_scheduler() const
  {
    return orig_scheduler_;
  }
  int set_stmt_participants(const common::ObPartitionArray& stmt_participants);
  int merge_stmt_participants(const common::ObPartitionArray& stmt_participants);
  int set_stmt_participants_pla(const common::ObPartitionLeaderArray& stmt_participants_pla);
  int merge_stmt_participants_pla(const common::ObPartitionLeaderArray& stmt_participants_pla);
  const common::ObPartitionArray& get_stmt_participants() const
  {
    return stmt_participants_;
  }
  int set_gc_participants(const common::ObPartitionArray& stmt_participants);
  int merge_gc_participants(const common::ObPartitionArray& stmt_participants);
  const common::ObPartitionArray& get_gc_participants() const
  {
    return gc_participants_;
  }
  void clear_stmt_participants()
  {
    stmt_participants_.reset();
  }
  void clear_stmt_participants_pla()
  {
    stmt_participants_pla_.reset();
  }
  const common::ObPartitionLeaderArray& get_stmt_participants_pla() const
  {
    return stmt_participants_pla_;
  }
  int merge_participants();
  int merge_participants_pla();
  int merge_participants(const common::ObPartitionArray& participants);
  int merge_participants_pla(const common::ObPartitionLeaderArray& participant_pla);
  int check_participants_size();
  const common::ObPartitionArray& get_participants() const
  {
    return participants_;
  }
  const common::ObPartitionLeaderArray& get_participants_pla() const
  {
    return participants_pla_;
  }
  int set_cur_stmt_desc(const ObStmtDesc& stmt_desc);
  const ObStmtDesc& get_cur_stmt_desc() const
  {
    return cur_stmt_desc_;
  }
  ObStmtDesc& get_cur_stmt_desc()
  {
    return cur_stmt_desc_;
  }
  void start_main_stmt()
  {
    inc_sql_no();
    stmt_min_sql_no_ = sql_no_;
  }
  void start_nested_stmt()
  {
    inc_sub_sql_no();
    stmt_min_sql_no_ = sql_no_;
    is_nested_stmt_ = true;
  }
  bool is_nested_stmt() const
  {
    return is_nested_stmt_;
  }
  void inc_sql_no()
  {
    max_sql_no_ = MAX(max_sql_no_, sql_no_);
    sql_no_ = max_sql_no_ = max_sql_no_ + 0x100000001;
  }
  void inc_sub_sql_no()
  {
    sql_no_ = ++max_sql_no_;
  }
  int64_t inc_savepoint();
  int64_t get_sql_no() const
  {
    return sql_no_;
  }
  int64_t get_sub_sql_no() const
  {
    return sql_no_ & 0xffffffff;
  }
  int64_t get_stmt_min_sql_no() const
  {
    return cluster_version_before_2271() ? sql_no_ : stmt_min_sql_no_;
  }
  void set_stmt_min_sql_no(const int64_t sql_no)
  {
    stmt_min_sql_no_ = sql_no;
  }
  int set_sql_no(const int64_t sql_no)
  {
    sql_no_ = sql_no;
    return common::OB_SUCCESS;
  }
  bool need_rollback() const
  {
    return need_rollback_;
  }
  void set_need_rollback()
  {
    need_rollback_ = true;
  }
  int set_trans_expired_time(const int64_t expired_time);
  int64_t get_trans_expired_time() const
  {
    return trans_expired_time_;
  }
  int set_cur_stmt_expired_time(const int64_t expired_time);
  int64_t get_cur_stmt_expired_time() const
  {
    return cur_stmt_expired_time_;
  }
  bool is_valid() const;
  bool is_valid_or_standalone_stmt() const;
  bool has_create_ctx(const common::ObPartitionKey& pkey) const;
  bool has_create_ctx(const common::ObPartitionKey& pkey, const common::ObAddr& addr) const;
  bool is_trans_timeout() const
  {
    return common::ObClockGenerator::getClock() >= trans_expired_time_;
  }
  bool is_stmt_timeout() const
  {
    return common::ObClockGenerator::getClock() >= cur_stmt_expired_time_;
  }
  bool is_readonly() const
  {
    return trans_param_.is_readonly();
  }
  bool is_autocommit() const
  {
    return trans_param_.is_autocommit();
  }
  int set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int set_part_ctx(ObTransCtx* part_ctx)
  {
    part_ctx_ = part_ctx;
    return common::OB_SUCCESS;
  }
  ObTransCtx* get_part_ctx()
  {
    return part_ctx_;
  }
  int set_sche_ctx(ObScheTransCtx* sche_ctx);
  ObScheTransCtx* get_sche_ctx()
  {
    return sche_ctx_;
  }
  storage::ObWarmUpCtx* get_warm_up_ctx() const
  {
    return warm_up_ctx_;
  }
  void set_warm_up_ctx(storage::ObWarmUpCtx* warm_up_ctx)
  {
    warm_up_ctx_ = warm_up_ctx;
  }
  void set_trans_type(const int32_t trans_type)
  {
    trans_type_ = trans_type;
  }
  int32_t get_trans_type() const
  {
    return trans_type_;
  }
  bool is_sp_trans() const
  {
    return (trans_type_ == TransType::SP_TRANS);
  }
  bool is_mini_sp_trans() const
  {
    return (trans_type_ == TransType::MINI_SP_TRANS);
  }
  bool is_sp_trans_exiting() const
  {
    return is_sp_trans_exiting_;
  }
  void set_sp_trans_exiting()
  {
    is_sp_trans_exiting_ = true;
  }
  int set_cluster_id(const uint64_t cluster_id);
  uint64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  int set_readonly()
  {
    return trans_param_.set_access_mode(ObTransAccessMode::READ_ONLY);
  }
  int set_cluster_version(const uint64_t cluster_version);
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  void set_all_select_stmt(const bool is_all_select_stmt)
  {
    is_all_select_stmt_ = is_all_select_stmt;
  }
  bool is_all_select_stmt() const
  {
    return is_all_select_stmt_;
  }
  int set_trans_consistency_type(const int32_t trans_consistency_type);
  bool is_identical_consistency_type(const int32_t consistency_type)
  {
    return local_consistency_type_ == consistency_type;
  }
  bool is_bounded_staleness_read() const
  {
    return trans_param_.is_bounded_staleness_read();
  }
  bool is_current_read() const
  {
    return trans_param_.is_current_read();
  }
  void set_trans_end()
  {
    trans_end_ = true;
  }
  bool is_trans_end() const
  {
    return trans_end_;
  }
  bool test_and_set_trans_end()
  {
    return ATOMIC_BCAS(&trans_end_, false, true);
  }
  bool is_inner_sql() const
  {
    return cur_stmt_desc_.inner_sql_;
  }
  bool is_inner_trans() const
  {
    return trans_param_.is_inner_trans();
  }
  void set_snapshot_gene_type(const int type)
  {
    snapshot_gene_type_ = type;
  }
  int32_t get_snapshot_gene_type() const
  {
    return snapshot_gene_type_;
  }
  void reset_snapshot_gene_type()
  {
    snapshot_gene_type_ = ObTransSnapshotGeneType::UNKNOWN;
  }
  int set_session_id(const uint32_t session_id);
  uint32_t get_session_id() const
  {
    return session_id_;
  }
  int set_proxy_session_id(const uint64_t proxy_session_id);
  uint64_t get_proxy_session_id() const
  {
    return proxy_session_id_;
  }
  int set_trans_app_trace_id_str(const common::ObString& app_trace_id_str);
  const common::ObString& get_trans_app_trace_id_str() const
  {
    return trace_info_.get_app_trace_id();
  }
  uint64_t get_last_end_stmt_ts() const
  {
    return last_end_stmt_ts_;
  };
  int set_last_end_stmt_ts(const uint64_t last_end_stmt_ts);
  ObTransTraceLog& get_tlog()
  {
    return tlog_;
  }
  void set_need_print_trace_log()
  {
    need_print_trace_log_ = true;
  }
  void set_can_elr(const bool can_elr)
  {
    can_elr_ = can_elr;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  int stmt_deep_copy(const ObTransDesc& trans_desc);
  int trans_deep_copy(const ObTransDesc& trans_desc);
  void set_dup_table_trans(bool is_dup_table_trans)
  {
    is_dup_table_trans_ = is_dup_table_trans;
  }
  bool is_dup_table_trans() const
  {
    return is_dup_table_trans_;
  }
  void set_local_trans(bool is_local_trans)
  {
    is_local_trans_ = is_local_trans;
  }
  bool is_local_trans() const
  {
    return is_local_trans_;
  }
  bool is_all_ro_participants(const common::ObPartitionArray& participants, const int64_t participant_cnt) const;
  int find_first_not_ro_participant(
      const common::ObPartitionArray& participants, common::ObPartitionKey*& ret_pkey, int64_t& pkey_index) const;
  bool maybe_violate_snapshot_consistency() const
  {
    return trans_param_.need_consistent_snapshot() && ObTransSnapshotGeneType::NOTHING == snapshot_gene_type_;
  }
  bool is_not_create_ctx_participant(
      const common::ObPartitionKey& pkey, const int64_t user_specified_snapshot = -1) const;
  int is_leader_stmt_participant(const common::ObPartitionKey& pkey, bool& is_leader) const;
  bool is_serializable_isolation() const
  {
    return trans_param_.is_serializable_isolation();
  }
  void set_trans_need_wait_wrap(const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us)
  {
    trans_need_wait_wrap_.set_trans_need_wait_wrap(receive_gts_ts, need_wait_interval_us);
  }
  void consistency_wait();
  void set_fast_select()
  {
    is_fast_select_ = true;
  }
  bool is_fast_select() const
  {
    return is_fast_select_;
  }
  void set_stmt_snapshot_info(const ObTransSnapInfo& snapshot_info)
  {
    stmt_snapshot_info_ = snapshot_info;
  }
  const ObTransSnapInfo& get_stmt_snapshot_info() const
  {
    return stmt_snapshot_info_;
  }
  ObStmtRollbackInfo& get_stmt_rollback_info()
  {
    return stmt_rollback_info_;
  }
  const ObStmtRollbackInfo& get_stmt_rollback_info() const
  {
    return stmt_rollback_info_;
  }
  void set_max_sql_no(const int64_t sql_no)
  {
    sql_no_ = max_sql_no_ = MAX(max_sql_no_, sql_no);
  }
  void restore_max_sql_no(const int64_t sql_no)
  {
    max_sql_no_ = MAX(max_sql_no_, sql_no);
    inc_savepoint();
  }
  int64_t get_max_sql_no() const
  {
    return max_sql_no_;
  }
  int64_t get_tenant_config_refresh_time() const
  {
    return tenant_config_refresh_time_;
  }
  void set_tenant_config_refresh_time(const int64_t t)
  {
    tenant_config_refresh_time_ = t;
  }
  const ObTraceInfo& get_trace_info() const
  {
    return trace_info_;
  }
  void reset_start_participant_ts()
  {
    start_participant_ts_ = 0;
  }
  int set_start_participant_ts(const int64_t start_participant_ts);
  int64_t get_start_participant_ts() const
  {
    return start_participant_ts_;
  }
  const ObStandaloneStmtDesc& get_standalone_stmt_desc() const
  {
    return standalone_stmt_desc_;
  }
  ObStandaloneStmtDesc& get_standalone_stmt_desc()
  {
    return standalone_stmt_desc_;
  }
  void setup_standalone_stmt_desc(const ObStandaloneStmtDesc& desc)
  {
    standalone_stmt_desc_ = desc;
  }
  bool is_standalone_stmt_desc() const
  {
    return standalone_stmt_desc_.is_valid();
  }
  void set_need_check_at_end_participant(const bool need_check_at_end_participant)
  {
    need_check_at_end_participant_ = need_check_at_end_participant;
  }
  bool need_check_at_end_participant() const
  {
    return need_check_at_end_participant_;
  }
  int set_xid(const ObXATransID& xid);
  const ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool is_xa_local_trans() const
  {
    return !xid_.empty();
  }
  int64_t get_xa_end_timeout_seconds() const
  {
    return xa_end_timeout_seconds_;
  }
  void set_xa_end_timeout_seconds(int64_t seconds)
  {
    xa_end_timeout_seconds_ = seconds;
  }
  bool is_trx_level_temporary_table_involved() const
  {
    return trx_level_temporary_table_involved_;
  }
  void set_trx_level_temporary_table_involved()
  {
    trx_level_temporary_table_involved_ = true;
  }
  inline void set_audit_record(sql::ObAuditRecordData* r)
  {
    audit_record_ = r;
  }
  inline sql::ObAuditRecordData* get_audit_record()
  {
    return audit_record_;
  }
  bool is_xa_tightly_coupled() const
  {
    return is_tightly_coupled_;
  }
  void set_xa_tightly_coupled(const bool is_tightly_coupled)
  {
    is_tightly_coupled_ = is_tightly_coupled;
  }
  inline bool is_trx_idle_timeout() const
  {
    return trx_idle_timeout_;
  }
  inline void set_trx_idle_timeout()
  {
    trx_idle_timeout_ = true;
  }
  TO_STRING_KV(K_(tenant_id), K_(trans_id), K_(snapshot_version), K_(trans_snapshot_version), K_(trans_param),
      K_(participants), K_(stmt_participants), K_(sql_no), K_(max_sql_no), K_(stmt_min_sql_no), K_(cur_stmt_desc),
      K_(need_rollback), K_(trans_expired_time), K_(cur_stmt_expired_time), K_(trans_type), K_(is_all_select_stmt),
      K_(stmt_participants_pla), K_(participants_pla), K_(is_sp_trans_exiting), K_(trans_end), K_(snapshot_gene_type),
      K_(local_consistency_type), "snapshot_gene_type_str", ObTransSnapshotGeneType::cstr(snapshot_gene_type_),
      "local_consistency_type_str", ObTransConsistencyType::cstr(local_consistency_type_), K_(session_id),
      K_(proxy_session_id), K_(app_trace_id_confirmed), K_(can_elr), K_(is_dup_table_trans), K_(is_local_trans),
      K_(trans_need_wait_wrap), K_(is_fast_select), K_(trace_info), K_(standalone_stmt_desc),
      K_(need_check_at_end_participant), K_(is_nested_stmt), K_(stmt_min_sql_no), K_(xid), K_(gc_participants));

public:
  static const int32_t MAX_XID_LENGTH = 128;

private:
  bool contain_(const common::ObPartitionArray& participants, const common::ObPartitionKey& participant) const;
  DISALLOW_COPY_AND_ASSIGN(ObTransDesc);

public:
  bool cluster_version_before_2271() const
  {
    return cluster_version_ < CLUSTER_VERSION_2271;
  }

private:
  uint64_t tenant_id_;
  ObTransID trans_id_;
  int64_t snapshot_version_;
  int64_t trans_snapshot_version_;
  int64_t commit_version_;
  ObStartTransParam trans_param_;
  common::ObAddr scheduler_;
  common::ObAddr orig_scheduler_;
  common::ObPartitionArray participants_;
  common::ObPartitionArray stmt_participants_;
  int64_t sql_no_;
  ObStmtDesc cur_stmt_desc_;
  bool need_rollback_;
  int64_t trans_expired_time_;
  int64_t cur_stmt_expired_time_;
  common::ObVersion memstore_version_;
  uint64_t cluster_version_;
  uint64_t cluster_id_;
  common::ObPartitionLeaderArray stmt_participants_pla_;
  common::ObPartitionLeaderArray participants_pla_;
  int snapshot_gene_type_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  bool app_trace_id_confirmed_;
  ObStmtInfo nested_stmt_info_;
  // elr for tenant level
  bool can_elr_;
  // duplicate transaction
  bool is_dup_table_trans_;
  bool is_local_trans_;
  ObStmtRollbackInfo stmt_rollback_info_;
  ObTraceInfo trace_info_;
  ObTransSnapInfo stmt_snapshot_info_;
  // fast select path
  bool is_fast_select_;
  ObXATransID xid_;
  int64_t xa_end_timeout_seconds_;
  bool is_nested_stmt_;
  int64_t max_sql_no_;
  int64_t stmt_min_sql_no_;
  bool is_tightly_coupled_;

private:
  // no need to serialize
  ObTransCtx* part_ctx_;
  ObScheTransCtx* sche_ctx_;
  int32_t trans_type_;
  storage::ObWarmUpCtx* warm_up_ctx_;
  bool is_all_select_stmt_;
  int32_t local_consistency_type_;
  bool is_sp_trans_exiting_;
  bool trans_end_;
  sql::ObBasicSessionInfo* session_;
  uint64_t last_end_stmt_ts_;
  ObTransTraceLog tlog_;
  bool need_print_trace_log_;
  ObTransNeedWaitWrap trans_need_wait_wrap_;
  int64_t tenant_config_refresh_time_;
  int64_t start_participant_ts_;
  mutable ObStandaloneStmtDesc standalone_stmt_desc_;
  bool need_check_at_end_participant_;
  common::ObPartitionArray gc_participants_;
  bool trx_level_temporary_table_involved_;
  // sql audit
  sql::ObAuditRecordData* audit_record_;
  // trans was killed after ob_trx_idle_timeout
  bool trx_idle_timeout_;
};

class ObTransIsolation {
public:
  enum {
    /*
     * after the discussion, we decide to adjust the value of
     * the following definition by exchanging READ_UNCOMMITTED and REPEATABLE_READ,
     * then the order is much better.
     */
    UNKNOWN = -1,
    READ_UNCOMMITTED = 0,
    READ_COMMITED = 1,
    REPEATABLE_READ = 2,
    SERIALIZABLE = 3,
    MAX_LEVEL
  };
  static const common::ObString LEVEL_NAME[MAX_LEVEL];

public:
  static bool is_valid(const int32_t level)
  {
    return level == READ_COMMITED || level == REPEATABLE_READ || level == SERIALIZABLE;
  }
  static int32_t get_level(const common::ObString& level_name);
  static const common::ObString& get_name(int32_t level);

private:
  ObTransIsolation()
  {}
  ~ObTransIsolation()
  {}
};

class ObPartTransAction {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t START_TASK = 1;
  static const int64_t END_TASK = 2;
  static const int64_t COMMIT = 3;
  static const int64_t ABORT = 4;
  static const int64_t DIED = 5;
  static const int64_t END = 6;

public:
  static bool is_valid(const int64_t state)
  {
    return state >= START_TASK && state < END;
  }

private:
  ObPartTransAction()
  {}
  ~ObPartTransAction()
  {}
};

class ObRunningState {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 0;
  static const int64_t ABORT = 200;

public:
  static bool is_valid(const int64_t state)
  {
    return INIT == state || ABORT == state;
  }

private:
  ObRunningState()
  {}
  ~ObRunningState()
  {}
};

class ObSpState {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 0;
  static const int64_t PREPARE = 100;
  static const int64_t COMMIT = 101;
  static const int64_t ABORT = 102;

public:
  static bool is_valid(const int64_t state)
  {
    return INIT == state || PREPARE == state || COMMIT == state || ABORT == state;
  }

private:
  ObSpState()
  {}
  ~ObSpState()
  {}
};

class ObStmtType
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t READ = 0;
  static const int64_t SFU = 1;
  static const int64_t WRITE = 2;
public:
  static bool is_valid(const int64_t stmt_type)
  { return READ == stmt_type || SFU == stmt_type || WRITE == stmt_type; }
private:
  ObStmtType() {}
  ~ObStmtType() {}
};

class Ob2PCState {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 0;
  static const int64_t PREPARE = 1;
  static const int64_t COMMIT = 2;
  static const int64_t ABORT = 3;
  static const int64_t CLEAR = 4;
  static const int64_t PRE_PREPARE = 5;
  static const int64_t PRE_COMMIT = 6;

public:
  static bool is_valid(const int64_t state)
  {
    return state >= INIT && state <= PRE_COMMIT;
  }

private:
  Ob2PCState()
  {}
  ~Ob2PCState()
  {}
};

class ObXATransState {
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t NON_EXISTING = 0;
  static const int32_t ACTIVE = 1;
  static const int32_t IDLE = 2;
  static const int32_t PREPARED = 3;
  static const int32_t COMMITTED = 4;
  static const int32_t ROLLBACKED = 5;
  static const int32_t PREPARING = 6;

public:
  static bool is_valid(const int32_t state)
  {
    return state >= NON_EXISTING && state <= PREPARING;
  }
  static bool is_prepared(const int32_t state)
  {
    return state == PREPARED;
  }
  static bool can_convert(const int32_t src_state, const int32_t dst_state);
  static const char* to_string(int32_t state)
  {
    const char* state_str = NULL;
    switch (state) {
      case NON_EXISTING:
        state_str = "NON-EXISTING";
        break;
      case ACTIVE:
        state_str = "ACTIVE";
        break;
      case IDLE:
        state_str = "IDLE";
        break;
      case PREPARED:
        state_str = "PREPARED";
        break;
      case COMMITTED:
        state_str = "COMMITTED";
        break;
      case ROLLBACKED:
        state_str = "ROLLBACKED";
        break;
      case PREPARING:
        state_str = "PREPARING";
        break;
      default:
        state_str = "UNKNOW";
        break;
    }
    return state_str;
  }
};

class ObXAFlag {
public:
  enum {
    TMNOFLAGS = 0,
    // non-standard xa protocol, to denote a readonly xa trans
    TMREADONLY = 0x100,
    // non-standard xa protocol, to denote a serializable xa trans
    TMSERIALIZABLE = 0x400,
    // non-standard xa protocol, to denote loosely coupled xa trans
    LOOSELY = 0x10000,
    TMJOIN = 0x200000,
    TMSUSPEND = 0x2000000,
    TMSUCCESS = 0x4000000,
    TMRESUME = 0x8000000,
    TMONEPHASE = 0x40000000,
    // non-standard xa protocol, to denote temp table xa trans
    TEMPTABLE = 0x100000000,
  };

public:
  static bool is_valid(const int64_t flag, const int64_t xa_req_type);
  static bool is_valid_inner_flag(const int64_t flag);
  static bool contain_tmreadonly(const int64_t flag)
  {
    return flag & TMREADONLY;
  }
  static bool contain_tmserializable(const int64_t flag)
  {
    return flag & TMSERIALIZABLE;
  }
  static bool is_tmnoflags(const int64_t flag, const int64_t xa_req_type);
  static bool contain_loosely(const int64_t flag)
  {
    return flag & LOOSELY;
  }
  static bool contain_tmjoin(const int64_t flag)
  {
    return flag & TMJOIN;
  }
  static bool is_tmjoin(const int64_t flag)
  {
    return flag == TMJOIN;
  }
  static bool contain_tmresume(const int64_t flag)
  {
    return flag & TMRESUME;
  }
  static bool is_tmresume(const int64_t flag)
  {
    return flag == TMRESUME;
  }
  static bool contain_tmsuccess(const int64_t flag)
  {
    return flag & TMSUCCESS;
  }
  static bool contain_tmsuspend(const int64_t flag)
  {
    return flag & TMSUSPEND;
  }
  static bool contain_tmonephase(const int64_t flag)
  {
    return flag & TMONEPHASE;
  }
  static bool is_tmonephase(const int64_t flag)
  {
    return flag == TMONEPHASE;
  }
  static bool contain_temptable(const int64_t flag)
  {
    return flag & TEMPTABLE;
  }
};

class ObTransSubmitLogState {
public:
  static const int64_t INIT = 0;
  static const int64_t SUBMIT_LOG = 1;
  static const int64_t SUBMIT_LOG_PENDING = 2;
  static const int64_t SUBMIT_LOG_SUCCESS = 3;

public:
  bool is_valid(const int64_t state)
  {
    return INIT == state || SUBMIT_LOG == state || SUBMIT_LOG_PENDING == state || SUBMIT_LOG_SUCCESS == state;
  }
};

class ObTransRetryTaskType {
public:
  static const int64_t UNKNOWN = -1;
  // submit log
  static const int64_t SUBMIT_LOG = 0;
  static const int64_t ALLOC_LOG_ID = 1;
  // rpc
  static const int64_t BACKFILL_NOP_LOG = 2;
  static const int64_t CALLBACK_TASK = 3;
  static const int64_t ERROR_MSG_TASK = 4;
  static const int64_t NOTIFY_TASK = 5;
  static const int64_t RETRY_STMT_ROLLBACK_TASK = 6;
  // elr
  static const int64_t CALLBACK_NEXT_TRANS_TASK = 7;
  static const int64_t CALLBACK_PREV_TRANS_TASK = 8;
  // big transaction
  static const int64_t BIG_TRANS_CALLBACK_TASK = 9;
  // duplicate transaction
  static const int64_t REDO_SYNC_TASK = 10;
  static const int64_t END_TRANS_CB_TASK = 11;
  static const int64_t AGGRE_LOG_TASK = 12;
  static const int64_t MAX = 13;

public:
  static bool is_valid(const int64_t task_type)
  {
    return task_type > UNKNOWN && task_type < MAX;
  }
};

class ObPartitionLeaderInfo final {
  OB_UNIS_VERSION(1);

public:
  ObPartitionLeaderInfo()
  {}
  ObPartitionLeaderInfo(const common::ObPartitionKey& partition, const common::ObAddr& addr)
      : partition_(partition), addr_(addr)
  {}
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int set_partition(const common::ObPartitionKey& partition);
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  int set_addr(const common::ObAddr& addr);
  bool is_valid() const
  {
    return partition_.is_valid() && addr_.is_valid();
  }

  TO_STRING_KV(K_(partition), K_(addr));

private:
  common::ObPartitionKey partition_;
  common::ObAddr addr_;
};

class ObPartitionLogInfo final {
  OB_UNIS_VERSION(1);

public:
  ObPartitionLogInfo() : log_id_(0), log_timestamp_(0)
  {}
  ObPartitionLogInfo(const common::ObPartitionKey& partition, const int64_t log_id, const int64_t log_timestamp)
      : partition_(partition), log_id_(log_id), log_timestamp_(log_timestamp)
  {}
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_log_timestamp() const
  {
    return log_timestamp_;
  }
  bool is_valid() const
  {
    return partition_.is_valid() && log_id_ > 0 && log_timestamp_ > 0;
  }
  int set_partition(const common::ObPartitionKey& pkey)
  {
    int ret = common::OB_SUCCESS;
    if (!pkey.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      partition_ = pkey;
    }
    return ret;
  }
  TO_STRING_KV(K_(partition), K_(log_id), K_(log_timestamp));

private:
  common::ObPartitionKey partition_;
  int64_t log_id_;
  int64_t log_timestamp_;
};

class ObPartitionEpochInfo final {
  OB_UNIS_VERSION(1);

public:
  ObPartitionEpochInfo() : partition_(), epoch_(0)
  {}
  ObPartitionEpochInfo(const common::ObPartitionKey& partition, const int64_t epoch)
      : partition_(partition), epoch_(epoch)
  {}
  void reset();
  int generate(const common::ObPartitionKey& partition, const int64_t epoch);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int64_t get_epoch() const
  {
    return epoch_;
  }
  bool is_valid() const
  {
    return partition_.is_valid() && epoch_ > 0;
  }
  bool operator==(const ObPartitionEpochInfo& other) const
  {
    return partition_ == other.partition_ && epoch_ == other.epoch_;
  }
  TO_STRING_KV(K_(partition), K_(epoch));

private:
  common::ObPartitionKey partition_;
  int64_t epoch_;
};

class ObTransCtxType {
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t SCHEDULER = 0;
  static const int64_t COORDINATOR = 1;
  static const int64_t PARTICIPANT = 2;
  static const int64_t SLAVE_PARTICIPANT = 3;

public:
  static bool is_valid(const int64_t type)
  {
    return SCHEDULER == type || COORDINATOR == type || PARTICIPANT == type || SLAVE_PARTICIPANT == type;
  }
};

typedef common::ObSEArray<int64_t, 16, TransModulePageAllocator> ObLeaderEpochArray;

class ObPartitionLeaderEpochInfo {
public:
  ObPartitionLeaderEpochInfo()
  {
    reset();
  }
  ~ObPartitionLeaderEpochInfo()
  {
    reset();
  }
  void reset();
  int init(const common::ObPartitionKey& partition);
  int push(const int64_t leader_epoch);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  bool is_all_identical_epoch() const;
  TO_STRING_KV(K_(partition), K_(epoch_arr));

private:
  bool is_inited_;
  common::ObPartitionKey partition_;
  ObLeaderEpochArray epoch_arr_;
};

class ObMemtableKeyInfo {
public:
  ObMemtableKeyInfo()
  {
    reset();
  };
  ~ObMemtableKeyInfo(){};
  int init(const uint64_t table_id, const uint64_t hash_val);
  void reset();
  char* get_buf()
  {
    return buf_;
  }
  const char* read_buf() const
  {
    return buf_;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  uint64_t get_hash_val() const
  {
    return hash_val_;
  }
  const void* get_row_lock() const
  {
    return row_lock_;
  }
  void set_row_lock(void* row_lock)
  {
    row_lock_ = row_lock;
  }
  bool operator==(const ObMemtableKeyInfo& other) const;

  TO_STRING_KV(K_(buf));

public:
  static const int MEMTABLE_KEY_INFO_BUF_SIZE = 512;

private:
  uint64_t table_id_;
  uint64_t hash_val_;
  void* row_lock_;
  char buf_[MEMTABLE_KEY_INFO_BUF_SIZE];
};

class ObElrTransInfo final {
  OB_UNIS_VERSION(1);

public:
  ObElrTransInfo()
  {
    reset();
  }
  void reset();
  int init(const ObTransID& trans_id, uint32_t ctx_id, const int64_t commit_version);
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  uint32_t get_ctx_id() const
  {
    return ctx_id_;
  }
  int64_t get_commit_version() const
  {
    return commit_version_;
  }
  int set_result(const int result)
  {
    result_ = result;
    return common::OB_SUCCESS;
  }
  uint64_t hash() const
  {
    return trans_id_.hash();
  }
  int get_result() const
  {
    return ATOMIC_LOAD(&result_);
  }
  bool cas_result(int o, int n)
  {
    return ATOMIC_BCAS(&result_, o, n);
  }
  TO_STRING_KV(K_(trans_id), K_(commit_version), K_(result), K_(ctx_id));

private:
  ObTransID trans_id_;
  int64_t commit_version_;
  int result_;
  uint32_t ctx_id_;
};

class ObTransResultState {
public:
  static const int INVALID = -1;
  static const int UNKNOWN = 0;
  static const int COMMIT = 1;
  static const int ABORT = 2;
  static const int MAX = 3;

public:
  static bool is_valid(const int state)
  {
    return state > INVALID && state < MAX;
  }
  static bool is_unknown(const int state)
  {
    return UNKNOWN == state;
  }
  static bool is_commit(const int state)
  {
    return COMMIT == state;
  }
  static bool is_abort(const int state)
  {
    return ABORT == state;
  }
  static bool is_decided_state(const int state)
  {
    return COMMIT == state || ABORT == state;
  }
};

class ObTransTask {
public:
  ObTransTask() : retry_interval_us_(0), next_handle_ts_(0), task_type_(ObTransRetryTaskType::UNKNOWN)
  {}
  ObTransTask(int64_t task_type) : retry_interval_us_(0), next_handle_ts_(0), task_type_(task_type)
  {}
  virtual ~ObTransTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t task_type);
  int set_retry_interval_us(const int64_t start_interval_us, const int64_t retry_interval_us);
  int64_t get_task_type() const
  {
    return task_type_;
  }
  bool ready_to_handle();
  TO_STRING_KV(K_(task_type), K_(retry_interval_us), K_(next_handle_ts));

public:
  static const int64_t RP_TOTAL_NUM = 512;
  static const int64_t RP_RESERVE_NUM = 64;
  static const int64_t RETRY_SLEEP_TIME_US = 100;

protected:
  int64_t retry_interval_us_;
  int64_t next_handle_ts_;
  int64_t task_type_;
};

class ObBatchCommitState {
public:
  static const int INVALID = -1;
  static const int INIT = 0;
  static const int ALLOC_LOG_ID_TS = 1;
  static const int GENERATE_PREPARE_LOG = 2;
  static const int GENERATE_REDO_PREPARE_LOG = 3;
  static const int BATCH_COMMITTED = 4;
};

enum ObPartitionAuditOperator {
  PART_AUDIT_SET_BASE_ROW_COUNT = 0,
  PART_AUDIT_INSERT_ROW,
  PART_AUDIT_DELETE_ROW,
  PART_AUDIT_UPDATE_ROW,  // put included
  PART_AUDIT_QUERY_ROW,
  PART_AUDIT_INSERT_SQL,
  PART_AUDIT_DELETE_SQL,
  PART_AUDIT_UPDATE_SQL,
  PART_AUDIT_QUERY_SQL,
  PART_AUDIT_TRANS,
  PART_AUDIT_SQL,
  PART_AUDIT_ROLLBACK_INSERT_ROW,
  PART_AUDIT_ROLLBACK_DELETE_ROW,
  PART_AUDIT_ROLLBACK_UPDATE_ROW,
  PART_AUDIT_ROLLBACK_INSERT_SQL,
  PART_AUDIT_ROLLBACK_DELETE_SQL,
  PART_AUDIT_ROLLBACK_UPDATE_SQL,
  PART_AUDIT_ROLLBACK_TRANS,
  PART_AUDIT_ROLLBACK_SQL,
  PART_AUDIT_OP_MAX
};

struct ObPartitionAuditInfoCache {
public:
  ObPartitionAuditInfoCache()
  {
    reset();
  }
  ~ObPartitionAuditInfoCache()
  {}
  void reset()
  {
    insert_row_count_ = 0;
    delete_row_count_ = 0;
    update_row_count_ = 0;
    cur_insert_row_count_ = 0;
    cur_delete_row_count_ = 0;
    cur_update_row_count_ = 0;
    /*
    query_row_count_ = 0;
    insert_sql_count_ = 0;
    delete_sql_count_ = 0;
    update_sql_count_ = 0;
    query_sql_count_ = 0;
    sql_count_ = 0;
    rollback_insert_row_count_ = 0;
    rollback_delete_row_count_ = 0;
    rollback_update_row_count_ = 0;
    rollback_insert_sql_count_ = 0;
    rollback_delete_sql_count_ = 0;
    rollback_update_sql_count_ = 0;
    rollback_sql_count_ = 0;
    */
  }
  int update_audit_info(const enum ObPartitionAuditOperator op, const int32_t count);
  int stmt_end_update_audit_info(bool commit);

public:
  int32_t insert_row_count_;
  int32_t delete_row_count_;
  int32_t update_row_count_;
  int32_t cur_insert_row_count_;
  int32_t cur_delete_row_count_;
  int32_t cur_update_row_count_;
  /*
  int32_t query_row_count_;
  int32_t insert_sql_count_;
  int32_t delete_sql_count_;
  int32_t update_sql_count_;
  int32_t query_sql_count_;
  int32_t sql_count_;
  int32_t rollback_insert_row_count_;
  int32_t rollback_delete_row_count_;
  int32_t rollback_update_row_count_;
  int32_t rollback_insert_sql_count_;
  int32_t rollback_delete_sql_count_;
  int32_t rollback_update_sql_count_;
  int32_t rollback_sql_count_;
  */
};

struct ObPartitionAuditInfo {
public:
  ObPartitionAuditInfo() : lock_(common::ObLatchIds::PARTITION_AUDIT_SPIN_LOCK)
  {
    reset();
  }
  ~ObPartitionAuditInfo()
  {}
  void reset()
  {
    base_row_count_ = 0;
    insert_row_count_ = 0;
    delete_row_count_ = 0;
    update_row_count_ = 0;
    /*
    query_row_count_ = 0;
    insert_sql_count_ = 0;
    delete_sql_count_ = 0;
    update_sql_count_ = 0;
    query_sql_count_ = 0;
    trans_count_ = 0;
    sql_count_ = 0;
    rollback_insert_row_count_ = 0;
    rollback_delete_row_count_ = 0;
    rollback_update_row_count_ = 0;
    rollback_insert_sql_count_ = 0;
    rollback_delete_sql_count_ = 0;
    rollback_update_sql_count_ = 0;
    rollback_trans_count_ = 0;
    rollback_sql_count_ = 0;
    */
  }
  void destroy()
  {
    reset();
  }
  ObPartitionAuditInfo& operator=(const ObPartitionAuditInfo& other);
  ObPartitionAuditInfo& operator+=(const ObPartitionAuditInfo& other);
  int update_audit_info(const ObPartitionAuditInfoCache& cache, const bool commit);
  void set_base_row_count(int64_t count)
  {
    ATOMIC_SET(&base_row_count_, count);
  }

public:
  common::ObSpinLock lock_;
  int64_t base_row_count_;
  int64_t insert_row_count_;
  int64_t delete_row_count_;
  int64_t update_row_count_;
  // dead statistic event, need to be removed
  /*
  int64_t query_row_count_;
  int64_t insert_sql_count_;
  int64_t delete_sql_count_;
  int64_t update_sql_count_;
  int64_t query_sql_count_;
  int64_t trans_count_;
  int64_t sql_count_;
  int64_t rollback_insert_row_count_;
  int64_t rollback_delete_row_count_;
  int64_t rollback_update_row_count_;
  int64_t rollback_insert_sql_count_;
  int64_t rollback_delete_sql_count_;
  int64_t rollback_update_sql_count_;
  int64_t rollback_trans_count_;
  int64_t rollback_sql_count_;
  */
};

class ObCoreLocalPartitionAuditInfo : public common::ObCoreLocalStorage<ObPartitionAuditInfo*> {
public:
  ObCoreLocalPartitionAuditInfo()
  {}
  ~ObCoreLocalPartitionAuditInfo()
  {
    destroy();
  }
  int init(int64_t array_len);
  void reset();
  void destroy()
  {
    reset();
  }
};

// elr state
enum ELRState { ELR_INIT = 0, ELR_PREPARING = 1, ELR_PREPARED = 2 };

// CHANGING_LEADER_STATE state machine
//
// Original state when no leader transfer is on going:                NO_CHANGING_LEADER
// If stmt info is not matched during preparing change leader:        NO_CHANGING_LEADER -> STATEMENT_NOT_FINISH
// - If stmt info matches before leader transfer and submit log:      STATEMENT_NOT_FINISH -> NO_CHANGING_LEADER
//   - if exists a on-the-fly log during submit STATE log:            STATEMENT_NOT_FINISH -> LOGGING_NOT_FINISH
// If there exists a on-the-fly log during submit STATE log:          NO_CHANGING_LEADER -> LOGGING_NOT_FINISH
// - If the prev log is synced before leader transfer and submit log: LOGGING_NOT_FINISH -> NO_CHANGING_LEADER
// If the leader revokes:                                             STATEMENT_NOT_FINISH/LOGGING_NOT_FINISH ->
// NO_CHANGING_LEADER
enum CHANGING_LEADER_STATE { NO_CHANGING_LEADER = 0, STATEMENT_NOT_FINISH = 1, LOGGING_NOT_FINISH = 2 };

class ObAddrLogId {
public:
  ObAddrLogId(const common::ObAddr& addr, const uint64_t log_id) : addr_(addr), log_id_(log_id)
  {}
  ObAddrLogId()
  {
    reset();
  }
  ~ObAddrLogId()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  bool operator==(const ObAddrLogId& other) const;
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  TO_STRING_KV(K_(addr), K_(log_id));

private:
  common::ObAddr addr_;
  uint64_t log_id_;
};

enum class ObTransTableStatusType : int64_t { RUNNING = 0, COMMIT, ABORT };

class ObUndoAction {
  OB_UNIS_VERSION(1);

public:
  ObUndoAction()
  {
    reset();
  }
  ObUndoAction(const int64_t undo_from, const int64_t undo_to) : undo_from_(undo_from), undo_to_(undo_to)
  {}
  ~ObUndoAction()
  {
    destroy();
  }
  void reset()
  {
    undo_from_ = 0;
    undo_to_ = 0;
  }
  void destroy()
  {
    reset();
  }
  bool is_valid() const
  {
    return undo_from_ > 0 && undo_to_ > 0 && undo_from_ >= undo_to_;
  }
  bool is_contain(const int64_t sql_no) const
  {
    return sql_no > undo_to_ && sql_no <= undo_from_;
  }
  bool is_contain(const ObUndoAction& other) const
  {
    return undo_from_ >= other.undo_from_ && undo_to_ <= other.undo_to_;
  }
  bool is_less_than(const int64_t sql_no) const
  {
    return sql_no > undo_from_;
  }
  bool is_conjoint(const int64_t sql_no) const
  {
    return sql_no >= undo_to_ && sql_no <= undo_from_;
  }
  bool is_conjoint(const ObUndoAction& other) const
  {
    return is_conjoint(other.undo_to_) || is_conjoint(other.undo_from_);
  }
  int merge(const ObUndoAction& other);
  int64_t get_undo_from()
  {
    return undo_from_;
  }
  TO_STRING_KV(K_(undo_from), K_(undo_to));

private:
  // from > to
  int64_t undo_from_;
  int64_t undo_to_;
};

typedef common::ObSEArray<ObUndoAction, 1> ObUndoActionArray;

class ObTransUndoStatus {
  OB_UNIS_VERSION(1);

public:
  ObTransUndoStatus() : latch_(common::ObLatchIds::UNDO_STATUS_LOCK)
  {
    reset();
  }
  ~ObTransUndoStatus()
  {
    destroy();
  }
  void reset()
  {
    undo_action_arr_.reset();
  }
  void destroy()
  {
    reset();
  }
  int set(const ObTransUndoStatus& undo_status);
  int undo(const int64_t undo_to, const int64_t undo_from);
  bool is_contain(const int64_t sql_no) const;
  int deep_copy(ObTransUndoStatus& status);
  TO_STRING_KV(K_(undo_action_arr));

protected:
  mutable common::ObSpinLock latch_;
  ObUndoActionArray undo_action_arr_;
};

class ObTransStatusInfo {
public:
  ObTransStatusInfo()
      : status_(ObTransTableStatusType::RUNNING),
        trans_version_(common::OB_INVALID_VERSION),
        undo_status_(),
        start_log_ts_(0),
        end_log_ts_(INT64_MAX)
  {}
  ObTransTableStatusType status_;
  int64_t trans_version_;
  ObTransUndoStatus undo_status_;
  int64_t start_log_ts_;
  int64_t end_log_ts_;
  TO_STRING_KV(K_(status), K_(trans_version), K_(undo_status), K_(start_log_ts), K_(end_log_ts));
};

class ObTransTableStatusInfo {
  OB_UNIS_VERSION(1);

public:
  ObTransTableStatusInfo()
  {
    reset();
  }
  ~ObTransTableStatusInfo()
  {
    destroy();
  }
  int set(const ObTransTableStatusType& status, const int64_t trans_version, const ObTransUndoStatus& undo_status,
      const int64_t terminate_log_ts, const uint64_t& checksum, const int64_t checksum_log_ts);
  int set(const ObTransTableStatusType& status, const int64_t trans_version, const ObTransUndoStatus& undo_status,
      const int64_t terminate_log_ts);
  void set_status(const ObTransTableStatusType type)
  {
    status_ = type;
  }
  void get_transaction_status(ObTransTableStatusType& status, int64_t& trans_version) const
  {
    status = status_;
    trans_version = trans_version_;
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int64_t get_checksum_log_ts() const
  {
    return checksum_log_ts_;
  }
  uint64_t get_checksum() const
  {
    return checksum_;
  }
  void set_checksum_log_ts(const int64_t log_ts)
  {
    checksum_log_ts_ = log_ts;
  }
  void set_checksum(const uint64_t& checksum)
  {
    checksum_ = checksum;
  }
  int64_t get_terminate_log_ts() const
  {
    return terminate_log_ts_;
  }
  const ObTransUndoStatus& get_undo_status() const
  {
    return undo_status_;
  }
  bool is_terminated() const
  {
    return ObTransTableStatusType::COMMIT == status_ || ObTransTableStatusType::ABORT == status_;
  }
  bool is_commit() const
  {
    return ObTransTableStatusType::COMMIT == status_;
  }
  bool is_running() const
  {
    return ObTransTableStatusType::RUNNING == status_;
  }
  int64_t get_trans_version() const
  {
    return trans_version_;
  }
  TO_STRING_KV(K_(status), K_(trans_version), K_(undo_status), K_(terminate_log_ts), K_(checksum), K_(checksum_log_ts));

private:
  ObTransTableStatusType status_;
  int64_t trans_version_;
  ObTransUndoStatus undo_status_;
  int64_t terminate_log_ts_;
  uint64_t checksum_;
  int64_t checksum_log_ts_;
};

class ObSameLeaderPartitionArr {
public:
  ObSameLeaderPartitionArr()
  {
    reset();
  }
  ~ObSameLeaderPartitionArr()
  {
    destroy();
  }
  void reset()
  {
    leader_.reset();
    partition_arr_.reset();
  }
  void destroy()
  {
    reset();
  }
  int init(const common::ObAddr& leader, const common::ObPartitionKey& partition);
  const common::ObAddr& get_leader() const
  {
    return leader_;
  }
  common::ObPartitionArray& get_partition_arr()
  {
    return partition_arr_;
  }
  bool is_valid() const
  {
    return leader_.is_valid() && partition_arr_.count() > 0;
  }
  TO_STRING_KV(K_(leader), K_(partition_arr));

private:
  common::ObAddr leader_;
  common::ObPartitionArray partition_arr_;
};

typedef common::ObSEArray<ObSameLeaderPartitionArr, 10> ObSameLeaderPartitionArrArray;

class ObSameLeaderPartitionArrMgr {
public:
  ObSameLeaderPartitionArrMgr()
  {
    reset();
  }
  ~ObSameLeaderPartitionArrMgr()
  {
    destroy();
  }
  int push(const common::ObAddr& leader, const common::ObPartitionKey& partition);
  int64_t count() const
  {
    return array_.count();
  }
  ObSameLeaderPartitionArr& at(int64_t index)
  {
    return array_.at(index);
  }
  void set_ready()
  {
    is_ready_ = true;
  }
  bool is_ready() const
  {
    return is_ready_;
  }
  void reset();
  void destroy()
  {
    reset();
  }
  TO_STRING_KV(K_(array));

private:
  ObSameLeaderPartitionArrArray array_;
  bool is_ready_;
};

class ObPartTransSameLeaderBatchRpcItem {
public:
  ObPartTransSameLeaderBatchRpcItem()
  {
    reset();
  }
  ~ObPartTransSameLeaderBatchRpcItem()
  {
    destroy();
  }
  void reset()
  {
    msg_type_ = -1;
    participants_.reset();
    trans_id_.reset();
    last_add_ts_ = 0;
    base_timestamp_ = 0;
  }
  void destroy()
  {
    reset();
  }
  int add_batch_rpc(const ObTransID& trans_id, const int64_t msg_type, const common::ObPartitionKey& partition,
      const int64_t base_ts, const int64_t batch_count, bool& need_response, common::ObPartitionArray& batch_partitions,
      int64_t& same_leader_batch_base_ts);
  int try_preempt(const ObTransID& trans_id);
  TO_STRING_KV(K_(msg_type), K_(trans_id), K_(participants));
  static const int64_t ALLOW_PREEMPT_INTERVAL_US = 1 * 1000 * 1000;

private:
  bool allow_to_preempt_(const ObTransID& trans_id);

private:
  common::ObLatch lock_;
  int64_t msg_type_;
  ObTransID trans_id_;
  common::ObPartitionArray participants_;
  int64_t last_add_ts_;
  int64_t base_timestamp_;
} CACHE_ALIGNED;

class ObPartTransSameLeaderBatchRpcMgr {
public:
  ObPartTransSameLeaderBatchRpcMgr()
  {
    reset();
  }
  ~ObPartTransSameLeaderBatchRpcMgr()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int add_batch_rpc(const ObTransID& trans_id, const int64_t msg_type, const common::ObPartitionKey& partition,
      const int64_t base_ts, const int64_t batch_count, bool& need_response, common::ObPartitionArray& batch_partitions,
      int64_t& same_leader_batch_base_ts);
  int try_preempt(const ObTransID& trans_id);
  // 2^17
  static const int64_t SAME_LEADER_BATCH_RPC_ITEM_ARR_SIZE = 131072;

private:
  ObPartTransSameLeaderBatchRpcItem batch_rpc_item_arr_[SAME_LEADER_BATCH_RPC_ITEM_ARR_SIZE];
};

class ObTransLogBufferAggreContainer {
public:
  ObTransLogBufferAggreContainer()
  {
    reset();
  }
  ~ObTransLogBufferAggreContainer()
  {
    destroy();
  }
  void reset()
  {
    aggre_buffer_ = NULL;
    task_ = NULL;
    offset_ = 0;
    last_flush_ts_ = 0;
    is_inited_ = false;
    base_timestamp_ = 0;
  }
  void destroy();
  int init(const common::ObPartitionKey& partition);
  void reuse();
  int leader_revoke();
  int fill(const char* buf, const int64_t size, ObITransSubmitLogCb* cb, const int64_t base_ts, bool& need_submit_log);
  int flush(bool& need_submit_log);
  clog::ObAggreBuffer* get_aggre_buffer()
  {
    return aggre_buffer_;
  }
  AggreLogTask* get_task()
  {
    return task_;
  }
  int64_t get_base_timestamp() const
  {
    return base_timestamp_;
  }
  static const int64_t FLUSH_INTERVAL_US = 1 * 1000 * 1000;
  TO_STRING_KV(K_(offset), K_(last_flush_ts), K_(is_inited));

private:
  void flush_(bool& need_submit_log);

private:
  clog::ObAggreBuffer* aggre_buffer_;
  AggreLogTask* task_;
  int64_t offset_;
  int64_t last_flush_ts_;
  bool is_inited_;
  int64_t base_timestamp_;
};

class ObLightTransCtxItem {
public:
  ObLightTransCtxItem()
  {
    reset();
  }
  ~ObLightTransCtxItem()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  bool get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx);
  bool get_and_remove_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx);
  bool remove_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id);
  bool add_trans_ctx(ObTransCtx* ctx);
  ObTransCtx* get_trans_ctx()
  {
    return trans_ctx_;
  }

private:
  common::ObLatch lock_;
  ObTransCtx* trans_ctx_;
};

class ObLightTransCtxMgr {
public:
  ObLightTransCtxMgr()
  {
    reset();
  }
  ~ObLightTransCtxMgr()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int init(ObPartTransCtxMgr* part_trans_ctx_mgr);
  int leader_takeover(const common::ObPartitionKey& partition);
  int remove_partition(const common::ObPartitionKey& partition);
  uint64_t get_index(const common::ObPartitionKey& partition, const ObTransID& trans_id);
  bool get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx);
  bool get_and_remove_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx);
  bool remove_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id);
  bool add_trans_ctx(ObTransCtx* ctx);

public:
  static const int64_t LIGHT_TRANS_CTX_MGR_TABLE_SIZE = 400000;

private:
  ObLightTransCtxItem item_array_[LIGHT_TRANS_CTX_MGR_TABLE_SIZE];
  ObPartTransCtxMgr* part_trans_ctx_mgr_;
};

typedef common::ObSEArray<ObElrTransInfo, 1, TransModulePageAllocator> ObElrTransInfoArray;
typedef common::ObSEArray<ObPartitionLogInfo, 10, TransModulePageAllocator> PartitionLogInfoArray;
typedef common::ObSEArray<int64_t, 10, TransModulePageAllocator> ObRedoLogIdArray;

struct ObTransSSTableDurableCtxInfo {
  OB_UNIS_VERSION(1);

public:
  ObTransSSTableDurableCtxInfo()
  {
    reset();
  }
  ~ObTransSSTableDurableCtxInfo()
  {
    destroy();
  }
  void reset()
  {
    trans_table_info_.reset();
    partition_.reset();
    trans_param_.reset();
    tenant_id_ = 0;
    trans_expired_time_ = -1;
    cluster_id_ = 0;
    scheduler_.reset();
    coordinator_.reset();
    participants_.reset();
    prepare_status_ = common::OB_SUCCESS;
    prev_redo_log_ids_.reset();
    app_trace_id_str_.reset();
    partition_log_info_arr_.reset();
    prev_trans_arr_.reset();
    can_elr_ = false;
    max_durable_log_ts_ = 0;
    global_trans_version_ = -1;
    commit_log_checksum_ = 0;
    state_ = Ob2PCState::UNKNOWN;
    prepare_version_ = ObTransVersion::INVALID_TRANS_VERSION;
    max_durable_sql_no_ = 0;
    trans_type_ = TransType::UNKNOWN;
    elr_prepared_state_ = 0;
    is_dup_table_trans_ = false;
    redo_log_no_ = 0;
    mutator_log_no_ = 0;
    stmt_info_.reset();
    min_log_ts_ = 0;
    min_log_id_ = 0;
    sp_user_request_ = 0;
    need_checksum_ = false;
    prepare_log_id_ = 0;
    prepare_log_timestamp_ = 0;
    clear_log_base_ts_ = 0;
    prev_checkpoint_id_ = 0;
  }
  void destroy()
  {
    reset();
  }
  TO_STRING_KV(K_(trans_table_info), K_(partition), K_(trans_param), K_(tenant_id), K_(trans_expired_time),
      K_(cluster_id), K_(scheduler), K_(coordinator), K_(participants), K_(prepare_status), K_(prev_redo_log_ids),
      K_(app_trace_id_str), K_(partition_log_info_arr), K_(prev_trans_arr), K_(can_elr), K_(max_durable_log_ts),
      K_(global_trans_version), K_(commit_log_checksum), K_(state), K_(prepare_version), K_(max_durable_sql_no),
      K_(trans_type), K_(elr_prepared_state), K_(is_dup_table_trans), K_(redo_log_no), K_(mutator_log_no),
      K_(stmt_info), K_(min_log_ts), K_(min_log_id), K_(sp_user_request), K_(need_checksum), K_(prepare_log_id),
      K_(prepare_log_timestamp), K_(prev_checkpoint_id));
  ObTransTableStatusInfo trans_table_info_;
  common::ObPartitionKey partition_;
  ObStartTransParam trans_param_;
  uint64_t tenant_id_;
  int64_t trans_expired_time_;
  uint64_t cluster_id_;
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int prepare_status_;
  ObRedoLogIdArray prev_redo_log_ids_;
  common::ObString app_trace_id_str_;
  PartitionLogInfoArray partition_log_info_arr_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  int64_t max_durable_log_ts_;
  int64_t global_trans_version_;
  uint64_t commit_log_checksum_;
  int64_t state_;
  int64_t prepare_version_;
  int32_t max_durable_sql_no_;
  int32_t trans_type_;
  int32_t elr_prepared_state_;
  bool is_dup_table_trans_;
  int64_t redo_log_no_;
  int64_t mutator_log_no_;
  ObTransStmtInfo stmt_info_;
  int64_t min_log_ts_;
  int64_t min_log_id_;
  int sp_user_request_;
  bool need_checksum_;
  int64_t prepare_log_id_;
  int64_t prepare_log_timestamp_;
  int64_t clear_log_base_ts_;
  uint64_t prev_checkpoint_id_;
};

struct CtxInfo final {
  CtxInfo() : ctx_(NULL)
  {}
  CtxInfo(ObTransCtx* ctx) : ctx_(ctx)
  {}
  TO_STRING_KV(KP(ctx_));
  ObTransCtx* ctx_;
};

typedef common::ObSEArray<CtxInfo, 9, TransModulePageAllocator> ObTransCtxArray;
class ObTransStateTable {
public:
  ObTransStateTable() : partition_trans_ctx_mgr_(NULL)
  {}
  ~ObTransStateTable()
  {
    reset();
  }
  void reset()
  {
    release_ref();
    partition_trans_ctx_mgr_ = NULL;
  }
  void acquire_ref(ObPartitionTransCtxMgr* part_mgr)
  {
    partition_trans_ctx_mgr_ = part_mgr;
  }
  void release_ref();
  int check_trans_table_valid(const common::ObPartitionKey& pkey, bool& valid) const;
  ObPartitionTransCtxMgr* get_partition_trans_ctx_mgr()
  {
    return partition_trans_ctx_mgr_;
  }
  // =============== Interface for sstable to get txn information =====================
  // check whether the row key is locked by txn DATA_TRANS_ID
  // return the existence of the rowkey and the commit version if committed
  int check_row_locked(const common::ObStoreRowkey& key, memtable::ObIMvccCtx& ctx,
      const transaction::ObTransID& read_trans_id, const transaction::ObTransID& data_trans_id,
      const int32_t sql_sequence, storage::ObStoreRowLockState& lock_state);
  // check whether txn DATA_TRANS_ID with SQL_SEQUENCE is readable
  // (SQL_SEQUENCE may be unreadable for txn or stmt rollback)
  // return whether can read
  int check_sql_sequence_can_read(
      const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read);
  // fetch the state of txn DATA_TRANS_ID when replaying to LOG_TS
  // return the txn state and commit version if committed, INT64_MAX if running
  // and 0 if rollbacked when replaying to LOG_ID
  int get_transaction_status_with_log_ts(const transaction::ObTransID& data_trans_id, const int64_t log_ts,
      ObTransTableStatusType& status, int64_t& trans_version);
  // the txn READ_TRANS_ID use SNAPSHOT_VERSION to read the data,
  // and check whether the data is locked, readable or unreadable
  // by txn DATA_TRANS_ID.
  // READ_LATEST is used to check whether read the data belong to
  // the same txn
  // return whether the data is readable, and corresponding state and version
  int lock_for_read(
      const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state);
  TO_STRING_KV(KP_(partition_trans_ctx_mgr));

private:
  ObPartitionTransCtxMgr* partition_trans_ctx_mgr_;
};

class ObTransStateTableGuard {
public:
  ObTransStateTableGuard()
  {
    reset();
  }
  ~ObTransStateTableGuard()
  {
    reset();
  }
  void reset()
  {
    trans_state_table_.reset();
  }
  int set_trans_state_table(ObPartitionTransCtxMgr* part_mgr);  // acquire_ref
  void release_ref()
  {
    trans_state_table_.release_ref();
  }
  ObTransStateTable& get_trans_state_table()
  {
    return trans_state_table_;
  }
  int check_trans_table_valid(const common::ObPartitionKey& pkey, bool& valid)
  {
    return trans_state_table_.check_trans_table_valid(pkey, valid);
  }
  TO_STRING_KV(K_(trans_state_table));

private:
  ObTransStateTable trans_state_table_;
};

typedef common::ObSEArray<ObPartitionLeaderInfo, 10, TransModulePageAllocator> ObTransLocationCache;
typedef common::ObSEArray<ObPartitionEpochInfo, 10, TransModulePageAllocator> ObPartitionEpochArray;
typedef common::ObSEArray<ObMemtableKeyInfo, 16, TransModulePageAllocator> ObMemtableKeyArray;
typedef common::ObSEArray<ObAddrLogId, 10, TransModulePageAllocator> ObAddrLogIdArray;
const int64_t OB_TRANS_REDO_LOG_RESERVE_SIZE = 128 * 1024;
const int64_t MAX_ONE_PC_TRANS_SIZE = 1500000;
// parameters config transaction related
const int64_t TRANS_ACCESS_STAT_INTERVAL = 60 * 1000 * 1000;  // 60s
const int64_t TRANS_MEM_STAT_INTERVAL = 5 * 1000 * 1000;      // 60s

// in elr transaction, curr_trans_commit_version - prev_trans_commit_version <= 400ms;
#ifdef ERRSIM
static const int64_t MAX_ELR_TRANS_INTERVAL = 20 * 1000 * 1000;
#else
static const int64_t MAX_ELR_TRANS_INTERVAL = 400 * 1000;
#endif

// max scheudler context in single server
static const int64_t MAX_SCHE_CTX_COUNT = 100 * 1000;
static const int64_t MAX_PART_CTX_COUNT = 700 * 1000;

static const int DUP_TABLE_LEASE_LIST_MAX_COUNT = 8;
#define TRANS_AGGRE_LOG_TIMESTAMP OB_INVALID_TIMESTAMP

bool is_single_leader(const ObTransLocationCache& trans_location_cache);

static const int64_t GET_GTS_AHEAD_INTERVAL = 300;
}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_
