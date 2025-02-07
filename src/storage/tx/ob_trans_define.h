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
#include "common/ob_clock_generator.h"
#include "common/ob_range.h"
#include "common/storage/ob_sequence.h"
#include "common/ob_tablet_id.h"
#include "lib/core_local/ob_core_local_storage.h"
#include "lib/list/ob_list.h"
#include "lib/trace/ob_trace_event.h"
#include "logservice/palf/lsn.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"
#include "share/ob_cluster_version.h"
#include "share/ob_ls_id.h"
#include "share/allocator/ob_reserve_arena.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_trans_result.h"
#include "storage/tx/ob_xa_define.h"
#include "storage/tx/ob_direct_load_tx_ctx_define.h"
#include "storage/tx/ob_multi_data_source_tx_buffer_node.h"
#include "storage/tx/ob_tx_on_demand_print.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
namespace detector
{
class ObDetectorInnerReportInfo;
}
}
namespace common
{
class ObAddr;
template<int64_t N> class ObFixedLengthString;
class ObString;
class ObStoreRowkey;
class ObTabletID;
}

namespace storage
{
struct ObStoreRowLockState;
}

namespace sql
{
class ObBasicSessionInfo;
struct ObAuditRecordData;
}

namespace clog
{
class ObAggreBuffer;
}

namespace memtable
{
class ObMvccAccessCtx;
class ObMemtableCtx;
}

namespace transaction
{
class ObLSTxCtxMgr;
class ObTransCtx;
class ObPartTransCtx;
class ObMemtableKeyInfo;
class AggreLogTask;
class ObXACtx;
class ObITxCallback;
class ObTxMultiDataSourceLog;
enum class NotifyType : int64_t;
typedef palf::LSN LogOffSet;
enum { MAX_CALLBACK_LIST_COUNT = 64 };
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

class ObReserveAllocator : public ObIAllocator
{
public:
  ObReserveAllocator() : pos_(0), size_(0) {}
  ~ObReserveAllocator() { reset(); }
  void *alloc(const int64_t sz)
  {
    return alloc_from_buf_(sz);
  }
  void* alloc(const int64_t sz, const ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc_from_buf_(sz);
  }
  void free(void *p)
  {
    // do nothing
  }
public:
  bool is_contain(void *p) const
  {
    return ((int64_t)p >= (int64_t)buf_) && ((int64_t)p < (int64_t)buf_ + size_);
  }
  void reset()
  {
    pos_ = 0;
    size_ = 0;
  }
  void reuse()
  {
    pos_ = 0;
  }
private:
  void *alloc_from_buf_(const int64_t sz)
  {
    void *ptr = NULL;
    const int64_t aligned_sz = ob_aligned_to2(sz, 16);
    if (pos_ + aligned_sz < size_) {
      ptr = reinterpret_cast<void *>(buf_ + pos_);
      pos_ = pos_ + aligned_sz;
    }
    return ptr;
  }
private:
  static const int64_t RESERVED_MEM_SIZE = 256;
private:
  char buf_[RESERVED_MEM_SIZE];
  int64_t pos_;
  int64_t size_;
};

class TransModulePageAllocator : public common::ModulePageAllocator
{
public:
  TransModulePageAllocator(const lib::ObLabel &label = "TransModulePage",
                           int64_t tenant_id = common::OB_SERVER_TENANT_ID,
                           int64_t ctx_id = 0)
    : ModulePageAllocator(label, tenant_id, ctx_id) {}
  explicit TransModulePageAllocator(common::ObIAllocator &allocator)
    : ModulePageAllocator(allocator) {}
  virtual ~TransModulePageAllocator() {}
  void *alloc(const int64_t sz)
  {
    void *ret = NULL;
    if (OB_UNLIKELY(ObTransErrsim::is_memory_errsim())) {
      ret = NULL;
    } else {
      ret = inner_alloc_(sz, attr_);
    }
    return ret;
  }
  void *alloc(const int64_t sz, const ObMemAttr &attr)
  {
    void *ret = NULL;
    if (OB_UNLIKELY(ObTransErrsim::is_memory_errsim())) {
      ret = NULL;
    } else {
      ret = inner_alloc_(sz, attr);
    }
    return ret;
  }
  void free(void *ptr)
  {
    if (NULL != ptr) {
      inner_free_(ptr);
    }
  }
  void reset()
  {
    common::ModulePageAllocator::reset();
    reserve_allocator_.reset();
  }
protected:
  void *inner_alloc_(const int64_t sz, const ObMemAttr &attr)
  {
    void *ptr = ModulePageAllocator::alloc(sz, attr);
    if (NULL == ptr) {
      ptr = reserve_allocator_.alloc(sz);
    }
    return ptr;
  }
  void inner_free_(void *ptr)
  {
    if (reserve_allocator_.is_contain(ptr)) {
      reserve_allocator_.free(ptr);
    } else {
      ModulePageAllocator::free(ptr);
    }
  }
private:
  ObReserveAllocator reserve_allocator_;
};

class ObTransID
{
  OB_UNIS_VERSION(1);
public:
  ObTransID() : tx_id_(0) {}
  ObTransID(const int64_t tx_id) : tx_id_(tx_id) {}
  ~ObTransID() { tx_id_ = 0; }
  ObTransID &operator=(const ObTransID &r) {
    if (this != &r) {
      tx_id_ = r.tx_id_;
    }
    return *this;
  }
  ObTransID &operator=(const int64_t &id) {
    tx_id_ = id;
    return *this;
  }
  bool operator<(const ObTransID &id) {
    bool bool_ret = false;
    if (this->compare(id) < 0) {
      bool_ret = true;
    }
    return bool_ret;
  }
  bool operator>(const ObTransID &id) {
    bool bool_ret = false;
    if (this->compare(id) > 0) {
      bool_ret = true;
    }
    return bool_ret;
  }
  int64_t get_id() const { return tx_id_; }
  uint64_t hash() const
  {
    return murmurhash(&tx_id_, sizeof(tx_id_), 0);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool is_valid() const { return tx_id_ > 0; }
  void reset() { tx_id_ = 0; }
  int compare(const ObTransID& other) const;
  operator int64_t() const { return tx_id_; }
  bool operator==(const ObTransID &other) const
  { return tx_id_ == other.tx_id_; }
  bool operator!=(const ObTransID &other) const
  { return tx_id_ != other.tx_id_; }
  /*  XA  */
  int parse(char *b) {
    UNUSED(b);
    return OB_SUCCESS;
  }
  TO_STRING_AND_YSON(OB_ID(txid), tx_id_);
private:
  int64_t tx_id_;
};

struct ObLockForReadArg
{
  ObLockForReadArg(memtable::ObMvccAccessCtx &acc_ctx,
                   ObTransID data_trans_id,
                   ObTxSEQ data_sql_sequence,
                   bool read_latest,
                   bool read_uncommitted,
                   share::SCN scn)
    : mvcc_acc_ctx_(acc_ctx),
    data_trans_id_(data_trans_id),
    data_sql_sequence_(data_sql_sequence),
    read_latest_(read_latest),
    read_uncommitted_(read_uncommitted),
    scn_(scn) {}

  DECLARE_TO_STRING;

  memtable::ObMvccAccessCtx &mvcc_acc_ctx_;
  ObTransID data_trans_id_;
  ObTxSEQ data_sql_sequence_;
  bool read_latest_;
  bool read_uncommitted_;
  // Compare with transfer_start_scn, sstable is end_scn, and memtable is ObMvccTransNode scn
  share::SCN scn_;
};

class ObTransKey final
{
public:
  ObTransKey() { /*reset();*/ }
  ObTransKey(const ObTransKey &other)
  {
    ls_id_ = other.ls_id_;
    trans_id_ = other.trans_id_;
    hash_val_ = other.hash_val_;
  }
  explicit ObTransKey(const share::ObLSID &ls_id, const ObTransID &trans_id)
      : ls_id_(ls_id), trans_id_(trans_id), hash_val_(0) { calc_hash_(); }
public:
  bool is_valid() const { return ls_id_.is_valid() && trans_id_.is_valid(); }
  ObTransKey &operator=(const ObTransKey &other)
  {
    ls_id_ = other.ls_id_;
    trans_id_ = other.trans_id_;
    hash_val_ = other.hash_val_;
    return *this;
  }
  bool operator==(const ObTransKey &other) const
  { return hash_val_ == other.hash_val_ && ls_id_ == other.ls_id_ &&  trans_id_ == other.trans_id_; }
  bool operator!=(const ObTransKey &other) const
  { return hash_val_ != other.hash_val_ || ls_id_ != other.ls_id_ || trans_id_ != other.trans_id_; }
  OB_INLINE uint64_t hash() const { return hash_val_; }
  int compare(const ObTransKey& other) const
  {
    int compare_ret = 0;
    compare_ret = ls_id_.compare(other.ls_id_);
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
    ls_id_.reset();
    trans_id_.reset();
    hash_val_ = 0;
  }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const ObTransID get_trans_id() const { return trans_id_; }
  DECLARE_TO_STRING_AND_YSON;
private:
  OB_INLINE uint64_t calc_hash_()
  {
    uint64_t hash_val = 0;
    uint64_t trans_id_hash_val = 0;
    uint64_t ls_val = 0;
    trans_id_hash_val = trans_id_.hash();
    ls_val = ls_id_.hash();
    hash_val = common::murmurhash(&trans_id_hash_val, sizeof(trans_id_hash_val), ls_val);
    return hash_val_ = hash_val;
  }
private:
  share::ObLSID ls_id_;
  ObTransID trans_id_;
  uint64_t hash_val_;
};

class ObTransAccessMode
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t READ_ONLY = 0;
  static const int32_t READ_WRITE = 1;
public:
  static bool is_valid(const int32_t mode)
  { return READ_ONLY == mode || READ_WRITE == mode; }
private:
  ObTransAccessMode() {}
  ~ObTransAccessMode() {}
};

class ObTransSnapshotGeneType
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t CONSULT = 0;
  static const int32_t APPOINT = 1;
  static const int32_t NOTHING = 2;
public:
  static bool is_valid(const int32_t type)
  { return CONSULT == type
      || APPOINT == type
      || NOTHING == type; }
  static const char *cstr(int32_t type)
  {
    const char *ret_str = "UNKNOWN";
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
  ObTransSnapshotGeneType() {}
  ~ObTransSnapshotGeneType() {}
};

enum TransType : int32_t
{
  UNKNOWN_TRANS = -1,
  SP_TRANS = 0,
  DIST_TRANS = 2
};

const char *trans_type_to_cstr(const TransType &trans_type);

// class TransType
// {
// public:
//   static const int32_t UNKNOWN = -1;
//   static const int32_t SP_TRANS = 0;
//   static const int32_t MINI_SP_TRANS = 1;
//   static const int32_t DIST_TRANS = 2;
//   static const int32_t EMPTY_TRANS = 3;
// public:
//   static bool is_valid(const int32_t type)
//   { return SP_TRANS == type || MINI_SP_TRANS == type || DIST_TRANS == type || EMPTY_TRANS == type; }
// private:
//   TransType() {}
//   ~TransType() {}
// };
//
class ObTransType
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t TRANS_NORMAL = 0;
  static const int32_t TRANS_MAJOR_FREEZE = 1;
  static const int32_t TRANS_SYSTEM = 2;
  static const int32_t TRANS_USER = 3;
public:
  static bool is_valid(const int32_t type)
  { return TRANS_NORMAL == type || TRANS_MAJOR_FREEZE == type
           || TRANS_SYSTEM == type || TRANS_USER == type; }
private:
  ObTransType() {}
  ~ObTransType() {}
};

enum class ObGlobalTxType : uint8_t
{
  PLAIN = 0,
  XA_TRANS = 1,
  DBLINK_TRANS = 2,
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

class ObTransConsistencyType
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t CURRENT_READ = 0;
  static const int32_t BOUNDED_STALENESS_READ = 1;
public:
  static bool is_valid(const int64_t consistency_type)
  { return CURRENT_READ == consistency_type || BOUNDED_STALENESS_READ == consistency_type; }
  static bool is_bounded_staleness_read(const int64_t consistency_type)
  {
    return BOUNDED_STALENESS_READ == consistency_type;
  }
  static bool is_current_read(const int64_t consistency_type)
  {
    return CURRENT_READ == consistency_type;
  }
  static const char *cstr(const int64_t consistency_type)
  {
    const char *str = "UNKNOWN";
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
  ObTransConsistencyType() {}
  ~ObTransConsistencyType() {}
};

class ObTransReadSnapshotType
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t TRANSACTION_SNAPSHOT = 0;
  static const int32_t STATEMENT_SNAPSHOT = 1;
  static const int32_t PARTICIPANT_SNAPSHOT = 2;
public:
  static bool is_valid(const int32_t type)
  {
    return TRANSACTION_SNAPSHOT == type
        || STATEMENT_SNAPSHOT == type
        || PARTICIPANT_SNAPSHOT == type;
  }
  static bool is_consistent_snapshot(const int32_t type)
  {
    return TRANSACTION_SNAPSHOT == type || STATEMENT_SNAPSHOT == type;
  }
  static const char *cstr(const int64_t type)
  {
    const char *str = "UNKNOWN";
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

// transaction parameter
class ObStartTransParam  // unreferenced, need remove
{
  OB_UNIS_VERSION(1);
public:
  const static uint64_t INVALID_CLUSTER_VERSION = 0;
public:
  ObStartTransParam() { reset(); }
  ~ObStartTransParam() { magic_ = INVALID_MAGIC_NUM; }
  void reset();
public:
  void set_expire_ts(const int64_t expired_ts)
  { expired_ts_ = expired_ts; }
  int64_t get_expire_ts() const { return expired_ts_; }
  int set_access_mode(const int32_t access_mode);
  int32_t get_access_mode() const { return access_mode_; }
  int set_type(const int32_t type);
  int32_t get_type() const { return type_; }
  int set_isolation(const int32_t isolation);
  int32_t get_isolation() const { return isolation_; }
  void set_autocommit(const bool autocommit) { autocommit_ = autocommit; }
  bool is_autocommit() const { return autocommit_; }
  void set_consistency_type(const int32_t consistency_type)
  { consistency_type_ = consistency_type; }
  int32_t get_consistency_type() const { return consistency_type_; }
  bool is_bounded_staleness_read() const
  { return ObTransConsistencyType::is_bounded_staleness_read(consistency_type_); }
  bool is_current_read() const
  { return ObTransConsistencyType::is_current_read(consistency_type_); }
  void set_read_snapshot_type(const int32_t type) { read_snapshot_type_ = type; }
  int32_t get_read_snapshot_type() const { return read_snapshot_type_; }
  void set_cluster_version(uint64_t cluster_version) { cluster_version_ = cluster_version; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  void set_inner_trans(const bool is_inner_trans) { is_inner_trans_ = is_inner_trans; }
  bool is_inner_trans() const { return is_inner_trans_; }
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
  bool is_readonly() const { return ObTransAccessMode::READ_ONLY == access_mode_; }
  bool is_valid() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
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
  int64_t expired_ts_;
};

//record the info from user to db
class ObTraceInfo
{
public:
  ObTraceInfo():
    app_trace_info_(sizeof(app_trace_info_buffer_), 0, app_trace_info_buffer_),
    app_trace_id_(sizeof(app_trace_id_buffer_), 0, app_trace_id_buffer_) { }
  ~ObTraceInfo() {}
  void reset();
  //app trace info
  int set_app_trace_info(const common::ObString &app_trace_info);
  const common::ObString &get_app_trace_info() const { return app_trace_info_; }
  //app trace id
  int set_app_trace_id(const common::ObString &app_trace_id);
  const common::ObString &get_app_trace_id() const { return app_trace_id_; }
  common::ObString &get_app_trace_id() { return app_trace_id_; }
  TO_STRING_KV(K_(app_trace_info), K_(app_trace_id));
private:
  static const int64_t MAX_TRACE_INFO_BUFFER = 128;
private:
  char app_trace_info_buffer_[MAX_TRACE_INFO_BUFFER + 1];
  common::ObString app_trace_info_;
  char app_trace_id_buffer_[common::OB_MAX_TRACE_ID_BUFFER_SIZE + 1];
  common::ObString app_trace_id_;
};

class ObTransConsistencyLevel
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t STRONG = 0;
  static const int64_t WEAK = 1;
public:
  static bool is_valid(const int64_t type)
  { return STRONG == type || WEAK == type; }
private:
  ObTransConsistencyLevel() {}
  ~ObTransConsistencyLevel() {}
};

typedef common::ObReserveArenaAllocator<1024> ObTxReserveArenaAllocator;

class ObTransTraceLog : public common::ObTraceEventRecorder
{
public:
  ObTransTraceLog()
      : common::ObTraceEventRecorder::ObTraceEventRecorderBase(
          true, common::ObLatchIds::TRANS_TRACE_RECORDER_LOCK) {}
  ~ObTransTraceLog() {}
  void destroy() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t ret = 0;
    check_lock();
    ret = ObTraceEventRecorder::to_string(buf, buf_len);
    check_unlock();
    return ret;
  }
};

class ObStmtInfo  // unreferenced, need remove
{
  OB_UNIS_VERSION(1);
public:
  ObStmtInfo() : nested_sql_(false), start_stmt_cnt_(0), end_stmt_cnt_(0) { }
  ~ObStmtInfo() {}
  void reset();
  int set_nested_sql() { nested_sql_ = true; return common::OB_SUCCESS; }
  bool is_nested_sql() const { return nested_sql_; }
  int inc_start_stmt_cnt() { start_stmt_cnt_++; return common::OB_SUCCESS; }
  int64_t get_start_stmt_cnt() const { return start_stmt_cnt_; }
  int inc_end_stmt_cnt() { end_stmt_cnt_++; return common::OB_SUCCESS; }
  int64_t get_end_stmt_cnt() const { return end_stmt_cnt_; }
  void reset_stmt_info();
  TO_STRING_KV(K_(nested_sql), K_(start_stmt_cnt), K_(end_stmt_cnt));
private:
  bool nested_sql_;
  int64_t start_stmt_cnt_;
  int64_t end_stmt_cnt_;
};

class ObTaskInfo // unreferenced, need remove
{
  OB_UNIS_VERSION(1);
public:
  ObTaskInfo()
      : sql_no_(0),
        active_task_cnt_(1),
        snapshot_version_(common::OB_INVALID_VERSION),
        seq_no_(0) {}
  ObTaskInfo(const int32_t sql_no, const int64_t seq_no, const int64_t snapshot_version)
      : sql_no_(sql_no),
        active_task_cnt_(1),
        snapshot_version_(snapshot_version),
        seq_no_(seq_no) {}
  bool is_task_match() const { return 0 == active_task_cnt_; }
  TO_STRING_KV(K_(sql_no), K_(seq_no), K_(active_task_cnt), K_(snapshot_version));
public:
  int32_t sql_no_;
  int32_t active_task_cnt_;
  int64_t snapshot_version_;
  int64_t seq_no_;
};

class ObTransVersion
{
public:
  static const int64_t INVALID_TRANS_VERSION = -1;
  static const int64_t MAX_TRANS_VERSION = INT64_MAX;
public:
  static bool is_valid(const int64_t trans_version) { return trans_version >= 0; }
};

typedef ObMonotonicTs MonotonicTs;

class ObTransNeedWaitWrap
{
public:
  ObTransNeedWaitWrap() { reset(); }
  ~ObTransNeedWaitWrap() { destroy(); }
  void reset()
  {
    receive_gts_ts_.reset();
    need_wait_interval_us_ = 0;
  }
  void destroy() { reset(); }
  int64_t get_remaining_wait_interval_us() const;
  void set_trans_need_wait_wrap(const MonotonicTs receive_gts_ts,
                                const int64_t need_wait_interval_us);
  MonotonicTs get_receive_gts_ts() const { return receive_gts_ts_; }
  int64_t get_need_wait_interval_us() const { return need_wait_interval_us_; }
  bool need_wait() const { return get_remaining_wait_interval_us() > 0; }
  TO_STRING_KV(K(receive_gts_ts_), K_(need_wait_interval_us));
private:
  MonotonicTs receive_gts_ts_;
  int64_t need_wait_interval_us_;
};

class ObTransSnapInfo //unused , to be removed
{
  OB_UNIS_VERSION(1);
public:
  ObTransSnapInfo() { reset(); }
  ~ObTransSnapInfo() { destroy(); }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  void set_snapshot_version(const int64_t snapshot_version)
  {
    snapshot_version_ = snapshot_version;
  }
  void set_read_seq_no(const int64_t seq_no) { read_seq_no_ = sql_no_ = seq_no; }
  int64_t get_read_seq_no() const { return read_seq_no_; }
  void set_trans_id(const ObTransID &trans_id)
  {
    trans_id_ = trans_id;
  }
  const ObTransID &get_trans_id() const { return trans_id_; }
  void set_is_cursor_or_nested(const bool is_cursor_or_nested)
  {
    is_cursor_or_nested_ = is_cursor_or_nested;
  }
  bool is_cursor_or_nested() const { return is_cursor_or_nested_; }
  void reset()
  {
    snapshot_version_ = common::OB_INVALID_VERSION;
    read_seq_no_ = 0;
    sql_no_ = 0;
    trans_id_.reset();
    is_cursor_or_nested_ = false;
  }
  void destroy() { reset(); }
public:
  bool is_valid() const;
  TO_STRING_KV(K_(snapshot_version), K_(read_seq_no), K_(sql_no), K_(trans_id), K_(is_cursor_or_nested));
private:
  int64_t snapshot_version_;
  int64_t read_seq_no_;
  int64_t sql_no_; // deprecated, for compact reason only
  ObTransID trans_id_;
  bool is_cursor_or_nested_;
};

class ObStmtPair //unused , to be removed
{
  OB_UNIS_VERSION(1);
public:
  ObStmtPair() : from_(0), to_(0) {}
  ObStmtPair(const int64_t from, const int64_t to) : from_(from), to_(to) {}
  ~ObStmtPair() {}
  bool is_valid() const { return (from_ > to_) && (to_ >= 0); }
  int64_t get_from() const { return from_; }
  int64_t &get_from() { return from_; }
  int64_t get_to() const { return to_; }
  TO_STRING_KV(K_(from), K_(to));
private:
  int64_t from_;
  int64_t to_;
};

class ObStmtRollbackInfo //unused , to be removed
{
  OB_UNIS_VERSION(1);
public:
  ObStmtRollbackInfo() {}
  ~ObStmtRollbackInfo() {}
  void reset();
public:
  int push(const ObStmtPair &stmt_pair);
  int search(const int64_t sql_no, ObStmtPair &stmt_pair) const;
  int assign(const ObStmtRollbackInfo &other);
  TO_STRING_KV(K_(stmt_pair_array));
private:
  common::ObSArray<ObStmtPair> stmt_pair_array_;
};

class ObTransDesc;

class ObSQLTransAction
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t START_TRANS = 0;
  static const int64_t END_TRANS = 1;
  static const int64_t MAX = 2;
public:
  static bool is_valid(const int64_t action)
  { return action >= START_TRANS && action <= END_TRANS; }
  static bool is_in_transaction(const int64_t action)
  { return action == START_TRANS; }
};

static const int64_t DEFAULT_BLOCKED_TRANS_ID_COUNT = 1;
typedef common::ObSEArray<ObTransID, DEFAULT_BLOCKED_TRANS_ID_COUNT> ObBlockedTransArray;

class ObTransDesc
{
  OB_UNIS_VERSION(1);
public:
  void reset() {}
  void destroy() {}
  bool is_valid() const { return false; }
  TO_STRING_KV("hello", "world");
  int a_;
};

struct ObTransExecResult {};

class ObTransIsolation
{
public:
  enum {
    /*
     * after the discussion with yanran and zehuan, we decide to adjust the value of
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
    return level == READ_UNCOMMITTED
        || level == READ_COMMITED
        || level == REPEATABLE_READ
        || level == SERIALIZABLE;
  }
  static int32_t get_level(const common::ObString &level_name);
  static const common::ObString &get_name(int32_t level);
private:
  ObTransIsolation() {}
  ~ObTransIsolation() {}
};

class ObPartTransAction
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 1;
  static const int64_t START = 2;
  static const int64_t COMMIT = 3;
  static const int64_t ABORT = 4;
  static const int64_t DIED = 5;
  static const int64_t END = 6;
public:
  static bool is_valid(const int64_t state)
  { return state >= START && state < END; }
private:
  ObPartTransAction() {}
  ~ObPartTransAction() {}
};

class ObRunningState
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 0;
  static const int64_t ABORT = 200;
public:
  static bool is_valid(const int64_t state)
  { return INIT == state || ABORT == state; }
private:
  ObRunningState() {}
  ~ObRunningState() {}
};

// sub state is volatile state of tx ctx
class ObTxSubState
{
public:
  ObTxSubState() : flag_() {}
  ~ObTxSubState() {}
  void reset() { flag_.reset(); }

  bool is_info_log_submitted() const { return flag_.info_log_submitted_; }
  void set_info_log_submitted() { flag_.info_log_submitted_ = 1; }
  void clear_info_log_submitted() { flag_.info_log_submitted_ = 0; }

  bool is_gts_waiting() const { return flag_.gts_waiting_; }
  void set_gts_waiting() { flag_.gts_waiting_ = 1; }
  void clear_gts_waiting() { flag_.gts_waiting_ = 0; }

  bool is_state_log_submitting() const { return flag_.state_log_submitting_; }
  void set_state_log_submitting() { flag_.state_log_submitting_ = 1; }
  void clear_state_log_submitting() { flag_.state_log_submitting_ = 0; }

  bool is_state_log_submitted() const { return flag_.state_log_submitted_; }
  void set_state_log_submitted() { flag_.state_log_submitted_ = 1; }
  void clear_state_log_submitted() { flag_.state_log_submitted_ = 0; }

  bool is_prepare_notified() const { return flag_.prepare_notify_; }
  void set_prepare_notified() { flag_.prepare_notify_ = 1; }
  void clear_prepare_notified() { flag_.prepare_notify_ = 0; }

  bool is_force_abort() const { return flag_.force_abort_; }
  void set_force_abort() { flag_.force_abort_ = 1; }
  void clear_force_abort() { flag_.force_abort_ = 0; }

  bool is_transfer_blocking() const { return flag_.transfer_blocking_; }
  void set_transfer_blocking() { flag_.transfer_blocking_ = 1; }
  void clear_transfer_blocking() { flag_.transfer_blocking_ = 0; }

  DECLARE_ON_DEMAND_TO_STRING
  TO_STRING_KV("info_log_submitted",
               flag_.info_log_submitted_,
               "gts_waiting",
               flag_.gts_waiting_,
               "state_log_submitting",
               flag_.state_log_submitting_,
               "state_log_submitted",
               flag_.state_log_submitted_,
               // "prepare_notify",
               // flag_.prepare_notify_,
               "force_abort",
               flag_.force_abort_,
               "transfer_blocking",
               flag_.transfer_blocking_);

  bool is_valid() const { return flag_.is_valid(); }
private:
  struct BitFlag
  {
    unsigned int info_log_submitted_ : 1;
    unsigned int gts_waiting_ : 1;
    unsigned int state_log_submitting_ : 1;
    unsigned int state_log_submitted_ : 1;
    unsigned int prepare_notify_ : 1;
    unsigned int force_abort_ : 1;
    unsigned int transfer_blocking_ : 1;

    void reset()
    {
      info_log_submitted_ = 0;
      gts_waiting_ = 0;
      state_log_submitting_ = 0;
      state_log_submitted_ = 0;
      prepare_notify_ = 0;
      force_abort_ = 0;
      transfer_blocking_ = 0;
    }

    bool is_valid() const
    {
      return info_log_submitted_ > 0 || gts_waiting_ > 0 || state_log_submitted_ > 0
          || state_log_submitting_ > 0 || prepare_notify_ > 0 || force_abort_ > 0
          || transfer_blocking_ > 0;
    }

    BitFlag() { reset(); }
  } flag_;
};

class Ob2PCPrepareState
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t INIT = 0;
  static const int64_t REDO_PREPARING = 11;
  static const int64_t REDO_PREPARED = 12;
  static const int64_t VERSION_PREPARING = 13;
  static const int64_t VERSION_PREPARED = 14;
public:
  static bool is_valid(const int64_t state)
  { return state >= INIT && state <= VERSION_PREPARED; }
  static bool in_state(const int64_t state)
  { return state >= REDO_PREPARING && state <= VERSION_PREPARED; }
  static bool is_redo_prepared(const int64_t state)
  { return state >= REDO_PREPARED; }
private:
  Ob2PCPrepareState() {}
  ~Ob2PCPrepareState() {}
};

class ObTransSubmitLogState
{
public:
  static const int64_t INIT = 0;
  static const int64_t SUBMIT_LOG = 1;
  static const int64_t SUBMIT_LOG_PENDING = 2;
  static const int64_t SUBMIT_LOG_SUCCESS = 3;
public:
  bool is_valid(const int64_t state)
  { return INIT == state || SUBMIT_LOG == state
      || SUBMIT_LOG_PENDING == state || SUBMIT_LOG_SUCCESS == state; }
};

class ObTransRetryTaskType
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t END_TRANS_CB_TASK = 0;
  static const int64_t ADVANCE_LS_CKPT_TASK = 1;
  static const int64_t STANDBY_CLEANUP_TASK = 2;
  static const int64_t DUP_TABLE_TX_REDO_SYNC_RETRY_TASK = 3;
  static const int64_t MAX = 4;
public:
  static bool is_valid(const int64_t task_type)
  { return task_type > UNKNOWN && task_type < MAX; }
};

class ObTransCtxType
{
public:
  static const int64_t UNKNOWN = -1;
  static const int64_t SCHEDULER = 0;
  static const int64_t COORDINATOR = 1;
  static const int64_t PARTICIPANT = 2;
  static const int64_t SLAVE_PARTICIPANT = 3;
public:
  static bool is_valid(const int64_t type)
  { return SCHEDULER == type || COORDINATOR == type || PARTICIPANT == type
          || SLAVE_PARTICIPANT == type; }
};

typedef common::ObSArray<int64_t> ObLeaderEpochArray;

class ObMemtableKeyInfo
{
public:
  ObMemtableKeyInfo() { reset(); };
  ~ObMemtableKeyInfo() {};
  int init(const uint64_t hash_val);
  void reset();
  char *get_buf() { return buf_; }
  const char *read_buf() const { return buf_; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  uint64_t get_hash_val() const { return hash_val_; }
  const void *get_row_lock() const { return row_lock_; }
  void set_row_lock(void *row_lock) { row_lock_ = row_lock; }
  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  bool operator==(const ObMemtableKeyInfo &other) const;

  TO_STRING_KV(K_(buf));

public:
  static const int MEMTABLE_KEY_INFO_BUF_SIZE = 128;

private:
  ObTabletID tablet_id_;
  uint64_t hash_val_;
  void *row_lock_;
  char buf_[MEMTABLE_KEY_INFO_BUF_SIZE];
};

class ObElrTransInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObElrTransInfo() { reset(); }
  void reset();
  int init(const ObTransID &trans_id, uint32_t ctx_id, const share::SCN commit_version);
  const ObTransID &get_trans_id() const { return trans_id_; }
  uint32_t get_ctx_id() const { return ctx_id_; }
  share::SCN get_commit_version() const { return commit_version_; }
  int set_result(const int result) { result_ = result; return common::OB_SUCCESS; }
  uint64_t hash() const { return trans_id_.hash(); }
  int get_result() const { return ATOMIC_LOAD(&result_); }
  bool cas_result(int o, int n) { return ATOMIC_BCAS(&result_, o, n); }
  TO_STRING_KV(K_(trans_id), K_(commit_version), K_(result), K_(ctx_id));
private:
  ObTransID trans_id_;
  share::SCN commit_version_;
  int result_;
  uint32_t ctx_id_;
};

class ObTransResultState
{
public:
  static const int INVALID = -1;
  static const int UNKNOWN = 0;
  static const int COMMIT = 1;
  static const int ABORT = 2;
  static const int MAX = 3;
public:
  static bool is_valid(const int state)
  { return state > INVALID && state < MAX; }
  static bool is_unknown(const int state)
  { return UNKNOWN == state; }
  static bool is_commit(const int state)
  { return COMMIT == state; }
  static bool is_abort(const int state)
  { return ABORT == state; }
  static bool is_decided_state(const int state)
  { return COMMIT == state || ABORT == state; }
};

class ObTransTask
{
public:
  ObTransTask() : retry_interval_us_(0), next_handle_ts_(0), task_type_(ObTransRetryTaskType::UNKNOWN) {}
  ObTransTask(int64_t task_type) : retry_interval_us_(0), next_handle_ts_(0), task_type_(task_type) {}
  virtual ~ObTransTask() { destroy(); }
  void reset();
  void destroy() { reset(); }
  int make(const int64_t task_type);
  int set_retry_interval_us(const int64_t start_interval_us, const int64_t retry_interval_us);
  int64_t get_task_type() const { return task_type_; }
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

class ObBatchCommitState
{
public:
  static const int INVALID = -1;
  static const int INIT = 0;
  static const int ALLOC_LOG_ID_TS = 1;
  static const int GENERATE_PREPARE_LOG = 2;
  static const int GENERATE_REDO_PREPARE_LOG = 3;
  static const int BATCH_COMMITTED = 4;
};

enum ObPartitionAuditOperator
{
  PART_AUDIT_SET_BASE_ROW_COUNT = 0,
  PART_AUDIT_INSERT_ROW,
  PART_AUDIT_DELETE_ROW,
  PART_AUDIT_UPDATE_ROW, // put included
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

struct ObPartitionAuditInfoCache
{
public:
  ObPartitionAuditInfoCache(){ reset(); }
  ~ObPartitionAuditInfoCache(){}
  void reset() {
    insert_row_count_ = 0;
    delete_row_count_ = 0;
    update_row_count_ = 0;
    cur_insert_row_count_ = 0;
    cur_delete_row_count_ = 0;
    cur_update_row_count_ = 0;
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
};

struct ObPartitionAuditInfo
{
public:
  ObPartitionAuditInfo() : lock_(common::ObLatchIds::PARTITION_AUDIT_SPIN_LOCK) { reset(); }
  ~ObPartitionAuditInfo(){}
  void reset() {
    base_row_count_ = 0;
    insert_row_count_ = 0;
    delete_row_count_ = 0;
    update_row_count_ = 0;
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
  }
  void destroy() { reset(); }
  ObPartitionAuditInfo& operator=(const ObPartitionAuditInfo &other);
  ObPartitionAuditInfo& operator+=(const ObPartitionAuditInfo &other);
  int update_audit_info(const ObPartitionAuditInfoCache &cache, const bool commit);
  void set_base_row_count(int64_t count) { ATOMIC_SET(&base_row_count_, count); }
public:
  common::ObSpinLock lock_;
  int64_t base_row_count_;
  int64_t insert_row_count_;
  int64_t delete_row_count_;
  int64_t update_row_count_;
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
};

class ObCoreLocalPartitionAuditInfo : public common::ObCoreLocalStorage<ObPartitionAuditInfo*>
{
public:
  ObCoreLocalPartitionAuditInfo() {}
  ~ObCoreLocalPartitionAuditInfo() { destroy(); }
  int init(int64_t array_len);
  void reset();
  void destroy() { reset(); }
};

// CHANGING_LEADER_STATE state machine
//
// Original state when no leader transfer is on going:                NO_CHANGING_LEADER
// If stmt info is not matched during preparing change leader:        NO_CHANGING_LEADER -> STATEMENT_NOT_FINISH
// - If stmt info matches before leader transfer and submit log:      STATEMENT_NOT_FINISH -> NO_CHANGING_LEADER
//   - if exists a on-the-fly log during submit STATE log:            STATEMENT_NOT_FINISH -> LOGGING_NOT_FINISH
// If there exists a on-the-fly log during submit STATE log:          NO_CHANGING_LEADER -> LOGGING_NOT_FINISH
// - If the prev log is synced before leader transfer and submit log: LOGGING_NOT_FINISH -> NO_CHANGING_LEADER
// If the leader revokes:                                             STATEMENT_NOT_FINISH/LOGGING_NOT_FINISH -> NO_CHANGING_LEADER
enum CHANGING_LEADER_STATE
{
  NO_CHANGING_LEADER = 0,
  STATEMENT_NOT_FINISH = 1,
  LOGGING_NOT_FINISH = 2
};

class ObAddrLogId
{
public:
  ObAddrLogId(const common::ObAddr &addr, const uint64_t log_id)
    : addr_(addr), log_id_(log_id) {}
  ObAddrLogId() { reset(); }
  ~ObAddrLogId() { destroy(); }
  void reset();
  void destroy() { reset(); }
  bool operator==(const ObAddrLogId &other) const;
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_log_id() const { return log_id_; }
  TO_STRING_KV(K_(addr), K_(log_id));

private:
  common::ObAddr addr_;
  uint64_t log_id_;
};

enum class ObTransTableStatusType:int64_t
{
  RUNNING = 0,
  COMMIT,
  ABORT
};

/*
 * Undo range : (to, from]
 */
class ObUndoAction
{
  OB_UNIS_VERSION(1);
public:
  ObUndoAction() { reset(); }
  ObUndoAction(const ObTxSEQ undo_from, const ObTxSEQ undo_to)
      : undo_from_(undo_from), undo_to_(undo_to) {}
  ~ObUndoAction() { destroy(); }
  void reset()
  {
    undo_from_.reset();
    undo_to_.reset();
  }
  void destroy() { reset(); }
  bool is_valid() const
  {
    return (undo_from_.is_valid()
            && undo_to_.is_valid()
            && undo_from_.get_branch() == undo_to_.get_branch()
            && undo_from_ > undo_to_);
  }

  bool is_contain(const ObTxSEQ seq_no) const
  {
    const bool can_cmp = undo_to_.get_branch() == 0
      || undo_to_.get_branch() == seq_no.get_branch();
    return can_cmp
      && seq_no.get_seq() > undo_to_.get_seq()
      && seq_no.get_seq() <= undo_from_.get_seq();
  }

  bool is_contain(const ObUndoAction &other) const
  {
    const bool can_cmp = undo_to_.get_branch() == 0
      || undo_to_.get_branch() == other.undo_to_.get_branch();
    return can_cmp &&
      undo_from_.get_seq() >= other.undo_from_.get_seq()
      && undo_to_.get_seq() <= other.undo_to_.get_seq();
  }
  TO_STRING_KV(K_(undo_from), K_(undo_to));
public:
  // from > to
  ObTxSEQ undo_from_; // inclusive
  ObTxSEQ undo_to_;   // exclusive
};

class ObLSLogInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObLSLogInfo() : id_(), offset_() {}
  ObLSLogInfo(const share::ObLSID &id, const palf::LSN &offset) : id_(id), offset_(offset) {}
  const share::ObLSID &get_ls_id() const { return id_; }
  const palf::LSN &get_lsn() const { return offset_; }
  bool is_valid() const
  {
    return id_.is_valid() && offset_.is_valid();
  }

  bool operator==(const ObLSLogInfo &log_info) const
  {
    return id_ == log_info.id_ && offset_ == log_info.offset_;
  }

  TO_STRING_KV(K(id_), K(offset_))
private:
  share::ObLSID id_;
  palf::LSN offset_;
};

struct ObTxExecPart
{
  OB_UNIS_VERSION(1);
public:
  ObTxExecPart() : ls_id_(),
                     exec_epoch_(-1),
                     transfer_epoch_(-1) {}
  ObTxExecPart(share::ObLSID ls_id, int64_t epoch, int64_t transfer_epoch)
                   : ls_id_(ls_id),
                     exec_epoch_(epoch),
                     transfer_epoch_(transfer_epoch) {}
  inline bool operator==(const ObTxExecPart &other) const {
    return other.ls_id_ == ls_id_ &&
           other.exec_epoch_ == exec_epoch_ &&
           other.transfer_epoch_ == transfer_epoch_;
  }
  bool is_valid() const {
    return    ls_id_.is_valid()
           && (exec_epoch_ > 0
           || transfer_epoch_ > 0);
  }
  share::ObLSID ls_id_;
  int64_t exec_epoch_;
  int64_t transfer_epoch_;

  TO_STRING_KV(K_(ls_id), K_(exec_epoch), K_(transfer_epoch));
};

struct ObStandbyCheckInfo
{
  OB_UNIS_VERSION(1);
public:
  ObStandbyCheckInfo() :
      check_info_ori_ls_id_(-1),
      check_part_()
  {}
  ~ObStandbyCheckInfo() {}
  bool operator==(const ObStandbyCheckInfo &other) const {
    bool bool_ret =    check_info_ori_ls_id_ == other.check_info_ori_ls_id_
                    && check_part_ == other.check_part_;
    return bool_ret;
  }
  void operator=(const ObStandbyCheckInfo &other) {
    check_info_ori_ls_id_ = other.check_info_ori_ls_id_;
    check_part_ = other.check_part_;
  }
  bool is_valid() const { return check_info_ori_ls_id_.is_valid()
                                 && check_part_.is_valid(); }
  share::ObLSID check_info_ori_ls_id_; // those carrry check info origin ls id
  ObTxExecPart check_part_;
  TO_STRING_KV(K_(check_info_ori_ls_id), K_(check_part));
};

class ObStateInfo
{
public:
  ObStateInfo() : state_(ObTxState::UNKNOWN), version_(), snapshot_version_(), check_info_() {}
  ObStateInfo(const share::ObLSID &ls_id,
              const ObTxState &state,
              const share::SCN &version,
              const share::SCN &snapshot_version) :
              ls_id_(ls_id), state_(state)
  {
    version_ = version;
    snapshot_version_ = snapshot_version;
  }
  ~ObStateInfo() {}
  bool is_valid() const
  {
    return ls_id_.is_valid() && version_.is_valid() && snapshot_version_.is_valid();  }
  void operator=(const ObStateInfo &state_info)
  {
    ls_id_ = state_info.ls_id_;
    state_ = state_info.state_;
    version_ = state_info.version_;
    snapshot_version_ = state_info.snapshot_version_;
    check_info_ = state_info.check_info_;
  }

  bool need_update(const ObStateInfo &state_info);
  TO_STRING_KV(K_(ls_id), K_(state), K_(version), K_(snapshot_version), K_(check_info))
  OB_UNIS_VERSION(1);
public:
  share::ObLSID ls_id_;
  ObTxState state_;
  share::SCN version_;
  share::SCN snapshot_version_;
// for epoch check
  ObStandbyCheckInfo check_info_;
};

typedef common::ObSEArray<ObElrTransInfo, 1, TransModulePageAllocator> ObElrTransInfoArray;
typedef common::ObSEArray<int64_t, 10, TransModulePageAllocator> ObRedoLogIdArray;
typedef common::ObSEArray<palf::LSN, 10, ModulePageAllocator> ObRedoLSNArray;
typedef common::ObSEArray<ObLSLogInfo, 10, ModulePageAllocator> ObLSLogInfoArray;
typedef common::ObSEArray<ObStateInfo, 1, ModulePageAllocator> ObStateInfoArray;

struct CtxInfo final
{
  CtxInfo() : ctx_(NULL) {}
  CtxInfo(ObTransCtx *ctx) : ctx_(ctx) {}
  TO_STRING_KV(KP(ctx_));
  ObTransCtx *ctx_;
};

typedef common::ObSEArray<ObMemtableKeyInfo, 16, TransModulePageAllocator> ObMemtableKeyArray;
typedef common::ObSEArray<ObAddrLogId, 10, TransModulePageAllocator> ObAddrLogIdArray;
const int64_t OB_TRANS_REDO_LOG_RESERVE_SIZE = 128 * 1024;
const int64_t MAX_ONE_PC_TRANS_SIZE = 1500000;
// parmeters config transaction related
const int64_t TRANS_ACCESS_STAT_INTERVAL = 60 * 1000 * 1000; // 60s
const int64_t TRANS_MEM_STAT_INTERVAL = 5 * 1000 * 1000;  // 60s

// in elr transaction, curr_trans_commit_version - prev_trans_commit_version <= 400ms;
#ifdef ERRSIM
static const int64_t MAX_ELR_TRANS_INTERVAL = 20 * 1000 * 1000;
#else
static const int64_t MAX_ELR_TRANS_INTERVAL = 400 * 1000;
#endif

// max scheudler context in single server
static const int64_t MAX_PART_CTX_COUNT = 700 * 1000;

static const int DUP_TABLE_LEASE_LIST_MAX_COUNT = 8;
#define TRANS_AGGRE_LOG_TIMESTAMP OB_INVALID_TIMESTAMP



typedef common::ObSEArray<ObTxExecPart, share::OB_DEFAULT_LS_COUNT> ObTxCommitParts;
typedef common::ObSEArray<ObTxExecPart, share::OB_DEFAULT_LS_COUNT> ObTxRollbackParts;

#define CONVERT_COMMIT_PARTS_TO_PARTS(commit_parts, parts)                   \
  for (int64_t idx = 0; OB_SUCC(ret) && idx < commit_parts.count(); idx++) { \
    if (OB_FAIL(parts.push_back(commit_parts.at(idx).ls_id_))) {             \
      TRANS_LOG(WARN, "parts push failed", K(ret));                          \
    }                                                                        \
  }                                                                          \
  if (OB_FAIL(ret)) {                                                        \
    parts.reset();                                                           \
  }
#define CONVERT_PARTS_TO_COMMIT_PARTS(parts, commit_parts)                      \
  for (int64_t idx = 0; OB_SUCC(ret) && idx < parts.count(); idx++) {           \
    if (OB_FAIL(commit_parts.push_back(ObTxExecPart(parts.at(idx), -1, -1)))) { \
      TRANS_LOG(WARN, "parts push failed", K(ret));                             \
    }                                                                           \
  }                                                                             \
  if (OB_FAIL(ret)) {                                                           \
    commit_parts.reset();                                                       \
  }

class ObEndParticipantsRes
{
public:
  void reset()
  {
    blocked_trans_ids_.reset();
  }
  int assign(const ObEndParticipantsRes &other);
  int add_blocked_trans_id(const transaction::ObTransID &trans_id);
  ObBlockedTransArray &get_blocked_trans_ids();
  TO_STRING_KV(K_(blocked_trans_ids));
private:
  ObBlockedTransArray blocked_trans_ids_;
};

enum class PartCtxSource
{
  UNKNOWN = 0,
  MVCC_WRITE = 1,
  REGISTER_MDS = 2,
  REPLAY = 3,
  RECOVER = 4,
  TRANSFER = 5,
  TRANSFER_RECOVER = 6,
};

static const char * to_str_ctx_source(const PartCtxSource & ctx_src)
{
  const char * str = "INVALID";
  switch(ctx_src)
  {
    TRX_ENUM_CASE_TO_STR(PartCtxSource, UNKNOWN);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, MVCC_WRITE);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, REGISTER_MDS);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, REPLAY);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, RECOVER);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, TRANSFER);
    TRX_ENUM_CASE_TO_STR(PartCtxSource, TRANSFER_RECOVER);
  }
  return str;
}

bool is_transfer_ctx(PartCtxSource ctx_source);


enum class RetainCause : int16_t
{
  UNKOWN = -1,
  MDS_WAIT_GC_COMMIT_LOG = 0,
  MAX = 1
};

class ObTxMDSCache;

static const int64_t MAX_TABLET_MODIFY_RECORD_COUNT = 16;
// exec info need to be persisted by "trans context table"
template<typename T>
struct ObIArrayPrintTrait {
  const ObIArray<T> &arr_;
  ObIArrayPrintTrait(const ObIArray<T> &arr): arr_(arr) {}
  DECLARE_TO_STRING
  {
    int64_t pos = 0;
    J_ARRAY_START();
    for(int i =0; i < arr_.count(); i++) {
      BUF_PRINTO(arr_.at(i));
      if (i != arr_.count() - 1) { J_COMMA(); }
    }
    J_ARRAY_END();
    return pos;
  }
};
struct ObTxExecInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTxExecInfo() {}
  explicit ObTxExecInfo(TransModulePageAllocator &allocator)
    : participants_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "PARTICIPANT")),
      incremental_participants_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "INC_PART`")),
      intermediate_participants_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "INTER_PART`")),
      redo_lsns_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "REDO_LSNS")),
      multi_data_source_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "MDS_ARRAY")),
      checksum_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "TX_CHECKSUM")),
      checksum_scn_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "TX_CHECKSUM")),
      prepare_log_info_arr_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "PREPARE_INFO"))
  {
    checksum_.push_back(0);
    checksum_scn_.push_back(share::SCN());
  }
public:
  int generate_mds_buffer_ctx_array();
  void mrege_buffer_ctx_array_to_multi_data_source() const;
  void clear_buffer_ctx_in_multi_data_source();
  void reset();
  // can not destroy in tx_ctx_table
  void destroy(ObTxMDSCache &mds_cache);
  int assign(const ObTxExecInfo &exec_info);

private:
  ObTxExecInfo &operator=(const ObTxExecInfo &info);
  int assign_commit_parts(const share::ObLSArray &participants,
                          const ObTxCommitParts &commit_parts);

public:
  DECLARE_ON_DEMAND_TO_STRING

  DECLARE_TO_STRING {
//    const_cast<ObIArray<uint64_t>>(checksum_).set_max_print_count(512);
//    const_cast<ObIArray<share::SCN>>(checksum_scn_).set_max_print_count(512);
    int64_t pos = 0;
    J_KV(K_(state),
               K_(upstream),
               K_(participants),
               K_(incremental_participants),
               K_(intermediate_participants),
               K_(prev_record_lsn),
               K_(redo_lsns),
               "redo_log_no", redo_lsns_.count(),
               K_(multi_data_source),
               K_(scheduler),
               K_(prepare_version),
               K_(trans_type),
               K_(next_log_entry_no),
               K_(max_applied_log_ts),
               K_(max_applying_log_ts),
               K_(max_applying_part_log_no),
               K_(max_submitted_seq_no),
               K_(checksum),
               K_(checksum_scn),
               K_(max_durable_lsn),
               K_(data_complete),
               K_(is_dup_tx),
               //K_(touched_pkeys),
               K_(prepare_log_info_arr),
               K_(xid),
               K_(need_checksum),
               K_(is_sub2pc),
               K_(is_transfer_blocking),
               K_(commit_parts),
               K_(transfer_parts),
               K_(is_empty_ctx_created_by_transfer),
               K_(exec_epoch),
               K_(serial_final_scn),
               K_(serial_final_seq_no),
               K(dli_batch_set_.size()));
    return pos;
  }
  ObTxState state_;
  share::ObLSID upstream_;
  share::ObLSArray participants_;
  ObTxCommitParts commit_parts_;
  // for tree phase commit
  share::ObLSArray incremental_participants_;
  ObTxCommitParts intermediate_participants_;
  ObTxCommitParts transfer_parts_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray redo_lsns_;
  ObTxBufferNodeArray multi_data_source_;
  ObTxBufferCtxArray mds_buffer_ctx_array_;
  common::ObAddr scheduler_;
  share::SCN prepare_version_;
  int64_t trans_type_;
  int64_t next_log_entry_no_;
  share::SCN max_applied_log_ts_;
  share::SCN max_applying_log_ts_;
  int64_t max_applying_part_log_no_; // start from 0 on follower and always be INT64_MAX on leader
  ObTxSEQ max_submitted_seq_no_; // maintains on Leader and transfer to Follower via ActiveInfoLog
  ObSEArray<uint64_t,1> checksum_;
  ObSEArray<share::SCN,1> checksum_scn_;
  palf::LSN max_durable_lsn_;
  bool data_complete_;
  bool is_dup_tx_;
  // NB: Ensure where to put it the touched pkeys
  //for liboblog
  ObLSLogInfoArray prepare_log_info_arr_;
  // for xa
  ObXATransID xid_;
  bool need_checksum_;
  bool is_sub2pc_;
  bool is_transfer_blocking_;
  bool is_empty_ctx_created_by_transfer_;
  int64_t exec_epoch_;
  // if valid, this txCtx is logged by multi parallel thread
  // since this scn
  share::SCN serial_final_scn_;
  // the logic time of serial final log submitted
  // used to decide whether a branch level savepoint rollback log
  // need set pre-barrier to wait previous redo replayed
  ObTxSEQ serial_final_seq_no_;
  ObDLIBatchSet dli_batch_set_;
};

static const int64_t USEC_PER_SEC = 1000 * 1000;

struct ObMulSourceDataNotifyArg
{
  ObTransID tx_id_;
  share::SCN scn_; // the log ts of current notify type
  // in case of abort transaction, trans_version_ is invalid
  share::SCN trans_version_;
  NotifyType notify_type_;
  bool for_replay_;
  bool redo_submitted_;
  bool redo_synced_;
  bool willing_to_commit_;
  // force kill trans without abort scn
  bool is_force_kill_;
  bool is_incomplete_replay_;

  ObMulSourceDataNotifyArg() { reset(); }
  void reset();

  TO_STRING_KV(K_(tx_id),
               K_(scn),
               K_(trans_version),
               K_(for_replay),
               K_(notify_type),
               K_(redo_submitted),
               K_(redo_synced),
               K_(is_incomplete_replay),
               K_(willing_to_commit),
               K_(is_force_kill));

  // The redo log of current buf_node has been submitted;
  bool is_redo_submitted() const;
  // The redo log of current buf_node has been confirmed by the majority but possibly not yet
  // callbacked by ON_REDO;
  bool is_redo_confirmed() const;
  // The redo log of current buf_node has been callbacked by ON_REDO;
  bool is_redo_synced() const;
};

enum class TxEndAction : int8_t
{
  COMMIT_TX,
  ABORT_TX,
  DELAY_ABORT_TX,
  KILL_TX_FORCEDLY
};

inline bool IS_CORNER_IMPL(const char *func, const int64_t line, const int64_t ppm)
{
  int ret = common::OB_SUCCESS;
  bool bool_ret = false;
#ifdef ENABLE_DEBUG_LOG
  bool_ret = (ObRandom::rand(0, 999999) < ppm);
  TRANS_LOG(WARN, "IS_CORNER", K(func), K(line));
#endif
  UNUSED(ret);
  return bool_ret;
}

#define IS_CORNER(ppm) IS_CORNER_IMPL(__FUNCTION__, __LINE__, ppm)

inline bool is_effective_trans_version(const share::SCN trans_version)
{
  return trans_version.is_valid()
    && !trans_version.is_min()
    && !trans_version.is_max();
}

inline bool is_effective_trans_version(const int64_t trans_version)
{
  return -1 != trans_version
    && 0 != trans_version
    && INT64_MAX != trans_version;
}

template<typename T>
struct ObIArraySerDeTrait {
  ObIArray<T> &arr_;
  ObIArraySerDeTrait(ObIArray<T> &arr): arr_(arr) {}
  int64_t get_serialize_size() const
  {
    int64_t size = 0;
    size += serialization::encoded_length_vi64(arr_.count());
    for (int64_t i = 0; i < arr_.count(); i ++) {
      size += serialization::encoded_length(arr_.at(i));
    }
    return size;
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    int i = -1;
    if (OB_SUCC(serialization::encode_vi64(buf, buf_len, pos, arr_.count()))) {
      for (i = 0; i < arr_.count() && OB_SUCC(ret); i ++) {
        ret = serialization::encode(buf, buf_len, pos, arr_.at(i));
      }
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "", K(i), K(ret));
    }
    return ret;
  }
  int deserialize(const char *buf, int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    arr_.reuse();
    bool need_push_back = true;
    int64_t count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      TRANS_LOG(WARN, "decode count fail", K(ret));
    } else if (OB_FAIL(arr_.prepare_allocate(count))) {
      if (OB_NOT_SUPPORTED == ret) {
        ret = arr_.reserve(count);
      }
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "pre-allocate fail", K(ret), K(count));
      }
    } else { need_push_back = false; }
    for (int i = 0; i < count && OB_SUCC(ret); i++) {
      if (need_push_back) {
        T it;
        if (OB_FAIL(serialization::decode(buf, data_len, pos, it))) {
          TRANS_LOG(WARN, "item decode fail", K(ret), K(i));
        } else if (OB_FAIL(arr_.push_back(it))) {
          TRANS_LOG(WARN, "push fail", K(ret), K(i));
        }
      } else if (OB_FAIL(serialization::decode(buf, data_len, pos, arr_.at(i)))) {
        TRANS_LOG(WARN, "item decode fail", K(ret), K(i));
      }
    }
    return ret;
  }
};

} // transaction
} // oceanbase

#include "ob_trans_define_v4.h"

#endif // OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_
