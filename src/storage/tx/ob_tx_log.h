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

#ifndef OCEANBASE_TRANSACTION_OB_TX_LOG
#define OCEANBASE_TRANSACTION_OB_TX_LOG

#include <type_traits>
#include "share/ob_ls_id.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_tx_serialization.h"
#include "storage/tx/ob_tx_big_segment_buf.h"
#include "storage/tx/ob_trans_factory.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "share/ob_admin_dump_helper.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "lib/utility/ob_unify_serialize.h"
//#include <cstdint>

#define OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF

namespace oceanbase
{

namespace memtable
{
class ObMemtableMutatorIterator;
class ObMemtableMutatorRow;
class ObRowData;
}

namespace share
{
class ObLSID;
struct ObAdminMutatorStringArg;

}

namespace palf
{
class LSN;
}

namespace common
{
class ObDataBuffer;
} // namespace common

namespace storage
{
class ObLSDDLLogHandler;
}

namespace transaction
{

class ObTxLogCb;
class ObPartTransCtx;

typedef int64_t TxID;
typedef share::ObLSID LSKey;
typedef palf::LSN LogOffSet;

// Trans Log
enum class ObTxLogType : int64_t
{
  UNKNOWN = 0,
  TX_REDO_LOG = 0x1,
  TX_ROLLBACK_TO_LOG = 0x2,
  TX_MULTI_DATA_SOURCE_LOG = 0x4,
  TX_DIRECT_LOAD_INC_LOG = 0x8,
  TX_ACTIVE_INFO_LOG = 0x10,
  TX_RECORD_LOG = 0x20,
  TX_COMMIT_INFO_LOG = 0x40,
  TX_PREPARE_LOG = 0x80,
  TX_COMMIT_LOG = 0x100,
  TX_ABORT_LOG = 0x200,
  TX_CLEAR_LOG = 0x400,

  // logstream-level log
  TX_START_WORKING_LOG = 0x100000,
  // BigSegment log
  TX_BIG_SEGMENT_LOG = 0x200000,
  TX_LOG_TYPE_LIMIT
};

class ObTxCbArg
{
public:
  ObTxCbArg() : log_type_(ObTxLogType::UNKNOWN), arg_(NULL) {}
  ObTxCbArg(const ObTxLogType log_type, const void *arg) : log_type_(log_type), arg_(arg) {}
  ObTxLogType get_log_type() const { return log_type_; }
  const void *get_arg() const { return arg_; }
  TO_STRING_KV(KP_(log_type), KP_(arg));
private:
  ObTxLogType log_type_;
  const void *arg_;
};

typedef common::ObSEArray<ObTxCbArg, 3> ObTxCbArgArray;

inline bool is_contain(const ObTxCbArgArray &array, const ObTxLogType log_type)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < array.count(); i++) {
    if ((array.at(i).get_log_type() == log_type)) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

static inline int64_t operator&(const ObTxLogType left, const ObTxLogType right)
{
  return (
      static_cast<int64_t>(left) &
      static_cast<int64_t>(right) );
}

static inline ObTxLogType operator|(const ObTxLogType left, const ObTxLogType right)
{
  return static_cast<ObTxLogType>(
      static_cast<int64_t>(left) |
      static_cast<int64_t>(right) );
}

static const ObTxLogType TX_LOG_TYPE_MASK = (ObTxLogType::TX_REDO_LOG |
                                             ObTxLogType::TX_ROLLBACK_TO_LOG |
                                             ObTxLogType::TX_MULTI_DATA_SOURCE_LOG |
                                             ObTxLogType::TX_DIRECT_LOAD_INC_LOG |
                                             ObTxLogType::TX_ACTIVE_INFO_LOG |
                                             ObTxLogType::TX_RECORD_LOG |
                                             ObTxLogType::TX_COMMIT_INFO_LOG |
                                             ObTxLogType::TX_PREPARE_LOG |
                                             ObTxLogType::TX_COMMIT_LOG |
                                             ObTxLogType::TX_ABORT_LOG |
                                             ObTxLogType::TX_CLEAR_LOG |
                                             ObTxLogType::TX_START_WORKING_LOG);

class ObTxLogTypeChecker {
public:
  static bool is_data_log(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_REDO_LOG == log_type || ObTxLogType::TX_RECORD_LOG == log_type
           || ObTxLogType::TX_ROLLBACK_TO_LOG == log_type
           || ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type
           || ObTxLogType::TX_DIRECT_LOAD_INC_LOG == log_type;
  }
  static bool is_state_log(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_PREPARE_LOG == log_type ||
           ObTxLogType::TX_COMMIT_LOG  == log_type ||
           ObTxLogType::TX_ABORT_LOG   == log_type ||
           ObTxLogType::TX_CLEAR_LOG   == log_type;
  }
  static bool is_valid_log(const ObTxLogType log_type)
  {
    return (TX_LOG_TYPE_MASK & log_type) != 0;
  }
  static bool is_ls_log(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_START_WORKING_LOG == log_type;
  }
  static bool can_be_spilt(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type
  #ifdef OB_TX_LOG_TEST
           || ObTxLogType::TX_COMMIT_INFO_LOG == log_type
  #endif
        ;
  }

  static logservice::ObReplayBarrierType
  need_replay_barrier(const ObTxLogType log_type, const ObTxDataSourceType data_source_type);
  static int decide_final_barrier_type(const logservice::ObReplayBarrierType tmp_log_barrier_type,
                                       logservice::ObReplayBarrierType &final_barrier_type);
};

inline bool is_contain_stat_log(const ObTxCbArgArray &array)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < array.count(); i++) {
    if ((ObTxLogTypeChecker::is_state_log(array.at(i).get_log_type()))) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

class ObTxPrevLogType
{
public:
  enum TypeEnum : uint8_t
  {
    UNKOWN = 0,
    SELF = 1,
    COMMIT_INFO = 2,
    PREPARE = 3,
    TRANSFER_IN = 10,
  };

public:
  NEED_SERIALIZE_AND_DESERIALIZE;
  ObTxPrevLogType() { reset(); }
  ObTxPrevLogType(const TypeEnum prev_log_type) : prev_log_type_(prev_log_type) {}

  bool is_valid() const { return prev_log_type_ > 0; }
  void set_self() { prev_log_type_ = TypeEnum::SELF; }
  bool is_self() const { return TypeEnum::SELF == prev_log_type_; }
  void set_tranfer_in() { prev_log_type_ = TypeEnum::TRANSFER_IN; }
  bool is_transfer_in() const { return TypeEnum::TRANSFER_IN == prev_log_type_; }

  void set_prepare() { prev_log_type_ = TypeEnum::PREPARE; }
  void set_commit_info() { prev_log_type_ = TypeEnum::COMMIT_INFO; }
  bool is_normal_log() const
  {
    return TypeEnum::COMMIT_INFO == prev_log_type_ || TypeEnum::PREPARE == prev_log_type_;
  }

  ObTxLogType convert_to_tx_log_type();

  void reset() { prev_log_type_ = TypeEnum::UNKOWN; }

  TO_STRING_KV("val", prev_log_type_);

private:
  TypeEnum prev_log_type_;
};

// ============================== Tx Log Header ==============================
class ObTxLogHeader
{
public:
  static const int64_t TX_LOG_HEADER_SIZE = sizeof(ObTxLogType);

  NEED_SERIALIZE_AND_DESERIALIZE;
  ObTxLogHeader() : tx_log_type_(ObTxLogType::UNKNOWN) {}
  ObTxLogHeader(const ObTxLogType type) : tx_log_type_(type) {}
  ObTxLogType get_tx_log_type() const { return tx_log_type_; };
  TO_STRING_KV(K(tx_log_type_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxLogHeader);
  ObTxLogType tx_log_type_;
};

// ============================== Tx Log Body ==============================
class ObTxRedoLogTempRef
{
public:
  ObTxRedoLogTempRef() {}
public:
  // remove member variable ObCLogEncryptInfo encrypt_info_
  // there may be added other variables in the future
};

struct ObCtxRedoInfo
{
  ObCtxRedoInfo(const int64_t cluster_version)
      : cluster_version_(cluster_version)
  {
  };
  int before_serialize();

  ObTxSerCompatByte compat_bytes_;
  // serialize before mutator_buf by fixed length
  uint64_t cluster_version_;
  // remove member variable ObCLogEncryptInfo encrypt_info_
  // there may be added other variables in the future

  OB_UNIS_VERSION(1);
};

class ObTxRedoLog
{
  OB_UNIS_VERSION(1);
// public:
//   NEED_SERIALIZE_AND_DESERIALIZE;

public:
  ObTxRedoLog(ObTxRedoLogTempRef &temp_ref)
      : mutator_buf_(nullptr), replay_mutator_buf_(nullptr), mutator_size_(-1),
        ctx_redo_info_(0)
        // (ctx_redo_info_.clog_encrypt_info_)(temp_ref.encrypt_info_), (ctx_redo_info_.cluster_version_)(0)
  {
    before_serialize();
  }
  ObTxRedoLog(const uint64_t &cluster_version)
      : mutator_buf_(nullptr), replay_mutator_buf_(nullptr), mutator_size_(-1),
        ctx_redo_info_(cluster_version)
  // (ctx_redo_info_.clog_encrypt_info_)(encrypt_info),(ctx_redo_info_.cluster_version_)(cluster_version)
  {
    before_serialize();
  }

  char *get_mutator_buf() { return mutator_buf_; }
  const char *get_replay_mutator_buf() const { return replay_mutator_buf_; }
  const int64_t &get_mutator_size() const { return mutator_size_; }
  const uint64_t &get_cluster_version() const { return ctx_redo_info_.cluster_version_; }

  //------------ Only invoke in ObTxLogBlock
  int set_mutator_buf(char *buf);
  int set_mutator_size(const int64_t size, const bool after_fill);
  void reset_mutator_buf();
  //------------

  //for ob_admin dump self
  int ob_admin_dump(memtable::ObMemtableMutatorIterator *iter_ptr,
                    share::ObAdminMutatorStringArg &arg,
                    const char *block_name,
                    palf::LSN lsn,
                    int64_t tx_id,
                    share::SCN scn,
                    bool &has_output);
  //------------

  static const int8_t MUTATOR_SIZE_NEED_BYTES = 4;
  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(
      K(LOG_TYPE),
      // KP(mutator_buf_),
      K(mutator_size_),
      // KP(replay_mutator_buf_),
      // K(clog_encrypt_info_),
      K(ctx_redo_info_.cluster_version_));

public:
  int before_serialize() { return ctx_redo_info_.before_serialize(); }
private:
  //------------ For ob_admin
  int format_mutator_row_(const memtable::ObMemtableMutatorRow &row, share::ObAdminMutatorStringArg &arg);
  int smart_dump_rowkey_(const ObStoreRowkey &rowkey, share::ObAdminMutatorStringArg &arg);
  int format_row_data_(const memtable::ObRowData &row_data, share::ObAdminMutatorStringArg &arg);
  //------------
private:
  char *mutator_buf_;              // for fill
  const char *replay_mutator_buf_; // for replay, only set by deserialize
  int64_t mutator_size_;

  ObCtxRedoInfo ctx_redo_info_;
};

// for dist trans write it's multi source data, the same as redo,
// for simplicity, add a new log type
class ObTxMultiDataSourceLog
{
  OB_UNIS_VERSION(1);

public:
  const static int64_t MAX_MDS_LOG_SIZE = 512 * 3 * 1024;
  const static int64_t MAX_PENDING_BUF_SIZE = 512 * 1024;

public:
  ObTxMultiDataSourceLog()
  {
    reset();
  }
  ~ObTxMultiDataSourceLog() {}
  void reset();
  const ObTxBufferNodeArray &get_data() const { return data_; }

  int fill_MDS_data(const ObTxBufferNode &node);
  int64_t count() const { return data_.count(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K_(data));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  ObTxBufferNodeArray data_;
};

class ObTxDLIncLogBuf
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;

  ObTxDLIncLogBuf() : submit_buf_(nullptr), replay_buf_(nullptr), dli_buf_size_(0), is_alloc_(false)
  {}
  // ObTxDLIncLogBuf(void *buf, const int64_t buf_size)
  //     : dli_buf_(buf), dli_buf_size_(buf_size), is_alloc_(false)
  // {}

  template <typename DDL_LOG>
  int serialize_log_object(const DDL_LOG *ddl_log)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(submit_buf_) || dli_buf_size_ > 0 || is_alloc_ || OB_ISNULL(ddl_log)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid arguments", K(ret), KPC(this), KPC(ddl_log));
    } else {
      is_alloc_ = true;
      dli_buf_size_ = ddl_log->get_serialize_size();
      submit_buf_ = static_cast<char *>(share::mtl_malloc(dli_buf_size_, "DLI_TMP_BUF"));
      int64_t pos = 0;
      if (OB_ISNULL(submit_buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memory failed", K(ret), KPC(this), KPC(ddl_log));
      } else if (OB_FALSE_IT(memset(submit_buf_, 0, dli_buf_size_))) {
        // do nothing
      } else if (OB_FAIL(ddl_log->serialize(static_cast<char *>(submit_buf_), dli_buf_size_, pos))) {
        TRANS_LOG(WARN, "serialize ddl log buf failed", K(ret), KPC(this), KPC(ddl_log));
      }

      // TRANS_LOG(INFO, "serialize ddl log object", K(ret), KPC(ddl_log), K(pos),
      // K(dli_buf_size_)); int tmp_ret = OB_SUCCESS; int64_t tmp_pos = 0; DDL_LOG tmp_ddl_log; if
      // (OB_TMP_FAIL(tmp_ddl_log.deserialize(submit_buf_, dli_buf_size_, tmp_pos))) {
      //   TRANS_LOG(WARN, "deserialize ddl log buf failed", K(ret), K(tmp_ret), KPC(this),
      //             K(tmp_ddl_log), KPC(ddl_log));
      // }

#ifdef ENABLE_DEBUG_LOG
      TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> serialize ddl log object", K(ret), KP(submit_buf_),
                K(pos), K(dli_buf_size_), KPC(ddl_log));
#endif
    }
    return ret;
  }

  template <typename DDL_LOG>
  int deserialize_log_object(DDL_LOG *ddl_log) const
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (OB_ISNULL(replay_buf_) || dli_buf_size_ <= 0 || OB_ISNULL(ddl_log)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid arguments", K(ret), KPC(this), KPC(ddl_log));
    } else if (OB_FAIL(ddl_log->deserialize(replay_buf_, dli_buf_size_, pos))) {
      TRANS_LOG(WARN, "deserialize ddl log buf failed", K(ret), KPC(this), KPC(ddl_log));
    }

#ifdef ENABLE_DEBUG_LOG
    TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> deserialize ddl log object", K(ret), KP(replay_buf_),
              K(pos), K(dli_buf_size_), KPC(ddl_log));
#endif
    return ret;
  }

  const char *get_buf() { return replay_buf_; }
  int64_t get_buf_size() { return dli_buf_size_; }

  void reset()
  {
    if (is_alloc_) {
      if (OB_NOT_NULL(submit_buf_)) {
        share::mtl_free(submit_buf_);
      }
      is_alloc_ = false;
    }
    submit_buf_ = nullptr;
    dli_buf_size_ = 0;
  }

  ~ObTxDLIncLogBuf() { reset(); }
  TO_STRING_KV(KP(submit_buf_), KP(replay_buf_), K(dli_buf_size_), K(is_alloc_));

private:
  char *submit_buf_;
  const char *replay_buf_;
  int64_t dli_buf_size_;

  bool is_alloc_;
};

class TxSubmitBaseArg
{
public:
  ObTxLogCb *log_cb_;
  share::SCN base_scn_;
  int64_t replay_hint_;
  logservice::ObReplayBarrierType replay_barrier_type_;
  int64_t suggested_buf_size_;
  bool hold_tx_ctx_ref_;

  TxSubmitBaseArg() { reset(); }
  // ~TxSubmitBaseArg() = default;
  void reset()
  {
    log_cb_ = nullptr;
    base_scn_.set_invalid();
    replay_hint_ = 0;
    replay_barrier_type_ = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
    suggested_buf_size_ = 0;
    hold_tx_ctx_ref_ = 0;
  }
  bool is_valid()
  {
    return log_cb_ != nullptr && base_scn_.is_valid() && replay_hint_ >= 0
           && replay_barrier_type_ != logservice::ObReplayBarrierType::INVALID_BARRIER
           && hold_tx_ctx_ref_ >= 0;
  }
  TO_STRING_KV(KP(log_cb_),
               K(base_scn_),
               K(replay_hint_),
               K(replay_barrier_type_),
               K(suggested_buf_size_),
               K(hold_tx_ctx_ref_));
};

class TxReplayBaseArg
{
public:
  int64_t part_log_no_;
  TxReplayBaseArg() { reset(); }
  // ~TxReplayBaseArg() = default;
  void reset() { part_log_no_ = -1; }
  bool is_valid() { return part_log_no_ > 0; }
  TO_STRING_KV(K(part_log_no_));
};

class ObTxDirectLoadIncLog
{
  OB_UNIS_VERSION(1);

public:
  enum class DirectLoadIncLogType
  {
    UNKNOWN = 0,
    DLI_REDO = 1,
    DLI_START = 2,
    DLI_END = 3
  };

  class TempRef
  {
  public:
    // empty class, only for template
    ObTxDLIncLogBuf dli_log_buf_;
  };
  class ConstructArg
  {
  public:
    DirectLoadIncLogType ddl_log_type_;
    ObDDLIncLogBasic batch_key_;
    ObTxDLIncLogBuf &dli_buf_;

    TO_STRING_KV(K(ddl_log_type_), K(batch_key_), K(dli_buf_));

    ConstructArg(DirectLoadIncLogType ddl_log_type,
                 const ObDDLIncLogBasic &batch_key,
                 ObTxDLIncLogBuf &dli_buf_ref)
        : ddl_log_type_(ddl_log_type), batch_key_(batch_key), dli_buf_(dli_buf_ref)
    {}
    ConstructArg(ObTxDirectLoadIncLog::TempRef &temp_ref)
        : ddl_log_type_(DirectLoadIncLogType::UNKNOWN), batch_key_(),
          dli_buf_(temp_ref.dli_log_buf_)
    {}
  };
  class ReplayArg : public oceanbase::transaction::TxReplayBaseArg
  {
  public:
    ObLSDDLLogHandler *ddl_log_handler_ptr_;
    DirectLoadIncLogType ddl_log_type_;
    ObDDLIncLogBasic batch_key_;

    ReplayArg() { reset(); }
    void reset()
    {
      oceanbase::transaction::TxReplayBaseArg::reset();
      ddl_log_handler_ptr_ = nullptr;
      ddl_log_type_ = DirectLoadIncLogType::UNKNOWN;
      batch_key_.reset();
    }
    INHERIT_TO_STRING_KV("base_replay_arg_",
                         oceanbase::transaction::TxReplayBaseArg,
                         KP(ddl_log_handler_ptr_),
                         K(ddl_log_type_),
                         K(batch_key_));
  };
  class SubmitArg : public oceanbase::transaction::TxSubmitBaseArg
  {
  public:
    logservice::AppendCb *extra_cb_;
    bool need_free_extra_cb_;

    SubmitArg() { reset(); }
    void reset()
    {
      oceanbase::transaction::TxSubmitBaseArg::reset();
      extra_cb_ = nullptr;
      need_free_extra_cb_ = false;
    }
    INHERIT_TO_STRING_KV("base_submit_arg_",
                         oceanbase::transaction::TxSubmitBaseArg,
                         KPC(extra_cb_), K(need_free_extra_cb_));
  };

  ObTxDirectLoadIncLog(const ObTxDirectLoadIncLog::ConstructArg &arg)
      : ddl_log_type_(arg.ddl_log_type_), log_buf_(arg.dli_buf_)
  {
    before_serialize();
  }

  // const ObDDLRedoLog &get_ddl_redo_log() { return ddl_redo_log_; }

  DirectLoadIncLogType get_ddl_log_type() { return ddl_log_type_; }
  const ObTxDLIncLogBuf &get_dli_buf() { return log_buf_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(ddl_log_type_), K(log_buf_));

public:
  int before_serialize();

#ifdef ENABLE_DEBUG_LOG
  // only for unittest
  static int64_t direct_load_inc_submit_log_cnt_;
  static int64_t direct_load_inc_apply_log_cnt_;
  static int64_t direct_load_inc_submit_log_size_;
  static int64_t direct_load_inc_apply_log_size_;

#endif

private:
  ObTxSerCompatByte compat_bytes_;

  DirectLoadIncLogType ddl_log_type_;

  ObTxDLIncLogBuf &log_buf_;
  // ObDDLRedoLog &ddl_redo_log_;
};

class ObTxActiveInfoLogTempRef {
public:
  ObTxActiveInfoLogTempRef() : scheduler_(), app_trace_id_str_(), proposal_leader_(), xid_() {}

public:
  common::ObAddr scheduler_;
  common::ObString app_trace_id_str_;
  common::ObAddr proposal_leader_;
  ObXATransID xid_;
};

class ObTxActiveInfoLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxActiveInfoLog(ObTxActiveInfoLogTempRef &temp_ref)
      : scheduler_(temp_ref.scheduler_), trans_type_(TransType::SP_TRANS), session_id_(0), associated_session_id_(0),
        app_trace_id_str_(temp_ref.app_trace_id_str_), schema_version_(0), can_elr_(false),
        proposal_leader_(temp_ref.proposal_leader_), cur_query_start_time_(0), is_sub2pc_(false),
        is_dup_tx_(false), tx_expired_time_(0), epoch_(0), last_op_sn_(0), first_seq_no_(),
        last_seq_no_(), max_submitted_seq_no_(), serial_final_seq_no_(), cluster_version_(0),
        xid_(temp_ref.xid_)
  {
    before_serialize();
  }
  ObTxActiveInfoLog(common::ObAddr &scheduler,
                    int trans_type,
                    int session_id,
                    uint32_t associated_session_id,
                    common::ObString &app_trace_id_str,
                    int64_t schema_version,
                    bool elr,
                    common::ObAddr &proposal_leader,
                    int64_t cur_query_start_time,
                    bool is_sub2pc,
                    bool is_dup_tx,
                    int64_t tx_expired_time,
                    int64_t epoch,
                    int64_t last_op_sn,
                    ObTxSEQ first_seq_no,
                    ObTxSEQ last_seq_no,
                    ObTxSEQ max_submitted_seq_no,
                    uint64_t cluster_version,
                    const ObXATransID &xid,
                    ObTxSEQ serial_final_seq_no)
      : scheduler_(scheduler), trans_type_(trans_type), session_id_(session_id),
        associated_session_id_(associated_session_id),
        app_trace_id_str_(app_trace_id_str), schema_version_(schema_version), can_elr_(elr),
        proposal_leader_(proposal_leader), cur_query_start_time_(cur_query_start_time),
        is_sub2pc_(is_sub2pc), is_dup_tx_(is_dup_tx), tx_expired_time_(tx_expired_time),
        epoch_(epoch), last_op_sn_(last_op_sn), first_seq_no_(first_seq_no), last_seq_no_(last_seq_no),
        max_submitted_seq_no_(max_submitted_seq_no), serial_final_seq_no_(serial_final_seq_no),
        cluster_version_(cluster_version), xid_(xid)
  {
    before_serialize();
  };

  const common::ObAddr &get_scheduler() const { return scheduler_; }
  int get_trans_type() const { return trans_type_; }
  int get_session_id() const { return session_id_; }
  int get_associated_session_id() const { return associated_session_id_; }
  const common::ObString &get_app_trace_id() const { return app_trace_id_str_; }
  const int64_t &get_schema_version() { return schema_version_; }
  bool is_elr() const { return can_elr_; }
  const common::ObAddr &get_proposal_leader() { return proposal_leader_; }
  const int64_t &get_cur_query_start_time() { return cur_query_start_time_; }
  bool is_sub2pc() const { return is_sub2pc_; }
  bool is_dup_tx() const { return is_dup_tx_; }
  int64_t get_tx_expired_time() const { return tx_expired_time_; }
  int64_t get_last_op_sn() const { return last_op_sn_; }
  int64_t get_epoch() const { return epoch_; }
  ObTxSEQ get_first_seq_no() const { return first_seq_no_; }
  ObTxSEQ get_last_seq_no() const { return last_seq_no_; }
  ObTxSEQ get_max_submitted_seq_no() const { return max_submitted_seq_no_; }
  ObTxSEQ get_serial_final_seq_no() const { return serial_final_seq_no_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  const ObXATransID &get_xid() const { return xid_; }
  // for ob_admin
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(scheduler_),
               K(trans_type_),
               K(session_id_),
               K(associated_session_id_),
               K(app_trace_id_str_),
               K(schema_version_),
               K(can_elr_),
               K(proposal_leader_),
               K(cur_query_start_time_),
               K(is_sub2pc_),
               K(is_dup_tx_),
               K(tx_expired_time_),
               K(epoch_),
               K(last_op_sn_),
               K(first_seq_no_),
               K(last_seq_no_),
               K(max_submitted_seq_no_),
               K(serial_final_seq_no_),
               K(cluster_version_),
               K(xid_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  common::ObAddr &scheduler_;
  int trans_type_;
  int session_id_;
  uint32_t associated_session_id_;
  common::ObString &app_trace_id_str_;
  int64_t schema_version_;
  bool can_elr_;
  common::ObAddr &proposal_leader_;
  int64_t cur_query_start_time_;
  bool is_sub2pc_;
  bool is_dup_tx_;
  int64_t tx_expired_time_;

  // sql execution relative
  int64_t epoch_;
  int64_t last_op_sn_;
  ObTxSEQ first_seq_no_;
  ObTxSEQ last_seq_no_;

  // ctrl savepoint written log
  ObTxSEQ max_submitted_seq_no_;
  ObTxSEQ serial_final_seq_no_;
  uint64_t cluster_version_;
  ObXATransID xid_;
};

class ObTxCommitInfoLogTempRef
{
public:
  ObTxCommitInfoLogTempRef()
      : scheduler_(), participants_(), app_trace_id_str_(), app_trace_info_(),
        incremental_participants_(), prev_record_lsn_(), redo_lsns_(), xid_()
  {}

public:
  common::ObAddr scheduler_;
  share::ObLSArray participants_;
  common::ObString app_trace_id_str_;
  common::ObString app_trace_info_;
  share::ObLSArray incremental_participants_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray redo_lsns_;
  ObXATransID xid_;
};

class ObTxCommitInfoLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxCommitInfoLog(ObTxCommitInfoLogTempRef &temp_ref)
      : scheduler_(temp_ref.scheduler_), participants_(temp_ref.participants_), upstream_(),
        is_sub2pc_(false), is_dup_tx_(false), can_elr_(false),
        incremental_participants_(temp_ref.incremental_participants_), cluster_version_(0),
        app_trace_id_str_(temp_ref.app_trace_id_str_), app_trace_info_(temp_ref.app_trace_info_),
        prev_record_lsn_(temp_ref.prev_record_lsn_), redo_lsns_(temp_ref.redo_lsns_),
        xid_(temp_ref.xid_), commit_parts_(), epoch_(0)
  {
    before_serialize();
  }
  ObTxCommitInfoLog(common::ObAddr &scheduler,
                    share::ObLSArray &participants,
                    LSKey &upstream,
                    bool is_sub2pc,
                    bool is_dup_tx,
                    bool is_elr,
                    common::ObString &app_trace_id,
                    const common::ObString &app_trace_info,
                    const LogOffSet &prev_record_lsn,
                    ObRedoLSNArray &redo_lsns,
                    share::ObLSArray &incremental_participants,
                    uint64_t cluster_version,
                    const ObXATransID &xid,
                    const ObTxCommitParts &commit_parts,
                    int64_t epoch)
      : scheduler_(scheduler), participants_(participants), upstream_(upstream),
        is_sub2pc_(is_sub2pc), is_dup_tx_(is_dup_tx), can_elr_(is_elr),
        incremental_participants_(incremental_participants), cluster_version_(cluster_version),
        app_trace_id_str_(app_trace_id), app_trace_info_(app_trace_info),
        prev_record_lsn_(prev_record_lsn), redo_lsns_(redo_lsns), xid_(xid), commit_parts_(commit_parts), epoch_(epoch)
  {
    before_serialize();
  };

  const common::ObAddr &get_scheduler() const { return scheduler_; }
  const share::ObLSArray &get_participants() const { return participants_; }
  const LSKey &get_upstream() const { return upstream_; }
  bool is_sub2pc() const { return is_sub2pc_; }
  bool is_dup_tx() const { return is_dup_tx_; }
  bool is_elr() const { return can_elr_; }
  const common::ObString &get_app_trace_id() const { return app_trace_id_str_; }
  const common::ObString &get_app_trace_info() const { return app_trace_info_; }
  const ObRedoLSNArray &get_redo_lsns() const { return redo_lsns_; }
  const LogOffSet &get_prev_record_lsn() const { return prev_record_lsn_; }
  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  const ObXATransID &get_xid() const { return xid_; }
  int64_t get_epoch() const { return epoch_; }
  const ObTxCommitParts &get_commit_parts() const { return commit_parts_; }
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(scheduler_),
               K(participants_),
               K(upstream_),
               K(is_sub2pc_),
               K(is_dup_tx_),
               K(can_elr_),
               K(incremental_participants_),
               K(cluster_version_),
               K(app_trace_id_str_),
               K(app_trace_info_),
               K(prev_record_lsn_),
               K(redo_lsns_),
               K(xid_),
               K(commit_parts_),
               K(epoch_))
public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  common::ObAddr &scheduler_;
  share::ObLSArray &participants_;
  LSKey upstream_;
  bool is_sub2pc_;
  bool is_dup_tx_;
  bool can_elr_;
  share::ObLSArray &incremental_participants_;
  uint64_t cluster_version_;

  //--------- for liboblog -----------
  common::ObString &app_trace_id_str_;
  common::ObString app_trace_info_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray &redo_lsns_;
  // for xa
  ObXATransID xid_;
  ObTxCommitParts commit_parts_;
  int64_t epoch_;
};

class ObTxPrepareLogTempRef
{
public:
  ObTxPrepareLogTempRef() : incremental_participants_() {}
public:
  share::ObLSArray incremental_participants_;
};

class ObTxPrepareLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxPrepareLog(ObTxPrepareLogTempRef &temp_ref)
      : incremental_participants_(temp_ref.incremental_participants_), prev_lsn_(), prev_log_type_()
  {
    before_serialize();
  };
  ObTxPrepareLog(share::ObLSArray &incremental_participants, LogOffSet &commit_info_lsn, ObTxPrevLogType prev_log_type)
      : incremental_participants_(incremental_participants), prev_lsn_(commit_info_lsn), prev_log_type_(prev_log_type)
  {
    before_serialize();
  };

  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  const LogOffSet &get_prev_lsn() { return prev_lsn_; }
  const ObTxPrevLogType &get_prev_log_type() const  { return prev_log_type_; }
  void set_prev_lsn(const LogOffSet &lsn) { prev_lsn_ = lsn; }
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(incremental_participants_), K(prev_lsn_), K(prev_log_type_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::ObLSArray &incremental_participants_;

  //--------- for liboblog -----------
  LogOffSet prev_lsn_;
  ObTxPrevLogType prev_log_type_;
};

class ObTxDataBackup
{
  OB_UNIS_VERSION(1);

public:
  ObTxDataBackup();
  int init(const share::SCN &start_scn);
  void reset();

  share::SCN get_start_log_ts() const { return start_log_ts_; }

  TO_STRING_KV(K(start_log_ts_));

private:
  share::SCN start_log_ts_;
};

class ObTxCommitLogTempRef
{
public:
  ObTxCommitLogTempRef() : checksum_signature_(), incremental_participants_(), multi_source_data_(), ls_log_info_arr_()
  { checksum_signature_.set_max_print_count(1024); }

public:
  ObSEArray<uint8_t,1> checksum_signature_;
  share::ObLSArray incremental_participants_;
  ObTxBufferNodeArray multi_source_data_;
  ObLSLogInfoArray ls_log_info_arr_;
};

class ObTxCommitLog
{
  OB_UNIS_VERSION(1);

public:
  const static int64_t INVALID_COMMIT_VERSION = ObTransVersion::INVALID_TRANS_VERSION;

public:
  ObTxCommitLog(ObTxCommitLogTempRef &temp_ref)
      : commit_version_(), checksum_(0), checksum_sig_(temp_ref.checksum_signature_),
        checksum_sig_serde_(checksum_sig_),
        incremental_participants_(temp_ref.incremental_participants_),
        multi_source_data_(temp_ref.multi_source_data_), trans_type_(TransType::SP_TRANS),
        tx_data_backup_(), prev_lsn_(), ls_log_info_arr_(temp_ref.ls_log_info_arr_), prev_log_type_()
  {
    before_serialize();
  }
  ObTxCommitLog(share::SCN commit_version,
                uint64_t checksum,
                ObIArray<uint8_t> &checksum_sig,
                share::ObLSArray &incremental_participants,
                ObTxBufferNodeArray &multi_source_data,
                int32_t trans_type,
                LogOffSet prev_lsn,
                ObLSLogInfoArray &ls_log_info_arr,
                ObTxPrevLogType prev_log_type)
      : checksum_(checksum), checksum_sig_(checksum_sig), checksum_sig_serde_(checksum_sig_),
        incremental_participants_(incremental_participants), multi_source_data_(multi_source_data),
        trans_type_(trans_type), prev_lsn_(prev_lsn), ls_log_info_arr_(ls_log_info_arr), prev_log_type_(prev_log_type)
  {
    commit_version_ = commit_version;
    before_serialize();
  }
  int init_tx_data_backup(const share::SCN &start_scn);
  const ObTxDataBackup &get_tx_data_backup() const { return tx_data_backup_; }
  share::SCN get_commit_version() const { return commit_version_; }
  uint64_t get_checksum() const { return checksum_; }
  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  const ObTxBufferNodeArray &get_multi_source_data() const { return multi_source_data_; }
  const int32_t &get_trans_type() const { return trans_type_; }
  const ObLSLogInfoArray &get_ls_log_info_arr() const { return ls_log_info_arr_; }
  const LogOffSet &get_prev_lsn() const { return prev_lsn_; }
  const ObTxPrevLogType &get_prev_log_type() const  { return prev_log_type_; }
  void set_prev_lsn(const LogOffSet &lsn) { prev_lsn_ = lsn; }

  const share::SCN get_backup_start_scn() { return tx_data_backup_.get_start_log_ts(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(commit_version_),
               K(checksum_),
               K_(checksum_sig),
               K(incremental_participants_),
               K(multi_source_data_),
               K(trans_type_),
               K(tx_data_backup_),
               K(prev_lsn_),
               K(ls_log_info_arr_),
               K(prev_log_type_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::SCN commit_version_; // equal to INVALID_COMMIT_VERSION in Single LS Transaction
  uint64_t checksum_;
  ObIArray<uint8_t> &checksum_sig_;
  ObIArraySerDeTrait<uint8_t> checksum_sig_serde_;
  share::ObLSArray &incremental_participants_;
  ObTxBufferNodeArray &multi_source_data_;

  // need by a committed transaction which replay from commit log
  int32_t trans_type_;

  ObTxDataBackup tx_data_backup_;

  //--------- for liboblog -----------
  LogOffSet prev_lsn_;
  ObLSLogInfoArray &ls_log_info_arr_;
  ObTxPrevLogType prev_log_type_;
};

class ObTxClearLogTempRef
{
public:
  ObTxClearLogTempRef() : incremental_participants_() {}

public:
  share::ObLSArray incremental_participants_;
};

class ObTxClearLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxClearLog(ObTxClearLogTempRef &temp_ref)
      : incremental_participants_(temp_ref.incremental_participants_)
  {
    before_serialize();
  }
  ObTxClearLog(share::ObLSArray &incremental_participants)
      : incremental_participants_(incremental_participants)
  {
    before_serialize();
  }
  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(incremental_participants_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::ObLSArray &incremental_participants_;
};

class ObTxAbortLogTempRef
{
public:
  ObTxAbortLogTempRef() : multi_source_data_() {};
public:
  ObTxBufferNodeArray multi_source_data_;
};

class ObTxAbortLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxAbortLog(ObTxAbortLogTempRef &temp_ref)
      : multi_source_data_(temp_ref.multi_source_data_),
        tx_data_backup_()
  {
    before_serialize();
  }
  ObTxAbortLog(ObTxBufferNodeArray &multi_source_data)
      : multi_source_data_(multi_source_data), tx_data_backup_()
  {
    before_serialize();
  }
  const ObTxBufferNodeArray &get_multi_source_data() const { return multi_source_data_; }

  int init_tx_data_backup(const share::SCN &start_scn);

  const ObTxDataBackup &get_tx_data_backup() const { return tx_data_backup_; }

  const share::SCN get_backup_start_scn() { return tx_data_backup_.get_start_log_ts(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(multi_source_data_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  ObTxBufferNodeArray &multi_source_data_;
  ObTxDataBackup tx_data_backup_;
};

class ObTxRecordLogTempRef
{
public:
  ObTxRecordLogTempRef() : prev_record_lsn_(), redo_lsns_() {}

public:
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray redo_lsns_;
};

class ObTxRecordLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxRecordLog(ObTxRecordLogTempRef &temp_ref)
      : prev_record_lsn_(temp_ref.prev_record_lsn_), redo_lsns_(temp_ref.redo_lsns_)
  {
    before_serialize();
  }
  ObTxRecordLog(const LogOffSet &prev_record_lsn, ObRedoLSNArray &redo_lsns)
      : prev_record_lsn_(prev_record_lsn), redo_lsns_(redo_lsns)
  {
    before_serialize();
  }

  const ObRedoLSNArray &get_redo_lsns() { return redo_lsns_; }
  const LogOffSet &get_prev_record_lsn() { return prev_record_lsn_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(prev_record_lsn_), K(redo_lsns_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray &redo_lsns_;
};


class ObTxStartWorkingLogTempRef
{
public:
  ObTxStartWorkingLogTempRef() {}
};

class ObTxStartWorkingLog
{
public:
  OB_UNIS_VERSION(1);

public:
  ObTxStartWorkingLog(ObTxStartWorkingLogTempRef &temp_ref) : leader_epoch_(0)
  {
    UNUSED(temp_ref);
    before_serialize();
  }
  ObTxStartWorkingLog(int64_t leader_epoch) : leader_epoch_(leader_epoch) { before_serialize(); }

  const int64_t &get_leader_epoch() { return leader_epoch_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(leader_epoch_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  int64_t leader_epoch_;
};

class ObTxRollbackToLog
{
public:
  OB_UNIS_VERSION(1);
public:
  ObTxRollbackToLog() = default;
  ObTxRollbackToLog(const ObTxSEQ from, const ObTxSEQ to)
    : from_(from), to_(to) {before_serialize();}


  ObTxSEQ get_from() const { return from_; }
  ObTxSEQ get_to() const { return to_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(KP(this), K(LOG_TYPE), K_(from), K_(to));

public:
  int before_serialize();
private:
  ObTxSerCompatByte compat_bytes_;
  ObTxSEQ from_;
  ObTxSEQ to_;
};

// ============================== Tx Log Blcok ==============================

class ObTxLogBlockHeader
{
  OB_UNIS_VERSION(1);
public:
  struct FixSizeTrait_int64_t {
    FixSizeTrait_int64_t(int64_t &v): v_(v) {}
    int64_t &v_;
    int serialize(SERIAL_PARAMS) const { return NS_::encode_i64(buf, buf_len, pos, v_); }
    int64_t get_serialize_size(void) const { return NS_::encoded_length_i64(v_); }
    int deserialize(DESERIAL_PARAMS) { return NS_::decode_i64(buf, data_len, pos, &v_); }
  };
public:
  void reset()
  {
    org_cluster_id_ = 0;
    cluster_version_ = 0;
    log_entry_no_ = 0;
    tx_id_ = 0;
    scheduler_.reset();
    flags_ = 0;
    serialize_size_ = 0;
  }
  ObTxLogBlockHeader(): __log_entry_no_(log_entry_no_)
  {
    reset();
    before_serialize();
  };
  ObTxLogBlockHeader(const uint64_t org_cluster_id,
                     const int64_t cluster_version,
                     const int64_t log_entry_no,
                     const ObTransID &tx_id,
                     const common::ObAddr &scheduler)
    : __log_entry_no_(log_entry_no_), flags_(0)
  {
    init(org_cluster_id, cluster_version, log_entry_no, tx_id, scheduler);
    before_serialize();
  }
  void init(const uint64_t org_cluster_id,
            const int64_t cluster_version,
            const int64_t log_entry_no,
            const ObTransID &tx_id,
            const common::ObAddr &scheduler) {
    org_cluster_id_ = org_cluster_id;
    cluster_version_ = cluster_version;
    log_entry_no_ = log_entry_no;
    tx_id_ = tx_id;
    scheduler_ = scheduler;
    flags_ = 0;
    serialize_size_ = 0;
  }
  void calc_serialize_size_();
  uint64_t get_org_cluster_id() const { return org_cluster_id_; }
  int64_t get_cluster_version() const { return cluster_version_; }
  int64_t get_log_entry_no() const { return log_entry_no_; }
  void set_log_entry_no(int64_t entry_no) { log_entry_no_ = entry_no; }
  const ObTransID &get_tx_id() const { return tx_id_; }
  const common::ObAddr &get_scheduler() const { return scheduler_; }

  bool is_valid() const { return org_cluster_id_ >= 0; }
  void set_serial_final() { flags_ |= SERIAL_FINAL; }
  bool is_serial_final() const { return (flags_ & SERIAL_FINAL) == SERIAL_FINAL; }
  uint8_t flags() const { return flags_; }
  TO_STRING_KV(K_(compat_bytes), K_(org_cluster_id), K_(cluster_version), K_(log_entry_no), K_(tx_id), K_(scheduler), K_(flags));

public:
  int before_serialize();
  // the last serial log
  static const uint8_t SERIAL_FINAL = ((uint8_t)1) << 0;
private:
  int64_t serialize_size_;
  ObTxSerCompatByte compat_bytes_;
  uint64_t org_cluster_id_;
  int64_t cluster_version_;
  int64_t log_entry_no_;
  FixSizeTrait_int64_t __log_entry_no_; // serialize helper member, hiden for others
  ObTransID tx_id_;
  common::ObAddr scheduler_;
  uint8_t flags_;
};

class ObTxAdaptiveLogBuf
{
public:
  ObTxAdaptiveLogBuf() : buf_(default_buf_), len_(sizeof(default_buf_))
  {
    default_buf_[0] = '\0';
  }
  ~ObTxAdaptiveLogBuf() { reset(); }
  int init(const int64_t suggested_buf_size)
  {
    int ret = OB_SUCCESS;
    char *ptr = NULL;
    if (suggested_buf_size <= MIN_LOG_BUF_SIZE) {
      // do nothing
    } else if (suggested_buf_size <= NORMAL_LOG_BUF_SIZE) {
      if (OB_ISNULL(ptr = alloc_normal_buf_())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        buf_ = ptr;
        len_ = NORMAL_LOG_BUF_SIZE;
      }
    } else {
      if (OB_ISNULL(ptr = alloc_big_buf_())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        buf_ = ptr;
        len_ = BIG_LOG_BUF_SIZE;
      }
    }
    return ret;
  }
  int extend_and_copy(const int64_t pos)
  {
    int ret = OB_SUCCESS;
    char *ptr = NULL;
    if (buf_ == default_buf_) {
      if (OB_ISNULL(ptr = alloc_normal_buf_())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (pos > 0) {
          memcpy(ptr, buf_, pos);
        }
        buf_ = ptr;
        len_ = NORMAL_LOG_BUF_SIZE;
      }
    // it will be enabled after clog support big clog
    } else if (NORMAL_LOG_BUF_SIZE == len_) {
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        TRANS_LOG(WARN, "get data version failed", K(ret));
      } else if ((data_version < DATA_VERSION_4_2_2_0)
          || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_1_0)) {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "big log is not supported", K(ret));
      } else if (OB_ISNULL(ptr = alloc_big_buf_())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (pos > 0) {
          memcpy(ptr, buf_, pos);
        }
        free_buf_(buf_);
        buf_ = ptr;
        len_ = BIG_LOG_BUF_SIZE;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  void reset()
  {
    if (NULL != buf_ && buf_ != default_buf_) {
      free_buf_(buf_);
      buf_ = NULL;
    }
    default_buf_[0] = '\0';
    len_ = 0;
  }
  char *get_buf()
  {
    return buf_;
  }
  int64_t get_length() const
  {
    return len_;
  }
  TO_STRING_KV(KP_(buf), KP_(default_buf), K_(len));
private:
  char *alloc_normal_buf_()
  {
    int ret = OB_ALLOCATE_MEMORY_FAILED;
    char *ptr = NULL;
    if (OB_ISNULL(ptr = static_cast<char *>(ob_malloc(NORMAL_LOG_BUF_SIZE, "NORMAL_CLOG_BUF")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc clog normal buffer failed", K(ret));
    }
    return ptr;
  }
  char *alloc_big_buf_()
  {
    int ret = OB_ALLOCATE_MEMORY_FAILED;
    char *ptr = NULL;
    if (OB_ISNULL(ptr = static_cast<char *>(ob_malloc(BIG_LOG_BUF_SIZE, "BIG_CLOG_BUF")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc clog big buffer failed", K(ret));
    }
    return ptr;
  }
  void free_buf_(char *buf)
  {
    if (OB_NOT_NULL(buf)) {
      ob_free(buf);
    }
  }
public:
  static const int64_t MIN_LOG_BUF_SIZE = 2048;
  static const int64_t NORMAL_LOG_BUF_SIZE = common::OB_MAX_LOG_ALLOWED_SIZE;
  static const int64_t BIG_LOG_BUF_SIZE = palf::MAX_LOG_BODY_SIZE;
  STATIC_ASSERT((BIG_LOG_BUF_SIZE > 3 * 1024 * 1024 && BIG_LOG_BUF_SIZE < 4 * 1024 * 1024), "unexpected big log buf size");
private:
  char *buf_;
  int64_t len_;
  char default_buf_[MIN_LOG_BUF_SIZE];
};


class ObTxLogBlock
{
public:
  // static const int MIN_LOG_BLOCK_HEADER_SIZE;
  static const logservice::ObLogBaseType DEFAULT_LOG_BLOCK_TYPE; // TRANS_LOG
  static const int32_t DEFAULT_BIG_ROW_BLOCK_SIZE;
  static const int64_t BIG_SEGMENT_SPILT_SIZE;
  NEED_SERIALIZE_AND_DESERIALIZE;
  ObTxLogBlock();
  void reset();
  int reuse_for_fill();
  int init_for_fill(const int64_t suggested_buf_size = ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE);
  int init_for_replay(const char *buf, const int64_t &size);
  int init_for_replay(const char *buf, const int64_t &size, int skip_pos); // init before replay
  ObTxLogBlockHeader &get_header() { return header_; }
  logservice::ObLogBaseHeader &get_log_base_header() { return log_base_header_; }
  typedef logservice::ObReplayBarrierType ObReplayBarrierType;
  int seal(const int64_t replay_hint, const ObReplayBarrierType barrier_type = ObReplayBarrierType::NO_NEED_BARRIER);
  ~ObTxLogBlock() { reset(); }
  bool is_inited() const { return inited_; }
  int get_next_log(ObTxLogHeader &header,
                   ObTxBigSegmentBuf *big_segment_buf = nullptr,
                   bool *contain_big_segment = nullptr);
  const ObTxCbArgArray &get_cb_arg_array() const { return cb_arg_array_; }
  template <typename T>
  int deserialize_log_body(T &tx_log_body); // make cur_log_type_ is UNKNOWN
  // fill log except RedoLog
  template <typename T>
  int add_new_log(T &tx_log_body, ObTxBigSegmentBuf * big_segment_buf = nullptr);
  // fill RedoLog and mutator_buf
  int prepare_mutator_buf(ObTxRedoLog &redo);
  int finish_mutator_buf(ObTxRedoLog &redo, const int64_t &mutator_size);
  int extend_log_buf();
  TO_STRING_KV(K(fill_buf_),
               KP(replay_buf_),
               K(len_),
               K(pos_),
               K(cur_log_type_),
               K(cb_arg_array_),
               KPC(big_segment_buf_),
               K_(inited),
               K_(header),
               K_(log_base_header));

public:
  // get fill buf for submit log
  char *get_buf() { return fill_buf_.get_buf(); }
  const int64_t &get_size() { return pos_; }

  int set_prev_big_segment_scn(const share::SCN prev_scn);
  int acquire_segment_log_buf(const ObTxLogType big_segment_log_type, ObTxBigSegmentBuf *big_segment_buf = nullptr);
private:
  int serialize_log_block_header_();
  int deserialize_log_block_header_();
  int update_next_log_pos_(); // skip log body if  cur_log_type_ is UNKNOWN (depend on
                              // DESERIALIZE_HEADER in ob_unify_serialize.h)
  DISALLOW_COPY_AND_ASSIGN(ObTxLogBlock);
  bool inited_;
  logservice::ObLogBaseHeader log_base_header_;
  ObTxLogBlockHeader header_;
  ObTxAdaptiveLogBuf fill_buf_;
  const char *replay_buf_;
  int64_t len_;
  int64_t pos_;
  ObTxLogType cur_log_type_;
  ObTxCbArgArray cb_arg_array_;

  ObTxBigSegmentBuf *big_segment_buf_;
};

template <typename T>
int ObTxLogBlock::deserialize_log_body(T &tx_log_body)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  if (OB_ISNULL(replay_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (T::LOG_TYPE != cur_log_type_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid log_body type", K(T::LOG_TYPE), K(cur_log_type_));
  } else {
    if (OB_NOT_NULL(big_segment_buf_)) {
      if (OB_FAIL(big_segment_buf_->deserialize_object(tx_log_body))) {
        TRANS_LOG(WARN, "deserialize log body from big segment buf failed", K(ret), K(tx_log_body),
                  KPC(this));
      } else {
        // big_segment_buf_->reset();
        big_segment_buf_ = nullptr;
      }
    } else if (OB_FAIL(tx_log_body.deserialize(replay_buf_, len_, tmp_pos))) {
      TRANS_LOG(WARN, "desrialize tx_log_body error", K(ret), K(*this));
    }
    if (OB_SUCC(ret)) {
      pos_ = tmp_pos;
      cur_log_type_ = ObTxLogType::UNKNOWN;
    }
  }
  return ret;
}

template <typename T>
int ObTxLogBlock::add_new_log(T &tx_log_body, ObTxBigSegmentBuf *big_segment_buf)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  ObTxLogHeader header(T::LOG_TYPE);
  if ((OB_ISNULL(fill_buf_.get_buf()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::TX_REDO_LOG == cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf is using");
  } else if (OB_NOT_NULL(big_segment_buf_) && big_segment_buf_->is_active()) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "big segment is not empty, can not add new log", K(ret), KPC(this));
  } else if (T::LOG_TYPE == ObTxLogType::TX_REDO_LOG) {
    // ret = OB_EAGAIN;
    TRANS_LOG(DEBUG, "insert redo_log type into cb_arg_array_", K(tx_log_body), KPC(this));
  } else if (OB_FAIL(tx_log_body.before_serialize())) {
    TRANS_LOG(WARN, "before serialize failed", K(ret), K(tx_log_body), K(*this));
  } else {
    const int64_t serialize_size = header.get_serialize_size() + tx_log_body.get_serialize_size();
    if (ObTxLogTypeChecker::can_be_spilt(T::LOG_TYPE) && BIG_SEGMENT_SPILT_SIZE < serialize_size + tmp_pos) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      while (OB_SUCC(ret) && len_ < serialize_size + tmp_pos) {
        if (OB_FAIL(extend_log_buf())) {
          ret = OB_BUF_NOT_ENOUGH;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (len_ < serialize_size + tmp_pos) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (OB_FAIL(header.serialize(fill_buf_.get_buf(), len_, tmp_pos))) {
        TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
      } else if (OB_FAIL(tx_log_body.serialize(fill_buf_.get_buf(), len_, tmp_pos))) {
        TRANS_LOG(WARN, "serialize tx_log_body error", K(ret), K(tx_log_body), K(*this));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret)) {
    // it should be not failed
    if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(T::LOG_TYPE, NULL)))) {
      TRANS_LOG(ERROR, "push log cb arg failed", K(ret), K(*this));
    } else {
      pos_ = tmp_pos;
    }
  } else if (OB_BUF_NOT_ENOUGH == ret && cb_arg_array_.empty()
             && ObTxLogTypeChecker::can_be_spilt(T::LOG_TYPE)) {
    // use big segment
    if (OB_ISNULL(big_segment_buf)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), KPC(big_segment_buf));
    } else if (OB_FALSE_IT(big_segment_buf_ = big_segment_buf)) {
    } else if (OB_FAIL(big_segment_buf_->init_for_serialize(header.get_serialize_size()
                                                            + tx_log_body.get_serialize_size()))) {
      TRANS_LOG(WARN, "init big segment buf failed", K(ret), KPC(big_segment_buf_));
    } else if (OB_FAIL(big_segment_buf_->serialize_object(header))) {
      TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
    } else if (OB_FAIL(big_segment_buf_->serialize_object(tx_log_body))) {
      TRANS_LOG(WARN, "serialize tx_log_body error", K(ret), K(tx_log_body), K(*this));
    } else {
      ret = OB_LOG_TOO_LARGE;
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[TxLogBlock] add new log", K(ret), K(tx_log_body), K(*this), K(tmp_pos));
  }

  return ret;
}

} // namespace transaction
} // namespace oceanbase
#endif


