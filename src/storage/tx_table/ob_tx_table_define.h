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

#ifndef OCEANBASE_STORAGE_TX_TABLE_OB_TX_TABLE_DEFINE
#define OCEANBASE_STORAGE_TX_TABLE_OB_TX_TABLE_DEFINE

#include "lib/lock/ob_tc_rwlock.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"

namespace oceanbase
{
namespace storage
{

const int64_t MAX_TX_CTX_TABLE_ID_LENGTH = 100; // the real length is no more than 64 + 1
const int64_t MAX_TX_CTX_TABLE_META_LENGTH = 128;
const int64_t MAX_TX_CTX_TABLE_VALUE_LENGTH = OB_MAX_USER_ROW_LENGTH -
  MAX_TX_CTX_TABLE_ID_LENGTH - MAX_TX_CTX_TABLE_META_LENGTH;
static_assert(MAX_TX_CTX_TABLE_VALUE_LENGTH > 0, "MAX_TX_CTX_TABLE_VALUE_LENGTH is not enough");


struct TxDataDefaultAllocator : public ObIAllocator {
  void *alloc(const int64_t size) override;
  void *alloc(const int64_t size, const ObMemAttr &attr) override { return ob_malloc(size, attr); }
  void free(void *ptr) override { ob_free(ptr); }
  static TxDataDefaultAllocator &get_default_allocator() {
    static TxDataDefaultAllocator default_allocator;
    return default_allocator;
  }
};

#define DEFAULT_TX_DATA_ALLOCATOR TxDataDefaultAllocator::get_default_allocator()

struct ObTxCtxTableCommonHeader
{
public:
  ObTxCtxTableCommonHeader() = delete;
  ObTxCtxTableCommonHeader(const int64_t version, const int64_t data_len)
    : MAGIC_VERSION_(version), DATA_LEN_(data_len) {}
  ~ObTxCtxTableCommonHeader(){}

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_data_len() const { return DATA_LEN_; }

private:
  const int64_t MAGIC_VERSION_;
  const int64_t DATA_LEN_;
};

struct ObTxCtxTableInfo
{
private:
  const static int64_t UNIS_VERSION = 1;
  const static int64_t MAGIC_NUM = 0xABAB;
  const static int64_t MAGIC_VERSION = MAGIC_NUM + UNIS_VERSION;
public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf,
                  const int64_t buf_len,
                  int64_t &pos,
                  ObTxDataTable &tx_data_table);
  int64_t get_serialize_size() const;

  void set_compatible_version(const int32_t version) {
    compatible_version_ = version;
  }
private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf,
                   const int64_t buf_len,
                   int64_t &pos,
                   ObTxDataTable &tx_data_table);
  int64_t get_serialize_size_() const;

public:
  ObTxCtxTableInfo() { reset(); }
  ~ObTxCtxTableInfo() { destroy(); }
  bool is_valid() const;
  void reset()
  {
    tx_id_.reset();
    ls_id_.reset();
    cluster_id_ = OB_INVALID_CLUSTER_ID;
    cluster_version_ = 0;
    tx_data_guard_.reset();
    exec_info_.reset();
    table_lock_info_.reset();
    compatible_version_ = -1;
  }
  void destroy() { reset(); }
  TO_STRING_KV(K_(tx_id), K_(ls_id), K_(cluster_id), K_(tx_data_guard),
               K_(exec_info), K_(table_lock_info), K_(cluster_version));
  transaction::ObTransID tx_id_;
  share::ObLSID ls_id_;
  int64_t cluster_id_;
  uint64_t cluster_version_;
  ObTxDataGuard tx_data_guard_;
  transaction::ObTxExecInfo exec_info_;
  transaction::tablelock::ObTableLockInfo table_lock_info_;
  // used to handle compatible issue when deserialize,
  // not serialized, set from ObTxCtxTableMeta
  int32_t compatible_version_;
};

struct ObTxCtxTableMeta
{
private:
  const static int64_t UNIS_VERSION = 1;
  const static int64_t MAGIC_NUM = 0xACAC;
  const static int64_t MAGIC_VERSION = MAGIC_NUM + UNIS_VERSION;

public:
  ObTxCtxTableMeta() { reset(); }
  ~ObTxCtxTableMeta() { destroy(); }

  bool is_valid() const
  { return tx_id_.is_valid() && ls_id_.is_valid(); }

  void reset()
  {
    version_ = VERSION_1;
    tx_id_.reset();
    ls_id_.reset();
    tx_ctx_serialize_size_ = 0;
    row_num_ = 0;
    row_idx_ = 0;
  }
  void destroy() { reset(); }
  ObTxCtxTableMeta &operator=(const ObTxCtxTableMeta &r)
  {
    version_ = r.version_;
    tx_id_ = r.tx_id_;
    ls_id_ = r.ls_id_;
    tx_ctx_serialize_size_ = r.tx_ctx_serialize_size_;
    row_num_ = r.row_num_;
    row_idx_ = r.row_idx_;
    return *this;
  }

  void init(transaction::ObTransID tx_id,
            share::ObLSID ls_id,
            int64_t row_value_serialize_size,
            int32_t row_num,
            int32_t row_idx) {
    tx_id_ = tx_id;
    ls_id_ = ls_id;
    tx_ctx_serialize_size_ = row_value_serialize_size;
    row_num_ = row_num;
    row_idx_ = row_idx;
  }

public:
  int serialize(char* buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char* buf, const int64_t buf_len, int64_t &pos);
  int64_t get_serialize_size() const;

  bool is_single_row_tx_ctx() const
  { return 1 == row_num_ && 0 == row_idx_; }

  bool is_multi_row_last_extent() const
  { return row_num_ > 1 && row_num_ - 1 == row_idx_; }

  int get_multi_row_next_extent(ObTxCtxTableMeta& next) const
  {
    int ret = OB_SUCCESS;
    if (row_num_ <= 1 || row_num_ - 1 == row_idx_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      next = *this;
      next.row_idx_ = row_idx_ + 1;
    }
    return ret;
  }

  bool is_multi_row_next_extent(const ObTxCtxTableMeta& next) const
  {
    return next.tx_id_ == tx_id_ &&
           next.ls_id_ == ls_id_ &&
           next.tx_ctx_serialize_size_ == tx_ctx_serialize_size_ &&
           next.row_num_ == row_num_ &&
           next.row_idx_ == row_idx_ + 1 &&
           next.row_idx_ <  row_num_;
  }

  int64_t get_tx_ctx_serialize_size() const
  {
    return tx_ctx_serialize_size_;
  }
  int32_t get_version() const
  {
    return version_;
  }
  TO_STRING_KV(K_(tx_id), K_(ls_id), K_(tx_ctx_serialize_size), K_(row_num), K_(row_idx), K_(version));
private:
  int serialize_(char* buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char* buf, const int64_t buf_len, int64_t &pos);
  int64_t get_serialize_size_() const;
public:
  static constexpr int VERSION_0 = 0;
  // V1, fix bug:
  //   ctx serialized size record in header not equals to real serialized size
  //   it is because of the CommonID's get serialize_size always return 8
  //   but it use variant encoding
  static constexpr int VERSION_1 = 1;
private:
  int32_t version_;
  transaction::ObTransID tx_id_;
  share::ObLSID ls_id_;
  int64_t tx_ctx_serialize_size_;
  int32_t row_num_;
  int32_t row_idx_;
};

struct ObTxDataCheckData
{
public:
  ObTxDataCheckData()
    : state_(0),
    commit_version_(),
    end_scn_(),
    is_rollback_(false) {}
  TO_STRING_KV(K_(state), K_(commit_version), K_(end_scn), K_(is_rollback));
public:
  int32_t state_;
  share::SCN commit_version_;
  share::SCN end_scn_;
  bool is_rollback_;
};

class ObITxDataCheckFunctor
{
public:
  ObITxDataCheckFunctor()
    : tx_data_check_data_(),
    may_exist_undecided_state_in_tx_data_table_(false) {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) = 0;
  virtual bool recheck() { return false; }
  virtual bool is_decided() const;
  virtual ObTxDataCheckData &get_tx_data_check_data() { return tx_data_check_data_; }
  virtual void resolve_tx_data_check_data_(const int32_t state,
                                           const share::SCN commit_version,
                                           const share::SCN end_scn,
                                           const bool is_rollback);
  // In the tx_data_table, defensive error reporting strategies are implemented,
  // which means that potential errors are proactively handled and reported
  // before they can escalate. Concurrently, there might be cases where
  // information is retrieved directly from the tx data table without undergoing
  // these checks. To address this scenario, we utilize configuration settings
  // to govern the behavior of error reporting, determining when and how such
  // errors should be reported or managed.
  bool may_exist_undecided_state_in_tx_data_table() const;
  void set_may_exist_undecided_state_in_tx_data_table();

  VIRTUAL_TO_STRING_KV(K_(tx_data_check_data));
public:
  // In the process of transfers, the data during transfer needs to rely both on
  // the tx table state from the transfer src before the transfer_scn, as well as
  // the tx table state from the transfer dest after the transfer_scn.
  // Otherwise:
  //   1. If the tx table state from the transfer src before the transfer_scn is
  //      not relied upon, there could be a loss of rollbacks in the transfer
  //      src's undo_status.
  //   2. If the tx table state from the transfer dest after the transfer_scn is
  //      not relied upon, there could be a loss of the most recent decided txn
  //      state in the transfer dest side.
  //
  // So, in the context of a transfer, when the src side has uncommitted data,
  // it's necessary to both fuse the tx data state from the src and dest sides.
  // Abstractly speaking, the reason for this fusion stems from the most
  // critical abstraction:
  //   - For the transfer, there exists a transfer out log. Data and txn states
  //     preceding this log are located on the src side, while data and txn
  //     states following this log are situated on the dest side.
  //
  // Therefore, it's necessary to fuse the txn states. While we should note that
  // txn is composed of txn states(state), commit versions(commit_version) and
  // rollback sequences(undo_status). Among these:
  //   1. From the src side, what's needed are the txn states of the already
  //      committed transactions before the transfer_scn, along with their
  //      commit versions and rollback sequences, as well as the rollback
  //      sequences of txns that are not yet committed.
  //   2. From the dest side, what's required are the transaction states,
  //      commit versions, and rollback sequences of transactions after the
  //      transfer_scn.
  //
  // Hence, the dest of the txn state for uncommitted data on the src side
  // should follow these steps:
  //   - Starting from the src side, if a txn has been committed(meaning there
  //     is a transaction state, commit version, or contained in the rollback
  //     sequence), it can be directly obtained from the source side.
  //   - If a txn is uncommitted on the src side (no decided txn state, commit
  //     version, or rollback sequence exists), then its details need to be
  //     determined from the txn state, commit version, or rollback sequence on
  //     the destination side.
  ObTxDataCheckData tx_data_check_data_;
  // In the tx_data_table, defensive error reporting strategies are implemented,
  // which means that potential errors are proactively handled and reported
  // before they can escalate. Concurrently, there might be cases where
  // information is retrieved directly from the tx data table without undergoing
  // these checks. To address this scenario, we utilize configuration settings
  // to govern the behavior of error reporting, determining when and how such
  // errors should be reported or managed.
  bool may_exist_undecided_state_in_tx_data_table_;
};

class ObCommitVersionsArray
{
private:
  const static int64_t UNIS_VERSION = 1;

public:
  struct Node {
    share::SCN start_scn_;
    share::SCN commit_version_;

    Node() : start_scn_(), commit_version_() {}

    Node(const share::SCN start_scn, const share::SCN commit_version)
      : start_scn_(start_scn), commit_version_(commit_version) {}

    bool operator==(const Node &rhs) const 
    {
      bool is_equal = true;
      if (this->start_scn_ != rhs.start_scn_
          || this->commit_version_ != rhs.commit_version_) {
        is_equal = false;
      }
      return is_equal;
    }

    DECLARE_TO_STRING;
  };

  void reset() { array_.reset(); }

  ObCommitVersionsArray &operator=(const ObCommitVersionsArray& rhs)
  {
    this->array_.reset();
    for (int i = 0; i < rhs.array_.count(); i++) {
      this->array_.push_back(rhs.array_.at(i));
    }
    return *this;
  }

public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  bool is_valid();

  static void print_to_stderr(const ObCommitVersionsArray &commit_versions)
  {
    fprintf(stderr, "pre-process data for upper trans version calculation : ");
    for (int i = 0; i < commit_versions.array_.count(); i++) {
      if (i % 3 == 0) {
        fprintf(stderr, "\n        ");
      }
      fprintf(stderr, "(start_scn=%-20s, commit_version=%-20s) ",
              to_cstring(commit_versions.array_.at(i).start_scn_),
              to_cstring(commit_versions.array_.at(i).commit_version_));
    }
    fprintf(stderr, "\npre-process data end.\n");
  }
    
  DECLARE_TO_STRING;

private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_() const;

public:
  ObSEArray<Node, 128> array_;
};

class CalcUpperTransSCNCache
{
public:
  CalcUpperTransSCNCache()
      : is_inited_(false),
        cache_version_(),
        lock_(common::ObLatchIds::TX_TABLE_LOCK),
        commit_versions_() {}

  void reset()
  {
    is_inited_ = false;
    cache_version_.reset();
    commit_versions_.reset();
  }

  TO_STRING_KV(K_(is_inited), K_(cache_version), K_(commit_versions));

public:
  bool is_inited_;

  // The end_scn of the sstable will be used as the cache_version
  share::SCN cache_version_;
  
  mutable common::TCRWLock lock_;

  ObCommitVersionsArray commit_versions_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_TX_TABLE_OB_TX_TABLE_DEFINE
