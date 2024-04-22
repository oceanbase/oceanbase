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

// class ObPartTransCtx
// {
// public:

#ifndef OCEANBASE_TRANSACTION_DIRECT_LOAD_TX_CTX_DEFINE_HEADER
#define OCEANBASE_TRANSACTION_DIRECT_LOAD_TX_CTX_DEFINE_HEADER

#include "common/ob_tablet_id.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "share/scn.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/tx/ob_tx_serialization.h"

namespace oceanbase
{

namespace transaction
{

using namespace storage;

class ObTxDirectLoadIncBatchInfo
{
  OB_UNIS_VERSION(1);

public:
  ObTxDirectLoadIncBatchInfo() : batch_key_(), start_scn_(), tmp_start_scn_(), tmp_end_scn_() {}
  ObTxDirectLoadIncBatchInfo(const ObDDLIncLogBasic &ddl_inc_basic)
      : batch_key_(ddl_inc_basic), start_scn_(), tmp_start_scn_(), tmp_end_scn_()
  {}

public:
  uint64_t hash() const { return batch_key_.hash(); }
  int hash(uint64_t &hash_val) const { return batch_key_.hash(hash_val); }

  bool operator==(const ObTxDirectLoadIncBatchInfo &other) const
  {
    return batch_key_ == other.batch_key_;
  }

private:
  static const int64_t LOG_SYNC_SUCC_BIT = 1UL << 1;

public:
  const ObDDLIncLogBasic &get_batch_key() const { return batch_key_; }

  // void set_start_scn(const share::SCN scn) { start_scn_ = scn; }
  share::SCN get_start_scn() const { return start_scn_; }
  void set_tmp_start_scn(const share::SCN scn) { tmp_start_scn_ = scn; }
  share::SCN get_tmp_start_scn() const { return tmp_start_scn_; }
  void set_tmp_end_scn(const share::SCN scn) { tmp_end_scn_ = scn; }
  share::SCN get_tmp_end_scn() const { return tmp_end_scn_; }

  bool need_compensate_ddl_end() const { return is_start_log_synced() && !is_ddl_end_logging(); }
  bool is_ddl_start_logging() const
  {
    return tmp_start_scn_.is_valid_and_not_min() && !start_scn_.is_valid_and_not_min();
  }
  bool is_ddl_start_log_synced() const { return is_start_log_synced(); }
  bool is_ddl_end_logging() const { return tmp_end_scn_.is_valid_and_not_min(); }

private:
  // union Flag
  // {
  //   int64_t val_;
  //   struct BitFlag
  //   {
  //     bool start_log_sync_succ_ : 1;
  //
  //     TO_STRING_KV(K(start_log_sync_succ_));
  //   } bit_;
  // };
  //
public:
  int set_start_log_synced(); // void clear_start_log_synced() { start_scn_.set_invalid(); }
  bool is_start_log_synced() const { return start_scn_.is_valid_and_not_min(); }

  TO_STRING_KV(K(batch_key_), K(start_scn_), K(tmp_start_scn_), K(tmp_end_scn_));

private:
  ObDDLIncLogBasic batch_key_;
  share::SCN start_scn_;
  // Flag flag_;

  /*in memory*/
  share::SCN tmp_start_scn_;
  share::SCN tmp_end_scn_;
};

typedef common::hash::ObHashSet<ObTxDirectLoadIncBatchInfo, common::hash::NoPthreadDefendMode>
    ObTxDirectLoadBatchSet;
typedef common::ObSEArray<ObDDLIncLogBasic, 4> ObTxDirectLoadBatchKeyArray;

// hash_count  | hashkey 1 | hashkey 2 | hashkey 3 | ...
class ObDLIBatchSet : public ObTxDirectLoadBatchSet
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;

  // bool operator==(const ObDLIBatchSet &other) const
  // {
  //   return this == &other;
  // }
public:
  int before_submit_ddl_start(const ObDDLIncLogBasic &key,
                              const share::SCN &start_scn = share::SCN::invalid_scn());
  int submit_ddl_start_succ(const ObDDLIncLogBasic &key, const share::SCN &start_scn);
  int sync_ddl_start_succ(const ObDDLIncLogBasic &key, const share::SCN &start_scn);
  int sync_ddl_start_fail(const ObDDLIncLogBasic &key);

  int before_submit_ddl_end(const ObDDLIncLogBasic &key,
                            const share::SCN &end_scn = share::SCN::invalid_scn());
  int submit_ddl_end_succ(const ObDDLIncLogBasic &key, const share::SCN &end_scn);
  int sync_ddl_end_succ(const ObDDLIncLogBasic &key, const share::SCN &end_scn);
  int sync_ddl_end_fail(const ObDDLIncLogBasic &key);

  int remove_unlog_batch_info(const ObTxDirectLoadBatchKeyArray &batch_key_array);

  int assign(const ObDLIBatchSet &other);
};

} // namespace transaction
} // namespace oceanbase

#endif

//
// };
