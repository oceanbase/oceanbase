/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_STORAGE_COMPACTION_OB_TRANS_CACHE_H_
#define SRC_STORAGE_COMPACTION_OB_TRANS_CACHE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashutils.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"

namespace oceanbase
{

namespace storage
{
class ObTxData;
}

namespace compaction
{
struct ObMergeCachedTransKey {
  ObMergeCachedTransKey()
    : trans_id_(),
      sql_sequence_(0)
  {}
  ObMergeCachedTransKey(
    transaction::ObTransID trans_id,
    int64_t sql_sequence)
    : trans_id_(trans_id),
      sql_sequence_(sql_sequence)
  {}
  ~ObMergeCachedTransKey() {}
  inline bool operator == (const ObMergeCachedTransKey &other) const
  {
    return trans_id_ == other.trans_id_ && sql_sequence_ == other.sql_sequence_;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_value = trans_id_.hash();
    hash_value = murmurhash(&sql_sequence_, sizeof(sql_sequence_), hash_value);
    return hash_value;
  }
  inline bool is_valid() const
  {
    return trans_id_.is_valid() && 0 != sql_sequence_;
  }
  TO_STRING_KV(K_(trans_id), K_(sql_sequence));

  transaction::ObTransID trans_id_;
  int64_t sql_sequence_;
};

struct ObMergeCachedTransState {
  ObMergeCachedTransState()
    : key_(),
      trans_version_(0),
      trans_state_(INT32_MAX),
      can_read_(INVALID_BOOL_VALUE),
      is_determined_state_(INVALID_BOOL_VALUE)
  {}
  ObMergeCachedTransState(
    transaction::ObTransID trans_id,
    int64_t sql_sequence,
    int64_t trans_version,
    int32_t trans_state,
    int16_t can_read,
    int16_t is_determined_state)
    : key_(trans_id, sql_sequence),
      trans_version_(trans_version),
      trans_state_(trans_state),
      can_read_(can_read),
      is_determined_state_(is_determined_state)
  {}
  virtual ~ObMergeCachedTransState() {}
  inline bool is_valid() const
  {
    return key_.is_valid() && 0 != trans_version_ && INT32_MAX != trans_state_ &&
      INVALID_BOOL_VALUE != can_read_ && INVALID_BOOL_VALUE != is_determined_state_;
  }
  TO_STRING_KV(K_(key), K_(trans_state), K_(trans_version), K_(can_read), K_(is_determined_state));

  static const int16_t INVALID_BOOL_VALUE = -1;
  ObMergeCachedTransKey key_;
  int64_t trans_version_;
  int32_t trans_state_;
  int16_t can_read_; // 0 false; 1 true
  int16_t is_determined_state_; // 0 false; 1 true
};

class ObCachedTransStateMgr {
public:
  ObCachedTransStateMgr(common::ObIAllocator &allocator)
    : is_inited_(false),
      max_cnt_(0),
      allocator_(allocator),
      array_(nullptr)
  {}
  ~ObCachedTransStateMgr() { destroy(); }
  int init(int64_t max_cnt);
  void destroy();
  inline uint64_t cal_idx(const ObMergeCachedTransKey &key) { return key.hash() % max_cnt_; }
  int get_trans_state(const transaction::ObTransID &trans_id, const int64_t sql_seq, ObMergeCachedTransState &trans_state);
  int add_trans_state(
    const transaction::ObTransID &trans_id,
    const int64_t sql_seq,
    const int64_t trans_version,
    const int32_t trans_state,
    const int16_t can_read,
    const int16_t is_determined_state);
private:
  bool is_inited_;
  int64_t max_cnt_;
  common::ObIAllocator &allocator_;
  ObMergeCachedTransState *array_;
};

} // namespace compaction
} // namespace oceanbase

#endif