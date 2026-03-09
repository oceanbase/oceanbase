/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_FTS_STRUCT_H_
#define OB_FTS_STRUCT_H_

#include "lib/charset/ob_charset.h"
#include "lib/hash/ob_hashmap.h"
#include "object/ob_object.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/fts/ob_fts_literal.h"

namespace oceanbase
{
namespace storage
{

class ObFTToken final
{
public:
  ObFTToken() : is_calc_hash_val_(false), hash_val_(0), hash_func_(nullptr), cmp_func_(nullptr), meta_(), token_() { }
  ~ObFTToken() = default;
  int init(const char *ptr,
           const int64_t length,
           const ObObjMeta &meta,
           const sql::ObExprHashFuncType hash_func,
           const ObDatumCmpFuncType cmp_func);
  OB_INLINE const ObDatum &get_token() const { return token_; }
  OB_INLINE ObCollationType get_collation_type() const { return meta_.get_collation_type(); }
  OB_INLINE bool empty() const { return token_.get_string().empty(); }
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObFTToken &other) const;
  OB_INLINE bool operator !=(const ObFTToken &other) const { return !(other == *this); }

  TO_STRING_KV(K_(is_calc_hash_val), K_(hash_val), KP_(hash_func), KP_(cmp_func), K_(meta), K_(token));

private:
  int do_compare(const ObFTToken &other, bool &is_equal) const;

private:
  mutable bool is_calc_hash_val_;
  mutable uint64_t hash_val_;
  sql::ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
  ObObjMeta meta_;
  ObDatum token_;
};

class ObFTTokenInfo final
{
public:
  ObFTTokenInfo()
      : allocator_(nullptr),
        count_(0),
        position_list_(nullptr),
        position_list_holder_(nullptr)
  {}
  ObFTTokenInfo(const ObFTTokenInfo &other);
  ObFTTokenInfo &operator=(const ObFTTokenInfo &other);
  ~ObFTTokenInfo();

  int update_one_position(ObIAllocator &allocator, const int64_t position);
  int update_without_pos_list();

  TO_STRING_KV(K_(count), KPC_(position_list), KP_(position_list_holder));
private:
  static const int64_t INITIAL_POSITION_LIST_COUNT = 1;
  struct ObFTPositionListHolder
  {
    ObFTPositionListHolder() : ref_cnt_(1), position_list_() {}
    int32_t ref_cnt_;
    common::ObSEArray<int64_t, INITIAL_POSITION_LIST_COUNT> position_list_;
  };
  void retain_position_list_();
  void release_position_list_();
public:
  ObIAllocator *allocator_; // for position list memory allocation
  int64_t count_;
  common::ObSEArray<int64_t, INITIAL_POSITION_LIST_COUNT> *position_list_;
  ObFTPositionListHolder *position_list_holder_;
};
typedef common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> ObFTTokenPair;

typedef common::hash::ObHashMap<
    ObFTToken,
    ObFTTokenInfo,
    common::hash::NoPthreadDefendMode,
    common::hash::hash_func<ObFTToken>,
    common::hash::equal_to<ObFTToken>,
    common::hash::SimpleAllocer<
        typename common::hash::HashMapTypes<ObFTToken, ObFTTokenInfo>::AllocType,
        common::hash::NodeNumTraits<typename common::hash::HashMapTypes<ObFTToken, ObFTTokenInfo>::AllocType>::NODE_NUM,
        common::hash::NoPthreadDefendMode>> ObFTTokenMap;

class ObProcessTokenFlag final
{
private:
  static const uint64_t PTF_NONE         = 0;
  static const uint64_t PTF_MIN_MAX_TOKEN = 1 << 0; // filter tokens that are less than a minimum or greater
                                                   // than a maximum word length.
  static const uint64_t PTF_STOP_TOKEN     = 1 << 1; // filter by sotp token table.
  static const uint64_t PTF_CASEDOWN     = 1 << 2; // convert characters from uppercase to lowercase.
  static const uint64_t PTF_GROUPBY_TOKEN = 1 << 3; // distinct and token aggregation
public:
  ObProcessTokenFlag() : flag_(PTF_NONE) {}
  ~ObProcessTokenFlag() { reset(); }
  void reset() { flag_ = PTF_NONE; }
public:
  void set_flag(const uint64_t flag) { flag_ |= flag; }
private:
  void clear_flag(const uint64_t flag) { flag_ &= ~flag; }
  bool has_flag(const uint64_t flag) const { return (flag_ & flag) == flag; }
public:
  void set_min_max_token() { set_flag(PTF_MIN_MAX_TOKEN); }
  void set_stop_token() { set_flag(PTF_STOP_TOKEN); }
  void set_casedown_token() { set_flag(PTF_CASEDOWN); }
  void set_groupby_token() { set_flag(PTF_GROUPBY_TOKEN); }
  void clear() { flag_ = PTF_NONE; }
  void clear_min_max_token() { clear_flag(PTF_MIN_MAX_TOKEN); }
  void clear_stop_token() { clear_flag(PTF_STOP_TOKEN); }
  void clear_casedown_token() { clear_flag(PTF_CASEDOWN); }
  void clear_groupby_token() { clear_flag(PTF_GROUPBY_TOKEN); }
  bool min_max_token() const { return has_flag(PTF_MIN_MAX_TOKEN); }
  bool stop_token() const { return has_flag(PTF_STOP_TOKEN); }
  bool casedown_token() const { return has_flag(PTF_CASEDOWN); }
  bool groupby_token() const { return has_flag(PTF_GROUPBY_TOKEN); }

  TO_STRING_KV(K_(flag));
private:
  uint64_t flag_;
};

typedef ObProcessTokenFlag ObAddWordFlag;

} // end namespace storage
} // end namespace oceanbase

#endif// OB_FTS_STRUCT_H_
