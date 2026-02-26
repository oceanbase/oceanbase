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
#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace storage
{

class ObFTWord final
{
public:
  ObFTWord() : word_(), meta_() {}
  ObFTWord(const int64_t length, const char *ptr, const ObObjMeta &meta) : meta_(meta)
  {
    word_.set_string(ptr, length);
  }
  ~ObFTWord() = default;

  OB_INLINE const ObDatum &get_word() const { return word_; }
  OB_INLINE ObCollationType get_collation_type() const { return meta_.get_collation_type(); }
  OB_INLINE bool empty() const { return word_.get_string().empty(); }
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObFTWord &other) const;
  OB_INLINE bool operator !=(const ObFTWord &other) const { return !(other == *this); }

  TO_STRING_KV(K_(meta), K_(word));

private:
  ObDatum word_;
  ObObjMeta meta_;
};

typedef common::hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;

class ObAddWordFlag final
{
private:
  static const uint64_t AWF_NONE         = 0;
  static const uint64_t AWF_MIN_MAX_WORD = 1 << 0; // filter words that are less than a minimum or greater
                                                   // than a maximum word length.
  static const uint64_t AWF_STOPWORD     = 1 << 1; // filter by sotp word table.
  static const uint64_t AWF_CASEDOWN     = 1 << 2; // convert characters from uppercase to lowercase.
  static const uint64_t AWF_GROUPBY_WORD = 1 << 3; // distinct and word aggregation
public:
  ObAddWordFlag() : flag_(AWF_NONE) {}
  ~ObAddWordFlag() = default;
public:
  void set_flag(const uint64_t flag) { flag_ |= flag; }
private:
  void clear_flag(const uint64_t flag) { flag_ &= ~flag; }
  bool has_flag(const uint64_t flag) const { return (flag_ & flag) == flag; }
public:
  void set_min_max_word() { set_flag(AWF_MIN_MAX_WORD); }
  void set_stop_word() { set_flag(AWF_STOPWORD); }
  void set_casedown() { set_flag(AWF_CASEDOWN); }
  void set_groupby_word() { set_flag(AWF_GROUPBY_WORD); }
  void clear() { flag_ = AWF_NONE; }
  void clear_min_max_word() { clear_flag(AWF_MIN_MAX_WORD); }
  void clear_stop_word() { clear_flag(AWF_STOPWORD); }
  void clear_casedown() { clear_flag(AWF_CASEDOWN); }
  void clear_groupby_word() { clear_flag(AWF_GROUPBY_WORD); }
  bool min_max_word() const { return has_flag(AWF_MIN_MAX_WORD); }
  bool stopword() const { return has_flag(AWF_STOPWORD); }
  bool casedown() const { return has_flag(AWF_CASEDOWN); }
  bool groupby_word() const { return has_flag(AWF_GROUPBY_WORD); }
  TO_STRING_KV(K_(flag));
private:
  uint64_t flag_;
};

} // end namespace storage
} // end namespace oceanbase

#endif// OB_FTS_STRUCT_H_
