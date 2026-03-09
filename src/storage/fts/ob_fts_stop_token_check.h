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

#ifndef OB_FTS_STOP_WORD_H_
#define OB_FTS_STOP_WORD_H_

#include "lib/hash/ob_hashset.h"
#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

class ObFTParserProperty;

#define FTS_STOP_TOKEN_MAX_LENGTH 10

static const char ob_stop_token_table_utf8[][FTS_STOP_TOKEN_MAX_LENGTH] = {
  u8"a",
  u8"about",
  u8"an",
  u8"are",
  u8"as",
  u8"at",
  u8"be",
  u8"by",
  u8"com",
  u8"de",
  u8"en",
  u8"for",
  u8"from",
  u8"how",
  u8"i",
  u8"in",
  u8"is",
  u8"it",
  u8"la",
  u8"of",
  u8"on",
  u8"or",
  u8"that",
  u8"the",
  u8"this",
  u8"to",
  u8"was",
  u8"what",
  u8"when",
  u8"where",
  u8"who",
  u8"will",
  u8"with",
  u8"und",
  u8"www"
};

typedef common::hash::ObHashSet<
    storage::ObFTToken,
    common::hash::NoPthreadDefendMode,
    common::hash::hash_func<storage::ObFTToken>,
    common::hash::equal_to<storage::ObFTToken>,
    common::hash::SimpleAllocer<
        typename common::hash::HashSetTypes<storage::ObFTToken>::AllocType,
        common::hash::NodeNumTraits<typename common::hash::HashSetTypes<storage::ObFTToken>::AllocType>::NODE_NUM,
        common::hash::NoPthreadDefendMode>> ObStopTokenTable;

class ObStopTokenChecker final
{
public:
  ObStopTokenChecker() :
      is_inited_(false), collation_type_(CS_TYPE_INVALID), stop_token_hash_table_(nullptr) { }
  ~ObStopTokenChecker() { reset(); }
  int init(const ObCollationType coll, ObStopTokenTable *stop_token_hash_table);
  void reset() { is_inited_ = false; collation_type_ = CS_TYPE_INVALID; stop_token_hash_table_ = nullptr; }
  int check_is_stop_token(const ObFTToken &token, bool &is_stop_token) const;

private:
  bool is_inited_;
  ObCollationType collation_type_;
  ObStopTokenTable *stop_token_hash_table_;
};

class ObStopTokenCheckerGen final
{
public:
  ObStopTokenCheckerGen() :
      is_inited_(false),
      allocator_(),
      lock_(common::ObLatchIds::OB_FTS_STOP_TOKEN_CHECKER_GEN_LOCK),
      stop_token_hash_tables_()
  { }
  ~ObStopTokenCheckerGen()
  {
    reset();
  }

  int init();
  void reset();
  int get_stop_token_checker_by_coll(const ObCollationType collation_type,
                                     ObStopTokenChecker &stop_token_checker);

private:
  int generate_stop_token_hash_table_by_coll(const ObCollationType coll);
  int convert_charset(const ObString &src_string,
                      const ObCollationType from_collation,
                      const ObCollationType to_collation,
                      ObString &converted_string);

private:
  static const int64_t DEFAULT_STOP_TOKEN_NUMBERS = 36L;
  static const int64_t DEFAULT_STOP_TOKEN_TABLE_CAPACITY =
      static_cast<int64_t>(1) << (64 - __builtin_clzll(DEFAULT_STOP_TOKEN_NUMBERS));

  typedef common::hash::ObHashMap<
      ObCollationType,
      ObStopTokenTable *,
      common::hash::NoPthreadDefendMode,
      common::hash::hash_func<int64_t>,
      common::hash::equal_to<ObCollationType>,
      common::hash::SimpleAllocer<
          typename common::hash::HashMapTypes<ObCollationType, ObStopTokenTable *>::AllocType,
          common::hash::NodeNumTraits<typename common::hash::HashMapTypes<ObCollationType, ObStopTokenTable *>::AllocType>::NODE_NUM,
          common::hash::NoPthreadDefendMode>> StopTokenHashMap;

private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  common::TCRWLock lock_;
  StopTokenHashMap stop_token_hash_tables_;

  static_assert(sizeof(ob_stop_token_table_utf8) / sizeof(ob_stop_token_table_utf8[0]) <= DEFAULT_STOP_TOKEN_NUMBERS,
      "ob_stop_token_table's number shouldn't be greater than DEFAULT_STOP_TOKEN_NUMBERS");
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_STOP_WORD_H_
