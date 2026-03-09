/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_TOKEN_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_TOKEN_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/list/ob_list.h"
#include "storage/fts/ik/ob_fast_list.h"

namespace oceanbase
{
namespace storage
{


static constexpr uint32_t HANDLE_SIZE_LIMIT = 256;

enum class ObIKTokenType : int8_t
{
  IK_CHINESE_TOKEN = 0,
  IK_ENGLISH_TOKEN = 1,
  IK_NUMBER_TOKEN = 2,
  IK_ARABIC_TOKEN = 3,
  IK_MIX_TOKEN = 4,
  IK_CNNUM_TOKEN = 5,
  IK_COUNT_TOKEN = 6,
  IK_CNQUAN_TOKEN = 7,
  IK_OTHER_CJK_TOKEN = 8,
  IK_SURROGATE_TOKEN = 9,
};

/** class ObIKToken:
 * @brief Token of the fulltext index.
 * It contains the start position and length of the word.
 * It holds the pointer to the original string.
 * Todo(@xinglipeng.xlp): maybe the pointer show be removed or moved to cursor
 */
struct ObIKToken
{
public:
  // current ptr is pointed to the fulltext();
  const char *ptr_;
  int64_t offset_;
  int64_t length_;
  int64_t char_cnt_;
  ObIKTokenType type_;

public:
  ~ObIKToken() {}
  OB_INLINE bool operator==(const ObIKToken &token) const
  {
    return (offset_ == token.offset_ && length_ == token.length_);
  }

  OB_INLINE bool operator>(const ObIKToken &token) const
  {
    return offset_ > token.offset_ || (offset_ == token.offset_ && length_ < token.length_);
  }

  OB_INLINE bool operator<(const ObIKToken &token) const
  {
    return offset_ < token.offset_ || (offset_ == token.offset_ && length_ > token.length_);
  }
};

template <typename ListType>
class ObFTSortList
{
public:
  ObFTSortList(ObIAllocator &alloc) : tokens_(alloc) {}
  ~ObFTSortList() { tokens_.reset(); }

  int add_token(const ObIKToken &token);

  bool is_empty() const { return tokens_.empty(); }

  void reset() { tokens_.reset(); }
  void reuse() { tokens_.reuse(); }

  int64_t min();

  int64_t max();

  ListType &tokens() { return tokens_; }
  const ListType &tokens() const { return tokens_; }

public:
  typedef typename ListType::iterator CellIter;
  typedef typename ListType::const_iterator ConstCellIter;

private:
  ListType tokens_;
};

typedef ObFTSortList<ObList<ObIKToken, ObIAllocator>> ObFTLightSortList;
typedef ObFTSortList<ObFastList<ObIKToken, HANDLE_SIZE_LIMIT>> ObFTFastSortList;

class ObIKTokenChain
{
public:
  ObIKTokenChain(ObIAllocator &alloc) : list_(alloc) {}
  ~ObIKTokenChain() { list_.reset(); }

public:
  int add_token_if_conflict(const ObIKToken &token, bool &added);

  int add_token_if_no_conflict(const ObIKToken &token, bool &added);

  int pop_back(ObIKToken &token);

  bool check_conflict(const ObIKToken &token);

  ObFTLightSortList &list() { return list_; }

  bool better_than(const ObIKTokenChain &other) const;

  int copy(ObIKTokenChain *other);

  int64_t min_offset() const { return min_offset_; }

  int64_t max_offset() const { return max_offset_; }

  int64_t offset_len() const { return max_offset_ - min_offset_; }

  int64_t payload() const { return payload_; }

  int64_t x_weight() const;

  int64_t p_weight() const;

private:
  int min_offset_ = -1;
  int max_offset_ = -1;
  int payload_ = -1;
  ObFTLightSortList list_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_TOKEN_H_
