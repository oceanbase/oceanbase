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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
class TokenizeContext
{
public:
  TokenizeContext(ObCollationType coll_type,
                  ObIAllocator &allocator,
                  const char *fulltext,
                  int64_t fulltext_len,
                  bool is_smart);

  ~TokenizeContext();

  int init();
  int reset_resource();

  int get_next_token(const char *&word, int64_t &word_len, int64_t &offset, int64_t &char_cnt);

  int compound(ObIKToken &result);

  int current_char(const char *&ch, uint8_t &char_len);
  int current_char_type(ObFTCharUtil::CharType &type);

  int step_next();

  ObCollationType collation() const;
  int64_t get_end_cursor() const;
  const char *fulltext() const;
  int64_t fulltext_len() const;
  int64_t get_cursor() const;

  bool is_last() const;
  bool iter_end() const;
  bool is_smart() const;

  int add_chain(ObIKTokenChain *chain);
  int add_token(const char *fulltext,
                int64_t offset,
                int64_t length,
                int64_t char_cnt,
                ObIKTokenType type);

  ObFTSortList &token_list() { return token_list_; }

  ObList<ObIKToken, ObIAllocator> &result_list() { return result_list_; }

  int32_t handle_size() const { return handle_size_; }

private:
  int prepare_next_char();

  ObCollationType coll_type_;
  const char *fulltext_;
  int64_t fulltext_len_;

  int64_t cursor_;
  int64_t next_char_len_;
  ObFTCharUtil::CharType next_char_type_;

  uint32_t handle_size_;
  bool is_smart_;

  ObFTSortList token_list_;
  ObList<ObIKToken, ObIAllocator> result_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(TokenizeContext);
};

class ObIIKProcessor
{
public:
  ObIIKProcessor() {}

  virtual ~ObIIKProcessor() {}

  int process(TokenizeContext &ctx);

  virtual int do_process(TokenizeContext &ctx,
                         const char *ch,
                         const uint8_t char_len,
                         const ObFTCharUtil::CharType type)
      = 0;
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_