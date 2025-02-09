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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
class TokenizeContext;

class ObIKArbitrator
{
public:
  ObIKArbitrator();
  ~ObIKArbitrator();

  int process(TokenizeContext &ctx);

  int output_result(TokenizeContext &ctx);

private:
  int prepare(TokenizeContext &ctx);

  int add_chain(ObIKTokenChain *chain);

  int optimize(TokenizeContext &ctx,
               ObIKTokenChain *option,
               ObFTSortList::CellIter iter,
               int64_t fulltext_len,
               ObIKTokenChain *&best);

  int try_add_next_words(ObIKTokenChain *chain,
                         ObFTSortList::CellIter iter,
                         ObIKTokenChain *option,
                         bool need_conflict,
                         ObList<ObFTSortList::CellIter, ObIAllocator> &conflict_stack);

  int remove_conflict(const ObIKToken &token, ObIKTokenChain *option);

  bool keep_single() const { return true; }

private:
  ObArenaAllocator alloc_;
  hash::ObHashMap<int64_t, ObIKTokenChain *> chains_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIKArbitrator);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_
