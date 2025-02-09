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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ik/ob_ik_arbitrator.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
int ObIKArbitrator::process(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  ObList<ObIKToken, ObIAllocator> &tokens = ctx.token_list().tokens();
  ObIKTokenChain *chain_need_arbitrate = nullptr;
  bool use_smart = ctx.is_smart();
  if (OB_FAIL(prepare(ctx))) {
  } else if (OB_ISNULL(chain_need_arbitrate
                       = static_cast<ObIKTokenChain *>(alloc_.alloc(sizeof(ObIKTokenChain))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else if (FALSE_IT(new (chain_need_arbitrate) ObIKTokenChain(alloc_))) {
  } else {
    while (OB_SUCC(ret) && !tokens.empty()) {
      ObIKToken token = tokens.get_first();
      bool is_add = false;
      if (OB_FAIL(tokens.pop_front())) {
        LOG_WARN("pop_front failed", K(ret));
      } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
        LOG_WARN("add token if conflict failed", K(ret));
      } else if (!is_add) {
        ObFTSortList::CellIter iter = chain_need_arbitrate->list().tokens().begin();
        ObIKTokenChain *judge_result;
        if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
          if (OB_FAIL(add_chain(chain_need_arbitrate))) {
            LOG_WARN("Add a chain failed", K(ret));
          } else {
            // add ok
          }
        } else if (OB_FAIL(optimize(ctx,
                                    chain_need_arbitrate,
                                    iter,
                                    chain_need_arbitrate->offset_len(),
                                    judge_result))) {
          LOG_WARN("Failed to optimize chain.", K(ret));
        } else if (OB_FAIL(add_chain(judge_result))) {
          LOG_WARN("Failed to add chain", K(ret));
        } else {
          // add best chain and delete origin chain
          chain_need_arbitrate->~ObIKTokenChain();
          alloc_.free(chain_need_arbitrate);
          chain_need_arbitrate = nullptr;
        }

        // start a new chain
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(chain_need_arbitrate = static_cast<ObIKTokenChain *>(
                                 alloc_.alloc(sizeof(ObIKTokenChain))))) {
        } else if (FALSE_IT(new (chain_need_arbitrate) ObIKTokenChain(alloc_))) {
        } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
          LOG_WARN("add token if conflict failed", K(ret));
        } else {
          // Add first, it should be added.
        }
      } else {
        // still use same cross chain
      }
    }

    // handle last chain
    ObIKTokenChain *judge_result;
    if (OB_FAIL(ret)) {
      if (nullptr != chain_need_arbitrate) {
        chain_need_arbitrate->~ObIKTokenChain();
        alloc_.free(chain_need_arbitrate);
      }
    } else if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
      if (OB_FAIL(add_chain(chain_need_arbitrate))) {
        LOG_WARN("Failed to add last chain", K(ret));
      }
    } else if (OB_FAIL(optimize(ctx,
                                chain_need_arbitrate,
                                chain_need_arbitrate->list().tokens().begin(),
                                chain_need_arbitrate->offset_len(),
                                judge_result))) {
    } else if (OB_FAIL(add_chain(judge_result))) {
      LOG_WARN("Failed to add last chain", K(ret));
    }
  }

  return ret;
}

int ObIKArbitrator::output_result(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  int64_t char_len = 0;
  ObIKTokenChain *chain = nullptr;
  for (int64_t current = 0; OB_SUCC(ret) && current < ctx.fulltext_len();) {
    ObFTCharUtil::CharType type;
    // maybe not so good to keep single, check it later
    if (OB_FAIL(ObCharset::first_valid_char(ctx.collation(),
                                            ctx.fulltext() + current,
                                            ctx.fulltext_len() - current,
                                            char_len))) {
      LOG_WARN("Failed to get next valid char", K(ret));
    } else if (OB_FAIL(ObFTCharUtil::classify_first_char(ctx.collation(),
                                                         ctx.fulltext() + current,
                                                         char_len,
                                                         type))) {
      LOG_WARN("Failed to classify first char", K(ret));
    } else if (ObFTCharUtil::CharType::USELESS == type) {
      current += char_len; // skip useless char
    } else if (OB_FAIL(chains_.get_refactored(current, chain)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("Failed to find in chain map", K(ret));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (!keep_single()) {
        // skip single word
      } else {
        // output single word
        ObIKToken token;
        token.offset_ = current;
        token.length_ = char_len;
        token.ptr_ = ctx.fulltext();
        token.char_cnt_ = 1;
        bool is_ignore = false;
        if (ObFTCharUtil::CharType::CHINESE == type) {
          token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
          if (OB_FAIL(ctx.result_list().push_back(token))) {
            LOG_WARN("Failed to output result to ctx", K(ret));
          }
        } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
          if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                         ctx.fulltext() + current,
                                                         char_len,
                                                         is_ignore))) {
            LOG_WARN("Failed to check ignore", K(ret));
          } else if (!is_ignore && !FALSE_IT(token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN)
                     && OB_FAIL(ctx.result_list().push_back(token))) {
            LOG_WARN("Failed to add token to ctx result", K(ret));
          } else {
            // ignore
          }
        }
      }
      current += char_len;
    } else {
      for (ObList<ObIKToken, ObIAllocator>::const_iterator iter = chain->list().tokens().begin();
           OB_SUCC(ret) && iter != chain->list().tokens().end();
           iter++) {
        const ObIKToken &token = *iter;
        if (!keep_single()) {
          current = token.offset_;
        } else {
          // output single word between two token
          while (OB_SUCC(ret) && current < token.offset_) {
            if (OB_FAIL(ObCharset::first_valid_char(ctx.collation(),
                                                    ctx.fulltext() + current,
                                                    ctx.fulltext_len() - current,
                                                    char_len))) {
              LOG_WARN("Failed to get next valid char, ", K(ret));
              break;
            } else {
              ObIKToken token;
              token.offset_ = current;
              token.length_ = char_len;
              token.ptr_ = ctx.fulltext();
              token.char_cnt_ = 1;
              bool is_ignore = false;
              if (ObFTCharUtil::CharType::CHINESE == type) {
                token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
                ctx.result_list().push_back(token);
              } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
                if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                               ctx.fulltext() + current,
                                                               char_len,
                                                               is_ignore))) {
                  LOG_WARN("Failed to check ignore", K(ret));
                } else if (!is_ignore) {
                  token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN;
                  ctx.result_list().push_back(token);
                } else {
                }
              }
              current += char_len;
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ctx.result_list().push_back(token))) {
          LOG_WARN("Failed to add token to ctx result", K(ret));
        } else
          // output the token
          current = token.offset_ + token.length_;
      }
    }
    if (OB_FAIL(ret)) {
      break;
    } else {
      // pass
    }
  }

  return ret;
}

int ObIKArbitrator::optimize(TokenizeContext &ctx,
                             ObIKTokenChain *chain,
                             ObFTSortList::CellIter iter,
                             int64_t fulltext_len,
                             ObIKTokenChain *&best)
{
  int ret = OB_SUCCESS;

  ObIKTokenChain *option = nullptr;
  ObList<ObFTSortList::CellIter, ObIAllocator> conflict_stack(alloc_);

  if (OB_ISNULL(option = static_cast<ObIKTokenChain *>(alloc_.alloc(sizeof(ObIKTokenChain))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else if (FALSE_IT(new (option) ObIKTokenChain(alloc_))) {
  } else if (OB_FAIL(try_add_next_words(chain, iter, option, true, conflict_stack))) {
    LOG_WARN("Failed to add chain and record conflict token.", K(ret));
  } else if (OB_ISNULL(best
                       = static_cast<ObIKTokenChain *>(alloc_.alloc(sizeof(ObIKTokenChain))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Alloc memory failed");
  } else if (FALSE_IT(new (best) ObIKTokenChain(alloc_))) {
  } else if (OB_FAIL(best->copy(option))) {
    LOG_WARN("Copy best option failed", K(ret));
  } else {
    while (OB_SUCC(ret) && !conflict_stack.empty()) {
      ObFTSortList::CellIter iter = conflict_stack.get_last();
      conflict_stack.pop_back();
      if (OB_FAIL(remove_conflict(*iter, option))) {
        LOG_WARN("Failed to remove conflict", K(ret));
      } else if (OB_FAIL(try_add_next_words(chain, iter, option, false, conflict_stack))) {
        LOG_WARN("Failed to add next words", K(ret));
      } else if (option->better_than(*best)) {
        best->~ObIKTokenChain();
        alloc_.free(best);
        if (OB_ISNULL(best = static_cast<ObIKTokenChain *>(alloc_.alloc(sizeof(ObIKTokenChain))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Alloc memory failed");
        } else if (FALSE_IT(new (best) ObIKTokenChain(alloc_))) {
        } else if (OB_FAIL(best->copy(option))) {
          LOG_WARN("Copy best option failed", K(ret));
        } else {
          // best option is copied
        }
      } else {
        // ignore worse chain
      }
    }
  }

  if (!OB_ISNULL(option)) {
    option->~ObIKTokenChain();
    alloc_.free(option);
  }

  if (OB_FAIL(ret)) {
    if (OB_ISNULL(best)) {
      best->~ObIKTokenChain();
      alloc_.free(best);
    }
  }
  return ret;
}

int ObIKArbitrator::try_add_next_words(ObIKTokenChain *chain,
                                       ObFTSortList::CellIter iter,
                                       ObIKTokenChain *option,
                                       bool need_conflict,
                                       ObList<ObFTSortList::CellIter, ObIAllocator> &conflict_stack)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chain) || OB_ISNULL(option)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iter is null", K(ret));
  }

  while (OB_SUCC(ret) && iter != chain->list().tokens().end()) {
    bool is_add = false;
    if (OB_FAIL(option->add_token_if_no_conflict(*iter, is_add))) {
      LOG_WARN("Failed to add token", K(ret));
    } else if (!is_add && need_conflict && OB_FAIL(conflict_stack.push_back(iter))) {
      LOG_WARN("push_back failed", K(ret));
    } else {
      // no add or push back over
      iter++;
    }
  }

  return ret;
}

int ObIKArbitrator::remove_conflict(const ObIKToken &token, ObIKTokenChain *option)
{
  int ret = OB_SUCCESS;
  ObIKToken tmp;
  while (OB_SUCC(ret) && option->check_conflict(token)) {
    if (OB_FAIL(option->pop_back(tmp))) {
      LOG_WARN("Failed to pop back token", K(ret));
    }
  }
  return ret;
}

int ObIKArbitrator::prepare(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  int cal_bucket_num = MAX(ctx.fulltext_len() / 100, 10);
  cal_bucket_num = MIN(cal_bucket_num, 100);
  if (OB_FAIL(chains_.create(cal_bucket_num, ObMemAttr(MTL_ID(), "IK ARBITRATE")))) {
    LOG_WARN("create chain map failed", K(ret));
  }
  return ret;
}

ObIKArbitrator::ObIKArbitrator() : alloc_(lib::ObMemAttr(MTL_ID(), "IK Arbitrator")) {}

int ObIKArbitrator::add_chain(ObIKTokenChain *chain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chain) && chain->list().is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid chain argument", K(ret));
  } else if (OB_FAIL(chains_.set_refactored(chain->min_offset(), chain))) {
    LOG_WARN("Failed to add chain to map", K(ret));
  }
  return ret;
}

ObIKArbitrator::~ObIKArbitrator()
{
  chains_.destroy();
  alloc_.reset();
}
} // namespace storage
} // namespace oceanbase