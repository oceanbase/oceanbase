/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/dict/ob_ft_range_dict.h"

#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_smart_var.h"
#include "share/scn.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_cache_dict.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_mgr.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"
#include "storage/fts/dict/ob_ft_dict_table_iter.h"
#include "storage/fts/dict/ob_ft_trie.h"
#include "storage/tx/ob_trans_service.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{
int ObFTRangeDict::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    if (OB_FAIL(build_dict_from_cache(*range_container_))) {
      LOG_WARN("Failed to build dict from cache", K(ret));
    }
    is_inited_ = true;
  }
  return ret;
}

int ObFTRangeDict::match(const ObString &single_word, ObDATrieHit &hit) const
{
  int ret = OB_SUCCESS;
  ObIFTDict *dict = nullptr;
  if (OB_FAIL(find_first_char_range(single_word, dict)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to find first char range", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    hit.set_unmatch();
    ret = OB_SUCCESS;
  } else {
    // do nothing
    hit.dict_ = dict; // set dict
    if (OB_FAIL(dict->match(single_word, hit))) {
      LOG_WARN("Failed to match", K(ret));
    }
  }
  return ret;
}

int ObFTRangeDict::match(const ObString &words, bool &is_match) const
{
  // find first char range and find dict
  int ret = OB_SUCCESS;
  ObIFTDict *dict = nullptr;

  int64_t char_len;

  if (OB_FAIL(
          ObCharset::first_valid_char(desc_.coll_type_, words.ptr(), words.length(), char_len))) {
    LOG_WARN("Failed to find first char", K(ret));
  } else if (OB_FAIL(find_first_char_range(ObString(char_len, words.ptr()), dict))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_match = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to find first char range", K(ret));
    }
  } else if (OB_UNLIKELY(OB_ISNULL(dict))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Dict is null", K(ret));
  } else if (OB_FAIL(dict->match(words, is_match))) {
    LOG_WARN("Failed to match", K(ret));
  }
  return ret;
}

int ObFTRangeDict::match_with_hit(const ObString &single_word,
                                  const ObDATrieHit &last_hit,
                                  ObDATrieHit &hit) const
{
  return last_hit.dict_->match_with_hit(single_word, last_hit, hit);
}

int ObFTRangeDict::find_first_char_range(const ObString &single_word, ObIFTDict *&dict) const
{
  int ret = OB_SUCCESS;
  int64_t left = 0;
  int64_t right = range_dicts_.size() - 1;
  int64_t candidate = -1;
  dict = nullptr;
  while (left <= right) {
    const int64_t mid = left + ((right - left) >> 1);
    const ObFTRange &mid_range = range_dicts_[mid];
    const int cmp_start_ret = ObCharset::strcmp(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                                mid_range.start_.get_token(),
                                                single_word);
    if (cmp_start_ret <= 0) {
      candidate = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  if (OB_LIKELY(candidate >= 0)) {
    const ObFTRange &range = range_dicts_[candidate];
    const int cmp_end_ret = ObCharset::strcmp(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                              range.end_.get_token(),
                                              single_word);
    if (cmp_end_ret >= 0) {
      dict = range.dict_;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObFTRangeDict::build_dict_from_cache(const ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  for (ObList<ObFTCacheRangeHandle *, ObIAllocator>::const_iterator iter = range_container.get_handles().begin();
       OB_SUCC(ret) && iter != range_container.get_handles().end();
       iter++) {
    ObFTCacheRangeHandle *ptr = *iter;
    ObFTCacheDict *dict = nullptr;
    const ObFTDAT *dat = ptr->value_->dat_block_;
    if (OB_ISNULL(dict = OB_NEWx(ObFTCacheDict, &range_alloc_, ObCollationType::CS_TYPE_UTF8MB4_BIN, dat))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret));
    } else {
      ObFTRange range;
      range.start_ = dat->start_token_;
      range.end_ = dat->end_token_;
      range.dict_ = dict;
      if (OB_FAIL(range_dicts_.push_back(range))) {
        LOG_WARN("Failed to push back range dict", K(ret));
      }
    }
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
