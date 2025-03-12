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

#include "storage/fts/dict/ob_ft_range_dict.h"

#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "ob_smart_var.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_cache_dict.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"
#include "storage/fts/dict/ob_ft_dict_table_iter.h"
#include "storage/fts/dict/ob_ft_trie.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{
int ObFTRangeDict::build_one_range(const ObFTDictDesc &desc,
                                   const int32_t range_id,
                                   ObIFTDictIterator &iter,
                                   ObFTCacheRangeContainer &container,
                                   bool &build_next_range)
{
  int ret = OB_SUCCESS;
  build_next_range = true;

  ObArenaAllocator tmp_alloc(lib::ObMemAttr(MTL_ID(), "Temp trie"));

  ObFTDATBuilder<void> builder(tmp_alloc);
  storage::ObFTTrie<void> trie(tmp_alloc, desc.coll_type_);

  int count = 0;
  bool range_end = false;

  int64_t first_char_len = 0;
  ObFTSingleWord end_char;
  ObFTSingleWord start_char;

  ObFTDAT *dat_buff = nullptr;
  size_t buffer_size = 0;

  while (OB_SUCC(ret) && !range_end) {
    ObString key;
    if (OB_FAIL(iter.get_key(key))) {
      LOG_WARN("Failed to get key", K(ret));
    } else if (OB_FALSE_IT(++count)) {
      // do nothing
    } else if (count >= DEFAULT_KEY_PER_RANGE
               && OB_FAIL(ObCharset::first_valid_char(desc.coll_type_,
                                                      key.ptr(),
                                                      key.length(),
                                                      first_char_len))) {
      LOG_WARN("First char is not valid.");
    } else if (DEFAULT_KEY_PER_RANGE == count
               && OB_FAIL(end_char.set_word(key.ptr(), first_char_len))) {
      LOG_WARN("Failed to record first char.", K(ret));
    } else if (count > DEFAULT_KEY_PER_RANGE
               && (end_char.get_word() != ObString(first_char_len, key.ptr()))) {
      // end of range, this key is not consumed.
      range_end = true;
    } else if (OB_FAIL(trie.insert(key, {}))) {
      LOG_WARN("Failed to insert key to trie", K(ret));
    } else if (OB_FAIL(iter.next()) && OB_ITER_END != ret) {
      LOG_WARN("Failed to step to next word entry.", K(ret));
    }
  }

  if (OB_ITER_END == ret) {
    build_next_range = false; // no more data
    ret = OB_SUCCESS;
  }
  ObFTCacheRangeHandle *info = nullptr;

  if (OB_FAIL(ret)) {
    // to do clean up
  } else if (OB_FAIL(builder.init(trie))) {
    LOG_WARN("Failed to build dat.", K(ret));
  } else if (OB_FAIL(builder.build_from_trie(trie))) {
    LOG_WARN("Failed to build datrie.", K(ret));
  } else if (OB_FAIL(builder.get_mem_block(dat_buff, buffer_size))) {
    LOG_WARN("Failed to get mem block.", K(ret));
  } else if (OB_FAIL(container.fetch_info_for_dict(info))) {
    LOG_WARN("Failed to fetch info for dict.", K(ret));
  } else if (OB_FAIL(ObFTCacheDict::make_and_fetch_cache_entry(desc,
                                                               dat_buff,
                                                               buffer_size,
                                                               range_id,
                                                               info->value_,
                                                               info->handle_))) {
    LOG_WARN("Failed to put dict into kv cache");
  } else {
    // okay
  }
  tmp_alloc.reset();

  return ret;
}

int ObFTRangeDict::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    if (OB_FAIL(build_dict_from_cache(*range_container_))) {
      LOG_WARN("Failed to build dict from cache.", K(ret));
    }
    is_inited_ = true;
  }
  return ret;
}
int ObFTRangeDict::build_ranges(const ObFTDictDesc &desc,
                                ObIFTDictIterator &iter,
                                ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;

  bool build_next_range = true;

  int32_t range_id = 0;
  while (OB_SUCC(ret) && build_next_range) {
    if (OB_FAIL(ObFTRangeDict::build_one_range(desc,
                                               range_id++,
                                               iter,
                                               range_container,
                                               build_next_range))) {
      LOG_WARN("fail to build range", K(ret));
    }
  }
  return ret;
}

int ObFTRangeDict::match(const ObString &single_word, ObDATrieHit &hit) const
{
  int ret = OB_SUCCESS;
  ObIFTDict *dict = nullptr;
  if (OB_FAIL(find_first_char_range(single_word, dict)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to find first char range.", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    hit.set_unmatch();
    ret = OB_SUCCESS;
  } else {
    // do nothing
    hit.dict_ = dict; // set dict
    if (OB_FAIL(dict->match(single_word, hit))) {
      LOG_WARN("Failed to match.", K(ret));
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
      LOG_WARN("Failed to find first char range.", K(ret));
    }
  } else if (OB_ISNULL(dict)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dict is null.", K(ret));
  } else if (OB_FAIL(dict->match(words, is_match))) {
    LOG_WARN("Failed to match.", K(ret));
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
  bool found = false;
  for (int i = 0; OB_SUCC(ret) && !found && i < range_dicts_.size(); ++i) {
    if (ObCharset::strcmp(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                          range_dicts_[i].start_.get_word(),
                          single_word)
            <= 0
        && ObCharset::strcmp(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                             range_dicts_[i].end_.get_word(),
                             single_word)
               >= 0) {
      dict = range_dicts_[i].dict_;
      found = true;
    }
  }
  if (!found) {
    // not found, dis match
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObFTRangeDict::build_dict_from_cache(const ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  for (ObList<ObFTCacheRangeHandle *, ObIAllocator>::const_iterator iter
       = range_container.get_handles().begin();
       OB_SUCC(ret) && iter != range_container.get_handles().end();
       iter++) {
    ObFTCacheRangeHandle *ptr = *iter;
    ObFTCacheDict *dict = nullptr;

    if (OB_ISNULL(dict = static_cast<ObFTCacheDict *>(range_alloc_.alloc(sizeof(ObFTCacheDict))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory.", K(ret));
    } else {
      ObFTDAT *dat = ptr->value_->dat_block_;
      new (dict) ObFTCacheDict(ObCollationType::CS_TYPE_UTF8MB4_BIN, dat);
      ObFTRange range;
      range.start_ = dat->start_word_;
      range.end_ = dat->end_word_;
      range.dict_ = dict;
      if (OB_FAIL(range_dicts_.push_back(range))) {
        LOG_WARN("Failed to push back range dict.", K(ret));
      }
    }
  }
  return ret;
}

int ObFTRangeDict::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;

  ObString table_name;
  switch (desc.type_) {
  case ObFTDictType::DICT_IK_MAIN: {
    table_name = ObString(share::OB_FT_DICT_IK_UTF8_TNAME);
  } break;
  case ObFTDictType::DICT_IK_QUAN: {
    table_name = ObString(share::OB_FT_QUANTIFIER_IK_UTF8_TNAME);
  } break;
  case ObFTDictType::DICT_IK_STOP: {
    table_name = ObString(share::OB_FT_STOPWORD_IK_UTF8_TNAME);
  } break;
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported dict type.", K(ret));
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      ObFTDictTableIter iter_table(result);
      if (OB_FAIL(iter_table.init(table_name))) {
        LOG_WARN("Failed to init iterator.", K(ret));
      } else if (OB_FAIL(ObFTRangeDict::build_ranges(desc, iter_table, range_container))) {
        LOG_WARN("Failed to build ranges.", K(ret));
      }
    }
  }

  return ret;
}

int ObFTRangeDict::try_load_cache(const ObFTDictDesc &desc,
                                  const uint32_t range_count,
                                  ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  uint64_t name = static_cast<uint64_t>(desc.type_);

  for (int i = 0; OB_SUCC(ret) && i < range_count; ++i) {
    ObDictCacheKey key(name, MTL_ID(), desc.type_, i);
    ObFTCacheRangeHandle *info = nullptr;
    if (OB_FAIL(range_container.fetch_info_for_dict(info))) {
      LOG_WARN("Failed to fetch info for dict.", K(ret));
    } else if (OB_FAIL(ObDictCache::get_instance().get_dict(key, info->value_, info->handle_))
               && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get dict from kv cache.", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // not found, build cache outthere
    } else if (FALSE_IT(info->type_ = desc.type_)) {
      // impossible
    }
  }

  if (OB_FAIL(ret)) {
    range_container.reset();
  }

  return ret;
}

} //  namespace storage
} //  namespace oceanbase
