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

#include <cstdint>
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_cache_dict.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
int ObFTCacheDict::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObFTCacheDict::match(const ObString &single_word, ObDATrieHit &hit) const
{
  return reader_.match_with_hit(single_word, hit, hit);
}

int ObFTCacheDict::match(const ObString &words, bool &is_match) const
{
  int ret = OB_SUCCESS;

  int64_t char_len = 0;
  ObDATrieHit hit(this, 0);
  for (int64_t offset = 0; OB_SUCC(ret) && offset < words.length(); offset += char_len) {
    if (OB_FAIL(ObCharset::first_valid_char(coll_type_,
                                            words.ptr() + offset,
                                            words.length() - offset,
                                            char_len))) {
      LOG_WARN("Invalid string encoding", K(ret), K(words));
    } else if (OB_FAIL(match_with_hit(ObString(char_len, words.ptr() + offset), hit, hit))) {
      LOG_WARN("Failed to do match with hit", K(ret));
    } else if (hit.is_match() && offset + char_len == words.length()) {
      is_match = true;
      break;
    } else if (hit.is_unmatch()) {
      is_match = false;
      break;
    } else {
      // continue to match
    }
  }
  return ret;
}

int ObFTCacheDict::match_with_hit(const ObString &single_word,
                                  const ObDATrieHit &last_hit,
                                  ObDATrieHit &hit) const
{
  return reader_.match_with_hit(single_word, last_hit, hit);
}

int ObFTCacheDict::make_and_fetch_cache_entry(const ObFTDictDesc &desc,
                                              ObFTDAT *dat_buff,
                                              const size_t buff_size,
                                              const int32_t range_id,
                                              const ObDictCacheValue *&value,
                                              ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  ObDictCache &cache = ObDictCache::get_instance();

  uint64_t name = static_cast<uint64_t>(desc.type_);
  const ObDictCacheKey put_key(name, MTL_ID(), desc.type_, range_id);
  const ObDictCacheValue put_value(dat_buff);
  if (OB_FAIL(cache.put_and_fetch_dict(put_key, put_value, value, handle))) {
    LOG_WARN("Failed to put dict into kv cache", K(ret));
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
