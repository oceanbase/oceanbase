/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "storage/fts/dict/ob_ft_cache_container.h"
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

int ObFTCacheDict::put_and_fetch_cache_entry(const uint64_t table_id,
                                              const uint64_t tenant_id,
                                              const int32_t range_id,
                                              const ObFTDAT *dat_buff,
                                              const int64_t snapshot_version,
                                              const int32_t range_count,
                                              ObFTCacheRangeHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObDictCacheKey key(table_id, tenant_id, range_id);
  const ObDictCacheValue value(dat_buff, snapshot_version, range_count);
  if (OB_FAIL(ObDictCache::get_instance().put_and_fetch_dict(key, value, handle.value_, handle.handle_, true))) {
    LOG_WARN("Failed to put dict into kvcache", K(ret), K(table_id), K(tenant_id), K(range_id));
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
