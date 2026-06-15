/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
class ObFTCacheDict final : public ObIFTDict
{
public:
  ObFTCacheDict(ObCollationType coll_type, const ObFTDAT *dat)
      : coll_type_(coll_type), dat_(dat), reader_(dat)
  {
  }
  int init() override;
  int match(const ObString &single_word, ObDATrieHit &hit) const override;
  int match(const ObString &words, bool &is_match) const override;
  int match_with_hit(const ObString &single_word,
                     const ObDATrieHit &last_hit,
                     ObDATrieHit &hit) const override;

  static int put_and_fetch_cache_entry(const uint64_t table_id,
                                        const uint64_t tenant_id,
                                        const int32_t range_id,
                                        const ObFTDAT *dat_buff,
                                        const int64_t snapshot_version,
                                        const int32_t range_count,
                                        ObFTCacheRangeHandle &handle);

private:
  common::ObKVCacheHandle handle_; // used to pin the mem block later
  ObCollationType coll_type_;
  const ObFTDAT *dat_;
  ObFTDATReader<void> reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTCacheDict);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_
