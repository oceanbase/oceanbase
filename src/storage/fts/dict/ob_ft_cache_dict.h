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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/fts/dict/ob_ft_cache.h"
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
  ObFTCacheDict(ObCollationType coll_type, ObFTDAT *dat)
      : coll_type_(coll_type), dat_(dat), reader_(dat)
  {
  }
  int init() override;
  int match(const ObString &single_word, ObDATrieHit &hit) const override;
  int match(const ObString &words, bool &is_match) const override;
  int match_with_hit(const ObString &single_word,
                     const ObDATrieHit &last_hit,
                     ObDATrieHit &hit) const override;

public:
  static int make_and_fetch_cache_entry(const ObFTDictDesc &desc,
                                        ObFTDAT *dat_buff,
                                        const size_t buff_size,
                                        const int32_t range_id,
                                        const ObDictCacheValue *&value,
                                        ObKVCacheHandle &handle);

private:
  ObKVCacheHandle handle_; // used to pin the mem block later
  ObCollationType coll_type_;
  ObFTDAT *dat_ = nullptr;
  ObFTDATReader<void> reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTCacheDict);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_DICT_H_
