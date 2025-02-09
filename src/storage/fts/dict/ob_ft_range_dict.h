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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_RANGE_DICT_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_RANGE_DICT_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_vector.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObFTRangeDict final : public ObIFTDict
{
public:
  ObFTRangeDict(ObIAllocator &alloc,
                ObFTCacheRangeContainer *range_container,
                const ObFTDictDesc &desc)
      : is_inited_(false), desc_(desc), range_alloc_(lib::ObMemAttr(MTL_ID(), "Range Dict")),
        range_dicts_(&range_alloc_), range_container_(range_container)
  {
  }
  ~ObFTRangeDict() override{};

public:
  struct ObFTRange final
  {
    ObFTSingleWord start_;
    ObFTSingleWord end_;
    ObIFTDict *dict_; // a cache dict
  };

public:
  int init() override;
  int match(const ObString &single_word, ObDATrieHit &hit) const override;
  int match(const ObString &words, bool &is_match) const override;
  int match_with_hit(const ObString &single_word,
                     const ObDATrieHit &last_hit,
                     ObDATrieHit &hit) const override;

public:
  int build_dict_from_cache(const ObFTCacheRangeContainer &range_container);

  static int try_load_cache(const ObFTDictDesc &desc,
                            const uint32_t range_count,
                            ObFTCacheRangeContainer &range_container);
  static int build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &range_container);

private:
  // build cache
  static int build_ranges(const ObFTDictDesc &desc,
                          ObIFTDictIterator &iter,
                          ObFTCacheRangeContainer &range_container);

  // build one range's cache
  static int build_one_range(const ObFTDictDesc &desc,
                             const int32_t range_id,
                             ObIFTDictIterator &iter,
                             ObFTCacheRangeContainer &container,
                             bool &build_next_range);

private:
  void destroy()
  {
    for (int64_t i = 0; i < range_dicts_.size(); i++) {
      range_dicts_[i].dict_->~ObIFTDict(); // destroy dict
    }
    range_dicts_.reset();
    range_alloc_.reset();
  }

  int find_first_char_range(const ObString &single_word, ObIFTDict *&dict) const;

private:
  static constexpr int DEFAULT_KEY_PER_RANGE = 50000; // by estimated
  bool is_inited_;
  ObFTDictDesc desc_;
  ObArenaAllocator range_alloc_;
  ObVector<ObFTRange, ObArenaAllocator> range_dicts_;
  ObFTCacheRangeContainer *range_container_; // only used to read cache

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTRangeDict);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_RANGE_DICT_H_
