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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/ob_errno.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

#include <cstdint>

namespace oceanbase
{
namespace storage
{
// typedef uint64_t ObFTTableID;
// typedef ObFTTableID ObFTDictTableID;

class ObFTDictInfo
{
public:
  ObFTDictInfo()
      : name_(""), type_(ObFTDictType::DICT_TYPE_INVALID), charset_(CHARSET_INVALID), version_(0),
        range_count_(0)
  {
  }

public:
  char name_[2048]; // for now
  ObFTDictType type_;
  ObCharsetType charset_;
  int64_t version_; // in memory
  int32_t range_count_;
};

class ObFTDictHub
{
public:
  ObFTDictHub() : is_inited_(false), dict_map_() {}

  int init();

  int destroy();

  int get_dict_info(const uint64_t &name, ObFTDictInfo &info);

  int put_dict_info(const uint64_t &name, const ObFTDictInfo &info);

  int push_dict_version(const uint64_t &dict_name);

private:
  bool is_inited_;
  // holds info of dict
  common::ObConcurrentHashMap<uint64_t, ObFTDictInfo> dict_map_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
