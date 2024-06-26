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

#ifndef OCEANBASE_TABLE_REDIS_OB_REDIS_META_H
#define OCEANBASE_TABLE_REDIS_OB_REDIS_META_H

#include "share/table/ob_table.h"

namespace oceanbase
{
namespace table
{
class ObRedisMeta
{
  OB_UNIS_VERSION(1);

public:
  virtual ~ObRedisMeta()
  {}
  VIRTUAL_TO_STRING_KV(K_(ttl));
  virtual void reset()
  {
    ttl_ = INT_MAX64;
    has_meta_value_ = false;
    meta_col_name_ = "";
    // reset means key insert time is now
    insert_ts_ = ObTimeUtility::current_time();
  }

  virtual int decode(const ObString &encoded_content);

  virtual int encode(ObIAllocator &allocator, ObString &encoded_content) const;

protected:
  explicit ObRedisMeta() : ttl_(INT_MAX64), has_meta_value_(false), meta_col_name_(), insert_ts_(ObTimeUtility::current_time())
  {}

  // element count in meta value
  static const int64_t ELE_COUNT = 4;
  static const int64_t RESERVED_SIZE = 4;
  int64_t ttl_;
  bool has_meta_value_;
  ObString meta_col_name_;
  int64_t insert_ts_;
  int64_t reserved_[RESERVED_SIZE];  // reserved for future
};

class ObRedisListMeta : public ObRedisMeta
{
  OB_UNIS_VERSION(1);

public:
  ObRedisListMeta()
      : count_(0),
        left_idx_(INIT_INDEX),
        right_idx_(INIT_INDEX),
        ttl_(INT_MAX64)
  {
    has_meta_value_ = true;
  }
  ~ObRedisListMeta()
  {}
  TO_STRING_KV(K_(count), K_(left_idx), K_(right_idx), K_(ttl));
  void reset() override
  {
    count_ = 0;
    left_idx_ = INIT_INDEX;
    right_idx_ = INIT_INDEX;
    ttl_ = INT_MAX64;
  }

  virtual int decode(const ObString &encoded_content) override;
  virtual int encode(ObIAllocator &allocator, ObString &encoded_content) const override;

public:
  static const int64_t INIT_INDEX = 0;
  // element count in list meta value
  static const int64_t ELE_COUNT = 4;
  static const int64_t RESERVED_SIZE = 2;
  static const char META_SPLIT_FLAG = ':';

  int64_t count_;
  int64_t left_idx_;
  int64_t right_idx_;
  int64_t ttl_;
  int64_t reserved_[RESERVED_SIZE];  // reserved for future
};
}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_REDIS_OB_REDIS_META_H */
