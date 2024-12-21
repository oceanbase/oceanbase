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

#include "share/table/redis/ob_redis_common.h"
#include "share/table/ob_table.h"

namespace oceanbase
{
namespace table
{
class ObRedisCtx;
class ObRedisMeta;
class ObRedisMetaUtil
{
public:
  static int create_redis_meta_by_model(ObIAllocator &allocator, ObRedisModel model, ObRedisMeta *&meta);
  static int build_meta_rowkey_by_model(
      ObIAllocator &allocator,
      ObRedisModel model,
      int64_t db,
      const ObString &key,
      ObRowkey &meta_rowkey);
};
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
    insert_ts_ = ObTimeUtility::fast_current_time();
    is_exists_ = true;
  }
  virtual OB_INLINE bool is_expired() const
  {
    return ttl_ < ObTimeUtility::fast_current_time();
  }
  virtual OB_INLINE int64_t get_expire_ts() const
  {
    return ttl_;
  }

  virtual OB_INLINE void set_expire_ts(int64_t expire_ts)
  {
    ttl_ = expire_ts;
  }

  virtual OB_INLINE void set_insert_ts(int64_t insert_ts)
  {
    insert_ts_ = insert_ts;
  }

  virtual OB_INLINE void set_is_exists(bool val)
  {
    is_exists_ = val;
  }

  virtual OB_INLINE bool has_meta_value() const
  {
    return has_meta_value_;
  }

  virtual OB_INLINE const ObString &get_meta_col_name() const
  {
    return meta_col_name_;
  }

  virtual OB_INLINE int64_t get_insert_ts() const
  {
    return insert_ts_;
  }

  virtual OB_INLINE bool is_exists() const
  {
    return is_exists_;
  }

  virtual int decode(const ObString &encoded_content);

  virtual int encode(ObIAllocator &allocator, ObString &encoded_content) const;
  virtual int build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const = 0;
  virtual int put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const;
  virtual int get_meta_from_entity(const ObITableEntity &meta_entity);
  virtual int decode_meta_value(const ObObj &meta_value_obj);
  virtual int encode_meta_value(ObIAllocator &allocator, ObObj &meta_value_obj) const;

protected:
  explicit ObRedisMeta() :
    ttl_(INT_MAX64),
    has_meta_value_(false),
    meta_col_name_(),
    insert_ts_(ObTimeUtility::fast_current_time()),
    is_exists_(true)
  {}

  // element count in meta value
  static const int64_t ELE_COUNT = 4;
  static const int64_t RESERVED_SIZE = 4;
  int64_t ttl_;
  bool has_meta_value_;
  ObString meta_col_name_;
  int64_t insert_ts_;
  bool is_exists_;
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
        ins_region_left_(INT64_MAX),
        ins_region_right_(INT64_MIN)
  {
    has_meta_value_ = true;
    meta_col_name_ = ObRedisUtil::VALUE_PROPERTY_NAME;
  }
  ~ObRedisListMeta()
  {}
  TO_STRING_KV(K_(count), K_(left_idx), K_(right_idx), K_(ttl));
  void reset() override
  {
    count_ = 0;
    left_idx_ = INIT_INDEX;
    right_idx_ = INIT_INDEX;
    ins_region_left_ = INT64_MAX;
    ins_region_right_ = INT64_MIN;
    ttl_ = INT_MAX64;
    insert_ts_ = ObTimeUtility::fast_current_time();
  }

  void reset_ins_region()
  {
    ins_region_left_ = INT64_MAX;
    ins_region_right_ = INT64_MIN;
  }
  virtual OB_INLINE bool is_expired() const override
  {
    return ttl_ < ObTimeUtility::fast_current_time();
  }

  virtual int decode(const ObString &encoded_content) override;
  virtual int encode(ObIAllocator &allocator, ObString &encoded_content) const override;
  int build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity)
      const override;
  OB_INLINE bool has_ins_region()
  {
    return ins_region_left_ < ins_region_right_;
  }
  OB_INLINE bool has_ins_region() const
  {
    return ins_region_left_ < ins_region_right_;
  }

public:
  static const int64_t INIT_INDEX = 0;
  static const int64_t INDEX_STEP = 1 << 10;
  static const int64_t SECOND_INDEX_STEP = 1 << 5;
  static const int64_t META_INDEX = INT64_MIN;
  static const int64_t ISDATA_SEQNUM_IN_ROWKEY = 2;
  static const int64_t INDEX_SEQNUM_IN_ROWKEY = 3;

  static const char META_SPLIT_FLAG = ObRedisUtil::INT_FLAG;
  // element count in list meta value
  static const int64_t ELE_COUNT = 5;

  int64_t count_;
  int64_t left_idx_;
  int64_t right_idx_;
  int64_t ins_region_left_;
  int64_t ins_region_right_;
};

class ObRedisHashMeta : public ObRedisMeta
{
  OB_UNIS_VERSION(1);

public:
  ObRedisHashMeta()
  {}
  ~ObRedisHashMeta()
  {}
  int build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const override;
  int put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const override;
};

class ObRedisSetMeta : public ObRedisMeta
{
  OB_UNIS_VERSION(1);

public:
  ObRedisSetMeta()
  {}
  ~ObRedisSetMeta()
  {}
  int build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const override;
};

class ObRedisZSetMeta : public ObRedisSetMeta
{
  OB_UNIS_VERSION(1);

public:
  ObRedisZSetMeta()
  {}
  ~ObRedisZSetMeta()
  {}
  int put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const override;
};

}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_REDIS_OB_REDIS_META_H */
