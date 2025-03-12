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

#ifndef OCEANBASE_TABLE_OB_REDIS_TTL_H_
#define OCEANBASE_TABLE_OB_REDIS_TTL_H_

#include "observer/table/redis/ob_redis_meta.h"

namespace oceanbase
{
namespace table
{
class ObRedisTTLCtx
{
public:
  explicit ObRedisTTLCtx() : meta_(nullptr), model_(ObRedisModel::INVALID), return_meta_(false)
  {}
  virtual ~ObRedisTTLCtx()
  {
    reset();
  }
  void reset();
  OB_INLINE const ObRedisMeta *get_meta() const
  {
    return meta_;
  }
  OB_INLINE bool is_return_meta() const
  {
    return return_meta_;
  }
  OB_INLINE void set_meta(const ObRedisMeta *meta)
  {
    meta_ = meta;
  }
  OB_INLINE void set_return_meta(bool flag)
  {
    return_meta_ = flag;
  }
  OB_INLINE ObRedisModel get_model() const
  {
    return model_;
  }
  OB_INLINE void set_model(ObRedisModel model)
  {
    model_ = model;
  }

private:
  const ObRedisMeta *meta_;
  ObRedisModel model_;
  bool return_meta_;  // result include redis meta (whether meta is expired or not)
};

}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_OB_REDIS_TTL_H_ */
