/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_server_object_pool.h"

using namespace oceanbase::common;

ObServerObjectPoolRegistry::PoolPair ObServerObjectPoolRegistry::pool_list_[ObServerObjectPoolRegistry::MAX_POOL_NUM];
int64_t ObServerObjectPoolRegistry::pool_num_ = 0;
int64_t ObServerObjectPoolRegistry::ArenaIterator::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pool_idx), K_(arena_idx));
  J_OBJ_END();
  return pos;
}
int64_t ObPoolArenaHead::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K(lock), K(borrow_cnt), K(return_cnt), K(miss_cnt), K(miss_return_cnt), K(last_borrow_ts),K(last_return_ts), K(last_miss_ts), K(last_miss_return_ts), KP(next));
  J_OBJ_END();
  return pos;
}
