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

#ifndef OCEANBASE_ENGINE_OB_MOCK_VEC2_H_
#define OCEANBASE_ENGINE_OB_MOCK_VEC2_H_

#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace sql
{

struct MockRowMeta {
  OB_UNIS_VERSION_V(1);
  static const int64_t MAX_LOCAL_BUF_LEN = 128;
public:
  MockRowMeta() : col_cnt_(0), extra_size_(0),
              fixed_cnt_(0),
              nulls_off_(0), var_offsets_off_(0), extra_off_(0),
              fix_data_off_(0), var_data_off_(0)
  {
  }
  TO_STRING_KV(K_(col_cnt), K_(extra_size), K_(fixed_cnt), K_(nulls_off), K_(var_offsets_off),
               K_(extra_off), K_(fix_data_off), K_(var_data_off));
private:
  inline bool fixed_expr_reordered() const { return fixed_cnt_ > 0; }

private:
  char buf_[MAX_LOCAL_BUF_LEN];
public:
  int32_t col_cnt_;
  int32_t extra_size_;
  int32_t fixed_cnt_;
  int32_t nulls_off_;
  int32_t var_offsets_off_;
  int32_t extra_off_;
  int32_t fix_data_off_;
  int32_t var_data_off_;
};

class MockObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  MockObTempBlockStore()
  : tenant_id_(0), label_(), ctx_id_(0), mem_limit_(0)
  {
    label_[0] = '\0';
  }
  TO_STRING_KV(K_(tenant_id), K_(label), K_(ctx_id), K_(mem_limit));
private:
  int64_t get_block_cnt() const { return 0; }

  uint64_t tenant_id_;
  char label_[lib::AOBJECT_LABEL_SIZE + 1];
  int64_t ctx_id_;
  int64_t mem_limit_;

};

class MockObTempRowStore : public MockObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  MockObTempRowStore()
   : MockObTempBlockStore(), col_cnt_(0),
     row_meta_(), max_batch_size_(0) {}
  INHERIT_TO_STRING_KV("MockObTempBlockStore", MockObTempBlockStore,
                       K_(col_cnt), K_(row_meta), K_(max_batch_size));
private:
  int64_t col_cnt_;
  MockRowMeta row_meta_;
  int64_t max_batch_size_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_MOCK_VEC2_H_