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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/ob_mock_vec2.h"

namespace oceanbase
{
namespace sql
{

OB_DEF_SERIALIZE(MockRowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  return ret;
}


OB_DEF_DESERIALIZE(MockRowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  if (fixed_expr_reordered()) {
    int32_t projector = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
      OB_UNIS_DECODE(projector);
    }
    int32_t fixed_offsets = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i <= fixed_cnt_; ++i) {
      OB_UNIS_DECODE(fixed_offsets);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(MockRowMeta)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  return len;
}


OB_DEF_SERIALIZE(MockObTempBlockStore)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label_);
  const int64_t count = 0;
  OB_UNIS_ENCODE(count);
  return ret;
}


OB_DEF_DESERIALIZE(MockObTempBlockStore)
{
  int ret = OB_SUCCESS;
  char label[lib::AOBJECT_LABEL_SIZE + 1];
  int64_t count = 0;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label);
  if (OB_SUCC(ret)) {
    int64_t raw_size = 0;
    int64_t blk_cnt = 0;
    OB_UNIS_DECODE(blk_cnt);
    for (int64_t i = 0; i < blk_cnt && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(raw_size);
      pos += raw_size;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(MockObTempBlockStore)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label_);
  const int64_t count = 0;
  OB_UNIS_ADD_LEN(count);
  return len;
}


OB_DEF_SERIALIZE(MockObTempRowStore)
{
  int ret = MockObTempBlockStore::serialize(buf, buf_len, pos);
  LST_DO_CODE(OB_UNIS_ENCODE,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  return ret;
}

OB_DEF_DESERIALIZE(MockObTempRowStore)
{
  int ret = MockObTempBlockStore::deserialize(buf, data_len, pos);
  LST_DO_CODE(OB_UNIS_DECODE,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(MockObTempRowStore)
{
  int64_t len = MockObTempBlockStore::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  return len;
}

} // end namespace sql
} // end namespace oceanbase
