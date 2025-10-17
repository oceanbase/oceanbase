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

#ifndef OCEANBASE_SHARE_VECTOR_OB_UNIFORM_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_UNIFORM_BASE_H_

#include "share/vector/ob_vector_base.h"

namespace oceanbase
{
namespace sql
{
  struct ObEvalInfo;
}

namespace common
{

class ObUniformBase : public ObVectorBase
{
public:
  ObUniformBase(ObDatum *datums, sql::ObEvalInfo *eval_info)
    : ObVectorBase(), datums_(datums), eval_info_(eval_info)
  {}

  DECLARE_TO_STRING;

  inline ObDatum *get_datums() { return datums_; }
  inline const ObDatum *get_datums() const { return datums_; }
  OB_INLINE void set_datums(ObDatum *datums) { datums_ = datums; }
  inline sql::ObEvalInfo *get_eval_info() { return eval_info_; }
  inline const sql::ObEvalInfo *get_eval_info() const { return eval_info_; }
  OB_INLINE void append_rows_multiple_times(ObDatum *datums,
                              const int64_t src_start_idx, const int64_t src_end_idx,
                              const int64_t times, const int64_t dst_start_idx)
  {
    const int64_t interval = src_end_idx - src_start_idx;
    const ObDatum *src = datums + src_start_idx;
    ObDatum *dst = datums_ + dst_start_idx;
    if (interval > MEMCPY_THRESHOLD) {
      for (int64_t i = 0; i < times; ++i) {
        MEMCPY(dst + i * interval, src, sizeof(ObDatum) * interval);
      }
    } else {
      for (int64_t i = 0; i < interval; ++i) {
        for (int64_t j = 0; j < times; ++j) {
          dst[i + j * interval] = src[i];
        }
      }
    }
  };
  virtual int to_rows(const sql::RowMeta &row_meta,
                       sql::ObCompactRow **stored_rows,
                       const uint16_t selector[],
                       const int64_t size,
                       const int64_t col_idx) const override final;

  virtual int to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                       const int64_t size, const int64_t col_idx) const override final;

protected:
  ObDatum *datums_;
  sql::ObEvalInfo *eval_info_; // just used for maintain has_null flag in uniform vector
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_UNIFORM_Base_H_
