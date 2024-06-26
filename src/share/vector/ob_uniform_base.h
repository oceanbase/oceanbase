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
  virtual void to_rows(const sql::RowMeta &row_meta,
                       sql::ObCompactRow **stored_rows,
                       const uint16_t selector[],
                       const int64_t size,
                       const int64_t col_idx) const override final;

  virtual void to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                       const int64_t size, const int64_t col_idx) const override final;

protected:
  ObDatum *datums_;
  sql::ObEvalInfo *eval_info_; // just used for maintain has_null flag in uniform vector
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_UNIFORM_Base_H_
