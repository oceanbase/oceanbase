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
#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_PARAM_CONSTRAINT_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_PARAM_CONSTRAINT_

#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_object.h"
#include "sql/ob_sql_define.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
namespace sql
{
class ObPCParamConstraint
{
public:
  explicit ObPCParamConstraint(uint64_t idx) : param_idx_(idx) {}
  virtual int deep_copy(ObIAllocator &allocator, ObPCParamConstraint *&to) = 0;
  virtual int build(const common::ParamStore &params, void *ctx) = 0; // build specific constraint based on params
  virtual int match(const common::ParamStore &params, void *ctx, bool &is_match) = 0;
protected:
  uint64_t param_idx_;
public:
  TO_STRING_KV(K_(param_idx));
};

class ObPCUnixTimestampParamConstraint : public ObPCParamConstraint
{
public:
  explicit ObPCUnixTimestampParamConstraint(uint64_t idx):
    ObPCParamConstraint(idx), type_(ObMaxType), precision_(-1), scale_(-1) {}
  ObPCUnixTimestampParamConstraint(uint64_t idx, uint8_t type, int16_t precision, int16 scale):
    ObPCParamConstraint(idx), type_(type), precision_(precision), scale_(scale) {}
  virtual ~ObPCUnixTimestampParamConstraint() {};
  virtual int deep_copy(ObIAllocator &allocator, ObPCParamConstraint *&to) override final;
  int build(const common::ParamStore &params, void *ctx) override final;
  int match(const common::ParamStore &params, void *ctx, bool &is_match) override final;
  int deduce_result_type(const common::ParamStore &params, uint8_t &type, int16_t &precision, int16_t &scale);
private:
  uint8_t type_;
  int16_t precision_;
  int16_t scale_;
public:
  TO_STRING_KV(K_(param_idx), K_(type), K_(precision), K_(scale));
};
}
}
#endif //OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_PARAM_CONSTRAINT_