/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "pl/ob_pl.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSVectorMySql
{
public:
  ObDBMSVectorMySql() {}
  virtual ~ObDBMSVectorMySql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(refresh_index);
  DECLARE_FUNC(rebuild_index);
  DECLARE_FUNC(refresh_index_inner);
  DECLARE_FUNC(rebuild_index_inner);
  DECLARE_FUNC(index_vector_memory_advisor);
  DECLARE_FUNC(index_vector_memory_estimate);

#undef DECLARE_FUNC

  static int parse_idx_param(const ObString &idx_type_str,
                             const ObString &idx_param_str,
                             uint32_t dim_count,
                             ObVectorIndexParam &index_param);

private:
  static int get_estimate_memory_str(ObVectorIndexParam index_param,
                                     uint64_t num_vectors,
                                     uint64_t tablet_max_num_vectors,
                                     uint64_t tablet_count,
                                     ObStringBuffer &res_buf);
  static int print_mem_size(uint64_t mem_size, ObStringBuffer &res_buf);
};

} // namespace pl
} // namespace oceanbase