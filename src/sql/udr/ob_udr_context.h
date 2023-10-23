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


#ifndef OB_SQL_UDR_OB_UDR_CONTEXT_H_
#define OB_SQL_UDR_OB_UDR_CONTEXT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase
{
namespace sql
{

struct ObUDRContext
{
public:
  ObUDRContext()
  : is_ps_mode_(false),
    tenant_id_(OB_INVALID_ID),
    pattern_digest_(0),
    coll_type_(common::CS_TYPE_INVALID),
    db_name_(),
    normalized_pattern_(),
    raw_param_list_() {}
  virtual ~ObUDRContext() {}

  TO_STRING_KV(K_(tenant_id),
               K_(pattern_digest),
               K_(coll_type),
               K_(db_name),
               K_(normalized_pattern));

  bool is_ps_mode_;
  uint64_t tenant_id_;
  uint64_t pattern_digest_;
  ObCollationType coll_type_;
  common::ObString db_name_;
  common::ObString normalized_pattern_;
  common::ObSEArray<ObPCParam*, 16> raw_param_list_;
};

} // namespace sql end
} // namespace oceanbase end

#endif