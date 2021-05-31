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

#ifndef OCEANBASE_SQL_SPM_OB_SPM_UTIL_
#define OCEANBASE_SQL_SPM_OB_SPM_UTIL_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "common/object/ob_object.h"
#include "sql/ob_sql_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace sql {

struct ObParamInfo {
  static const int64_t MAX_STR_DES_LEN = 17;
  common::ParamFlag flag_;
  common::ObScale scale_;
  common::ObObjType type_;
  common::ObObjType ext_real_type_;
  bool is_oracle_empty_string_;

  ObParamInfo()
      : scale_(0), type_(common::ObNullType), ext_real_type_(common::ObNullType), is_oracle_empty_string_(false)
  {}

  void reset();

  TO_STRING_KV(K_(flag), K_(scale), K_(type), K_(ext_real_type), K_(is_oracle_empty_string));

  OB_UNIS_VERSION_V(1);
};
}  // namespace sql
}  // namespace oceanbase

#endif
