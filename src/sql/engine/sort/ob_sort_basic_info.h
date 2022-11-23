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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_BASIC_INFO_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_BASIC_INFO_H_

#include "lib/ob_define.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace sql
{
struct ObSortFieldCollation
{
  OB_UNIS_VERSION(1);
public:
  ObSortFieldCollation(uint32_t field_idx,
      common::ObCollationType cs_type,
      bool is_ascending,
      common::ObCmpNullPos null_pos)
    : field_idx_(field_idx),
    cs_type_(cs_type),
    is_ascending_(is_ascending),
    null_pos_(null_pos)
  {}
  ObSortFieldCollation()
    : field_idx_(UINT32_MAX),
    cs_type_(common::CS_TYPE_INVALID),
    is_ascending_(true),
    null_pos_(common::NULL_LAST)
  {}
  TO_STRING_KV(K_(field_idx), K_(cs_type), K_(is_ascending), K_(null_pos));
  uint32_t field_idx_;
  common::ObCollationType cs_type_;
  bool is_ascending_;
  common::ObCmpNullPos null_pos_;
};

typedef common::ObCmpFunc ObSortCmpFunc;
typedef common::ObFixedArray<ObSortFieldCollation, common::ObIAllocator> ObSortCollations;
typedef common::ObFixedArray<ObSortCmpFunc, common::ObIAllocator> ObSortFuncs;

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_BASIC_INFO_H_ */
