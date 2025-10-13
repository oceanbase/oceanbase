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

#ifndef OCEANBASE_SHARE_OB_QUERY_REQUEST_H_
#define OCEANBASE_SHARE_OB_QUERY_REQUEST_H_

#include "ob_request_base.h"

namespace oceanbase
{
namespace share
{

class ObQueryReqFromJson : public ObReqFromJson
{
public :
  ObQueryReqFromJson()
    : ObReqFromJson(), select_items_(), group_items_(),
      having_items_(), score_items_(), output_all_columns_(true),
      score_alias_() {}
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> select_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> group_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> having_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_items_;
  int translate(char *buf, int64_t buf_len, int64_t &res_len);
  int add_score_item(ObIAllocator &alloc, ObReqExpr *score_item);
  inline bool is_score_item_exist() { return !score_items_.empty(); }
  bool output_all_columns_;
  common::ObString score_alias_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> match_idxs_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_QUERY_REQUEST_H_