/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_LOB_OB_LOB_TASK_INFO_H_
#define OCEANBASE_SHARE_LOB_OB_LOB_TASK_INFO_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace common {
class ObJsonNode;
}

namespace share {

struct ObLobTaskInfo {
  ObLobTaskInfo()
  : exception_allocator_(common::ObMemAttr(MTL_ID(), "LobTaskInfo")),
    lob_not_found_cnt_(0),
    lob_length_mismatch_cnt_(0),
    lob_orphan_cnt_(0),
    not_found_tablets_json_(nullptr),
    mismatch_len_tablets_json_(nullptr),
    orphan_tablets_json_(nullptr),
    not_found_removed_tablets_json_(nullptr),
    mismatch_len_removed_tablets_json_(nullptr),
    orphan_removed_tablets_json_(nullptr) {}

  ~ObLobTaskInfo() {
    OB_DELETEx(ObJsonNode, &exception_allocator_, not_found_tablets_json_);
    OB_DELETEx(ObJsonNode, &exception_allocator_, mismatch_len_tablets_json_);
    OB_DELETEx(ObJsonNode, &exception_allocator_, orphan_tablets_json_);
    OB_DELETEx(ObJsonNode, &exception_allocator_, not_found_removed_tablets_json_);
    OB_DELETEx(ObJsonNode, &exception_allocator_, mismatch_len_removed_tablets_json_);
    OB_DELETEx(ObJsonNode, &exception_allocator_, orphan_removed_tablets_json_);
  }

  common::ObArenaAllocator exception_allocator_;
  int64_t                  lob_not_found_cnt_;
  int64_t                  lob_length_mismatch_cnt_;
  int64_t                  lob_orphan_cnt_;
  common::ObJsonNode*      not_found_tablets_json_; // for LOB check/repair lob not found tablets
  common::ObJsonNode*      mismatch_len_tablets_json_; // for LOB check/repair mismatch len tablets
  common::ObJsonNode*      orphan_tablets_json_; // for LOB check/repair orphan tablets
  common::ObJsonNode*      not_found_removed_tablets_json_; // for LOB check/repair lob not found tablets
  common::ObJsonNode*      mismatch_len_removed_tablets_json_; // for LOB check/repair mismatch len tablets
  common::ObJsonNode*      orphan_removed_tablets_json_; // for LOB check/repair orphan tablets
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_LOB_OB_LOB_TASK_INFO_H_
