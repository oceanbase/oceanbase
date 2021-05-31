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

#ifndef OCEANBASE_SHARE_OB_BUILD_INDEX_STRUCT_H_
#define OCEANBASE_SHARE_OB_BUILD_INDEX_STRUCT_H_

#include "share/ob_define.h"

namespace oceanbase {
namespace share {

struct ObBuildIndexAppendLocalDataParam {
  ObBuildIndexAppendLocalDataParam()
      : execution_id_(common::OB_INVALID_ID),
        task_id_(common::OB_INVALID_ID),
        index_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        task_cnt_(0)
  {}
  virtual ~ObBuildIndexAppendLocalDataParam()
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != execution_id_ && common::OB_INVALID_ID != task_id_ &&
           common::OB_INVALID_ID != index_id_ && common::OB_INVALID_VERSION != schema_version_ && 0 != task_cnt_ &&
           task_id_ < task_cnt_;
  }
  TO_STRING_KV(K_(execution_id), K_(task_id), K_(index_id), K_(schema_version), K_(task_cnt));
  uint64_t execution_id_;
  uint64_t task_id_;
  uint64_t index_id_;
  int64_t schema_version_;
  uint64_t task_cnt_;
};

struct ObBuildIndexAppendSSTableParam {
  ObBuildIndexAppendSSTableParam()
      : index_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        execution_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObBuildIndexAppendSSTableParam()
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != index_id_ && common::OB_INVALID_VERSION != schema_version_ &&
           common::OB_INVALID_ID != execution_id_;
  }
  TO_STRING_KV(K_(index_id), K_(schema_version), K_(execution_id));
  uint64_t index_id_;
  int64_t schema_version_;
  uint64_t execution_id_;
};

}  // end namespace share
}  // end namespace oceanbase
#endif  // OCEANBASE_SHARE_OB_BUILD_INDEX_STRUCT_H_
