/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_MVIEW_INFO_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_MVIEW_INFO_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace libobcdc
{
struct ObLogMViewInfo
{
  enum MappingState { MAPPED, COMPAT_MISSING };
  ObLogMViewInfo()
      : container_table_id_(common::OB_INVALID_ID), mview_id_(common::OB_INVALID_ID),
        mview_name_(), mapping_state_(COMPAT_MISSING) {}

  void reset()
  {
    container_table_id_ = common::OB_INVALID_ID;
    mview_id_ = common::OB_INVALID_ID;
    mview_name_.reset();
    mapping_state_ = COMPAT_MISSING;
  }

  bool is_mapped() const { return MAPPED == mapping_state_; }

  uint64_t container_table_id_;
  uint64_t mview_id_;
  common::ObString mview_name_;
  MappingState mapping_state_;
  TO_STRING_KV(K_(container_table_id), K_(mview_id), K_(mview_name), K_(mapping_state));
};

struct ObLogMViewContainerInfo
{
  ObLogMViewContainerInfo()
      : container_table_id_(common::OB_INVALID_ID), association_table_id_(common::OB_INVALID_ID) {}

  bool has_association() const
  { return common::OB_INVALID_ID != association_table_id_ && 0 != association_table_id_; }

  uint64_t container_table_id_;
  uint64_t association_table_id_;
  TO_STRING_KV(K_(container_table_id), K_(association_table_id));
};

struct ObLogMViewTICInfo
{
  ObLogMViewTICInfo()
      : table_id_(common::OB_INVALID_ID), container_table_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID), tenant_name_(), database_name_(), table_name_() {}

  uint64_t table_id_;
  uint64_t container_table_id_;
  uint64_t database_id_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString table_name_;
  TO_STRING_KV(K_(table_id), K_(container_table_id), K_(database_id),
      K_(tenant_name), K_(database_name), K_(table_name));
};
}
}
#endif
