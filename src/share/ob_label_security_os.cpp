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

#ifndef OB_BUILD_LABEL_SECURITY
#define USING_LOG_PREFIX SHARE

#include "share/ob_label_security.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace share {

int ObLabelSeResolver::resolve_label_text(const ObString &label_text, ObLabelSeDecomposedLabel &label_comps)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeResolver::construct_label_text(const ObLabelSeDecomposedLabel &label_comps,
                                            ObIAllocator *allocator,
                                            ObString &label_text)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeResolver::resolve_policy_name(uint64_t tenant_id,
                                           const ObString &policy_name,
                                           ObSchemaGetterGuard &schema_guard,
                                           uint64_t &policy_id)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeResolver::serialize_session_labels(const common::ObIArray<ObLabelSeSessionLabel> &labels,
                                                ObIAllocator &allocator,
                                                ObString &labels_str)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLabelSeResolver::deserialize_session_labels(const ObString &labels_str,
                                                  common::ObIArray<ObLabelSeSessionLabel> &labels)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLabelSeUtil::validate_user_auth(
    uint64_t tenant_id,
    uint64_t policy_id,
    uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    const ObLabelSeLabelCompNums &label_comp_nums,
    bool check_lower_bound)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeUtil::convert_label_comps_name_to_num(
    uint64_t tenant_id,
    uint64_t policy_id,
    schema::ObSchemaGetterGuard &schema_guard,
    const ObLabelSeDecomposedLabel &label_comps,
    ObLabelSeLabelCompNums &label_comp_nums)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeUtil::load_default_session_label(
    uint64_t tenant_id,
    uint64_t policy_id,
    uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    ObLabelSeSessionLabel &session_label)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObLabelSeUtil::check_policy_column(uint64_t tenant_id,
                                       const ObString &schema_name,
                                       const ObString &table_name,
                                       const ObString &column_name,
                                       ObSchemaGetterGuard &schema_guard,
                                       bool &is_policy_column_exist,
                                       bool &is_policy_already_applied_to_column)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLabelSeSessionLabel,
                    policy_id_,
                    read_label_tag_,
                    write_label_tag_);
OB_SERIALIZE_MEMBER(ObLabelSeLabelTag,
                    label_tag_);


}
}
#endif