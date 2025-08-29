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

#define USING_LOG_PREFIX SERVER

#include "share/table/ob_table_ddl_struct.h"

using namespace oceanbase::table;

ObCreateHTableDDLParam::ObCreateHTableDDLParam()
  : table_group_arg_(),
    cf_arg_list_()
  {}

ObDropHTableDDLParam::ObDropHTableDDLParam()
  : table_group_arg_()
{}

OB_SERIALIZE_MEMBER(ObCreateHTableDDLParam,
                    table_group_arg_,
                    cf_arg_list_);

bool ObCreateHTableDDLParam::is_valid() const
{
  bool is_valid = table_group_arg_.is_valid();

  for (int64_t i = 0; is_valid && i < cf_arg_list_.count(); i++) {
    is_valid &= cf_arg_list_.at(i).is_valid();
  }

  return is_valid;
}

bool ObCreateHTableDDLParam::is_allow_when_upgrade() const
{
  bool is_allow = table_group_arg_.is_allow_when_upgrade();

  for (int64_t i = 0; is_allow && i < cf_arg_list_.count(); i++) {
    is_allow &= cf_arg_list_.at(i).is_allow_when_upgrade();
  }

  return is_allow;
}

int ObCreateHTableDDLParam::assign(const ObHTableDDLParam &other)
{
  int ret = OB_SUCCESS;
  const ObCreateHTableDDLParam &param = static_cast<const ObCreateHTableDDLParam &>(other);

  if (OB_FAIL(table_group_arg_.assign(param.table_group_arg_))) {
    LOG_WARN("fail to assign table_group_arg_", K(ret), K(param));
  } else if (OB_FAIL(cf_arg_list_.assign(param.cf_arg_list_))) {
    LOG_WARN("fail to assign cf_arg_list_", K(ret), K(param));
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObSetKvAttributeParam,
                    session_id_,
                    database_name_,
                    table_group_name_,
                    is_disable_);

int ObSetKvAttributeParam::assign(const ObHTableDDLParam &other)
{
  int ret = OB_SUCCESS;
  const ObSetKvAttributeParam &param = static_cast<const ObSetKvAttributeParam &>(other);
  session_id_ = param.session_id_;
  database_name_ = param.database_name_;
  table_group_name_ = param.table_group_name_;
  is_disable_ = param.is_disable_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDropHTableDDLParam, table_group_arg_);

bool ObDropHTableDDLParam::is_valid() const
{
  return table_group_arg_.is_valid();
}

bool ObDropHTableDDLParam::is_allow_when_upgrade() const
{
  return table_group_arg_.is_allow_when_upgrade();
}

int ObDropHTableDDLParam::assign(const ObHTableDDLParam &other)
{
  int ret = OB_SUCCESS;
  const ObDropHTableDDLParam &param = static_cast<const ObDropHTableDDLParam &>(other);
  if (OB_FAIL(table_group_arg_.assign(param.table_group_arg_))) {
    LOG_WARN("fail to assign table_group_arg_", K(ret), K(param));
  }
  return ret;
}
