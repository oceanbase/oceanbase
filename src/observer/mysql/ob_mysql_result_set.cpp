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

#define USING_LOG_PREFIX SERVER
#include "observer/mysql/ob_mysql_result_set.h"

#include "observer/mysql/obsm_utils.h"
#include "lib/container/ob_array.h"
#include "rpc/obmysql/ob_mysql_field.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::obmysql;

int ObMySQLResultSet::to_mysql_field(const ObField& field, ObMySQLField& mfield)
{
  int ret = OB_SUCCESS;
  mfield.dname_ = field.dname_;
  mfield.tname_ = field.tname_;
  mfield.org_tname_ = field.org_tname_;
  mfield.cname_ = field.cname_;

  // If there is no parameterized template, there must be an alias name or a column itself
  // as: select 1 as a from dual, select col_name form t
  // use org_cname_
  // otherwise select item is a expr,column name is auto generated,org_cname_ is same with cname_
  if (field.is_paramed_select_item_) {
    if (OB_ISNULL(field.paramed_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (0 == field.paramed_ctx_->paramed_cname_.length()) {
      mfield.org_cname_ = field.org_cname_;
    } else {
      mfield.org_cname_ = field.cname_;
    }
  } else {
    mfield.org_cname_ = field.org_cname_;
  }
  if (OB_SUCC(ret)) {
    mfield.accuracy_ = field.accuracy_;
    // mfield.type_ = oceanbase::obmysql::MYSQL_TYPE_LONG;
    // mfield.default_value_ = field.default_value_;

    // To distinguish between binary and nonbinary data for string data types,
    // check whether the charsetnr value is 63. Also, flag must be set to binary accordingly
    mfield.charsetnr_ = field.charsetnr_;
    mfield.flags_ = field.flags_;
    mfield.length_ = field.length_;

    // Varchar,need check charset:
    if (ObCharset::is_valid_collation(static_cast<ObCollationType>(field.charsetnr_)) &&
        ObCharset::is_bin_sort(static_cast<ObCollationType>(field.charsetnr_))) {
      mfield.flags_ |= OB_MYSQL_BINARY_FLAG;
    }
    ObScale decimals = mfield.accuracy_.get_scale();
    // TIMESTAMP, UNSIGNED are directly mapped through map
    if (0 == field.type_name_.case_compare("SYS_REFCURSOR")) {
      mfield.type_ = oceanbase::obmysql::MYSQL_TYPE_CURSOR;
    } else {
      ret = ObSMUtils::get_mysql_type(field.type_.get_type(), mfield.type_, mfield.flags_, decimals);
    }

    mfield.type_owner_ = field.type_owner_;
    mfield.type_name_ = field.type_name_;
    mfield.accuracy_.set_scale(decimals);
    mfield.inout_mode_ = field.inout_mode_;
    if (field.is_hidden_rowid_) {
      mfield.inout_mode_ |= 0x04;
    }
  }
  LOG_TRACE("to mysql field", K(mfield), K(field));
  return ret;
}

int ObMySQLResultSet::next_field(ObMySQLField& obmf)
{
  int ret = OB_SUCCESS;
  int64_t field_cnt = 0;
  const ColumnsFieldIArray* fields = get_field_columns();
  if (OB_ISNULL(fields)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    field_cnt = get_field_cnt();
    if (field_index_ >= field_cnt) {
      ret = OB_ITER_END;
    } else {
      const ObField& field = fields->at(field_index_++);
      if (OB_FAIL(to_mysql_field(field, obmf))) {
        // do nothing
      } else {
        replace_lob_type(get_session(), field, obmf);
      }
    }
  }
  set_errcode(ret);
  return ret;
}

int ObMySQLResultSet::next_param(ObMySQLField& obmf)
{
  int ret = OB_SUCCESS;
  const ParamsFieldIArray* params = get_param_fields();
  if (OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t param_cnt = params->count();
    if (param_index_ >= param_cnt) {
      ret = OB_ITER_END;
    }
    if (OB_SUCC(ret)) {
      ObField field;
      ret = params->at(param_index_++, field);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(to_mysql_field(field, obmf))) {
          // do nothing
        } else {
          replace_lob_type(get_session(), field, obmf);
        }
      }
    }
  }
  set_errcode(ret);
  return ret;
}
