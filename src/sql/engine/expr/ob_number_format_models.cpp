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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_number_format_models.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/system_variable/ob_nls_system_variable.h"

namespace oceanbase
{
namespace sql
{
const int64_t MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS= 256;

const ObNFMKeyWord ObNFMElem::NFM_KEYWORDS[MAX_TYPE_NUMBER] =
{
  {",", NFM_COMMA, GROUPING_GROUP, 1},
  {".", NFM_PERIOD, GROUPING_GROUP, 1},
  {"$", NFM_DOLLAR, DOLLAR_GROUP, 1},
  {"0", NFM_ZERO, NUMBER_GROUP, 1},
  {"9", NFM_NINE, NUMBER_GROUP, 1},
  {"B", NFM_B, BLANK_GROUP, 0},
  {"C", NFM_C, CURRENCY_GROUP, 7},
  {"D", NFM_D, ISO_GROUPING_GROUP, 1},
  {"EEEE", NFM_EEEE, EEEE_GROUP, 5},
  {"G", NFM_G, ISO_GROUPING_GROUP, 1},
  {"L", NFM_L, CURRENCY_GROUP, 10},
  {"MI", NFM_MI, SIGN_GROUP, 1},
  {"PR", NFM_PR, SIGN_GROUP, 2},
  {"RN", NFM_RN, ROMAN_GROUP, 15},
  {"S", NFM_S, SIGN_GROUP, 1},
  {"TME", NFM_TME, TM_GROUP, 64},
  {"TM9", NFM_TM9, TM_GROUP, 64},
  {"TM", NFM_TM, TM_GROUP, 64},
  {"U", NFM_U, CURRENCY_GROUP, 10},
  {"V", NFM_V, MULTI_GROUP, 0},
  {"X", NFM_X, HEX_GROUP, 1},
  {"FM", NFM_FM, FILLMODE_GROUP, 0}
};

ObString ObNFMElem::get_elem_type_name() const
{
  ObString result;
  if (OB_ISNULL(keyword_) || is_valid_type(keyword_->elem_type_) || offset_ < 0) {
    result = ObString("INVALID ELEMENT TYPE");
  } else {
    result = ObNFMElem::NFM_KEYWORDS[keyword_->elem_type_].to_obstring();
  }
  return result;
}

int ObNFMDescPrepare::check_conflict_group(const ObNFMElem *elem_item,
                                           const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(elem_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid elem item", K(ret));
  } else {
    const ObNFMElem::ElementGroup elem_group = elem_item->keyword_->elem_group_;
    switch(elem_group) {
    case ObNFMElem::NUMBER_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::GROUPING_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_iso_grouping_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::ISO_GROUPING_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::DOLLAR_GROUP:
      if (ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::CURRENCY_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::EEEE_GROUP:
      if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::ROMAN_GROUP:
      if (ObNFMElem::has_type(~NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::MULTI_GROUP:
      if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::HEX_GROUP:
      if (ObNFMElem::has_type((~NFM_HEX_FLAG) & (~NFM_ZERO_FLAG) & (~NFM_NINE_FLAG)
                              & (~NFM_FILLMODE_FLAG) & (~NFM_COMMA_FLAG)
                              & (~NFM_G_FLAG), fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::SIGN_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_sign_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::BLANK_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::TM_GROUP:
      if (ObNFMElem::has_type(~NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::FILLMODE_GROUP:
      // do nothing
      break;
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown group type", K(ret), K(elem_group));
    }
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_comma_is_valid(const ObNFMElem *elem_item,
                                                const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear in the integral part of a number
  // can't appear before the number
  // can't appear at the same time as element 'EEEE'
  // can't appear after the element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (-1 == fmt_desc.digital_start_
            || elem_item->offset_ >= fmt_desc.decimal_pos_
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem comma is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_period_is_valid(const ObNFMElem *elem_item,
                                                 const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear at the same time as element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem period is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_dollar_is_valid(const ObNFMElem *elem_item,
                                                 const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem dollar is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_zero_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after element 'EEEE' or element 'X'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem zero is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_nine_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after element 'EEEE' or element 'X'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem nine is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_b_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_BLANK_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem blank is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_c_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem currency is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_d_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem d is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_eeee_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as thousands separator
  // when element 'V' appears in front of all numbers, element 'EEEE' can't appear behind
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            && fmt_desc.multi_ == fmt_desc.pre_num_count_)
            || ObNFMElem::has_type(NFM_COMMA_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem eeee is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_g_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear in the integral part of a number
  // can't appear before the number
  // can't appear after the element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (-1 == fmt_desc.digital_start_
            || elem_item->offset_ >= fmt_desc.decimal_pos_
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem g is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_l_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_C_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem l is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_mi_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the end of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_MI_FLAG, fmt_desc.elem_flag_)
            || !elem_item->is_last_elem_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem mi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_pr_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the end of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc.elem_flag_)
            || !elem_item->is_last_elem_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem pr is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_rn_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem rn is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_s_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the front or the end of the fmt string
  // but 'FM' + 'S' is an exception
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc.elem_flag_)
            || (0 !=elem_item->offset_
            && (!elem_item->is_last_elem_
            && elem_item->prefix_type_ != ObNFMElem::NFM_FM))) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem s is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_tm_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem tm is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_u_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_U_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem u is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_v_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as element '.' or element 'D'
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem multi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_x_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::NFM_NINE == elem_item->prefix_type_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem multi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_fm_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the front of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WARN("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)
            || elem_item->offset_ != 0) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("check elem hex is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::fmt_desc_prepare(const common::ObSEArray<ObNFMElem*, 64> &fmt_elem_list,
                                       OBNFMDesc &fmt_desc, bool need_check/*true*/)
{
  int ret = OB_SUCCESS;

  for (int32_t i = 0; OB_SUCC(ret) && i < fmt_elem_list.count(); ++i) {
    const ObNFMElem *elem_item = fmt_elem_list.at(i);
    if (OB_ISNULL(elem_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid elem item", K(ret));
    } else {
      switch (elem_item->keyword_->elem_type_) {
      case ObNFMElem::NFM_COMMA:
        if (need_check && OB_FAIL(check_elem_comma_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem comma is valid", K(ret));
        } else {
          fmt_desc.last_separator_ = elem_item->offset_;
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_COMMA_FLAG;
        }
        break;
      case ObNFMElem::NFM_PERIOD:
        if (need_check && OB_FAIL(check_elem_period_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem period is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_PERIOD_FLAG;
          fmt_desc.decimal_pos_ = elem_item->offset_;
        }
        break;
      case ObNFMElem::NFM_DOLLAR:
        if (need_check && OB_FAIL(check_elem_dollar_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem dollar is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_DOLLAR_FLAG;
        }
        break;
      case ObNFMElem::NFM_ZERO:
        if (need_check && OB_FAIL(check_elem_zero_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem zero is valid", K(ret));
        } else {
          if (-1 == fmt_desc.digital_start_) {
            fmt_desc.digital_start_ = elem_item->offset_;
            fmt_desc.zero_start_ = elem_item->offset_;
          } else if (-1 == fmt_desc.zero_start_) {
            fmt_desc.zero_start_ = elem_item->offset_;
          }
          fmt_desc.zero_end_ = elem_item->offset_;
          fmt_desc.digital_end_ = elem_item->offset_;
          // whether the decimal point appears
          if (INT32_MAX == fmt_desc.decimal_pos_) {
            ++fmt_desc.pre_num_count_;
          } else {
            ++fmt_desc.post_num_count_;
          }
          // if element 'V' appears, count the number of digits after element 'V'
          if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
            ++fmt_desc.multi_;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_ZERO_FLAG;
        }
        break;
      case ObNFMElem::NFM_NINE:
        if (need_check && OB_FAIL(check_elem_nine_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem nine is valid", K(ret));
        } else {
          if (-1 == fmt_desc.digital_start_) {
            fmt_desc.digital_start_ = elem_item->offset_;
          }
          fmt_desc.digital_end_ = elem_item->offset_;
          // whether the decimal point appears
          if (INT32_MAX == fmt_desc.decimal_pos_) {
            ++fmt_desc.pre_num_count_;
          } else {
            ++fmt_desc.post_num_count_;
          }
          // if element 'V' appears, count the number of digits after element 'V'
          if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
            ++fmt_desc.multi_;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_NINE_FLAG;
        }
        break;
      case ObNFMElem::NFM_B:
        if (need_check && OB_FAIL(check_elem_b_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem blank is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_BLANK_FLAG;
        }
        break;
      case ObNFMElem::NFM_C:
        if (need_check && OB_FAIL(check_elem_c_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem c is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WARN("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_C_FLAG;
        }
        break;
      case ObNFMElem::NFM_D:
        if (need_check && OB_FAIL(check_elem_d_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem d is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_D_FLAG;
          fmt_desc.decimal_pos_ = elem_item->offset_;
        }
        break;
      case ObNFMElem::NFM_EEEE:
        if (need_check && OB_FAIL(check_elem_eeee_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem eeee is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_EEEE_FLAG;
        }
        break;
      case ObNFMElem::NFM_G:
        if (need_check && OB_FAIL(check_elem_g_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem g is valid", K(ret));
        } else {
          fmt_desc.last_separator_ = elem_item->offset_;
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_G_FLAG;
        }
        break;
      case ObNFMElem::NFM_L:
        if (need_check && OB_FAIL(check_elem_l_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem l is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WARN("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_L_FLAG;
        }
        break;
      case ObNFMElem::NFM_MI:
        if (need_check && OB_FAIL(check_elem_mi_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem mi is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_MI_FLAG;
        }
        break;
      case ObNFMElem::NFM_PR:
        if (need_check && OB_FAIL(check_elem_pr_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem pr is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_PR_FLAG;
        }
        break;
      case ObNFMElem::NFM_RN:
        if (need_check && OB_FAIL(check_elem_rn_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem rn is valid", K(ret));
        } else {
          if (elem_item->case_mode_ == ObNFMElem::UPPER_CASE) {
            fmt_desc.upper_case_flag_ |= NFM_RN_FLAG;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_RN_FLAG;
        }
        break;
      case ObNFMElem::NFM_S:
        if (need_check && OB_FAIL(check_elem_s_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem s is valid", K(ret));
        } else {
          if (elem_item->is_last_elem_) {
            fmt_desc.sign_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            fmt_desc.sign_appear_pos_ = OBNFMDesc::FIRST_POS;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_S_FLAG;
        }
        break;
      case ObNFMElem::NFM_TME:
        if (need_check && OB_FAIL(check_elem_tm_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem tme is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_TME_FLAG;
        }
        break;
      case ObNFMElem::NFM_TM:
      case ObNFMElem::NFM_TM9:
        if (need_check && OB_FAIL(check_elem_tm_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem tm9 is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_TM_FLAG;
        }
        break;
      case ObNFMElem::NFM_U:
        if (need_check && OB_FAIL(check_elem_u_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem u is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WARN("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_U_FLAG;
        }
        break;
      case ObNFMElem::NFM_V:
        if (need_check && OB_FAIL(check_elem_v_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem v is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_MULTI_FLAG;
        }
        break;
      case ObNFMElem::NFM_X:
        if (0 == fmt_desc.elem_x_count_
            && elem_item->case_mode_ == ObNFMElem::UPPER_CASE) {
          fmt_desc.upper_case_flag_ |= NFM_HEX_FLAG;
        }
        if (need_check && OB_FAIL(check_elem_x_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem hex is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_HEX_FLAG;
          fmt_desc.elem_x_count_++;
        }
        break;
      case ObNFMElem::NFM_FM:
        if (need_check && OB_FAIL(check_elem_fm_is_valid(elem_item, fmt_desc))) {
          LOG_WARN("check elem fm is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_FILLMODE_FLAG;
        }
        break;
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown elem type", K(ret), K(elem_item->keyword_->elem_type_));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
      fmt_desc.output_len_ = fmt_desc.output_len_ - fmt_desc.pre_num_count_ + 1;
    }
    if (!ObNFMElem::has_sign_group(fmt_desc.elem_flag_)
      && !ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)) {
      // if no sign or element 'RN' appears, add a sign character
      fmt_desc.output_len_ += 1;
    }
  }
  return ret;
}

const char *const ObNFMBase::LOWER_RM1[9] =
{"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix"};
const char *const ObNFMBase::LOWER_RM10[9] =
{"x", "xx", "xxx", "xl", "l", "lx", "lxx", "lxxx", "xc"};
const char *const ObNFMBase::LOWER_RM100[9] =
{"c", "cc", "ccc", "cd", "d", "dc", "dcc", "dccc", "cm"};
const char *const ObNFMBase::UPPER_RM1[9] =
{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"};
const char *const ObNFMBase::UPPER_RM10[9] =
{"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"};
const char *const ObNFMBase::UPPER_RM100[9] =
{"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"};

int ObNFMBase::search_keyword(const char *cur_ch, const int32_t remain_len,
                              const ObNFMKeyWord *&match_key,
                              ObNFMElem::ElemCaseMode &case_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_ch) || remain_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fmt str", K(ret), K(fmt_str_), K(remain_len));
  } else {
    // if the first character of an element is lowercase, it is interpreted as lowercase
    // otherwise, it is interpreted as uppercase
    if ((*cur_ch >= 'a') && (*cur_ch <= 'z')) {
      case_mode = ObNFMElem::LOWER_CASE;
    } else if ((*cur_ch >= 'A') && (*cur_ch <= 'Z')) {
      case_mode = ObNFMElem::UPPER_CASE;
    } else {
      case_mode = ObNFMElem::IGNORE_CASE;
    }
    int32_t index = 0;
    for (index = 0; index < ObNFMElem::MAX_TYPE_NUMBER; ++index) {
      const ObNFMKeyWord &keyword = ObNFMElem::NFM_KEYWORDS[index];
      if (remain_len >= keyword.len_
          && (0 == strncasecmp(cur_ch, keyword.ptr_, keyword.len_))) {
        match_key = &keyword;
        break;
      }
    }
    if (ObNFMElem::MAX_TYPE_NUMBER == index) {
      ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
      LOG_WARN("invalid fmt character ", K(ret), K(fmt_str_), K(remain_len));
    }
  }
  return ret;
}

int ObNFMBase::parse_fmt(const char* fmt_str, const int32_t fmt_len, bool need_check/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt_str) || fmt_len <= 0) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("invalid fmt str", K(ret), K(fmt_len));
  } else {
    fmt_str_.assign_ptr(fmt_str, fmt_len);
    int32_t remain_len = fmt_len;
    int32_t index = 0;
    const char* cur_ch = fmt_str;
    const char* fmt_end = fmt_str + fmt_len;
    ObNFMElem *last_elem = NULL;
    ObNFMElem::ElementType prefix_type = ObNFMElem::INVALID_TYPE;
    for (index = 0; OB_SUCC(ret) && remain_len > 0 && cur_ch < fmt_end; ++index) {
      char *elem_buf = NULL;
      const ObNFMKeyWord *match_keyword = NULL;
      ObNFMElem::ElemCaseMode case_mode = ObNFMElem::IGNORE_CASE;
      if (OB_FAIL(search_keyword(cur_ch, remain_len, match_keyword, case_mode))) {
        LOG_WARN("fail to search match keyword", K(ret), K(fmt_str_), K(remain_len));
      } else if (OB_ISNULL(elem_buf = static_cast<char*>(allocator_.alloc(sizeof(ObNFMElem))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory");
      } else {
        ObNFMElem *cur_elem = new(elem_buf) ObNFMElem;
        cur_elem->keyword_ = match_keyword;
        cur_elem->offset_ = index;
        cur_elem->case_mode_ = case_mode;
        cur_elem->prefix_type_ = prefix_type;
        if (OB_NOT_NULL(last_elem)) {
          last_elem->suffix_type_ = cur_elem->keyword_->elem_type_;
        }
        prefix_type = cur_elem->keyword_->elem_type_;
        last_elem = cur_elem;
        cur_ch += cur_elem->keyword_->len_;
        remain_len -= cur_elem->keyword_->len_;
        if (remain_len <= 0) {
          cur_elem->is_last_elem_ = true;
        }
        if (OB_FAIL(fmt_elem_list_.push_back(cur_elem))) {
          LOG_WARN("fail to push back elem to fmt_elem_list", K(ret));
        }
      }
    }
    // fill format desc
    if (OB_SUCC(ret) && OB_FAIL(ObNFMDescPrepare::fmt_desc_prepare(fmt_elem_list_,
                                                                   fmt_desc_,
                                                                   need_check))) {
      LOG_WARN("fail to prepare format desc", K(ret));
    }
  }
  return ret;
}

int ObNFMBase::fill_str(char *str, const int32_t str_len, const int32_t offset,
                        const char fill_ch, const int32_t fill_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || (str_len - offset < fill_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill str", K(ret), K(str_len), K(offset), K(fill_len));
  } else {
    MEMSET(str + offset, fill_ch, fill_len);
  }
  return ret;
}

int ObNFMBase::append_decimal_digit(BigNumber &num, const uint32_t decimal_digit) const
{
  int ret = OB_SUCCESS;
  uint64_t carry = decimal_digit;
  for (size_t i = 0; i < num.count(); ++i) {
    uint64_t product = num.at(i) * (uint64_t)10 + carry;
    num[i] = static_cast<uint32_t>(product);
    carry = product >> 32;
  }
  if (carry > 0 && OB_FAIL(num.push_back(carry))) {
    LOG_WARN("fail to push digit", K(ret));
  }
  return ret;
}

int ObNFMBase::decimal_to_hex(const ObString &origin_str, char *buf,
                              const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  BigNumber num;
  const char *ptr = origin_str.ptr();
  int32_t num_pos = 0;
  pos = 0;
  if (origin_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("origin_str is empty", K(ret));
  } else {
    // if is negative, fill it with '#'
    if ('-' == ptr[num_pos] && !is_zero(origin_str)) {
      if (OB_FAIL(fill_str(buf, buf_len, 0, '#', fmt_desc_.output_len_))) {
        LOG_WARN("fail to fill str", K(ret));
      }
      pos = fmt_desc_.output_len_;
    } else if (ObNFMElem::has_type((~NFM_HEX_FLAG) & (~NFM_ZERO_FLAG)
                                  & (~NFM_NINE_FLAG) & (~NFM_FILLMODE_FLAG),
                                  fmt_desc_.elem_flag_)) {
      ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
      LOG_WARN("incompatible with other formats", K(ret));
    } else {
      while (OB_SUCC(ret) && num_pos < origin_str.length()) {
        if (is_digit(ptr[num_pos]) && OB_FAIL(append_decimal_digit(num, ptr[num_pos] - '0'))) {
          LOG_WARN("fail to append decimal digit", K(ret));
        }
        ++num_pos;
      }
      if (num.empty()) {
        *buf = '0';
        pos = 1;
      } else {
        uint32_t last_digit = num.at(num.count() - 1);
        if (fmt_desc_.upper_case_flag_ & NFM_HEX_FLAG) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%X", last_digit))) {
            LOG_WARN("fail to print hex", K(ret));
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%x", last_digit))) {
            LOG_WARN("fail to print hex", K(ret));
          }
        }
        for (ssize_t i = num.count() - 2; OB_SUCC(ret) && i >= 0; --i) {
          if (fmt_desc_.upper_case_flag_ & NFM_HEX_FLAG) {
            ret = databuff_printf(buf, buf_len, pos, "%08X", num.at(i));
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%08x", num.at(i));
          }
        }
      }
      if (OB_SUCC(ret)) {
        // if the length of the fmt is less than the length of the result, fill it with '#'
        int32_t valid_len = fmt_desc_.elem_x_count_ + fmt_desc_.pre_num_count_;
        if (pos > valid_len) {
          if (OB_FAIL(fill_str(buf, buf_len, 0, '#', fmt_desc_.output_len_))) {
            LOG_WARN("fail to fill str", K(ret));
          }
          pos = fmt_desc_.output_len_;
        } else {
          ObString hex_str(pos, buf);
          LOG_DEBUG("decimal_to_hex", K(hex_str));
          if (pos < valid_len
                    && ObNFMElem::has_type(NFM_ZERO_FLAG, fmt_desc_.elem_flag_)) {
            // if the length of the fmt is greater than the length of the result string
            // and the fmt model contains leading zero, the return result needs to be
            // supplemented with '0'
            // eg: to_char(100, '00x') --> '064'
            int64_t fill_zero_count = valid_len - pos;
            MEMMOVE(buf + fill_zero_count, buf, pos);
            if (OB_FAIL(fill_str(buf, buf_len, 0, '0', fill_zero_count))) {
              LOG_WARN("fail to fill str", K(ret));
            } else {
              pos += fill_zero_count;
            }
          }
          // process fill mode
          if (OB_SUCC(ret) && OB_FAIL(process_fillmode(buf, buf_len, pos))) {
            LOG_WARN("fail to process fillmode", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObNFMBase::check_hex_str_valid(const char *str,
                                   const int32_t str_len,
                                   const int32_t offset) const
{
  int ret = OB_SUCCESS;
  int32_t hex_str_len = 0;
  int32_t fmt_len = fmt_desc_.elem_x_count_ + fmt_desc_.pre_num_count_;
  for (int32_t i = offset; OB_SUCC(ret) && i < str_len; i++) {
    if ((str[i] >= '0' && str[i] <= '9')
        || (str[i] >= 'a' && str[i] <= 'f')
        || (str[i] >= 'A' && str[i] <= 'F')) {
      ++hex_str_len;
    } else {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("invalid hex character", K(ret));
    }
  }
  if (OB_SUCC(ret) && hex_str_len > fmt_len) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("overflowed digit format", K(ret), K(hex_str_len), K(fmt_len));
  }
  return ret;
}

// we'll represent an arbitrary-precision non-negative hexadecimal string as a vector of uint64
// every 15 hexadecimal strings are converted into a uint64 number
// eg: 12345123456789abcdef --> '12345' and '123456789abcdef'
// '12345' and '123456789abcdef' --> [81985529216486900, 74565]
int ObNFMBase::build_hex_number(const char *str, const int32_t str_len,
                                int32_t &offset, HexNumber &nums) const
{
  int ret = OB_SUCCESS;
  int32_t digit = 0;
  uint64_t num = 0;
  int32_t index = 0;
  const int32_t batch_size = 15;
  for (int32_t i = str_len - 1; OB_SUCC(ret) && i >= offset; i--) {
    ++index;
    if (OB_FAIL(hex_to_num(str[i], digit))) {
      LOG_WARN("invalid hex character", K(ret));
    } else {
      uint64_t power_val = pow(16, index - 1);
      num = digit * power_val + num;
      if (index % batch_size == 0) {
        nums.push_back(num);
        index = 0;
        num = 0;
      }
    }
  }
  if (OB_SUCC(ret) && index % batch_size != 0) {
    nums.push_back(num);
  }
  return ret;
}

int ObNFMBase::int_to_roman_str(const int64_t num, char *buf,
                                const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if ((num < 1 || num > 3999)) {
    if (OB_FAIL(fill_str(buf, buf_len, 0, '#', fmt_desc_.output_len_))) {
      LOG_WARN("fail to fill str", K(ret));
    }
    pos = fmt_desc_.output_len_;
  } else {
    int64_t str_len = 0;
    char num_str[12] = {0};
    str_len = snprintf(num_str, sizeof(num_str), "%d", static_cast<int32_t>(num));
    for (char *p = num_str; OB_SUCC(ret) && *p != '\0'; p++, --str_len) {
      int32_t val = *p - 49;
      if (val < 0) {
        continue;
      }
      if (str_len > 3) {
        while (OB_SUCC(ret) && val-- != -1) {
          if (fmt_desc_.upper_case_flag_ & NFM_RN_FLAG) {
            ret = databuff_printf(buf, buf_len, pos, "%s", "M");
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%s", "m");
          }
        }
      } else {
        if (str_len == 3) {
          if (fmt_desc_.upper_case_flag_ & NFM_RN_FLAG) {
            ret = databuff_printf(buf, buf_len, pos, "%s", UPPER_RM100[val]);
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%s", LOWER_RM100[val]);
          }
        } else if (str_len == 2) {
          if (fmt_desc_.upper_case_flag_ & NFM_RN_FLAG) {
            ret = databuff_printf(buf, buf_len, pos, "%s", UPPER_RM10[val]);
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%s", LOWER_RM10[val]);
          }
        } else if (str_len == 1) {
          if (fmt_desc_.upper_case_flag_ & NFM_RN_FLAG) {
            ret = databuff_printf(buf, buf_len, pos, "%s", UPPER_RM1[val]);
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%s", LOWER_RM1[val]);
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(process_fillmode(buf, buf_len, pos))) {
      LOG_WARN("fail to process fillmode", K(ret));
    }
  }
	return ret;
}

int ObNFMBase::get_integer_part_len(const common::ObString &num_str,
                                    int32_t &integer_part_len) const
{
  int ret = OB_SUCCESS;
  const char *ptr = num_str.ptr();
  integer_part_len = 0;
  bool digit_appear = false;
  for (int32_t i = 0; i < num_str.length(); ++i) {
    if (ptr[i] >= '0' && ptr[i] <= '9') {
      integer_part_len++;
      digit_appear = true;
    } else if ('.' == ptr[i] || 'E' == ptr[i]) {
      // if it's scientific notation
      // eg: to_number('1E+00', '9BEEEE') --> integer_part_len = 1
      break;
    } else if (digit_appear && ptr[i] != ',') {
      // to_number('USD123.123', 'c999.999')
      // to_number('23USD00', '99C99') --> integer_part_len = 2
      break;
    }
  }
  return ret;
}

int ObNFMBase::get_decimal_part_len(const common::ObString &num_str,
                                    int32_t &decimal_part_len) const
{
  int ret = OB_SUCCESS;
  const char *ptr = num_str.ptr();
  decimal_part_len = 0;
  bool dc_appear = false;
  for (int32_t i = 0; i < num_str.length(); ++i) {
    if ('.' == ptr[i]) {
      if (dc_appear) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("decimal already appear", K(ret));
      } else {
        dc_appear = true;
      }
    } else if ('E' == ptr[i]) {
      // if it's scientific notation
      // eg: to_number('1.23E+03', '9.99EEEE') --> decimal_part_len = 2
      break;
    } else if (ptr[i] < '0' || ptr[i] > '9') {
      if (dc_appear) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("decimal already appear", K(ret));
      } else {
        dc_appear = true;
      }
    } else if (dc_appear && (ptr[i] >= '0' && ptr[i] <= '9')) {
      ++decimal_part_len;
    }
  }
  return ret;
}

bool ObNFMBase::is_digit(const char c) const
{
  return c >= '0' && c <= '9';
}

bool ObNFMBase::is_zero(const common::ObString &num_str) const
{
  const char *ptr = num_str.ptr();
  for (int32_t i = 0; i < num_str.length(); i++) {
    if (ptr[i] != '-' && ptr[i] != '.' && ptr[i] != '0') {
      return false;
    }
  }
  return true;
}

int ObNFMBase::hex_to_num(const char c, int32_t &val) const
{
  int ret = OB_SUCCESS;
  if (c >= '0' && c <= '9') {
    val = c - '0';
  } else if (c >= 'a' && c <= 'f') {
    val = c - 'a' + 10;
  } else if (c >= 'A' && c <= 'F') {
    val = c - 'A' + 10;
  } else {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("invalid hex character", K(ret));
  }
  return ret;
}

int ObNFMBase::process_fillmode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < 0 || fmt_desc_.output_len_ < pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(buf_len));
  } else {
    int32_t space_size = fmt_desc_.output_len_ - pos;
    if (!ObNFMElem::has_type(NFM_TME_FLAG, fmt_desc_.elem_flag_)
        && !ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc_.elem_flag_)
        && !ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc_.elem_flag_)) {
      // if is not fill mode. needs to be moved to the end to fill leading ' '
      if (space_size) {
        MEMMOVE(buf + space_size, buf, pos);
        for (int32_t i = 0; i < space_size; ++i) {
          buf[i] = ' ';
        }
      }
      pos = fmt_desc_.output_len_;
    }
  }
  return ret;
}

int ObNFMBase::get_nls_currency(const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (ObNFMElem::has_type(NFM_C_FLAG, fmt_desc_.elem_flag_)) {
    fmt_desc_.nls_currency_ = session.get_iso_nls_currency();
  } else if (ObNFMElem::has_type(NFM_L_FLAG, fmt_desc_.elem_flag_)) {
    if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_NLS_CURRENCY,
                fmt_desc_.nls_currency_))) {
      LOG_WARN("fail to get sys variable", K(ret));
    }
  } else if (ObNFMElem::has_type(NFM_U_FLAG, fmt_desc_.elem_flag_)) {
    if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_NLS_DUAL_CURRENCY,
                fmt_desc_.nls_currency_))) {
      LOG_WARN("fail to get sys variable", K(ret));
    }
  }
  return ret;
}

int ObNFMBase::get_iso_grouping(const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_NLS_NUMERIC_CHARACTERS,
                     fmt_desc_.iso_grouping_))) {
    LOG_WARN("fail to get sys variable", K(ret));
  }
  return ret;
}

int ObNFMBase::conv_num_to_nfm_obj(const common::ObObj &obj,
                                   common::ObExprCtx &expr_ctx,
                                   ObNFMObj &nfm_obj)
{
  int ret = OB_SUCCESS;
  number::ObNumber num;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (obj.is_float() || obj.is_ufloat() || obj.is_double() || obj.is_udouble()) {
      if (obj.is_double() || obj.is_udouble()) {
        nfm_obj.set_obj_type(ObDoubleType);
        nfm_obj.set_double(obj.get_double());
      } else {
        nfm_obj.set_obj_type(ObFloatType);
        nfm_obj.set_float(obj.get_float());
      }
    } else {
      EXPR_GET_NUMBER_V2(obj, num);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to cast obj as number", K(ret));
      } else {
        nfm_obj.set_obj_type(ObNumberType);
        nfm_obj.set_number(num);
      }
    }
  }
  return ret;
}

int ObNFMBase::conv_num_to_nfm_obj(const common::ObObjMeta &obj_meta,
                                   const ObDatum &obj,
                                   ObNFMObj &nfm_obj,
                                   common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const bool is_float = obj_meta.is_float();
  const bool is_double = obj_meta.is_double();
  const bool is_ufloat = obj_meta.is_ufloat();
  const bool is_udouble = obj_meta.is_udouble();
  const bool is_int_tc = obj_meta.is_integer_type();
  if (is_float || is_ufloat || is_double || is_udouble) {
    if (is_float || is_ufloat) {
      nfm_obj.set_obj_type(ObFloatType);
      nfm_obj.set_float(obj.get_float());
    } else {
      nfm_obj.set_obj_type(ObDoubleType);
      nfm_obj.set_double(obj.get_double());
    }
  } else {
    number::ObNumber num;
    if (is_int_tc) { // int tc, to support PLS_INTERGER type
      if (OB_FAIL(num.from(obj.get_int(), alloc))) {
        LOG_WARN("fail to int_number", K(ret));
      }
    } else { // number
      num.assign(obj.get_number().desc_.desc_,
                          const_cast<uint32_t *>(&(obj.get_number().digits_[0])));
    }
    if (OB_SUCC(ret)) {
      nfm_obj.set_obj_type(ObNumberType);
      nfm_obj.set_number(num);
    }
  }
  return ret;
}

int ObNFMBase::cast_obj_to_int(const ObNFMObj &nfm_obj, int64_t &res_val)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = nfm_obj.get_obj_type();
  int32_t scale = 0;
  if (ObFloatType == obj_type) {
    res_val = static_cast<int64_t>(ObExprUtil::round_double(nfm_obj.get_float(), scale));
  } else if (ObDoubleType == obj_type) {
    res_val = static_cast<int64_t>(ObExprUtil::round_double(nfm_obj.get_double(), scale));
  } else {
    number::ObNumber num = nfm_obj.get_number();
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(nmb.from(num, tmp_alloc))) {
      LOG_WARN("copy number failed.", K(ret), K(num));
    } else if (OB_FAIL(nmb.round(scale))) {
      LOG_WARN("round failed.", K(ret), K(num.format()), K(scale));
    } else if (!nmb.is_valid_int64(res_val)) {
      // if it is not a valid int64, negative number returns INT64_MIN
      // positive number returns INT64_MAX
      if (nmb.is_negative()) {
        res_val = INT64_MIN;
      } else {
        res_val = INT64_MAX;
      }
    }
  }
  LOG_DEBUG("cast_obj_to_int", K(ret), K(res_val));
  return ret;
}

int ObNFMBase::remove_leading_zero(char *buf, int64_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf");
  } else {
    bool is_negative = false;
    int32_t pos = 0;
    if (offset > 0 && '-' == buf[0]) {
      is_negative = true;
      pos++;
    }
    int32_t leading_zero = 0;
    for (; pos < offset; ++pos) {
      if ('0' == buf[pos]) {
        ++leading_zero;
      } else {
        break;
      }
    }
    if (leading_zero > 0) {
      if (is_negative) {
        MEMMOVE(buf + 1, buf + leading_zero + 1, offset - leading_zero);
      } else {
        MEMMOVE(buf, buf + leading_zero, offset - leading_zero);
      }
      offset -= leading_zero;
    }
  }
  return ret;
}

int ObNFMBase::cast_obj_to_num_str(const ObNFMObj &nfm_obj,
                                   const int64_t scale,
                                   common::ObString &num_str)
{
  int ret = OB_SUCCESS;
  bool is_negative = false;
  int64_t num_str_len = 0;
  number::ObNumber nmb;
  char *num_str_buf = NULL;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  ObObjType obj_type = nfm_obj.get_obj_type();
  if (OB_ISNULL(num_str_buf = static_cast<char *>(
                              allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    if (ObFloatType == obj_type || ObDoubleType == obj_type) {
      if (ObFloatType == obj_type) {
        num_str_len = ob_gcvt_strict(nfm_obj.get_float(), OB_GCVT_ARG_FLOAT, alloc_size,
                                    num_str_buf, NULL, lib::is_oracle_mode(), TRUE, FALSE);
      } else if (ObDoubleType == obj_type) {
        num_str_len = ob_gcvt_strict(nfm_obj.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size,
                                    num_str_buf, NULL, lib::is_oracle_mode(), TRUE, FALSE);
      }
      if (OB_FAIL(nmb.from(num_str_buf, num_str_len, allocator_))) {
        LOG_WARN("number from str failed", K(ret));
      }
      num_str_len = 0;
    } else {
      number::ObNumber num = nfm_obj.get_number();
      if (OB_FAIL(nmb.from(num, allocator_))) {
        LOG_WARN("copy number failed.", K(ret), K(num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nmb.format_v2(num_str_buf, alloc_size, num_str_len, scale, false))) {
      LOG_WARN("fail to convert number to string", K(ret));
    } else {
      is_negative = nmb.is_negative();
      num_str.assign_ptr(num_str_buf, static_cast<int32_t>(num_str_len));
      // is if "-0", special treatment for compatible with oracle
      // eg: to_char(-0.4000,  '0000') --> -0000
      if (is_zero(num_str)) {
        int32_t pos = 0;
        if (is_negative) {
          num_str_buf[pos++] = '-';
        }
        if (fmt_desc_.pre_num_count_ != 0
            && 0 == fmt_desc_.post_num_count_) {
          num_str_buf[pos++] = '0';
        }
        num_str_buf[pos++] = '.';
        if (fmt_desc_.post_num_count_ != 0) {
          num_str_buf[pos++] = '0';
        }
        num_str.assign_ptr(num_str_buf, pos);
      } else if (OB_FAIL(remove_leading_zero(num_str_buf, num_str_len))) {
        LOG_WARN("fail to remove leading zero", K(ret));
      } else {
        num_str.assign_ptr(num_str_buf, static_cast<int32_t>(num_str_len));
      }
      LOG_DEBUG("cast_obj_to_num_str", K(num_str));
    }
  }
  return ret;
}

int ObNFMBase::num_str_to_sci(const common::ObString &num_str, const int32_t scale,
                              char *buf, const int64_t len, int64_t &pos, bool is_tm) const
{
  int ret = OB_SUCCESS;
  const char *ptr = num_str.ptr();
  int64_t str_len = num_str.length();
  int64_t origin = pos;
  const int64_t SCI_NUMBER_LENGTH = 40;
  if (OB_UNLIKELY(pos > len || len < 0 || pos < 0 || NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), K(pos), K(len), KP(buf));
  } else if (is_zero(num_str)) {
    if (OB_FAIL(databuff_printf(buf, len, pos, "%.*s", num_str.length(),
                num_str.ptr()))) {
      LOG_WARN("fail to fill num str", K(ret), K(len), K(num_str));
    } else if (OB_FAIL(databuff_printf(buf, len, pos, "%s", "E+00"))) {
      LOG_WARN("fail to fill num str", K(ret));
    }
  } else {
    int64_t raw_pos = 0;
    //the string of the exponent part
    char pow_str[6];
    int64_t pow_size = 0;
    int64_t pow_index = 0;
    bool pre_dot = false;
    bool is_negative = false;
    int64_t width_count = 0;
    pow_str[pow_index++] = 'E';
    pow_str[pow_index++] = '+';
    // handle the negative sign or decimal point at the beginning of num str
    if ('-' == ptr[raw_pos]) {
      raw_pos++;
      buf[pos++] = '-';
      is_negative = true;
    }
    if ('.' == ptr[raw_pos]) {
      raw_pos++;
      pre_dot = true;
      pow_str[pow_index - 1] = '-';
    }
    int64_t zero_count = 0;
    // first non-zero number found
    while ('0' == ptr[raw_pos] && raw_pos < str_len) {
      raw_pos++;
      zero_count++;
    }
    buf[pos++] = ptr[raw_pos++];
    if (scale != 0) {
      buf[pos++] = '.';
    }
    // determine the index part and the number part according to
    // whether there is a decimal point in the front
    if (pre_dot) {
      pow_size = zero_count + 1;
      if (pow_size >= 0 && pow_size <= 99) {
        width_count = 2;
      } else {
        width_count = 3;
      }
      if (scale > 0) {
        for (int32_t count = 0; count < scale
            && pos < SCI_NUMBER_LENGTH - width_count - pow_index + origin;
             ++count) {
          if (raw_pos >= str_len) {
            buf[pos++] = '0';
          } else {
            buf[pos++] = ptr[raw_pos++];
          }
        }
      } else if (scale < 0) {
        while (raw_pos < str_len
              && pos < SCI_NUMBER_LENGTH - width_count - pow_index + origin) {
          buf[pos++] = ptr[raw_pos++];
        }
      }
    } else if (!pre_dot && 0 == zero_count) {
      int64_t index_count = 0;
      // if numbers greater than 10, always need to traverse the number
      // array to know the final exponent. when a decimal point is encountered
      // it will not be traversed. the index value will affect the number of bytes
      // in the index part
      for (int64_t i = raw_pos; i < str_len && ptr[i] != '.'; ++i) {
        index_count++;
      }
      if (index_count >= 0 && index_count <= 99) {
        width_count = 2;
      } else {
        width_count = 3;
      }
      if (scale > 0) {
        for (int32_t count = 0; count < scale
             && pos < SCI_NUMBER_LENGTH - pow_index - width_count + origin;) {
          if (raw_pos >= str_len) {
            buf[pos++] = '0';
            ++count;
          } else if ('.' == ptr[raw_pos]) {
            raw_pos++;
          } else {
            buf[pos++] = ptr[raw_pos++];
            ++count;
          }
        }
      } else if (scale < 0) {
        while (raw_pos < str_len && pos < SCI_NUMBER_LENGTH - pow_index - width_count + origin) {
          if ('.' == ptr[raw_pos]) {
            raw_pos++;
          } else {
            buf[pos++] = ptr[raw_pos++];
          }
        }
      }
      pow_size = index_count;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the number raw str is unexpected", K(ret));
    }
    // round the last digit and process the carry
    if (!is_tm && OB_SUCC(ret)) {
      int64_t carry = 0;
      int64_t carry_pos = pos;
      int64_t digit_start_pos = is_negative ? origin + 1 : origin;
      if ('.' == ptr[raw_pos]) { ++raw_pos; }
      if (raw_pos < str_len && ptr[raw_pos] >= '5' && ptr[raw_pos] <= '9') {
        carry = 1;
        carry_pos--;
        while (carry && carry_pos >= digit_start_pos && OB_SUCC(ret)) {
          if (buf[carry_pos] >= '0' && buf[carry_pos] <= '8') {
            buf[carry_pos] = (char)((int)buf[carry_pos] + carry);
            carry = 0;
            carry_pos--;
          } else if ('9' == buf[carry_pos]) {
            carry = 1;
            buf[carry_pos--] = '0';
          } else if ('.' == buf[carry_pos]) {
            carry_pos--;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("It's unexpected to round the number sci", K(ret));
          }
        }
        if (1 == carry && digit_start_pos - 1 == carry_pos && OB_SUCC(ret)) {
          bool decimal_appear = false;
          for (int64_t i = pos - 1; i >= digit_start_pos + 1; --i) {
            if (buf[i - 1] != '.') {
              buf[i] = buf[i - 1];
            } else {
              decimal_appear = true;
            }
          }
          buf[digit_start_pos] = '1';
          if (decimal_appear) {
            buf[digit_start_pos + 1] = '.';
          }
          ++pow_size;
        }
      }
    }
    if (OB_SUCC(ret) && is_tm) {
      int32_t offset = 0;
      // 1.0000000000000000000000000000000000E+65 --> 1E+65
      for (offset = pos - 1; offset > origin; offset--) {
        if (buf[offset] != '0') {
          break;
        }
      }
      pos = offset + 1;
      if ('.' == buf[offset]) {
        --pos;
      }
    }
    // print exponent
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(pow_str, sizeof(pow_str), pow_index, "%02ld", pow_size))) {
        LOG_WARN("fail to generate pow str", K(ret));
      } else {
        for (int i = 0; i < pow_index; ++i) {
          buf[pos++] = pow_str[i];
        }
      }
    }
  }
  return ret;
}

int ObNFMToChar::process_mul_format(const ObNFMObj &nfm_obj, common::ObString &num_str)
{
  int ret = OB_SUCCESS;
  number::ObNumber num_val;
  number::ObNumber base_num;
  number::ObNumber power_num;
  number::ObNumber origin_num;
  const int64_t base_val = 10;
  const int64_t scale = 0;
  int64_t origin_str_len = 0;
  char *origin_str_buf = NULL;
  int64_t exponent = fmt_desc_.multi_;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  if (OB_ISNULL(origin_str_buf = static_cast<char *>(
                allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObObjType obj_type = nfm_obj.get_obj_type();
    int64_t power_val = pow(10, exponent);
    if (ObFloatType == obj_type) {
      origin_str_len = ob_fcvt(ObExprUtil::round_double(nfm_obj.get_float() * power_val, scale),
                               scale, alloc_size, origin_str_buf, NULL);
    } else if (ObDoubleType == obj_type) {
      origin_str_len = ob_fcvt(ObExprUtil::round_double(nfm_obj.get_double() * power_val, scale),
                               scale, alloc_size, origin_str_buf, NULL);
    } else if (ObNumberType == obj_type) {
      num_val = nfm_obj.get_number();
      if (OB_FAIL(base_num.from(base_val, allocator_))) {
        LOG_WARN("fail to cast int to number", K(ret));
      } else if (OB_FAIL(base_num.power(exponent, power_num, allocator_))) {
        LOG_WARN("power calc failed", K(ret));
      } else if (OB_FAIL(num_val.mul_v2(power_num, origin_num, allocator_))) {
        LOG_WARN("fail to mul number", K(ret));
      } else if (OB_FAIL(origin_num.round(scale))) {
        LOG_WARN("round failed", K(ret), K(origin_num), K(scale));
      } else if (OB_FAIL(origin_num.format_v2(origin_str_buf, alloc_size,
                 origin_str_len, scale, false))) {
        LOG_WARN("fail to convert number to string", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid obj type", K(ret), K(obj_type));
    }
    if (OB_SUCC(ret)) {
      num_str.assign_ptr(origin_str_buf, static_cast<int32_t>(origin_str_len));
      LOG_DEBUG("obj_to_multi_num_str", K(num_str));
    }
  }
  return ret;
}

int ObNFMToChar::process_roman_format(const ObNFMObj &nfm_obj, char *buf,
                                      const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t val;
  if (OB_FAIL(cast_obj_to_int(nfm_obj, val))) {
    LOG_WARN("fail to cast obj to int", K(ret));
  } else if (OB_FAIL(int_to_roman_str(val, buf, buf_len, pos))) {
    LOG_WARN("fail to convert int to roman str", K(ret));
  }
  return ret;
}

int ObNFMToChar::process_hex_format(const ObNFMObj &nfm_obj, char *buf,
                                    const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t scale = 0;
  ObString origin_str;
  if (OB_FAIL(cast_obj_to_num_str(nfm_obj, scale, origin_str))) {
    LOG_WARN("fail to cast obj to num str", K(ret));
  } else if (OB_FAIL(decimal_to_hex(origin_str, buf, buf_len, pos))) {
    LOG_WARN("fail to convert decimal to hex str", K(ret));
  }
  return ret;
}

int ObNFMToChar::process_tm_format(const ObNFMObj &nfm_obj, char *buf,
                                   const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t num_str_len = 0;
  char *num_str_buf = NULL;
  ObString num_str;
  const int32_t scale = -1;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  if (OB_ISNULL(num_str_buf = static_cast<char *>(
                              allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObObjType obj_type = nfm_obj.get_obj_type();
    if (ObFloatType == obj_type) {
      num_str_len = ob_gcvt_opt(nfm_obj.get_float(), OB_GCVT_ARG_FLOAT, static_cast<int32_t>(alloc_size),
                                num_str_buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (ObDoubleType == obj_type) {
      num_str_len = ob_gcvt_opt(nfm_obj.get_double(), OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(alloc_size),
                                num_str_buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (ObNumberType == obj_type) {
      number::ObNumber num = nfm_obj.get_number();
      number::ObNumber nmb;
      if (OB_FAIL(nmb.from(num, allocator_))) {
        LOG_WARN("copy number failed.", K(ret), K(num));
      } else if (OB_FAIL(nmb.format_v2(num_str_buf, alloc_size, num_str_len, scale, false))) {
        LOG_WARN("fail to format", K(ret), K(nmb));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid obj type", K(ret), K(obj_type));
    }
    if (OB_SUCC(ret)) {
      num_str.assign_ptr(num_str_buf, static_cast<int32_t>(num_str_len));
      LOG_DEBUG("process_tme_format", K(ret), K(num_str_buf), K(num_str_len));
      if (num_str_len <= 64) {
        MEMCPY(buf, num_str_buf, num_str_len);
        pos += num_str_len;
      } else if (num_str_len > 64) {
        if (OB_FAIL(num_str_to_sci(num_str, scale, buf, buf_len, pos, true))) {
          LOG_WARN("failed to convert num to sci str", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(process_fillmode(buf, buf_len, pos))) {
      LOG_WARN("fail to process fillmode", K(ret));
    }
  }
  return ret;
}

int ObNFMToChar::process_tme_format(const ObNFMObj &nfm_obj, char *buf,
                                    const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t num_str_len = 0;
  char *num_str_buf = NULL;
  ObString num_str;
  const int32_t scale = -1;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  if (OB_ISNULL(num_str_buf = static_cast<char *>(
                              allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObObjType obj_type = nfm_obj.get_obj_type();
    if (ObFloatType == obj_type) {
      num_str_len = ob_gcvt_opt(nfm_obj.get_float(), OB_GCVT_ARG_FLOAT, static_cast<int32_t>(alloc_size),
                                num_str_buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (ObDoubleType == obj_type) {
      num_str_len = ob_gcvt_opt(nfm_obj.get_double(), OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(alloc_size),
                                num_str_buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (ObNumberType == obj_type) {
      number::ObNumber num = nfm_obj.get_number();
      number::ObNumber nmb;
      if (OB_FAIL(nmb.from(num, allocator_))) {
        LOG_WARN("copy number failed.", K(ret), K(num));
      } else if (OB_FAIL(nmb.format_v2(num_str_buf, alloc_size, num_str_len, scale, false))) {
        LOG_WARN("fail to format", K(ret), K(nmb));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid obj type", K(ret), K(obj_type));
    }
    if (OB_SUCC(ret)) {
      num_str.assign_ptr(num_str_buf, static_cast<int32_t>(num_str_len));
      LOG_DEBUG("process_tme_format", K(ret), K(num_str_buf), K(num_str_len));
      if (OB_FAIL(num_str_to_sci(num_str, scale, buf, buf_len, pos, true))) {
        LOG_WARN("failed to convert num to sci str", K(ret));
      } else if (OB_FAIL(process_fillmode(buf, buf_len, pos))) {
        LOG_WARN("fail to process fillmode", K(ret));
      }
    }
  }
  return ret;
}

int ObNFMToChar::process_sci_format(const common::ObString &origin_str, const int32_t scale,
                                    common::ObString &num_str)
{
  int ret = OB_SUCCESS;
  int64_t sci_str_len = 0;
  char *sci_str_buf = NULL;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  if (fmt_desc_.pre_num_count_ < 1) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("invalid number fmt model", K_(fmt_str));
  } else if (OB_ISNULL(sci_str_buf = static_cast<char *>(
                       allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    if (OB_FAIL(num_str_to_sci(origin_str, scale, sci_str_buf, alloc_size, sci_str_len, false))) {
      LOG_WARN("failed to convert num to sci str", K(ret));
    } else {
      num_str.assign_ptr(sci_str_buf, sci_str_len);
    }
  }
  return ret;
}

int ObNFMToChar::process_output_fmt(const common::ObString &str,
                                    const int32_t integer_part_len,
                                    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const char *num_str = str.ptr();
  int32_t num_str_len = str.length();
  int32_t num_str_pos = 0;
  pos = 0;
  bool is_negative = false;
  if ('-' == num_str[num_str_pos]) {
    num_str_pos++;
    is_negative = true;
  }
  // processing leading positive or negative sign element
  if (ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::FIRST_POS == fmt_desc_.sign_appear_pos_) {
      if (is_negative) {
        ret = databuff_printf(buf, buf_len, pos, "%c", '-');
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%c", '+');
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        ret = databuff_printf(buf, buf_len, pos, "%c", '<');
      }
    }
  } else {
    // if there is no leading positive or negative sign element
    // fill in '-' before negative
    if (is_negative) {
      ret = databuff_printf(buf, buf_len, pos, "%c", '-');
    }
  }
  // processing element '$'
  if (OB_SUCC(ret) && ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc_.elem_flag_)) {
    ret = databuff_printf(buf, buf_len, pos, "%c", '$');
  }
  if (OB_SUCC(ret)) {
    int32_t need_skip_num = fmt_desc_.pre_num_count_ - integer_part_len;
    bool digit_appear = false;
    int32_t traversed_num_count = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < fmt_elem_list_.count(); ++i) {
      const ObNFMElem *elem_item = fmt_elem_list_.at(i);
      if (OB_ISNULL(elem_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid elem item", K(ret));
      } else {
        switch (elem_item->keyword_->elem_type_) {
        case ObNFMElem::NFM_COMMA:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (!digit_appear ||
                traversed_num_count > integer_part_len) {
              // skip, do nothing
            } else {
              ret = databuff_printf(buf, buf_len, pos, "%c", ',');
            }
          } else {
            if (!digit_appear ||
              (need_skip_num && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i))) {
              // skip, do nothing
            } else {
              ret = databuff_printf(buf, buf_len, pos, "%c", ',');
            }
          }
          break;
        case ObNFMElem::NFM_PERIOD:
          ret = databuff_printf(buf, buf_len, pos, "%c", '.');
          if ('.' == num_str[num_str_pos]) {
            num_str_pos++;
          }
          break;
        case ObNFMElem::NFM_ZERO:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (traversed_num_count >= integer_part_len) {
              // skip, do nothing
            } else if (num_str_pos >= num_str_len || !is_digit(num_str[num_str_pos])) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WARN("invalid number", K(ret), K(str));
            } else {
              digit_appear = true;
              ret = databuff_printf(buf, buf_len, pos, "%c", num_str[num_str_pos++]);
            }
          } else {
            if (need_skip_num) {
              // need to fill leading '0'
              ret = databuff_printf(buf, buf_len, pos, "%c", '0');
              need_skip_num--;
              digit_appear = true;
            } else if (num_str_pos >= num_str_len || !is_digit(num_str[num_str_pos])) {
              // need to fill '0' at last
              // eg: to_char(123456789, 'FM999999999.00000') --> 123456789.00000
              ret = databuff_printf(buf, buf_len, pos, "%c", '0');
              digit_appear = true;
            } else {
              ret = databuff_printf(buf, buf_len, pos, "%c", num_str[num_str_pos++]);
              digit_appear = true;
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_NINE:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (traversed_num_count >= integer_part_len) {
              // skip, do nothing
            } else if (num_str_pos >= num_str_len || !is_digit(num_str[num_str_pos])) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WARN("invalid number", K(ret), K(str));
            } else {
              digit_appear = true;
              ret = databuff_printf(buf, buf_len, pos, "%c", num_str[num_str_pos++]);
            }
          } else {
            if (need_skip_num) {
              // if element '0' has appeared, the following numbers need to fill '0'
              // eg: to_char(123.123, '9099999.999') --> 000123.123
              if (fmt_desc_.zero_start_ >= 0 && fmt_desc_.zero_start_ < i) {
                digit_appear = true;
                ret = databuff_printf(buf, buf_len, pos, "%c", '0');
              }
              need_skip_num--;
            } else if (num_str_pos >= num_str_len || !is_digit(num_str[num_str_pos])) {
              // eg: to_char(10000, 'FM99,999.99')  --> 10,000.
              // eg: to_char(10000, '99,999.99') --> 10,000.00
              if (!ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc_.elem_flag_)
                  || (fmt_desc_.zero_end_ >= 0 && fmt_desc_.zero_end_ > i)) {
                digit_appear = true;
                ret = databuff_printf(buf, buf_len, pos, "%c", '0');
              }
            } else {
              digit_appear = true;
              ret = databuff_printf(buf, buf_len, pos, "%c", num_str[num_str_pos++]);
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_D:
          if (fmt_desc_.iso_grouping_.length() < 2) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid iso grouping", K(ret), K(fmt_desc_.iso_grouping_));
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%c", fmt_desc_.iso_grouping_.ptr()[0]);
            // eg: to_char(123, 'FM999.9999') --> 123.
            if (fmt_desc_.iso_grouping_.ptr()[0] == num_str[num_str_pos]) {
              num_str_pos++;
            }
          }
          break;
        case ObNFMElem::NFM_G:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (!digit_appear ||
                traversed_num_count > integer_part_len) {
              // skip, do nothing
            } else {
              if (fmt_desc_.iso_grouping_.length() < 2) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid iso grouping", K(ret), K(fmt_desc_.iso_grouping_));
              } else {
                ret = databuff_printf(buf, buf_len, pos, "%c", fmt_desc_.iso_grouping_.ptr()[1]);
              }
            }
          } else {
            if (!digit_appear ||
                (need_skip_num && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i))) {
              // skip, do nothing
            } else {
              if (fmt_desc_.iso_grouping_.length() < 2) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid iso grouping", K(ret), K(fmt_desc_.iso_grouping_));
              } else {
                ret = databuff_printf(buf, buf_len, pos, "%c", fmt_desc_.iso_grouping_.ptr()[1]);
              }
            }
          }
          break;
        case ObNFMElem::NFM_C:
        case ObNFMElem::NFM_L:
        case ObNFMElem::NFM_U:
          ret = databuff_printf(buf, buf_len, pos, "%.*s",
                                fmt_desc_.nls_currency_.length(), fmt_desc_.nls_currency_.ptr());
          if (OBNFMDesc::MIDDLE_POS == fmt_desc_.currency_appear_pos_) {
            // if the element('C', 'L', 'U') appears in the middle, it will be regarded as the
            // decimal point, so the iso currency symbol is used to replace the decimal point
            if (num_str_pos < num_str_len && '.' == num_str[num_str_pos]) {
              num_str_pos++;
            }
          }
          break;
        case ObNFMElem::NFM_EEEE:
          while (OB_SUCC(ret) && num_str_pos < num_str_len) {
            buf[pos++] = num_str[num_str_pos];
            num_str_pos++;
          }
          break;
        default:
          break;
        }
      }
    }
  }
  // processing the last positive or negative sign element
  if (OB_SUCC(ret) && ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::LAST_POS == fmt_desc_.sign_appear_pos_) {
      if (is_negative) {
        ret = databuff_printf(buf, buf_len, pos, "%c", '-');
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%c", '+');
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        ret = databuff_printf(buf, buf_len, pos, "%c", '>');
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%c", ' ');
      }
    } else if (ObNFMElem::has_type(NFM_MI_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        ret = databuff_printf(buf, buf_len, pos, "%c", '-');
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%c", ' ');
      }
    }
  }
  // processing result is zero and has element 'B'
  if (OB_SUCC(ret) && ObNFMElem::has_type(NFM_BLANK_FLAG, fmt_desc_.elem_flag_) && is_zero(str)) {
    if (ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc_.elem_flag_)) {
      fmt_desc_.output_len_ = 0;
    } else {
      if (OB_FAIL(fill_str(buf, buf_len, 0, ' ', fmt_desc_.output_len_))) {
        LOG_WARN("fail to fill str", K(ret));
      }
    }
    pos = fmt_desc_.output_len_;
  }
  if (pos > fmt_desc_.output_len_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("res str is larger than output len", K(ret), K(pos), K(fmt_desc_.output_len_));
  }
  if (OB_SUCC(ret) && OB_FAIL(process_fillmode(buf, buf_len, pos))) {
    LOG_WARN("fail to process fillmode", K(ret));
  }
  return ret;
}

int ObNFMToChar::process_fmt_conv(const ObSQLSessionInfo &session,
                                  const char *fmt_str, const int32_t fmt_len,
                                  const ObNFMObj &nfm_obj, char *res_buf,
                                  const int64_t res_buf_len, int64_t &offset)
{
  int ret = OB_SUCCESS;
  ObString num_str;
  bool is_overflow = false;
  int32_t integer_part_len = 0;
  if (OB_FAIL(parse_fmt(fmt_str, fmt_len))) {
    LOG_WARN("fail to parse fmt model", K(ret), K(fmt_len));
  } else {
    // processing calculation conversion element
    if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc_.elem_flag_)) {
      if (OB_FAIL(process_roman_format(nfm_obj, res_buf, res_buf_len, offset))) {
        LOG_WARN("fail to process roman fmt", K(ret));
      }
    } else if (ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc_.elem_flag_)) {
      if (OB_FAIL(process_hex_format(nfm_obj, res_buf, res_buf_len, offset))) {
        LOG_WARN("fail to process hex fmt", K(ret));
      }
    } else if (ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc_.elem_flag_)) {
      if (OB_FAIL(process_tm_format(nfm_obj, res_buf, res_buf_len, offset))) {
        LOG_WARN("fail to process tm fmt", K(ret));
      }
    } else if (ObNFMElem::has_type(NFM_TME_FLAG, fmt_desc_.elem_flag_)) {
      if (OB_FAIL(process_tme_format(nfm_obj, res_buf, res_buf_len, offset))) {
        LOG_WARN("fail to process tme fmt", K(ret));
      }
    } else {
      // the number of digits after the decimal point means how many decimal places are reserved
      // eg: to_char(123.123, '999.99') --> 123.12
      // the value is rounded to two decimal places
      const int32_t scale = fmt_desc_.post_num_count_;
      if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc_.elem_flag_)) {
        if (OB_FAIL(process_mul_format(nfm_obj, num_str))) {
          LOG_WARN("fail to process mul format", K(ret));
        }
      } else {
        if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
          if (OB_FAIL(cast_obj_to_num_str(nfm_obj, -1, num_str))) {
            LOG_WARN("fail to cast obj to num str", K(ret));
          }
        } else {
          if (OB_FAIL(cast_obj_to_num_str(nfm_obj, scale, num_str))) {
            LOG_WARN("fail to cast obj to num str", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
          if (OB_FAIL(process_sci_format(num_str, scale, num_str))) {
            LOG_WARN("fail to process sci fmt", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // if the length of the integer part exceeds the number of the integer part in fmt str
          // fill with '#'
          // eg: to_char(123.12, '99.99') --> ######
          if (OB_FAIL(get_integer_part_len(num_str, integer_part_len))) {
            LOG_WARN("fail to get num str pre num count", K(ret));
          } else if (!is_zero(num_str) && integer_part_len > fmt_desc_.pre_num_count_) {
            is_overflow = true;
            if (OB_FAIL(fill_str(res_buf, res_buf_len, 0, '#', fmt_desc_.output_len_))) {
              LOG_WARN("fail to fill str", K(ret));
            } else {
              offset = fmt_desc_.output_len_;
            }
          }
        }
      }
      if (!is_overflow) {
        if (OB_SUCC(ret) && ObNFMElem::has_currency_group(fmt_desc_.elem_flag_)
            && OB_FAIL(get_nls_currency(session))) {
          LOG_WARN("fail to get nls currency", K(ret));
        }
        if (OB_SUCC(ret) && ObNFMElem::has_iso_grouping_group(fmt_desc_.elem_flag_)
            && OB_FAIL(get_iso_grouping(session))) {
          LOG_WARN("fail to get iso grouping", K(ret));
        }
        if (OB_SUCC(ret) && OB_FAIL(process_output_fmt(num_str, integer_part_len,
                                    res_buf, res_buf_len, offset))) {
          LOG_WARN("fail to processor fmt print", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObNFMToChar::convert_num_to_fmt_str(const ObObj &obj,
                                        const char *fmt_str,
                                        const int32_t fmt_len,
                                        ObExprCtx &expr_ctx,
                                        ObString &res_str)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  char *res_buf = NULL;
  ObNFMObj nfm_obj;
  ObSQLSessionInfo *session = expr_ctx.my_session_;
  const int64_t res_buf_len = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  if (OB_ISNULL(fmt_str) || OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(expr_ctx.calc_buf_),
             K(session));
  } else if (fmt_len >= MAX_FMT_STR_LEN) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("invalid fmt string", K(ret));
  } else if (OB_ISNULL(res_buf = static_cast<char *>(
                       expr_ctx.calc_buf_->alloc(res_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(conv_num_to_nfm_obj(obj, expr_ctx, nfm_obj))) {
    LOG_WARN("fail to conv obj to nfm obj", K(ret));
  } else if (OB_FAIL(process_fmt_conv(*session, fmt_str, fmt_len, nfm_obj,
                                      res_buf, res_buf_len, offset))) {
    LOG_WARN("fail to process fmt conversion", K(ret), K(fmt_len));
  } else {
    res_str.assign_ptr(res_buf, offset);
  }
  return ret;
}

int ObNFMToChar::calc_result_length(const common::ObObj &obj, int32_t &length)
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  ObExprCtx expr_ctx;
  expr_ctx.calc_buf_ = &allocator_;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  ObString fmt_str;
  EXPR_GET_VARCHAR_V2(obj, fmt_str);
  if (OB_FAIL(ret)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input format. need varchar.", K(ret), K(obj));
  } else if (OB_FAIL(parse_fmt(fmt_str.ptr(), fmt_str.length(), need_check))) {
    LOG_WARN("fail to parse fmt model", K(ret), K(fmt_str));
  } else {
    length = fmt_desc_.output_len_;
  }
  return ret;
}

int ObNFMToChar::convert_num_to_fmt_str(const common::ObObjMeta &obj_meta,
                                        const common::ObDatum &obj,
                                        common::ObIAllocator &alloc,
                                        const char *fmt_str, const int32_t fmt_len,
                                        ObEvalCtx &ctx,
                                        common::ObString &res_str)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  char *res_buf = NULL;
  ObNFMObj nfm_obj;
  const int64_t res_buf_len = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(fmt_str) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(session));
  } else if (fmt_len >= MAX_FMT_STR_LEN) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("invalid fmt string", K(ret));
  } else if (OB_ISNULL(res_buf = static_cast<char *>(alloc.alloc(res_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(conv_num_to_nfm_obj(obj_meta, obj, nfm_obj, alloc))) {
    LOG_WARN("fail to conv obj to nfm obj", K(ret));
  } else if (OB_FAIL(process_fmt_conv(*session, fmt_str, fmt_len, nfm_obj,
                                      res_buf, res_buf_len, offset))) {
    LOG_WARN("fail to process fmt conversion", K(ret), K(fmt_len));
  } else {
    res_str.assign_ptr(res_buf, offset);
  }
  return ret;
}

int ObNFMToNumber::process_hex_format(const common::ObString &in_str,
                                      number::ObNumber &res_num,
                                      common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const char *str = in_str.ptr();
  int32_t str_len = in_str.length();
  int32_t str_pos = 0;
  HexNumber nums;
  // remove leading spaces
  // eg: to_number(' fff', 'xxx') --> 4095
  // eg: to_number('fff ', 'xxxx') --> invalid number
  while (str_pos < str_len && isspace(str[str_pos])) {
    ++str_pos;
  }
  if (ObNFMElem::has_type(NFM_ZERO_FLAG, fmt_desc_.elem_flag_)) {
    if (str_len - str_pos != fmt_desc_.pre_num_count_ + fmt_desc_.elem_x_count_) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("fmt does not match", K(ret), K(in_str));
    }
  } else {
    if (str_len - str_pos > fmt_desc_.elem_x_count_) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("fmt does not match", K(ret), K(in_str));
    }
  }
  if (OB_FAIL(ret)) {
  } else if ('-' == str[str_pos]) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("not support negative number", K(ret), K(in_str));
  } else if (ObNFMElem::has_type((~NFM_HEX_FLAG) & (~NFM_ZERO_FLAG)
                                  & (~NFM_NINE_FLAG) & (~NFM_FILLMODE_FLAG),
                                  fmt_desc_.elem_flag_)) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("incompatible with other formats", K(ret), K(in_str));
  } else if (OB_FAIL(check_hex_str_valid(str, str_len, str_pos))) {
    LOG_WARN("invalid hex character", K(ret), K(in_str));
  } else if (OB_FAIL(build_hex_number(str, str_len, str_pos, nums))) {
    LOG_WARN("fail to build hex number", K(ret), K(in_str));
  } else {
    int32_t hex_num_size = nums.count();
    if (1 == hex_num_size) {
      if (OB_FAIL(res_num.from(nums.at(0), allocator))) {
        LOG_WARN("failed to cast obj as number", K(ret));
      }
    } else {
      const uint64_t base = 16;
      const uint64_t exponent = 15;
      number::ObNumber base_num;
      number::ObNumber power_num;
      number::ObNumber res;
      if (base_num.from(base, allocator)) {
        LOG_WARN("fail to cast uint64 to number", K(ret), K(base));
      } else if (OB_FAIL(base_num.power(exponent, power_num, allocator))) {
        LOG_WARN("power calc failed", K(ret));
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < hex_num_size; i++) {
          uint64_t val = nums.at(i);
          number::ObNumber num;
          if (num.from(val, allocator)) {
            LOG_WARN("fail to cast uint64 to number", K(ret), K(val));
          } else {
            for (int32_t j = i; OB_SUCC(ret) && j > 0; j--) {
              if (num.mul_v2(power_num, num, allocator)) {
                LOG_WARN("fail to mul number", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(res.add(num, res, allocator))) {
              LOG_WARN("fail to add number", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          res_num = res;
        }
      }
    }
  }
  return ret;
}

int ObNFMToNumber::process_output_fmt(const ObString &in_str,
                                      const int32_t integer_part_len,
                                      common::ObString &num_str)
{
  int ret = OB_SUCCESS;
  const char *str = in_str.ptr();
  int32_t str_len = in_str.length();
  int32_t str_pos = 0;
  int64_t offset = 0;
  bool is_negative = false;
  char *buf = NULL;
  const int64_t buf_len = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  // skip leading spaces
  while (str_pos < str_len && ' ' == str[str_pos]) {
    ++str_pos;
  }
  // reserve the positive and negative sign position
  ++offset;
  // eg: to_number(' ', 'B') --> 0
  if (str_pos == str_len
      && (!ObNFMElem::has_type(NFM_BLANK_FLAG, fmt_desc_.elem_flag_)
      || str_len != fmt_elem_list_.count())) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WARN("invalid number", K(ret), K(in_str));
  } else if (OB_ISNULL(buf = static_cast<char *>(
                allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    // processing leading positive or negative sign element
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::FIRST_POS == fmt_desc_.sign_appear_pos_) {
      if (str_pos >= str_len) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      } else if ('-' == str[str_pos]) {
        is_negative = true;
        ++str_pos;
      } else if ('+' == str[str_pos]) {
        ++str_pos;
      } else {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (str_pos < str_len) {
        if ('<' == str[str_pos]) {
          is_negative = true;
          ++str_pos;
        }
      }
    }
  } else {
    if (str_pos < str_len) {
      if ('-' == str[str_pos]) {
        is_negative = true;
        ++str_pos;
      }
    }
  }
  // processing element '$'
  if (OB_SUCC(ret) && ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc_.elem_flag_)) {
    if (str_pos >= str_len || str[str_pos++] != '$'
       || (0 == fmt_desc_.pre_num_count_ && 0 == fmt_desc_.post_num_count_)) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
    }
  }
  // processing the last positive or negative sign element
  if (OB_SUCC(ret) && ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::LAST_POS == fmt_desc_.sign_appear_pos_) {
      if ('-' == str[str_len - 1]) {
        is_negative = true;
        --str_len;
      } else if ('+' == str[str_len - 1]) {
        --str_len;
      } else {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        if (str[str_len - 1] != '>') {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
        --str_len;
      }
    } else if (ObNFMElem::has_type(NFM_MI_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        if ('-' == str[str_len - 1]) {
          is_negative = true;
          --str_len;
        } else {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
      } else {
        if (' ' == str[str_len - 1]) {
          --str_len;
        } else if ('-' == str[str_len - 1]) {
          is_negative = true;
          --str_len;
        } else if (!ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc_.elem_flag_)) {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // need to skip leading and trailing redundant numbers in the fmt string
    // eg: to_number('123.123', '999999999.999999999')
    // pre_need_skip_num = 6;
    const int32_t pre_need_skip_num = fmt_desc_.pre_num_count_ - integer_part_len;
    int32_t traversed_num_count = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < fmt_elem_list_.count(); ++i) {
      const ObNFMElem *elem_item = fmt_elem_list_.at(i);
      if (OB_ISNULL(elem_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid elem item", K(ret));
      } else {
        ObNFMElem::ElementType elem_type = elem_item->keyword_->elem_type_;
        // skip thousands separator
        // eg: to_number('123,12,12,', '99999999999999') --> 1231212
        // eg: to_number('1,,,.235', '9GGD999') --> 1.235
        if (elem_type != ObNFMElem::NFM_COMMA && elem_type != ObNFMElem::NFM_G) {
          if (traversed_num_count > 0
              && traversed_num_count <= fmt_desc_.pre_num_count_
              && fmt_desc_.last_separator_ < i) {
            while (str_pos < str_len && ((',' == str[str_pos])
                  || (ObNFMElem::has_iso_grouping_group(fmt_desc_.elem_flag_)
                  && str[str_pos] == fmt_desc_.iso_grouping_.ptr()[1]))) {
              ++str_pos;
            }
          }
        }
        switch (elem_type) {
        case ObNFMElem::NFM_COMMA:
          if (traversed_num_count <= pre_need_skip_num
              && ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          } else if (traversed_num_count <= pre_need_skip_num
              && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i)) {
            // skip, do nothing
          } else if (str_pos >= str_len || str[str_pos] != ',') {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),K(str_pos), K(i));
          } else {
            ++str_pos;
          }
          break;
        case ObNFMElem::NFM_PERIOD:
          // ignore decimal point
          // eg: to_number(1265, '0000.000')
          if (str_pos >= str_len) {
            // do nothing
          } else {
            if (str[str_pos] != '.') {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos]);
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_ZERO:
          // if there is fractional part and the number of decimal parts exceeds the precision
          // the following numbers need to be skipped
          // eg: to_number('123.123', '999999999.999999999')
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (fmt_desc_.pre_num_count_ - traversed_num_count > 1
              || (fmt_desc_.decimal_pos_ != INT32_MAX
              && fmt_desc_.decimal_pos_ < i
              && str_pos >= str_len)) {
              // do nothing
            } else if (str_pos >= str_len || !is_digit(str[str_pos])) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
            }
          } else {
            if (fmt_desc_.decimal_pos_ != INT32_MAX
                && fmt_desc_.decimal_pos_ < i
                && str_pos >= str_len) {
              // do nothing
            } else if (str_pos >= str_len || !is_digit(str[str_pos])) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              if (traversed_num_count < pre_need_skip_num) {
                str_pos++;
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_NINE:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (fmt_desc_.pre_num_count_ - traversed_num_count > 1
              || (fmt_desc_.decimal_pos_ != INT32_MAX
              && fmt_desc_.decimal_pos_ < i
              && str_pos >= str_len)) {
              // do nothing
            } else {
              if (str_pos >= str_len || !is_digit(str[str_pos])) {
                // do nothing
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          } else {
            // if there is fractional part and the number of decimal parts exceeds the precision
            // the following numbers need to be skipped
            // eg: to_number('123.123', '999999999.999999999')
            if (fmt_desc_.decimal_pos_ != INT32_MAX
                && fmt_desc_.decimal_pos_ < i
                && str_pos >= str_len) {
              // do nothing
            } else if (traversed_num_count < pre_need_skip_num) {
              // if element '0' has appeared, the following numbers need to fill '0'
              // eg: to_char(123.123, '9099999.999') --> 000123.123
              // eg: to_number('000123.123', '9099999.999') --> 123.123
              if ((fmt_desc_.zero_start_ >= 0 && fmt_desc_.zero_start_ < i)) {
                if (str_pos >= str_len || !is_digit(str[str_pos])) {
                  ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                  LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                  K(str_pos), K(i));
                } else {
                  ++str_pos;
                }
              }
            } else {
              if (str_pos >= str_len || !is_digit(str[str_pos])) {
                ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                K(str_pos), K(i));
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_D:
          // ignore decimal point
          // eg: to_number(1265, '0000.000')
          if (str_pos >= str_len) {
            // do nothing
          } else {
            if (str[str_pos] != fmt_desc_.iso_grouping_.ptr()[0]) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos]);
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_G:
          if (traversed_num_count <= pre_need_skip_num
              && ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          } else if (traversed_num_count <= pre_need_skip_num
              && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i)) {
            // skip, do nothing
          } else {
            if (str_pos >= str_len || str[str_pos] != fmt_desc_.iso_grouping_.ptr()[1]) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_C:
        case ObNFMElem::NFM_L:
        case ObNFMElem::NFM_U:
          if (OBNFMDesc::MIDDLE_POS == fmt_desc_.currency_appear_pos_) {
            if (str_pos >= str_len) {
              // do nothing
            } else {
              if ((str[str_pos] != '.'
                  && (str_len - str_pos < fmt_desc_.nls_currency_.length()
                  || (0 != strncasecmp(str + str_pos, fmt_desc_.nls_currency_.ptr(),
                  fmt_desc_.nls_currency_.length()))))
                  || (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
                  && OBNFMDesc::LAST_POS == fmt_desc_.sign_appear_pos_)) {
                // eg: to_number('23$00+', '99L99S') --> error 1722
                ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                K(str_pos), K(i));
              } else {
                if (str[str_pos] == '.') {
                  ++str_pos;
                } else {
                  str_pos += fmt_desc_.nls_currency_.length();
                }
                // if the element('C', 'L', 'U') appears in the middle, it will be regarded as the
                // decimal point, so use decimal point to replace the iso currency
                ret = databuff_printf(buf, buf_len, offset, "%c", '.');
              }
            }
          } else {
            if (str_pos >= str_len || str_len - str_pos < fmt_desc_.nls_currency_.length()
              || (0 != strncasecmp(str + str_pos, fmt_desc_.nls_currency_.ptr(),
              fmt_desc_.nls_currency_.length()))) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              str_pos += fmt_desc_.nls_currency_.length();
            }
          }
          break;
        case ObNFMElem::NFM_EEEE:
          if (str_pos < str_len && str[str_pos] != 'e' && str[str_pos] != 'E') {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WARN("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          }
          // eg: to_number('1.2E+03 ', '9.9EEEEMI')
          while (str_pos < str_len && str[str_pos] != ' ') {
            buf[offset++] = str[str_pos++];
          }
          break;
        default:
          break;
        }
      }
    }
  }
  // eg: to_number('123.', '999') --> 123
  // eg: to_number('123,', '999') --> 123
  for (; OB_SUCC(ret) && str_pos < str_len; ++str_pos) {
    if (str[str_pos] != ',' && str[str_pos] != '.') {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("fmt does not match", K(ret), K(str_pos), K(str_len), K(in_str), K_(fmt_str));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_negative) {
      buf[0] = '-';
    } else {
      buf[0] = '+';
    }
    // if string is empty, set num_str to '+0' or '-0' as a valid number.
    if (offset == 1) {
      buf[offset++] = '0';
    }
    num_str.assign(buf, offset);
  }
  return ret;
}

int ObNFMToNumber::process_fmt_conv(const ObSQLSessionInfo &session,
                                    const common::ObString &in_str,
                                    const common::ObString &in_fmt_str,
                                    common::ObIAllocator &alloc,
                                    common::number::ObNumber &res_num)
{
  int ret = OB_SUCCESS;
  int32_t integer_part_len = 0;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  ObString num_str;
  if (OB_FAIL(parse_fmt(in_fmt_str.ptr(), in_fmt_str.length()))) {
    LOG_WARN("fail to parse format model", K(ret), K(in_fmt_str));
  } else if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_TME_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc_.elem_flag_)) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WARN("not support elem type", K(ret));
  } else if (OB_FAIL(get_integer_part_len(in_str, integer_part_len))) {
    LOG_WARN("fail to get integer part len", K(ret));
  } else if (ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc_.elem_flag_)) {
    if (OB_FAIL(process_hex_format(in_str, res_num, alloc))) {
      LOG_WARN("fail to process hex format", K(ret));
    }
  } else {
    // here are two cases, return invalid number
    // eg: to_number('1.23E+03', '9.9EEEE') invalid number
    // eg: to_number('123.12', '99.99') invalid number
    if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
      if (fmt_desc_.pre_num_count_ <= 0) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("fmt does not match", K(ret), K(in_str), K(in_fmt_str));
      }
    } else {
      if (integer_part_len > fmt_desc_.pre_num_count_) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WARN("overflowed digit format", K(ret), K(integer_part_len),
                K(fmt_desc_.pre_num_count_), K(fmt_desc_.post_num_count_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (ObNFMElem::has_currency_group(fmt_desc_.elem_flag_)
               && OB_FAIL(get_nls_currency(session))) {
      LOG_WARN("fail to get nls currency", K(ret));
    } else if (ObNFMElem::has_iso_grouping_group(fmt_desc_.elem_flag_)
               && OB_FAIL(get_iso_grouping(session))) {
      LOG_WARN("fail to get iso grouping", K(ret));
    } else if (OB_FAIL(process_output_fmt(in_str, integer_part_len, num_str))) {
      LOG_WARN("fail to process output fmt", K(ret));
    } else {
      if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
        if (OB_FAIL(res_num.from_sci_opt(num_str.ptr(), num_str.length(), alloc,
                                         &res_precision, &res_scale))) {
          LOG_WARN("fail to calc function to_number with", K(ret), K(num_str));
        }
      } else {
        if (OB_FAIL(res_num.from(num_str.ptr(), num_str.length(), alloc, NULL,
                                 &res_precision, &res_scale))) {
          LOG_WARN("fail to calc function to_number with", K(ret), K(num_str));
        }
      }
    }
  }
  return ret;
}

int ObNFMToNumber::convert_char_to_num(const common::ObString &in_str,
                                       const common::ObString &in_fmt_str,
                                       common::ObExprCtx &expr_ctx,
                                       common::number::ObNumber &res_num)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = expr_ctx.my_session_;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session));
  } else if (in_fmt_str.length() >= MAX_FMT_STR_LEN) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("invalid format string", K(ret));
  } else if (OB_FAIL(process_fmt_conv(*session, in_str, in_fmt_str,
                                      *(expr_ctx.calc_buf_), res_num))) {
    LOG_WARN("fail to parse format model", K(ret), K(in_fmt_str));
  }
  return ret;
}

int ObNFMToNumber::convert_char_to_num(const common::ObString &in_str,
                                       const common::ObString &in_fmt_str,
                                       common::ObIAllocator &alloc,
                                       ObEvalCtx &ctx,
                                       common::number::ObNumber &res_num)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(session));
  } else if (in_fmt_str.length() >= MAX_FMT_STR_LEN) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("invalid format string", K(ret));
  } else if (OB_FAIL(process_fmt_conv(*session, in_str, in_fmt_str,
                                      alloc, res_num))) {
    LOG_WARN("fail to parse format model", K(ret), K(in_fmt_str));
  }
  return ret;
}
}
}
