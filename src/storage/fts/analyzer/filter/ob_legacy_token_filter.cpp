/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/fts/analyzer/filter/ob_legacy_token_filter.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

// ============================================================
// ObLegacyMinMaxTokenFilter
// ============================================================

int ObLegacyMinMaxTokenFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("min max token filter already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_MIN_MAX != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter type", K(ret), K(spec.type_));
  } else {
    const ObLegacyMinMaxTokenFilterSpec &mm_spec = static_cast<const ObLegacyMinMaxTokenFilterSpec &>(spec);
    alloc_ = &alloc;
    coll_type_ = mm_spec.coll_type_;
    min_token_size_ = mm_spec.min_token_size_;
    max_token_size_ = mm_spec.max_token_size_;
    is_inited_ = true;
  }
  return ret;
}

bool ObLegacyMinMaxTokenFilter::is_out_of_range_(const int64_t code_point_cnt) const
{
  return (code_point_cnt > MAX_CHAR_COUNT_PER_TOKEN) ||
         (code_point_cnt < min_token_size_ || code_point_cnt > max_token_size_);
}

int ObLegacyMinMaxTokenFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("min max token filter not initialized", K(ret));
  } else if (OB_ISNULL(input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input stream is null", K(ret));
  } else {
    int32_t pending_pos_inc = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(input_->get_next_token(token))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next token from input stream", K(ret));
        }
      } else {
        const int64_t code_point_cnt = static_cast<int64_t>(
            common::ObCharset::strlen_char(coll_type_, token.token_ptr_, token.token_len_));
        if (is_out_of_range_(code_point_cnt)) {
          pending_pos_inc += token.pos_inc_;
          LOG_DEBUG("skip token outside min/max range", K(token), K(code_point_cnt),
                    K_(min_token_size), K_(max_token_size));
          continue;
        } else {
          token.pos_inc_ += pending_pos_inc;
          break;
        }
      }
    }
  }
  return ret;
}

void ObLegacyMinMaxTokenFilter::reset()
{
  is_inited_ = false;
  alloc_ = nullptr;
  coll_type_ = common::CS_TYPE_INVALID;
  min_token_size_ = 0;
  max_token_size_ = 0;
  input_ = nullptr;
}

// ============================================================
// ObLegacyStopTokenFilter
// ============================================================

int ObLegacyStopTokenFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("stop token filter already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_LEGACY_STOP != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter type", K(ret), K(spec.type_));
  } else {
    const ObLegacyStopTokenFilterSpec &stop_spec = static_cast<const ObLegacyStopTokenFilterSpec &>(spec);
    alloc_ = &alloc;
    token_meta_ = stop_spec.token_meta_;
    coll_type_ = token_meta_.get_collation_type();

    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        token_meta_.get_type(), token_meta_.get_collation_type());
    cmp_func_ = get_datum_cmp_func(token_meta_, token_meta_);
    if (OB_ISNULL(basic_funcs) || OB_ISNULL(cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get basic funcs or cmp func", K(ret), K(token_meta_));
    } else if (OB_ISNULL(basic_funcs->default_hash_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the default hash is null", K(ret));
    } else if (FALSE_IT(hash_func_ = basic_funcs->default_hash_)) {
    } else if (OB_FAIL(ObFTParsePluginData::instance().get_stop_token_checker(coll_type_, stop_checker_))) {
      LOG_WARN("fail to get stop token checker", K(ret), K(coll_type_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLegacyStopTokenFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stop token filter not initialized", K(ret));
  } else if (OB_ISNULL(input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input stream is null", K(ret));
  } else if (OB_UNLIKELY(common::CS_TYPE_INVALID == coll_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collation type not set before get_next_token", K(ret));
  } else {
    int32_t pending_pos_inc = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(input_->get_next_token(token))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next token from input stream", K(ret));
        }
      } else {
        bool is_stop = false;
        ObFTToken ft_token;
        if (OB_FAIL(ft_token.init(token.token_ptr_, token.token_len_,
                                   token_meta_, hash_func_, cmp_func_))) {
          LOG_WARN("fail to init ft token for stop check", K(ret));
        } else if (OB_FAIL(stop_checker_.check_is_stop_token(ft_token, is_stop))) {
          LOG_WARN("fail to check stop token", K(ret));
        } else if (is_stop) {
          pending_pos_inc += token.pos_inc_;
          LOG_DEBUG("skip stop token", K(token));
          continue;
        } else {
          token.pos_inc_ += pending_pos_inc;
          break;
        }
      }
    }
  }
  return ret;
}

void ObLegacyStopTokenFilter::reset()
{
  is_inited_ = false;
  alloc_ = nullptr;
  coll_type_ = common::CS_TYPE_INVALID;
  hash_func_ = nullptr;
  cmp_func_ = nullptr;
  stop_checker_.reset();
  input_ = nullptr;
}

} // namespace storage
} // namespace oceanbase
