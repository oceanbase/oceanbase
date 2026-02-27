/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/fts/ob_ft_token_processor.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace storage
{
/**
* -----------------------------------ObFTTokenProcessor-----------------------------------
*/
int ObFTTokenProcessor::init(const ObFTParserProperty &property,
                             const ObObjMeta &meta,
                             const ObProcessTokenFlag &flag,
                             ObFTTokenMap *token_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObFTTokenProcessor has been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == token_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(token_map));
  } else if (OB_FAIL(ObFTParsePluginData::instance().get_stop_token_checker(meta.get_collation_type(),
                                                                            stop_token_checker_))) {
    LOG_WARN("fail to get stop token checker by coll", K(ret), K(meta));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(meta.get_type(), meta.get_collation_type());
    ObDatumCmpFuncType cmp_func = get_datum_cmp_func(meta, meta);
    token_meta_ = meta;
    token_map_ = token_map;
    min_max_token_cnt_ = 0;
    non_stop_token_cnt_ = 0;
    stop_token_cnt_ = 0;
    min_token_size_ = property.min_token_size_;
    max_token_size_ = property.max_token_size_;
    flag_ = flag;

    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get basic funcs or cmp func",
          K(ret), K(token_meta_), KP(basic_funcs), KP(cmp_func));
    } else if (OB_UNLIKELY(nullptr == basic_funcs->default_hash_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the default hash is null", K(ret));
    } else {
      hash_func_ = basic_funcs->default_hash_;
      cmp_func_ = cmp_func;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObFTTokenProcessor::reset()
{
  if (OB_LIKELY(is_inited_)) {
    token_map_->destroy();
    token_meta_.reset();
    stop_token_cnt_ = 0;
    non_stop_token_cnt_ = 0;
    min_max_token_cnt_ = 0;
    min_token_size_ = 0;
    max_token_size_ = 0;
    flag_.reset();
    hash_func_ = nullptr;
    cmp_func_ = nullptr;
    is_inited_ = false;
  }
}

void ObFTTokenProcessor::reuse()
{
  stop_token_cnt_ = 0;
  non_stop_token_cnt_ = 0;
}

int ObFTTokenProcessor::process_token(
    const bool need_pos_list,
    const char *token,
    const int64_t token_len,
    const int64_t char_cnt,
    const int64_t position)
{
  int ret = OB_SUCCESS;
  bool is_stop_token = false;
  ObFTToken src_token;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObFTTokenProcessor hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == token || 0 >= token_len || 0 >= char_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid arguments", K(ret), KP(token), K(token_len), K(char_cnt));
  } else if (OB_FAIL(src_token.init(token, token_len, token_meta_, hash_func_, cmp_func_))) {
    LOG_WARN("fail to initialize src token", K(ret), K(token), K(token_len), K(token_meta_));
  } else if (OB_UNLIKELY(is_min_max_token(char_cnt))) {
    ++min_max_token_cnt_;
    LOG_DEBUG("skip too small or large token", K(ret), K(src_token), K(char_cnt));
  } else if (flag_.stop_token() && OB_FAIL(stop_token_checker_.check_is_stop_token(src_token, is_stop_token))) {
    LOG_WARN("fail to check stop token", K(ret), K(src_token));
  } else if (OB_UNLIKELY(is_stop_token)) {
    ++stop_token_cnt_;
    LOG_DEBUG("skip stop token", K(ret), K(src_token));
  } else if (OB_FAIL(groupby_token(need_pos_list, src_token, position))) {
    LOG_WARN("fail to groupby token", K(ret), K(src_token), K(position));
  } else {
    non_stop_token_cnt_ += 1;
    LOG_DEBUG("process non stop token", K(ret), KP(token), K(token_len), K(char_cnt), K(position), K(src_token));
  }
  return ret;
}

inline bool ObFTTokenProcessor::is_min_max_token(const int64_t c_len) const
{
  return  (c_len > MAX_CHAR_COUNT_PER_TOKEN) || (flag_.min_max_token() && (c_len < min_token_size_ || c_len > max_token_size_));
}

int ObFTTokenProcessor::groupby_token(const bool need_pos_list, const ObFTToken &token, const int64_t position)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(flag_.groupby_token())) {
    if (need_pos_list) {
      UpdateTokenCallBack update_token_func(scratch_allocator_, position);
      ObFTTokenInfo token_info;
      if (OB_FAIL(token_info.update_one_position(scratch_allocator_, position))) {
        LOG_WARN("fail to update one position", K(ret), K(token), K(position));
      } else if (OB_FAIL(token_map_->set_or_update(token, token_info, update_token_func))) {
        LOG_WARN("fail to set fulltext token and count", K(ret), K(token), K(position));
      }
    } else {
      UpdateTokenWithoutPosListCallBack update_token_func;
      ObFTTokenInfo token_info;
      if (OB_FAIL(token_info.update_without_pos_list())) {
        LOG_WARN("fail to update without pos list", K(ret), K(token));
      } else if (OB_FAIL(token_map_->set_or_update(token, token_info, update_token_func))) {
        LOG_WARN("fail to set fulltext token and count", K(ret), K(token));
      }
    }
  } else {
    // do nothing now
  }
  return ret;
}

int ObFTTokenProcessor::UpdateTokenCallBack::operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pair.second.update_one_position(allocator_, position_))) {
    LOG_WARN("Fail to update one position", K(ret), K(position_), K(pair.first), K(pair.second));
  }
  return ret;
}

int ObFTTokenProcessor::UpdateTokenWithoutPosListCallBack::operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pair.second.update_without_pos_list())) {
    LOG_WARN("Fail to update without pos list", K(ret), K(pair.first), K(pair.second));
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase
