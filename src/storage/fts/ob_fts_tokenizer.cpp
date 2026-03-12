/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_fts_tokenizer.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

int ObFTSTokenizer::tokenize(
    ObIAllocator &alloc,
    const ObString &query_text,
    const ObString &parser_name,
    const ObString &parser_properties,
    const ObObjMeta &meta,
    ObIArray<ObString> &query_tokens)
{
  int ret = OB_SUCCESS;
  if (!query_text.empty()) {
    ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "Tkzn"));
    ObFTTokenMap token_map;
    if (OB_FAIL(segment(tmp_allocator, query_text, parser_name, parser_properties, meta,
        share::schema::OB_FTS_INDEX_TYPE_MATCH, token_map))) {
      LOG_WARN("failed to segment", K(ret));
    }
    for (ObFTTokenMap::const_iterator iter = token_map.begin();
         OB_SUCC(ret) && iter != token_map.end();
         ++iter) {
      const ObFTToken &token = iter->first;
      const ObFTTokenInfo &info = iter->second;
      ObString token_string;
      if (OB_FAIL(ob_write_string(alloc, token.get_token().get_string(), token_string))) {
        LOG_WARN("failed to deep copy query token", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < info.count_; ++i) {
        if (OB_FAIL(query_tokens.push_back(token_string))) {
          LOG_WARN("failed to append query token", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObFTSTokenizer::tokenize(
    ObIAllocator &alloc,
    const ObString &query_text,
    const ObString &parser_name,
    const ObString &parser_properties,
    const ObObjMeta &meta,
    ObIArray<ObString> &query_tokens,
    ObIArray<int64_t> &token_ids,
    ObIArray<int64_t> &token_positions,
    bool &has_duplicate_tokens)
{
  int ret = OB_SUCCESS;
  if (!query_text.empty()) {
    ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "Tkzn"));
    ObFTTokenMap token_map;
    if (OB_FAIL(segment(tmp_allocator, query_text, parser_name, parser_properties, meta,
        share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH, token_map))) {
      LOG_WARN("failed to segment", K(ret));
    }
    has_duplicate_tokens = false;
    for (ObFTTokenMap::const_iterator iter = token_map.begin();
         OB_SUCC(ret) && iter != token_map.end();
         ++iter) {
      const ObFTToken &token = iter->first;
      const ObFTTokenInfo &info = iter->second;
      ObString token_string;
      int64_t token_id = query_tokens.count();
      if (OB_FAIL(ob_write_string(alloc, token.get_token().get_string(), token_string))) {
        LOG_WARN("failed to deep copy query token", KR(ret));
      } else if (OB_FAIL(query_tokens.push_back(token_string))) {
        LOG_WARN("failed to append query token", KR(ret));
      } else if (info.position_list_->count() > 1) {
        has_duplicate_tokens = true;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < info.position_list_->count(); ++i) {
        if (OB_FAIL(token_ids.push_back(token_id))) {
          LOG_WARN("failed to append token id", K(ret));
        } else if (OB_FAIL(token_positions.push_back(info.position_list_->at(i)))) {
          LOG_WARN("failed to append token offset", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObFTSTokenizer::segment(
    ObIAllocator &alloc,
    const ObString &query_text,
    const ObString &parser_name,
    const ObString &parser_properties,
    const ObObjMeta &meta,
    const share::schema::ObFTSIndexType fts_index_type,
    ObFTTokenMap &token_map)
{
  int ret = OB_SUCCESS;
  storage::ObFTParseHelper tokenize_helper;
  const int64_t ft_word_bkt_cnt = MAX(query_text.length() / 10, 2);
  int64_t doc_length = 0;
  if (OB_FAIL(tokenize_helper.init(&alloc, parser_name, parser_properties, fts_index_type))) {
    LOG_WARN("failed to init tokenize helper", KR(ret));
  } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, ObMemAttr(MTL_ID(), "FTWordMap")))) {
    LOG_WARN("failed to create token map", KR(ret));
  } else if (OB_FAIL(tokenize_helper.segment(
      meta, query_text.ptr(), query_text.length(), doc_length, token_map))) {
    LOG_WARN("failed to segment", KR(ret), K(query_text), K(meta), K(doc_length));
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase