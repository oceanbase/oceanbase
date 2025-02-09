/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_IK_FT_PARSER_H_
#define _OCEANBASE_STORAGE_FTS_OB_IK_FT_PARSER_H_

#include "lib/allocator/ob_allocator.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"

#include <cstdint>
namespace oceanbase
{
namespace storage
{
class ObFTDictHub;

class ObIKFTParser final : public plugin::ObITokenIterator
{
public:
  ObIKFTParser(ObIAllocator &allocator, ObFTDictHub *hub)
      : allocator_(allocator), is_inited_(false), ctx_(nullptr), hub_(hub), segmenters_(allocator_),
        cache_main_(allocator), cache_quan_(allocator), cache_stop_(allocator), dict_main_(nullptr),
        dict_quan_(nullptr), dict_stop_(nullptr)
  {
  }

  virtual ~ObIKFTParser() { reset(); }

  int init(const plugin::ObFTParserParam &param);

  int get_next_token(const char *&word,
                     int64_t &word_len,
                     int64_t &char_cnt,
                     int64_t &word_freq) override;

  VIRTUAL_TO_STRING_KV(K(is_inited_));

private:
  int produce();

  int process_next_batch();

  int process_one_char(TokenizeContext &ctx,
                       const char *ch,
                       const uint8_t char_len,
                       const ObFTCharUtil::CharType type);

private:
  int init_dict(const plugin::ObFTParserParam &param);

  int init_single_dict(ObFTDictDesc desc, ObFTCacheRangeContainer &container);

  int init_segmenter(const plugin::ObFTParserParam &param);

  int init_ctx(const plugin::ObFTParserParam &param);

  void reset();

  bool should_read_newest_table() const;

  int build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  int load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  int build_dict_from_cache(const ObFTDictDesc &desc,
                            ObFTCacheRangeContainer &container,
                            ObIFTDict *&dict);

private:
  static constexpr int SEGMENT_LIMIT = 1000;
  ObIAllocator &allocator_;
  bool is_inited_;

  ObCollationType coll_type_;
  TokenizeContext *ctx_;
  ObFTDictHub *hub_;
  ObList<ObIIKProcessor *, ObIAllocator> segmenters_;

  // For now there's no change of dict in one query, so we can pin dict this level.
  ObFTCacheRangeContainer cache_main_;
  ObFTCacheRangeContainer cache_quan_;
  ObFTCacheRangeContainer cache_stop_;

  ObIFTDict *dict_main_;
  ObIFTDict *dict_quan_;
  ObIFTDict *dict_stop_;

  DISABLE_COPY_ASSIGN(ObIKFTParser);
};

class ObIKFTParserDesc final : public plugin::ObIFTParserDesc
{
public:
  ObIKFTParserDesc() {}
  virtual ~ObIKFTParserDesc() = default;
  virtual int init(plugin::ObPluginParam *param) override;
  virtual int deinit(plugin::ObPluginParam *param) override;
  virtual int segment(plugin::ObFTParserParam *param, plugin::ObITokenIterator *&iter) const override;
  virtual void free_token_iter(plugin::ObFTParserParam *param,
                               plugin::ObITokenIterator *&iter) const override;
  virtual int get_add_word_flag(ObAddWordFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }

private:
  bool is_inited_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_IK_FT_PARSER_H_
