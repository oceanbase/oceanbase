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
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ob_i_ft_parser.h"
#include "share/rc/ob_tenant_base.h"

#include <cstdint>
namespace oceanbase
{
namespace storage
{
class ObFTDictHub;

class ObIKFTParser final : public ObIFTParser
{
public:
  ObIKFTParser(ObIAllocator &metadata_alloc, ObFTDictHub *hub)
      : is_inited_(false),
        metadata_alloc_(metadata_alloc),
        coll_type_(ObCollationType::CS_TYPE_INVALID),
        ctx_(nullptr),
        hub_(hub),
        segmenters_(metadata_alloc),
        cache_main_(metadata_alloc),
        cache_quan_(metadata_alloc),
        cache_stop_(metadata_alloc),
        dict_main_(nullptr),
        dict_quan_(nullptr),
        dict_stop_(nullptr),
        arb_(),
        scratch_alloc_()
  {
    const int64_t tenant_id = MTL_ID() == OB_INVALID_TENANT_ID ? OB_SERVER_TENANT_ID : MTL_ID();
    scratch_alloc_.set_attr(common::ObMemAttr(tenant_id, "ft_segment_data"));
  }

  virtual ~ObIKFTParser() { reset(); }

  int init(const plugin::ObFTParserParam &param);

  int get_next_token(const char *&word,
                     int64_t &word_len,
                     int64_t &char_cnt,
                     int64_t &word_freq) override;

  virtual int reuse_parser(const char *fulltext, const int64_t fulltext_len) override;

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

  int build_dict_from_cache(const ObFTDictDesc &desc,
                            ObFTCacheRangeContainer &container,
                            ObIFTDict *&dict);

private:
  bool is_inited_;
  ObIAllocator &metadata_alloc_;

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
  ObIKArbitrator arb_;
  ObArenaAllocator scratch_alloc_;

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
  virtual int get_add_word_flag(ObProcessTokenFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }

private:
  bool is_inited_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_IK_FT_PARSER_H_
