/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_BENG_FT_PARSER_H_
#define OB_BENG_FT_PARSER_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/text_analysis/ob_text_analyzer.h"
#include "storage/fts/ob_i_ft_parser.h"

namespace oceanbase
{
namespace storage
{

class ObBEngFTParser final : public ObIFTParser
{
public:
  static const int64_t FT_MIN_WORD_LEN = 3;
  static const int64_t FT_MAX_WORD_LEN = 84;
public:
  ObBEngFTParser(common::ObIAllocator &metadata_alloc, common::ObIAllocator &scratch_alloc)
    : metadata_alloc_(metadata_alloc),
      scratch_alloc_(scratch_alloc),
      analysis_ctx_(),
      english_analyzer_(),
      doc_(),
      token_stream_(nullptr),
      is_inited_(false)
  {}
  ~ObBEngFTParser() { reset(); }

  int init(plugin::ObFTParserParam *param);
  void reset();
  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_len,
      int64_t &word_freq) override;
  virtual int reuse_parser(const char *fulltext, const int64_t fulltext_len) override;

  VIRTUAL_TO_STRING_KV(K_(analysis_ctx), K_(english_analyzer), KP_(token_stream), K_(is_inited));
private:
  int segment(
      const common::ObDatum &doc,
      share::ObIFTTokenStream *&token_stream);
private:
  common::ObIAllocator &metadata_alloc_;
  common::ObIAllocator &scratch_alloc_;
  share::ObTextAnalysisCtx analysis_ctx_;
  share::ObEnglishTextAnalyzer english_analyzer_;
  common::ObDatum doc_;
  share::ObIFTTokenStream *token_stream_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObBEngFTParser);
};

class ObBasicEnglishFTParserDesc final : public plugin::ObIFTParserDesc
{
public:
  ObBasicEnglishFTParserDesc();
  virtual ~ObBasicEnglishFTParserDesc() = default;
  virtual int init(plugin::ObPluginParam *param) override;
  virtual int deinit(plugin::ObPluginParam *param) override;
  virtual int segment(plugin::ObFTParserParam *param, plugin::ObITokenIterator *&iter) const override;
  virtual void free_token_iter(plugin::ObFTParserParam *param, plugin::ObITokenIterator *&iter) const override;
  virtual int get_add_word_flag(ObProcessTokenFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }
private:
  bool is_inited_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_BENG_FT_PARSER_H_
