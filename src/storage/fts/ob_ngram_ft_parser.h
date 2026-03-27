/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_NGRAM_FT_PARSER_H_
#define OB_NGRAM_FT_PARSER_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/fts/utils/ob_ft_ngram_impl.h"
#include "storage/fts/ob_i_ft_parser.h"

namespace oceanbase
{
namespace storage
{

class ObNgramFTParser final : public ObIFTParser
{
public:
  ObNgramFTParser();
  virtual ~ObNgramFTParser();

  int init(plugin::ObFTParserParam *param);
  void reset();
  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_len,
      int64_t &word_freq) override;
  virtual int reuse_parser(const char *fulltext, const int64_t fulltext_len) override;

  VIRTUAL_TO_STRING_KV(K_(is_inited));

private:
  ObFTNgramImpl ngram_impl_;
  bool is_inited_;
private:
  DISABLE_COPY_ASSIGN(ObNgramFTParser);
};

class ObNgramFTParserDesc final : public plugin::ObIFTParserDesc
{
public:
  ObNgramFTParserDesc();
  virtual ~ObNgramFTParserDesc() = default;
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

#endif // OB_NGRAM_FT_PARSER_H_
