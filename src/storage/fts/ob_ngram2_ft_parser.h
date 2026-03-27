/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_
#define _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_

#include "storage/fts/utils/ob_ft_ngram_impl.h"
#include "storage/fts/ob_i_ft_parser.h"

namespace oceanbase
{
namespace storage
{
class ObNgram2FTParser final : public ObIFTParser
{
public:
  ObNgram2FTParser();
  virtual ~ObNgram2FTParser();

  int init(plugin::ObFTParserParam *param);
  void reset();
  virtual int get_next_token(const char *&word,
                             int64_t &word_len,
                             int64_t &char_len,
                             int64_t &word_freq) override;
  virtual int reuse_parser(const char *fulltext, const int64_t fulltext_len) override;

  VIRTUAL_TO_STRING_KV(K_(is_inited));

private:
  ObFTNgramImpl ngram_impl_;
  bool is_inited_;

private:
  DISABLE_COPY_ASSIGN(ObNgram2FTParser);
};

class ObNgram2FTParserDesc final : public plugin::ObIFTParserDesc
{
public:
  ObNgram2FTParserDesc();
  virtual ~ObNgram2FTParserDesc() = default;
  virtual int init(plugin::ObPluginParam *param) override;
  virtual int deinit(plugin::ObPluginParam *param) override;
  virtual int segment(plugin::ObFTParserParam *param,
                      plugin::ObITokenIterator *&iter) const override;
  virtual void free_token_iter(plugin::ObFTParserParam *param,
                               plugin::ObITokenIterator *&iter) const override;
  virtual int get_add_word_flag(ObProcessTokenFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }

private:
  bool is_inited_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_
