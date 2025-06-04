/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_
#define _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_

#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "storage/fts/utils/ob_ft_ngram_impl.h"

namespace oceanbase
{
namespace storage
{
class ObNgram2FTParser final : public plugin::ObITokenIterator
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
  virtual int get_add_word_flag(ObAddWordFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }

private:
  bool is_inited_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_NGRAM2_FT_PARSER_H_
