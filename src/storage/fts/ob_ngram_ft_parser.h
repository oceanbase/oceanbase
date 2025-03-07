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

#ifndef OB_NGRAM_FT_PARSER_H_
#define OB_NGRAM_FT_PARSER_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"

namespace oceanbase
{
namespace storage
{

class ObNgramFTParser final : public plugin::ObITokenIterator
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

  VIRTUAL_TO_STRING_KV(KP_(cs), KP_(start), KP_(next), KP_(end), K_(c_nums), K_(ngram_token_size), K_(is_inited));
private:
  const ObCharsetInfo *cs_;
  const char *start_;
  const char *next_;
  const char *end_;
  int64_t c_nums_;
  int64_t ngram_token_size_;
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
  virtual int get_add_word_flag(ObAddWordFlag &flag) const override;
  OB_INLINE void reset() { is_inited_ = false; }
private:
  bool is_inited_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_NGRAM_FT_PARSER_H_
