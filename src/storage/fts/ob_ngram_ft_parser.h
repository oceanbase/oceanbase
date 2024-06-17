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

#include "lib/ob_plugin.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

class ObNgramFTParser final : public lib::ObITokenIterator
{
public:
  static const int64_t NGRAM_TOKEN_SIZE = 2; // TODO: @jinzhu, please apply one system variable later, and keep the same as mysql.
public:
  ObNgramFTParser();
  virtual ~ObNgramFTParser();

  int init(lib::ObFTParserParam *param);
  void reset();
  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_len,
      int64_t &word_freq) override;

  VIRTUAL_TO_STRING_KV(KP_(cs), KP_(start), KP_(next), KP_(end), K_(is_inited));
private:
  const ObCharsetInfo *cs_;
  const char *start_;
  const char *next_;
  const char *end_;
  int64_t c_nums_;
  bool is_inited_;
private:
  DISABLE_COPY_ASSIGN(ObNgramFTParser);
};

class ObNgramFTParserDesc final : public lib::ObIFTParserDesc
{
public:
  ObNgramFTParserDesc();
  virtual ~ObNgramFTParserDesc() = default;
  virtual int init(lib::ObPluginParam *param) override;
  virtual int deinit(lib::ObPluginParam *param) override;
  virtual int segment(lib::ObFTParserParam *param, lib::ObITokenIterator *&iter) const override;
  virtual void free_token_iter(lib::ObFTParserParam *param, lib::ObITokenIterator *&iter) const override;
  OB_INLINE void reset() { is_inited_ = false; }
private:
  bool is_inited_;
};

static ObNgramFTParserDesc ngram_parser;

} // end namespace storage
} // end namespace oceanbase

#endif // OB_NGRAM_FT_PARSER_H_
