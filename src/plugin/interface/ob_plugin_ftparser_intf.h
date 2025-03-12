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

#pragma once

#include "plugin/interface/ob_plugin_intf.h"

struct ObCharsetInfo;

namespace oceanbase
{

namespace storage
{
class ObAddWordFlag;
}
namespace plugin
{

/**
 * The fulltext parser parameter export to plugin
 */
class ObFTParserParamExport
{
public:
  ObFTParserParamExport() = default;
  virtual ~ObFTParserParamExport() = default;

  inline bool is_valid() const
  {
    return nullptr != cs_
        && nullptr != fulltext_
        && 0 < ft_length_
        && 0 <= parser_version_;
  }
  virtual void reset()
  {
    new (this) ObFTParserParamExport();
  }

  VIRTUAL_TO_STRING_KV(KP_(cs),
                       K_(fulltext),
                       K_(ft_length),
                       K_(parser_version),
                       KP_(plugin_param),
                       KP_(user_data));

public:
  const ObCharsetInfo  *cs_             = nullptr;
  const char           *fulltext_       = nullptr;
  int64_t               ft_length_      = 0;
  ObPluginVersion       parser_version_ = 0;
  ObPluginParam        *plugin_param_   = nullptr;
  ObPluginDatum         user_data_      = nullptr;
};

class ObFTIKParam final
{
public:
  enum class Mode : uint8_t
  {
    SMART    = 0,
    MAX_WORD = 1,
  };

  ObFTIKParam(Mode mode = Mode::SMART)
      : mode_(mode), main_dict_(""), quan_dict_(""), stopword_dict_("")
  {
  }

public:
  Mode mode_;
  common::ObString main_dict_;
  common::ObString quan_dict_;
  common::ObString stopword_dict_;
};

/**
 * The fulltext parser parameter used internal
 *
 * @note if you're going to export more data members, move
 * them to the ObFTParserParamExport struct
 */
class ObFTParserParam final : public ObFTParserParamExport
{
public:
  static const int64_t NGRAM_TOKEN_SIZE = 2;
public:
  ObFTParserParam() = default;
  virtual ~ObFTParserParam() { reset(); }

  inline void reset()
  {
    ObFTParserParamExport::reset();
    allocator_ = nullptr;
    ngram_token_size_ = NGRAM_TOKEN_SIZE;
  }

  INHERIT_TO_STRING_KV("base", ObFTParserParamExport, KP_(allocator), K_(ngram_token_size));

public:
  common::ObIAllocator *allocator_ = nullptr;

  // ik parser params
  ObFTIKParam ik_param_;
  int64_t ngram_token_size_ = NGRAM_TOKEN_SIZE;
};

class ObITokenIterator
{
public:
  ObITokenIterator() = default;
  virtual ~ObITokenIterator() = default;
  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_cnt,
      int64_t &word_freq) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

/**
 * fulltext parser descriptor interface for domain index
 * - splitting a document into many tokenizations.
 */
class ObIFTParserDesc : public ObIPluginDescriptor
{
public:
  ObIFTParserDesc() = default;
  virtual ~ObIFTParserDesc() = default;

  /**
   * split fulltext into multiple word segments
   *
   * @param[in]  param, the document to be tokenized and parameters related to word segmentation.
   * @param[out] iter, the tokenized words' iterator.
   *
   * @return error code, such as, OBP_SUCCESS, OBP_INVALID_ARGUMENT, ...
   */
  virtual int segment(ObFTParserParam *param, ObITokenIterator *&iter) const = 0;

  /**
   * get AddWordFlag
   * @details ref to ObAddWordFlag for more details
   * @param[out] flag the AddWordFlag
   */
  virtual int get_add_word_flag(storage::ObAddWordFlag &flag) const = 0;

  /**
   * Release resources held by the iterator and free token iterator.
   * @param[in] param the fulltext parameter
   * @param[out] iter The token iterator which retrieve tokens
   */
  virtual void free_token_iter(ObFTParserParam *param, ObITokenIterator *&iter) const
  {
    if (OB_NOT_NULL(iter)) {
      iter->~ObITokenIterator();
    }
  }
};

} // namespace plugin
} // namespace oceanbase
