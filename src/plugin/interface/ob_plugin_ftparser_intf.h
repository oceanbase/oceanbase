/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "plugin/interface/ob_plugin_intf.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/fts/ob_fts_struct.h"

struct ObCharsetInfo;

namespace oceanbase
{
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
      : mode_(mode), main_dict_id_(OB_INVALID_ID), quan_dict_id_(OB_INVALID_ID), stopword_dict_id_(OB_INVALID_ID),
        main_dict_name_(), quan_dict_name_(), stopword_dict_name_()
  {
  }

  TO_STRING_KV(K_(mode), K_(main_dict_id), K_(quan_dict_id), K_(stopword_dict_id),
               K_(main_dict_name), K_(quan_dict_name), K_(stopword_dict_name));

public:
  Mode mode_;
  uint64_t main_dict_id_;
  uint64_t quan_dict_id_;
  uint64_t stopword_dict_id_;
  common::ObString main_dict_name_;
  common::ObString quan_dict_name_;
  common::ObString stopword_dict_name_;
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
  ObFTParserParam()
      : ObFTParserParamExport(),
        metadata_alloc_(nullptr),
        scratch_alloc_(nullptr),
        ik_param_(),
        ngram_token_size_(NGRAM_TOKEN_SIZE),
        min_ngram_size_(NGRAM_TOKEN_SIZE),
        max_ngram_size_(NGRAM_TOKEN_SIZE),
        is_ddl_mode_(false),
        need_casedown_(false)
  {
  }
  virtual ~ObFTParserParam() { reset(); }

  inline void reset()
  {
    ObFTParserParamExport::reset();
    metadata_alloc_ = nullptr;
    scratch_alloc_ = nullptr;
    ngram_token_size_ = NGRAM_TOKEN_SIZE;
    is_ddl_mode_ = false;
    need_casedown_ = false;
  }

  INHERIT_TO_STRING_KV("ObFTParserParamExport", ObFTParserParamExport,
      KP_(metadata_alloc), KP_(scratch_alloc), K_(ngram_token_size),
      K_(min_ngram_size), K_(max_ngram_size), K_(ik_param), K_(is_ddl_mode), K_(need_casedown));

public:
  common::ObIAllocator *metadata_alloc_;
  common::ObIAllocator *scratch_alloc_;
  ObFTIKParam ik_param_;
  int64_t ngram_token_size_;
  int64_t min_ngram_size_;
  int64_t max_ngram_size_;

  bool is_ddl_mode_;
  bool need_casedown_;
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

  /**
   * get AddWordFlag
   * @details ref to ObAddWordFlag for more details
   * @param[out] flag the ObAddWordFlag
   */
  virtual int get_add_word_flag(storage::ObAddWordFlag &flag) const = 0;

  virtual int check_if_charset_supported(const ObCharsetInfo *cs) const { return OB_SUCCESS; }
};

} // namespace plugin
} // namespace oceanbase
