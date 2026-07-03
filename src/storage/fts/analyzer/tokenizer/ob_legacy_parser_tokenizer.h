/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LEGACY_PARSER_TOKENIZER_H_
#define OCEANBASE_STORAGE_OB_LEGACY_PARSER_TOKENIZER_H_

#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "storage/fts/analyzer/ob_i_tokenizer.h"

namespace oceanbase
{
namespace storage
{

class ObIFTParser;

// ============================================================
// Tokenizer Spec structures — one per parser type
// ============================================================

struct ObSpaceTokenizerSpec : public ObTokenizerSpec
{
  ObSpaceTokenizerSpec() { type_ = ObTokenizerType::TOKENIZER_TYPE_SPACE; }
  VIRTUAL_TO_STRING_KV(K_(type));
};

struct ObNgramTokenizerSpec : public ObTokenizerSpec
{
  int64_t ngram_token_size_;
  ObNgramTokenizerSpec() : ngram_token_size_(2) { type_ = ObTokenizerType::TOKENIZER_TYPE_NGRAM; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(ngram_token_size));
};

struct ObBengTokenizerSpec : public ObTokenizerSpec
{
  ObBengTokenizerSpec() { type_ = ObTokenizerType::TOKENIZER_TYPE_BENG; }
  VIRTUAL_TO_STRING_KV(K_(type));
};

struct ObIKTokenizerSpec : public ObTokenizerSpec
{
  uint64_t main_dict_id_;
  uint64_t quan_dict_id_;
  uint64_t stopword_dict_id_;
  common::ObString main_dict_name_;
  common::ObString quan_dict_name_;
  common::ObString stopword_dict_name_;
  bool ik_mode_smart_;
  bool is_ddl_mode_;

  ObIKTokenizerSpec()
    : main_dict_id_(OB_INVALID_ID),
      quan_dict_id_(OB_INVALID_ID),
      stopword_dict_id_(OB_INVALID_ID),
      main_dict_name_(),
      quan_dict_name_(),
      stopword_dict_name_(),
      ik_mode_smart_(true),
      is_ddl_mode_(false)
  { type_ = ObTokenizerType::TOKENIZER_TYPE_IK; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(main_dict_id), K_(quan_dict_id), K_(stopword_dict_id),
      K_(main_dict_name), K_(quan_dict_name), K_(stopword_dict_name), K_(ik_mode_smart), K_(is_ddl_mode));
};

struct ObNgram2TokenizerSpec : public ObTokenizerSpec
{
  int64_t min_ngram_size_;
  int64_t max_ngram_size_;
  ObNgram2TokenizerSpec() : min_ngram_size_(2), max_ngram_size_(2)
  { type_ = ObTokenizerType::TOKENIZER_TYPE_NGRAM2; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(min_ngram_size), K_(max_ngram_size));
};

// ============================================================
// ObLegacyParserTokenizer — virtual base class
// ============================================================
//
// Shared pull-model adapter that wraps an ObIFTParser as ObITokenizer.
// Subclasses implement create_parser_impl() / destroy_parser_impl()
// to directly construct the specific parser (space, ngram, beng, ik, ngram2).

class ObLegacyParserTokenizer : public ObITokenizer
{
public:
  ObLegacyParserTokenizer()
    : alloc_(nullptr),
      legacy_parser_(nullptr),
      position_(0),
      is_inited_(false)
  {}
  virtual ~ObLegacyParserTokenizer() { reset(); }

  // init() is subclass-specific
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override = 0;
  // set_input() uses shared reuse logic, delegates parser creation to subclass
  virtual int set_input(const char *text, int64_t text_len, ObCollationType coll_type) override;
  virtual int get_next_token(ObTokenAttr &token) override;
  virtual void reset() override;

protected:
  // Subclasses must implement: create/destroy the specific parser.
  // create_parser_impl() should allocate and init the parser, assigning to legacy_parser_.
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) = 0;
  // destroy_parser_impl() destructs and frees the parser, setting legacy_parser_ to nullptr.
  // Default implementation works for all legacy parsers; subclasses may override if needed.
  virtual void destroy_parser_impl();

  common::ObIAllocator *alloc_;
  ObIFTParser          *legacy_parser_;
  int32_t               position_;
  bool                  is_inited_;

private:
  int convert_token(const char *word, int64_t word_len, ObTokenAttr &token);
  DISALLOW_COPY_AND_ASSIGN(ObLegacyParserTokenizer);
};

// ============================================================
// 5 Tokenizer subclasses
// ============================================================

class ObSpaceTokenizer final : public ObLegacyParserTokenizer
{
public:
  ObSpaceTokenizer() = default;
  virtual ~ObSpaceTokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
protected:
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) override;
};

class ObNgramTokenizer final : public ObLegacyParserTokenizer
{
public:
  ObNgramTokenizer() : ngram_token_size_(2) {}
  virtual ~ObNgramTokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
protected:
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) override;
private:
  int64_t ngram_token_size_;
};

class ObBengTokenizer final : public ObLegacyParserTokenizer
{
public:
  ObBengTokenizer() = default;
  virtual ~ObBengTokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
protected:
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) override;
};

class ObIKTokenizer final : public ObLegacyParserTokenizer
{
public:
  ObIKTokenizer() : ik_mode_smart_(true) {}
  virtual ~ObIKTokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
protected:
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) override;
private:
  uint64_t main_dict_id_;
  uint64_t quan_dict_id_;
  uint64_t stopword_dict_id_;
  common::ObString main_dict_name_;
  common::ObString quan_dict_name_;
  common::ObString stopword_dict_name_;
  bool ik_mode_smart_;
  bool is_ddl_mode_;
};

class ObNgram2Tokenizer final : public ObLegacyParserTokenizer
{
public:
  ObNgram2Tokenizer() : min_ngram_size_(2), max_ngram_size_(2) {}
  virtual ~ObNgram2Tokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
protected:
  virtual int create_parser_impl(const ObCharsetInfo *cs, const char *text, int64_t text_len) override;
private:
  int64_t min_ngram_size_;
  int64_t max_ngram_size_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LEGACY_PARSER_TOKENIZER_H_
