/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_STOP_WORD_FILTER_H_
#define OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_STOP_WORD_FILTER_H_

#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

// Built-in stopword set selector; upper layers map Analyzer / JSON configuration to this enum.
enum class ObStopWordLanguageKind
{
  LANGUAGE_INVALID = 0,
  LANGUAGE_ENGLISH,
  LANGUAGE_THAI,
  LANGUAGE_VIETNAMESE,
  LANGUAGE_INDONESIAN,
  LANGUAGE_MALAY,
  // Analyzer JSON `stopwords: "_none_"`: keep all tokens.
  LANGUAGE_NONE
};

// Token filter config: remove stop words by built-in dictionary or future custom table.
// Upper layers map Analyzer / JSON configuration to ObStopWordLanguageKind before execution.
struct ObStopWordFilterSpec : public ObTokenFilterSpec
{
  ObStopWordFilterSpec()
      : language_(ObStopWordLanguageKind::LANGUAGE_INVALID),
        stopword_table_()
  {
    type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_STOP;
  }

  // Built-in stopword dictionary; LANGUAGE_INVALID selects the same default as English FTS ("_english_").
  ObStopWordLanguageKind language_;
  // Reserved: custom stopword table db.table (MySQL-style), not implemented yet.
  common::ObString stopword_table_;
  TO_STRING_KV(K_(type), K_(language), K_(stopword_table));
};

class ObStopWordFilter : public ObITokenFilter
{
public:
  ObStopWordFilter();
  ~ObStopWordFilter() override;

  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;

private:
  class ObBuiltinStopWordChecker;

  enum class StopwordSourceType
  {
    SOURCE_INVALID = 0,
    SOURCE_BUILTIN,
    SOURCE_CUSTOM_TABLE
  };

  int init_custom_stop_word_checker(const common::ObString &table_name, common::ObIAllocator &alloc);
  int check_is_stop_word(const ObTokenAttr &token, bool &is_stop_word) const;
  void destroy_builtin_checker();
  void destroy_stopword_table_name();

  ObBuiltinStopWordChecker *builtin_checker_;
  common::ObString stopword_table_name_;
  common::ObIAllocator *checker_owner_alloc_;
  StopwordSourceType source_type_;
  ObStopWordLanguageKind language_kind_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObStopWordFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_STOP_WORD_FILTER_H_
