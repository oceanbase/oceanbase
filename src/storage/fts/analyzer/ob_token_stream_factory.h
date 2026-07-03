/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TOKEN_STREAM_FACTORY_H_
#define OCEANBASE_STORAGE_OB_TOKEN_STREAM_FACTORY_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "storage/fts/analyzer/ob_analyzer.h"
#include "storage/fts/analyzer/filter/ob_icu_normalizer2_filter.h"
#include "storage/fts/analyzer/filter/ob_snowball_filter.h"
#include "storage/fts/analyzer/filter/ob_stop_word_filter.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
class ObJsonNode;
}

namespace storage
{

enum class ObStopWordLanguageKind : int;

// Factory class responsible for converting analyzer JSON into an ObAnalyzerSpec.
class ObAnalyzerSpecFactory
{
public:
  static int create_analyzer_spec(const common::ObString &analysis_json,
                                  common::ObIAllocator &allocator,
                                  ObAnalyzerSpec *&analyzer_spec);
  static int check_analyzer_use_ik_tokenizer(const common::ObString &analysis_json,
                                             common::ObIAllocator &allocator,
                                             bool &use_ik_tokenizer);
  static void destroy_analyzer_spec(common::ObIAllocator &allocator,
                                    ObAnalyzerSpec *&analyzer_spec);
private:
  static int create_builtin_analyzer_spec_(const common::ObString &analyzer_type,
                                           common::ObIAllocator &allocator,
                                           ObAnalyzerSpec *&analyzer_spec);
  static int create_custom_analyzer_spec_(common::ObJsonNode &root_json,
                                          common::ObIAllocator &allocator,
                                          ObAnalyzerSpec *&analyzer_spec);
  static int resolve_builtin_analyzer_type_(const common::ObString &analyzer_type,
                                            ObAnalyzerType &type);
  // Custom analyzer JSON helpers
  static bool is_builtin_tokenizer_type_(const common::ObString &name);
  static bool is_builtin_token_filter_name_(const common::ObString &name);
  static int resolve_builtin_tokenizer_spec_(const common::ObString &type_name,
                                             common::ObIAllocator &allocator,
                                             ObTokenizerSpec *&tokenizer_spec);
  static int resolve_custom_tokenizer_spec_(const common::ObIJsonBase &tok_def_json,
                                            common::ObIAllocator &allocator,
                                            ObTokenizerSpec *&tokenizer_spec);
  static int resolve_custom_token_filter_spec_(const common::ObIJsonBase &tf_def_json,
                                               common::ObIAllocator &allocator,
                                               ObTokenFilterSpec *&token_filter_spec);
  static int resolve_builtin_token_filter_spec_(const common::ObString &filter_name,
                                                common::ObIAllocator &allocator,
                                                ObTokenFilterSpec *&token_filter_spec);
  // Per-language full pipeline spec creation (tokenizer + token filters)
  static int build_standard_analyzer_spec_(common::ObIAllocator &allocator,
                                           ObAnalyzerSpec &analyzer_spec);
  static int build_english_analyzer_spec_(common::ObIAllocator &allocator,
                                          ObAnalyzerSpec &analyzer_spec);
  static int build_thai_analyzer_spec_(common::ObIAllocator &allocator,
                                       ObAnalyzerSpec &analyzer_spec);
  static int build_vietnamese_analyzer_spec_(common::ObIAllocator &allocator,
                                             ObAnalyzerSpec &analyzer_spec);
  static int build_indonesian_analyzer_spec_(common::ObIAllocator &allocator,
                                             ObAnalyzerSpec &analyzer_spec);
  static int build_malay_analyzer_spec_(common::ObIAllocator &allocator,
                                        ObAnalyzerSpec &analyzer_spec);
  // Shared helpers
  static int create_standard_tokenizer_spec_(common::ObIAllocator &allocator,
                                             ObAnalyzerSpec &analyzer_spec);
  // Init the default filters that every non-legacy analyzer needs:
  //   - utf8mb4_bin char filter (prepended to char_filter_specs_)
  //   - optional MinMax token filter (prepended to token_filter_specs_)
  // Token filter capacity is reserved for extra_token_filter_count plus the
  // optional prepended MinMax filter so callers can append their own filters
  // afterwards. Both builtin and custom analyzers MUST call this before
  // appending analyzer-specific filters.
  static int init_default_filter_specs_(int64_t extra_char_filter_count,
                                        int64_t extra_token_filter_count,
                                        bool need_min_max_token_filter,
                                        common::ObIAllocator &allocator,
                                        ObAnalyzerSpec &analyzer_spec);
  // Individual filter spec append helpers
  static int append_min_max_token_filter_spec_(common::ObIAllocator &allocator,
                                               ObAnalyzerSpec &analyzer_spec);
  static int append_english_possessive_filter_spec_(common::ObIAllocator &allocator,
                                                    ObAnalyzerSpec &analyzer_spec);
  static int append_lowercase_filter_spec_(common::ObIAllocator &allocator,
                                           ObAnalyzerSpec &analyzer_spec);
  static int append_decimal_digit_filter_spec_(common::ObIAllocator &allocator,
                                               ObAnalyzerSpec &analyzer_spec);
  static int append_stop_filter_spec_(common::ObIAllocator &allocator,
                                      ObAnalyzerSpec &analyzer_spec,
                                      ObStopWordLanguageKind language =
                                          ObStopWordLanguageKind::LANGUAGE_INVALID);
  static int append_snowball_filter_spec_(common::ObIAllocator &allocator,
                                          ObAnalyzerSpec &analyzer_spec,
                                          ObSnowballFilterSpec::Algorithm algo);
  static int append_icu_normalizer_filter_spec_(common::ObIAllocator &allocator,
                                                ObAnalyzerSpec &analyzer_spec,
                                                ObICUNormalizer2FilterSpec::Name name =
                                                    ObICUNormalizer2FilterSpec::Name::NFKC_CF,
                                                UNormalization2Mode mode =
                                                    UNormalization2Mode::UNORM2_COMPOSE);
  static int append_icu_folding_filter_spec_(common::ObIAllocator &allocator,
                                             ObAnalyzerSpec &analyzer_spec);
};

// Factory class responsible for converting ObAnalyzerSpec into a fully assembled ObFTSAnalyzer.
class ObTokenStreamFactory
{
public:
  static int create_analyzer(const ObAnalyzerSpec &spec,
                             const common::ObCollationType source_collation,
                             common::ObIAllocator &alloc,         // owner allocator for analyzer/components lifecycle
                             ObFTSAnalyzer *&analyzer);
  // Build an ObFTSAnalyzer that wraps a legacy built-in FT parser (space/ngram/beng/ik/ngram2).
  // Internally constructs an ObAnalyzerSpec and delegates to create_analyzer().
  // All configuration (parser name, properties, flags, meta, allocator) is bundled in param.
  static int create_analyzer_from_legacy_parser(
      const ObFTAnalyzerParam &param,
      ObFTSAnalyzer *&analyzer);
  static void reset_analyzer(ObFTSAnalyzer *analyzer);
private:
  static int create_char_filter(const ObCharFilterSpec &spec,
                                const common::ObCollationType source_collation,
                                common::ObIAllocator &alloc,
                                common::ObIAllocator &scratch_alloc,
                                ObICharFilter *&char_filter);
  static int create_tokenizer(const ObTokenizerSpec &spec,
                              common::ObIAllocator &alloc,
                              common::ObIAllocator &scratch_alloc,
                              ObITokenizer *&tokenizer);
  static int create_token_filter(const ObTokenFilterSpec &spec,
                                 common::ObIAllocator &alloc,
                                 common::ObIAllocator &scratch_alloc,
                                 ObITokenFilter *&token_filter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TOKEN_STREAM_FACTORY_H_
