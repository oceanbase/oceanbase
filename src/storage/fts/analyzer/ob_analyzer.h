/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_ANALYZER_H_
#define OCEANBASE_STORAGE_OB_ANALYZER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_schema_struct_fts.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/analyzer/ob_token_stream.h"
#include "storage/fts/analyzer/ob_i_char_filter.h"
#include "storage/fts/analyzer/ob_i_tokenizer.h"
#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

class ObFTParserProperty;

static constexpr int64_t ANALYZER_PARSER_VERSION_PLACEHOLDER = 1;

// Bundles all parameters for analyzer creation, modeled after ObFTParserParam.
// Populated in segment() and passed through create_fts_analyzer_() → factory.
class ObFTAnalyzerParam final
{
public:
  ObFTAnalyzerParam()
    : legacy_tokenizer_type_(ObTokenizerType::TOKENIZER_TYPE_INVALID),
      parser_property_(nullptr),
      fts_index_type_(share::schema::OB_FTS_INDEX_TYPE_INVALID),
      process_token_flag_(),
      meta_(),
      alloc_(nullptr),
      is_ddl_mode_(false),
      need_casedown_(false) {}
  ~ObFTAnalyzerParam() = default;
  bool is_valid() const
  {
    return nullptr != alloc_
        && nullptr != parser_property_
        && common::CS_TYPE_INVALID != meta_.get_collation_type();
  }
  void reset()
  {
    legacy_tokenizer_type_ = ObTokenizerType::TOKENIZER_TYPE_INVALID;
    parser_property_ = nullptr;
    fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_INVALID;
    process_token_flag_.reset();
    meta_.reset();
    alloc_ = nullptr;
    is_ddl_mode_= false;
    need_casedown_ = false;
  }
  TO_STRING_KV(K_(legacy_tokenizer_type), KP_(parser_property), K_(fts_index_type),
               K_(process_token_flag), K_(meta), KP_(alloc));
public:
  // --- for legacy tokenizer init (ObLegacyParserTokenizerSpec) ---
  ObTokenizerType legacy_tokenizer_type_;                   // legacy tokenizer type, e.g. TOKENIZER_TYPE_SPACE
  // --- for tokenizer + min_max token filter ---
  const ObFTParserProperty *parser_property_;             // parser-specific config (ngram_token_size, min/max_token_size, etc.)
  // --- for phrase_match position list ---
  share::schema::ObFTSIndexType fts_index_type_;          // fulltext index type (match/phrase_match)
  // --- determines which char filters / token filters to create ---
  ObProcessTokenFlag process_token_flag_;                 // PTF_CASEDOWN → lowercase char filter, PTF_MIN_MAX → min_max filter, PTF_STOP → stop filter
  // --- for stop token filter init (hash/cmp/checker) ---
  common::ObObjMeta meta_;                                // document column metadata, carries collation type
  // --- for all component allocation ---
  common::ObIAllocator *alloc_;                           // allocator for creating analyzer and its components
  bool is_ddl_mode_;                                      // whether the analyzer is created for DDL
  bool need_casedown_;                                     // whether to lowercase dict words for case-insensitive matching
};

enum class ObAnalyzerType
{
  ANALYZER_TYPE_INVALID = 0,
  ANALYZER_TYPE_LEGACY,     // wrapping existing parsers
  ANALYZER_TYPE_CUSTOM,     // user-defined analyzer from DDL JSON
  ANALYZER_TYPE_STANDARD,   // built-in standard analyzer (multilingual)
  ANALYZER_TYPE_ENGLISH,    // built-in english analyzer
  ANALYZER_TYPE_THAI,       // built-in thai analyzer
  ANALYZER_TYPE_VIETNAMESE, // built-in vietnamese analyzer
  ANALYZER_TYPE_INDONESIAN, // built-in indonesian analyzer
  ANALYZER_TYPE_MALAY,      // built-in malay analyzer
  ANALYZER_TYPE_MAX
};

// ObAnalyzerSpec corresponds to the in-memory structure parsed from the DDL 'analysis' JSON.
struct ObAnalyzerSpec
{
  ObAnalyzerType analyzer_type_;       // analyzer type, determines the pipeline behavior
  ObTokenizerSpec *tokenizer_spec_;    // tokenizer spec, exactly one per analyzer (required)
  common::ObFixedArray<ObCharFilterSpec*, common::ObIAllocator> char_filter_specs_;  // pre-tokenization char filters (optional, order matters)
  common::ObFixedArray<ObTokenFilterSpec*, common::ObIAllocator> token_filter_specs_; // post-tokenization token filters (optional, order matters)

  ObAnalyzerSpec(common::ObIAllocator &alloc)
    : analyzer_type_(ObAnalyzerType::ANALYZER_TYPE_INVALID),
      tokenizer_spec_(nullptr),
      char_filter_specs_(alloc),
      token_filter_specs_(alloc)
  {}
  bool is_valid() const { return ObAnalyzerType::ANALYZER_TYPE_INVALID != analyzer_type_ && tokenizer_spec_ != nullptr; }
  TO_STRING_KV(K_(analyzer_type), KP_(tokenizer_spec),
               K_(char_filter_specs), K_(token_filter_specs));
};

class ObTokenStreamFactory;

// ObFTSAnalyzer is the only public entry point, encapsulating the complete
// CharFilter -> Tokenizer -> TokenFilter pipeline. Constructed by ObTokenStreamFactory;
// callers only need to call analyze() to obtain the token stream.
class ObFTSAnalyzer
{
public:
  ObFTSAnalyzer(common::ObIAllocator &alloc)
    : analyzer_type_(ObAnalyzerType::ANALYZER_TYPE_INVALID),
      source_collation_(common::CS_TYPE_INVALID),
      char_filters_(alloc),
      tokenizer_(nullptr),
      token_filters_(alloc),
      tail_(nullptr),
      is_inited_(false),
      alloc_(&alloc),
      scratch_alloc_(common::ObMemAttr(MTL_ID(), "FTAScratch"))
  {}
  virtual ~ObFTSAnalyzer();

  int analyze(const char *text, const int64_t text_len,
              common::ObIAllocator &alloc, ObITokenStream *&token_stream);
  void reset();

private:
  ObAnalyzerType analyzer_type_;           // analyzer type (legacy, custom, or specific built-in type)
  ObCollationType source_collation_;        // original text collation from the indexed column
  common::ObFixedArray<ObICharFilter*, common::ObIAllocator>  char_filters_;   // pre-tokenization char filters, applied in order
  ObITokenizer *tokenizer_;                // the core tokenizer that splits text into tokens
  common::ObFixedArray<ObITokenFilter*, common::ObIAllocator> token_filters_;  // post-tokenization token filters, applied in order
  ObITokenStream *tail_;                   // tail of the filter chain, points to the last token filter or the tokenizer if no filters
  bool is_inited_;                         // whether the analyzer has been fully constructed by the factory
  // Long-lived allocator: owns every object that must survive across analyze() calls —
  // the char filter / tokenizer / token filter instances, and (for legacy tokenizers)
  // the underlying ObIFTParser instance plus any parser-side metadata. Set at
  // construction and freed in reset(). Must NOT be used for per-call scratch buffers.
  common::ObIAllocator *alloc_;
  // Per-call scratch arena: reused (ObArenaAllocator::reuse()) at the start of every
  // analyze() call. Use for short-lived buffers produced inside one analyze() call
  // (e.g. legacy char filter tolower output staged before being moved to the filter's
  // own arena, BEng parser's per-token word buffer). Components MUST NOT retain
  // pointers into this arena beyond a single analyze() call. Forwarded to legacy
  // parsers via ObFTParserParam::scratch_alloc_; see ObLegacyParserTokenizer for the
  // metadata_alloc_/scratch_alloc_ split.
  common::ObArenaAllocator scratch_alloc_;

  friend class ObTokenStreamFactory;
  DISALLOW_COPY_AND_ASSIGN(ObFTSAnalyzer);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ANALYZER_H_
