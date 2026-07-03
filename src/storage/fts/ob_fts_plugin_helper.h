/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_FTS_PLUGIN_HELPER_H_
#define OB_FTS_PLUGIN_HELPER_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "object/ob_object.h"
#include "share/ob_plugin_helper.h"
#include "share/schema/ob_schema_struct_fts.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/analyzer/ob_analyzer.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}

namespace plugin
{
class ObIFTParserDesc;
class ObPluginParam;
}

namespace storage
{

class ObStopTokenCheckerGen;
class ObStopTokenChecker;

#define FTS_BUILD_IN_PARSER_LIST                                                                   \
  FT_PARSER_TYPE(FTP_SPACE, space)                                                                 \
  FT_PARSER_TYPE(FTP_NGRAM, ngram)                                                                 \
  FT_PARSER_TYPE(FTP_BENG, beng)                                                                   \
  FT_PARSER_TYPE(FTP_IK, ik)                                                                       \
  FT_PARSER_TYPE(FTP_NGRAM2, ngram2)

class ObFTParser final
{
public:
  enum ParserType : int64_t {
    FTP_NON_BUILDIN = 0,
#define FT_PARSER_TYPE(ftp_type, parser_name) ftp_type,
    FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE
    FTP_MAX
  };
  static const char *NAME_STR[ParserType::FTP_MAX + 1];

public:
  ObFTParser() : parser_name_(), parser_version_(-1) {}
  ~ObFTParser() = default;
  int parse_from_str(const char *plugin_name, const int64_t buf_len);
  int serialize_to_str(char *buf, const int64_t buf_len);

#define FT_PARSER_TYPE(fts_type, parser_name)                          \
  OB_INLINE bool is_##parser_name() const {                            \
    ParserType type = fts_type;                                        \
    return share::ObPluginName(NAME_STR[type]) == parser_name_;        \
  }
  FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE

  OB_INLINE const share::ObPluginName &get_parser_name() const { return parser_name_; }
  OB_INLINE int64_t get_parser_version() const { return parser_version_; }
  OB_INLINE bool is_valid() const { return parser_name_.is_valid() && parser_version_ >= 0; }
  OB_INLINE bool is_type_before_4_3_5_1() const { return is_space() || is_beng() || is_ngram(); }
  OB_INLINE bool is_legacy_parser() const
  {
    return is_space() || is_ngram() || is_beng() || is_ik() || is_ngram2();
  }
  OB_INLINE static bool is_legacy_parser_name(const common::ObString &name)
  {
    return (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_SPACE)) ||
           (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_NGRAM)) ||
           (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_NGRAM2)) ||
           (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_BENG)) ||
           (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_IK));
  }
  // Returns true for "analyzer" (no version) or "analyzer.N" where N is a pure-digit version
  // string (e.g. "analyzer.1"). Multiple dots (e.g. "analyzer.1.2") or non-digit suffixes
  // (e.g. "analyzer.1a") return false.
  OB_INLINE static bool is_analyzer_parser_name(const common::ObString &name)
  {
    bool is_analyzer = false;
    if (nullptr == name.find('.')) {
      is_analyzer = (0 == name.case_compare(ObFTSLiteral::PARSER_NAME_ANALYZER));
    } else {
      common::ObString parser_name_with_version = name;
      common::ObString parser_name = parser_name_with_version.split_on('.');
      is_analyzer = (0 == parser_name.case_compare(ObFTSLiteral::PARSER_NAME_ANALYZER))
                    && !parser_name_with_version.empty();
      for (int64_t i = 0; is_analyzer && i < parser_name_with_version.length(); ++i) {
        is_analyzer = parser_name_with_version.ptr()[i] >= '0' && parser_name_with_version.ptr()[i] <= '9';
      }
    }
    return is_analyzer;
  }
  OB_INLINE bool is_analyzer_parser() const
  {
    return parser_name_ == share::ObPluginName(ObFTSLiteral::PARSER_NAME_ANALYZER);
  }
  OB_INLINE ObTokenizerType to_tokenizer_type() const
  {
    ObTokenizerType tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_INVALID;
    if (is_space()) {
      tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_SPACE;
    } else if (is_ngram()) {
      tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_NGRAM;
    } else if (is_beng()) {
      tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_BENG;
    } else if (is_ik()) {
      tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_IK;
    } else if (is_ngram2()) {
      tokenizer_type = ObTokenizerType::TOKENIZER_TYPE_NGRAM2;
    }
    return tokenizer_type;
  }
  OB_INLINE void set_name_and_version(const share::ObPluginName &name, const int64_t version)
  {
    parser_name_ = name;
    parser_version_ = version;
  }
  OB_INLINE bool operator ==(const ObFTParser &other) const
  {
    bool is_equal = true;
    if (this != &other) {
      is_equal = parser_name_ == other.get_parser_name() && parser_version_ == other.parser_version_;
    }
    return is_equal;
  }
  OB_INLINE bool operator !=(const ObFTParser &other) const { return !(*this == other); }
  TO_STRING_KV(K_(parser_name), K_(parser_version));
private:
  share::ObPluginName parser_name_;
  int64_t parser_version_;
};

class ObFTParsePluginData final
{
public:
  ObFTParsePluginData() :
      stop_token_checker_gen_(nullptr), handler_allocator_(), is_inited_(false) { }
  ~ObFTParsePluginData();

  /**
   * create a process global instance
   */
  static int  init_global();
  static void deinit_global();
  static ObFTParsePluginData &instance();

  int init();
  void destroy();

public:
  int get_stop_token_checker(const ObCollationType coll,
                             ObStopTokenChecker &stop_token_checker);
private:
  int init_stop_token_checker_gen();

private:
  ObStopTokenCheckerGen *stop_token_checker_gen_;
  common::ObFIFOAllocator handler_allocator_;
  bool is_inited_;
};

class ObFTParseHelper final
{
public:
  ObFTParseHelper();
  ~ObFTParseHelper();

  /**
   * initialize fulltext parse helper
   *
   * @param[in] allocator
   * @param[in] parser_name, which consists of two parts name and version.
   *                         e.g. default_parser.1
   *                                   |         |
   *                            parse name   paser version
   * @param[in] parser_properties, which is a parser configuration in JSON format.
   *                         e.g.  {
   *                                 "min_token_size":2,
   *                                 "max_token_size":84,
   *                                 "ngram_token_size":2,
   *                                 "stopword_table":"default",
   *                                 "dict_table":"none",
   *                                 "quanitfier_table":"none"
   *                               }
   * @param[in] fts_index_type, the type of fulltext index
   *
   * @return error code
   */
  int init(
      common::ObIAllocator *allocator,
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties,
      const share::schema::ObFTSIndexType fts_index_type,
      const bool is_ddl_mode = false);
  /**
   * Split document into multiple words
   *
   * @param[in] type, collation type for fulltext
   * @param[in] fulltext
   * @param[in] fulltext_len, length of the fulltext
   * @param[out] doc_length, length of document by word count
   * @param[out] words, word lists after segment
   */
  int segment(
      const common::ObObjMeta &meta,
      const char *fulltext,
      const int64_t fulltext_len,
      int64_t &doc_length,
      ObFTTokenMap &ft_token_map) const;
  int check_is_the_same(
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties,
      const share::schema::ObFTSIndexType fts_index_type,
      bool &is_same) const;
  /**
   * Make json document for fulltext search
   *
   * @param[in] words, word lists after segment
   * @param[in] doc_length, length of document by word count
   * @param[out] json_root, json document
   */
  int make_detail_json(
      const ObFTTokenMap &ft_token_map,
      const int64_t doc_length,
      common::ObIJsonBase *&json_root);

  /**
   * Make json document for fulltext search
   *
   * @param[in] words, word lists after segment
   * @param[out] json_root, json document
   */
  int make_token_array_json(
      const ObFTTokenMap &ft_token_map,
      common::ObIJsonBase *&json_root);

  void reset();

  OB_INLINE const ObFTParser &get_parser_name() const { return parser_name_; }

  OB_INLINE plugin::ObPluginParam *get_plugin_param() const { return plugin_param_; }

  OB_INLINE const ObFTParserProperty &get_parser_property() const { return parser_property_; }

  OB_INLINE const plugin::ObIFTParserDesc *get_parser_desc() const { return parser_desc_; }

  OB_INLINE const ObProcessTokenFlag &get_process_token_flags() const { return process_token_flag_; }

  OB_INLINE bool is_ddl_mode() const { return is_ddl_mode_; }

  OB_INLINE bool is_builtin_parser() const { return parser_name_.is_legacy_parser() || is_builtin_analyzer(); }
  OB_INLINE bool is_legacy_parser() const { return parser_name_.is_legacy_parser(); }

  OB_INLINE bool is_analyzer_parser() const { return parser_name_.is_analyzer_parser(); }

  OB_INLINE bool is_builtin_analyzer() const
  {
    return is_analyzer_parser() && OB_NOT_NULL(analyzer_spec_)
        && (analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_STANDARD
            || analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_ENGLISH
            || analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_THAI
            || analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_VIETNAMESE
            || analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_INDONESIAN
            || analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_MALAY);
  }
  OB_INLINE bool is_custom_analyzer() const
  {
    return is_analyzer_parser() && OB_NOT_NULL(analyzer_spec_)
        && analyzer_spec_->analyzer_type_ == ObAnalyzerType::ANALYZER_TYPE_CUSTOM;
  }

  OB_INLINE const common::ObString &get_analysis_json() const { return analysis_json_; }
  OB_INLINE const ObAnalyzerSpec *get_analyzer_spec() const { return analyzer_spec_; }

  // Create an analyzer from the given param. Caller owns the returned analyzer
  // and must destroy it after use.
  int create_analyzer(const ObFTAnalyzerParam &param, ObFTSAnalyzer *&fts_analyzer) const;

  TO_STRING_KV(KP_(allocator), K_(parser_name), KP_(parser_desc), K_(is_inited), K_(fts_index_type));

private:
  int set_process_token_flag(const plugin::ObIFTParserDesc &ftparser_desc);
  void free_analyzer_spec_();

private:
  common::ObIAllocator *allocator_;
  common::ObArenaAllocator analyzer_allocator_;  // separate arena for analyzer_spec_ and analysis_json_
  plugin::ObIFTParserDesc *parser_desc_;
  plugin::ObPluginParam *plugin_param_;
  ObFTParser parser_name_;
  ObProcessTokenFlag process_token_flag_;
  ObFTParserJsonProps props_;
  ObFTParserProperty parser_property_;
  common::ObString analysis_json_;   // for "analyzer" parser: raw analysis JSON
  ObAnalyzerSpec *analyzer_spec_;    // for non-legacy analyzers
  share::schema::ObFTSIndexType fts_index_type_;
  bool is_ddl_mode_;
  bool is_inited_;

private:
  static constexpr const char *ENTRY_NAME_DOC_LEN = "doc_len";
  static constexpr const char *ENTRY_NAME_TOKENS = "tokens";
  DISALLOW_COPY_AND_ASSIGN(ObFTParseHelper);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_PLUGIN_HELPER_H_
