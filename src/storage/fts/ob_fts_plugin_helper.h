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

#ifndef OB_FTS_PLUGIN_HELPER_H_
#define OB_FTS_PLUGIN_HELPER_H_

#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/fts/ob_fts_struct.h"
#include "share/ob_plugin_helper.h"
#include "storage/fts/ob_fts_parser_property.h"

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

class ObStopWordChecker;
class ObFTDictHub;
class ObAddWord;

#define FTS_BUILD_IN_PARSER_LIST               \
  FT_PARSER_TYPE(FTP_SPACE, space)             \
  FT_PARSER_TYPE(FTP_NGRAM, ngram)             \
  FT_PARSER_TYPE(FTP_BENG, beng)               \
  FT_PARSER_TYPE(FTP_IK, ik)

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
  ObFTParsePluginData() = default;
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
  ObStopWordChecker *stop_word_checker() const { return stop_word_checker_; }
  int get_dict_hub(ObFTDictHub *&hub);

private:
  int init_and_set_stopword_list();
  int init_dict_hub();

private:
  ObStopWordChecker *     stop_word_checker_ = nullptr;
  ObFTDictHub *           dict_hub_          = nullptr;
  common::ObFIFOAllocator handler_allocator_;
  bool                    is_inited_         = false;
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
   *
   * @return error code
   */
  int init(
      common::ObIAllocator *allocator,
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties);
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
      const common::ObCollationType &type,
      const char *fulltext,
      const int64_t fulltext_len,
      int64_t &doc_length,
      ObFTWordMap &words) const;
  int check_is_the_same(
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties,
      bool &is_same) const;
  /**
   * Make json document for fulltext search
   *
   * @param[in] words, word lists after segment
   * @param[in] doc_length, length of document by word count
   * @param[out] json_root, json document
   */
  int make_detail_json(
      const ObFTWordMap &words,
      const int64_t doc_length,
      common::ObIJsonBase *&json_root);

  /**
   * Make json document for fulltext search
   *
   * @param[in] words, word lists after segment
   * @param[out] json_root, json document
   */
  int make_token_array_json(
      const ObFTWordMap &words,
      common::ObIJsonBase *&json_root);

  void reset();

  TO_STRING_KV(KP_(allocator), KP_(parser_desc), K_(is_inited));
private:
  static int segment(
      const ObFTParserProperty &property,
      const int64_t parser_version,
      const plugin::ObIFTParserDesc *parser_desc,
      plugin::ObPluginParam *plugin_param,
      const ObCharsetInfo *cs,
      const char *fulltext,
      const int64_t fulltext_len,
      common::ObIAllocator &allocator,
      ObAddWord &add_word);
  int set_add_word_flag(const plugin::ObIFTParserDesc &ftparser_desc);
private:
  common::ObIAllocator *allocator_;
  plugin::ObIFTParserDesc *parser_desc_;
  plugin::ObPluginParam *plugin_param_;
  ObFTParser parser_name_;
  ObAddWordFlag add_word_flag_;
  ObFTParserProperty parser_property_;
  bool is_inited_;

private:
  static constexpr const char *ENTRY_NAME_DOC_LEN = "doc_len";
  static constexpr const char *ENTRY_NAME_TOKENS = "tokens";
  DISALLOW_COPY_AND_ASSIGN(ObFTParseHelper);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_PLUGIN_HELPER_H_
