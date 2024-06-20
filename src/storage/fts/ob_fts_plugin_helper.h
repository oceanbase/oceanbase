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

#include "lib/ob_errno.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/ob_fts_plugin_mgr.h"

namespace oceanbase
{
namespace storage
{

class ObAddWord;

class ObFTParser final
{
public:
  ObFTParser() : parser_name_(), parser_version_(-1) {}
  ~ObFTParser() = default;
  int parse_from_str(const char *plugin_name, const int64_t buf_len);
  int serialize_to_str(char *buf, const int64_t buf_len);
  OB_INLINE const share::ObPluginName &get_parser_name() const { return parser_name_; }
  OB_INLINE int64_t get_parser_version() const { return parser_version_; }
  OB_INLINE bool is_valid() const { return parser_name_.is_valid() && parser_version_ >= 0; }
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
   *
   * @return error code
   */
  int init(
      common::ObIAllocator *allocator,
      const common::ObString &plugin_name);
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
  const ObFTParser &get_parser_name() const { return parser_name_; }
  void reset();

  TO_STRING_KV(K_(plugin_param), KP_(allocator), KP_(parser_desc), K_(is_inited));
private:
  static int get_fulltext_parser_desc(
      const lib::ObIPluginHandler &handler,
      lib::ObIFTParserDesc *&parser_desc);
  static int segment(
      const int64_t parser_version,
      const lib::ObIFTParserDesc *parser_desc,
      const ObCharsetInfo *cs,
      const char *fulltext,
      const int64_t fulltext_len,
      common::ObIAllocator &allocator,
      ObAddWord &add_word);
  int set_add_word_flag(const ObFTParser &parser);
private:
  lib::ObPluginParam plugin_param_;
  common::ObIAllocator *allocator_;
  lib::ObIFTParserDesc *parser_desc_;
  ObFTParser parser_name_;
  ObAddWordFlag add_word_flag_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTParseHelper);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_PLUGIN_HELPER_H_
