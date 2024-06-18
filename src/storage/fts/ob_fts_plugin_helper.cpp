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

#define USING_LOG_PREFIX STORAGE_FTS

#include "lib/alloc/alloc_assist.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

// The plugin_name comes from index table schema and consists of two parts: name and
// version, e.g. default_parser.1, separated by dot.
int ObFTParser::parse_from_str(const char *plugin_name, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plugin_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin name is nullptr", K(ret), KP(plugin_name));
  } else if (OB_UNLIKELY(buf_len >= OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin name is too long", K(ret), K(buf_len));
  } else {
    char name[OB_PLUGIN_NAME_LENGTH];
    char *saveptr = nullptr;
    char *token = nullptr;
    char *end_ptr = nullptr;
    MEMCPY(name, plugin_name, buf_len);
    name[buf_len] = '\0';
    if (OB_ISNULL(token = STRTOK_R(name, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, plugin name is illegal", K(ret), KCSTRING(name));
    } else if (OB_FAIL(parser_name_.set_name(token))) {
      LOG_WARN("fail to set parser name", K(ret), KCSTRING(token));
    } else if (OB_ISNULL(token = STRTOK_R(nullptr, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, plugin name is illegal", K(ret), KCSTRING(name));
    } else if (OB_FAIL(ob_strtoll(token, end_ptr, parser_version_))) {
      LOG_WARN("failed to convert str to ll", KCSTRING(token));
    } else if (OB_NOT_NULL(token = STRTOK_R(nullptr, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, plugin name is illegal", K(ret), KCSTRING(name));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("plugin name isn't valid fulltext parser", K(ret), KCSTRING(plugin_name), KPC(this));
    }
  }
  return ret;
}

// The fulltext parser name consists of two parts: name and version, e.g. default_parser.1,
// separated by dot. This function is designed to serialize them into cstring.
int ObFTParser::serialize_to_str(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid fulltext parser doesn't support to serialize_to_str", K(ret), KPC(this));
  } else if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, "%.*s.%ld", parser_name_.len(), parser_name_.str(),
          parser_version_))) {
    LOG_WARN("fail to printf", K(ret), K(buf_len), K(parser_name_), K(parser_version_));
  }
  return ret;
}

int ObFTParseHelper::get_fulltext_parser_desc(
    const lib::ObIPluginHandler &handler,
    lib::ObIFTParserDesc *&parser_desc)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t size = 0;
  lib::ObPlugin *plugin = nullptr;
  lib::ObIPluginDesc *desc = nullptr;
  if (OB_FAIL(handler.get_plugin_version(version))) {
    LOG_WARN("fail to get plugin version", K(ret), K(handler));
  } else if (OB_FAIL(handler.get_plugin_size(size))) {
    LOG_WARN("fail to get plugin size", K(ret), K(handler));
  } else if (OB_UNLIKELY(OB_PLUGIN_INTERFACE_VERSION != version || sizeof(lib::ObPlugin) != size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the plugin interface version or size is invalid", K(ret), K(version), K(size));
  } else if (OB_FAIL(handler.get_plugin(plugin))) {
    LOG_WARN("fail to get plugin", K(ret), K(handler));
  } else if (OB_ISNULL(plugin)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin is nullptr", K(ret), K(handler));
  } else if (OB_UNLIKELY(lib::ObPluginType::OB_FT_PARSER_PLUGIN != plugin->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("this plugin isn't a fulltext parser", K(ret), K(plugin->type_), K(handler));
  } else {
    parser_desc = static_cast<lib::ObIFTParserDesc *>(plugin->desc_);
  }
  return ret;
}

int ObFTParseHelper::segment(
    const int64_t parser_version,
    const lib::ObIFTParserDesc *parser_desc,
    const ObCharsetInfo *cs,
    const char *ft,
    const int64_t ft_len,
    common::ObIAllocator &allocator,
    ObAddWord &add_word)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parser_version < 0 || nullptr == parser_desc || nullptr == cs || nullptr == ft || 0 >= ft_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser_version), KP(parser_desc), KP(cs), K(ft), K(ft_len));
  } else {
    lib::ObFTParserParam param;
    lib::ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    param.cs_ = cs;
    param.fulltext_ = ft;
    param.ft_length_ = ft_len;
    param.parser_version_ = parser_version;
    if (OB_FAIL(parser_desc->segment(&param, iter))) {
      LOG_WARN("fail to segment", K(ret), K(param));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, token iterator is nullptr", K(ret), KP(iter));
    } else {
      const char *word = nullptr;
      int64_t word_len = 0;
      int64_t char_cnt = 0;
      int64_t word_freq = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_token(word, word_len, char_cnt, word_freq))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next token", K(ret), KPC(iter));
          }
        } else if (OB_FAIL(add_word.process_word(word, word_len, char_cnt, word_freq))) {
          LOG_WARN("fail to process one word", K(ret), KP(word), K(word_len), K(char_cnt), K(word_freq));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      parser_desc->free_token_iter(&param, iter);
      iter = nullptr;
    }
  }
  return ret;
}

ObFTParseHelper::ObFTParseHelper()
  : plugin_param_(),
    allocator_(nullptr),
    parser_desc_(nullptr),
    parser_name_(),
    add_word_flag_(),
    is_inited_(false)
{
}

ObFTParseHelper::~ObFTParseHelper()
{
  reset();
}

int ObFTParseHelper::init(
    common::ObIAllocator *allocator,
    const common::ObString &plugin_name)
{
  int ret = OB_SUCCESS;
  lib::ObIPluginHandler *parse_handler = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this fulltext parse helper has been initialized", K(ret), KP(parser_desc_), K(is_inited_));
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(plugin_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(allocator), K(plugin_name));
  } else if (OB_FAIL(parser_name_.parse_from_str(plugin_name.ptr(), plugin_name.length()))) {
    LOG_WARN("fail to parse name from cstring", K(ret), K(plugin_name));
  } else if (OB_FAIL(OB_FT_PLUGIN_MGR.get_plugin_handler(parser_name_.get_parser_name(), parse_handler))) {
    LOG_WARN("fail to open plugin handler", K(ret), K(plugin_name));
  } else if (OB_ISNULL(parse_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, parse handler is nullptr", K(ret), KP(parse_handler));
  } else if (OB_FAIL(get_fulltext_parser_desc(*parse_handler, parser_desc_))) {
    LOG_WARN("fail to get fulltext parser descriptor", K(ret), KPC(parse_handler));
  } else if (OB_FAIL(set_add_word_flag(parser_name_))) {
    LOG_WARN("fail to set add word flag", K(ret), K(parser_name_));
  } else {
    plugin_param_.desc_ = parser_desc_;
    allocator_ = allocator;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObFTParseHelper::reset()
{
  parser_desc_ = nullptr;
  plugin_param_.reset();
  allocator_ = nullptr;
  add_word_flag_.clear();
  is_inited_ = false;
}

int ObFTParseHelper::segment(
    const ObCollationType &type,
    const char *fulltext,
    const int64_t fulltext_len,
    int64_t &doc_length,
    ObFTWordMap &words) const
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("this fulltext parser helper hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator ptr is nullptr", K(ret), KP_(allocator), K_(is_inited));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == type || type >= CS_TYPE_EXTENDED_MARK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, charset info is nullptr", K(ret), K(type));
  } else {
    words.reuse();
    ObAddWord add_word(type, add_word_flag_, *allocator_, words);
    if (OB_FAIL(segment(parser_name_.get_parser_version(), parser_desc_, cs, fulltext, fulltext_len, *allocator_,
            add_word))) {
      LOG_WARN("fail to segment fulltext", K(ret), K(parser_name_), KP(parser_desc_), KP(cs), KP(fulltext),
          K(fulltext_len), KP(allocator_));
    } else {
      doc_length = add_word.get_add_word_count();
    }
  }
  LOG_DEBUG("ft parse segment", K(ret), K(type), K(add_word_flag_), K(parser_name_),
      K(ObString(fulltext_len, fulltext)), K(words.size()));
  return ret;
}

int ObFTParseHelper::set_add_word_flag(const ObFTParser &parser)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!parser.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser));
  } else if (share::ObPluginName("space") == parser.get_parser_name()) {
    add_word_flag_.set_min_max_word();
    add_word_flag_.set_stop_word();
    add_word_flag_.set_casedown();
    add_word_flag_.set_groupby_word();
  } else if (share::ObPluginName("beng") == parser.get_parser_name()) {
    add_word_flag_.set_min_max_word();
    add_word_flag_.set_stop_word();
    add_word_flag_.set_groupby_word();
  } else if (share::ObPluginName("ngram") == parser.get_parser_name()) {
    add_word_flag_.set_casedown();
    add_word_flag_.set_groupby_word();
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported parser for fulltext search", K(ret), K(parser));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
