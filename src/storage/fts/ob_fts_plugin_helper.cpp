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

#include "share/ob_force_print_log.h"
#include "src/sql/engine/expr/ob_expr_operator.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "plugin/sys/ob_plugin_helper.h"

using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{

const char *ObFTParser::NAME_STR[ObFTParser::ParserType::FTP_MAX + 1] = {
  "non-builtin",
#define FT_PARSER_TYPE(ftp_type, parser_name) #parser_name,
  FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE
  "max type of parser"
};

// The plugin_name comes from index table schema and consists of two parts: name and
// version, e.g. default_parser.1, separated by dot.
int ObFTParser::parse_from_str(const char *plugin_name, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plugin_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin name is nullptr", K(ret), KP(plugin_name));
  } else if (OB_UNLIKELY(buf_len >= share::OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin name is too long", K(ret), K(buf_len));
  } else {
    char name[share::OB_PLUGIN_NAME_LENGTH];
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
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < share::OB_PLUGIN_NAME_LENGTH)) {
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
////////////////////////////////////////////////////////////////////////////////
// ObFTParsePluginData

static ObFTParsePluginData *g_ftparse_plugin_data = nullptr;
static constexpr const char *FTPARSE_PLUGIN_DATA_MEMORY_LABEL = "FtParse";

int ObFTParsePluginData::init_global()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(g_ftparse_plugin_data)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ftparse plugin data init twice", K(ret));
  } else if (OB_ISNULL(g_ftparse_plugin_data = OB_NEW(ObFTParsePluginData, FTPARSE_PLUGIN_DATA_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(sizeof(ObFTParsePluginData)));
  } else if (OB_FAIL(g_ftparse_plugin_data->init())) {
    LOG_WARN("failed to init global ftparse plugin data object", K(ret));
  }
  return ret;
}

void ObFTParsePluginData::deinit_global()
{
  if (OB_NOT_NULL(g_ftparse_plugin_data)) {
    OB_DELETE(ObFTParsePluginData, FTPARSE_PLUGIN_DATA_MEMORY_LABEL, g_ftparse_plugin_data);
    g_ftparse_plugin_data = nullptr;
  }
}

ObFTParsePluginData &ObFTParsePluginData::instance()
{
  return *g_ftparse_plugin_data;
}

ObFTParsePluginData::~ObFTParsePluginData()
{
  destroy();
}

int ObFTParsePluginData::init()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr;
  mem_attr.label_ = FTPARSE_PLUGIN_DATA_MEMORY_LABEL;

  if (OB_FAIL(handler_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      mem_attr))) {
    LOG_WARN("fail to init tenant plugin handler allocator", K(ret));
  } else if (OB_FAIL(init_and_set_stopword_list())) {
    LOG_WARN("fail to init and set stopword list", K(ret));
  } else if (OB_FAIL(init_dict_hub())) {
    LOG_WARN("fail to init dict hub", K(ret));
  } else {
    is_inited_ = true;
    FLOG_INFO("succeed to initialize ObTenantFTPluginMgr", KP(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObFTParsePluginData::init_and_set_stopword_list()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stop_word_checker_ = OB_NEWx(ObStopWordChecker, &handler_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create stop word checker", K(ret));
  } else if (OB_FAIL(stop_word_checker_->init())) {
    LOG_WARN("failed to init stop word checker", K(ret));
  }
  return ret;
}

int ObFTParsePluginData::init_dict_hub()
{
  // make dict
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_hub_
                = static_cast<ObFTDictHub *>(handler_allocator_.alloc(sizeof(ObFTDictHub))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for dict hub.", K(ret));
  } else if (FALSE_IT(new (dict_hub_) ObFTDictHub())) {
  } else if (OB_FAIL(dict_hub_->init())) {
    LOG_WARN("Failed to init dict hub.", K(ret));
  }
  return ret;
}

void ObFTParsePluginData::destroy()
{
  if (OB_NOT_NULL(stop_word_checker_)) {
    stop_word_checker_->destroy();
    OB_DELETEx(ObStopWordChecker, &handler_allocator_, stop_word_checker_);
    stop_word_checker_ = nullptr;
  }

  if (!OB_ISNULL(dict_hub_)) {
    dict_hub_->destroy();
    dict_hub_->~ObFTDictHub();
    handler_allocator_.free(dict_hub_);
    dict_hub_ = nullptr;
  }

  handler_allocator_.reset();
  is_inited_ = false;
}

int ObFTParsePluginData::get_dict_hub(ObFTDictHub *&hub)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_hub_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dict hub is null.", K(ret));
  } else {
    hub = dict_hub_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObFTParseHelper
int ObFTParseHelper::segment(
    const ObFTParserProperty &property,
    const int64_t parser_version,
    const ObIFTParserDesc *parser_desc,
    ObPluginParam *plugin_param,
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
    ObFTParserParam param;
    ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    param.cs_ = cs;
    param.fulltext_ = ft;
    param.ft_length_ = ft_len;
    param.parser_version_ = parser_version;
    param.plugin_param_ = plugin_param;
    param.ngram_token_size_ = property.ngram_token_size_;
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
    : allocator_(nullptr),
    parser_desc_(nullptr),
    plugin_param_(nullptr),
    parser_name_(),
    add_word_flag_(),
    parser_property_(),
    is_inited_(false)
{
}

ObFTParseHelper::~ObFTParseHelper()
{
  reset();
}

int ObFTParseHelper::init(
    common::ObIAllocator *allocator,
    const common::ObString &plugin_name,
    const common::ObString &plugin_properties)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this fulltext parse helper has been initialized", K(ret), KP(parser_desc_), K(is_inited_));
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(plugin_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(allocator), K(plugin_name));
  } else if (OB_FAIL(parser_name_.parse_from_str(plugin_name.ptr(), plugin_name.length()))) {
    LOG_WARN("fail to parse name from cstring", K(ret), K(plugin_name));
  } else if (OB_FAIL(parser_property_.parse_for_parser_helper(parser_name_, plugin_properties))) {
    LOG_WARN("fail to parse parser property from cstring", K(ret), K(plugin_properties), K(parser_name_));
  } else if (OB_FAIL(ObPluginHelper::find_ftparser(parser_name_.get_parser_name().str(),
                                                   parser_desc_, plugin_param_))) {
    if (OB_FUNCTION_NOT_DEFINED == ret) {
      LOG_DEBUG("no such parser", K(parser_name_), K(ret));
    } else {
      LOG_WARN("fail to open plugin handler", K(ret), K(plugin_name));
    }
  } else if (OB_ISNULL(parser_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, parse desc is nullptr", K(ret), KP(parser_desc_));
  } else if (OB_FAIL(set_add_word_flag(*parser_desc_))) {
    LOG_WARN("fail to set add word flag", K(ret), K(parser_name_));
  } else {
    allocator_ = allocator;
    is_inited_ = true;
    LOG_TRACE("succeed to init ft parser helper", K(ret), K(plugin_name), K(plugin_properties), KPC(this));
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObFTParseHelper::reset()
{
  parser_desc_ = nullptr;
  plugin_param_ = nullptr;
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
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == type || type >= CS_TYPE_PINYIN_BEGIN_MARK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, charset info is nullptr", K(ret), K(type));
  } else {
    words.reuse();
    ObAddWord add_word(parser_property_, type, add_word_flag_, *allocator_, words);
    if (OB_FAIL(segment(parser_property_, parser_name_.get_parser_version(), parser_desc_, plugin_param_, cs, fulltext, fulltext_len, *allocator_,
            add_word))) {
      LOG_WARN("fail to segment fulltext", K(ret), K(parser_name_), KP(parser_desc_), KP(cs), KP(fulltext),
          K(fulltext_len), KP(allocator_), K(parser_property_));
    } else {
      doc_length = add_word.get_add_word_count();
    }
  }
  LOG_DEBUG("ft parse segment", K(ret), K(type), K(add_word_flag_), K(parser_name_),
      K(ObString(fulltext_len, fulltext)), K(words.size()));
  return ret;
}

int ObFTParseHelper::check_is_the_same(
    const common::ObString &plugin_name,
    const common::ObString &plugin_properties,
    bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (is_inited_) {
    storage::ObFTParser parser_name;
    ObFTParserProperty parser_property;
    if (OB_UNLIKELY(plugin_name.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(plugin_name));
    } else if (OB_FAIL(parser_name.parse_from_str(plugin_name.ptr(), plugin_name.length()))) {
      LOG_WARN("fail to parse name from cstring", K(ret), K(plugin_name));
    } else if (OB_FAIL(parser_property.parse_for_parser_helper(parser_name, plugin_properties))) {
      LOG_WARN("fail to parse parser property from cstring", K(ret), K(plugin_properties), K(parser_name_));
    } else if (parser_name == parser_name_ && parser_property.is_equal(parser_property_)) {
      is_same = true;
    }
  }
  LOG_TRACE("ft parse helper check is the same", K(is_same), K(plugin_name), K(plugin_properties),
      K(parser_name_), K(parser_property_));
  return ret;
}

int ObFTParseHelper::make_detail_json(
    const ObFTWordMap &words,
    const int64_t doc_length,
    common::ObIJsonBase *&json_root)
{
 int ret = OB_SUCCESS;
  ObJsonObject *root_obj = static_cast<ObJsonObject *>(allocator_->alloc(sizeof(ObJsonObject)));

  ObJsonInt *cnt = static_cast<ObJsonInt *>(allocator_->alloc(sizeof(ObJsonInt)));

  ObJsonArray *token_array = static_cast<ObJsonArray *>(allocator_->alloc(sizeof(ObJsonArray)));

  if (OB_UNLIKELY(OB_ISNULL(root_obj) || OB_ISNULL(cnt) || OB_ISNULL(token_array))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for json", K(ret));
  } else {
    new (root_obj) ObJsonObject(allocator_);
    new (cnt) ObJsonInt(doc_length);
    new (token_array) ObJsonArray(allocator_);

    for (ObFTWordMap::const_iterator it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it) {
      ObString key = it->first.get_word();
      ObJsonObject *node = static_cast<ObJsonObject *>(allocator_->alloc(sizeof(ObJsonObject)));
      ObJsonInt *token_cnt_node = static_cast<ObJsonInt *>(allocator_->alloc(sizeof(ObJsonInt)));
      if (OB_ISNULL(token_cnt_node) || OB_ISNULL(node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc memory for json int", K(ret));
        break;
      } else {
        new (node) ObJsonObject(allocator_);
        int64_t token_cnt = it->second;
        new (token_cnt_node) ObJsonInt(token_cnt);

        if (OB_FAIL(node->add(key, token_cnt_node))) {
          LOG_WARN("Fail to add token count to json", K(ret));
          break;
        } else if (OB_FAIL(token_array->append(node))) {
          LOG_WARN("Fail to append json object", K(ret));
          break;
        } else {
          // pass
        }
      }
    } // for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(root_obj->add(ENTRY_NAME_TOKENS, token_array))) {
        LOG_WARN("Fail to add token array to json", K(ret));
      } else if (OB_FAIL(root_obj->add(ENTRY_NAME_DOC_LEN, cnt))) {
        LOG_WARN("Fail to add doc len to json", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    json_root = root_obj;
  }

  return ret;
}

int ObFTParseHelper::make_token_array_json(
    const ObFTWordMap &words,
    common::ObIJsonBase *&json_root)
{
  int ret = OB_SUCCESS;
  ObJsonArray *token_array = static_cast<ObJsonArray *>(allocator_->alloc(sizeof(ObJsonArray)));
  if (OB_UNLIKELY(OB_ISNULL(token_array))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for json", K(ret));
  } else {
    new (token_array) ObJsonArray(allocator_);
    for (ObFTWordMap::const_iterator it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it) {
      ObString key = it->first.get_word();
      ObJsonString *token = static_cast<ObJsonString *>(allocator_->alloc(sizeof(ObJsonString)));
      if (OB_UNLIKELY(OB_ISNULL(token))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc memory for json int", K(ret));
        break;
      } else {
        new (token) ObJsonString(key);
        if (OB_FAIL(token_array->append(token))) {
          LOG_WARN("Fail to append json string", K(ret));
          break;
        } else {
        }
      }
    } // for
  }
  if (OB_SUCC(ret)) {
    json_root = token_array;
  }
  return ret;
}

int ObFTParseHelper::set_add_word_flag(const ObIFTParserDesc &ftparser_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ftparser_desc.get_add_word_flag(add_word_flag_))) {
    LOG_WARN("failed to set add_word_flag", K(ret));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
