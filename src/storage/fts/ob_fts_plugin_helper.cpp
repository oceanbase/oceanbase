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

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_plugin_helper.h"

#include "lib/json_type/ob_json_tree.h"
#include "lib/worker.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "share/ob_force_print_log.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_ft_token_processor.h"

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
  } else if (OB_FAIL(init_stop_token_checker_gen())) {
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

int ObFTParsePluginData::get_stop_token_checker(
    const ObCollationType coll,
    ObStopTokenChecker &stop_token_checker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObFTParsePluginData is not initialized", K(ret));
  } else if (OB_FAIL(stop_token_checker_gen_->get_stop_token_checker_by_coll(coll, stop_token_checker))) {
    LOG_WARN("failed to get stop token checker", K(ret), K(coll));
  }
  return ret;
}

int ObFTParsePluginData::init_stop_token_checker_gen()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stop_token_checker_gen_ = OB_NEWx(ObStopTokenCheckerGen, &handler_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create stop token checker gen", K(ret));
  } else if (OB_FAIL(stop_token_checker_gen_->init())) {
    LOG_WARN("failed to init stop token checker gen", K(ret));
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObStopTokenCheckerGen, &handler_allocator_, stop_token_checker_gen_);
    stop_token_checker_gen_ = nullptr;
  }
  return ret;
}

int ObFTParsePluginData::init_dict_hub()
{
  // make dict
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_hub_ = OB_NEWx(ObFTDictHub, &handler_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for dict hub.", K(ret));
  } else if (OB_FAIL(dict_hub_->init())) {
    LOG_WARN("Failed to init dict hub.", K(ret));
  }
  return ret;
}

void ObFTParsePluginData::destroy()
{
  if (OB_NOT_NULL(stop_token_checker_gen_)) {
    stop_token_checker_gen_->reset();
    OB_DELETEx(ObStopTokenCheckerGen, &handler_allocator_, stop_token_checker_gen_);
    stop_token_checker_gen_ = nullptr;
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

ObFTParseHelper::ObFTParseHelper()
    : allocator_(nullptr),
    parser_desc_(nullptr),
    plugin_param_(nullptr),
    parser_name_(),
    process_token_flag_(),
    parser_property_(),
    fts_index_type_(share::schema::OB_FTS_INDEX_TYPE_INVALID),
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
    const common::ObString &plugin_properties,
    const share::schema::ObFTSIndexType fts_index_type)
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
  } else if (OB_FAIL(set_process_token_flag(*parser_desc_))) {
    LOG_WARN("fail to set add word flag", K(ret), K(parser_name_));
  } else {
    allocator_ = allocator;
    fts_index_type_ = fts_index_type;
    is_inited_ = true;
    LOG_TRACE("succeed to init ft parser helper", K(ret), K(plugin_name), K(plugin_properties), K(fts_index_type), KPC(this));
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
  process_token_flag_.clear();
  fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_INVALID;
  is_inited_ = false;
}

int ObFTParseHelper::segment(
    const ObObjMeta &meta,
    const char *fulltext,
    const int64_t fulltext_len,
    int64_t &doc_length,
    ObFTTokenMap &ft_token_map) const
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;
  ObCollationType type = meta.get_collation_type();
  ObString ft_str(fulltext_len, fulltext);
  ObString regularized_ft_str;
  const bool need_tolower = process_token_flag_.casedown_token();
  doc_length = 0;
  ft_token_map.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("this fulltext parser helper hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == type || type >= CS_TYPE_PINYIN_BEGIN_MARK ||
                         nullptr == fulltext || fulltext_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), KP(fulltext), K(fulltext_len));
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, charset info is nullptr", K(ret), K(type));
  } else if (need_tolower && OB_FAIL(ObCharset::tolower(cs, ft_str, regularized_ft_str, *allocator_))) {
    LOG_WARN("fail to tolower fulltext", K(ret));
  } else if (need_tolower && OB_UNLIKELY(regularized_ft_str.empty())) {
    // illegal fulltext
  } else {
    ObFTTokenProcessor token_processor(*allocator_);
    ObFTParserParam param;
    ObITokenIterator *iter = nullptr;
    param.scratch_alloc_ = allocator_;
    param.metadata_alloc_ = allocator_;
    param.cs_ = cs;
    param.fulltext_ = need_tolower ? regularized_ft_str.ptr() : fulltext;
    param.ft_length_ = need_tolower ? regularized_ft_str.length() : fulltext_len;
    param.parser_version_ = parser_name_.get_parser_version();
    param.plugin_param_ = plugin_param_;
    param.ngram_token_size_ = parser_property_.ngram_token_size_;
    param.ik_param_.mode_ = (parser_property_.ik_mode_smart_ ? ObFTIKParam::Mode::SMART : ObFTIKParam::Mode::MAX_WORD);
    param.min_ngram_size_ = parser_property_.min_ngram_token_size_;
    param.max_ngram_size_ = parser_property_.max_ngram_token_size_;

    if (OB_FAIL(token_processor.init(parser_property_, meta, process_token_flag_, &ft_token_map))) {
      LOG_WARN("fail to initialize add word", K(ret), K(token_processor));
    } else if (OB_FAIL(parser_desc_->segment(&param, iter))) {
      LOG_WARN("fail to get token iterator", K(ret), K(param));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, token iterator is nullptr", K(ret), KP(iter));
    } else {
      const char *word = nullptr;
      int64_t word_len = 0;
      int64_t char_cnt = 0;
      int64_t word_freq = 0;
      int64_t token_interval_cnt = 0;
      constexpr int64_t FTS_SEGMENT_CHECK_STATUS_TOKEN_INTERVAL_CNT = 100;
      int64_t simple_pos = 0;
      const bool need_pos_list = (fts_index_type_ == share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_token(word, word_len, char_cnt, word_freq))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next token", K(ret), KPC(iter));
          }
        } else if (OB_FAIL(token_processor.process_token(need_pos_list, word, word_len, char_cnt, simple_pos++))) {
          LOG_WARN("fail to process token", K(ret), KP(word), K(word_len), K(char_cnt), K(need_pos_list));
        } else if (++token_interval_cnt >= FTS_SEGMENT_CHECK_STATUS_TOKEN_INTERVAL_CNT) {
          if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("worker interrupt during fulltext segment", K(ret));
          } else {
            token_interval_cnt = 0;
          }
        }
      }
      if (OB_ITER_END == ret) {
        doc_length = token_processor.get_non_stop_token_count();
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      parser_desc_->free_token_iter(&param, iter);
      iter = nullptr;
    }
  }
  LOG_DEBUG("ft parse segment", K(ret), K(type), K(process_token_flag_), K(parser_name_),
      K(ObString(fulltext_len, fulltext)), K(ft_token_map.size()));
  return ret;
}

int ObFTParseHelper::check_is_the_same(
    const common::ObString &plugin_name,
    const common::ObString &plugin_properties,
    const share::schema::ObFTSIndexType fts_index_type,
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
    } else if (parser_name == parser_name_
        && parser_property.is_equal(parser_property_)
        && fts_index_type == fts_index_type_) {
      is_same = true;
    }
  }
  LOG_TRACE("ft parse helper check is the same", K(is_same), K(plugin_name), K(plugin_properties),
      K(fts_index_type), K_(parser_name), K_(parser_property), K_(fts_index_type));
  return ret;
}

int ObFTParseHelper::make_detail_json(
    const ObFTTokenMap &ft_token_map,
    const int64_t doc_length,
    common::ObIJsonBase *&json_root)
{
 int ret = OB_SUCCESS;

 ObJsonObject *root_obj = nullptr;

 ObJsonInt *cnt = nullptr;

 ObJsonArray *token_array = nullptr;

 if (OB_ISNULL(root_obj = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
   LOG_WARN("Fail to alloc memory for json", K(ret));
 } else if (OB_ISNULL(cnt = OB_NEWx(ObJsonInt, allocator_, doc_length))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
   LOG_WARN("Fail to alloc memory for json", K(ret));
 } else if (OB_ISNULL(token_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
   LOG_WARN("Fail to alloc memory for json", K(ret));
 } else {
   for (ObFTTokenMap::const_iterator it = ft_token_map.begin(); OB_SUCC(ret) && it != ft_token_map.end(); ++it) {
     ObString key = it->first.get_token().get_string();
     ObJsonObject *node = nullptr;
     ObJsonInt *token_cnt_node = nullptr;
     if (OB_ISNULL(node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("Fail to alloc memory for json int", K(ret));
     } else if (OB_ISNULL(token_cnt_node = OB_NEWx(ObJsonInt, allocator_, it->second.count_))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("Fail to alloc memory for json", K(ret));
     } else if (OB_FAIL(node->add(key, token_cnt_node))) {
       LOG_WARN("Fail to add token count to json", K(ret));
     } else if (OB_FAIL(token_array->append(node))) {
       LOG_WARN("Fail to append json object", K(ret));
     } else {
       // pass
     }

     if (OB_FAIL(ret)) {
       OB_DELETEx(ObJsonObject, allocator_, node);
       OB_DELETEx(ObJsonInt, allocator_, token_cnt_node);
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
  } else {
    OB_DELETEx(ObJsonObject, allocator_, root_obj);
    OB_DELETEx(ObJsonInt, allocator_, cnt);
    OB_DELETEx(ObJsonArray, allocator_, token_array);
  }

  return ret;
}

int ObFTParseHelper::make_token_array_json(
    const ObFTTokenMap &ft_token_map,
    common::ObIJsonBase *&json_root)
{
  int ret = OB_SUCCESS;
  ObJsonArray *token_array = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(token_array = OB_NEWx(ObJsonArray, allocator_, allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for json", K(ret));
  } else {
    for (ObFTTokenMap::const_iterator it = ft_token_map.begin(); OB_SUCC(ret) && it != ft_token_map.end(); ++it) {
      ObString key = it->first.get_token().get_string();
      ObJsonString *token = nullptr;
      if (OB_UNLIKELY(OB_ISNULL(token = OB_NEWx(ObJsonString, allocator_, key)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc memory for json int", K(ret));
      } else {
        if (OB_FAIL(token_array->append(token))) {
          LOG_WARN("Fail to append json string", K(ret));
          OB_DELETEx(ObJsonString, allocator_, token);
        } else {
        }
      }
    } // for
  }
  if (OB_SUCC(ret)) {
    json_root = token_array;
  } else {
    OB_DELETEx(ObJsonArray, allocator_, token_array);
  }
  return ret;
}

int ObFTParseHelper::set_process_token_flag(const ObIFTParserDesc &ftparser_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ftparser_desc.get_add_word_flag(process_token_flag_))) {
    LOG_WARN("failed to set process token flag", K(ret));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
