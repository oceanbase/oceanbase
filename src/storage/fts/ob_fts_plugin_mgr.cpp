/**
 * Copyright (c) 2024 OceanBase
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

#include "share/rc/ob_tenant_base.h"
#include "share/ob_force_print_log.h"
#include "storage/fts/ob_fts_plugin_mgr.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_helper.h"

#include "storage/fts/ob_fts_buildin_parser_register.ipp"

namespace oceanbase
{
namespace storage
{

ObTenantFTPluginMgr::ObTenantFTPluginMgr(const uint64_t tenant_id)
  : handler_allocator_(tenant_id),
    handler_map_(),
    stopword_set_(),
    stopword_type_(ObCollationType::CS_TYPE_INVALID),
    tenant_id_(tenant_id),
    is_inited_(false)
{
}

void ObTenantFTPluginMgr::destroy()
{
  FLOG_INFO("destroy ObTenantFTPluginMgr", KP(this));
  for(PluginHandlerMap::const_iterator iter = handler_map_.begin(); iter != handler_map_.end(); ++iter) {
    oceanbase::lib::ObIPluginHandler *handler = iter->second;
    handler_allocator_.free(handler);
  }
  stopword_set_.destroy();
  stopword_type_ = ObCollationType::CS_TYPE_INVALID;
  handler_map_.destroy();
  handler_allocator_.reset();
  is_inited_ = false;
}

int ObTenantFTPluginMgr::register_plugins()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(register_plugin<ObBuildInWhitespaceFTParser>())) {
    LOG_WARN("fail to register default fulltext parser", K(ret));
  } else if (OB_FAIL(register_plugin<ObBuildInNgramFTParser>())) {
    LOG_WARN("fail to register ngram fulltext parser", K(ret));
  } else if (OB_FAIL(register_plugin<ObBuildInBEngFTParser>())) {
    LOG_WARN("fail to register basic english fulltext parser", K(ret));
  }
  return ret;
}

void ObTenantFTPluginMgr::unregister_plugins()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unregister_plugin<ObBuildInWhitespaceFTParser>())) {
    LOG_ERROR("fail to unregister default fulltext parser", K(ret));
  } else if (OB_FAIL(unregister_plugin<ObBuildInNgramFTParser>())) {
    LOG_ERROR("fail to unregister ngram fulltext parser", K(ret));
  } else if (OB_FAIL(unregister_plugin<ObBuildInBEngFTParser>())) {
    LOG_ERROR("fail to unregister basic english fulltext parser", K(ret));
  }
}

int ObTenantFTPluginMgr::mtl_new(ObTenantFTPluginMgr *&ft_parser_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ft_parser_mgr = OB_NEW(ObTenantFTPluginMgr, ObMemAttr(tenant_id, "FTParserMgr"), tenant_id);
  if (OB_ISNULL(ft_parser_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fulltext parser manager memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObTenantFTPluginMgr::mtl_destroy(ObTenantFTPluginMgr *&ft_parser_mgr)
{
  if (OB_ISNULL(ft_parser_mgr)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fulltext parser manager is nullptr", KP(ft_parser_mgr));
  } else {
    OB_DELETE(ObTenantFTPluginMgr, oceanbase::ObModIds::OMT_TENANT, ft_parser_mgr);
    ft_parser_mgr = nullptr;
  }
}

__attribute__((weak))
ObTenantFTPluginMgr &ObTenantFTPluginMgr::get_ft_plugin_mgr()
{
  return (*(MTL(ObTenantFTPluginMgr *)));
}

int ObTenantFTPluginMgr::init()
{
  int ret = OB_SUCCESS;
  const lib::ObMemAttr mem_attr(tenant_id_, "FTPluginHandle");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantFTParserMgr has been initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(handler_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                                             mem_attr))) {
    LOG_WARN("fail to init tenant plugin handler allocator", K(ret));
  } else if (OB_FAIL(init_plugin_handler())) {
    LOG_WARN("fail to init plugin handler", K(ret));
  } else if (OB_FAIL(init_and_set_stopword_list())) {
    LOG_WARN("fail to init and set stopword list", K(ret));
  } else {
    is_inited_ = true;
    FLOG_INFO("succeed to initialize ObTenantFTPluginMgr", KP(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObTenantFTPluginMgr::init_plugin_handler()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handler_map_.create(DEFAULT_PLUGIN_BUCKET_NUM, "FTPluginMap", "FTPluginMap", tenant_id_))) {
    LOG_WARN("fail to init plugin handlers map", K(ret));
  } else if (OB_FAIL(set_plugin_handler<ObBuildInWhitespaceFTParser>())) {
    LOG_WARN("fail to set default fulltext parser", K(ret));
  } else if (OB_FAIL(set_plugin_handler<ObBuildInNgramFTParser>())) {
    LOG_WARN("fail to set ngram fulltext parser", K(ret));
  } else if (OB_FAIL(set_plugin_handler<ObBuildInBEngFTParser>())) {
    LOG_WARN("fail to set basic english fulltext parser", K(ret));
  }
  return ret;
}

int ObTenantFTPluginMgr::init_and_set_stopword_list()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet", tenant_id_))) {
    LOG_WARN("fail to create stop word set", K(ret));
  } else {
    stopword_type_ = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
        LOG_WARN("fail to set stop word", K(ret), K(stopword));
      }
    }
  }
  return ret;
}

int ObTenantFTPluginMgr::get_plugin_handler(
    const share::ObPluginName &name,
    lib::ObIPluginHandler *&plugin_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFTPluginMgr hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(name));
  } else if (OB_FAIL(handler_map_.get_refactored(name, plugin_handler))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_FUNCTION_NOT_DEFINED;
      LOG_WARN("Function not defined", K(ret));
      LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, name.len(), name.str());
    }
    LOG_WARN("fail to get plugin handler", K(ret), K(name));
  }
  return ret;
}

int ObTenantFTPluginMgr::get_ft_parser(
    const share::ObPluginName &parser_name,
    ObFTParser &parser)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  lib::ObIPluginHandler *handler = nullptr;
  lib::ObPlugin *plugin = nullptr;
  if (OB_FAIL(get_plugin_handler(parser_name, handler))) {
    LOG_WARN("fail to get plugin handler", K(ret), K(parser_name));
  } else if (OB_ISNULL(handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, parser handler is nullptr", K(ret), K(parser_name), KP(handler));
  } else if (OB_FAIL(handler->get_plugin(plugin))) {
    LOG_WARN("fail to get plugin", K(ret), K(parser_name), KPC(handler));
  } else if (OB_ISNULL(plugin)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin is nullptr", K(ret), K(parser_name), KPC(handler));
  } else if (OB_UNLIKELY(lib::ObPluginType::OB_FT_PARSER_PLUGIN != plugin->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin type isn't fulltext parser", K(ret), K(parser_name), KPC(plugin));
  } else if (OB_UNLIKELY(plugin->version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin version is invalid", K(ret), K(parser_name), KPC(plugin));
  } else {
    version = plugin->version_;
    parser.set_name_and_version(parser_name, version);
  }
  return ret;
}

int ObTenantFTPluginMgr::check_stopword(
    const ObFTWord &word,
    bool &is_stopword)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "ChkStopWord"));
  common::ObString cmp_word_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFTPluginMgr hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(word.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("word is empty", K(ret), K(word));
  } else {
    const bool conv_cs = word.get_collation_type() != stopword_type_;
    if (conv_cs && OB_FAIL(common::ObCharset::charset_convert(allocator, word.get_word(), word.get_collation_type(),
            stopword_type_, cmp_word_str))) {
      LOG_WARN("fail to convert charset", K(ret), K(word), K(stopword_type_));
    } else {
      ObFTWord cmp_word(cmp_word_str.length(), cmp_word_str.ptr(), stopword_type_);
      ret = stopword_set_.exist_refactored(conv_cs ? cmp_word : word);
      if (OB_HASH_NOT_EXIST == ret) {
        is_stopword = false;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        is_stopword = true;
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the exist of hastset shouldn't return success", K(ret), K(word), K(conv_cs), K(cmp_word));
      } else {
        LOG_WARN("fail to do exist", K(ret), K(word), K(conv_cs), K(cmp_word));
      }
    }
  }
  return ret;
}

} // end storage
} // end oceanbase
