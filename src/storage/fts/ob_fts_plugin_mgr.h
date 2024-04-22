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

#ifndef OB_FTS_PLUGIN_MGR_H_
#define OB_FTS_PLUGIN_MGR_H_

#include "lib/ob_plugin.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "share/ob_define.h"
#include "share/ob_plugin_helper.h"
#include "storage/fts/ob_fts_struct.h"

namespace oceanbase
{
namespace storage
{

class ObFTParser;

class ObTenantFTPluginMgr final
{
public:
  static int register_plugins();
  static void unregister_plugins();

  static int mtl_new(ObTenantFTPluginMgr *&ft_parser_mgr);
  static void mtl_destroy(ObTenantFTPluginMgr *&ft_parser_mgr);
  static ObTenantFTPluginMgr &get_ft_plugin_mgr();
  ~ObTenantFTPluginMgr() { destroy(); }

  int init();
  void destroy();

  int get_plugin_handler(
      const share::ObPluginName &name,
      lib::ObIPluginHandler *&plugin_handler);
  int get_ft_parser(
      const share::ObPluginName &parser_name,
      ObFTParser &parser);
  int check_stopword(
      const ObFTWord &word,
      bool &is_stopword);

  TO_STRING_KV(K_(tenant_id), K_(is_inited), "plugin count", handler_map_.size(),
      "stopword count", stopword_set_.size());

private:
  static const int64_t DEFAULT_PLUGIN_BUCKET_NUM = 53L;
  static const int64_t DEFAULT_STOPWORD_BUCKET_NUM = 37L;
  typedef common::hash::ObHashMap<share::ObPluginName, lib::ObIPluginHandler *> PluginHandlerMap;
  typedef common::hash::ObHashSet<storage::ObFTWord> StopWordSet;

private:
  explicit ObTenantFTPluginMgr(const uint64_t tenant_id);
  int init_plugin_handler();
  int init_and_set_stopword_list();
  template <typename PluginHandler>
  int set_plugin_handler();
  template <typename PluginHandler>
  static int register_plugin();
  template <typename PluginHandler>
  static int unregister_plugin();

private:
  common::ObFIFOAllocator handler_allocator_;
  PluginHandlerMap handler_map_;
  StopWordSet stopword_set_;
  ObCollationType stopword_type_;
  uint64_t tenant_id_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantFTPluginMgr);
};

template <typename PluginHandler>
int ObTenantFTPluginMgr::set_plugin_handler()
{
  int ret = OB_SUCCESS;
  share::ObPluginName plugin_name;
  PluginHandler *plugin_handler = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = handler_allocator_.alloc(sizeof(PluginHandler)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_FTS_LOG(WARN, "fail to allocate plugin handler memory", K(ret));
  } else {
    lib::ObPlugin *plugin = nullptr;
    plugin_handler = new (buf) PluginHandler();
    if (OB_FAIL(plugin_handler->get_plugin(plugin))) {
      STORAGE_FTS_LOG(WARN, "fail to get plugin", K(ret), KPC(plugin_handler));
    } else if (OB_FAIL(plugin_name.set_name(plugin->name_))) {
      STORAGE_FTS_LOG(WARN, "fail to set name", K(ret), KPC(plugin));
    } else if (OB_FAIL(handler_map_.set_refactored(plugin_name,
                                                   static_cast<lib::ObIPluginHandler *>(plugin_handler)))) {
      STORAGE_FTS_LOG(WARN, "fail to set plugin handler", K(ret), K(plugin_name));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    if (OB_NOT_NULL(plugin_handler)) {
      plugin_handler->~PluginHandler();
      plugin_handler = nullptr;
    }
    handler_allocator_.free(buf);
    buf = nullptr;
  }
  return ret;
}

template <typename PluginHandler>
int ObTenantFTPluginMgr::register_plugin()
{
  int ret = OB_SUCCESS;
  PluginHandler plugin_handler;
  lib::ObPlugin *plugin = nullptr;
  lib::ObPluginParam plugin_param;
  if (OB_FAIL(plugin_handler.get_plugin(plugin))) {
    STORAGE_FTS_LOG(WARN, "fail to get plugin", K(ret), K(plugin_handler));
  } else if (FALSE_IT(plugin_param.desc_ = plugin->desc_)) {
  } else if (OB_FAIL(plugin->desc_->init(&plugin_param))) {
    STORAGE_FTS_LOG(WARN, "fail to init plugin descriptor", K(ret), K(plugin_param));
  }
  return ret;
}

template <typename PluginHandler>
int ObTenantFTPluginMgr::unregister_plugin()
{
  int ret = OB_SUCCESS;
  PluginHandler plugin_handler;
  lib::ObPlugin *plugin = nullptr;
  lib::ObPluginParam plugin_param;
  if (OB_FAIL(plugin_handler.get_plugin(plugin))) {
    STORAGE_FTS_LOG(WARN, "fail to get plugin", K(ret), K(plugin_handler));
  } else if (FALSE_IT(plugin_param.desc_ = plugin->desc_)) {
  } else if (OB_FAIL(plugin->desc_->deinit(&plugin_param))) {
    STORAGE_FTS_LOG(WARN, "fail to deinit plugin descriptor", K(ret));
  }
  return ret;
}

#define OB_FT_PLUGIN_MGR ObTenantFTPluginMgr::get_ft_plugin_mgr()

} // end storage
} // end oceanbase
#endif // OB_FTS_PLUGIN_MGR_H_
