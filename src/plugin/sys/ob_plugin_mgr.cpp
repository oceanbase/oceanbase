/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_handle.h"
#include "plugin/sys/ob_plugin_entry_handle.h"
#include "plugin/sys/ob_plugin_load_param.h"
#include "plugin/sys/ob_plugin_builtin.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace oceanbase::lib;

namespace oceanbase {
namespace plugin {

int ObPluginMgr::init(const ObString &plugin_dir)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = OB_PLUGIN_MEMORY_LABEL;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_plugin_handle_maps(mem_attr))) {
    LOG_WARN("failed to init plugin entry handle map", K(ret));
  } else if (OB_FAIL(plugin_handle_map_.create(DEFAULT_PLUGIN_BUCKET_NUM, mem_attr))) {
    LOG_WARN("failed to init plugin handle map", K(ret));
  } else {
    inited_     = true;
    plugin_dir_ = plugin_dir;
  }
  LOG_INFO("init plugin manager done", K(ret), K(plugin_dir));
  return ret;
}
ObPluginMgr::~ObPluginMgr()
{
  destroy();
}

int ObPluginMgr::init_plugin_handle_maps(const ObMemAttr &mem_attr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    for (int64_t i = 0; i < OBP_PLUGIN_TYPE_MAX && OB_SUCC(ret); i++) {
      if (OB_FAIL(entry_handle_maps_[i].create(DEFAULT_PLUGIN_BUCKET_NUM, mem_attr))) {
        LOG_WARN("failed to init plugin handle map", K(ret));
      }
    }
  }
  return ret;
}

class PluginHandleReleaseCallback
{
public:
  PluginHandleReleaseCallback(const ObMemAttr &mem_attr) : mem_attr_(mem_attr)
  {}

  int operator()(common::hash::HashMapPair<ObString, ObPluginHandle *> &node) const
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(node.second)) {
      OB_DELETE(ObPluginHandle, mem_attr_, node.second);
      node.second = nullptr;
    }
    return ret;
  }

private:
  const ObMemAttr &mem_attr_;
};

class PluginEntryHandleReleaseCallback
{
public:
  PluginEntryHandleReleaseCallback(const ObMemAttr &mem_attr) : mem_attr_(mem_attr)
  {}

  int operator()(common::hash::HashMapPair<ObString, ObPluginMgr::PluginEntryList *> &node) const
  {
    int ret = OB_SUCCESS;
    using PluginEntryList = ObPluginMgr::PluginEntryList;
    if (OB_NOT_NULL(node.second)) {
      PluginEntryList *entry_list = node.second;
      for (int64_t i = 0; i < entry_list->count(); i++) {
        ObPluginEntryHandle *entry = entry_list->at(i);
        OB_DELETE(ObPluginEntryHandle, mem_attr_, entry);
      }
      OB_DELETE(PluginEntryList, mem_attr_, node.second);
      node.second = nullptr;
    }
    return ret;
  }

private:
  const ObMemAttr &mem_attr_;
};

void ObPluginMgr::destroy()
{
  int ret = OB_SUCCESS;

  inited_  = false;

  ObMemAttr mem_attr;
  mem_attr.label_ = OB_PLUGIN_MEMORY_LABEL;

  // we must relase plugin entry handle before plugin handle
  PluginEntryHandleReleaseCallback plugin_entry_release_callback(mem_attr);
  for (int64_t i = 0; i < OBP_PLUGIN_TYPE_MAX; i++) {
    entry_handle_maps_[i].foreach_refactored(plugin_entry_release_callback);
    entry_handle_maps_[i].destroy();
  }

  PluginHandleReleaseCallback plugin_release_callback(mem_attr);
  plugin_handle_map_.foreach_refactored(plugin_release_callback);
  plugin_handle_map_.destroy();
}

int ObPluginMgr::find_plugin(ObPluginType plugin_type, const ObString &plugin_name, ObPluginEntryHandle *&entry_handle)
{
  int ret = OB_SUCCESS;

  PluginEntryList *plugin_entry_list = nullptr;
  entry_handle = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (OB_FAIL(find_plugin_entry_list(plugin_type, plugin_name, plugin_entry_list))) {
  } else if (OB_ISNULL(plugin_entry_list) || plugin_entry_list->count() == 0) {
    ret = OB_FUNCTION_NOT_DEFINED;
  } else if (FALSE_IT(entry_handle = plugin_entry_list->at(0))) {
  } else if (OB_ISNULL(entry_handle)) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("got a null entry handle", KP(entry_handle), K(plugin_entry_list->count()), K(ret));
  } else if (!entry_handle->ready()) {
    entry_handle = nullptr;
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("find the plugin but it's not ready", K(plugin_name), K(ret));
  } else {
    LOG_DEBUG("find a plugin", K(plugin_type), K(plugin_name));
  }
  return ret;
}

static ObPluginVersion plugin_version(const ObPluginEntryHandle *plugin_entry)
{
  ObPluginVersion version = 0;
  if (OB_NOT_NULL(plugin_entry)) {
    version = plugin_entry->library_version();
  }
  return version;
}

struct ObPluginEntryVersionComparator
{
  bool operator() (ObPluginEntryHandle * const &entry1, const ObPluginVersion &version2) const
  {
    bool bret = false;
    ObPluginVersion version1 = plugin_version(entry1);

    bret = (version1 > version2);
    LOG_DEBUG("plugin entry version comparator", K(version1), K(version2));
    return bret;
  }
};

struct ObPluginEntryVersionEqualer
{
  bool operator() (ObPluginEntryHandle * const &entry1, const ObPluginVersion &version2) const
  {
    bool bret = false;
    ObPluginVersion version1 = plugin_version(entry1);

    bret = (version1 == version2);
    return bret;
  }
};

struct ObPluginEntry2EntryComparator
{
  bool operator() (const ObPluginEntryHandle * const &entry1, const ObPluginEntryHandle * const &entry2) const
  {
    bool bret = false;
    ObPluginVersion version1 = plugin_version(entry1);
    ObPluginVersion version2 = plugin_version(entry2);

    bret = (version1 > version2);
    return bret;
  }
};

int ObPluginMgr::find_plugin(ObPluginType type,
                             const ObString &name,
                             ObPluginVersion version,
                             ObPluginEntryHandle *&entry_handle)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("find plugin", K(type), K(name), K(version));

  entry_handle = nullptr;
  PluginEntryList *plugin_entry_list = nullptr;
  PluginEntryList::iterator entry_iterator;
  ObPluginEntryVersionComparator comparator;
  ObPluginEntryVersionEqualer equaler;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (OB_FAIL(find_plugin_entry_list(type, name, plugin_entry_list))) {
  } else if (OB_ISNULL(plugin_entry_list) || plugin_entry_list->count() == 0) {
    ret = OB_FUNCTION_NOT_DEFINED;
  } else if (OB_FAIL(plugin_entry_list->find(version, entry_iterator, comparator, equaler))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_FUNCTION_NOT_DEFINED;
    }
    LOG_DEBUG("failed to find plugin entry", K(ret));
  } else if (entry_iterator == plugin_entry_list->end()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find plugin entry but got an end iterator", K(plugin_entry_list->count()));
  } else if (OB_ISNULL(entry_handle = *entry_iterator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find a plugin entry but is null", K(plugin_entry_list->count()), K(ret));
  } else if (!entry_handle->ready()) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("plugin is not ready", K(ret), KPC(entry_handle));
  } else {
    LOG_DEBUG("got a plugin entry", KPC(entry_handle));
  }
  return ret;
}

int ObPluginMgr::find_plugin_entry_list(ObPluginType type, const ObString &name, PluginEntryList *&plugin_entry_list)
{
  int ret = OB_SUCCESS;

  plugin_entry_list = nullptr;
  if (OB_UNLIKELY(type <= OBP_PLUGIN_TYPE_INVALID || type >= OBP_PLUGIN_TYPE_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("try to find invalid plugin type", K(ret), K(type));
  } else if (OB_FAIL(entry_handle_maps_[type].get_refactored(name, plugin_entry_list))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_FUNCTION_NOT_DEFINED;
    }
  }

  if (OB_FUNCTION_NOT_DEFINED == ret) {
    LOG_DEBUG("failed to find plugin entry list", K(type), K(name), K(ret));
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to find plugin entry list", K(type), K(name), K(ret));
  }
  return ret;
}

int ObPluginMgr::register_plugin(const ObPluginEntry &plugin_entry)
{
  int ret = OB_SUCCESS;
  ObPluginEntryHandle *entry_handle = nullptr;
  PluginEntryList *plugin_entry_list = nullptr;
  PluginEntryList::iterator entry_iterator;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (!plugin_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin is invalid", K(ret), K(plugin_entry));
  } else if (OB_ISNULL(plugin_entry.plugin_handle) || OB_ISNULL(plugin_entry.plugin_handle->plugin())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin entry is invalid: plugin_handle is null or no plugin pointer",
             K(ret), KPC(plugin_entry.plugin_handle));
  } else if (OB_FUNCTION_NOT_DEFINED == find_plugin_entry_list(plugin_entry.interface_type,
                                                          plugin_entry.name,
                                                          plugin_entry_list)) {
    if (OB_ISNULL(plugin_entry_list = OB_NEW(PluginEntryList, OB_PLUGIN_MEMORY_LABEL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate plugin entry list", K(ret));
    } else if (OB_FAIL(entry_handle_maps_[plugin_entry.interface_type]
                         .set_refactored(plugin_entry.name, plugin_entry_list))) {
      LOG_WARN("failed to insert plugin entry list into plugin entry handle map", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ENTRY_NOT_EXIST != (ret = plugin_entry_list->find(plugin_entry.plugin_handle->plugin()->version,
                                                            entry_iterator,
                                                            ObPluginEntryVersionComparator(),
                                                            ObPluginEntryVersionEqualer()))) {
    if (OB_SUCC(ret)) {
      ret = OB_ENTRY_EXIST;
    }
    LOG_WARN("plugin with the same version already exists", K(plugin_entry), K(ret));
  } else if (OB_ISNULL(entry_handle = OB_NEW(ObPluginEntryHandle, OB_PLUGIN_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate plugin handle", K(ret));
  } else if (OB_FAIL(entry_handle->init(plugin_entry))) {
    LOG_WARN("failed to init plugin entry handle", K(ret));
  } else if (OB_FAIL(entry_handle->init_plugin())) {
    LOG_WARN("failed to init plugin", K(ret), KPC(entry_handle));
  } else if (OB_FAIL(plugin_entry_list->insert(entry_handle, entry_iterator, ObPluginEntry2EntryComparator()))) {
    LOG_WARN("failed to insert plugin entry into list", K(ret), K(plugin_entry));
  } else {
    LOG_INFO("register plugin success",
             KPC(entry_handle), K(ObPluginAdaptor(plugin_entry.plugin_handle->plugin())), K(ret), KCSTRING(lbt()));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(entry_handle)) {
    OB_DELETE(ObPluginEntryHandle, OB_PLUGIN_MEMORY_LABEL, entry_handle);
  }

  return ret;
}

int ObPluginMgr::install_library(const ObString &dl_name)
{
  int ret = OB_SUCCESS;

  ObPluginHandle *plugin_handle = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (OB_FAIL(find_or_load_dl(dl_name, plugin_handle))) {
    LOG_WARN("cannot find or load dl", K(dl_name));
  } else if (OB_FAIL(load_plugin(plugin_handle))) {
    LOG_WARN("failed to load plugin suite", K(ret), K(dl_name));
  }
  return ret;
}

int ObPluginMgr::find_or_load_dl(const ObString &dl_name, ObPluginHandle *&plugin_handle)
{
  int ret = OB_SUCCESS;
  plugin_handle = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (dl_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dl name is empty", K(dl_name));
  } else if (OB_FAIL(plugin_handle_map_.get_refactored(dl_name, plugin_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_FUNCTION_NOT_DEFINED;
    } else {
      LOG_WARN("failed to find dl", K(ret));
    }
  } else if (OB_ISNULL(plugin_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find plugin dl success but handle is null", K(ret));
  } else if (OB_FAIL(plugin_handle->valid())) {
    LOG_WARN("suite handle is invalid", K(ret));
    plugin_handle = nullptr;
  }

  if (OB_FUNCTION_NOT_DEFINED == ret) {
    if (plugin_dir_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot install library because plugin dir is empty.");
    }

    // load dynamic library
    ObPluginHandle *this_plugin_handle = nullptr;
    if (OB_SUCC(ret)) {
    } else if (OB_ISNULL(this_plugin_handle = OB_NEW(ObPluginHandle, OB_PLUGIN_MEMORY_LABEL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for plugin suite handle", K(ret));
    } else if (OB_FAIL(this_plugin_handle->init(this, plugin_dir_, dl_name))) {
      LOG_WARN("failed to init suite handle", K(ret), K(dl_name));
    } else if (OB_FAIL(plugin_handle_map_.set_refactored(this_plugin_handle->name(), this_plugin_handle))) {
      LOG_WARN("failed to insert suite handle into map", K(ret), K(dl_name));
      OB_DELETE(ObPluginHandle, OB_PLUGIN_MEMORY_LABEL, this_plugin_handle);
      this_plugin_handle = nullptr;
    } else if (OB_FAIL(this_plugin_handle->valid())) {
      LOG_WARN("suite handle is invalid", K(ret), K(dl_name));
    } else {
      plugin_handle = this_plugin_handle;
    }
  }
  return ret;
}

int ObPluginMgr::load_builtin_plugins()
{
  int ret = OB_SUCCESS;

  ObMemAttr mem_attr;
  mem_attr.label_ = OB_PLUGIN_MEMORY_LABEL;
  ObArray<ObBuiltinPlugin> builtin_plugins;
  builtin_plugins.set_attr(mem_attr);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (OB_FAIL(plugin_register_global_plugins(builtin_plugins))) {
    LOG_WARN("failed to get global builtin plugins", K(ret));
  } else {
    LOG_INFO("got builtin plugins", K(builtin_plugins.count()));

    for (int64_t i = 0; i < builtin_plugins.count() && OB_SUCC(ret); i++) {
      ObBuiltinPlugin &builtin_plugin = builtin_plugins.at(i);
      if (OB_ISNULL(builtin_plugin.plugin.plugin())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got a null builtin plugin pointer", K(ret), K(builtin_plugin));
      } else if (OB_FAIL(load_builtin_plugin(builtin_plugin))) {
        LOG_WARN("failed to load builtin plugin", K(builtin_plugin), K(ret));
        LOG_DBA_ERROR_V2(OB_SERVER_LOAD_BUILTIN_PLUGIN_FAIL, ret, "load builtin plugin fail: ", builtin_plugin);
      }
    }
  }
  return ret;
}

int ObPluginMgr::load_builtin_plugin(const ObBuiltinPlugin &builtin_plugin)
{
  int ret = OB_SUCCESS;

  ObPlugin *plugin = builtin_plugin.plugin.plugin();
  ObPluginHandle *plugin_handle = nullptr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(plugin));
  } else if (OB_ISNULL(plugin_handle = OB_NEW(ObPluginHandle, OB_PLUGIN_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for plugin suite handle", K(ret));
  } else if (OB_FAIL(plugin_handle->init(this, OBP_PLUGIN_API_VERSION_CURRENT, builtin_plugin.name, plugin))) {
    LOG_WARN("failed to init plugin suite handle", K(ret));
  } else if (OB_FAIL(plugin_handle_map_.set_refactored(plugin_handle->name(), plugin_handle))) {
    LOG_WARN("failed to insert suite handle into suite handle map", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(plugin_handle)) {
      plugin_handle->destroy();
      OB_DELETE(ObPluginHandle, OB_PLUGIN_MEMORY_LABEL, plugin_handle);
    }
  } else if (OB_FAIL(load_plugin(plugin_handle))) {
    LOG_WARN("failed load plugin in plugin suite", K(ret));
  } else {
    LOG_INFO("load plugin suite success", KPC(plugin_handle));
  }

  return ret;
}

int ObPluginMgr::load_plugin(ObPluginHandle *plugin_handle)
{
  int ret = OB_SUCCESS;
  ObPlugin *plugin = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(plugin_handle)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(plugin = plugin_handle->plugin())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no plugin in plugin suite");
  } else if (!ObPluginHandle::plugin_is_valid(plugin) || OB_ISNULL(plugin->init)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin is invalid", K(ret), K(ObPluginAdaptor(plugin)));
  } else if (OB_FAIL(plugin->init(reinterpret_cast<ObPluginDatum>(&plugin_handle->plugin_param())))) {
    LOG_WARN("failed to call plugin init", K(ret), K(ObPluginAdaptor(plugin)), K(plugin_handle->plugin_param()));
  } else {
    LOG_INFO("load plugin success", KPC(plugin_handle));
  }
  return ret;
}

int ObPluginMgr::load_dynamic_plugins(const ObString &plugins_load)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = OB_PLUGIN_MEMORY_LABEL;

  ObArray<ObPluginLoadParam> plugin_load_params;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else if (plugins_load.empty()) {
    // do nothing
  } else if (FALSE_IT(plugin_load_params.set_attr(mem_attr))) {
  } else if (OB_FAIL(ObPluginLoadParamParser::parse(plugins_load, plugin_load_params))) {
    LOG_WARN("[ignore] failed to parse plugin load param", K(plugins_load), K(ret));
    LOG_DBA_WARN_V2(OB_SERVER_LOAD_DYNAMIC_PLUGIN_FAIL, ret, "parse loads_plugin param fail: ", plugins_load);
    ret = OB_SUCCESS;
  } else {
    ObPluginLoadParam plugin_load_param;
    const int64_t param_count = plugin_load_params.count();
    for (int64_t i = 0; i < param_count && OB_SUCC(ret); i++) {
      if (OB_FAIL(plugin_load_params.at(i, plugin_load_param))) {
        LOG_WARN("failed to get param", K(i), K(ret));
      } else if (plugin_load_param.library_name.empty() ||
                 plugin_load_param.load_option.value() <= ObPluginLoadOption::INVALID ||
                 plugin_load_param.load_option.value() >= ObPluginLoadOption::MAX) {
        LOG_WARN("[ignore] invalid plugin load param", K(plugin_load_param), K(OB_INVALID_ARGUMENT));
        LOG_DBA_WARN_V2(OB_SERVER_LOAD_DYNAMIC_PLUGIN_FAIL, OB_INVALID_ARGUMENT,
                        "invalid loads_plugin param option: ", plugin_load_param.load_option);
        ret = OB_SUCCESS;
      } else if (ObPluginLoadOption::OFF == plugin_load_param.load_option.value()) {
        // ignore this plugin library
        LOG_DEBUG("plugin ignored: option is off", K(plugin_load_param));
      } else if (OB_FAIL(install_library(plugin_load_param.library_name))) {
        LOG_WARN("failed to load library(ignore)", K(plugin_load_param), K(ret));
        LOG_DBA_WARN_V2(OB_SERVER_LOAD_DYNAMIC_PLUGIN_FAIL, ret,
                        "install dynamic library failed or init plugin failed: ", plugin_load_param.library_name);
        ret = OB_SUCCESS; // ignore the error
      } else {
        LOG_INFO("load plugin library success", K(plugin_load_param));
      }
    }
  }
  return ret;
}

class ObCollectPluginCallback final
{
public:
  ObCollectPluginCallback(ObIArray<ObPluginEntryHandle *> &entry_array)
      : entry_array_(entry_array)
  {}

  int operator()(const common::hash::HashMapPair<ObString, ObPluginMgr::PluginEntryList *> &node)
  {
    int ret = OB_SUCCESS;
    ObPluginMgr::PluginEntryList *entry_list = node.second;
    if (OB_ISNULL(entry_list)) {
      LOG_DEBUG("got a nullptr", K(node.first));
    } else {
      for (int64_t i = 0; i < entry_list->count() && OB_SUCC(ret); i++) {
        ObPluginEntryHandle *entry = entry_list->at(i);
        if (OB_ISNULL(entry)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("got a null plugin entry while iterate plugin entry list", K(node.first), K(ret));
        } else if (OB_FAIL(entry_array_.push_back(entry))) {
          LOG_WARN("failed to push plugin handle pointer into array", K(ret));
        }
      }
    }
    return ret;
  }

private:
  ObIArray<ObPluginEntryHandle *> &entry_array_;
};

int ObPluginMgr::list_all_plugin_entries(ObIArray<ObPluginEntryHandle *> &plugin_entries)
{
  int ret = OB_SUCCESS;
  ObCollectPluginCallback collector(plugin_entries);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("plugin mgr is not inited", K(ret));
  } else {
    for (int64_t i = 0; i < OBP_PLUGIN_TYPE_MAX && OB_SUCC(ret); i++) {
      if (OB_FAIL(entry_handle_maps_[i].foreach_refactored(collector))) {
        LOG_WARN("failed to iterate plugin entry map", K(ret), K(i));
      }
    }
  }
  LOG_DEBUG("list plugins", K(plugin_entries.count()), K(ret));
  return ret;
}

} // namespace plugin
} // namespace oceanbase
