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

#define USING_LOG_PREFIX STORAGE

#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_force_print_log.h"
#include "share/ob_plugin_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

#define OB_PLUGIN_GETTER(buf, buf_len, name, name_len, suffix, suffix_len)              \
do {                                                                                    \
  const int64_t prefix_len = STRLEN(OB_PLUGIN_PREFIX);                                  \
  if (OB_UNLIKELY(buf_len <= prefix_len + name_len + suffix_len)) {                     \
    ret = OB_INVALID_ARGUMENT;                                                          \
    LOG_WARN("This buffer is too small to accommodate all of name", K(ret), K(buf_len), \
        K(prefix_len), K(name_len), K(suffix_len));                                     \
  } else {                                                                              \
    MEMCPY(buf + prefix_len + name_len, suffix, suffix_len);                            \
    MEMCPY(buf + prefix_len, name, name_len);                                           \
    MEMCPY(buf, OB_PLUGIN_PREFIX, prefix_len);                                          \
    buf[prefix_len + name_len + suffix_len] = '\0';                                     \
  }                                                                                     \
} while (false)

int get_plugin_version_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_VERSION_SUFFIX,
                   STRLEN(OB_PLUGIN_VERSION_SUFFIX));
  return ret;
}

int get_plugin_size_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_SIZE_SUFFIX,
                   STRLEN(OB_PLUGIN_SIZE_SUFFIX));
  return ret;
}

int get_plugin_str(
    char *buf,
    const int64_t buf_len,
    const char *name,
    const int64_t name_len)
{
  int ret = common::OB_SUCCESS;
  OB_PLUGIN_GETTER(buf,
                   buf_len,
                   name,
                   name_len,
                   OB_PLUGIN_SUFFIX,
                   STRLEN(OB_PLUGIN_SUFFIX));
  return ret;
}

int ObPluginName::set_name(const char *name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The name is nullptr", K(ret), KP(name));
  } else if (OB_UNLIKELY(STRLEN(name) >= OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The name is too long", K(ret), KCSTRING(name));
  } else {
    int i = 0;
    while ('\0' != name[i]) {
      name_[i] = tolower(name[i]);
      ++i;
    }
    name_[i] = '\0';
  }
  return ret;
}

ObPluginSoHandler::ObPluginSoHandler()
  : so_handler_(nullptr),
    has_opened_(false)
{
  memset(plugin_name_, 0x0, OB_PLUGIN_NAME_LENGTH);
}

ObPluginSoHandler::~ObPluginSoHandler()
{
  reset();
}

int ObPluginSoHandler::open(const char *plugin_name, const char *file_name)
{
  int ret = OB_SUCCESS;
  const uint64_t plugin_name_len = nullptr == plugin_name ? 0 : STRLEN(plugin_name);
  const uint64_t file_name_len = nullptr == file_name ? 0 : STRLEN(file_name);
  if (OB_UNLIKELY(nullptr == plugin_name
               || plugin_name_len >= OB_PLUGIN_NAME_LENGTH
               || file_name_len >= OB_PLUGIN_FILE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), KP(plugin_name), K(plugin_name_len), KP(file_name),
        K(file_name_len));
  } else if (OB_UNLIKELY(has_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("This dynamic liabrary has be opened", K(ret), K(has_opened_), K(plugin_name),
        K(file_name));
  } else if (OB_ISNULL(so_handler_ = ob_dlopen(file_name, RTLD_LAZY))) {
    ret = OB_ERR_SYS;
    const char *errmsg = ob_dlerror();
    LOG_WARN("fail to open dynamic library", K(ret), K(errmsg), K(plugin_name), K(file_name));
  } else {
    STRCPY(plugin_name_, plugin_name);
    has_opened_ = true;
    FLOG_INFO("succeed to open a dynamic library", KP(so_handler_), K(plugin_name), K(file_name));
  }
  if (OB_FAIL(ret) && !has_opened_) {
    reset();
  }
  return ret;
}

void ObPluginSoHandler::reset()
{
  if (has_opened_) {
    (void)close();
  }
  so_handler_ = nullptr;
  memset(plugin_name_, 0x0, OB_PLUGIN_NAME_LENGTH);
}

int ObPluginSoHandler::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!has_opened_)) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("The dynamic library hasn't be opened, couldn't be closed", K(ret), K(has_opened_),
        K(plugin_name_));
  } else if (OB_ISNULL(so_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, so handler is nullptr", K(ret), K(plugin_name_), KP(so_handler_),
        K(has_opened_));
  } else if (OB_UNLIKELY(0 != ob_dlclose(so_handler_))) {
    ret = OB_ERR_SYS;
    const char *errmsg = ob_dlerror();
    LOG_WARN("fail to close dynamic library", K(ret), K(errmsg), K(plugin_name_), KP(so_handler_));
  } else {
    has_opened_ = false;
    FLOG_INFO("succeed to close a dynamic library", K(plugin_name_), KP(so_handler_));
  }
  return ret;
}

int ObPluginSoHandler::get_plugin(lib::ObPlugin *&plugin) const
{
  int ret = OB_SUCCESS;
  void *plugin_symbol = nullptr;
  char plugin_name[OB_PLUGIN_SYMBOL_NAME_LENGTH];
  if (OB_UNLIKELY(!has_opened_)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("The dynamic library hasn't be opened", K(ret), K(has_opened_), K(plugin_name_));
  } else if (OB_FAIL(get_plugin_str(plugin_name, OB_PLUGIN_SYMBOL_NAME_LENGTH, plugin_name_,
          STRLEN(plugin_name_)))) {
    LOG_WARN("fail to get plugin str", K(ret));
  } else if (OB_FAIL(get_symbol_ptr(plugin_name, plugin_symbol))) {
    LOG_WARN("fail to get symbol pointer", K(ret), K(plugin_name));
  } else if (OB_ISNULL(plugin_symbol)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin desc symbol ptr is nullptr", K(ret), K(plugin_name_),
        KP(plugin_symbol));
  } else {
    plugin = static_cast<lib::ObPlugin *>(plugin_symbol);
  }
  return ret;
}

int ObPluginSoHandler::get_plugin_version(int64_t &version) const
{
  int ret = OB_SUCCESS;
  void *plugin_version_symbol = nullptr;
  char plugin_version_name[OB_PLUGIN_SYMBOL_NAME_LENGTH];
  if (OB_UNLIKELY(!has_opened_)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("The dynamic library hasn't be opened", K(ret), K(has_opened_), K(plugin_name_));
  } else if (OB_FAIL(get_plugin_version_str(plugin_version_name, OB_PLUGIN_SYMBOL_NAME_LENGTH,
          plugin_name_, STRLEN(plugin_name_)))) {

  } else if (OB_FAIL(get_symbol_ptr(plugin_version_name, plugin_version_symbol))) {
    LOG_WARN("fail to get symbol pointer", K(ret), K(plugin_version_name));
  } else if (OB_ISNULL(plugin_version_symbol)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin desc symbol ptr is nullptr", K(ret), K(plugin_name_),
        KP(plugin_version_symbol));
  } else {
    version = *static_cast<int64_t *>(plugin_version_symbol);
  }
  return ret;
}

int ObPluginSoHandler::get_plugin_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  void *plugin_size_symbol = nullptr;
  char plugin_size_name[OB_PLUGIN_SYMBOL_NAME_LENGTH];
  if (OB_UNLIKELY(!has_opened_)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("The dynamic library hasn't be opened", K(ret), K(has_opened_), K(plugin_name_));
  } else if (OB_FAIL(get_plugin_size_str(plugin_size_name, OB_PLUGIN_SYMBOL_NAME_LENGTH,
          plugin_name_, STRLEN(plugin_name_)))) {
    LOG_WARN("fail to get plugin size name", K(ret));
  } else if (OB_FAIL(get_symbol_ptr(plugin_size_name, plugin_size_symbol))) {
    LOG_WARN("fail to get symbol pointer", K(ret), K(plugin_size_name));
  } else if (OB_ISNULL(plugin_size_symbol)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, plugin desc symbol ptr is nullptr", K(ret), K(plugin_name_),
        KP(plugin_size_symbol));
  } else {
    size = *static_cast<int64_t *>(plugin_size_symbol);
  }
  return ret;
}

int ObPluginSoHandler::get_symbol_ptr(const char *sym_name, void *&sym_ptr) const
{
  int ret = OB_SUCCESS;
  const uint64_t sym_name_len = nullptr == sym_name ? 0 : STRLEN(sym_name);
  if (OB_UNLIKELY(!has_opened_)) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("The dynamic library hasn't be opened", K(ret), K(plugin_name_), K(has_opened_));
  } else if (OB_ISNULL(so_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, so handler is nullptr", K(ret), K(plugin_name_), KP(so_handler_),
        K(has_opened_));
  } else if (OB_UNLIKELY(nullptr == sym_name || sym_name_len >= OB_PLUGIN_SYMBOL_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(plugin_name_), K(sym_name), K(sym_name_len));
  } else if (OB_ISNULL(sym_ptr = ob_dlsym(so_handler_, sym_name))) {
    ret = OB_SEARCH_NOT_FOUND;
    const char *errmsg = ob_dlerror();
    LOG_WARN("Don't find symbol in dynamic library", K(ret), K(errmsg), K(plugin_name_), K(sym_name),
        KP(so_handler_));
  } else {
    LOG_DEBUG("succeed to get a symbol", K(sym_name), KP(sym_ptr), K(plugin_name_), KP(so_handler_));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
