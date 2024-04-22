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

#ifndef OB_PLUGIN_HELPER_H_
#define OB_PLUGIN_HELPER_H_

#include <dlfcn.h>

#include "lib/ob_plugin.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

inline void *ob_dlopen(const char *file_name, int flags)
{
  return ::dlopen(file_name, flags);
}

inline void *ob_dlsym(void *__restrict handle, const char *__restrict symbol_name)
{
  return ::dlsym(handle, symbol_name);
}

inline int ob_dlclose(void *handle)
{
  return ::dlclose(handle);
}

inline char *ob_dlerror(void)
{
  return ::dlerror();
}

class ObPluginName final
{
public:
  ObPluginName() { memset(name_, 0x0, OB_PLUGIN_NAME_LENGTH); }
  explicit ObPluginName(const char *name) { OB_ASSERT(common::OB_SUCCESS == set_name(name)); }
  ~ObPluginName() = default;

  int set_name(const char *name);
  OB_INLINE bool is_valid() const { return STRLEN(name_) > 0; }
  OB_INLINE int len() const { return STRLEN(name_); }
  OB_INLINE char *str() { return name_; }
  OB_INLINE const char *str() const { return name_; }
  OB_INLINE int hash(uint64_t &value) const
  {
    value = murmurhash(name_, static_cast<int32_t>(STRLEN(name_)), 0);
    return OB_SUCCESS;
  }

  OB_INLINE bool operator ==(const ObPluginName &other) const
  {
    return 0 == STRCMP(name_, other.name_);
  }
  OB_INLINE bool operator !=(const ObPluginName &other) const
  {
    return 0 != STRCMP(name_, other.name_);
  }
  OB_INLINE bool operator <(const ObPluginName &other) const
  {
    return 0 > STRCMP(name_, other.name_);
  }
  TO_STRING_KV(K_(name));
private:
  char name_[OB_PLUGIN_NAME_LENGTH];
};

class ObPluginSoHandler final : public lib::ObIPluginHandler
{
public:
  ObPluginSoHandler();
  ~ObPluginSoHandler();

  void reset();
  /**
   * open dynamic plugin library
   *  - if file_name is nullptr, then the so handle is for the main program.
   *
   * @param[in] plugin_name
   * @param[in] file_name
   * @return error code
   */
  int open(const char *plugin_name, const char *file_name);
  int close();

  virtual int get_plugin(lib::ObPlugin *&plugin) const override;
  virtual int get_plugin_version(int64_t &version) const override;
  virtual int get_plugin_size(int64_t &size) const override;

  VIRTUAL_TO_STRING_KV(K_(plugin_name), KP_(so_handler), K_(has_opened));

private:
  int get_symbol_ptr(const char *sym_name, void *&sym_ptr) const;

private:
  char plugin_name_[OB_PLUGIN_NAME_LENGTH];
  void *so_handler_;
  bool has_opened_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPluginSoHandler);
};

} // end namespace share
} // end namespace oceanbase

#endif // OB_PLUGIN_HELPER_H_
