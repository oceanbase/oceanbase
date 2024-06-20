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

#ifndef OB_PLUGIN_H_
#define OB_PLUGIN_H_

#include <stdint.h>

#include "lib/ob_errno.h"
#include "lib/charset/ob_ctype.h"
#include "lib/utility/ob_print_utils.h"

#define OB_PLUGIN_AUTHOR_OCEANBASE "OceanBase Corporation"

#define OB_PLUGIN_EXPORT

#define OB_PLUGIN_INTERFACE_VERSION 0x01  // plugin interface version

#define OB_PLUGIN_NAME_LENGTH 128         // plugin name length
#define OB_PLUGIN_FILE_NAME_LENGTH 512    // plugin file name length
#define OB_PLUGIN_SYMBOL_NAME_LENGTH 1024 // symbol name length in plunin dynamic library

#define OB_PLUGIN_STR_(str) #str
#define OB_PLUGIN_STR(str) OB_PLUGIN_STR_(str)

#define OB_PLUGIN_PREFIX OB_PLUGIN_STR(ob_builtin_)
#define OB_PLUGIN_VERSION_SUFFIX OB_PLUGIN_STR(_plugin_version)
#define OB_PLUGIN_SIZE_SUFFIX OB_PLUGIN_STR(_sizeof_plugin)
#define OB_PLUGIN_SUFFIX OB_PLUGIN_STR(_plugin)

#define OB_PLUGIN_VERSION_SYMBOL(name) ob_builtin_##name##_plugin_version
#define OB_PLUGIN_SIZE_SYMBOL(name)    ob_builtin_##name##_sizeof_plugin
#define OB_PLUGIN_SYMBOL(name)         ob_builtin_##name##_plugin

#define OB_DECLARE_PLUGIN_(name, version, size, plugin)                 \
  OB_PLUGIN_EXPORT int64_t version = OB_PLUGIN_INTERFACE_VERSION;       \
  OB_PLUGIN_EXPORT int64_t size = sizeof(oceanbase::lib::ObPlugin);     \
  OB_PLUGIN_EXPORT oceanbase::lib::ObPlugin plugin =

// this is used to define a plugin
//
//  - C/C++ code for example,
//
//    OB_DECLARE_PLUGIN(example_plugin)
//    {
//      OB_FT_PARSER_PLUGIN,                // type
//      "ExamplePlugin",                    // name
//      "OceanBase Corporation",            // author
//      "This is a plugin example.",        // brief specification
//      0x00001,                            // version
//      OB_MULAN_V2_LICENSE,                // license
//      &example_plugin,                    // plugin instance
//    };
#define OB_DECLARE_PLUGIN(name)                                         \
  OB_DECLARE_PLUGIN_(name,                                              \
                     ob_builtin_##name##_plugin_version,                \
                     ob_builtin_##name##_sizeof_plugin,                 \
                     ob_builtin_##name##_plugin)

#define OB_GET_PLUGIN_VALUE_FUNC(name, value_type, value_name)             \
  virtual int get_plugin_##name(value_type &value) const override          \
  {                                                                        \
    value = value_name;                                                    \
    return oceanbase::common::OB_SUCCESS;                                  \
  }

#define OB_GET_PLUGIN_FUNC(value_name)                                     \
  virtual int get_plugin(oceanbase::lib::ObPlugin *&plugin) const override \
  {                                                                        \
    plugin = &(value_name);                                                \
    return oceanbase::common::OB_SUCCESS;                                  \
  }

#define OB_GET_PLUGIN_VERSION_FUNC(value_name)                             \
  OB_GET_PLUGIN_VALUE_FUNC(version, int64_t, value_name)
#define OB_GET_PLUGIN_SIZE_FUNC(value_name)                                \
  OB_GET_PLUGIN_VALUE_FUNC(size, int64_t, value_name)

#define OB_DECLARE_BUILDIN_PLUGIN_HANDLER(plugin_handler, plugin_name)     \
class plugin_handler final : public oceanbase::lib::ObIPluginHandler       \
{                                                                          \
public:                                                                    \
  plugin_handler() = default;                                              \
  ~plugin_handler() = default;                                             \
  OB_GET_PLUGIN_FUNC(ob_builtin_##plugin_name##_plugin);                   \
  OB_GET_PLUGIN_VERSION_FUNC(ob_builtin_##plugin_name##_plugin_version);   \
  OB_GET_PLUGIN_SIZE_FUNC(ob_builtin_##plugin_name##_sizeof_plugin);       \
  VIRTUAL_TO_STRING_KV(KCSTRING("##plugin_handler##"));                    \
};

namespace oceanbase
{
namespace lib
{

class ObIPluginDesc;

// define plugin type
enum class ObPluginType : uint64_t
{
  OB_FT_PARSER_PLUGIN = 1, // fulltext parser plugin
  OB_MAX_PLUGIN_TYPE = 2,  // max plugin type
};

// define plugin license
enum class ObPluginLicenseType : uint64_t
{
  OB_Mulan_PubL_V2_LICENSE = 1,   // Mulan PubL v2 license
  OB_MAX_PLUGIN_LICENSE_TYPE = 2, // max plugin license type
};

class ObPluginParam final
{
public:
  ObPluginParam() : desc_(nullptr) {}
  ~ObPluginParam() { reset(); }

  inline bool is_valid() const { return nullptr != desc_; }
  inline void reset() { desc_ = nullptr; }

  TO_STRING_KV(KP_(desc));
public:
  ObIPluginDesc *desc_;
};

// descriptor interface of base plugin
class ObIPluginDesc
{
public:
  ObIPluginDesc() = default;
  virtual ~ObIPluginDesc() = default;

public:
  // plugin initialize function
  virtual int init(ObPluginParam *param) = 0;
  // plugin de-initialize function
  virtual int deinit(ObPluginParam *param) = 0;
};

// ob plugin description structure
class ObPlugin final
{
public:
  static const int64_t PLUGIN_VERSION = 0x01;
public:
  ObPlugin()
    : type_(ObPluginType::OB_MAX_PLUGIN_TYPE),
      name_(nullptr),
      author_(nullptr),
      spec_(nullptr),
      version_(PLUGIN_VERSION),
      license_(ObPluginLicenseType::OB_MAX_PLUGIN_LICENSE_TYPE),
      desc_(nullptr)
  {}
  ObPlugin(
      const ObPluginType &type,
      const char *name,
      const char *author,
      const char *spec,
      const int64_t version,
      const ObPluginLicenseType &license,
      ObIPluginDesc *desc)
    : type_(type),
      name_(name),
      author_(author),
      spec_(spec),
      version_(version),
      license_(license),
      desc_(desc)
  {}

  ~ObPlugin() = default;
  inline bool is_valid() const
  {
    return (ObPluginType::OB_FT_PARSER_PLUGIN <= type_ && type_ < ObPluginType::OB_MAX_PLUGIN_TYPE)
        && nullptr != name_
        && nullptr != author_
        && nullptr != spec_
        && PLUGIN_VERSION == version_
        && (ObPluginLicenseType::OB_Mulan_PubL_V2_LICENSE <= license_
            && license_ < ObPluginLicenseType::OB_MAX_PLUGIN_LICENSE_TYPE)
        && nullptr != desc_;
  }
  TO_STRING_KV(KP_(type), KCSTRING_(name), KCSTRING_(author), KCSTRING_(spec), K_(version),
      K_(license), KP_(desc));
public:
  ObPluginType type_;           // type of the plugin
  const char *name_;            // name for the plugin
  const char *author_;          // author for the plugin
  const char *spec_;            // brief specification of the plugin
  int64_t version_;             // version for the plugin
  ObPluginLicenseType license_; // license for the plugin
  ObIPluginDesc *desc_;         // the plugin descriptor
};

class ObIPluginHandler
{
public:
  ObIPluginHandler() = default;
  virtual ~ObIPluginHandler() = default;

  virtual int get_plugin(lib::ObPlugin *&plugin) const = 0;
  virtual int get_plugin_version(int64_t &version) const = 0;
  virtual int get_plugin_size(int64_t &size) const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObFTParserParam final
{
public:
  ObFTParserParam()
    : allocator_(nullptr),
      cs_(nullptr),
      fulltext_(nullptr),
      ft_length_(0),
      parser_version_(-1)
  {}
  ~ObFTParserParam() = default;

  inline bool is_valid() const
  {
    return nullptr != allocator_
        && nullptr != cs_
        && nullptr != fulltext_
        && 0 < ft_length_
        && 0 <= parser_version_;
  }
  inline void reset()
  {
    allocator_ = nullptr;
    cs_ = nullptr;
    fulltext_ = nullptr;
    ft_length_ = 0;
    parser_version_ = 0;
  }

  TO_STRING_KV(KP_(allocator), KP_(cs), K_(fulltext), K_(ft_length), K_(parser_version));
public:
  common::ObIAllocator *allocator_;
  const ObCharsetInfo *cs_;
  const char *fulltext_;
  int64_t ft_length_;
  int64_t parser_version_;
};

class ObITokenIterator
{
public:
  ObITokenIterator() = default;
  virtual ~ObITokenIterator() = default;
  virtual int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_cnt,
      int64_t &word_freq) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

// fulltext parser descriptor interface for domain index
//  - splitting a document into many tokenizations.
class ObIFTParserDesc : public ObIPluginDesc
{
public:
  ObIFTParserDesc() = default;
  virtual ~ObIFTParserDesc() = default;

  /**
   * split fulltext into multiple word segments
   *
   * @param[in]  param, the document to be tokenized and parameters related to word segmentation.
   * @param[out] iter, the tokenized words' iterator.
   *
   * @return error code, such as, OB_SUCCESS, OB_INVALID_ARGUMENT, ...
   */
  virtual int segment(ObFTParserParam *param, ObITokenIterator *&iter) const = 0;

  /**
   * Release resources held by the iterator and free token iterator.
   */
  virtual void free_token_iter(ObFTParserParam *param, ObITokenIterator *&iter) const
  {
    if (OB_NOT_NULL(iter)) {
      iter->~ObITokenIterator();
    }
  }
};

} // end namespace lib
} // end namespace oceanbase

#endif // OB_PLUGIN_H_
