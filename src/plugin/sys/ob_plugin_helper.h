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

#pragma once

#include "plugin/sys/ob_plugin_utils.h"
#include "oceanbase/ob_plugin_ftparser.h"

namespace oceanbase {

namespace storage {
class ObFTParser;
} // namespace storage

namespace plugin {

class ObIPluginDescriptor;
class ObIFTParserDesc;
class ObPluginEntryHandle;
class ObPluginParam;

/**
 * A helper function to register, find plugins
 */
class ObPluginHelper final
{
public:
  static int find_ftparser(const common::ObString &parser_name, storage::ObFTParser &ftparser);
  static int find_ftparser(const common::ObString &parser_name, ObIFTParserDesc *&ftparser, ObPluginParam *&param);

  template<typename T>
  static int register_builtin_ftparser(ObPluginParamPtr param, const char *name, const char *description)
  {
    int ret = OB_SUCCESS;
    ret = register_builtin_plugin<T>(param,
                                     OBP_PLUGIN_TYPE_FT_PARSER,
                                     name,
                                     OBP_FTPARSER_INTERFACE_VERSION_CURRENT,
                                     description);
    return ret;
  }

  template<typename T>
  static int register_builtin_plugin(ObPluginParamPtr param,
                                     ObPluginType type,
                                     const char *name,
                                     ObPluginVersion interface_version,
                                     const char *description)
  {
    int ret = OB_SUCCESS;
    T *descriptor = OB_NEW(T, OB_PLUGIN_MEMORY_LABEL);
    if (OB_ISNULL(description)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ret = register_plugin_entry(param, type, name, interface_version, descriptor, description);
    }
    return ret;
  }

public:
  // used internally and by export routines
  static int register_plugin_entry(ObPluginParamPtr param,
                                   ObPluginType type,
                                   const char *name,
                                   ObPluginVersion interface_version,
                                   ObIPluginDescriptor *descriptor,
                                   const char *description);

private:
  static int find_ftparser_entry(const ObString &parser_name, ObPluginEntryHandle *&entry_handle);
};

} // namespace plugin
} // namespace oceanbase
