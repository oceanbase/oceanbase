/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/ob_malloc.h"
#include "oceanbase/ob_plugin_ftparser.h"
#include "oceanbase/ob_plugin_external.h"
#include "plugin/sys/ob_plugin_utils.h"

namespace oceanbase {

namespace storage {
class ObFTParser;
} // namespace storage

namespace plugin {

class ObIPluginDescriptor;
class ObIFTParserDesc;
class ObPluginEntryHandle;
class ObPluginParam;
class ObPluginKmsIntf;
class ObIExternalDescriptor;

/**
 * A helper function to register, find plugins
 */
class ObPluginHelper final
{
public:
  static int find_ftparser(const common::ObString &parser_name, storage::ObFTParser &ftparser);
  static int find_ftparser(const common::ObString &parser_name, ObIFTParserDesc *&ftparser, ObPluginParam *&param);

  static const common::ObString KMS_NAME_PREFIX;
  static int find_kms(const common::ObString &name, ObPluginKmsIntf *&kms, ObPluginParam *&param);

  static int find_external_table(const common::ObString &name, ObIExternalDescriptor *&external_desc);

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
  static int register_builtin_external(ObPluginParamPtr param, const char *name, const char *description)
  {
    int ret = OB_SUCCESS;
    ret = register_builtin_plugin<T>(param,
                                     OBP_PLUGIN_TYPE_EXTERNAL,
                                     name,
                                     OBP_EXTERNAL_INTERFACE_VERSION_CURRENT,
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

  // Trigger one-time lazy discovery of external data-source sub-plugins through the generic
  // "java" descriptor. Called when an external table lookup misses or when listing all plugins.
  static int discover_external_sub_plugins();

private:
  static int find_ftparser_entry(const ObString &parser_name, ObPluginEntryHandle *&entry_handle);
  static int find_plugin_entry(const ObString &name,
                               ObPluginType type,
                               ObPluginVersion current_version,
                               ObPluginEntryHandle *&entry_handle);

};

} // namespace plugin
} // namespace oceanbase
