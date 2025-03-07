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

#pragma once

#include "oceanbase/ob_plugin_base.h"
#include "oceanbase/ob_plugin_errno.h"
#include "oceanbase/ob_plugin_log.h"
#include "oceanbase/ob_plugin_allocator.h"

/**
 * @brief OceanBase Plugin Interface
 * @defgroup ObPlugin
 */
/* @{ */

#define OBP_AUTHOR_OCEANBASE "OceanBase Corporation"

/**
 * plugin API version and this is the minimum version
 * @details This is a constant value
 */
#define OBP_PLUGIN_API_VERSION OBP_MAKE_VERSION(0, 1, 0)
/**
 * current API version
 * @details This value will change if we add more API
 */
#define OBP_PLUGIN_API_VERSION_CURRENT OBP_PLUGIN_API_VERSION

#ifdef OBP_DYNAMIC_PLUGIN
/** in dynamic library plugin **/
#define OBP_DECLARE_PLUGIN_(name)                                                                       \
  OBP_PLUGIN_EXPORT const char *OBP_DYNAMIC_PLUGIN_NAME_VAR = OBP_STRINGIZE(name);                      \
  OBP_PLUGIN_EXPORT int64_t OBP_DYNAMIC_PLUGIN_API_VERSION_VAR = OBP_PLUGIN_API_VERSION_CURRENT;        \
  OBP_PLUGIN_EXPORT const char *OBP_DYNAMIC_PLUGIN_BUILD_REVISION_VAR = OBP_BUILD_REVISION;             \
  OBP_PLUGIN_EXPORT const char *OBP_DYNAMIC_PLUGIN_BUILD_BRANCH_VAR = OBP_BUILD_BRANCH;                 \
  OBP_PLUGIN_EXPORT const char *OBP_DYNAMIC_PLUGIN_BUILD_DATE_VAR = OBP_BUILD_DATE;                     \
  OBP_PLUGIN_EXPORT const char *OBP_DYNAMIC_PLUGIN_BUILD_TIME_VAR = OBP_BUILD_TIME;                     \
  OBP_PLUGIN_EXPORT int64_t OBP_DYNAMIC_PLUGIN_SIZEOF_VAR = sizeof(struct _ObPlugin);                   \
  OBP_PLUGIN_EXPORT ObPlugin OBP_DYNAMIC_PLUGIN_PLUGIN_VAR =

#else /* OBP_DYNAMIC_PLUGIN */

/** in static plugin that built with observer **/
#define OBP_DECLARE_PLUGIN_(name)                                         \
  OBP_PLUGIN_EXPORT ObPlugin OBP_BUILTIN_PLUGIN_VAR(name) =

#endif /* OBP_DYNAMIC_PLUGIN */

/**
 * this is used to define a plugin
 * @details - C/C++ code for example,
 * <p>
 * @code
 *    OBP_DECLARE_PLUGIN(example_plugin)
 *    {
 *      "OceanBase Corporation",            // author
 *      OBP_MAKE_VERSION(1, 0, 0),          // version
 *      OBP_LICENSE_MULAN_PSL_V2,           // license
 *      plugin_init,                        // init routine
 *      plugin_deinit,                      // deinit routine(optional)
 *    } OBP_DECLARE_PLUGIN_END;
 * @endcode
 */
#define OBP_DECLARE_PLUGIN(name) \
  OBP_DECLARE_PLUGIN_(name)

/**
 * A help macro
 * @ref OBP_DECLARE_PLUGIN
 */
#define OBP_DECLARE_PLUGIN_END

/** @NOTE all API should be declared as C interface **/
#ifdef __cplusplus
extern "C" {
#endif

/**
 * The plugin init/deinit param type
 */
typedef ObPluginDatum ObPluginParamPtr;

/**
 * Plugin types
 */
enum OBP_PUBLIC_API ObPluginType
{
  OBP_PLUGIN_TYPE_INVALID = 0,
  OBP_PLUGIN_TYPE_FT_PARSER,   /**< fulltext parser plugin */
  OBP_PLUGIN_TYPE_MAX,         /**< max plugin type */
};

/**
 * define plugin licenses
 * @details You can use other strings
 */
#define OBP_LICENSE_GPL           "GPL"
#define OBP_LICENSE_BSD           "BSD"
#define OBP_LICENSE_MIT           "MIT"
#define OBP_LICENSE_APACHE_V2     "Apache 2.0"
#define OBP_LICENSE_MULAN_PUBL_V2 "Mulan PubL v2"
#define OBP_LICENSE_MULAN_PSL_V2  "Mulan PSL v2"

/**
 * The version type
 * @details A version contains 3 fields:
 * - **major** indicates significant changes that may include backward-incompatible updates.
 * - **minor** represents backward-compatible feature additions and improvements.
 * - **patch** backward-compatible bug fixes and minor corrections.
 *
 * Use `OBP_MAKE_VERSION` to create a version data.
 */
typedef uint64_t ObPluginVersion;

/**< create a version number */
#define OBP_MAKE_VERSION(major, minor, patch) \
  ((major) * OBP_VERSION_FIELD_NUMBER * OBP_VERSION_FIELD_NUMBER + (minor) * OBP_VERSION_FIELD_NUMBER + (patch))

/**
 * ob plugin description structure
 * @note you should use OBP_DECLARE_PLUGIN instead
 * @ref OBP_DECLARE_PLUGIN
 */
struct OBP_PUBLIC_API _ObPlugin
{
  const char *        author;
  ObPluginVersion     version;      /**< library version for the plugin */
  const char *        license;      /**< The license of the plugin. You can use `OBP_LICENSE_` or a const string */

  /**
   * The plugin init function, NULL if no need
   * @return OBP_SUCCESS if success, otherwise failed
   */
  int (*init) (ObPluginParamPtr param);

  /**
   * The plugin deinit function, NULL if no need
   * @return OBP_SUCCESS if success, otherwise failed
   */
  int (*deinit)(ObPluginParamPtr param);
};

typedef struct _ObPlugin ObPlugin;

/**
 * get the `struct ObPlugin` from the parameter
 */
OBP_PUBLIC_API ObPlugin *obp_param_plugin(ObPluginParamPtr param);

/**
 * get the plugin instance specific(user data) from the parameter
 * @details the plugin instance specific data is a pointer and you can set it to any data if you want to.
 */
OBP_PUBLIC_API ObPluginDatum obp_param_plugin_user_data(ObPluginParamPtr param);

/**
 * set the plugin instance specific(user data) in the parameter
 */
OBP_PUBLIC_API void obp_param_set_plugin_user_data(ObPluginParamPtr param, ObPluginDatum plugin_spec);

#ifdef __cplusplus
} // extern "C"
#endif

/* @} */
