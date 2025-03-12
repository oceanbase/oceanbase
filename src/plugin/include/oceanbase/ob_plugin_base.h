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

#include <stdint.h>
#include <stddef.h>

#define OBP_PUBLIC_API __attribute__((visibility("default")))

#ifdef __cplusplus
#define OBP_PLUGIN_EXPORT extern "C" OBP_PUBLIC_API
#else // __cplusplus
#define OBP_PLUGIN_EXPORT OBP_PUBLIC_API
#endif // __cplusplus

#define OBP_STRINGIZE_(str) #str
#define OBP_STRINGIZE(str) OBP_STRINGIZE_(str)

#define OBP_DYNAMIC_PLUGIN_NAME_VAR               _ob_plugin_name
#define OBP_DYNAMIC_PLUGIN_API_VERSION_VAR        _ob_plugin_interface_version
#define OBP_DYNAMIC_PLUGIN_SIZEOF_VAR             _ob_plugin_sizeof
#define OBP_DYNAMIC_PLUGIN_PLUGIN_VAR             _ob_plugin
#define OBP_DYNAMIC_PLUGIN_BUILD_REVISION_VAR     _ob_plugin_build_revision
#define OBP_DYNAMIC_PLUGIN_BUILD_BRANCH_VAR       _ob_plugin_build_branch
#define OBP_DYNAMIC_PLUGIN_BUILD_DATE_VAR         _ob_plugin_build_date
#define OBP_DYNAMIC_PLUGIN_BUILD_TIME_VAR         _ob_plugin_build_time

#define OBP_DYNAMIC_PLUGIN_NAME_NAME              OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_NAME_VAR)
#define OBP_DYNAMIC_PLUGIN_API_VERSION_NAME       OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_API_VERSION_VAR)
#define OBP_DYNAMIC_PLUGIN_SIZEOF_NAME            OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_SIZEOF_VAR)
#define OBP_DYNAMIC_PLUGIN_PLUGIN_NAME            OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_PLUGIN_VAR)
#define OBP_DYNAMIC_PLUGIN_BUILD_REVISION_NAME    OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_BUILD_REVISION_VAR)
#define OBP_DYNAMIC_PLUGIN_BUILD_BRANCH_NAME      OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_BUILD_BRANCH_VAR)
#define OBP_DYNAMIC_PLUGIN_BUILD_DATE_NAME        OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_BUILD_DATE_VAR)
#define OBP_DYNAMIC_PLUGIN_BUILD_TIME_NAME        OBP_STRINGIZE(OBP_DYNAMIC_PLUGIN_BUILD_TIME_VAR)

#define OBP_BUILTIN_PLUGIN_VAR(name) ob_builtin_plugin_##name

#ifndef BUILD_REVISION
#define OBP_BUILD_REVISION ""
#else
#define OBP_BUILD_REVISION BUILD_REVISION
#endif

#ifndef BUILD_BRANCH
#define OBP_BUILD_BRANCH ""
#else
#define OBP_BUILD_BRANCH BUILD_BRANCH
#endif

#define OBP_BUILD_DATE __DATE__
#define OBP_BUILD_TIME __TIME__

/**
 * The maximum number of each field of version
 * @details Please refer to `OBP_MAKE_VERSION` for details
 * @NOTE don't touch me
 */
#define OBP_VERSION_FIELD_NUMBER 1000L

/**
 * Used for param type
 */
typedef void * ObPluginDatum;
