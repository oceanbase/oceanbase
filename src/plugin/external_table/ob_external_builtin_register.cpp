/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/external_table/ob_external_java_plugin.h"
#include "plugin/external_table/ob_external_jni_utils.h"

static int plugin_init(ObPluginParamPtr plugin)
{
  using namespace oceanbase::plugin;
  using namespace oceanbase::common;

  // Pre-allocate ObJniTool singleton here (single-threaded context).
  // JVM is started lazily on first use; only register the plugin descriptor here.
  int ret = OBP_SUCCESS;
  if (OB_FAIL(ObJniTool::init_global_instance())) {
    LOG_WARN("failed to init jni tool instance", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_external<ObJavaExternalPlugin>(
          plugin, ObJavaExternalPlugin::PLUGIN_NAME,
          "This is the java external table data source plugin."))) {
    LOG_WARN("failed to register java external table data source plugin", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_INFO("ignore errors about java plugin during startup", K(ret));
    ret = OBP_SUCCESS;
  }
  return ret;
}
static int plugin_deinit(ObPluginParamPtr /*plugin*/)
{
  using namespace oceanbase::plugin;

  ObJniTool::destroy_global_instance();
  return OBP_SUCCESS;
}

OBP_DECLARE_PLUGIN(java_external)
{
  OBP_AUTHOR_OCEANBASE,
  OBP_MAKE_VERSION(0, 1, 0),
  OBP_LICENSE_APACHE_V2,
  plugin_init,
  plugin_deinit
} OBP_DECLARE_PLUGIN_END;
