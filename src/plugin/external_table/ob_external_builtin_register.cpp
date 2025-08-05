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

#define USING_LOG_PREFIX SHARE

#include <thread>
#include "sql/engine/connector/ob_java_env.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/external_table/ob_external_java_plugin.h"
#include "plugin/external_table/ob_external_jni_utils.h"
#include "lib/allocator/ob_malloc.h"

static int __plugin_init(ObPluginParamPtr plugin)
{
  // we couldn't move the `using namespace` statements below to the global scope,
  // as the oceanbase use unity compiling and it would pollute other files.
  using namespace oceanbase::plugin;
  using namespace oceanbase::common;
  using namespace oceanbase::sql;

  int ret = OBP_SUCCESS;
  ObArray<ObString> java_data_source_names;
  ObJavaDataSourceFactoryHelper datasource_factory_helper;

  // note that we don't free the memory allocated by `list_plugins` which used by plugin manager.
  // Plugin manager doesn't copy the plugin name. We should fix this in ther future.
  ObMalloc allocator(OB_PLUGIN_MEMORY_LABEL);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::init_global_instance())) {
    LOG_WARN("failed to init jni tool", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_external<ObJavaExternalPlugin>(
                   plugin, "java",
                   "This is the java external table data source plugin."))) {
    LOG_WARN("failed to register java external table data source plugin", K(ret));
  } else if (OB_FAIL(datasource_factory_helper.init())) {
    LOG_WARN("failed to init datasource_factory_helper", K(ret));
  } else if (OB_FAIL(datasource_factory_helper.list_plugins(allocator, java_data_source_names))) {
    LOG_WARN("failed to list java data source names", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < java_data_source_names.count(); i++) {
      if (OB_FAIL(ObPluginHelper::register_builtin_external<ObJavaExternalPlugin>(
              plugin, java_data_source_names.at(i).ptr(), "This is a java external data source"))) {
        LOG_WARN("failed to register java data source");
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_INFO("ignore all errors about java while starting observer", K(ret));
    ret = OBP_SUCCESS;
  }
  return ret;
}

static int plugin_init(ObPluginParamPtr plugin)
{
  int ret = OBP_SUCCESS;
  // Because Java cannot work on this inner_main thread, including JNI_CreateJavaVM and AttachCurrentThread,
  // so I create a temporary thread to initialize things about Java.
  // `plugin_init` executed in observer::init which executed in `inner_main`. OceanBase created a new stack
  // to execute `inner_main`, but Java will detect stack information by `pthread_attr_getstack` which would
  // tell an address that not corresponding with the stack of `inner_main`.
  //
  // And another information, Java also create the JVM in a new thread but not the `main` thread. It seems
  // that `main` thread has a special stack.
  // PS. I have test that initializing JVM in the `main` thread (not inner_main) and I succeed.
  std::thread init_java_plugin_thread(__plugin_init, plugin);
  init_java_plugin_thread.join();
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
  OBP_LICENSE_MULAN_PUBL_V2,
  plugin_init,
  plugin_deinit
} OBP_DECLARE_PLUGIN_END;
