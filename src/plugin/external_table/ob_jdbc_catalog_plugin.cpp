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

#include "plugin/external_table/ob_jdbc_catalog_plugin.h"
#include "plugin/external_table/ob_external_jni_utils.h"
#include "plugin/external_table/ob_external_java_plugin.h"
#include "share/catalog/jdbc/ob_jdbc_catalog_types.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
namespace plugin
{
using namespace oceanbase::common;

int ObJdbcCatalogPlugin::supports(const ObString &plugin_name, bool &supported)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jclass factory_class = nullptr;
  jstring java_plugin_name = nullptr;
  jmethodID supports_method = nullptr;
  jboolean java_supported = JNI_FALSE;
  LOCAL_REF_GUARD_ENV(factory_class, jni_env);
  LOCAL_REF_GUARD_ENV(java_plugin_name, jni_env);
  supported = false;
  if (OB_UNLIKELY(plugin_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc catalog plugin name is empty", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_string(
                 jni_env, plugin_name, java_plugin_name))) {
    LOG_WARN("failed to create java jdbc catalog plugin name", K(ret));
  }
  OBJNI_RUN(factory_class = jni_env->FindClass(DATA_SOURCE_FACTORY_CLASS_NAME));
  CK(OB_NOT_NULL(factory_class));
  OBJNI_RUN(supports_method = jni_env->GetStaticMethodID(
      factory_class, "supportsJdbcCatalog", "(Ljava/lang/String;)Z"));
  CK(OB_NOT_NULL(supports_method));
  OBJNI_RUN(java_supported = jni_env->CallStaticBooleanMethod(factory_class, supports_method, java_plugin_name));
  if (OB_SUCC(ret)) {
    supported = JNI_TRUE == java_supported;
  }
  return ret;
}

int ObJdbcCatalogPlugin::init(const ObString &plugin_name)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jclass factory_class = nullptr;
  jclass catalog_plugin_class = nullptr;
  jstring java_plugin_name = nullptr;
  jobject catalog_plugin = nullptr;
  jmethodID create_method = nullptr;
  LOCAL_REF_GUARD_ENV(factory_class, jni_env);
  LOCAL_REF_GUARD_ENV(catalog_plugin_class, jni_env);
  LOCAL_REF_GUARD_ENV(java_plugin_name, jni_env);
  LOCAL_REF_GUARD_ENV(catalog_plugin, jni_env);

  if (OB_NOT_NULL(catalog_plugin_.handle())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("java jdbc catalog initialized twice", K(ret));
  } else if (OB_UNLIKELY(plugin_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc plugin name is empty", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  }

  OBJNI_RUN(factory_class = jni_env->FindClass(DATA_SOURCE_FACTORY_CLASS_NAME));
  CK(OB_NOT_NULL(factory_class));
  OBJNI_RUN(create_method = jni_env->GetStaticMethodID(
      factory_class,
      "createJdbcCatalogPlugin",
      "(Ljava/lang/String;)Lcom/oceanbase/external/internal/JdbcCatalogBridge;"));
  CK(OB_NOT_NULL(create_method));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().create_java_string(jni_env, plugin_name, java_plugin_name))) {
    LOG_WARN("failed to create java jdbc catalog plugin name", K(ret));
  }
  OBJNI_RUN(catalog_plugin = jni_env->CallStaticObjectMethod(
      factory_class, create_method, java_plugin_name));
  CK(OB_NOT_NULL(catalog_plugin));
  OBJNI_RUN(catalog_plugin_class = jni_env->GetObjectClass(catalog_plugin));
  CK(OB_NOT_NULL(catalog_plugin_class));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(catalog_plugin_.from_local_ref(catalog_plugin, jni_env))) {
    LOG_WARN("failed to hold java jdbc catalog bridge", K(ret));
  } else if (OB_FAIL(catalog_plugin_class_.from_local_ref(catalog_plugin_class, jni_env))) {
    LOG_WARN("failed to hold java jdbc catalog bridge class", K(ret));
  }

  OBJNI_RUN(connect_method_ = jni_env->GetMethodID(catalog_plugin_class, "connect", "([B)Ljava/sql/Connection;"));
  OBJNI_RUN(disconnect_method_ = jni_env->GetMethodID(catalog_plugin_class, "disconnect", "(Ljava/sql/Connection;)V"));
  OBJNI_RUN(connection_reusable_method_ = jni_env->GetStaticMethodID(catalog_plugin_class, "isJdbcConnectionReusable", "(Ljava/lang/Throwable;)Z"));
  OBJNI_RUN(list_namespace_names_method_ = jni_env->GetMethodID(
      catalog_plugin_class, "listNamespaceNames", "(Ljava/sql/Connection;)Ljava/util/List;"));
  OBJNI_RUN(list_table_names_method_ = jni_env->GetMethodID(
      catalog_plugin_class,
      "listTableNames",
      "(Ljava/sql/Connection;[B)Ljava/util/List;"));
  OBJNI_RUN(fetch_namespace_schema_method_ = jni_env->GetMethodID(
      catalog_plugin_class,
      "fetchNamespaceSchema",
      "(Ljava/sql/Connection;[B)[B"));
  OBJNI_RUN(fetch_lake_table_metadata_method_ = jni_env->GetMethodID(
      catalog_plugin_class,
      "fetchTableMetadata",
      "(Ljava/sql/Connection;[B[BLjava/lang/String;)[B"));

  if (OB_SUCC(ret)
      && (OB_ISNULL(connect_method_)
          || OB_ISNULL(disconnect_method_)
          || OB_ISNULL(connection_reusable_method_)
          || OB_ISNULL(list_namespace_names_method_)
          || OB_ISNULL(list_table_names_method_)
          || OB_ISNULL(fetch_namespace_schema_method_)
          || OB_ISNULL(fetch_lake_table_metadata_method_))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to initialize java jdbc catalog bridge methods", K(ret));
  }
  return ret;
}

void ObJdbcCatalogPlugin::destroy(ObIAllocator &allocator)
{
  this->~ObJdbcCatalogPlugin();
  allocator.free(this);
}

void ObJdbcCatalogPlugin::check_exception(JNIEnv *jni_env,
                                          int &retcode,
                                          ObJniExceptionResult &result,
                                          bool &connection_reusable) const
{
  jthrowable exception = nullptr;
  LOCAL_REF_GUARD_ENV(exception, jni_env);
  connection_reusable = false;
  if (OB_NOT_NULL(jni_env)) {
    // Keep a separate local reference before generic handling clears the pending exception.
    exception = jni_env->ExceptionOccurred();
  }
  ObJniTool::instance().check_exception(jni_env, retcode, result);
  if (OB_SUCCESS == retcode) {
    connection_reusable = true;
  } else if (OB_NOT_NULL(jni_env) && OB_NOT_NULL(exception)
             && OB_NOT_NULL(catalog_plugin_class_.handle()) && OB_NOT_NULL(connection_reusable_method_)
             && !jni_env->ExceptionCheck()) {
    const jboolean reusable = jni_env->CallStaticBooleanMethod(catalog_plugin_class_.class_handle(),
        connection_reusable_method_, exception);
    if (jni_env->ExceptionCheck()) {
      // Preserve the original operation error if classification itself fails.
      jni_env->ExceptionDescribe();
      jni_env->ExceptionClear();
    } else {
      connection_reusable = JNI_TRUE == reusable;
    }
  }

  if (result.exception_occured() && (OB_IO_ERROR == retcode || OB_JNI_ERROR == retcode)) {
    ObArenaAllocator allocator(ob_plugin_mem_attr());
    ObString message;
    const int message_ret = result.get_message(allocator, message);
    if (OB_SUCCESS == message_ret) {
      retcode = OB_JNI_JAVA_EXCEPTION_ERROR;
      LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, message.length(), message.ptr());
    } else {
      LOG_WARN_RET(message_ret, "failed to format jdbc exception", K(retcode));
    }
  }
}

int ObJdbcCatalogPlugin::connect(const ObString &jdbc_url, ObJavaGlobalRef &connection)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jobject local_connection = nullptr;
  jbyteArray java_jdbc_url = nullptr;
  bool connection_reusable = false;
  LOCAL_REF_GUARD_ENV(local_connection, jni_env);
  LOCAL_REF_GUARD_ENV(java_jdbc_url, jni_env);
  NAMED_DEFER(disconnect_unpromoted,
    if (OB_NOT_NULL(local_connection) && OB_NOT_NULL(jni_env)) {
      int ret = OB_SUCCESS;
      OBJNI_RUN(jni_env->CallVoidMethod(catalog_plugin_.handle(), disconnect_method_, local_connection));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to disconnect unpromoted jdbc connection", K(ret));
      }
    }
  );
  if (OB_NOT_NULL(connection.handle())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("jdbc connection is already initialized", K(ret));
  } else if (jdbc_url.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc url is empty", K(ret));
  } else if (OB_ISNULL(catalog_plugin_.handle()) || OB_ISNULL(connect_method_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("java jdbc catalog is not initialized", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_bytes(jni_env, jdbc_url, java_jdbc_url))) {
    LOG_WARN("failed to create java jdbc url", K(ret));
  }
  OBJNI_RUN_JDBC(*this, local_connection = jni_env->CallObjectMethod(
      catalog_plugin_.handle(), connect_method_, java_jdbc_url), connection_reusable);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(local_connection)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("java jdbc catalog returned null connection", K(ret));
  } else if (OB_FAIL(connection.from_local_ref(local_connection, jni_env))) {
    LOG_WARN("failed to create global ref of jdbc connection", K(ret));
  } else {
    disconnect_unpromoted.deactivate();
  }
  return ret;
}

int ObJdbcCatalogPlugin::disconnect(ObJavaGlobalRef &connection)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  if (OB_NOT_NULL(connection.handle())) {
    if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (OB_ISNULL(jni_env)) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("jni env is null", K(ret));
    }
    OBJNI_RUN(jni_env->CallVoidMethod(catalog_plugin_.handle(), disconnect_method_, connection.handle()));
    connection.clear();
  }
  return ret;
}

int ObJdbcCatalogPlugin::list_namespace_names(jobject connection, ObIAllocator &allocator,
                                              ObIArray<ObString> &namespace_names,
                                              bool &connection_reusable)
{
  int ret = OB_SUCCESS;
  connection_reusable = false;
  JNIEnv *jni_env = nullptr;
  jobject java_names = nullptr;
  LOCAL_REF_GUARD_ENV(java_names, jni_env);
  if (OB_ISNULL(catalog_plugin_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("java jdbc catalog is not initialized", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  }
  OBJNI_RUN_JDBC(*this, java_names = jni_env->CallObjectMethod(
      catalog_plugin_.handle(), list_namespace_names_method_, connection), connection_reusable);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(java_names)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("java jdbc catalog returned null namespace list", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().bytes_list_from_java(
                 jni_env, java_names, allocator, namespace_names))) {
    LOG_WARN("failed to convert java namespace names", K(ret));
  }
  return ret;
}

int ObJdbcCatalogPlugin::list_table_names(jobject connection,
                                          ObIAllocator &allocator,
                                          const ObString &namespace_name,
                                          ObIArray<ObString> &table_names,
                                          bool &namespace_found,
                                          bool &connection_reusable)
{
  int ret = OB_SUCCESS;
  connection_reusable = false;
  JNIEnv *jni_env = nullptr;
  jbyteArray java_namespace_name = nullptr;
  jobject java_names = nullptr;
  LOCAL_REF_GUARD_ENV(java_namespace_name, jni_env);
  LOCAL_REF_GUARD_ENV(java_names, jni_env);
  namespace_found = false;
  if (OB_ISNULL(catalog_plugin_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("java jdbc catalog is not initialized", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_bytes(
                 jni_env, namespace_name, java_namespace_name))) {
    LOG_WARN("failed to create java namespace name", K(ret));
  }
  OBJNI_RUN_JDBC(*this, java_names = jni_env->CallObjectMethod(
      catalog_plugin_.handle(), list_table_names_method_, connection, java_namespace_name), connection_reusable);
  if (OB_SUCC(ret) && OB_NOT_NULL(java_names)) {
    namespace_found = true;
    if (OB_FAIL(ObJniTool::instance().bytes_list_from_java(
            jni_env, java_names, allocator, table_names))) {
      LOG_WARN("failed to convert java table names", K(ret));
    }
  }
  return ret;
}

int ObJdbcCatalogPlugin::fetch_namespace_schema(jobject connection, ObIAllocator &allocator,
                                                const ObString &namespace_name,
                                                ObString &canonical_namespace_name,
                                                bool &namespace_found,
                                                bool &connection_reusable)
{
  int ret = OB_SUCCESS;
  connection_reusable = false;
  JNIEnv *jni_env = nullptr;
  jbyteArray java_namespace_name = nullptr;
  jbyteArray java_canonical_name = nullptr;
  LOCAL_REF_GUARD_ENV(java_namespace_name, jni_env);
  LOCAL_REF_GUARD_ENV(java_canonical_name, jni_env);
  namespace_found = false;
  canonical_namespace_name.reset();
  if (OB_ISNULL(catalog_plugin_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("java jdbc catalog is not initialized", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_bytes(
                 jni_env, namespace_name, java_namespace_name))) {
    LOG_WARN("failed to create java namespace name", K(ret));
  }
  OBJNI_RUN_JDBC(*this, java_canonical_name = static_cast<jbyteArray>(jni_env->CallObjectMethod(
                catalog_plugin_.handle(), fetch_namespace_schema_method_, connection, java_namespace_name)), connection_reusable);
  if (OB_SUCC(ret) && OB_NOT_NULL(java_canonical_name)) {
    namespace_found = true;
    if (OB_FAIL(ObJniTool::instance().bytes_from_java(
            jni_env, java_canonical_name, allocator, canonical_namespace_name))) {
      LOG_WARN("failed to convert java namespace schema", K(ret));
    }
  }
  return ret;
}

int ObJdbcCatalogPlugin::fetch_lake_table_metadata(jobject connection, ObIAllocator &allocator,
                                                   const ObString &namespace_name,
                                                   const ObString &table_name,
                                                   const ObCompatibilityMode target_mode,
                                                   ObString &metadata_json,
                                                   bool &table_found,
                                                   bool &connection_reusable)
{
  int ret = OB_SUCCESS;
  connection_reusable = false;
  JNIEnv *jni_env = nullptr;
  jbyteArray java_namespace_name = nullptr;
  jbyteArray java_table_name = nullptr;
  jstring java_target_mode = nullptr;
  jbyteArray java_metadata = nullptr;
  jsize metadata_length = 0;
  const char *target_mode_name = ORACLE_MODE == target_mode ? "ORACLE" : "MYSQL";
  LOCAL_REF_GUARD_ENV(java_namespace_name, jni_env);
  LOCAL_REF_GUARD_ENV(java_table_name, jni_env);
  LOCAL_REF_GUARD_ENV(java_target_mode, jni_env);
  LOCAL_REF_GUARD_ENV(java_metadata, jni_env);
  table_found = false;
  metadata_json.reset();

  if (OB_ISNULL(catalog_plugin_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("java jdbc catalog is not initialized", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_bytes(jni_env, namespace_name, java_namespace_name))) {
    LOG_WARN("failed to create java namespace name", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_bytes(jni_env, table_name, java_table_name))) {
    LOG_WARN("failed to create java table name", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().create_java_string(jni_env, target_mode_name, java_target_mode))) {
    LOG_WARN("failed to create java target mode", K(ret));
  }

  OBJNI_RUN_JDBC(*this, java_metadata =
                static_cast<jbyteArray>(jni_env->CallObjectMethod(catalog_plugin_.handle(),
                                                                  fetch_lake_table_metadata_method_,
                                                                  connection,
                                                                  java_namespace_name,
                                                                  java_table_name,
                                                                  java_target_mode)), connection_reusable);
  if (OB_SUCC(ret) && OB_NOT_NULL(java_metadata)) {
    OBJNI_RUN(metadata_length = jni_env->GetArrayLength(java_metadata));
    if (OB_FAIL(ret)) {
    } else if (metadata_length <= 0 || metadata_length > share::MAX_METADATA_JSON_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid java jdbc metadata payload length", K(ret), K(metadata_length));
    } else if (OB_FAIL(ObJniTool::instance().bytes_from_java(
                   jni_env, java_metadata, allocator, metadata_json))) {
      LOG_WARN("failed to copy jdbc metadata payload", K(ret), K(metadata_length));
    } else {
      table_found = true;
    }
  }
  return ret;
}

} // namespace plugin
} // namespace oceanbase
