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

#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"
#include "plugin/external_table/ob_external_jni_utils.h"

namespace oceanbase
{
namespace plugin
{

#define OBJNI_RUN_JDBC(jdbc_plugin, statement, reusable)                                            \
  do {                                                                                              \
    if (OB_SUCC(ret)) {                                                                             \
      statement;                                                                                    \
      ObJniExceptionResult exception_result;                                                        \
      (jdbc_plugin).check_exception(jni_env, ret, exception_result, reusable);                      \
      if (exception_result.exception_occured()) {                                                   \
        LOG_WARN("fail to run jdbc jni", "statement", #statement, K(ret), K(exception_result));     \
      } else {                                                                                      \
        LOG_TRACE("success to run jdbc jni", "statement", #statement);                              \
      }                                                                                             \
    }                                                                                               \
  } while (0)

class ObJdbcCatalogPlugin final
{
public:
  ObJdbcCatalogPlugin() = default;
  ~ObJdbcCatalogPlugin() = default;

  static int supports(const common::ObString &plugin_name, bool &supported);
  int init(const common::ObString &plugin_name);
  int connect(const common::ObString &jdbc_url, ObJavaGlobalRef &connection);
  int disconnect(ObJavaGlobalRef &connection);
  void check_exception(JNIEnv *jni_env,
                       int &retcode,
                       ObJniExceptionResult &result,
                       bool &connection_reusable) const;
  int list_namespace_names(jobject connection,
                           common::ObIAllocator &allocator,
                           common::ObIArray<common::ObString> &namespace_names,
                           bool &connection_reusable);
  int list_table_names(jobject connection, common::ObIAllocator &allocator,
                       const common::ObString &namespace_name,
                       common::ObIArray<common::ObString> &table_names,
                       bool &namespace_found,
                       bool &connection_reusable);
  int fetch_namespace_schema(jobject connection, common::ObIAllocator &allocator,
                             const common::ObString &namespace_name,
                             common::ObString &canonical_namespace_name,
                             bool &namespace_found,
                             bool &connection_reusable);
  int fetch_lake_table_metadata(jobject connection, common::ObIAllocator &allocator,
                                const common::ObString &namespace_name,
                                const common::ObString &table_name,
                                const common::ObCompatibilityMode target_mode,
                                common::ObString &metadata_json,
                                bool &table_found,
                                bool &connection_reusable);
  void destroy(common::ObIAllocator &allocator);

private:
  ObJavaGlobalRef catalog_plugin_;
  ObJavaGlobalRef catalog_plugin_class_;

  jmethodID connect_method_ = nullptr;
  jmethodID disconnect_method_ = nullptr;
  jmethodID connection_reusable_method_ = nullptr;
  jmethodID list_namespace_names_method_ = nullptr;
  jmethodID list_table_names_method_ = nullptr;
  jmethodID fetch_namespace_schema_method_ = nullptr;
  jmethodID fetch_lake_table_metadata_method_ = nullptr;

  DISALLOW_COPY_AND_ASSIGN(ObJdbcCatalogPlugin);
};

} // namespace plugin
} // namespace oceanbase
