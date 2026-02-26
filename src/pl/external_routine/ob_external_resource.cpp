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

#define USING_LOG_PREFIX PL

#include "pl/external_routine/ob_external_resource.h"

#include <curl/curl.h>

#include "pl/external_routine/ob_java_utils.h"
#include "common/ob_smart_var.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_external_resource_mgr.h"
#include "pl/external_routine/ob_py_utils.h"


namespace oceanbase
{

using share::schema::ObSchemaGetterGuard;
using share::schema::ObSimpleExternalResourceSchema;

namespace pl
{

int ObExternalURLJar::fetch_impl(ObIAllocator &alloc, const ResourceKey &key, Self *&node)
{
  int ret = OB_SUCCESS;

  Self *new_node = nullptr;
  ObSqlString jar;
  jobject *class_loader = nullptr;

  node = nullptr;

  if (key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lbt()));
  } else if (key.prefix_match_ci("http://") || key.prefix_match_ci("https://")) {
    if (OB_FAIL(curl_fetch(key, jar))) {
      LOG_WARN("failed to fetch jar", K(ret), K(key), K(jar));
    }
  } else if (key.prefix_match_ci("file://")) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unknow jar file protocol", K(ret), K(key));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external UDF file protocol");
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(class_loader = static_cast<jobject*>(alloc.alloc(sizeof(jobject))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (FALSE_IT(*class_loader = nullptr)) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::load_routine_jar(jar.string(), *class_loader))) {
    LOG_WARN("failed to load jar", K(ret), K(jar));
  } else if (OB_ISNULL(*class_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class_loader", K(ret));
  } else if (OB_ISNULL(new_node = static_cast<Self*>(alloc.alloc(sizeof(Self))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(new_node = new(new_node)Self(alloc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct new node", K(ret));
  } else {
    new_node->data_ = class_loader;
    node = new_node;
  }

  return ret;
}

int ObExternalURLJar::curl_fetch(const ObString &url, ObSqlString &jar)
{
  int ret = OB_SUCCESS;

  SMART_VAR(char[OB_MAX_URI_LENGTH + 1], url_buf) {
    int64_t length = std::min(OB_MAX_URI_LENGTH, static_cast<int64_t>(url.length()));
    STRNCPY(url_buf, url.ptr(), length);
    url_buf[length] = '\0';

    CURL *curl = nullptr;
    CURLcode cc = CURLE_OK;

    int64_t http_code = 0;

    int64_t timeout_ms = THIS_WORKER.get_timeout_remain() / 1000;

    if (strlen(url_buf) != url.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("url length is of invalid format or too long", K(ret), K(url_buf), K(length));
    } else if (OB_ISNULL(curl = curl_easy_init())) {
      ret = OB_CURL_ERROR;
      LOG_WARN("fail to init curl", K(ret));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, true))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_NOSIGNAL));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, true))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_TCP_NODELAY));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_TIMEOUT_MS), K(timeout_ms));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_CONNECTTIMEOUT_MS), K(timeout_ms));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_URL, url_buf))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_URL), K(url_buf));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_WRITEFUNCTION));
    } else if (CURLE_OK !=
                (cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &jar))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set curl option", K(ret), K(cc), K(CURLOPT_WRITEDATA));
    } else if (CURLE_OK !=
                (cc = curl_easy_perform(curl))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to curl_easy_perform", K(ret), K(cc), K(curl_easy_strerror(cc)), K(url_buf));
    } else if (CURLE_OK !=
                (cc = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to curl_easy_getinfo", K(ret), K(cc), K(CURLINFO_RESPONSE_CODE));
    } else if (2 != http_code / 100) {  // HTTP 2xx means success
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to fetch jar from url", K(ret), K(url_buf), K(http_code), K(jar));
    }

    // always cleanup
    if (OB_NOT_NULL(curl)) {
      curl_easy_cleanup(curl);
    }
  }

  return ret;
}

size_t ObExternalURLJar::curl_write_callback(const void *ptr,
                                             size_t size,
                                             size_t nmemb,
                                             void *buffer)
  {
  int ret = OB_SUCCESS;

  size_t total_size = size * nmemb;

  if (OB_ISNULL(ptr) || OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for curl_write_callback", K(ret), K(ptr), K(size), K(nmemb), K(buffer));
  } else if (total_size > 0) {
    ObSqlString &result = *static_cast<ObSqlString*>(buffer);

    if (OB_FAIL(result.append(static_cast<const char*>(ptr), total_size))) {
      LOG_WARN("failed to append to result", K(ret), K(result), K(ptr), K(total_size));
    }
  }

  return OB_SUCC(ret) ? total_size : 0;
}

int ObExternalSchemaJar::fetch_impl(ObIAllocator &alloc,
                                    const ResourceKey &key,
                                    Self *&node,
                                    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  Self *new_node = nullptr;
  ObSqlString jar;
  jobject *class_loader = nullptr;

  const ObSimpleExternalResourceSchema *schema = nullptr;

  node = nullptr;

  if (OB_INVALID_ID == key.first || key.second.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "database_id", key.first, "external_resource_name", key.second);
  } else if (OB_FAIL(schema_guard.get_external_resource_schema(MTL_ID(), key.first, key.second, schema))) {
    LOG_WARN("failed to get_external_resource_schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("external resource not exist", K(ret), K(MTL_ID()), "database_id", key.first, "external_resource_name", key.second);
  } else if (!schema->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid external resource schema", K(ret), KPC(schema));
  } else if (OB_ISNULL(new_node = static_cast<Self*>(alloc.alloc(sizeof(Self))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(new_node = new(new_node)Self(alloc, schema->get_resource_id(), schema->get_schema_version()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct new node", K(ret));
  } else if (OB_FAIL(new_node->fetch_from_inner_table(jar))) {
    LOG_WARN("failed to fetch_from_inner_table", K(ret));
  } else if (OB_ISNULL(class_loader = static_cast<jobject*>(alloc.alloc(sizeof(jobject))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (FALSE_IT(*class_loader = nullptr)) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::load_routine_jar(jar.string(), *class_loader))) {
    LOG_WARN("failed to load jar", K(ret), K(jar));
  } else if (OB_ISNULL(*class_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class_loader", K(ret));
  } else {
    new_node->data_ = class_loader;
  }

  if (OB_SUCC(ret)) {
    node = new_node;
  }

  return ret;
}

int ObExternalSchemaJar::fetch_from_inner_table(ObSqlString &jar) const
{
  int ret = OB_SUCCESS;

  common::sqlclient::ObISQLConnectionPool *pool = nullptr;
  common::sqlclient::ObISQLConnection *connection = nullptr;

  ObSqlString sql;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(lbt()));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL GCTX.sql_proxy_", K(ret));
  } else if (OB_ISNULL(pool = GCTX.sql_proxy_->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL GCTX.sql_proxy_->get_pool()", K(ret));
  } else if (OB_FAIL(pool->acquire(connection, nullptr))) {
    LOG_WARN("failed to acquire connection", K(ret));
  } else if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL connection", K(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT content FROM %s WHERE tenant_id=0 AND resource_id=%lu AND schema_version=%ld",
                                    share::OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
                                    resource_id_,
                                    schema_version_))) {
    LOG_WARN("failed to append_fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObString content;
      ObObj value;
      value.set_null();

      if (OB_FAIL(connection->execute_read(MTL_ID(), sql.string(), res))) {
        LOG_WARN("failed to execute_read", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource in schema guard not found in __all_external_resource_history",
                   K(ret), K(resource_id_), K(schema_version_), K(sql));
        } else {
          LOG_WARN("failed to iter result set", K(ret), K(sql));
        }
      } else if (OB_FAIL(result->get_obj("content", value))) {
        LOG_WARN("failed to get obj from result", K(ret));
      } else if (OB_FAIL(value.get_string(content))) {
        LOG_WARN("failed to get string from obj", K(ret), K(value));
      } else if (OB_FAIL(jar.assign(content))) {
        LOG_WARN("failed to assign content to jar", K(ret), K(content));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to iter result set", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row count in result set", K(ret), K(sql));
      }
    }
  }

  if (OB_NOT_NULL(pool) && OB_NOT_NULL(connection)) {
    int tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(pool->release(connection, true))) {
      LOG_WARN("failed to release SPI connection", K(ret), K(tmp_ret));

      // do not overwrite ret
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else {
      connection = nullptr;
    }
  }

  return ret;
}

int ObExternalSchemaJar::check_valid_impl(share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  const ObSimpleExternalResourceSchema *schema = nullptr;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(lbt()));
  } else if (OB_FAIL(schema_guard.get_external_resource_schema(MTL_ID(), resource_id_, schema))){
    LOG_WARN("failed to get_external_resource_schema", K(ret), K(resource_id_));
  } else if (OB_ISNULL(schema) || schema->get_schema_version() != schema_version_){
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("external resource may be dropped or replaced", K(ret), K(resource_id_), K(schema_version_), KPC(schema));
  }

  return ret;
}


// Python resouce
int ObExternalURLPy::fetch_impl(ObIAllocator &alloc,
                                const ResourceKey &key,
                                Self *&node,
                                ObPyThreadState *tstate,
                                const ObString &func_name,
                                const int64_t udf_id)
{
  int ret = OB_SUCCESS;

  Self *new_node = nullptr;
  ObSqlString py;
  ObPyObject **pyfunc = nullptr;

  node = nullptr;

  if (key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lbt()));
  } else if (key.prefix_match_ci("http://") || key.prefix_match_ci("https://")) {
    if (OB_FAIL(ObExternalURLJar::curl_fetch(key, py))) {
      LOG_WARN("failed to fetch py", K(ret), K(key), K(py));
    }
  } else if (key.prefix_match_ci("file://")) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unknow py file protocol", K(ret), K(key));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external UDF file protocol");
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(pyfunc = static_cast<ObPyObject**>(alloc.alloc(sizeof(ObPyObject*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(ObPyUtils::load_routine_py(tstate, py.string(), udf_id,func_name, *pyfunc))) {
    LOG_WARN("failed to load py", K(ret), K(py), K(func_name));
  } else if (OB_ISNULL(*pyfunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pyfunc", K(ret));
  } else if (OB_ISNULL(new_node = static_cast<Self*>(alloc.alloc(sizeof(Self))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(new_node = new (new_node)Self(alloc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct new node", K(ret));
  } else {
    new_node->data_ = pyfunc;
    node = new_node;
  }

  return ret;
}

int ObExternalSchemaPy::fetch_impl(ObIAllocator &alloc,
                                   const ResourceKey &key,
                                   Self *&node,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   ObPyThreadState *tstate,
                                   const ObString &func_name,
                                   const int64_t udf_id)
{
  int ret = OB_SUCCESS;

  Self *new_node = nullptr;
  ObSqlString py;
  ObPyObject **pyfunc = nullptr;

  const ObSimpleExternalResourceSchema *schema = nullptr;

  node = nullptr;

  if (OB_INVALID_ID == key.first || key.second.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "database_id", key.first, "external_resource_name", key.second);
  } else if (OB_FAIL(schema_guard.get_external_resource_schema(MTL_ID(), key.first, key.second, schema))) {
    LOG_WARN("failed to get_external_resource_schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("external resource not exist", K(ret), K(MTL_ID()), "database_id", key.first, "external_resource_name", key.second);
  } else if (!schema->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid external resource schema", K(ret), KPC(schema));
  } else if (OB_ISNULL(new_node = static_cast<Self*>(alloc.alloc(sizeof(Self))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(new_node = new(new_node)Self(alloc, schema->get_resource_id(), schema->get_schema_version()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct new node", K(ret));
  } else if (OB_FAIL(new_node->fetch_from_inner_table(py))) {
    LOG_WARN("failed to fetch_from_inner_table", K(ret));
  } else if (OB_ISNULL(pyfunc = static_cast<ObPyObject**>(alloc.alloc(sizeof(ObPyObject*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(ObPyUtils::load_routine_py(tstate, py.string(), udf_id, func_name, *pyfunc))) {
    LOG_WARN("failed to load py", K(ret), K(py));
  } else if (OB_ISNULL(pyfunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pyfunc", K(ret));
  } else {
    new_node->data_ = pyfunc;
  }

  if (OB_SUCC(ret)) {
    node = new_node;
  }

  return ret;
}

int ObExternalSchemaPy::fetch_from_inner_table(ObSqlString &py) const
{
  int ret = OB_SUCCESS;

  common::sqlclient::ObISQLConnectionPool *pool = nullptr;
  common::sqlclient::ObISQLConnection *connection = nullptr;

  ObSqlString sql;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(lbt()));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL GCTX.sql_proxy_", K(ret));
  } else if (OB_ISNULL(pool = GCTX.sql_proxy_->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL GCTX.sql_proxy_->get_pool()", K(ret));
  } else if (OB_FAIL(pool->acquire(connection, nullptr))) {
    LOG_WARN("failed to acquire connection", K(ret));
  } else if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL connection", K(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT content FROM %s WHERE tenant_id=0 AND resource_id=%lu AND schema_version=%ld",
                                    share::OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
                                    resource_id_,
                                    schema_version_))) {
    LOG_WARN("failed to append_fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObString content;
      ObObj value;
      value.set_null();

      if (OB_FAIL(connection->execute_read(MTL_ID(), sql.string(), res))) {
        LOG_WARN("failed to execute_read", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource in schema guard not found in __all_external_resource_history",
                   K(ret), K(resource_id_), K(schema_version_), K(sql));
        } else {
          LOG_WARN("failed to iter result set", K(ret), K(sql));
        }
      } else if (OB_FAIL(result->get_obj("content", value))) {
        LOG_WARN("failed to get obj from result", K(ret));
      } else if (OB_FAIL(value.get_string(content))) {
        LOG_WARN("failed to get string from obj", K(ret), K(value));
      } else if (OB_FAIL(py.assign(content))) {
        LOG_WARN("failed to assign content to py", K(ret), K(content));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to iter result set", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row count in result set", K(ret), K(sql));
      }
    }
  }

  if (OB_NOT_NULL(pool) && OB_NOT_NULL(connection)) {
    int tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(pool->release(connection, true))) {
      LOG_WARN("failed to release SPI connection", K(ret), K(tmp_ret));

      // do not overwrite ret
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else {
      connection = nullptr;
    }
  }

  return ret;
}

int ObExternalSchemaPy::check_valid_impl(share::schema::ObSchemaGetterGuard &schema_guard,
                                         ObPyThreadState *tstate,
                                         const ObString &func_name,
                                         const int64_t udf_id)
{
  int ret = OB_SUCCESS;

  const ObSimpleExternalResourceSchema *schema = nullptr;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(lbt()));
  } else if (schema_guard.get_external_resource_schema(MTL_ID(), resource_id_, schema)){
    LOG_WARN("failed to get_external_resource_schema", K(ret), K(resource_id_));
  } else if (OB_ISNULL(schema) || schema->get_schema_version() != schema_version_){
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("external resource may be dropped or replaced", K(ret), K(resource_id_), K(schema_version_), KPC(schema));
  }

  return ret;
}
// Python resouce


} // namespace pl
} // namespace oceanbase
