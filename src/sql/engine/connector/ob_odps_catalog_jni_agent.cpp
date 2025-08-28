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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_catalog_jni_agent.h"
#include <memory>

#include "lib/jni_env/ob_java_env.h"
#include "lib/jni_env/ob_jni_connector.h"

namespace oceanbase {
namespace sql {

#define CHECK_EXCPETION_AND_POINTER_NOT_NULL(env, ptr)          \
  if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env))) { \
    LOG_WARN("failed to check jni exception", K(ret));          \
  } else if (OB_ISNULL(ptr)) {                                  \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("pointer is null", K(ret));                        \
  }

int ObOdpsJniCatalogAgent::get_java_catalog_service_class()
{
  int ret = OB_SUCCESS;
  jclass catalog_service_factory_class = nullptr;
  jmethodID catalog_service_factory_constructor = nullptr;
  jobject catalog_service_factory_obj = nullptr;
  jmethodID catalog_service_class_getter = nullptr;
  if (OB_FAIL(ObJniConnector::get_jni_env(env_))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_FAIL(
          ObOdpsJniConnector::get_jni_class(env_, jni_catalog_factory_class_.ptr(), catalog_service_factory_class))) {
    LOG_WARN("failed to get catalog service factory class", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(
                 env_, catalog_service_factory_class, "<init>", "()V", catalog_service_factory_constructor))) {
    LOG_WARN("failed to get catalog service factory constructor", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::construct_jni_object(env_,
                 catalog_service_factory_obj,
                 catalog_service_factory_class,
                 catalog_service_factory_constructor))) {
    LOG_WARN("failed to get catalog service factory object", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(env_,
                 catalog_service_factory_class,
                 "getCatalogServiceClass",
                 "()Ljava/lang/Class;",
                 catalog_service_class_getter))) {
    LOG_WARN("failed to get catalog service factory constructor", K(ret));
  } else {
    jclass catalog_service_class = nullptr;
    catalog_service_class = (jclass)env_->CallObjectMethod(catalog_service_factory_obj, catalog_service_class_getter);
    CHECK_EXCPETION_AND_POINTER_NOT_NULL(env_, catalog_service_class);
    if (OB_SUCC(ret)) {
      jni_catalog_service_class_ = catalog_service_class;
    }
  }

  if (OB_NOT_NULL(catalog_service_factory_class)) {
    env_->DeleteLocalRef(catalog_service_factory_class);
  }
  if (OB_NOT_NULL(catalog_service_factory_obj)) {
    env_->DeleteLocalRef(catalog_service_factory_obj);
  }

  return ret;
}

int ObOdpsJniCatalogAgent::get_java_catalog_service_obj(ObIAllocator &allocator, const share::ObODPSCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  jmethodID catalog_service_constructor = nullptr;
  if (OB_FAIL(ObJniConnector::get_jni_env(env_))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_ISNULL(jni_catalog_service_class_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni catalog service class is null", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(
                 env_, jni_catalog_service_class_, "<init>", "(Ljava/util/Map;)V", catalog_service_constructor))) {
    LOG_WARN("failed to get catalog service constructor", K(ret));
  } else {
    jclass hashmap_class = env_->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = env_->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = env_->NewObject(
        hashmap_class, hashmap_constructor, share::ObODPSCatalogProperties::ObOdpsCatalogOptions::MAX_OPTIONS);
    jmethodID hashmap_put =
        env_->GetMethodID(hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
      LOG_WARN("failed to check jni exception", K(ret));
    } else {
      jstring access_id = nullptr;
      jstring j_access_id = nullptr;
      jstring access_key = nullptr;
      jstring j_access_key = nullptr;
      jstring project = nullptr;
      jstring j_project = nullptr;
      jstring endpoint = nullptr;
      jstring j_endpoint = nullptr;
      jstring tunnel_endpoint = nullptr;
      jstring j_tunnel_endpoint = nullptr;
      jstring region = nullptr;
      jstring j_region = nullptr;
      jstring quota = nullptr;
      jstring j_quota = nullptr;
      jstring compression_code = nullptr;
      jstring j_compression_code = nullptr;
      ObString access_id_str;
      ObString access_key_str;
      ObString project_str;
      ObString endpoint_str;
      ObString tunnel_endpoint_str;
      ObString region_str;
      ObString quota_str;
      ObString compression_code_str;
      if (OB_FAIL(ob_write_string(allocator, properties.access_id_, access_id_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.access_key_, access_key_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.project_, project_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.endpoint_, endpoint_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.tunnel_endpoint_, tunnel_endpoint_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.region_, region_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.quota_, quota_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, properties.compression_code_, compression_code_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "access_id", access_id))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, access_id_str.ptr(), j_access_id))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "access_key", access_key))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, access_key_str.ptr(), j_access_key))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "project", project))) {
        LOG_WARN("failed to gen jni string", K(ret));
        } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, project_str.ptr(), j_project))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "odps_url", endpoint))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, endpoint_str.ptr(), j_endpoint))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "tunnel_url", tunnel_endpoint))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(
                     ObOdpsJniConnector::gen_jni_string(env_, tunnel_endpoint_str.ptr(), j_tunnel_endpoint))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "region", region))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, region_str.ptr(), j_region))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "quota", quota))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, quota_str.ptr(), j_quota))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, "compression_code", compression_code))) {
        LOG_WARN("failed to gen jni string", K(ret));
      } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, compression_code_str.ptr(), j_compression_code))) {
        LOG_WARN("failed to gen jni string", K(ret));
      }
      if (OB_SUCC(ret)) {
        env_->CallObjectMethod(hashmap_object, hashmap_put, access_id, j_access_id);
        env_->CallObjectMethod(hashmap_object, hashmap_put, access_key, j_access_key);
        env_->CallObjectMethod(hashmap_object, hashmap_put, project, j_project);
        env_->CallObjectMethod(hashmap_object, hashmap_put, endpoint, j_endpoint);
        env_->CallObjectMethod(hashmap_object, hashmap_put, tunnel_endpoint, j_tunnel_endpoint);
        env_->CallObjectMethod(hashmap_object, hashmap_put, region, j_region);
        env_->CallObjectMethod(hashmap_object, hashmap_put, quota, j_quota);
        env_->CallObjectMethod(hashmap_object, hashmap_put, compression_code, j_compression_code);
        if (OB_FAIL(ObOdpsJniConnector::construct_jni_object(env_,
                jni_catalog_service_obj_,
                jni_catalog_service_class_,
                catalog_service_constructor,
                hashmap_object))) {
          LOG_WARN("failed to construct jni object", K(ret));
        }
      }
      env_->DeleteLocalRef(access_id);
      env_->DeleteLocalRef(j_access_id);
      env_->DeleteLocalRef(access_key);
      env_->DeleteLocalRef(j_access_key);
      env_->DeleteLocalRef(project);
      env_->DeleteLocalRef(j_project);
      env_->DeleteLocalRef(tunnel_endpoint);
      env_->DeleteLocalRef(j_tunnel_endpoint);
      env_->DeleteLocalRef(region);
      env_->DeleteLocalRef(j_region);
      env_->DeleteLocalRef(quota);
      env_->DeleteLocalRef(j_quota);
      env_->DeleteLocalRef(compression_code);
      env_->DeleteLocalRef(j_compression_code);
      allocator.free(access_id_str.ptr());
      allocator.free(access_key_str.ptr());
      allocator.free(project_str.ptr());
      allocator.free(endpoint_str.ptr());
      allocator.free(tunnel_endpoint_str.ptr());
      allocator.free(region_str.ptr());
      allocator.free(quota_str.ptr());
      allocator.free(compression_code_str.ptr());
    }
    env_->DeleteLocalRef(hashmap_class);
    env_->DeleteLocalRef(hashmap_object);
  }
  return ret;
}
int ObOdpsJniCatalogAgent::do_init(ObIAllocator &allocator, const share::ObODPSCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_java_catalog_service_class())) {
    LOG_WARN("failed to start java env", K(ret));
  } else if (OB_FAIL(get_java_catalog_service_class())) {
    LOG_WARN("failed to get java catalog service class", K(ret));
  } else if (OB_FAIL(get_java_catalog_service_obj(allocator, properties))) {
    LOG_WARN("failed to get java catalog service obj", K(ret));
  }
  return ret;
}

int ObOdpsJniCatalogAgent::do_query_table_list(ObIAllocator &allocator, ObIArray<ObString> &tbl_names) {
  int ret = OB_SUCCESS;
  jclass array_list_class = nullptr;
  jmethodID size_method = nullptr;
  jmethodID get_method = nullptr;
  if (OB_ISNULL(env_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_ISNULL(jni_catalog_service_obj_) || OB_ISNULL(jni_catalog_service_class_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni catalog service obj is null", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_class(env_, "java/util/ArrayList", array_list_class))) {
    LOG_WARN("failed to get array list class", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(env_, array_list_class, "size", "()I", size_method))) {
    LOG_WARN("failed to get size method", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(env_, array_list_class, "get", "(I)Ljava/lang/Object;", get_method))) {
    LOG_WARN("failed to get get method", K(ret));
  } else {
    jmethodID get_tables_method = nullptr;
    if (OB_FAIL(ObOdpsJniConnector::get_jni_method(env_, jni_catalog_service_class_, "listTableNames", "()Ljava/util/List;", get_tables_method))) {
      LOG_WARN("failed to get get tables method", K(ret));
    } else {
      jobject jstr_array_list = env_->CallObjectMethod(jni_catalog_service_obj_, get_tables_method);
      if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
        LOG_WARN("failed to check jni exception", K(ret));
      } else if (OB_ISNULL(jstr_array_list)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("jstr_array_list is null", K(ret));
      } else {
        jint size = env_->CallIntMethod(jstr_array_list, size_method);
        if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
          LOG_WARN("failed to check jni exception", K(ret));
        }
        for (jint i = 0; OB_SUCC(ret) && i < size; i++) {
          jstring j_table_name = (jstring)env_->CallObjectMethod(jstr_array_list, get_method, i);
          if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
            LOG_WARN("failed to check jni exception", K(ret));
          } else if (OB_ISNULL(j_table_name)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("j_table_name is null", K(ret));
          } else {
            const char *table_name = env_->GetStringUTFChars(j_table_name, nullptr);
            if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
              LOG_WARN("failed to check jni exception", K(ret));
            } else {
              ObString tbl_name;
              if (OB_FAIL(ob_write_string(allocator, table_name, tbl_name, true))) {
                LOG_WARN("failed to write string", K(ret));
              } else if (OB_FAIL(tbl_names.push_back(tbl_name))) {
                LOG_WARN("failed to push back string", K(ret));
              }
            }
            env_->ReleaseStringUTFChars(j_table_name, table_name);
            env_->DeleteLocalRef(j_table_name);
          }
        }
        env_->DeleteLocalRef(jstr_array_list);
      }
    }
  }
  env_->DeleteLocalRef(array_list_class);
  return ret;
}

int ObOdpsJniCatalogAgent::do_query_table_info(ObIAllocator &allocator, const ObString &tbl_name, int64_t &last_modification_time_s) {
  int ret = OB_SUCCESS;
  jmethodID get_table_info_method = nullptr;
  if (OB_ISNULL(env_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni env is null", K(ret));
  } else if (OB_ISNULL(jni_catalog_service_obj_) || OB_ISNULL(jni_catalog_service_class_) || OB_ISNULL(tbl_name.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni catalog service obj is null", K(ret));
  } else if (OB_FAIL(ObOdpsJniConnector::get_jni_method(env_, jni_catalog_service_class_, "getTableInfo", "(Ljava/lang/String;)[J", get_table_info_method))) {
    LOG_WARN("failed to get get table info method", K(ret));
  } else {
    jstring jstr_table_name = nullptr;
    ObString tbl_name_cstr;
    if (OB_FAIL(ob_write_string(allocator, tbl_name, tbl_name_cstr, true))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(ObOdpsJniConnector::gen_jni_string(env_, tbl_name_cstr.ptr(), jstr_table_name))) {
      LOG_WARN("failed to gen jni string", K(ret));
    } else {
      jlongArray jlong_array = (jlongArray)env_->CallObjectMethod(jni_catalog_service_obj_, get_table_info_method, jstr_table_name);
      if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
        LOG_WARN("failed to check jni exception", K(ret));
      } else if (OB_ISNULL(jlong_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("jlong_array is null", K(ret));
      } else {
        jsize array_length = env_->GetArrayLength(jlong_array);
        if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
          LOG_WARN("failed to check jni exception", K(ret));
        } else if (array_length == 3) {
          jlong* long_elements = env_->GetLongArrayElements(jlong_array, nullptr);
          if (OB_FAIL(ObOdpsJniConnector::check_jni_exception_(env_))) {
            LOG_WARN("failed to check jni exception", K(ret));
          } else {
            // 假设返回的long数组包含: [create_time, last_ddl_time, last_modification_time]
            // table_info.create_time_s = long_elements[0];
            // table_info.last_ddl_time_s = long_elements[1];
            // table_info.last_modification_time_s = long_elements[2];
            last_modification_time_s = long_elements[2];
            env_->ReleaseLongArrayElements(jlong_array, long_elements, JNI_ABORT);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("array length is less than expected", K(ret), K(array_length));
        }
        env_->DeleteLocalRef(jlong_array);
      }
      env_->DeleteLocalRef(jstr_table_name);
      allocator.free(tbl_name_cstr.ptr());
    }
  }
  return ret;
}

int ObOdpsJniCatalogAgent::do_close() {
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(env_)) {
    env_->DeleteLocalRef(jni_catalog_service_class_);
    env_->DeleteLocalRef(jni_catalog_service_obj_);
  }
  return ret;
}

JNICatalogPtr create_odps_jni_catalog()
{
  const char *catalog_factory_class = "com/oceanbase/external/odps/utils/OdpsTunnelConnectorFactory";
  return std::make_shared<ObOdpsJniCatalogAgent>(catalog_factory_class);
}
}
}