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

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_EXTERNAL_RESOURCE_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_EXTERNAL_RESOURCE_H_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "pl/external_routine/ob_java_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/jni_env/ob_java_env.h"
#include "lib/jni_env/ob_jni_connector.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObSchemaGetterGuard;

}

}

namespace pl
{

template <typename Key, typename Value, typename Node>
class ObExternalResourceBase
{
  friend Node;
private:
  explicit ObExternalResourceBase(ObIAllocator &alloc) : alloc_(alloc)
  {  }

  ObExternalResourceBase(const ObExternalResourceBase &) = delete;
  ObExternalResourceBase& operator=(const ObExternalResourceBase*) = delete;

public:
  using Self = Node;
  using ResourceKey = Key;
  using Resource = Value;

  template<typename ...ExtraInfo>
  static int fetch(ObIAllocator &alloc, const ResourceKey &key, Self *&node, ExtraInfo&& ...extra_info) { return Node::fetch_impl(alloc, key, node, std::forward<ExtraInfo>(extra_info)...); }

  template<typename ...ExtraInfo>
  int check_valid(ExtraInfo&& ...extra_info) { return static_cast<Self*>(this)->check_valid_impl(std::forward<ExtraInfo>(extra_info)...); }

  template<typename ...ExtraInfo>
  int get_resource(Resource *&data, ExtraInfo&& ...extra_info)
  {
    int ret = OB_SUCCESS;

    data = nullptr;

    if (OB_ISNULL(data_)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected NULL data_ in cache node", K(ret), K(lbt()));
    } else if (OB_FAIL(check_valid(std::forward<ExtraInfo>(extra_info)...))) {
      PL_LOG(WARN, "failed to check resource valid", K(ret));
    } else {
      data = data_;
    }

    return ret;
  }

protected:
  ObIAllocator &alloc_;
  Resource* data_ = nullptr;
};

// URL Resource is cached on ObExecContext
class ObExternalURLJar: public ObExternalResourceBase<ObString, jobject, ObExternalURLJar>
{
public:
  static int fetch_impl(ObIAllocator &alloc, const ResourceKey &key, Self *&node);

  // the only way to check whether the jar file is changed is to fetch it again.
  // but fetching is too expensive, so we cache the jar in ObExecContext and assume it is always valid.
  int check_valid_impl() { return OB_SUCCESS; }

  ~ObExternalURLJar()
  {
    if (OB_NOT_NULL(data_)) {
      ObJavaUtils::delete_global_ref(*data_);
      alloc_.free(static_cast<void*>(data_));
      data_ = nullptr;
    }
  }
private:
  explicit ObExternalURLJar(ObIAllocator &alloc) : ObExternalResourceBase(alloc)
  {  }

  static int curl_fetch(const ObString &url, ObSqlString &jar);
  static size_t curl_write_callback(const void *ptr, size_t size, size_t nmemb, void *buffer);
};

// Schema Resource is cached on ObSQLSessionInfo
class ObExternalSchemaJar: public ObExternalResourceBase<std::pair<uint64_t, ObString>, jobject, ObExternalSchemaJar>
{
public:
  static int fetch_impl(ObIAllocator &alloc, const ResourceKey &key, Self *&node, share::schema::ObSchemaGetterGuard &schema_guard);

  int check_valid_impl(share::schema::ObSchemaGetterGuard &schema_guard);

  bool is_inited() const { return OB_INVALID_ID != resource_id_ && OB_INVALID_VERSION != schema_version_; }

  ~ObExternalSchemaJar()
  {
    if (OB_NOT_NULL(data_)) {
      ObJavaUtils::delete_global_ref(*data_);
      alloc_.free(static_cast<void*>(data_));
      data_ = nullptr;
    }
  }

private:
  ObExternalSchemaJar(ObIAllocator &alloc, uint64_t resource_id, int64_t schema_version)
    : ObExternalResourceBase(alloc),
      resource_id_(resource_id),
      schema_version_(schema_version)
  {  }

  int fetch_from_inner_table(ObSqlString &jar) const;

private:
  uint64_t resource_id_ = OB_INVALID_ID;
  int64_t schema_version_ = OB_INVALID_VERSION;
};

template<typename Node>
class ObExternalResourceCache
{
public:
  using ResourceKey = typename Node::ResourceKey;
  using Resource = typename Node::Resource;

  int init() { return map_.create(MAX_RES_COUNT, ObMemAttr(MTL_ID(), "PlExtRes")); };

  template<typename ...ExtraInfo>
  int get_resource(int64_t udf_id, const ResourceKey &res_key, Resource &data, ExtraInfo&& ...extra_info)
  {
    int ret = OB_SUCCESS;

    Node *node = nullptr;
    Resource *result = nullptr;

    data = nullptr;

    if (OB_FAIL(map_.get_refactored(udf_id, node))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;

        Node *tmp_node = nullptr;
        if (OB_FAIL(fetch_resource_node(res_key, tmp_node, std::forward<ExtraInfo>(extra_info)...))) {
          PL_LOG(WARN, "failed to fetch resource node", K(ret));
        } else if (OB_FAIL(map_.set_refactored(udf_id, tmp_node))) {
          PL_LOG(WARN, "failed to set resource node to cache map", K(ret), K(udf_id), K(tmp_node));

          if (OB_NOT_NULL(tmp_node)) {
            tmp_node->~Node();
            alloc_.free(tmp_node);
            tmp_node = nullptr;
          }
        } else {
          node = tmp_node;
        }
      } else {
        PL_LOG(WARN, "failed to get resource from cache map", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected NULL cache node", K(ret), K(lbt()));
    } else if (OB_FAIL(node->get_resource(result, std::forward<ExtraInfo>(extra_info)...))) {
      PL_LOG(WARN, "failed to get resource from cache node", K(ret));

      if (OB_OLD_SCHEMA_VERSION == ret) {
        // do not overwrite ret
        int tmp_ret = map_.erase_refactored(udf_id);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          PL_LOG(WARN, "failed to erase old version of resource", K(ret), K(tmp_ret), K(udf_id));
        } else {
          node->~Node();
        }
      }
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected NULL resource", K(ret), K(udf_id));
    } else {
      data = *result;
    }

    return ret;
  }

  ~ObExternalResourceCache()
  {
    for (typename common::hash::ObHashMap<uint64_t, Node *>::iterator it = map_.begin();
         it != map_.end();
         ++it) {
      if (OB_NOT_NULL(it->second)) {
        it->second->~Node();
        alloc_.free(it->second);
        it->second = nullptr;
      }
    }

    map_.reuse();
  }

private:
  template<typename ...ExtraInfo>
  int fetch_resource_node(const ResourceKey &key, Node *&node, ExtraInfo&& ...extra_info)
  {
    return Node::fetch(alloc_, key, node, std::forward<ExtraInfo>(extra_info)...);
  }

private:
  static constexpr int64_t MAX_RES_COUNT = 1024;

  ObArenaAllocator alloc_;
  common::hash::ObHashMap<uint64_t, Node*> map_;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_EXTERNAL_RESOURCE_H_
