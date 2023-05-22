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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_keystore_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_utils.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;
using namespace common::hash;

ObKeystoreMgr::ObKeystoreMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      keystore_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_KEYSTORE, ObCtxIds::SCHEMA_SERVICE)),
      keystore_map_(SET_USE_500(ObModIds::OB_SCHEMA_KEYSTORE, ObCtxIds::SCHEMA_SERVICE))
{
}
ObKeystoreMgr::ObKeystoreMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      keystore_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_KEYSTORE, ObCtxIds::SCHEMA_SERVICE)),
      keystore_map_(SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_SCHEMA_KEYSTORE, ObCtxIds::SCHEMA_SERVICE)))
{
}
ObKeystoreMgr::~ObKeystoreMgr()
{
}
int ObKeystoreMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private keystore manager twice", K(ret));
  } else if (OB_FAIL(keystore_map_.init())) {
    LOG_WARN("init private keystore map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}
void ObKeystoreMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "keystore manger not init");
  } else {
    keystore_infos_.clear();
    keystore_map_.clear();
  }
}
ObKeystoreMgr &ObKeystoreMgr::operator =(const ObKeystoreMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("keystore manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}
int ObKeystoreMgr::assign(const ObKeystoreMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("keystore manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(keystore_map_.assign(other.keystore_map_))) {
      LOG_WARN("assign keystore map failed", K(ret));
    } else if (OB_FAIL(keystore_infos_.assign(other.keystore_infos_))) {
      LOG_WARN("assign keystore infos vector failed", K(ret));
    }
  }
  return ret;
}
int ObKeystoreMgr::deep_copy(const ObKeystoreMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (auto iter = other.keystore_infos_.begin();
         OB_SUCC(ret) && iter != other.keystore_infos_.end(); iter++) {
      ObKeystoreSchema *keystore = *iter;
      if (OB_ISNULL(keystore)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(keystore), K(ret));
      } else if (OB_FAIL(add_keystore(*keystore))) {
        LOG_WARN("add outline failed", K(*keystore), K(ret));
      }
    }
  }
  return ret;
}
int ObKeystoreMgr::add_keystore(const ObKeystoreSchema &keystore_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObKeystoreSchema *new_keystore_schema = NULL;
  KeystoreIter iter = NULL;
  ObKeystoreSchema *replaced_keystore = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("keystore manager not init", K(ret));
  } else if (OB_UNLIKELY(!keystore_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(keystore_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 keystore_schema,
                                                 new_keystore_schema))) {
    LOG_WARN("alloca keystore schema failed", K(ret));
  } else if (OB_ISNULL(new_keystore_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_keystore_schema), K(ret));
  } else if (OB_FAIL(keystore_infos_.replace(new_keystore_schema,
                                             iter,
                                             compare_keystore,
                                             equal_keystore,
                                             replaced_keystore))) {
    LOG_WARN("failed to add keystore schema", K(ret));
  } else {
    ObKeystoreHashWrapper hash_wrapper(new_keystore_schema->get_tenant_id());
    if (OB_FAIL(keystore_map_.set_refactored(hash_wrapper, new_keystore_schema, overwrite))) {
      LOG_WARN("build keystore hash map failed", K(ret));
    } else {
      LOG_INFO("add new keystore to keystore map", K(*new_keystore_schema));
    }
  }
  if (keystore_infos_.count() != keystore_map_.item_count()) {
    LOG_WARN("keystore info is non-consistent",
             "keystore_infos_count",
             keystore_infos_.count(),
             "keystore_map_item_count",
             keystore_map_.item_count(),
             "keystore_id",
             keystore_schema.get_keystore_id(),
             "keystore_name",
             keystore_schema.get_keystore_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObKeystoreMgr::rebuild_keystore_hashmap(keystore_infos_,
                                                                         keystore_map_))) {
      LOG_WARN("rebuild keystore hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}
//用keystore_infos进行重建keystore_map
int ObKeystoreMgr::rebuild_keystore_hashmap(const KeystoreInfos &keystore_infos,
                                            ObKeystoreMap& keystore_map)
{
  int ret = OB_SUCCESS;
  keystore_map.clear();
  ConstKeystoreIter iter = keystore_infos.begin();
  for (; iter != keystore_infos.end() && OB_SUCC(ret); ++iter) {
    ObKeystoreSchema *keystore_schema = *iter;
    if (OB_ISNULL(keystore_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keystore schema is NULL", K(keystore_schema), K(ret));
    } else {
      bool overwrite = true;
      ObKeystoreHashWrapper hash_wrapper(keystore_schema->get_tenant_id());
      if (OB_FAIL(keystore_map.set_refactored(hash_wrapper, keystore_schema, overwrite))) {
        LOG_WARN("build keystore hash map failed", K(ret));
      }
    }
  }
  return ret;
}
int ObKeystoreMgr::add_keystores(const common::ObIArray<ObKeystoreSchema> &keystore_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < keystore_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_keystore(keystore_schemas.at(i)))) {
      LOG_WARN("push keystore failed", K(ret));
    }
  }
  return ret;
}
int ObKeystoreMgr::get_keystore_schema(
    const uint64_t tenant_id,
    const ObKeystoreSchema *&keystore_schema) const
{
  int ret = OB_SUCCESS;
  keystore_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObKeystoreSchema *tmp_schema = NULL;
    ObKeystoreHashWrapper hash_wrap(tenant_id);
    if (OB_FAIL(keystore_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("keystore is not exist", K(tenant_id),
                 "map_cnt", keystore_map_.item_count());
      }
    } else {
      keystore_schema = tmp_schema;
    }
  }
  return ret;
}

bool ObKeystoreMgr::compare_with_tenant_keystore_id(const ObKeystoreSchema *lhs,
                                                    const ObTenantKeystoreId &tenant_keystore_id)
{
  return NULL != lhs ? (lhs->get_tenant_keystore_id() < tenant_keystore_id) : false;
}
bool ObKeystoreMgr::equal_to_tenant_keystore_id(const ObKeystoreSchema *lhs,
                                                const ObTenantKeystoreId &tenant_keystore_id)
{
  return NULL != lhs ? (lhs->get_tenant_keystore_id() == tenant_keystore_id) : false;
}

int ObKeystoreMgr::del_keystore(const ObTenantKeystoreId &keystore)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObKeystoreSchema *schema_to_del = NULL;
  if (!keystore.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(keystore));
  } else if (OB_FAIL(keystore_infos_.remove_if(keystore,
                                              compare_with_tenant_keystore_id,
                                              equal_to_tenant_keystore_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove keystore schema, ",
             "tenant_id",
             keystore.tenant_id_,
             "keystore_id",
             keystore.keystore_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed keystore schema return NULL, ",
             "tenant_id",
             keystore.tenant_id_,
             "keystore_id",
             keystore.keystore_id_,
             K(ret));
  } else {
    ObKeystoreHashWrapper keystore_wrapper(schema_to_del->get_tenant_id());
    hash_ret = keystore_map_.erase_refactored(keystore_wrapper);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete keystore from keystore hashmap, ",
               K(ret),
               K(hash_ret),
               "tenant_id", schema_to_del->get_tenant_id(),
               "name", schema_to_del->get_keystore_name());
    }
  }
  if (keystore_infos_.count() != keystore_map_.item_count()) {
    LOG_WARN("keystore info is non-consistent",
             "keystore_infos_count",
             keystore_infos_.count(),
             "keystore_map_item_count",
             keystore_map_.item_count(),
             "keystore_id",
             keystore.keystore_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObKeystoreMgr::rebuild_keystore_hashmap(keystore_infos_, keystore_map_))) {
      LOG_WARN("rebuild keystore hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}
int ObKeystoreMgr::get_keystore_info_version(uint64_t keystore_id, int64_t &keystore_version) const
{
  int ret = OB_SUCCESS;
  keystore_version = OB_INVALID_ID;
  ConstKeystoreIter iter = keystore_infos_.begin();
  for (; OB_SUCC(ret) && iter != keystore_infos_.end(); ++iter) {
    const ObKeystoreSchema *keystore = NULL;
    if (OB_ISNULL(keystore = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (keystore->get_keystore_id() == keystore_id) {
      keystore_version = keystore->get_schema_version();
      break;
    }
  }
  return ret;
}

int ObKeystoreMgr::get_keystore_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObKeystoreSchema *> &keystore_schemas) const
{
  int ret = OB_SUCCESS;
  keystore_schemas.reset();
  bool is_stop = false;
  for (ConstKeystoreIter iter = keystore_infos_.begin();
      OB_SUCC(ret) && iter != keystore_infos_.end() && !is_stop; ++iter) {
    const ObKeystoreSchema *keystore = NULL;
    if (OB_ISNULL(keystore = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(keystore));
    } else if (tenant_id != keystore->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(keystore_schemas.push_back(keystore))) {
      LOG_WARN("push back keystore failed", K(ret));
    }
  }
  return ret;
}

int ObKeystoreMgr::get_keystore_schema_count(int64_t &keystore_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    keystore_schema_count = keystore_infos_.count();
  }
  return ret;
}

int ObKeystoreMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = KEYSTORE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = keystore_infos_.size();
    for (ConstKeystoreIter it = keystore_infos_.begin(); OB_SUCC(ret) && it != keystore_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
