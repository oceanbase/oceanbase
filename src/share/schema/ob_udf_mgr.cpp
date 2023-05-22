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
#include "ob_udf_mgr.h"
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

ObSimpleUDFSchema::ObSimpleUDFSchema()
  : ObSchema(), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), udf_name_(),
    ret_(ObUDF::UDFRetType::UDF_RET_UNINITIAL), dl_(), type_(ObUDF::UDFType::UDF_TYPE_UNINITIAL),
    schema_version_(common::OB_INVALID_VERSION)
{
  reset();
}

ObSimpleUDFSchema::ObSimpleUDFSchema(ObIAllocator *allocator)
  : ObSchema(allocator), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), udf_name_(),
    ret_(ObUDF::UDFRetType::UDF_RET_UNINITIAL), dl_(), type_(ObUDF::UDFType::UDF_TYPE_UNINITIAL),
    schema_version_(common::OB_INVALID_VERSION)
{
  reset();
}

ObSimpleUDFSchema::ObSimpleUDFSchema(const ObSimpleUDFSchema &other)
  : ObSchema(), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), udf_name_(),
    ret_(ObUDF::UDFRetType::UDF_RET_UNINITIAL), dl_(), type_(ObUDF::UDFType::UDF_TYPE_UNINITIAL),
    schema_version_(common::OB_INVALID_VERSION)
{
  reset();
  *this = other;
}

ObSimpleUDFSchema::~ObSimpleUDFSchema()
{
}

void ObSimpleUDFSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  udf_id_ = OB_INVALID_ID;
  udf_name_.reset();
}

bool ObSimpleUDFSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_ ||
      schema_version_ < 0 ||
      udf_name_.empty()) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleUDFSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleUDFSchema);
  convert_size += udf_name_.length() + 1;
  convert_size += dl_.length() + 1;

  return convert_size;
}

ObSimpleUDFSchema &ObSimpleUDFSchema::operator =(const ObSimpleUDFSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    udf_id_ = other.udf_id_;
    schema_version_ = other.schema_version_;
    ret_ = other.ret_;
    type_ = other.type_;
    if (OB_FAIL(deep_copy_str(other.udf_name_, udf_name_))) {
      LOG_WARN("Fail to deep copy udf name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.dl_, dl_))) {
      LOG_WARN("Fail to deep copy udf name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}


//////////////////////////////////////////////////////////////////////////


ObUDFMgr::ObUDFMgr() :
    is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    udf_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_UDF, ObCtxIds::SCHEMA_SERVICE)),
    udf_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDF, ObCtxIds::SCHEMA_SERVICE))
  {
  }

ObUDFMgr::ObUDFMgr(common::ObIAllocator &allocator) :
    is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    udf_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_UDF, ObCtxIds::SCHEMA_SERVICE)),
    udf_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDF, ObCtxIds::SCHEMA_SERVICE))
{
}

ObUDFMgr::~ObUDFMgr()
{
}

int ObUDFMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private udf manager twice", K(ret));
  } else if (OB_FAIL(udf_map_.init())) {
    LOG_WARN("init private udf map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObUDFMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "udf manger not init");
  } else {
    udf_infos_.clear();
    udf_map_.clear();
  }
}

ObUDFMgr &ObUDFMgr::operator =(const ObUDFMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("udf manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObUDFMgr::assign(const ObUDFMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("udf manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(udf_map_.assign(other.udf_map_))) {
      LOG_WARN("assign udf map failed", K(ret));
    } else if (OB_FAIL(udf_infos_.assign(other.udf_infos_))) {
      LOG_WARN("assign udf infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObUDFMgr::deep_copy(const ObUDFMgr &other)
{
  int ret = OB_SUCCESS;
  UNUSED(other);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("udf manager not init", K(ret));
  } else if (this != &other) {
    reset();
    for (UDFIter iter = other.udf_infos_.begin();
       OB_SUCC(ret) && iter != other.udf_infos_.end(); iter++) {
      ObSimpleUDFSchema *udf_info = *iter;
      if (OB_ISNULL(udf_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(udf_info), K(ret));
      } else if (OB_FAIL(add_udf(*udf_info))) {
        LOG_WARN("add outline failed", K(*udf_info), K(ret));
      }
    }
  }
  return ret;
}

int ObUDFMgr::get_udf_schema_with_name(const uint64_t tenant_id,
                                       const common::ObString &name,
                                       const ObSimpleUDFSchema *&udf_schema) const
{
  int ret = OB_SUCCESS;
  udf_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObSimpleUDFSchema *tmp_schema = NULL;
    ObUDFHashWrapper hash_wrap(tenant_id, name);
    if (OB_FAIL(udf_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("udf is not exist", K(tenant_id), K(name));
      }
    } else {
      udf_schema = tmp_schema;
    }
  }
  return ret;
}

int ObUDFMgr::add_udf(const ObSimpleUDFSchema &udf_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObSimpleUDFSchema *new_udf_schema = NULL;
  UDFIter iter = NULL;
  ObSimpleUDFSchema *replaced_udf = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("udf manager not init", K(ret));
  } else if (OB_UNLIKELY(!udf_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(udf_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, udf_schema, new_udf_schema))) {
    LOG_WARN("alloca udf schema failed", K(ret));
  } else if (OB_ISNULL(new_udf_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_udf_schema), K(ret));
  } else if (OB_FAIL(udf_infos_.replace(new_udf_schema,
                                        iter,
                                        compare_udf,
                                        equal_udf,
                                        replaced_udf))) {
      LOG_WARN("failed to add udf schema", K(ret));
  } else {
    ObUDFHashWrapper hash_wrapper(new_udf_schema->get_tenant_id(),
                                  new_udf_schema->get_udf_name());
    if (OB_FAIL(udf_map_.set_refactored(hash_wrapper, new_udf_schema, overwrite))) {
      LOG_WARN("build udf hash map failed", K(ret));
    }
  }
  if (udf_infos_.count() != udf_map_.item_count()) {
    LOG_WARN("udf info is non-consistent",
             "udf infos count", udf_infos_.count(),
             "udf map item count", udf_map_.item_count(),
             "udf name", udf_schema.get_udf_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObUDFMgr::rebuild_udf_hashmap(udf_infos_, udf_map_))) {
      LOG_WARN("rebuild udf hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObUDFMgr::rebuild_udf_hashmap(const UDFInfos &udf_infos, ObUDFMap& udf_map)
{
  int ret = OB_SUCCESS;
  udf_map.clear();
  ConstUDFIter iter = udf_infos.begin();
  for (; iter != udf_infos.end() && OB_SUCC(ret); ++iter) {
    ObSimpleUDFSchema *udf_schema = *iter;
    if (OB_ISNULL(udf_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf schema is NULL", K(udf_schema), K(ret));
    } else {
      bool overwrite = true;
      ObUDFHashWrapper hash_wrapper(udf_schema->get_tenant_id(),
                                    udf_schema->get_udf_name());
      if (OB_FAIL(udf_map.set_refactored(hash_wrapper, udf_schema, overwrite))) {
        LOG_WARN("build udf hash map failed", K(ret));
      }
    }
  }
  return ret;
}

int ObUDFMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleUDFSchema *> schemas;
    if (OB_FAIL(get_udf_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get udf schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantUDFId tenant_udf_id(tenant_id,
                                    (*schema)->get_udf_name());
        if (OB_FAIL(del_udf(tenant_udf_id))) {
          LOG_WARN("del udf failed",
                   "tenant_id", tenant_udf_id.tenant_id_,
                   "udf_name", tenant_udf_id.udf_name_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}


int ObUDFMgr::add_udfs(const common::ObIArray<ObSimpleUDFSchema> &udf_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < udf_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_udf(udf_schemas.at(i)))) {
      LOG_WARN("push udf failed", K(ret));
    }
  }
  return ret;
}

int ObUDFMgr::del_udf(const ObTenantUDFId &udf)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSimpleUDFSchema *schema_to_del = NULL;
  if (!udf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(udf));
  } else if (OB_FAIL(udf_infos_.remove_if(udf,
                                          compare_with_tenant_udf_id,
                                          equal_to_tenant_udf_id,
                                          schema_to_del))) {
    LOG_WARN("failed to remove udf schema, ",
             "tenant id", udf.tenant_id_,
             "udf name", udf.udf_name_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed udf schema return NULL, ",
             "tenant id", udf.tenant_id_,
             "udf name", udf.udf_name_,
             K(ret));
  } else {
    ObUDFHashWrapper udf_wrapper(schema_to_del->get_tenant_id(),
                                 schema_to_del->get_udf_name());
    hash_ret = udf_map_.erase_refactored(udf_wrapper);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete udf from udf hashmap, ",
               K(ret),
               K(hash_ret),
               "tenant_id", schema_to_del->get_tenant_id(),
               "name", schema_to_del->get_udf_name());
    }
  }
  if (udf_infos_.count() != udf_map_.item_count()) {
    LOG_WARN("udf info is non-consistent",
             "udf infos count", udf_infos_.count(),
             "udf map item count", udf_map_.item_count(),
             "udf name", udf.udf_name_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObUDFMgr::rebuild_udf_hashmap(udf_infos_, udf_map_))) {
      LOG_WARN("rebuild udf hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

bool ObUDFMgr::compare_with_tenant_udf_id(const ObSimpleUDFSchema *lhs,
                                          const ObTenantUDFId &tenant_udf_id)
{
  return NULL != lhs ? (lhs->get_tenant_udf_id() < tenant_udf_id) : false;
}

bool ObUDFMgr::equal_to_tenant_udf_id(const ObSimpleUDFSchema *lhs,
                                      const ObTenantUDFId &tenant_udf_id)
{
  return NULL != lhs ? (lhs->get_tenant_udf_id() == tenant_udf_id) : false;
}


int ObUDFMgr::get_udf_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSimpleUDFSchema *> &udf_schemas) const
{
  int ret = OB_SUCCESS;
  udf_schemas.reset();

  ConstUDFIter tenant_udf_begin = udf_infos_.begin();
  for (ConstUDFIter iter = tenant_udf_begin;
      OB_SUCC(ret) && iter != udf_infos_.end(); ++iter) {
    const ObSimpleUDFSchema *udf = NULL;
    if (OB_ISNULL(udf = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(udf));
    } else if (tenant_id != udf->get_tenant_id()) {
      //do nothing
    } else if (OB_FAIL(udf_schemas.push_back(udf))) {
      LOG_WARN("push back udf failed", K(ret));
    }
  }

  return ret;
}

int ObUDFMgr::get_udf_schema(const uint64_t udf_id,
                             const ObSimpleUDFSchema *&udf_schema) const
{
  int ret = OB_SUCCESS;
  // traverse is not a good idea, but it seems that i have no choice.
  for (int64_t i = 0; i < udf_infos_.count() && OB_SUCC(ret); ++i) {
    if (udf_id == udf_infos_.at(i)->get_udf_id()) {
      udf_schema = udf_infos_.at(i);
    }
  }
  return ret;
}

int ObUDFMgr::get_udf_schema_count(int64_t &udf_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("udf manager not init", K(ret));
  } else {
    udf_schema_count = udf_infos_.size();
  }
  return ret;
}

int ObUDFMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = UDF_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = udf_infos_.size();
    for (ConstUDFIter it = udf_infos_.begin(); OB_SUCC(ret) && it != udf_infos_.end(); it++) {
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

}// end schema
}// end share
}// end oceanbase
