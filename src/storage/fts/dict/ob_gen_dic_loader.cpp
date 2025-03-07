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

#include "storage/fts/dict/ob_gen_dic_loader.h"

#include "rootserver/ob_root_service.h"
#include "share/ob_server_struct.h"
#include "storage/fts/dict/ob_ik_utf8_dic_loader.h"
#include "storage/fts/ob_fts_literal.h"
#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{
/**
 * -----------------------------------ObDicLoaderID-----------------------------------
 */
int ObGenDicLoader::ObGenDicLoaderKey::init(
    const uint64_t tenant_id,
    const ObString &parser_name,
    const ObCharsetType charset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || parser_name.empty() || CHARSET_INVALID == charset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(parser_name), K(charset));
  } else if (OB_FAIL(set_parser_name(parser_name))) {
    LOG_WARN("fail to set parser name", K(ret), K(parser_name));
  } else {
    tenant_id_ = tenant_id;
    charset_ = charset;
  }
  return ret;
}
int ObGenDicLoader::ObGenDicLoaderKey::assign(const ObGenDicLoaderKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the other is invalid", K(ret), K(other));
  } else if (OB_FAIL(set_parser_name(other.parser_name_))) {
    LOG_WARN("fail to set parser name", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    charset_ = other.charset_;
  }
  return ret;
}
int ObGenDicLoader::ObGenDicLoaderKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObGenDicLoader::ObGenDicLoaderKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&parser_name_, sizeof(parser_name_), hash_val);
  hash_val = murmurhash(&charset_, sizeof(charset_), hash_val);
  return hash_val;
}

int ObGenDicLoader::ObGenDicLoaderKey::set_parser_name(const char *parser_name)
{
  int ret = OB_SUCCESS;
  uint64_t len = STRLEN(parser_name);
  if (OB_ISNULL(parser_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The parser name is nullptr", K(ret), KP(parser_name));
  } else if (OB_UNLIKELY(len >= OB_PLUGIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The parser name is too long", K(ret), KCSTRING(parser_name));
  } else {
    MEMSET(parser_name_, '\0', OB_PLUGIN_NAME_LENGTH);
    MEMCPY(parser_name_, parser_name, len);
  }
  return ret;
}

int ObGenDicLoader::ObGenDicLoaderKey::set_parser_name(const ObString &parser_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parser_name.empty() || (parser_name.length() >= OB_PLUGIN_NAME_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parser name is not valid", K(ret), K(parser_name));
  } else {
    MEMSET(parser_name_, '\0', OB_PLUGIN_NAME_LENGTH);
    MEMCPY(parser_name_, parser_name.ptr(), parser_name.length());
  }
  return ret;
}

int ObGenDicLoader::ObNeedDeleteDicLoadersFn::operator() (hash::HashMapPair<ObGenDicLoaderKey, ObTenantDicLoader*> &entry)
{
  int ret = OB_SUCCESS;
  const ObGenDicLoaderKey &dic_loader_key = entry.first;
  if (OB_UNLIKELY(!dic_loader_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dic loader key", K(ret), K(dic_loader_key));
  } else {
    ObSchemaGetterGuard schema_guard;
    common::ObArray<uint64_t> all_tenant_ids;
    if (OB_ISNULL(GCTX.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root service is null", K(ret));
    } else if (OB_FAIL(GCTX.root_service_->get_schema_service().get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                                        schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_available_tenant_ids(all_tenant_ids))) {
      LOG_WARN("fail to get available tenant ids", K(ret));
    } else {
      bool is_delete = true;
      for (int64_t i = 0; is_delete && i < all_tenant_ids.count(); ++i) {
        if (dic_loader_key.get_tenant_id() == all_tenant_ids.at(i)) {
          is_delete = false;
        }
      }
      if (is_delete && OB_FAIL(need_delete_loaders_.push_back(dic_loader_key))) {
        LOG_WARN("fail to push back", K(ret), K(dic_loader_key));
      }
    }
  }
  return ret;
}

/**
 * -----------------------------------ObDicLoader-----------------------------------
 */
int ObGenDicLoader::init()
{
  int ret = OB_SUCCESS;
  const uint64_t cap = 16;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("gen dic loader initialize twice", K(ret));
  } else if (!dic_loader_map_.created()
      && OB_FAIL(dic_loader_map_.create(cap, ObMemAttr(OB_SERVER_TENANT_ID, "dic_loader_map")))) {
    LOG_WARN("fail to create dic loader map", K(ret), K(cap));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObGenDicLoader::get_dic_loader(const uint64_t tenant_id,
                                   const ObString &parser_name,
                                   const ObCharsetType charset,
                                   ObTenantDicLoaderHandle &loader_handle)
{
  int ret = OB_SUCCESS;
  ObGenDicLoaderKey dic_loader_key;
  ObTenantDicLoader *dic_loader = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gen dic loader is not inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)
             || parser_name.empty()
             || charset == CHARSET_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(parser_name), K(charset));
  } else if (OB_FAIL(dic_loader_key.init(tenant_id, parser_name, charset))) {
    LOG_WARN("fail to init dic loader key", K(ret), K(tenant_id), K(parser_name), K(charset));
  } else {
    TCWLockGuard guard(lock_);
    if (OB_FAIL(dic_loader_map_.get_refactored(dic_loader_key, dic_loader))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(gen_dic_loader(dic_loader_key, dic_loader))) {
          LOG_WARN("fail to gen dic loader", K(ret), K(dic_loader_key));
        } else if (OB_ISNULL(dic_loader)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the dic loader handle is not valid", K(ret), K(dic_loader_key));
        } else if (OB_FAIL(dic_loader_map_.set_refactored(dic_loader_key, dic_loader))) {
          LOG_WARN("fail to set dic loader map", K(ret), K(dic_loader_key), KPC(dic_loader));
        } else if (OB_FALSE_IT(dic_loader->inc_ref())) {
        } else if (OB_FAIL(loader_handle.set_loader(dic_loader))) {
          LOG_WARN("fail to set dic loader", K(ret), K(dic_loader_key), KPC(dic_loader));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get dic loader", K(ret), K(dic_loader_key));
      }
    } else if (OB_FAIL(loader_handle.set_loader(dic_loader))) {
      LOG_WARN("fail to set dic loader", K(ret), K(dic_loader_key), KPC(dic_loader));
    }
  }
  return ret;
}

int ObGenDicLoader::destroy_dic_loader_for_tenant()
{
  int ret = OB_SUCCESS;
  ObNeedDeleteDicLoadersFn need_del_dic_loader_fn;
  TCWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gen dic loader is not inited", K(ret));
  } else if (OB_FAIL(dic_loader_map_.foreach_refactored(need_del_dic_loader_fn))) {
    LOG_WARN("fail to foreach refactored", K(ret));
  } else {
    const ObIArray<ObGenDicLoaderKey> &need_delete_loaders = need_del_dic_loader_fn.need_delete_loaders_;
    for (int64_t i = 0; i < need_delete_loaders.count(); i++) { // ignore ret to delete other tenant's dic loader
      const ObGenDicLoaderKey &dic_loader_key = need_delete_loaders.at(i);
      ObTenantDicLoader *dic_loader = nullptr;
      // overwrite ret
      if (OB_UNLIKELY(!dic_loader_key.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the dic loader key is not valid", K(ret), K(dic_loader_key));
      } else if (OB_FAIL(dic_loader_map_.get_refactored(dic_loader_key, dic_loader))) {
        LOG_WARN("fail to get dic loader", K(ret), K(dic_loader_key));
      } else if (OB_FAIL(dic_loader_map_.erase_refactored(dic_loader_key))) {
        LOG_WARN("fail to erase dic loader", K(ret), K(dic_loader_key));
      } else if (OB_ISNULL(dic_loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the dic loader is null", K(ret), K(dic_loader_key));
      } else if (0 == dic_loader->dec_ref()) {
        ObMemAttr attr(OB_SERVER_TENANT_ID, "dic_loader");
        OB_DELETE(ObTenantDicLoader, attr, dic_loader);
      }
    }
  }
  return ret;
}

int ObGenDicLoader::gen_dic_loader(
    const ObGenDicLoaderKey &dic_loader_key,
    ObTenantDicLoader *&dic_loader)
{
  int ret = OB_SUCCESS;
  ObString parser_name = dic_loader_key.get_parser_name();
  ObCharsetType charset = dic_loader_key.get_charset();
  dic_loader = nullptr;
  if (nullptr != parser_name.find('.')) {
    parser_name = parser_name.split_on('.');
  }
  if (0 == parser_name.case_compare(ObFTSLiteral::PARSER_NAME_IK)) {
    ObMemAttr attr(OB_SERVER_TENANT_ID, "dic_loader");
    switch (charset)
    {
      case ObCharsetType::CHARSET_UTF8MB4: {
        dic_loader = OB_NEW(ObTenantIKUTF8DicLoader, attr);
        if (OB_ISNULL(dic_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for the loader", K(ret), K(dic_loader_key));
        } else if (OB_FAIL(dic_loader->init())) {
          LOG_WARN("fail to init the dic loader", K(ret), K(dic_loader_key));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the charset is");
        LOG_WARN("not support the charset", K(ret), K(charset));
        break;
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the parser is");
    LOG_WARN("not support the parser", K(ret), K(parser_name));
  }
  return ret;
}
} // end storage
} // end oceanbase