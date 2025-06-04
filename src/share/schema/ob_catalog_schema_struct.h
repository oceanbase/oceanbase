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

#ifndef _OB_OCEANBASE_SCHEMA_CATALOG_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_CATALOG_SCHEMA_STRUCT_H

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObTenantCatalogId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantCatalogId() : ObTenantCommonSchemaId() {}
  ObTenantCatalogId(const uint64_t tenant_id, const uint64_t catalog_id)
    : ObTenantCommonSchemaId(tenant_id, catalog_id) {}
};

class ObCatalogSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObCatalogSchema();

  //only for internal catalog schema
  ObCatalogSchema(bool is_mysql_mode);
  explicit ObCatalogSchema(common::ObIAllocator *allocator);
  virtual ~ObCatalogSchema();
  ObCatalogSchema(const ObCatalogSchema &src_schema);
  ObCatalogSchema &operator=(const ObCatalogSchema &src_schema);
  int assign(const ObCatalogSchema &src_schema);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_catalog_id(const uint64_t catalog_id) { catalog_id_ = catalog_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int set_catalog_name(const char *catalog_name)
  {
    return deep_copy_str(catalog_name, catalog_name_);
  }
  inline int set_catalog_name(const common::ObString &catalog_name)
  {
    return deep_copy_str(catalog_name, catalog_name_);
  }
  inline int set_catalog_properties(const char *catalog_properties)
  {
    return deep_copy_str(catalog_properties, catalog_properties_);
  }
  inline int set_catalog_properties(const common::ObString &catalog_properties)
  {
    return deep_copy_str(catalog_properties, catalog_properties_);
  }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) {name_case_mode_ = mode; }
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_catalog_id() const { return catalog_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_catalog_name() const { return extract_str(catalog_name_); }
  inline const common::ObString &get_catalog_name_str() const { return catalog_name_; }
  inline const char *get_catalog_properties() const { return extract_str(catalog_properties_); }
  inline const common::ObString &get_catalog_properties_str() const { return catalog_properties_; }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline ObTenantCatalogId get_tenant_catalog_id() const { return ObTenantCatalogId(tenant_id_, catalog_id_); }
  inline static const ObCatalogSchema* get_internal_catalog_schema_mysql() {
    static ObCatalogSchema internal_catalog_schema_mysql(true);
    return &internal_catalog_schema_mysql;
  }
  inline static const ObCatalogSchema* get_internal_catalog_schema_oracle() {
    static ObCatalogSchema internal_catalog_schema_oracle(false);
    return &internal_catalog_schema_oracle;
  }
  //other methods
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual void reset();
  TO_STRING_KV(K_(tenant_id), K_(catalog_id), K_(schema_version), K_(catalog_name),
               K_(catalog_properties), K_(name_case_mode), K_(comment));
private:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  int64_t schema_version_;
  common::ObString catalog_name_;
  common::ObString catalog_properties_;
  common::ObNameCaseMode name_case_mode_;//default:OB_NAME_CASE_INVALID
  common::ObString comment_;
};

struct ObCatalogPrivSortKey
{
  ObCatalogPrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObCatalogPrivSortKey(const uint64_t tenant_id, const uint64_t user_id,
                       const common::ObString &catalog)
      : tenant_id_(tenant_id), user_id_(user_id), catalog_(catalog)
  {}
  bool operator==(const ObCatalogPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (catalog_ == rhs.catalog_);
  }
  bool operator!=(const ObCatalogPrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObCatalogPrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = catalog_ < rhs.catalog_;
      }
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(catalog_.ptr(), catalog_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  int deep_copy(const ObCatalogPrivSortKey &src, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    tenant_id_ = src.tenant_id_;
    user_id_ = src.user_id_;
    if (OB_FAIL(common::ob_write_string(allocator, src.catalog_, catalog_))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy catalog", KR(ret), K(src.catalog_));
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(catalog));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString catalog_;
};

class ObCatalogPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  //constructor and destructor
  ObCatalogPriv()
      : ObSchema(), ObPriv()
  { }
  explicit ObCatalogPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(allocator)
  { }
  ObCatalogPriv(const ObCatalogPriv &other)
      : ObSchema(), ObPriv()
  { *this = other; }
  virtual ~ObCatalogPriv() { }

  //operator=
  ObCatalogPriv& operator=(const ObCatalogPriv &other);

  //for sort
  ObCatalogPrivSortKey get_sort_key() const
  { return ObCatalogPrivSortKey(tenant_id_, user_id_, catalog_); }
  static bool cmp(const ObCatalogPriv *lhs, const ObCatalogPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObCatalogPriv *lhs, const ObCatalogPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObCatalogPriv *lhs, const ObCatalogPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObCatalogPriv *lhs, const ObCatalogPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  //set methods
  inline int set_catalog_name(const char *catalog) { return deep_copy_str(catalog, catalog_); }
  inline int set_catalog_name(const common::ObString &catalog) { return deep_copy_str(catalog, catalog_); }

  //get methods
  inline const char* get_catalog_name() const { return extract_str(catalog_); }
  inline const common::ObString& get_catalog_name_str() const { return catalog_; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(catalog), "privileges", ObPrintPrivSet(priv_set_));
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  common::ObString catalog_;
};

template<class T, class V>
struct ObGetCatalogPrivKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};
template<>
struct ObGetCatalogPrivKey<ObCatalogPrivSortKey, ObCatalogPriv *>
{
  ObCatalogPrivSortKey operator()(const ObCatalogPriv *catalog_priv) const
  {
    ObCatalogPrivSortKey key;
    return NULL != catalog_priv ? catalog_priv->get_sort_key() : key;
  }
};

}//namespace schema
}//namespace share
}//namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */
