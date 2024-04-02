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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_MGR_H

#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObSimpleOutlineSchema : public ObSchema
{
public:
  ObSimpleOutlineSchema();
  explicit ObSimpleOutlineSchema(common::ObIAllocator *allocator);
  ObSimpleOutlineSchema(const ObSimpleOutlineSchema &src_schema);
  virtual ~ObSimpleOutlineSchema();
  ObSimpleOutlineSchema &operator =(const ObSimpleOutlineSchema &other);
  bool operator ==(const ObSimpleOutlineSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(outline_id),
               K_(schema_version),
               K_(database_id),
               K_(name),
               K_(signature));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_outline_id(const uint64_t outline_id) { outline_id_ = outline_id; }
  inline uint64_t get_outline_id() const { return outline_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline int set_name(const common::ObString &name)
  { return deep_copy_str(name, name_); }
  inline const char *get_name() const { return extract_str(name_); }
  inline const common::ObString &get_name_str() const { return name_; }
  inline int set_signature(const common::ObString &signature)
  { return deep_copy_str(signature, signature_); }
  inline int set_sql_id(const common::ObString &sql_id)
  { return deep_copy_str(sql_id, sql_id_); }
  inline const char *get_signature() const { return extract_str(signature_); }
  inline const char *get_sql_id() const { return extract_str(sql_id_); }
  inline const common::ObString &get_signature_str() const { return signature_; }
  inline const common::ObString &get_sql_id_str() const { return sql_id_; }
  inline ObTenantOutlineId get_tenant_outline_id() const
  { return ObTenantOutlineId(tenant_id_, outline_id_); }

private:
  uint64_t tenant_id_;
  uint64_t outline_id_;
  int64_t schema_version_;
  uint64_t database_id_;
  common::ObString name_;
  common::ObString signature_;
  common::ObString sql_id_;
};

template<class T, class V>
struct ObGetOutlineKeyV3
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetOutlineKeyV3<uint64_t, ObSimpleOutlineSchema *>
{
  uint64_t operator()(const ObSimpleOutlineSchema *outline_schema) const
  {
    return NULL != outline_schema ? outline_schema->get_outline_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetOutlineKeyV3<ObOutlineNameHashWrapper, ObSimpleOutlineSchema *>
{
  ObOutlineNameHashWrapper operator()(const ObSimpleOutlineSchema *outline_schema) const
  {
    ObOutlineNameHashWrapper name_wrap;
    if (!OB_ISNULL(outline_schema)) {
      name_wrap.set_tenant_id(outline_schema->get_tenant_id());
      name_wrap.set_database_id(outline_schema->get_database_id());
      name_wrap.set_name(outline_schema->get_name_str());
    }
    return name_wrap;
  }
};

template<>
struct ObGetOutlineKeyV3<ObOutlineSignatureHashWrapper, ObSimpleOutlineSchema *>
{
  ObOutlineSignatureHashWrapper operator()(const ObSimpleOutlineSchema *outline_schema) const
  {
    ObOutlineSignatureHashWrapper sql_wrap;
    if (!OB_ISNULL(outline_schema)) {
      sql_wrap.set_tenant_id(outline_schema->get_tenant_id());
      sql_wrap.set_database_id(outline_schema->get_database_id());
      sql_wrap.set_signature(outline_schema->get_signature_str());
    }
    return sql_wrap;
  }
};

template<>
struct ObGetOutlineKeyV3<ObOutlineSqlIdHashWrapper, ObSimpleOutlineSchema *>
{
  ObOutlineSqlIdHashWrapper operator()(const ObSimpleOutlineSchema *outline_schema) const
  {
    ObOutlineSqlIdHashWrapper sql_wrap;
    if (!OB_ISNULL(outline_schema)) {
      sql_wrap.set_tenant_id(outline_schema->get_tenant_id());
      sql_wrap.set_database_id(outline_schema->get_database_id());
      sql_wrap.set_sql_id(outline_schema->get_sql_id_str());
    }
    return sql_wrap;
  }
};



class ObOutlineMgr
{
  typedef common::ObSortedVector<ObSimpleOutlineSchema *> OutlineInfos;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleOutlineSchema *,
      ObGetOutlineKeyV3, 128> OutlineIdMap;
  typedef common::hash::ObPointerHashMap<ObOutlineNameHashWrapper, ObSimpleOutlineSchema *,
      ObGetOutlineKeyV3, 128> OutlineNameMap;
  typedef common::hash::ObPointerHashMap<ObOutlineSignatureHashWrapper, ObSimpleOutlineSchema *,
      ObGetOutlineKeyV3, 128> SignatureMap;
  typedef common::hash::ObPointerHashMap<ObOutlineSqlIdHashWrapper, ObSimpleOutlineSchema *,
      ObGetOutlineKeyV3, 128> SqlIdMap;
  typedef OutlineInfos::iterator OutlineIter;
  typedef OutlineInfos::const_iterator ConstOutlineIter;
public:
  ObOutlineMgr();
  explicit ObOutlineMgr(common::ObIAllocator &allocator);
  virtual ~ObOutlineMgr();
  int init();
  void reset();
  ObOutlineMgr &operator =(const ObOutlineMgr &other);
  int assign(const ObOutlineMgr &other);
  int deep_copy(const ObOutlineMgr &other);
  void dump() const;

  int add_outlines(const common::ObIArray<ObSimpleOutlineSchema> &outline_schemas);
  int del_outlines(const common::ObIArray<ObTenantOutlineId> &outlines);
  int add_outline(const ObSimpleOutlineSchema &outline_schema);
  int del_outline(const ObTenantOutlineId &outline);
  int get_outline_schema(const uint64_t outline_id,
                         const ObSimpleOutlineSchema *&outline_schema) const;
  int get_outline_schema_with_name(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &name,
                                   const ObSimpleOutlineSchema *&outline_schema) const;
  int get_outline_schema_with_signature(const uint64_t tenant_id,
                                        const uint64_t database_id,
                                        const common::ObString &signature,
                                        const ObSimpleOutlineSchema *&outline_schema) const;
  int get_outline_schema_with_sql_id(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const common::ObString &sql_id,
                                     const ObSimpleOutlineSchema *&outline_schema) const;
  int get_outline_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSimpleOutlineSchema *> &outline_schemas) const;
  int get_outline_schemas_in_database(const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObIArray<const ObSimpleOutlineSchema *> &outline_schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_outline_schema_count(int64_t &outline_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  inline bool check_inner_stat() const;
  inline static bool compare_outline(const ObSimpleOutlineSchema *lhs,
                                     const ObSimpleOutlineSchema *rhs);
  inline static bool equal_outline(const ObSimpleOutlineSchema *lhs,
                                   const ObSimpleOutlineSchema *rhs);
  inline static bool compare_with_tenant_outline_id(const ObSimpleOutlineSchema *lhs,
                                                    const ObTenantOutlineId &tenant_outline_id);
  inline static bool equal_with_tenant_outline_id(const ObSimpleOutlineSchema *lhs,
                                                  const ObTenantOutlineId &tenant_outline_id);
  int rebuild_outline_hashmap();
private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  OutlineInfos outline_infos_;
  OutlineIdMap outline_id_map_;
  OutlineNameMap outline_name_map_;
  SignatureMap signature_map_;
  SqlIdMap sql_id_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_OUTLINE_MGR_H_
