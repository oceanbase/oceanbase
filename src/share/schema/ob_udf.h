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

#ifndef _OB_UDF_H
#define _OB_UDF_H 1
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashutils.h"
#include "ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObTableSchema;
//TODO change this name to ObUDFInfo
class ObUDF : public ObSchema
{
  OB_UNIS_VERSION_V(1);

public:
  enum UDFType {
    UDF_TYPE_UNINITIAL,
    FUNCTION,
    AGGREGATE
  };

  enum UDFRetType {
    UDF_RET_UNINITIAL,
    STRING,
    INTEGER,
    REAL,
    DECIMAL,
    ROW,
  };

public:
  ObUDF() : ObSchema(), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), name_(), ret_(UDFRetType::UDF_RET_UNINITIAL),
  dl_(), type_(UDFType::UDF_TYPE_UNINITIAL), schema_version_(common::OB_INVALID_VERSION)
  { reset(); };
  explicit ObUDF(common::ObIAllocator *allocator);
  ObUDF(const ObUDF &src_schema);
  virtual ~ObUDF();

  //operators
  ObUDF &operator=(const ObUDF &src_schema);
  bool operator==(const ObUDF &r) const;
  bool operator!=(const ObUDF &r) const;

  //set methods
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_ret(const enum UDFRetType ret) { ret_ = ret; }
  inline void set_type(const enum UDFType type) { type_ = type; }
  inline void set_ret(const int ret) { ret_ = UDFRetType(ret); }
  inline void set_type(const int type) { type_ = UDFType(type); }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline int set_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  inline int set_dl(const common::ObString &dl) { return deep_copy_str(dl, dl_); }
  inline void set_udf_id(uint64_t id) { udf_id_ = id; }


  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_udf_id() const { return udf_id_; }
  inline enum UDFRetType get_ret() const { return ret_; }
  inline enum UDFType get_type() const { return type_; }
  inline int64_t get_schema_version() const { return schema_version_; }


  inline const char *get_dl() const { return extract_str(dl_); }
  inline const common::ObString &get_dl_str() const { return dl_; }
  inline const char *get_name() const { return extract_str(name_); }
  inline const common::ObString &get_name_str() const { return name_; }
  //only for retrieve udf
  inline const char *get_udf_name() const { return extract_str(name_); }
  inline common::ObString get_udf_name_str() const { return name_; }

  // other
  int64_t get_convert_size() const;
  virtual void reset() override;
  bool is_normal_udf() const { return type_ == FUNCTION; };
  bool is_agg_udf() const { return type_ == AGGREGATE; };

  TO_STRING_KV(K_(tenant_id),
               K_(udf_id),
               K_(name),
               K_(ret),
               K_(dl),
               K_(type),
               K_(schema_version));

private:
  uint64_t tenant_id_;
  uint64_t udf_id_;
  common::ObString name_;
  enum UDFRetType ret_;
  common::ObString dl_;
  enum UDFType type_;
  int64_t schema_version_; //the last modify timestamp of this version
};


/////////////////////////////////////////////

class ObUDFMeta
{
  OB_UNIS_VERSION_V(1);
public :
  ObUDFMeta() : tenant_id_(common::OB_INVALID_ID), name_(), ret_(ObUDF::UDFRetType::UDF_RET_UNINITIAL),
      dl_(), type_(ObUDF::UDFType::UDF_TYPE_UNINITIAL) {
  }
  virtual ~ObUDFMeta() = default;

  void assign(const ObUDFMeta &other) {
    tenant_id_ = other.tenant_id_;
    name_ = other.name_;
    ret_ = other.ret_;
    dl_ = other.dl_;
    type_ = other.type_;
  }

  ObUDFMeta &operator=(const class ObUDFMeta &other) {
    tenant_id_ = other.tenant_id_;
    name_ = other.name_;
    ret_ = other.ret_;
    dl_ = other.dl_;
    type_ = other.type_;
    return *this;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(name),
               K_(ret),
               K_(dl),
               K_(type));

  uint64_t tenant_id_;
  common::ObString name_;
  ObUDF::UDFRetType ret_;
  common::ObString dl_;
  ObUDF::UDFType type_;
};

}
}
}

#endif /* _OB_UDF_H */


