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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PACKAGE_INFO_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PACKAGE_INFO_H_

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
enum ObPackageType
{
  INVALID_PACKAGE_TYPE = 0,
  PACKAGE_TYPE = 1,
  PACKAGE_BODY_TYPE = 2,
};
enum ObPackageFlag
{
  PKG_FLAG_INVALID = 1,
  PKG_FLAG_NONEDITIONABLE = 2,
  PKG_FLAG_INVOKER_RIGHT = 4,
  PKG_FLAG_ACCESSIBLE_BY = 8,
};

#define COMPATIBLE_MODE_BIT     0x3
#define COMPATIBLE_ORACLE_MODE  0x0
#define COMPATIBLE_MYSQL_MODE   0x1

class ObPackageInfo: public ObSchema, public IObErrorInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPackageInfo() { reset(); }

  explicit ObPackageInfo(common::ObIAllocator *allocator)
  : ObSchema(allocator)
  {
    reset();
  }

  DISABLE_COPY_ASSIGN(ObPackageInfo);

  virtual ~ObPackageInfo() {}
  int assign(const ObPackageInfo &other);
  bool is_valid() const;
  void reset();
  int64_t get_convert_size() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_database_id() const { return database_id_; }
  void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  uint64_t get_package_id() const { return package_id_; }
  uint64_t get_object_id() const { return package_id_; }
  ObObjectType get_object_type() const { return is_package() ? ObObjectType::PACKAGE : ObObjectType::PACKAGE_BODY; }
  void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  const common::ObString &get_package_name() const { return package_name_; }
  int set_package_name(const common::ObString &package_name) { return deep_copy_str(package_name, package_name_); }
  void assign_package_name(const common::ObString &package_name) { package_name_ = package_name; }
  void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  int64_t get_schema_version() const { return schema_version_; }
  ObPackageType get_type() const { return type_; }
  void set_type(ObPackageType type) { type_ = type; }
  inline bool is_package() const { return type_ ==  PACKAGE_TYPE; }
  inline bool is_package_body() const { return type_ == PACKAGE_BODY_TYPE; }
  int64_t get_flag() const { return flag_; }
  void set_flag(int64_t flag) { flag_ = flag; }
  uint64_t get_owner_id() const { return owner_id_; }
  void set_owner_id(int64_t owner_id) { owner_id_ = owner_id; }
  int64_t get_comp_flag() const { return comp_flag_; }
  void set_comp_flag(int64_t comp_flag) { comp_flag_ = comp_flag; }
  void set_compatibility_mode(const common::ObCompatibilityMode compa_mode)
  {
    /* 当前comp_flag列默认是0, 为了保持一致, 这里根据模式做反转操作, 保证mysql模式comp_flag&0x3为1, oracle模式comp_flag&0x3为0*/
    if(common::MYSQL_MODE == compa_mode) {
      comp_flag_ |= COMPATIBLE_MYSQL_MODE;
    } else if (common::ORACLE_MODE == compa_mode) {
      comp_flag_ |= COMPATIBLE_ORACLE_MODE;
    } else {
      /*do nothing*/
    }
  }
  int64_t get_compatibility_mode() const
  {
    return comp_flag_ & COMPATIBLE_MODE_BIT;
  }
  const common::ObString &get_source() const { return source_; }
  int set_source(const common::ObString &source) { return deep_copy_str(source, source_); }
  void assign_source(const common::ObString &source) { source_ = source; }
  OB_INLINE int set_exec_env(const common::ObString &exec_env) { return deep_copy_str(exec_env, exec_env_); }
  OB_INLINE int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  OB_INLINE const common::ObString &get_route_sql() const { return route_sql_; }
  OB_INLINE int set_route_sql(const common::ObString &route_sql) { return deep_copy_str(route_sql, route_sql_); }
  OB_INLINE const common::ObString &get_exec_env() const { return exec_env_; }
  OB_INLINE const common::ObString &get_comment() const { return comment_; }
  bool is_for_trigger() const;

  OB_INLINE void set_pkg_invalid() { flag_ |= PKG_FLAG_INVALID; }
  OB_INLINE void set_noneditionable() { flag_ |= PKG_FLAG_NONEDITIONABLE; }
  OB_INLINE void set_invoker_right() { flag_ |= PKG_FLAG_INVOKER_RIGHT; }
  OB_INLINE void set_accessible_by_clause() { flag_ |= PKG_FLAG_ACCESSIBLE_BY; }

  OB_INLINE bool is_pkg_invalid() { return PKG_FLAG_INVALID == (flag_ & PKG_FLAG_INVALID); }
  OB_INLINE bool is_noneditionable() const
  {
    return PKG_FLAG_NONEDITIONABLE == (flag_ & PKG_FLAG_NONEDITIONABLE);
  }
  OB_INLINE bool is_invoker_right() const
  {
    return PKG_FLAG_INVOKER_RIGHT == (flag_ & PKG_FLAG_INVOKER_RIGHT);
  }
  OB_INLINE bool has_accessible_by_clause() const
  {
    return PKG_FLAG_ACCESSIBLE_BY == (flag_ & PKG_FLAG_ACCESSIBLE_BY);
  }

  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(owner_id),
               K_(package_id),
               K_(package_name),
               K_(schema_version),
               K_(type),
               K_(flag),
               K_(comp_flag),
               K_(exec_env),
               K_(source),
               K_(comment),
               K_(route_sql));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t owner_id_;
  uint64_t package_id_;
  common::ObString package_name_;
  int64_t schema_version_;
  ObPackageType type_;
  int64_t flag_;
  int64_t comp_flag_; /* bit0~1: 00->oracle mode, 01->mysql mode, reserve 10, 11 */
  common::ObString exec_env_;
  common::ObString source_;
  common::ObString comment_;
  common::ObString route_sql_;
};

}  //schema
}  //share
}  //oceanbase
#endif /* OCEANBASE_SHARE_SCHEMA_OB_PACKAGE_INFO_H_ */
