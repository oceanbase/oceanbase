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

#ifndef OCEANBASE_SCHEMA_CONSTRAINT_H_
#define OCEANBASE_SCHEMA_CONSTRAINT_H_
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashutils.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
namespace schema {

class ObTableSchema;
class ObConstraint : public ObSchema {
  OB_UNIS_VERSION_V(1);

public:
  ObConstraint();
  explicit ObConstraint(common::ObIAllocator* allocator);
  ObConstraint(const ObConstraint& src_schema) = delete;
  virtual ~ObConstraint();

  // operators
  ObConstraint& operator=(const ObConstraint& src_schema) = delete;
  int assign(const ObConstraint& src_schema);
  // Does not provide comparison operators, the number of member variables of this schema object is increasing,
  // the types are becoming more and more complex, and comparison operations are easy to make mistakes
  // When there is a need to compare schema objects,
  // it is recommended to customize the comparison function according to the needs
  bool operator==(const ObConstraint& r) const = delete;
  bool operator!=(const ObConstraint& r) const = delete;

  // set methods
  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_table_id(const uint64_t id)
  {
    table_id_ = id;
  }
  inline void set_constraint_id(const uint64_t id)
  {
    constraint_id_ = id;
  }
  inline void set_constraint_type(const ObConstraintType constraint_type)
  {
    constraint_type_ = constraint_type;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int set_constraint_name(const common::ObString& constraint_name)
  {
    return deep_copy_str(constraint_name, constraint_name_);
  }
  inline int set_check_expr(const common::ObString& check_expr)
  {
    return deep_copy_str(check_expr, check_expr_);
  }
  inline void set_rely_flag(const bool rely_flag)
  {
    rely_flag_ = rely_flag;
  }
  inline void set_enable_flag(const bool enable_flag)
  {
    enable_flag_ = enable_flag;
  }
  inline void set_validate_flag(const bool validate_flag)
  {
    validate_flag_ = validate_flag;
  }
  inline void set_is_modify_check_expr(const bool is_modify_check_expr)
  {
    is_modify_check_expr_ = is_modify_check_expr;
  }
  inline void set_is_modify_rely_flag(const bool is_modify_rely_flag)
  {
    is_modify_rely_flag_ = is_modify_rely_flag;
  }
  inline void set_is_modify_enable_flag(const bool is_modify_enable_flag)
  {
    is_modify_enable_flag_ = is_modify_enable_flag;
  }
  inline void set_is_modify_validate_flag(const bool is_modify_validate_flag)
  {
    is_modify_validate_flag_ = is_modify_validate_flag;
  }
  int assign_column_ids(const common::ObIArray<uint64_t>& column_ids_array);

  // get methods
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline uint64_t get_constraint_id() const
  {
    return constraint_id_;
  }
  inline ObConstraintType get_constraint_type() const
  {
    return constraint_type_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const char* get_check_expr() const
  {
    return extract_str(check_expr_);
  }
  inline const common::ObString& get_check_expr_str() const
  {
    return check_expr_;
  }
  inline const char* get_constraint_name() const
  {
    return extract_str(constraint_name_);
  }
  inline const common::ObString& get_constraint_name_str() const
  {
    return constraint_name_;
  }
  inline bool get_rely_flag() const
  {
    return rely_flag_;
  }
  inline bool get_enable_flag() const
  {
    return enable_flag_;
  }
  inline bool get_validate_flag() const
  {
    return validate_flag_;
  }
  inline bool get_is_modify_check_expr() const
  {
    return is_modify_check_expr_;
  }
  inline bool get_is_modify_rely_flag() const
  {
    return is_modify_rely_flag_;
  }
  inline bool get_is_modify_enable_flag() const
  {
    return is_modify_enable_flag_;
  }
  inline bool get_is_modify_validate_flag() const
  {
    return is_modify_validate_flag_;
  }
  inline int64_t get_column_cnt() const
  {
    return column_cnt_;
  }
  typedef const uint64_t* const_cst_col_iterator;
  inline const_cst_col_iterator cst_col_begin() const
  {
    return column_id_array_;
  }
  inline const_cst_col_iterator cst_col_end() const
  {
    return NULL == column_id_array_ ? NULL : &(column_id_array_[column_cnt_]);
  }

  // other
  int64_t get_convert_size() const;
  void reset();

  DECLARE_VIRTUAL_TO_STRING;

private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t constraint_id_;
  int64_t schema_version_;
  common::ObString constraint_name_;
  common::ObString check_expr_;
  ObConstraintType constraint_type_;
  bool rely_flag_;
  bool enable_flag_;
  bool validate_flag_;
  bool is_modify_check_expr_;
  bool is_modify_rely_flag_;
  bool is_modify_enable_flag_;
  bool is_modify_validate_flag_;
  int64_t column_cnt_;
  uint64_t* column_id_array_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OCEANBASE_SCHEMA_CONSTRAINT_H_
