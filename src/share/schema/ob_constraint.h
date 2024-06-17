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

namespace oceanbase
{
namespace share
{
namespace schema
{
#define NOT_NULL_STR_EXTRA_SIZE     14
#define NOT_NULL_STR_HEADER_LENGTH  1
class ObTableSchema;
class ObConstraint : public ObSchema
{
  OB_UNIS_VERSION_V(1);
public:
  ObConstraint();
  explicit ObConstraint(common::ObIAllocator *allocator);
  ObConstraint(const ObConstraint &src_schema) = delete;
  virtual ~ObConstraint();

  //operators
  ObConstraint &operator=(const ObConstraint &src_schema) = delete;
  int assign(const ObConstraint &src_schema);
  // 不提供比较运算符，这个 schema 对象的成员变量数量越来越多，类型越来越复杂，比较运算很容易错
  // 有比较 schema 对象的需求时建议根据需求定制比较函数
  bool operator==(const ObConstraint &r) const = delete;
  bool operator!=(const ObConstraint &r) const = delete;

  //set methods
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_table_id(const uint64_t id) { table_id_ = id; }
  inline void set_constraint_id(const uint64_t id) { constraint_id_ = id; }
  inline void set_constraint_type(const ObConstraintType constraint_type) {
    constraint_type_ = constraint_type;
  }
  inline void set_schema_version(const int64_t schema_version) {
    schema_version_ = schema_version;
  }
  inline int set_constraint_name(const common::ObString &constraint_name) {
    return deep_copy_str(constraint_name, constraint_name_);
  }
  inline int set_check_expr(const common::ObString &check_expr) {
    return deep_copy_str(check_expr, check_expr_);
  }
  inline void set_rely_flag(const bool rely_flag) { rely_flag_ = rely_flag; }
  inline void set_enable_flag(const bool enable_flag) { enable_flag_ = enable_flag; }
  inline void set_validate_flag(const ObCstFkValidateFlag validate_flag) { validate_flag_ = validate_flag; }
  inline void set_is_modify_check_expr(const bool is_modify_check_expr) {
    is_modify_check_expr_ = is_modify_check_expr;
  }
  inline void set_is_modify_rely_flag(const bool is_modify_rely_flag) {
    is_modify_rely_flag_ = is_modify_rely_flag;
  }
  inline void set_is_modify_enable_flag(const bool is_modify_enable_flag) {
    is_modify_enable_flag_ = is_modify_enable_flag;
  }
  inline void set_is_modify_validate_flag(const bool is_modify_validate_flag) {
    is_modify_validate_flag_ = is_modify_validate_flag;
  }
  inline void set_need_validate_data(const bool need_validate_data) {
    need_validate_data_ = need_validate_data;
  }
  int assign_column_ids(const common::ObIArray<uint64_t> &column_ids_array);
  int assign_not_null_cst_column_id(const uint64_t column_id);
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline uint64_t get_constraint_id() const { return constraint_id_; }
  inline ObConstraintType get_constraint_type() const { return constraint_type_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_check_expr() const { return extract_str(check_expr_); }
  inline const common::ObString &get_check_expr_str() const { return check_expr_; }
  inline const char *get_constraint_name() const { return extract_str(constraint_name_); }
  inline const common::ObString &get_constraint_name_str() const { return constraint_name_; }
  inline bool get_rely_flag() const { return rely_flag_; }
  inline bool get_enable_flag() const { return enable_flag_; }
  inline ObCstFkValidateFlag get_validate_flag() const { return validate_flag_ ; }
  inline bool get_is_modify_check_expr() const { return is_modify_check_expr_; }
  inline bool get_is_modify_rely_flag() const { return is_modify_rely_flag_; }
  inline bool get_is_modify_enable_flag() const { return is_modify_enable_flag_; }
  inline bool get_is_modify_validate_flag() const { return is_modify_validate_flag_ ; }
  inline bool get_need_validate_data() const { return need_validate_data_ ; }
  inline bool is_no_validate() const { return CST_FK_NO_VALIDATE == validate_flag_; }
  inline bool is_validated() const { return CST_FK_VALIDATED == validate_flag_; }
  inline bool is_validating() const { return CST_FK_VALIDATING == validate_flag_; }
  // TODO:@shanting get not null column name from fixed position of check_expr_str is not a good idea.
  // It's used for "alter table add column not null", so wrap ObConstraint and add AlterConstraint is better.
  // alter_constraint_type_ can be moved to AlterConstraint from AlterTableArg, then we can take
  // more than one type of constraint operation in one ddl.
  int get_not_null_column_name(common::ObString &cst_col_name) const;
  inline int64_t get_column_cnt() const { return column_cnt_; }
  typedef const uint64_t *const_cst_col_iterator;
  inline const_cst_col_iterator cst_col_begin() const { return column_id_array_; }
  inline const_cst_col_iterator cst_col_end() const
  { return NULL == column_id_array_ ? NULL : &(column_id_array_[column_cnt_]); }
  inline void set_name_generated_type(const ObNameGeneratedType name_generated_type) {
    name_generated_type_ = name_generated_type;
  }
  inline ObNameGeneratedType get_name_generated_type() const { return name_generated_type_; }
  bool is_sys_generated_name(bool check_unknown) const;

  // other
  int64_t get_convert_size() const;
  bool is_match_partition_exchange_constraint_conditions(const ObConstraint &r) const;
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
  ObCstFkValidateFlag validate_flag_;
  bool is_modify_check_expr_;
  bool is_modify_rely_flag_;
  bool is_modify_enable_flag_;
  bool is_modify_validate_flag_;
  int64_t column_cnt_;
  uint64_t *column_id_array_;
  bool need_validate_data_;
  ObNameGeneratedType name_generated_type_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OCEANBASE_SCHEMA_CONSTRAINT_H_
