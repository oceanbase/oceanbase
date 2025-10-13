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

#ifndef OCEANBASE_SHARE_OB_SHARE_UTIL_H_
#define OCEANBASE_SHARE_OB_SHARE_UTIL_H_
#include "lib/utility/ob_sort.h" // for ob_sort
#include "common/ob_member_list.h" // for ObMemberList
#include "share/ob_define.h"
#include "share/scn.h"
#include "share/ob_tenant_role.h"
namespace oceanbase
{
namespace common
{
class ObTimeoutCtx;
class ObISQLClient;
}
namespace share
{
class ObUnit;
namespace schema
{
class ObTenantSchema;
}
// available range is [start_id, end_id]
class ObIDGenerator
{
public:
  ObIDGenerator()
    : inited_(false),
      step_(0),
      start_id_(common::OB_INVALID_ID),
      end_id_(common::OB_INVALID_ID),
      current_id_(common::OB_INVALID_ID)
  {}
  ObIDGenerator(const uint64_t step)
    : inited_(false),
      step_(step),
      start_id_(common::OB_INVALID_ID),
      end_id_(common::OB_INVALID_ID),
      current_id_(common::OB_INVALID_ID)
  {}

  virtual ~ObIDGenerator() {}
  void reset();

  int init(const uint64_t step,
           const uint64_t start_id,
           const uint64_t end_id);
  int next(uint64_t &current_id);

  int get_start_id(uint64_t &start_id) const;
  int get_current_id(uint64_t &current_id) const;
  int get_end_id(uint64_t &end_id) const;
  int get_id_cnt(uint64_t &cnt) const;
  TO_STRING_KV(K_(inited), K_(step), K_(start_id), K_(end_id), K_(current_id));
protected:
  bool inited_;
  uint64_t step_;
  uint64_t start_id_;
  uint64_t end_id_;
  uint64_t current_id_;
};

class ObShareUtil
{
public:
  // priority to set timeout_ctx: ctx > worker > default_timeout
  static int set_default_timeout_ctx(common::ObTimeoutCtx &ctx, const int64_t default_timeout);
  // priority to get timeout: ctx > worker > default_timeout
  static int get_abs_timeout(const int64_t default_timeout, int64_t &abs_timeout);
  static int get_ctx_timeout(const int64_t default_timeout, int64_t &timeout);
  // data version must up to 4.1 with arbitration service
  // params[in]  tenant_id, which tenant to check
  // params[out] is_compatible, whether it is up to 4.1
  static int check_compat_version_for_arbitration_service(
      const uint64_t tenant_id,
      bool &is_compatible);
  // generate the count of arb replica of a log stream
  // @params[in]  tenant_id, which tenant to check
  // @params[in]  ls_id, which log stream to check
  // @params[out] arb_replica_num, the result
  static int generate_arb_replica_num(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      int64_t &arb_replica_num);

  // data version must up to 4.2 with read only replica
  // @params[in]  tenant_id, which tenant to check
  // @params[out] is_compatible, whether it is up to 4.2
  static int check_compat_version_for_readonly_replica(
      const uint64_t tenant_id,
      bool &is_compatible);

  static int fetch_current_cluster_version(
             common::ObISQLClient &client,
             uint64_t &cluster_version);

  static int fetch_current_data_version(
             common::ObISQLClient &client,
             const uint64_t tenant_id,
             uint64_t &data_version);

  // parse GCONF.all_server_list
  // @params[in]  excluded_server_list, servers which will not be included in the output
  // @params[out] config_all_server_list, servers in (GCONF.all_server_list - excluded_server_list)
  static int parse_all_server_list(
    const ObArray<ObAddr> &excluded_server_list,
    ObArray<ObAddr> &config_all_server_list);
  // get ora_rowscn from one row
  // @params[in]: tenant_id, the table owner
  // @params[in]: sql, the sql should be "select ORA_ROWSCN from xxx", where count() is 1
  // @params[out]: the ORA_ROWSCN
  static int get_ora_rowscn(
    common::ObISQLClient &client,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    SCN &ora_rowscn);
  static bool is_tenant_enable_rebalance(const uint64_t tenant_id);
  static bool is_tenant_enable_transfer(const uint64_t tenant_id);
  static bool is_tenant_enable_ls_leader_balance(const uint64_t tenant_id);
  static int mtl_get_tenant_role(const uint64_t tenant_id, ObTenantRole::Role &tenant_role);
  static int mtl_check_if_tenant_role_is_primary(const uint64_t tenant_id, bool &is_primary);
  static int mtl_check_if_tenant_role_is_standby(const uint64_t tenant_id, bool &is_standby);
  static int table_get_tenant_role(const uint64_t tenant_id, ObTenantRole &tenant_role);
  static int table_check_if_tenant_role_is_primary(const uint64_t tenant_id, bool &is_primary);
  static int table_check_if_tenant_role_is_standby(const uint64_t tenant_id, bool &is_standby);
  static int table_check_if_tenant_role_is_restore(const uint64_t tenant_id, bool &is_restore);
  static int check_compat_version_for_logonly_replica(bool &is_compatible);
  static int check_replica_type_with_version(
             const common::ObReplicaType &replica_type,
             const bool &check_for_unit);
  static int check_replica_type_in_locality(const share::schema::ObTenantSchema &tenant_schema);
  static int check_unit_type_match_replica_type(
             const common::ObReplicaType &replica_type,
             const share::ObUnit &unit);
  static int get_full_replica_number(
         const common::ObMemberList &member_list,
         int64_t &full_replica_number);
  static bool is_valid_replica_type_for_unit(
         const common::ObReplicaType &replica_type);
private:
  static bool is_supported_replica_type_(
         const common::ObReplicaType &replica_type,
         const bool &check_for_unit);
};

template <typename T>
class ObMatrix
{
public:
  ObMatrix()
    : inner_array_(),
      row_count_(0),
      column_count_(0),
      is_inited_(false) {}
  virtual ~ObMatrix() {}
  int init(const int64_t row_count, const int64_t column_count);
public:
  int set(const int64_t row, const int64_t column, const T &element);
  int get(const int64_t row, const int64_t column, T &element) const;
  const T *get(const int64_t row, const int64_t column) const;
  T *get(const int64_t row, const int64_t column);
  int64_t get_row_count() const;
  int64_t get_column_count() const;
  int64_t get_element_count() const;
  const T *get(const int64_t element_index) const;
  T *get(const int64_t element_index);
  bool is_valid() const;
  bool is_contain(const T &element) const;
  template <typename Func>
  int sort_column_group(
      const int64_t start_column_idx,
      const int64_t column_count,
      Func &func);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  ObArray<T> inner_array_;
  int64_t row_count_;
  int64_t column_count_;
  bool is_inited_;
};

template <typename T>
int64_t ObMatrix<T>::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("matrix");
  J_COLON();
  J_KV(K_(row_count), K_(column_count), K_(is_inited));
  J_COMMA();
  for (int64_t row_index = 0; row_index < row_count_; ++row_index) {
    J_NEWLINE();
    J_ARRAY_START();
    bool need_append_comma = false;
    for (int64_t column_index = 0; column_index < column_count_; ++column_index) {
      T cell;
      // append comma if needed
      if (need_append_comma) {
        J_COMMA();
      } else {
        need_append_comma = true;
      }
      // print cell
      if (OB_FAIL(get(row_index, column_index, cell))) {
        SHARE_LOG(WARN, "fail to get cell", KR(ret), K(row_index), K(column_index), K(inner_array_));
      } else {
        J_KV(K(cell));
      }
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

template <typename T>
int ObMatrix<T>::init(
    const int64_t row_count,
    const int64_t column_count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "matrix init twice", K(ret));
  } else if (OB_UNLIKELY(row_count <= 0
                         || column_count <= 0
                         || row_count >= INT32_MAX
                         || column_count >= INT32_MAX)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "row or column count invalid", K(ret), K(row_count), K(column_count));
  } else {
    row_count_ = row_count;
    column_count_ = column_count;
    int64_t product = row_count_ * column_count_;
    inner_array_.reset();
    T pure_element;
    for (int64_t i = 0; OB_SUCC(ret) && i < product; ++i) {
      if (OB_FAIL(inner_array_.push_back(pure_element))) {
        SHARE_LOG(WARN, "fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
int ObMatrix<T>::set(
    const int64_t row,
    const int64_t column,
    const T &element)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "matrix not init", K(ret));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    if (OB_FAIL(copy_assign(inner_array_.at(index), element))) {
      SHARE_LOG(WARN, "fail to set element", KR(ret), K(element));
    }
  }
  return ret;
}

template <typename T>
int ObMatrix<T>::get(
    const int64_t row,
    const int64_t column,
    T &element) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "matrix not init", K(ret));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    if (OB_FAIL(copy_assign(element, inner_array_.at(index)))) {
      SHARE_LOG(WARN, "failt o assign element", KR(ret));
    }
  }
  return ret;
}

template <typename T>
const T *ObMatrix<T>::get(const int64_t row, const int64_t column) const
{
  const T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    SHARE_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    err = common::OB_INVALID_ARGUMENT;
    SHARE_LOG_RET(WARN, err, "invalid argument", K(err), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    ptr_ret = &inner_array_.at(index);
  }
  return ptr_ret;
}

template <typename T>
T *ObMatrix<T>::get(const int64_t row, const int64_t column)
{
  T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    SHARE_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    err = common::OB_INVALID_ARGUMENT;
    SHARE_LOG_RET(WARN, err, "invalid argument", K(err), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    ptr_ret = &inner_array_.at(index);
  }
  return ptr_ret;
}

template <typename T>
const T *ObMatrix<T>::get(const int64_t element_index) const
{
  const T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    SHARE_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(element_index < 0 || element_index >= inner_array_.count())) {
    err = common::OB_INVALID_ARGUMENT;
    SHARE_LOG_RET(WARN, err, "invalid argument", K(err), K(element_index), K(row_count_), K(column_count_));
  } else {
    ptr_ret = &inner_array_.at(element_index);
  }
  return ptr_ret;
}

template <typename T>
T *ObMatrix<T>::get(const int64_t element_index)
{
  T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    SHARE_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(element_index < 0 || element_index >= inner_array_.count())) {
    err = common::OB_INVALID_ARGUMENT;
    SHARE_LOG_RET(WARN, err, "invalid argument", K(err), K(element_index), K(row_count_), K(column_count_));
  } else {
    ptr_ret = &inner_array_.at(element_index);
  }
  return ptr_ret;
}
template <typename T>
int64_t ObMatrix<T>::get_row_count() const
{
  return row_count_;
}

template <typename T>
int64_t ObMatrix<T>::get_column_count() const
{
  return column_count_;
}

template <typename T>
int64_t ObMatrix<T>::get_element_count() const
{
  return inner_array_.count();
}

template <typename T>
bool ObMatrix<T>::is_valid() const
{
  return is_inited_
         && (row_count_ > 0 && row_count_ < INT32_MAX)
         && (column_count_ > 0 && column_count_ < INT32_MAX)
         && (inner_array_.count() == row_count_ * column_count_);
}

template <typename T>
bool ObMatrix<T>::is_contain(const T &element) const
{
  return common::has_exist_in_array(inner_array_, element);
}

// Column_count elements starting from start_column_idx in each row,
// Sort according to the rules specified by Func operator
template <typename T>
template <typename Func>
int ObMatrix<T>::sort_column_group(
    const int64_t start_column_idx,
    const int64_t column_count,
    Func &func)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(start_column_idx < 0
        || column_count <= 0
        || start_column_idx + column_count > column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret));
  } else {
    for (int64_t row = 0; OB_SUCC(ret) && row < row_count_; ++row) {
      const int64_t end_column_idx = start_column_idx + column_count;
      lib::ob_sort(inner_array_.begin() + row * column_count_ + start_column_idx,
                inner_array_.begin() + row * column_count_ + end_column_idx,
                func);
      if (OB_FAIL(func.get_ret())) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "sort error", K(ret));
      } else {} // good, go on to sort the next row
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
#endif //OCEANBASE_SHARE_OB_SHARE_UTIL_H_
