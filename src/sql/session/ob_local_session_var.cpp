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

#define USING_LOG_PREFIX SQL_SESSION

#include "ob_local_session_var.h"
#include "sql/session/ob_basic_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

template<class T>
int ObLocalSessionVar::set_local_vars(T &var_array)
{
  int ret = OB_SUCCESS;
  if (!local_session_vars_.empty()) {
    local_session_vars_.reset();
  }
  if (OB_FAIL(local_session_vars_.reserve(var_array.count()))) {
    LOG_WARN("fail to reserve for local_session_vars", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < var_array.count(); ++i) {
      if (OB_FAIL(add_local_var(var_array.at(i)))) {
        LOG_WARN("fail to add session var", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::add_local_var(const ObSessionSysVar *var)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(var));
  } else if (OB_FAIL(add_local_var(var->type_, var->val_))) {
    LOG_WARN("fail to add local session var", K(ret));
  }
  return ret;
}

int ObLocalSessionVar::add_local_var(ObSysVarClassType var_type, const ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *cur_var = NULL;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(alloc_));
  } else if (OB_FAIL(get_local_var(var_type, cur_var))) {
    LOG_WARN("get local var failed", K(ret));
  } else if (NULL == cur_var) {
    ObSessionSysVar *new_var = OB_NEWx(ObSessionSysVar, alloc_);
    if (OB_ISNULL(new_var)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new var failed.", K(ret));
    } else if (OB_FAIL(local_session_vars_.push_back(new_var))) {
      LOG_WARN("push back new var failed", K(ret));
    } else if (OB_FAIL(deep_copy_obj(*alloc_, value, new_var->val_))) {
      LOG_WARN("fail to deep copy obj", K(ret));
    } else {
      new_var->type_ = var_type;
    }
  } else if (!cur_var->is_equal(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local session var added before is not equal to the new var", K(ret), KPC(cur_var), K(value));
  }
  return ret;
}

int ObLocalSessionVar::get_local_var(ObSysVarClassType var_type, ObSessionSysVar *&sys_var) const
{
  int ret = OB_SUCCESS;
  sys_var = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == sys_var && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(local_session_vars_));
    } else if (local_session_vars_.at(i)->type_ == var_type) {
      sys_var = local_session_vars_.at(i);
    }
  }
  return ret;
}

int ObLocalSessionVar::remove_local_var(ObSysVarClassType var_type)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(local_session_vars_));
    } else if (local_session_vars_.at(i)->type_ == var_type) {
      local_session_vars_.at(i) = local_session_vars_.at(local_session_vars_.count() - 1);
      find = true;
    }
  }
  if (OB_SUCC(ret) && find) {
    local_session_vars_.pop_back();
  }
  return ret;
}

int ObLocalSessionVar::get_local_vars(ObIArray<const ObSessionSysVar *> &var_array) const
{
  int ret = OB_SUCCESS;
  var_array.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i){
    if (OB_FAIL(var_array.push_back(local_session_vars_.at(i)))) {
      LOG_WARN("push back local session vars failed", K(ret));
    }
  }
  return ret;
}

int ObLocalSessionVar::remove_vars_same_with_session(const sql::ObBasicSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(session));
  } else {
    bool is_same = false;
    ObSEArray<ObSessionSysVar *, 4> new_var_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
      if (OB_FAIL(check_var_same_with_session(*session, local_session_vars_.at(i), is_same))) {
        LOG_WARN("fail to check var same with session", K(ret));
      } else if (is_same) {
        /* do nothing */
      } else if (OB_FAIL(new_var_array.push_back(local_session_vars_.at(i)))) {
        LOG_WARN("fail to push into new var array", K(ret));
      }
    }
    if (OB_SUCC(ret) && new_var_array.count() != local_session_vars_.count()) {
      if (OB_FAIL(local_session_vars_.assign(new_var_array))) {
        LOG_WARN("fail to set local session vars.", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::get_different_vars_from_session(const sql::ObBasicSessionInfo *session,
                                                       ObIArray<const ObSessionSysVar*> &local_diff_vars,
                                                       ObIArray<ObObj> &session_vals) const
{
  int ret = OB_SUCCESS;
  local_diff_vars.reuse();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(session));
  } else {
    bool is_same = false;
    ObObj session_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
      if (OB_ISNULL(local_session_vars_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i));
      } else if (SYS_VAR_SQL_MODE == local_session_vars_.at(i)->type_) {
        /* just ignore sql mode now */
      } else if (OB_FAIL(check_var_same_with_session(*session, local_session_vars_.at(i), is_same, &session_val))) {
        LOG_WARN("fail to check var same with session", K(ret));
      } else if (is_same) {
        /* do nothing */
      } else if (OB_FAIL(local_diff_vars.push_back(local_session_vars_.at(i)))) {
        LOG_WARN("fail to push back sys var", K(ret));
      } else if (OB_FAIL(session_vals.push_back(session_val))) {
        LOG_WARN("fail to push back obj", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::check_var_same_with_session(const sql::ObBasicSessionInfo &session,
                                                   const ObSessionSysVar *local_var,
                                                   bool &is_same,
                                                   ObObj *diff_val /* default null */ ) const
{
  int ret = OB_SUCCESS;
  is_same = false;
  ObObj session_val;
  if (OB_ISNULL(local_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(local_var));
  } else if (SYS_VAR_SQL_MODE == local_var->type_) {
    is_same = local_var->val_.get_uint64() == session.get_sql_mode();
    if (!is_same && NULL != diff_val) {
      diff_val->set_uint64(session.get_sql_mode());
    }
  } else if (OB_FAIL(session.get_sys_variable(local_var->type_, session_val))) {
    LOG_WARN("fail to get session variable", K(ret));
  } else {
    is_same = local_var->is_equal(session_val);
    if (!is_same && NULL != diff_val) {
      *diff_val = session_val;
    }
  }
  return ret;
}

int ObLocalSessionVar::gen_local_session_var_str(ObIAllocator &allocator,
                                                 ObString &local_session_var_str) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t buf_len = get_serialize_size();
  char *binary_str = NULL;
  char *hex_str = NULL;
  int64_t hex_pos = 0;
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  if (OB_ISNULL(binary_str = static_cast<char *>(tmp_allocator.alloc(buf_len)))
      || OB_ISNULL(hex_str = static_cast<char *>(allocator.alloc(buf_len * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for local_session_var failed", K(ret), KP(binary_str), KP(hex_str));
  } else if (OB_FAIL(serialize_(binary_str, buf_len, pos))) {
    LOG_WARN("fail to serialize local_session_var", K(ret));
  } else if (OB_FAIL(common::hex_print(binary_str, pos, hex_str, buf_len * 2, hex_pos))) {
    LOG_WARN("print hex string failed", K(ret));
  } else {
    local_session_var_str.assign(hex_str, hex_pos);
  }
  return ret;
}

int ObLocalSessionVar::fill_local_session_var_from_str(const ObString &local_session_var_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  char *value_buf = NULL;
  ObLength len = 0;
  int64_t pos = 0;
  if (OB_UNLIKELY(local_session_var_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty str", K(ret), K(local_session_var_str));
  } else if (OB_ISNULL(value_buf = static_cast<char*>(tmp_allocator.alloc(local_session_var_str.length())))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(WARN, "fail to alloc memory", K(ret));
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FALSE_IT(len = common::str_to_hex(local_session_var_str.ptr(), local_session_var_str.length(),
                                                  value_buf, local_session_var_str.length()))) {
  } else if (OB_FAIL(deserialize_(value_buf, static_cast<int64_t>(len), pos))) {
    LOG_WARN("fail to deserialize local_session_var", K(ret));
  }
  return ret;
}

int ObLocalSessionVar::deep_copy(const ObLocalSessionVar &other)
{
  int ret = OB_SUCCESS;
  local_session_vars_.reset();
  if (this == &other) {
    //do nothing
  } else if (NULL != other.alloc_) {
    if (NULL == alloc_) {
      alloc_ = other.alloc_;
      local_session_vars_.set_allocator(other.alloc_);
    }
  }
  if (OB_FAIL(set_local_vars(other.local_session_vars_))) {
    LOG_WARN("fail to add session var", K(ret));
  }
  return ret;
}

int ObLocalSessionVar::deep_copy_self()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator", K(ret));
  } else {
    ObSEArray<const ObSessionSysVar*, 4> var_array;
    if (OB_FAIL(get_local_vars(var_array))) {
      LOG_WARN("get local vars failed", K(ret));
    } else if (OB_FAIL(set_local_vars(var_array))) {
      LOG_WARN("set local vars failed", K(ret));
    }
  }
  return ret;
}

int ObLocalSessionVar::assign(const ObLocalSessionVar &other)
{
  int ret = OB_SUCCESS;
  local_session_vars_.reset();
  if (NULL != other.alloc_) {
    if (NULL == alloc_) {
      alloc_ = other.alloc_;
      local_session_vars_.set_allocator(other.alloc_);
    }
    if (OB_FAIL(local_session_vars_.reserve(other.local_session_vars_.count()))) {
      LOG_WARN("reserve failed", K(ret));
    } else if (OB_FAIL(local_session_vars_.assign(other.local_session_vars_))) {
      LOG_WARN("fail to push back local var", K(ret));
    }
  } else {
    //do nothing, other is not inited
  }
  return ret;
}

void ObLocalSessionVar::reset()
{
  local_session_vars_.reset();
}

int ObLocalSessionVar::set_local_var_capacity(int64_t sz)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(local_session_vars_.reserve(sz))) {
    LOG_WARN("reserve failed", K(ret), K(sz));
  }
  return ret;
}

bool ObLocalSessionVar::operator == (const ObLocalSessionVar& other) const
{
  bool is_equal = local_session_vars_.count() == other.local_session_vars_.count();
  if (is_equal) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; is_equal && i < local_session_vars_.count(); ++i) {
      ObSessionSysVar *var = local_session_vars_.at(i);
      ObSessionSysVar *other_val = NULL; ;
      if (OB_ISNULL(var)) {
       is_equal = false;
      } else if ((tmp_ret = other.get_local_var(var->type_, other_val)) != OB_SUCCESS) {
        is_equal = false;
      } else if (other_val == NULL) {
        is_equal = false;
      } else {
        is_equal = var->is_equal(other_val->val_);
      }
    }
  }
  return is_equal;
}

int64_t ObLocalSessionVar::get_deep_copy_size() const
{
  int64_t sz = sizeof(*this) + local_session_vars_.count() * sizeof(ObSessionSysVar *);
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      sz += local_session_vars_.at(i)->get_deep_copy_size();
    }
  }
  return sz;
}

bool ObSessionSysVar::is_equal(const ObObj &other) const
{
  bool bool_ret = false;
  if (val_.get_meta() != other.get_meta()) {
    bool_ret = false;
    if (ob_is_string_type(val_.get_type())
        && val_.get_type() == other.get_type()
        && val_.get_collation_type() != other.get_collation_type()) {
      //the collation type of string system variables will be set to the current connection collation type after updating values.
      //return true if the string values are equal.
      bool_ret = common::ObCharset::case_sensitive_equal(val_.get_string(), other.get_string());
    }
  } else if (val_.is_equal(other, CS_TYPE_BINARY)) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObSessionSysVar::get_sys_var_val_str(const ObSysVarClassType var_type,
                                         const ObObj &var_val,
                                         ObIAllocator &allocator,
                                         ObString &val_str)
{
  int ret = OB_SUCCESS;
  val_str.reset();
  char *buffer = NULL;
  int64_t length = 0;
  int64_t pos = 0;
  if (SYS_VAR_SQL_MODE == var_type) {
    ObObj res_obj;
    if (OB_FAIL(ob_sql_mode_to_str(var_val, res_obj, &allocator))) {
      LOG_WARN("fail to convert sql mode to str", K(ret), K(var_val));
    } else if (OB_FAIL(res_obj.get_string(val_str))) {
      LOG_WARN("fail to get string form obj", K(ret), K(res_obj));
    }
  } else if (OB_FAIL(var_val.print_sql_literal(buffer, length, pos, allocator))) {
    LOG_WARN("print value failed", K(ret));
  } else {
    val_str.assign(buffer, pos);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObSessionSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, type_, val_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSessionSysVar)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type_, val_);
  return len;
}

OB_DEF_DESERIALIZE(ObSessionSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type_, val_);
  return ret;
}

int64_t ObSessionSysVar::get_deep_copy_size() const {
  int64_t sz = sizeof(*this) + val_.get_deep_copy_size();
  return sz;
}

const ObSysVarClassType ObLocalSessionVar::ALL_LOCAL_VARS[] = {
  SYS_VAR_TIME_ZONE,
  SYS_VAR_SQL_MODE,
  SYS_VAR_NLS_DATE_FORMAT,
  SYS_VAR_NLS_TIMESTAMP_FORMAT,
  SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
  SYS_VAR_COLLATION_CONNECTION,
  SYS_VAR_MAX_ALLOWED_PACKET
};

//add all vars that can be solidified
int ObLocalSessionVar::load_session_vars(const sql::ObBasicSessionInfo *session) {
  int ret = OB_SUCCESS;
  int64_t var_num = sizeof(ALL_LOCAL_VARS) / sizeof(ObSysVarClassType);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (!local_session_vars_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local_session_vars can only be inited once", K(ret));
  } else if (OB_FAIL(local_session_vars_.reserve(var_num))) {
    LOG_WARN("reserve failed", K(ret), K(var_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < var_num; ++i) {
      ObObj var;
      if (OB_FAIL(session->get_sys_variable(ALL_LOCAL_VARS[i], var))) {
        LOG_WARN("fail to get session variable", K(ret));
      } else if (OB_FAIL(add_local_var(ALL_LOCAL_VARS[i], var))) {
        LOG_WARN("fail to add session var", K(ret), K(var));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::reserve_max_local_vars_capacity() {
  int ret = OB_SUCCESS;
  int64_t var_num = sizeof(ALL_LOCAL_VARS) / sizeof(ObSysVarClassType);
  if (!local_session_vars_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local_session_vars can only be inited once", K(ret));
  } else if (OB_FAIL(local_session_vars_.reserve(var_num))) {
    LOG_WARN("reserve failed", K(ret), K(var_num));
  }
  return ret;
}

int ObLocalSessionVar::update_session_vars_with_local(sql::ObBasicSessionInfo &session) const {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(session.update_sys_variable(local_session_vars_.at(i)->type_, local_session_vars_.at(i)->val_))) {
      LOG_WARN("fail to update sys variable", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObLocalSessionVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, local_session_vars_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE, *local_session_vars_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLocalSessionVar)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, local_session_vars_.count());
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, *local_session_vars_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLocalSessionVar)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  OB_UNIS_DECODE(cnt);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(local_session_vars_.reserve(cnt))) {
      LOG_WARN("reserve failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    ObSessionSysVar var;
    LST_DO_CODE(OB_UNIS_DECODE, var);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_local_var(&var))) {
        LOG_WARN("fail to add local session var", K(ret));
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObLocalSessionVar)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (i > 0) {
      J_COMMA();
    }
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      J_KV("type", local_session_vars_.at(i)->type_,
            "val", local_session_vars_.at(i)->val_);
    }
  }
  J_OBJ_END();
  return pos;
}

}//end of namespace sql
}//end of namespace oceanbase
