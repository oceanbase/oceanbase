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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_sql.h"
#include "pl/ob_pl_exception_handling.h"
#include "pl/ob_pl_package.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/ob_parser.h"
#include "sql/ob_spi.h"
#include "sql/engine/expr/ob_expr_pl_associative_index.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
using namespace sql;

namespace pl
{

int ObDbmsInfo::init()
{
  int ret = OB_SUCCESS;
  if (!define_columns_.created() &&
             OB_FAIL(define_columns_.create(common::hash::cal_next_prime(32),
                                           ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    SQL_ENG_LOG(WARN, "create sequence current value map failed", K(ret));
  } else if (!define_arrays_.created() &&
      OB_FAIL(define_arrays_.create(common::hash::cal_next_prime(32),
                                    ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    SQL_ENG_LOG(WARN, "create sequence current value map failed", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObDbmsInfo::reset()
{
  for (int64_t i = 0; i < bind_params_.count(); ++i) {
    (void)ObUserDefinedType::destruct_obj(bind_params_.at(i).param_value_);
  }
  param_names_.reset();
  into_names_.reset();
  bind_params_.reset();
  exec_params_.reset();
  sql_stmt_.reset();
  define_columns_.reuse();
  fields_.reset();
  if (nullptr != entity_) {
    DESTROY_CONTEXT(entity_);
    entity_ = nullptr;
  }
}


int ObDbmsInfo::deep_copy_field_columns(ObIAllocator& allocator,
                                        const common::ColumnsFieldIArray* src_fields,
                                        common::ColumnsFieldArray &dst_fields)
{
  int ret = OB_SUCCESS;
  dst_fields.reset();
  dst_fields.set_allocator(&allocator);
  if (OB_ISNULL(src_fields)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't copy null fields", K(ret));
  } else if (src_fields->count() < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src fields is null.", K(ret), K(src_fields->count()));
  } else if (0 == src_fields->count() ) {
    // do nothing
    // SELECT * INTO OUTFILE return null field
  } else if (OB_FAIL(dst_fields.reserve(src_fields->count()))) {
    LOG_WARN("fail to reserve column fields",
             K(ret), K(dst_fields.count()), K(src_fields->count()));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < src_fields->count(); ++i) {
      ObField tmp_field;
      if (OB_FAIL(tmp_field.deep_copy(src_fields->at(i), &allocator))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(dst_fields.push_back(tmp_field))) {
        LOG_WARN("push back field param failed", K(ret));
      } else { }
    }
  }
  return ret;
}

int ObDbmsInfo::deep_copy_field_columns(ObIAllocator& allocator,
                                        const common::ColumnsFieldArray src_fields,
                                        common::ColumnsFieldArray &dst_fields)
{
  int ret = OB_SUCCESS;
  dst_fields.reset();
  dst_fields.set_allocator(&allocator);
  if (src_fields.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src fields is null.", K(ret), K(src_fields.count()));
  } else if (OB_FAIL(dst_fields.reserve(src_fields.count()))) {
    LOG_WARN("fail to reserve column fields",
             K(ret), K(dst_fields.count()), K(src_fields.count()));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < src_fields.count(); ++i) {
      ObField tmp_field;
      if (OB_FAIL(tmp_field.deep_copy(src_fields.at(i), &allocator))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(dst_fields.push_back(tmp_field))) {
        LOG_WARN("push back field param failed", K(ret));
      } else { }
    }
  }
  return ret;
}

int ObDbmsInfo::init_params(int64_t param_count)
{
  int ret = OB_SUCCESS;
  ObIAllocator *alloc = NULL;
  OV (OB_NOT_NULL(entity_), OB_NOT_INIT, ps_sql_, param_count);
  OX (alloc = &entity_->get_arena_allocator());
  CK (OB_NOT_NULL(alloc));
  OX (param_names_.reset());
  OX (param_names_.set_allocator(alloc));
  OZ (param_names_.init(param_count), ps_sql_, param_count);
  OX (bind_params_.reset());
  OX (bind_params_.set_allocator(alloc));
  OZ (bind_params_.init(param_count), ps_sql_, param_count);
  OX (exec_params_.~Ob2DArray());
  OX (new (&exec_params_) ParamStore(ObWrapperAllocator(alloc)));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObDbmsInfo::expand_params()
{
  int ret = OB_SUCCESS;
  const ObObjParam *param_value = NULL;
  if (!exec_params_.empty()) {
    OX (exec_params_.reset());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_names_.count(); i++) {
    OX (param_value = get_bind_param(param_names_.at(i)));
    OV (OB_NOT_NULL(param_value), OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND, param_names_.at(i));
    // bind_params_数组中的param_value_已经做过深拷贝，这里不需要再做。
    OZ (exec_params_.push_back(*param_value), i, param_names_.at(i), *param_value);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < into_names_.count(); i++) {
    OX (param_value = get_bind_param(into_names_.at(i)));
    OV (OB_NOT_NULL(param_value), OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND, into_names_.at(i));
  }
  // oracle can reuse bind_params
  // OX (bind_params_.clear());
  return ret;
}
#endif

const ObObjParam *ObDbmsInfo::get_bind_param(const ObString &param_name) const
{
  const ObObjParam *param_value = NULL;
  for (int64_t i = 0; param_value == NULL && i < bind_params_.count(); i++) {
    if (0 == param_name.case_compare(bind_params_.at(i).param_name_)) { //直接匹配成功
      param_value = &bind_params_.at(i).param_value_;
    } else if (param_name.length() > 0 && ':' == param_name.ptr()[0]) {
      //param name比bind param多一个冒号
      ObString actual_param_name = param_name;
      ++actual_param_name;
      if (0 == actual_param_name.case_compare(bind_params_.at(i).param_name_)) {
        param_value = &bind_params_.at(i).param_value_;
      } else if (bind_params_.at(i).param_name_.length() > 0
          && ':' == bind_params_.at(i).param_name_.ptr()[0]) {
        //bind param比param name多一个冒号
        ObString actual_bind_name = bind_params_.at(i).param_name_;
        ++actual_bind_name;
        if (0 == actual_bind_name.case_compare(param_name)) {
          param_value = &bind_params_.at(i).param_value_;
        }
      } else { /*do nothing*/ }
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "BUG!!!: param name is not start with colon", K(param_name));
    }
  }
  return param_value;
}

int ObDbmsInfo::set_bind_param(const ObString &param_name, const ObObjParam&param_value)
{
  int ret  =OB_SUCCESS;
  OV (OB_NOT_NULL(entity_), OB_NOT_INIT, param_name, param_value);
  if (OB_SUCC(ret)) {
    ObIAllocator &alloc = entity_->get_arena_allocator();
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_INVALID_INDEX == idx && i < bind_params_.count(); i++) {
      if (0 == param_name.case_compare(bind_params_.at(i).param_name_)) {
        idx = i;
      }
    }

    if (OB_INVALID_INDEX != idx) {
      if (param_value.is_pl_extend()
              && param_value.get_ext() != 0
              && param_value.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        if (bind_params_.at(idx).param_value_.get_ext() != 0) {
          OZ (ObUserDefinedType::destruct_obj(bind_params_.at(idx).param_value_));
        }
        OZ (ObUserDefinedType::deep_copy_obj(alloc, param_value, bind_params_.at(idx).param_value_));
      } else {
        OZ (ob_write_obj(alloc, param_value, bind_params_.at(idx).param_value_),
            param_name, param_value);
      }
      OX (bind_params_.at(idx).param_value_.set_accuracy(param_value.get_accuracy()));
      OX (bind_params_.at(idx).param_value_.set_param_meta());
    } else {
      ObString clone_name;
      ObObj clone_value;
      BindParam clone_param;
      OZ (ob_write_string(alloc, param_name, clone_name), param_name, param_value);
      if (param_value.is_pl_extend()
              && param_value.get_ext() != 0
              && param_value.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        OZ (ObUserDefinedType::deep_copy_obj(alloc, param_value, clone_value));
      } else {
        OZ (ob_write_obj(alloc, param_value, clone_value), param_name, param_value);
      }
      OX (clone_param = BindParam(clone_name, clone_value));
      OX (clone_param.param_value_.set_accuracy(param_value.get_accuracy()));
      OX (clone_param.param_value_.set_param_meta());
      OZ (bind_params_.push_back(clone_param), param_name, param_value);
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int64_t ObDbmsCursorInfo::search_array(const ObString &name, ObIArray<ObString> &array)
{
  int64_t idx = OB_INVALID_INDEX;
  ObString left_name;
  if (name[0] == ':') {
    left_name = ObString(name.length() - 1, name.ptr() + 1);
  } else {
    left_name = name;
  }
  for (int64_t i = 0; i < array.count(); i++) {
    ObString right_name
      = (':' == array.at(i)[0])
          ? (ObString(array.at(i).length() - 1, array.at(i).ptr() + 1))
             : (array.at(i));
    if (0 == left_name.case_compare(right_name)) {
      idx = i;
      break;
    }
  }
  return idx;
}

int ObDbmsCursorInfo::variable_value(sql::ObSQLSessionInfo *session,
                                     ObIAllocator *allocator,
                                     const ObString &variable_name,
                                     sql::ObExprResType &result_type,
                                     ObObjParam &result)
{
  int ret = OB_SUCCESS;
  if (variable_name.empty()) {
    ret = OB_ERR_BIND_VAR_NOT_EXIST;
    LOG_WARN("bind variable does not exist", K(ret), K(variable_name));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    if (get_into_names().count() > 0 || ObStmt::is_select_stmt(get_stmt_type())) {
      if (!fetched_ || (fetched_ && 0 == rowcount_)) {
        // do nothing ...
      } else {
        idx = search_array(variable_name, get_into_names());
        if (idx != OB_INVALID_INDEX) {
          OZ (ObDbmsInfo::variable_value(session,
                                         allocator,
                                         idx,
                                         get_current_row().get_cell(idx),
                                         result_type,
                                         result));
        } else if ((idx = search_array(variable_name, get_param_names())) != OB_INVALID_INDEX) {
          CK (get_exec_params().count() > idx);
          OZ (ObDbmsInfo::variable_value(session,
                                         allocator,
                                         idx,
                                         get_exec_params().at(idx),
                                         result_type,
                                         result));
        } else {
          ret = OB_ERR_BIND_VAR_NOT_EXIST;
          LOG_WARN("bind variable does not exist", K(ret), K(variable_name));
        }
      }
    } else if ((idx = search_array(variable_name, get_param_names())) != OB_INVALID_INDEX) {
      CK (get_exec_params().count() > idx);
      OZ (ObDbmsInfo::variable_value(session,
                                     allocator,
                                     idx,
                                     get_exec_params().at(idx),
                                     result_type,
                                     result));
    } else {
      ret = OB_ERR_BIND_VAR_NOT_EXIST;
      LOG_WARN("bind variable does not exist", K(ret), K(variable_name));
    }
  }
  return ret;
}

int ObDbmsInfo::bind_variable(const ObString &param_name, const ObObjParam &param_value)
{
  int ret = OB_SUCCESS;
  OZ (set_bind_param(param_name, param_value));
  return ret;
}

int ObDbmsInfo::define_column(int64_t col_idx, ObObjType col_type,
                                    ObCollationType col_cs_type, int64_t col_size)
{
  int ret = OB_SUCCESS;
  if (col_idx < 0 || col_idx >= fields_.count()) {
    ret = OB_ERR_VARIABLE_NOT_IN_SELECT_LIST;
    LOG_WARN("define column position is invalid", K(col_idx), K(fields_), K(col_type), K(ret));
  } else if (!cast_supported(fields_.at(col_idx).type_.get_type(),
                             static_cast<common::ObCollationType>(fields_.at(col_idx).charsetnr_),
                             col_type, col_cs_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("define column position is invalid", K(col_idx), K(fields_), K(col_type), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "ORA-06562: type of out argument must match type of column or bind variable");
  } else {
    #define ENABLE_RECOVER_EXIST 1
    OZ (define_columns_.set_refactored(col_idx, col_size, ENABLE_RECOVER_EXIST));
    #undef ENABLE_RECOVER_EXIST
  }
  return ret;
}

int ObDbmsInfo::define_array(int64_t col_idx,
                                   uint64_t id,
                                   int64_t cnt,
                                   int64_t lower_bnd,
                                   ObDataType &elem_type)
{
  int ret = OB_SUCCESS;
  if (col_idx < 0 || col_idx >= fields_.count()) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("define column position is invalid", K(col_idx), K(fields_), K(id), K(ret));
  } else if (cnt <= 0) {
    ret = OB_ARRAY_CNT_IS_ILLEGAL;
    LOG_WARN("array cnt is illegal in dbms_sql.define_array", K(ret), K(cnt));
  } else if (!cast_supported(fields_.at(col_idx).type_.get_type(),
                             static_cast<common::ObCollationType>(fields_.at(col_idx).charsetnr_),
                               elem_type.get_obj_type(), elem_type.get_collation_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("define column position is invalid",
               K(col_idx), K(fields_), K(id),K(elem_type), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "ORA-06562: type of out argument must match type of column or bind variable");
  } else {
    #define ENABLE_RECOVER_EXIST 1
    ArrayDesc desc(id, cnt, lower_bnd, elem_type);
    OZ (define_arrays_.set_refactored(col_idx, desc, ENABLE_RECOVER_EXIST));
    #undef ENABLE_RECOVER_EXIST
  }

  if (OB_SUCC(ret)) {
    /*
     * DEFINE_ARRAY replaces DEFINE_COLUMN if DEFINE_ARRAY is after DEFINE_COLUMN.
     * Following is a successful example:
     *
     * declare
     * i number :=1;
     * names DBMS_SQL.varchar2_Table;
     * sals DBMS_SQL.varchar2_Table;
     * c NUMBER;
     * r NUMBER;
     * v number := 0;
     * sql_stmt VARCHAR2(32767) :=
     * 'SELECT last_name, salary FROM employees1';
     * BEGIN
     * c := DBMS_SQL.OPEN_CURSOR;
     * DBMS_SQL.PARSE(c, sql_stmt, dbms_sql.native);
     * DBMS_SQL.DEFINE_COLUMN(c, 2, v);
     * DBMS_SQL.DEFINE_ARRAY(c, 1.1, names, 5.3, 1.2);
     * DBMS_SQL.DEFINE_ARRAY(c, 2, sals, 4.5, 1.3);
     * r := DBMS_SQL.EXECUTE(c);
     * r := DBMS_SQL.FETCH_ROWS(c);
     * DBMS_SQL.COLUMN_VALUE(c, 1, names);
     * DBMS_SQL.COLUMN_VALUE(c, 2, sals);
     * DBMS_SQL.CLOSE_CURSOR(c);
     * END;
     * /
     *
     * But, DEFINE_COLUMN after DEFINE_ARRAY may cause error in DBMS_SQL.EXECUTE,
     * see DBMS_SQL.EXECUTE
     * */
    OZ (define_columns_.reuse());
  }

  return ret;
}

int ObDbmsInfo::column_value(sql::ObSQLSessionInfo *session,
                 ObIAllocator *allocator,
                 int64_t col_idx,
                 const ObObjParam src_obj,
                 sql::ObExprResType &result_type,
                 ObObjParam &result)
{
  int ret = OB_SUCCESS;
  int64_t column_size = OB_INVALID_SIZE;
  if (OB_SUCC(define_columns_.get_refactored(col_idx, column_size))) {
    ObObjParam src;
    if (OB_FAIL(ob_write_obj(*allocator, src_obj, src))) {
      LOG_WARN("write obj failed", K(ret), K(src_obj));
    } else if (OB_INVALID_SIZE != column_size
        && !ob_is_accuracy_length_valid_tc(result_type.get_type())) {
      ret = OB_ERR_BIND_TYPE_NOT_MATCH_COLUMN;
      LOG_WARN("define column type and column value type are not match",
        K(col_idx), K(fields_), K(result_type), K(result), K(ret));
    } else if (OB_FAIL((ObSPIService::spi_convert(session, allocator, src, result_type, result)))) {
      LOG_WARN("spi convert fail.", K(ret));
    } else if (OB_INVALID_SIZE != column_size && column_size < result.get_val_len()) {
      result.set_val_len(column_size);
    } else { /*do nothing*/ }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ArrayDesc *desc = const_cast<ArrayDesc *>(define_arrays_.get(col_idx));
    if (OB_ISNULL(desc)) {
      ret = OB_ERR_VARIABLE_NOT_IN_SELECT_LIST;
      LOG_WARN("column is not defined", K(col_idx), K(ret));
      LOG_USER_ERROR(OB_ERR_VARIABLE_NOT_IN_SELECT_LIST);
    } else {
      if (result.is_pl_extend() && result_type.is_ext() && result_type.get_udt_id() == desc->id_) {
        /*
         * can not reset Collection here, because if we call DEFINE_ARRAY twice and set ARRAY cnt smaller than first time, we should
         * remain the values fetched in first time not be replaces in the gap.
         */
        ObPLAssocArray *table = reinterpret_cast<ObPLAssocArray*>(result.get_ext());
        ObObjParam obj;
        ObObj key(ObInt32Type);
        ObExprResType element_type;
        ObExprPLAssocIndex::Info info;
        OX (info.for_write_ = true);
        OX (element_type.set_meta(desc->type_.get_meta_type()));
        OX (element_type.set_accuracy(desc->type_.get_accuracy()));
        for (int64_t i = 0; OB_SUCC(ret) && i < fetch_rows_.count(); ++i) {
          int64_t index = OB_INVALID_INDEX;
          ObNewRow &row = fetch_rows_.at(i);
          ObObjParam src = row.get_cell(col_idx);
          OX (key.set_int32(desc->lower_bnd_ + desc->cur_idx_));
          OZ (ObExprPLAssocIndex::do_eval_assoc_index(index, session, info, *table, key, *allocator));
          CK(table->get_count() >= index);
          OZ (ObSPIService::spi_convert(session, table->get_allocator(), src, element_type, obj));
          OZ (deep_copy_obj(*table->get_allocator(),
                            obj,
                            reinterpret_cast<ObObj*>(table->get_data())[index-1]));
          LOG_DEBUG("column add key ", K(col_idx), K(index), K(desc->cur_idx_), K(desc->lower_bnd_), K(key.get_int32()), K(table->get_key(index-1)->get_int32()));
          OX (++desc->cur_idx_);
        }
        LOG_DEBUG("column value set last and first", K(desc->lower_bnd_), K(table->get_first()), K(table->get_actual_count()), K(table->get_last()));
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("type of out argument must match type of column or bind variable",
                 K(result_type), K(result));
      }
    }
  } else {
    //do nothing, just report error
  }
  return ret;
}

	int ObDbmsInfo::variable_value(sql::ObSQLSessionInfo *session,
                               ObIAllocator *allocator,
                               int64_t col_idx,
                               const ObObjParam src_obj,
                               sql::ObExprResType &result_type,
                               ObObjParam &result)
{
  int ret = OB_SUCCESS;
  ObObjParam src;
  UNUSED(col_idx);
  OZ (ob_write_obj(*allocator, src_obj, src));
  OZ (ObSPIService::spi_convert(session, allocator, src, result_type, result));
  return ret;
}
#endif

int ObDbmsInfo::add_param_name(ObString &clone_name)
{
  return param_names_.push_back(clone_name);
}

	int ObDbmsInfo::set_into_names(int64_t into_cnt)
{
  int ret = OB_SUCCESS;
  if (into_cnt > 0) {
    ObIAllocator *alloc = NULL;
    int64_t param_cnt = param_names_.count();
    CK (into_cnt <= param_cnt);
    OV (OB_NOT_NULL(entity_), OB_NOT_INIT, ps_sql_, param_cnt);
    OX (alloc = &entity_->get_arena_allocator());
    CK (OB_NOT_NULL(alloc));
    OX (into_names_.reset());
    OX (into_names_.set_allocator(alloc));
    OZ (into_names_.init(into_cnt), ps_sql_, into_cnt);
    for (int64_t i = param_cnt - into_cnt; OB_SUCC(ret) && i < param_cnt; ++i) {
      OZ (into_names_.push_back(param_names_.at(i)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < into_cnt; ++i) {
      OX (param_names_.pop_back());
    }
  }
  return ret;
}

int ObDbmsCursorInfo::close(ObSQLSessionInfo &session, bool is_cursor_reuse, bool is_dbms_reuse)
{
  int ret = OB_SUCCESS;
  OZ (ObPLCursorInfo::close(session, is_cursor_reuse));
  reset_private();
  is_dbms_reuse ? ObDbmsInfo::reuse() : ObDbmsInfo::reset();
  return ret;
}

int ObDbmsCursorInfo::init()
{
  return ObDbmsInfo::init();
}

void ObDbmsCursorInfo::reset_private()
{
  affected_rows_ = -1;
}

void ObDbmsCursorInfo::reuse()
{
  // resue接口不重置cursor_id
  reset_private();
  ObDbmsInfo::reuse();
  ObPLCursorInfo::reuse();
}

void ObDbmsCursorInfo::reset()
{
  reset_private();
  ObDbmsInfo::reset();
  ObPLCursorInfo::reset();
}


int ObDbmsCursorInfo::prepare_entity(sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPLCursorInfo::prepare_entity(session, get_dbms_entity()))) {
    LOG_WARN("prepare dbms info entity fail.", K(ret), K(get_id()));
  } else if (OB_FAIL(
      ObPLCursorInfo::prepare_entity(session, get_cursor_entity()))) {
    LOG_WARN("prepare cursor entity fail.", K(ret), K(get_id()));
  } else { /* do nothing */ }
  return ret;
}

int ObDbmsCursorInfo::parse(const ObString &sql_stmt, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (!get_sql_stmt().empty()) {
    OZ (close(session, true));
  }
  OZ (prepare_entity(session), sql_stmt);
  OV (OB_NOT_NULL(get_dbms_entity()), OB_ALLOCATE_MEMORY_FAILED, sql_stmt);
  OV (OB_NOT_NULL(get_cursor_entity()), OB_ALLOCATE_MEMORY_FAILED, sql_stmt);
  OX (set_spi_cursor(NULL));
  if (OB_SUCC(ret)) {
    ObIAllocator &alloc = get_dbms_entity()->get_arena_allocator();
    ObParser parser(alloc, session.get_sql_mode(), session.get_charsets4parser());
    ParseResult parse_result;
    int64_t param_count = 0;
    char **param_names = NULL;
    ObString clone_name;
    // 解析语句
    if (OB_FAIL(parser.parse(sql_stmt, parse_result, DBMS_SQL_MODE))) {
      LOG_WARN("failed to parse sql_stmt", K(sql_stmt), K(ret));
      int64_t error_offset =
          parse_result.start_col_ > 0 ? (parse_result.start_col_ - 1) : 0;
      session.get_warnings_buffer().set_error_line_column(0, error_offset);
    }
    OZ (ObResolverUtils::resolve_stmt_type(parse_result, stmt_type_), sql_stmt);
    // cann't execute multi select stmt
    if (OB_SUCC(ret)
          && !parser.is_pl_stmt(sql_stmt)
          && !parser.is_single_stmt(sql_stmt)) {
      ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;
      LOG_WARN("execute immdeidate only support one stmt", K(ret));
    }
    OX (param_names = parse_result.question_mark_ctx_.name_);
    OX (param_count = parse_result.question_mark_ctx_.count_);
    // 取出所有绑定变量名
    OZ (init_params(param_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; i++) {
      OV (OB_NOT_NULL(param_names[i]), OB_INVALID_ARGUMENT, i, sql_stmt);
      /*
       * TODO: in bind_variable / variable_value, the ':' prefix is optional.
       */
      OZ (ob_write_string(alloc, ObString(param_names[i]), clone_name), i, sql_stmt);
      OZ (add_param_name(clone_name), i, sql_stmt);
    }
    OZ (ob_write_string(alloc, sql_stmt, sql_stmt_, true), sql_stmt);
  }
  return ret;
}

int64_t ObDbmsCursorInfo::get_dbms_id()
{
  int64_t id = -1;
  if (0 == (get_id() & CANDIDATE_CURSOR_ID)) {
    // ps cursor id
    id = get_id();
  } else {
    id = get_id() - CANDIDATE_CURSOR_ID;
  }
  return id;
}

int64_t ObDbmsCursorInfo::convert_to_dbms_cursor_id(int64_t id)
{
  int64_t new_id = -1;
  if (1 == (id & CANDIDATE_CURSOR_ID)) {
    // ps cursor id
    new_id = id;
  } else {
    new_id = id + CANDIDATE_CURSOR_ID;
  }
  return new_id;
}

#ifdef OB_BUILD_ORACLE_PL
int ObDbmsCursorInfo::column_value(sql::ObSQLSessionInfo *session,
                            ObIAllocator *allocator,
                            int64_t col_idx,
                            sql::ObExprResType &result_type,
                            ObObjParam &result)
{
  int ret = OB_SUCCESS;

  // check whether col_idx is defined first
  if (nullptr == get_define_columns().get(col_idx) &&
      nullptr == get_define_arrays().get(col_idx)) {
    ret = OB_ERR_VARIABLE_NOT_IN_SELECT_LIST;
    LOG_WARN("column is not defined", K(col_idx), K(ret));
    LOG_USER_ERROR(OB_ERR_VARIABLE_NOT_IN_SELECT_LIST);
  } else if (!fetched_ || (fetched_ && 0 == rowcount_)) {
    // do nothing;
  } else if (col_idx >= get_current_row().get_count()) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("column column idx out of range", K(ret), K(col_idx),
      K(get_current_row().get_count()));
  } else {
    ret = ObDbmsInfo::column_value(session, allocator, col_idx,
                                    get_current_row().get_cell(col_idx),
                                    result_type, result);
  }
  return ret;
}
#endif

#ifdef OB_BUILD_ORACLE_PL
int ObPLDbmsSql::open_cursor(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  int64_t param_count = params.count();
  number::ObNumber num;
  OV (param_count == 0 || param_count == 1, OB_INVALID_ARGUMENT, params);
  OV (OB_NOT_NULL(session));
  OZ (session->make_dbms_cursor(cursor));
  OV (OB_NOT_NULL(cursor));
  OZ (num.from(cursor->get_dbms_id(), exec_ctx.get_allocator()));
  OX (result.set_number(num));
  return ret;
}

int ObPLDbmsSql::parse_6p(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  int64_t param_count = params.count();
  OV (param_count == 6, OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_ext(), OB_INVALID_ARGUMENT, params);
  OV (params.at(2).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(3).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(4).is_tinyint(), OB_INVALID_ARGUMENT, params);
  OV (params.at(5).is_number(), OB_INVALID_ARGUMENT, params);

  ObPLCollection *sql_arr = NULL;
  OX (sql_arr = reinterpret_cast<ObPLCollection *>(params.at(1).get_ext()));
  CK (OB_NOT_NULL(sql_arr));
  CK (sql_arr->is_associative_array());
  int64_t low_bound = -1, upper_bound = -1;
  bool linefeed = false;
  int8_t flag = 0;
  OV (params.at(2).get_number().is_valid_int64(low_bound), OB_INVALID_ARGUMENT, params.at(2));
  OV (params.at(3).get_number().is_valid_int64(upper_bound), OB_INVALID_ARGUMENT, params.at(3));
  OZ (params.at(4).get_tinyint(flag), params.at(4));
  OX (linefeed = static_cast<bool>(flag));
  CK (sql_arr->get_count() >= upper_bound - low_bound);
  CK (low_bound <= upper_bound);
  ObSqlString sql_txt;
  if (OB_SUCC(ret)) {
    ObString elem_txt;
    ObPLAssocArray *assoc_arr = static_cast<ObPLAssocArray *>(sql_arr);
    int64_t *sort_arr = assoc_arr->get_sort();
    int64_t idx = assoc_arr->get_first() - 1;
    ObObj *keys = assoc_arr->get_key();
    CK (OB_NOT_NULL(keys));
    int32_t key = -1;
    int64_t low_idx = OB_INVALID_INDEX, upper_idx = OB_INVALID_INDEX;
    #define GET_NEXT() { \
      if (OB_SUCC(ret) && 0 <= idx && idx < assoc_arr->get_count()) { \
        if (OB_NOT_NULL(sort_arr)) { \
          idx = sort_arr[idx]; \
        } else { \
          idx++; \
        } \
      } else { \
        idx = OB_INVALID_INDEX; \
      } \
    }
    // 首先拿到low_bound和upper_bound对应的index;
    do {
      if (OB_SUCC(ret) && 0 <= idx && idx < assoc_arr->get_count()) {
        OZ (keys[idx].get_int32(key), keys[idx]);
        if (OB_SUCC(ret)) {
          if (static_cast<int64_t>(key) == low_bound) {
            low_idx = idx;
          }
          if (static_cast<int64_t>(key) == upper_bound) {
            upper_idx = idx;
          }
        }
      }
      if (OB_INVALID_INDEX != low_idx && OB_INVALID_INDEX != upper_idx) {
        break;
      }
      GET_NEXT();
    } while (OB_SUCC(ret) && OB_INVALID_INDEX != idx);

    CK (0 <= low_idx);
    CK (sql_arr->get_count() > low_idx);
    CK (sql_arr->get_count() > upper_idx);
    CK (low_idx <= upper_idx);

    if (OB_SUCC(ret)) {
      // reset idx
      OX (idx = assoc_arr->get_first() - 1);
      // 定位low bound
      while(0 < low_idx) {
        GET_NEXT();
        low_idx--;
        upper_idx--;
      }
      // 开始组装
      while (OB_SUCC(ret) && 0 <= upper_idx) {
        bool is_del = false;
        OZ (assoc_arr->is_elem_deleted(idx, is_del));
        if (OB_SUCC(ret)) {
          if (is_del) {
            ret = OB_READ_NOTHING;
          } else {
            ObObj &elem = reinterpret_cast<ObObj *>(assoc_arr->get_data())[idx];
            if (elem.is_null()) {
              ret = OB_READ_NOTHING;
            } else {
              OZ (elem.get_varchar(elem_txt), elem);
              OZ (sql_txt.append(elem_txt));
              if (linefeed) {
                OZ (sql_txt.append("\n"));
              }
              GET_NEXT();
            }
          }
        }
        upper_idx--;
      }
    }
  }
  ObDbmsCursorInfo *cursor = NULL;
  ObString sql_stmt;
  OZ (get_cursor(exec_ctx, params, cursor));// 这儿只用了params的第一个参数，cursor id
  OZ (ob_write_string(exec_ctx.get_allocator(), sql_txt.string(), sql_stmt));
  LOG_DEBUG("parse 6p, concated sql stmt", K(sql_stmt), K(sql_txt.string()));
  OZ (do_parse(exec_ctx, cursor, sql_stmt));
  return ret;
}

int ObPLDbmsSql::parse(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t param_count = params.count();

  // parse all param.
  ObString sql_stmt;
  OV (3 == param_count || 6 == param_count, OB_INVALID_ARGUMENT, params);
  if (OB_SUCC(ret)) {
    if (3 == param_count) {
      OV (params.at(1).is_varchar(), OB_INVALID_ARGUMENT, params);
      /*
      * TODO: support all string type for param 1.
      */
      OZ (params.at(1).get_varchar(sql_stmt), params.at(1));

      OZ (get_cursor(exec_ctx, params, cursor));
      OZ (do_parse(exec_ctx, cursor, sql_stmt));
    } else if (6 == param_count) {
      OZ (parse_6p(exec_ctx, params, result));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected argument count", K(ret), K(param_count));
    }
  }
  return ret;
}

int ObPLDbmsSql::do_parse(ObExecContext &exec_ctx, ObDbmsCursorInfo *cursor, ObString &sql_stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(cursor));
  // do parse.
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  OZ (cursor->parse(sql_stmt, *session), sql_stmt);
  if (OB_SUCC(ret)) {
    ObString ps_sql;
    stmt::StmtType stmt_type = stmt::StmtType::T_NONE;
    bool for_update = false;
    bool hidden_rowid = false;
    int64_t into_cnt = 0;
    bool skip_locked = false;
    ParamStore dummy_params;
    ObSqlString sql_str;
    ObPLExecCtx pl_ctx(cursor->get_allocator(), &exec_ctx, &dummy_params,
                     NULL/*result*/, &ret, NULL/*func*/, true);
    CK (OB_NOT_NULL(exec_ctx.get_my_session()));
    OZ (sql_str.append(sql_stmt));
    OX (cursor->get_field_columns().set_allocator(&cursor->get_dbms_entity()->get_arena_allocator()));
    OZ (ObSPIService::prepare_dynamic(&pl_ctx,
                                      cursor->get_dbms_entity()->get_arena_allocator(),
                                      false/*is_returning*/,
                                      true,
                                      cursor->get_param_name_count(),
                                      sql_str,
                                      ps_sql,
                                      stmt_type,
                                      for_update,
                                      hidden_rowid,
                                      into_cnt,
                                      skip_locked,
                                      &cursor->get_field_columns()));
    if (OB_SUCC(ret)) {
      cursor->set_ps_sql(ps_sql);
      cursor->set_stmt_type(stmt_type);
      if (for_update) {
        cursor->set_for_update();
      }
      if (hidden_rowid) {
        cursor->set_hidden_rowid();
      }
    }
    OZ (cursor->set_into_names(into_cnt));
  }
  //if (ret == OB_ERR_PARSE_SQL) {
  //  ret = OB_ERR_DBMS_SQL_INVALID_STMT;
  //}
  // execute if stmt is not dml nor select.
  if (OB_SUCC(ret) && check_stmt_need_to_be_executed_when_parsing(*cursor)) {
    OZ (do_execute(exec_ctx, *cursor));
    OX (cursor->get_ps_sql().reset());
    OX (cursor->get_sql_stmt().reset());
  }
  return ret;
}

bool ObPLDbmsSql::check_stmt_need_to_be_executed_when_parsing(ObDbmsCursorInfo &cursor)
{
  bool flag = ObStmt::is_ddl_stmt(cursor.get_stmt_type(), true);
  return flag;
}

int ObPLDbmsSql::bind_variable(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t param_count = params.count();
  int64_t out_value_size = 0;

  // parse all param.
  ObString param_name;
  ObObjParam param_value;
  OV (3 == param_count || 4 == param_count, OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_varchar(), OB_INVALID_ARGUMENT, params);
  /*
   * TODO: support all string type for param 1.
   */
  OZ (params.at(1).get_varchar(param_name), params.at(1));
  OX (param_value = params.at(2));
  if (OB_SUCC(ret) && 4 == param_count) {
    OV (params.at(3).is_number(), OB_INVALID_ARGUMENT, params);
    OZ (params.at(3).get_number().extract_valid_int64_with_trunc(out_value_size));
    OX (param_value.set_length(out_value_size));
  }
  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }

  // do bind.
  OZ (cursor->bind_variable(param_name, param_value), param_name, param_value);
  return ret;
}

int ObPLDbmsSql::define_column_number(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(exec_ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_SUCCESS;
  return ret;
}

int ObPLDbmsSql::define_column_varchar(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(exec_ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_SUCCESS;
  return ret;
}

int ObPLDbmsSql::define_column(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t param_count = params.count();

  // parse all param.
  int64_t column_pos = -1;
  ObObjType column_type = ObNullType;
  ObCollationType column_cs_type = CS_TYPE_INVALID;
  int64_t column_size = OB_INVALID_SIZE;
  OV (3 == param_count || 4 == param_count, OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(1).get_number().is_valid_int64(column_pos), OB_INVALID_ARGUMENT, params.at(1));
  OX (column_type =
      params.at(2).is_null() ? params.at(2).get_null_meta().get_type() : params.at(2).get_type());
  OX (column_cs_type = params.at(2).is_null()
                       ? params.at(2).get_null_meta().get_collation_type()
                       : params.at(2).get_collation_type());
  if (OB_SUCC(ret)) {
    if (ob_is_accuracy_length_valid_tc(column_type)) {
      if (4 == param_count) {
        OV (params.at(3).is_number(), OB_INVALID_ARGUMENT, params);
        OV (params.at(3).get_number().is_valid_int64(column_size), OB_INVALID_ARGUMENT, params.at(3));
      }
    } else if (3 == param_count) {
      //do nothing
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("column size cannot be used for this type", K(column_type), K(column_size), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "ORA-06562: type of out argument must match type of column or bind variable");
    }
  }

  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }

  // do define.
  OZ (cursor->define_column(column_pos - 1, column_type, column_cs_type, column_size),
                            column_pos, column_type);

  return ret;
}

int ObPLDbmsSql::define_array(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  int64_t param_count = params.count();

  // parse all param.
  int64_t column_pos = OB_INVALID_INDEX;
  int64_t cnt = OB_INVALID_COUNT;
  int64_t lower_bnd = OB_INVALID_INDEX;
  uint64_t type_id = OB_INVALID_ID;
  ObDataType elem_type;
  OV (5 == param_count, OB_INVALID_ARGUMENT, params);
  /*
   * DBMS_SQL.DEFINE_ARRAY (
   * c IN INTEGER,
   * position IN INTEGER, <table_variable> IN <datatype>,
   * cnt IN INTEGER,
   * lower_bnd IN INTEGER);
   * */
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(3).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(4).is_number(), OB_INVALID_ARGUMENT, params);
  OZ (params.at(1).get_number().extract_valid_int64_with_round(column_pos),
      OB_INVALID_ARGUMENT, params.at(1));
  OZ (params.at(3).get_number().extract_valid_int64_with_round(cnt),
      OB_INVALID_ARGUMENT, params.at(3));
  OV ((cnt < (1L << 31)), OB_NUMERIC_OVERFLOW, cnt);
  OZ (params.at(4).get_number().extract_valid_int64_with_round(lower_bnd),
      OB_INVALID_ARGUMENT, params.at(4));
  OV (params.at(2).is_pl_extend(), OB_INVALID_ARGUMENT, params.at(2).get_meta());
  OX (type_id = params.at(2).get_udt_id());
  if (OB_SUCC(ret)) {
    const ObCollectionType *coll_type = NULL;
    ObPLPackageGuard package_guard(exec_ctx.get_my_session()->get_effective_tenant_id());
    const ObUserDefinedType *user_type = NULL;
    CK (OB_NOT_NULL(exec_ctx.get_sql_ctx()->schema_guard_));
    OZ (ObResolverUtils::get_user_type(&exec_ctx.get_allocator(),
                                       exec_ctx.get_my_session(),
                                       exec_ctx.get_sql_proxy(),
                                       exec_ctx.get_sql_ctx()->schema_guard_,
                                       package_guard,
                                       type_id,
                                       user_type));
    CK (OB_NOT_NULL(
      coll_type = static_cast<const pl::ObCollectionType *>(user_type)));
    CK (OB_NOT_NULL(coll_type->get_element_type().get_data_type()));
    OX (elem_type = *coll_type->get_element_type().get_data_type());
  }

  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }

  // do define.
  OZ (cursor->define_array(column_pos - 1, type_id, cnt, lower_bnd, elem_type),
      column_pos, type_id, cnt, lower_bnd);

  return ret;
}

int ObPLDbmsSql::execute(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;

  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret)) {
    if (0 != cursor->get_define_columns().size() && 0 != cursor->get_define_arrays().size()) {
      /*
       * DEFINE_COLUMN after DEFINE_ARRAY cause error in DBMS_SQL.EXECUTE.
       * Following is a failed example:
       *
       * declare
       * names DBMS_SQL.varchar2_Table;
       * sals DBMS_SQL.varchar2_Table;
       * c NUMBER;
       * r NUMBER;
       * v number := 0;
       * sql_stmt VARCHAR2(32767) :=
       * 'SELECT last_name, salary FROM employees1';
       * BEGIN
       * c := DBMS_SQL.OPEN_CURSOR;
       * DBMS_SQL.PARSE(c, sql_stmt, dbms_sql.native);
       * DBMS_SQL.DEFINE_ARRAY(c, 1.1, names, 5.3, 1.2);
       * DBMS_SQL.DEFINE_ARRAY(c, 2, sals, 4.5, 1.3); --whatever this statement exists
       * DBMS_SQL.DEFINE_COLUMN(c, 2, v);
       * r := DBMS_SQL.EXECUTE(c);
       * DBMS_SQL.CLOSE_CURSOR(c);
       * END;
       * /
       * */
      ret = OB_ERR_CURSOR_CONTAIN_BOTH_REGULAR_AND_ARRAY;
      LOG_WARN("Cursor contains both regular and array defines which is illegal", K(ret));
    } else {
      // do execute.
      if (!check_stmt_need_to_be_executed_when_parsing(*cursor)) {
        OZ (do_execute(exec_ctx, *cursor, params, result));
        //every ececute should reset current index of Array
        for (ObDbmsCursorInfo::DefineArrays::iterator iter = cursor->get_define_arrays().begin();
               OB_SUCC(ret) && iter != cursor->get_define_arrays().end();
               ++iter) {
          ObDbmsCursorInfo::ArrayDesc &array_info = iter->second;
          array_info.cur_idx_ = 0;
        }
      } else {
        number::ObNumber num;
        int64_t res_num = 0;
        OZ (num.from(res_num, exec_ctx.get_allocator()));
        result.set_number(num);
      }
    }
  }

  return ret;
}

int ObPLDbmsSql::do_execute(ObExecContext &exec_ctx,
                            ObDbmsCursorInfo &dbms_cursor)
{
  int ret = OB_SUCCESS;
  ObPLExecCtx pl_ctx(dbms_cursor.get_allocator(), &exec_ctx, NULL/*params*/,
                     NULL/*result*/, &ret, NULL/*func*/, true);
  OZ (ObSPIService::dbms_dynamic_open(&pl_ctx, dbms_cursor));
  return ret;
}

int ObPLDbmsSql::do_execute(ObExecContext &exec_ctx,
                            ObDbmsCursorInfo &cursor,
                            ParamStore &params,
                            ObObj &result)
{
  int ret = OB_SUCCESS;
  number::ObNumber num;
  ObPLExecCtx pl_ctx(cursor.get_allocator(), &exec_ctx, &params,
                     NULL/*result*/, &ret, NULL/*func*/, true);
  OZ (cursor.expand_params());
  OZ (ObSPIService::dbms_dynamic_open(&pl_ctx, cursor, true));
  if (OB_SUCC(ret) && cursor.get_into_names().count() > 0) { // DML Returning
    OZ (do_fetch(exec_ctx, params, result, cursor));
  } else {
    OZ (num.from(cursor.get_affected_rows() < 0 ? 0 : cursor.get_affected_rows(), exec_ctx.get_allocator()));
    OX (result.set_number(num));
  }
  return ret;
}

int ObPLDbmsSql::fetch_rows(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t param_count = params.count();
  // parse all param.
  OV (1 == param_count, OB_INVALID_ARGUMENT, params);

  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }
  OZ (do_fetch(exec_ctx, params, result, *cursor));
  return ret;
}

int ObPLDbmsSql::do_fetch(ObExecContext &exec_ctx,
                          ParamStore &params,
                          ObObj &result,
                          ObDbmsCursorInfo &cursor) {
  int ret = OB_SUCCESS;
  bool is_first_fetch = !cursor.get_fetched();
  bool is_last_fetch_with_row = cursor.get_fetched_with_row();

  if (OB_SUCC(ret)) {
    ObPLExecCtx pl_ctx(cursor.get_allocator(), &exec_ctx, &params,
                        NULL/*result*/, &ret, NULL/*func*/, true);
    int64_t fetch_cnt = 0;
    ObNumber row_count;
    if (0 == cursor.get_define_arrays().size()) {
      OZ (ObSPIService::dbms_cursor_fetch(&pl_ctx, cursor));
      if (OB_SUCC(ret)) {
        bool found = false;
        bool isnull = false;
        OZ (cursor.get_found(found, isnull));
        OX (fetch_cnt = found ? 1 : 0);
      }
    } else {
      // if DEFINE_ARRAYs using different cnt, choose the small one
      int64_t define_cnt = OB_INVALID_COUNT;
      ObNewRow row;
      for (ObDbmsCursorInfo::DefineArrays::const_iterator iter = cursor.get_define_arrays().begin();
             OB_SUCC(ret) && iter != cursor.get_define_arrays().end();
             ++iter) {
        const ObDbmsCursorInfo::ArrayDesc &array_info = iter->second;
        if (OB_INVALID_COUNT == define_cnt || define_cnt > array_info.cnt_) {
          define_cnt = array_info.cnt_;
        }
      }
      CK (define_cnt > 0);
      OX (cursor.get_fetch_rows().reuse());
      for (int64_t i = 0; OB_SUCC(ret) && i < define_cnt; ++i) {
        OX (row.reset());
        OZ (ObSPIService::dbms_cursor_fetch(&pl_ctx, cursor));
        OZ (ob_write_row(cursor.get_dbms_entity()->get_arena_allocator(),
                         cursor.get_current_row(),
                         row));
        OZ (cursor.get_fetch_rows().push_back(row));
        OX (++fetch_cnt);
      }
      if (OB_SUCC(ret)) {
        if (fetch_cnt != define_cnt) {
          ret =  OB_ERR_FETCH_OUT_SEQUENCE;
          LOG_WARN("rows fetched unexpected", K(ret), K(fetch_cnt), K(define_cnt), K(cursor));
        }
      }
    }

    if (OB_READ_NOTHING == ret) {
      // 第一次读，重置success，因此这个时候用户不知道能否fetch到数据，返回success，并且置0.
      // 如果上次fetch有返回数据，那么下次fetch就算没有数据也不报错，逻辑同上，这个时候用户不知道是否这次能否fetch
      // 到数据，所以不抛异常。如果上次fetch结果是0，再调用fetch，那么该报错报错.
      if (is_first_fetch || (cursor.get_fetched() && is_last_fetch_with_row)) {
        ret = OB_SUCCESS;
        LOG_DEBUG("first fetch, reset ret to OB_SUCCESS");
      } else {
        ret =  OB_ERR_FETCH_OUT_SEQUENCE;
        LOG_WARN("rows fetched unexpected", K(ret), K(fetch_cnt), K(is_first_fetch), K(cursor));
      }
    }
    OZ (row_count.from(static_cast<int64_t>(fetch_cnt), exec_ctx.get_allocator()));
    OX (result.set_number(row_count));
  }
  return ret;
}

int ObPLDbmsSql::column_value(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t column_pos = -1;
  int64_t param_count = params.count();

  // parse all param.
  ObExprResType result_type;
  if (5 == param_count) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("five parameters for column_value not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "five parameters for column_value series procedure");
  }
  OV (3 == param_count, OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT, params);
  OV (params.at(1).get_number().is_valid_int64(column_pos), OB_INVALID_ARGUMENT, params.at(1));
  OX (result_type.set_meta(params.at(2).is_null()
      ? params.at(2).get_null_meta() : params.at(2).get_meta()));
  OX (result_type.set_accuracy(params.at(2).get_accuracy()));

  OZ (get_cursor(exec_ctx, params, cursor));
  CK(OB_NOT_NULL(cursor));

  OZ(cursor->column_value(exec_ctx.get_my_session(),
                          &exec_ctx.get_allocator(),
                          column_pos - 1,
                          result_type,
                          params.at(2)));

  return ret;
}

int ObPLDbmsSql::variable_value(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  ObString name;
  int64_t param_count = params.count();
  // parse all param.
  ObExprResType result_type;
  OV (3 == param_count, OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_varchar(), OB_INVALID_ARGUMENT, params);
  OZ (params.at(1).get_string(name));
  OX (result_type.set_meta(params.at(2).is_null()
      ? params.at(2).get_null_meta() : params.at(2).get_meta()));
  OX (result_type.set_accuracy(params.at(2).get_accuracy()));
  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  OZ (cursor->variable_value(exec_ctx.get_my_session(),
                             &exec_ctx.get_allocator(),
                             name,
                             result_type,
                             params.at(2)));
  return ret;
}

int ObPLDbmsSql::close_cursor(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  // do close.
  int64_t cursor_id = OB_INVALID_ID;
  OX (cursor_id = cursor->get_id());
  CK (OB_NOT_NULL(exec_ctx.get_my_session()));
  OZ (cursor->close(*exec_ctx.get_my_session()), cursor_id);
  OZ (exec_ctx.get_my_session()->close_dbms_cursor(cursor_id), cursor_id);

  OX (params.at(0).set_null());

  return ret;
}

int ObPLDbmsSql::describe_columns(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  return do_describe(exec_ctx, params, DESCRIBE);
}

int ObPLDbmsSql::describe_columns2(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  return do_describe(exec_ctx, params, DESCRIBE2);
}

int ObPLDbmsSql::describe_columns3(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  return do_describe(exec_ctx, params, DESCRIBE3);
}

int ObPLDbmsSql::do_describe(ObExecContext &exec_ctx, ParamStore &params, DescribeType type)
{
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }

  if (OB_SUCC(ret) && !ObStmt::is_select_stmt(cursor->get_stmt_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Only select statement can be described", K(cursor->get_stmt_type()), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "ORA-00900: invalid SQL statement, only select statement can be described");
  }

  OV (3 == params.count(), OB_INVALID_ARGUMENT, params);
  OV (params.at(1).is_number() || params.at(1).is_null(), OB_INVALID_ARGUMENT, params);
  OV (params.at(2).is_pl_extend(), OB_INVALID_ARGUMENT, params);

  if (OB_SUCC(ret)) {
    ObNumber num;
    OZ (num.from(cursor->get_field_columns().count(), exec_ctx.get_allocator()));
    OX (params.at(1).set_number(num));
  }

  if (OB_SUCC(ret)) {
    ObPLAssocArray *table = reinterpret_cast<ObPLAssocArray*>(params.at(2).get_ext());
    CK (OB_NOT_NULL(exec_ctx.get_my_session()));
    CK (OB_NOT_NULL(table));
    OZ (ObSPIService::spi_extend_assoc_array(exec_ctx.get_my_session()->get_effective_tenant_id(),
                                             NULL,
                                             exec_ctx.get_allocator(),
                                             *table,
                                             cursor->get_field_columns().count()));

  /*
   * TYPE desc_rec IS RECORD ( col_type BINARY_INTEGER := 0,
   * col_max_len BINARY_INTEGER := 0,
   * col_name VARCHAR2(32) := '',
   * col_name_len BINARY_INTEGER := 0,
   * col_schema_name VARCHAR2(32) := '',
   * col_schema_name_len BINARY_INTEGER := 0,
   * col_precision BINARY_INTEGER := 0,
   * col_scale BINARY_INTEGER := 0,
   * col_charsetid BINARY_INTEGER := 0,
   * col_charsetform BINARY_INTEGER := 0,
   * col_null_ok BOOLEAN := TRUE);
   *
   * TYPE desc_tab IS TABLE OF desc_rec INDEX BY BINARY_INTEGER;
   * */
    ObSEArray<ObObj, 13> row;
    ObObj obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < cursor->get_field_columns().count(); ++i) {
      ObField &field = cursor->get_field_columns().at(i);
      row.reuse();

      // col_type BINARY_INTEGER := 0,
      OX (obj.set_int32(field.type_.get_type()));
      OZ (row.push_back(obj));

      //col_max_len BINARY_INTEGER := 0,
      OX (obj.set_int32(field.length_));
      OZ (row.push_back(obj));

      // col_name VARCHAR2(32) := '',
      if (OB_SUCC(ret)) {
        if (DESCRIBE == type) {
          if (field.cname_.length() > 32) {
            ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
            LOG_WARN("character string buffer too small", K(ret), K(field.cname_));
          } else {
            OX (obj.set_varchar(field.cname_));
          }
        } else { //varchar2(32767)
          OX (obj.set_varchar(field.cname_));
        }
        OZ (row.push_back(obj));
      }

      // col_name_len BINARY_INTEGER := 0,
      OX (obj.set_int32(field.cname_.length()));
      OZ (row.push_back(obj));

      // col_schema_name VARCHAR2(32) := '',
      OX (obj.set_varchar(field.dname_));
      OZ (row.push_back(obj));

      // col_schema_name_len BINARY_INTEGER := 0,
      OX (obj.set_int32(field.dname_.length()));
      OZ (row.push_back(obj));

      // col_precision BINARY_INTEGER := 0,
      OX (obj.set_int32(field.accuracy_.get_precision()));
      OZ (row.push_back(obj));

      // col_scale BINARY_INTEGER := 0,
      OX (obj.set_int32(field.accuracy_.get_scale()));
      OZ (row.push_back(obj));

      // col_charsetid BINARY_INTEGER := 0,
      OX (OX (obj.set_int32(field.charsetnr_)));
      OZ (row.push_back(obj));

      // col_charsetform BINARY_INTEGER := 0,
      OX (obj.set_int32(field.charsetnr_));
      OZ (row.push_back(obj));

      // col_null_ok BOOLEAN := TRUE
      OX (obj.set_bool(0 == (field.flags_ & NOT_NULL_FLAG)));
      OZ (row.push_back(obj));

      if (OB_SUCC(ret) && DESCRIBE3 == type) {
        // col_type_name varchar2(32) := '',
        OX (obj.set_null());
        OZ (row.push_back(obj));

        // col_type_name_len binary_integer := 0,
        OX (obj.set_int32(0));
        OZ (row.push_back(obj));
      }
      OZ (table->set_row(row,  i));
    }

    OX (table->set_first(1));
    OX (table->set_last(cursor->get_field_columns().count()));
    OX (table->set_key(NULL));
    OX (table->set_sort(NULL));
  }

  return ret;
}

int ObPLDbmsSql::get_cursor(ObExecContext &exec_ctx, ParamStore &params, ObDbmsCursorInfo *&cursor)
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  int64_t security_level = 0;

  // parse and check all param.
  int64_t cursor_id = -1;
  int64_t id = -1;
  if (params.at(0).is_float()) {
    ret = OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST;
    LOG_WARN("cursor id is invalid int64 ", K(cursor_id), K(id), K(ret));
  } else if (params.at(0).is_number()) {
    params.at(0).get_number().is_valid_int64(id);
  }

  if (OB_SUCC(ret) && 0 == id) {
    ret = OB_OBEN_CURSOR_NUMBER_IS_ZERO;
    LOG_WARN("dbms_sql open cursor fail, cursor id is 0", K(ret), K(id), K(security_level));
  }
  // get cursor.
  OV (OB_NOT_NULL(session), OB_INVALID_ARGUMENT, id);
  OX (cursor_id = ObDbmsCursorInfo::convert_to_dbms_cursor_id(id))
  OV (OB_NOT_NULL(cursor = session->get_dbms_cursor(cursor_id)),
      OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST, cursor_id);

  return ret;
}

int ObPLDbmsSql::is_open(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  bool is_open = false;
  ObDbmsCursorInfo *cursor = NULL;
  if (params.at(0).is_null()
      || params.at(0).get_number().compare(static_cast<uint64_t>(0)) <= 0) {
    // cursor id is null, return false.
  } else if (OB_SUCC(get_cursor(exec_ctx, params, cursor))) {
    is_open = true;
  }
  result.set_bool(const_cast<const bool &>(is_open));
  return ret;
}

int ObPLDbmsSql::execute_and_fetch(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDbmsCursorInfo *cursor = NULL;
  int64_t row_count = 0;
  uint64_t param_count = params.count();
  ObNumber fetch_cnt;
  bool has_open = true;
  OZ (get_cursor(exec_ctx, params, cursor));
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret) && cursor->get_sql_stmt().empty()) {
    ret = OB_NO_STMT_PARSE;
    LOG_WARN("no statement parsed or DDL is executed when parsed", K(ret));
  }
  if (OB_SUCC(ret)) {
      //every ececute should reset current index of Array
    for (ObDbmsCursorInfo::DefineArrays::iterator iter = cursor->get_define_arrays().begin();
                OB_SUCC(ret) && iter != cursor->get_define_arrays().end();
                ++iter) {
      ObDbmsCursorInfo::ArrayDesc &array_info = iter->second;
      array_info.cur_idx_ = 0;
    }
  }
  // todo : when dbms_cursor support stream cursor need change here
  if (OB_SUCC(ret) && (!cursor->isopen()
                        || (cursor->isopen() && cursor->get_rowcount() == cursor->get_spi_cursor()->cur_))
                   && !check_stmt_need_to_be_executed_when_parsing(*cursor)) {
    // do execute.
    has_open = false;
    OZ (do_execute(exec_ctx, *cursor, params, result));
  }
  if (OB_SUCC(ret)) {
    if (2 == param_count) {
      if (params.at(1).is_null() && ObNullType == params.at(1).get_type()) {
        // not exact_fetch
      } else if (ObTinyIntType != params.at(1).get_type()) {
        ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
        LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, 17, "EXECUTE_AND_FETCH");
        LOG_WARN("wrong types of arguments", K(params.at(1).get_type()));
      } else if (params.at(1).get_bool()) {
        // exact fetch
        if (OB_NOT_NULL(cursor->get_spi_cursor())) {
          row_count = cursor->get_spi_cursor()->row_store_.get_row_cnt();
        }
        if (row_count < 1) {
          ret = OB_READ_NOTHING;
          LOG_WARN("OCI_EXACT_FETCH has not enoughrows.", K(ret));
        } else if (row_count > 1) {
          ret = OB_ERR_TOO_MANY_ROWS;
          LOG_WARN("OCI_EXACT_FETCH has too many rows.", K(ret));
        }
      } else { /* do nothing */ }
    }
  }
  if (!has_open || (OB_NOT_NULL(cursor) && !cursor->get_fetched())) {
    OZ (do_fetch(exec_ctx, params, result, *cursor));
  } else {
    OZ (fetch_cnt.from(static_cast<int64_t>(1), exec_ctx.get_allocator()));
    OX (result.set_number(fetch_cnt));
  }
  return ret;
}

/* now dbms cursor use [unstream cursor]
 * so we should fetch orig cursor info into a new unstream cursor
 * there is three things we should do
 *  1. fill cursor field
 *  2. fill spi_cursor. (which result row_store include)
 *  3. close old cursor
 *
 * orig cursor maybe two part
 *  1. stream cursor
 *      use fill_cursor to fill dbms cursor
 *  2. unstream cursor
 *      fetch row store to fill dbms cursor
 *
 */
int ObPLDbmsSql::fill_dbms_cursor(ObSQLSessionInfo *session,
                                  ObPLCursorInfo *cursor,
                                  ObDbmsCursorInfo *new_cursor)
{
  int ret = OB_SUCCESS;
  uint64_t size = 0;
  ObSPICursor *spi_cursor = NULL;
  OV (OB_NOT_NULL(session) && OB_NOT_NULL(new_cursor), OB_ERR_UNEXPECTED);
  OV (OB_NOT_NULL(cursor), OB_ERR_INVALID_CURSOR);

  // 1. fill cursor field
  OV (cursor->is_streaming() ? OB_NOT_NULL(cursor->get_cursor_handler())
        : OB_NOT_NULL(cursor->get_spi_cursor()));
  OZ (ObDbmsInfo::deep_copy_field_columns(
                  new_cursor->get_dbms_entity()->get_arena_allocator(),
                  cursor->is_streaming()
                    ? cursor->get_cursor_handler()->get_result_set()->get_field_columns()
                    : &(cursor->get_spi_cursor()->fields_),
                  new_cursor->get_field_columns()));

  // 2. fill spi_cursor
  OZ (session->get_tmp_table_size(size));
  OZ (new_cursor->prepare_spi_cursor(spi_cursor,
                                session->get_effective_tenant_id(),
                                size,
                                false,
                                session));
  OV (OB_NOT_NULL(spi_cursor));

  if OB_FAIL(ret) {
    // do nothing
  } else {
    // 2.* fill row store
    if (cursor->is_streaming()) {
      // we can't reopen the cursor, so if fill cursor has error. we will report to client.
      OZ (ObSPIService::fill_cursor(*(cursor->get_cursor_handler()->get_result_set()), spi_cursor, 0));
    } else {
      ObSPICursor *orig_spi_cursor = cursor->get_spi_cursor();
      for (int64_t i = 0; OB_SUCC(ret) && i < orig_spi_cursor->fields_.count(); ++i) {
        ObDataType type;
        type.set_meta_type(orig_spi_cursor->fields_.at(i).type_.get_meta());
        type.set_accuracy(orig_spi_cursor->fields_.at(i).accuracy_);
        if (OB_FAIL(spi_cursor->row_desc_.push_back(type))) {
          LOG_WARN("push back error", K(i), K(orig_spi_cursor->fields_.at(i).type_),
            K(orig_spi_cursor->fields_.at(i).accuracy_), K(ret));
        }
      }
      for (int64_t i = orig_spi_cursor->cur_; OB_SUCC(ret) && i < orig_spi_cursor->row_store_.get_row_cnt(); i++) {
        // pay attention to ra rowstore overload function of get_row
        const ObNewRow *cur_row = NULL;
        OZ (orig_spi_cursor->row_store_.get_row(i, cur_row));
        OZ (spi_cursor->row_store_.add_row(*cur_row));
      }
    }
  }
  OX (spi_cursor->row_store_.finish_add_row());
  OX (new_cursor->open(spi_cursor));
  if (OB_FAIL(ret) && NULL != spi_cursor) {
    spi_cursor->~ObSPICursor();
    LOG_WARN("fill cursor failed.", K(ret), K(new_cursor->get_id()), K(session->get_sessid()));
  }

  // 3. set old cursor invalid
  OX (cursor->set_invalid_cursor());
  //OZ (OB_INVALID_ID == cursor->get_id() ? cursor->close(*session) : session->close_cursor(cursor->get_id()));
  return ret;
}

int ObPLDbmsSql::to_cursor_number(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  /* TODO : to_cursor_number param type check
   * oracle :
   *  1. NULL type report OB_ERR_EXP_NOT_ASSIGNABLE
   *  2. wrong type report OB_ERR_CALL_WRONG_ARG
   *  3. cursor type but cursor not init or be set to null report OB_ERR_INVALID_CURSOR
   * OB :
   *  1. in out param not support const value, to_cursor_number(NULL) report -5592
   *  2. wrong type will report -5555 when pick_routine
   *  3. oracle report OB_ERR_INVALID_CURSOR when cursor type be set to null, other ext type will report OB_ERR_CALL_WRONG_ARG .
   *    IN OB, when a ext type be set to null, get_extend_type() will be set to a invalid type. we can't distinguish them by get_extend_type(),
   *    we will all report OB_ERR_CALL_WRONG_ARG now.
   */
  if (params.at(0).is_ext() && (params.at(0).get_meta().get_extend_type() == PL_CURSOR_TYPE
        || params.at(0).get_meta().get_extend_type() == PL_REF_CURSOR_TYPE)) {
    ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(params.at(0).get_ext());
    if (NULL != cursor) {
      ObDbmsCursorInfo *new_cursor = NULL;
      ObSQLSessionInfo *session = exec_ctx.get_my_session();
      ObNumber cursor_id;
      OV (!cursor->is_invalid_cursor(), OB_ERR_INVALID_CURSOR);
      OV (cursor->isopen(), OB_ERR_INVALID_CURSOR);
      OV (OB_NOT_NULL(session));
      OZ (session->make_dbms_cursor(new_cursor));
      OV (OB_NOT_NULL(new_cursor));
      OZ (new_cursor->prepare_entity(*session));
      OZ (fill_dbms_cursor(session, cursor, new_cursor));
      OX (new_cursor->set_ref_by_refcursor());
      OX (new_cursor->set_stmt_type(stmt::T_SELECT));
      OX (new_cursor->get_sql_stmt() = "SELECT");  // placeholder for SQL stmt
      if (OB_FAIL(ret) && NULL != new_cursor) {
        int close_ret = session->close_cursor(new_cursor->get_id());
        if (OB_SUCCESS != close_ret) {
          LOG_WARN("close new cursor fail.", K(ret));
        }
      }

      // set out param
      //OX (new_cursor->set_is_session_cursor());
      // OX (params.at(0).set_extend(reinterpret_cast<int64_t>(new_cursor), PL_REF_CURSOR_TYPE));
      // OX (params.at(0).set_param_meta());

      // set return value
      OZ (cursor_id.from(new_cursor->get_dbms_id(), exec_ctx.get_allocator()));
      OX (result.set_number(cursor_id));
    } else {
      ret = OB_ERR_INVALID_CURSOR;
      LOG_WARN("cursor not be inited yet. ", K(ret), K(params.at(0).get_type()), K(params.at(0).get_meta().get_extend_type()));
    }
  } else if (params.at(0).is_null()) {
    ret = OB_ERR_EXP_NOT_ASSIGNABLE;
    LOG_WARN("NULL type can't to cursor number. ", K(ret));
  } else {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("use wrong type. ", K(ret), K(params.at(0).get_type()), K(params.at(0).get_meta().get_extend_type()));
  }
  return ret;
}

int ObPLDbmsSql::last_error_position(ObExecContext &exec_ctx, ParamStore &params, ObObj &result) {
  int ret = OB_SUCCESS;

  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  CK(OB_NOT_NULL(session));
  CK(params.count() == 0);

  ObNumber res;
  int64_t pos = -1;
  OX (pos = session->get_warnings_buffer().get_error_column());
  OZ (res.from(pos, exec_ctx.get_eval_res_allocator()));
  OX (result.set_number(res));

  return ret;
}

// start of dbms_sql
// TODO: not support long type. define_column_long,
//       The implementation of define_column_long and column_value_long not support yet

int ObPLDbmsSql::define_column_long(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(exec_ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_ERR_BIND_TYPE_NOT_MATCH_COLUMN;
  LOG_WARN("not support long type yet.", K(ret));
  return ret;
}

int ObPLDbmsSql::column_value_long(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  UNUSED(exec_ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_ERR_BIND_TYPE_NOT_MATCH_COLUMN;
  LOG_WARN("not support long type yet.", K(ret));
  return ret;
}
#endif

}
}
