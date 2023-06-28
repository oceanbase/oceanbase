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
  } else if (src_fields->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src fields is null.", K(ret), K(src_fields->count()));
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
    ObParser parser(alloc, session.get_sql_mode(), session.get_local_collation_connection());
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



}
}
