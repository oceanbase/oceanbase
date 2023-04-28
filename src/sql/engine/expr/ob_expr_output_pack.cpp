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

#define USING_LOG_PREFIX  SQL_ENG
#include "sql/engine/expr/ob_expr_output_pack.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "io/easy_io.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_lob_access_utils.h"
namespace oceanbase{

using namespace common;

namespace sql{


ObExprOutputPack::ObExprOutputPack(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_OP_OUTPUT_PACK, N_OUTPUT_PACK,
                     MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                     INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOutputPack::~ObExprOutputPack()
{
}

int ObExprOutputPack::calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  if (OB_UNLIKELY(param_num <= 1)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    type.set_varbinary();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(OB_MAX_ROW_LENGTH);
  }
  return ret;
}

int ObExprOutputPack::cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOutputPackInfo::init_output_pack_info(raw_expr.get_extra(), cg_ctx.allocator_, rt_expr, type_))) {
    LOG_WARN("failed to init output pack info", K(ret));
  } else {
    rt_expr.eval_func_ = &eval_output_pack;
    rt_expr.eval_batch_func_ = &eval_output_pack_batch;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObOutputPackInfo, param_fields_);

int ObOutputPackInfo::deep_copy(ObIAllocator &allocator,
                                const ObExprOperatorType type,
                                ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  ObOutputPackInfo *output_pack_info = nullptr;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc expr extra info", K(ret));
  } else if (OB_ISNULL(output_pack_info = dynamic_cast<ObOutputPackInfo *>(copied_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  }
  else if (OB_FAIL(output_pack_info->param_fields_.prepare_allocate(param_fields_.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_fields_.count(); i++) {
      if (OB_FAIL(output_pack_info->param_fields_.at(i).deep_copy(param_fields_.at(i),
                                                                  &allocator))) {
        LOG_WARN("failed to write field", K(ret));
      }
    }
  }
  return ret;
}

int ObOutputPackInfo::init_output_pack_info(uint64_t extra,
                                            common::ObIAllocator *allocator,
                                            ObExpr &rt_expr,
                                            const ObExprOperatorType type)
{
  int ret = OB_SUCCESS;
  ObOutputPackInfo *output_pack_info = NULL;
  void *buf = NULL;
  ObPhysicalPlanCtx *pctx = nullptr;
  common::ObIArray<common::ObField> *fields = nullptr;
  if (OB_ISNULL(allocator)
      || OB_ISNULL(fields = reinterpret_cast<common::ObIArray<common::ObField> *> (extra))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg ctx is is invalid", K(ret));
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObOutputPackInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    output_pack_info = new(buf) ObOutputPackInfo(*allocator, type);
    output_pack_info->param_fields_.set_allocator(allocator);
    if (OB_FAIL(output_pack_info->param_fields_.reserve(rt_expr.arg_cnt_))) {
      LOG_WARN("fail to init param fields", K(ret));
    }
    //for output_pack expr , the 0th parameter indicates whether the ps protocol
    //only arg_cnt_ - 1 fields stored in pctx
    for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_ - 1; ++i) {
      ObField tmp_field;
      if (OB_UNLIKELY(i >= fields->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array pos", K(i), K(fields->count()), K(ret));
      } else if (OB_FAIL(tmp_field.full_deep_copy(fields->at(i), allocator))) {
        LOG_WARN("full deep copy field failed", K(ret));
      } else if (OB_FAIL(output_pack_info->param_fields_.push_back(tmp_field))) {
        LOG_WARN("failed to add field", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      rt_expr.extra_info_ = output_pack_info;
      LOG_DEBUG("succ init_output_pack_info", KPC(output_pack_info));
    }
  }
  return ret;
}

int ObExprOutputPack::eval_output_pack(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  ObSqlCtx *sql_ctx = nullptr;
  share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
  ObOutputPackInfo *extra_info = static_cast<ObOutputPackInfo *> (expr.extra_info_);
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &alloc = alloc_guard.get_allocator();

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())
      || OB_ISNULL(sql_ctx = ctx.exec_ctx_.get_sql_ctx())
      || OB_ISNULL(schema_guard = sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not init", K(session), K(sql_ctx), K(schema_guard));
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed to eval output datum", K(ret));
  } else if (OB_FAIL(process_oneline(expr, ctx, session, alloc,
                                     extra_info, schema_guard, expr_datum))) {
    LOG_WARN("failed to process oneline", K(ret));
  }
  return ret;
}

int ObExprOutputPack::eval_output_pack_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  ObSqlCtx *sql_ctx = nullptr;
  share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
  ObOutputPackInfo *extra_info = static_cast<ObOutputPackInfo *> (expr.extra_info_);
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &alloc = alloc_guard.get_allocator();
  ObDatum *result = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())
      || OB_ISNULL(sql_ctx = ctx.exec_ctx_.get_sql_ctx())
      || OB_ISNULL(schema_guard = sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not init", K(session), K(sql_ctx), K(schema_guard));
  } else if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      guard.set_batch_idx(i);
      if (OB_FAIL(process_oneline(expr, ctx, session, alloc,
                                  extra_info, schema_guard, result[i]))) {
        LOG_WARN("failed to process oneline", K(ret));
      }
      eval_flags.set(i);
    }
  }
  return ret;
}


int ObExprOutputPack::encode_cell(const ObObj &cell, const common::ObIArray<ObField> &param_fields,
                                  char *buf, int64_t len, int64_t &pos, char *bitmap,
                                  int64_t column_num, ObSQLSessionInfo *session,
                                  share::schema::ObSchemaGetterGuard *schema_guard,
                                  obmysql::MYSQL_PROTOCOL_TYPE encode_type)
{
  int ret = OB_SUCCESS;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  ret = ObSMUtils::cell_str(buf, len, cell, encode_type, pos, column_num, bitmap,
                            dtc_params, &param_fields.at(column_num), schema_guard,
                            session->get_effective_tenant_id());
  return ret;
}

int ObExprOutputPack::reset_bitmap(char *result_buffer, const int64_t len,
                                  const int64_t column_num, int64_t &pos, char *&bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t bitmap_bytes =  (column_num + 7 + 2) / 8;
  if (len - pos < 1 + bitmap_bytes) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    MEMSET(result_buffer + pos, 0, 1);
    pos++;
    bitmap = result_buffer + pos;
    MEMSET(bitmap, 0, bitmap_bytes);
    pos += bitmap_bytes;
  }
  return ret;
}

int ObExprOutputPack::convert_string_value_charset(common::ObObj &value,
                                                   common::ObIAllocator &alloc,
                                                   const ObSQLSessionInfo &my_session)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else {
    OZ (value.convert_string_value_charset(charset_type, alloc));
  }
  return ret;
}
int ObExprOutputPack::convert_lob_value_charset(common::ObObj &value,
                                                common::ObIAllocator &alloc,
                                                const ObSQLSessionInfo &my_session)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObLobLocator *lob_locator = NULL;
  ObCharsetType charset_type = CHARSET_INVALID;
  if (OB_FAIL(value.get_lob_locator(lob_locator))) {
    LOG_WARN("get lob locator failed", K(ret));
  } else if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lob locator", K(ret));
  } else if (OB_FAIL(lob_locator->get_payload(str))) {
    LOG_WARN("get lob locator payload failed", K(ret));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(value.get_collation_type());
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(value.get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == value.get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(value.get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != value.get_collation_type() && CS_TYPE_BINARY != collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char *buf = NULL;
      int32_t header_len = value.get_val_len() - lob_locator->payload_size_;
      int32_t str_len = lob_locator->payload_size_ * 4;
      uint32_t result_len = 0;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(
                alloc.alloc(header_len + str_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(header_len), K(str_len));
      } else {
        MEMCPY(buf, lob_locator, header_len);
        if (OB_FAIL(convert_string_charset(str, value.get_collation_type(), collation_type,
                                                buf + header_len, str_len, result_len))) {
          LOG_WARN("convert string charset failed", K(ret));
        } else {
          ObLobLocator *result_lob = reinterpret_cast<ObLobLocator *>(buf);
          result_lob->payload_size_ = result_len;
          value.set_lob_locator(*result_lob);
          value.set_collation_type(collation_type);
        }
      }
    }
  }
  return ret;
}

int ObExprOutputPack::convert_text_value_charset(common::ObObj& value,
                                                 bool res_has_lob_header,
                                                 common::ObIAllocator &alloc,
                                                 const ObSQLSessionInfo &my_session)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  ObString raw_str = value.get_string();
  if (OB_ISNULL(raw_str.ptr()) || raw_str.length() == 0) {
    // may need return error?
    LOG_DEBUG("get null lob locator v2", K(ret));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType to_collation_type = ObCharset::get_default_collation(charset_type);
    ObCollationType from_collation_type = value.get_collation_type();
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(from_collation_type);
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(to_collation_type);
    const ObObjType type = value.get_type();

    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(from_collation_type), K(to_collation_type));
    } else if (CS_TYPE_INVALID == from_collation_type || CS_TYPE_INVALID == to_collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(from_collation_type), K(to_collation_type), K(ret));
    } else if (CS_TYPE_BINARY != from_collation_type && CS_TYPE_BINARY != to_collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      if (value.is_lob_storage() && value.has_lob_header() && res_has_lob_header &&
          my_session.is_client_use_lob_locator() && lib::is_oracle_mode()) {
        ObLobLocatorV2 lob;
        ObString inrow_data;
        if (OB_FAIL(process_lob_locator_results(value, alloc, my_session))) {
          LOG_WARN("fail to process lob locator", K(ret), K(value));
        } else if (OB_FAIL(value.get_lob_locatorv2(lob))) {
          LOG_WARN("fail to lob locator v2", K(ret), K(value));
        } else if (!lob.has_inrow_data()) {
          // do nothing
        } else if (OB_FAIL(lob.get_inrow_data(inrow_data))) {
          LOG_WARN("fail to get inrow data", K(ret), K(lob));
        } else if (inrow_data.length() == 0) {
          // do nothing
        } else {
          int64_t lob_data_byte_len = inrow_data.length();
          int64_t offset_len = reinterpret_cast<uint64_t>(inrow_data.ptr()) - reinterpret_cast<uint64_t>(lob.ptr_);
          int64_t res_len = offset_len + lob_data_byte_len * ObCharset::CharConvertFactorNum;
          char *buf = static_cast<char*>(alloc.alloc(res_len));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(res_len));
          } else {
            MEMCPY(buf, lob.ptr_, offset_len);
            uint32_t result_len = 0;
            if (OB_FAIL(convert_string_charset(inrow_data, from_collation_type, to_collation_type,
                                               buf + offset_len, res_len - offset_len, result_len))) {
              LOG_WARN("Lob: convert string charset failed", K(ret));
            } else {
              // refresh payload size
              if (lob.has_extern()) {
                ObMemLobExternHeader *ex_header = nullptr;
                if (OB_FAIL(lob.get_extern_header(ex_header))) {
                  LOG_WARN("fail to get extern header", K(ret), K(lob));
                } else {
                  ex_header->payload_size_ = result_len;
                }
              }
              // refresh lob data size
              if (OB_SUCC(ret)) {
                ObLobCommon *lob_common = nullptr;
                if (OB_FAIL(lob.get_disk_locator(lob_common))) {
                  LOG_WARN("fail to get lob common", K(ret), K(lob));
                } else if (lob_common->is_init_) {
                  ObLobData *lob_data = reinterpret_cast<ObLobData*>(lob_common->buffer_);
                  lob_data->byte_size_ = result_len;
                }
              }
              if (OB_SUCC(ret)) {
                LOG_INFO("Lob: new temp convert_text_value_charset in convert_text_value",
                K(ret), K(raw_str), K(type), K(to_collation_type), K(from_collation_type),
                K(from_charset_info->csname), K(to_charset_info->csname));
                value.set_lob_value(type, buf, offset_len + result_len);
                value.set_collation_type(to_collation_type);
                value.set_has_lob_header();
              }
            }
          }
        }
      } else {
        // get full data, buffer size is full byte length * 4
        ObString data_str;
        ObLobLocatorV2 loc(raw_str, value.has_lob_header());
        ObTextStringIter str_iter(value);
        if (OB_FAIL(str_iter.init(0, &my_session, &alloc))) {
          LOG_WARN("Lob: init lob str iter failed ", K(ret), K(value));
        } else if (OB_FAIL(str_iter.get_full_data(data_str))) {
          LOG_WARN("Lob: init lob str iter failed ", K(ret), K(value));
        } else {
          // mock result buffer and reserve data length
          // could do streaming charset convert
          ObTextStringResult new_tmp_lob(type, res_has_lob_header, &alloc);
          char *buf = NULL;
          int64_t buf_len = 0;
          int64_t lob_data_byte_len = 0;
          uint32_t result_len = 0;

          if (OB_FAIL(loc.get_lob_data_byte_len(lob_data_byte_len))) {
            LOG_WARN("Lob: get lob data byte len failed", K(ret), K(loc));
          } else if (OB_FAIL(new_tmp_lob.init(lob_data_byte_len * 4))) {
            LOG_WARN("Lob: init tmp lob failed", K(ret), K(lob_data_byte_len * 4));
          } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_len))) {
            LOG_WARN("Lob: get empty buffer failed", K(ret), K(lob_data_byte_len * 4));
          } else if (OB_FAIL(convert_string_charset(data_str, from_collation_type, to_collation_type,
                                                    buf, buf_len, result_len))) {
            LOG_WARN("convert string charset failed", K(ret));
          } else if (OB_FAIL(new_tmp_lob.lseek(result_len, 0))) {
            LOG_WARN("temp lob lseek failed", K(ret));
          } else {
            ObString lob_loc_str;
            new_tmp_lob.get_result_buffer(lob_loc_str);
            LOG_INFO("Lob: new temp convert_text_value_charset in convert_text_value",
                K(ret), K(raw_str), K(type), K(to_collation_type), K(from_collation_type),
                K(from_charset_info->csname), K(to_charset_info->csname));
            value.set_lob_value(type, lob_loc_str.ptr(), lob_loc_str.length());
            value.set_collation_type(to_collation_type);
            if (new_tmp_lob.has_lob_header()) {
              value.set_has_lob_header();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprOutputPack::convert_lob_locator_to_longtext(common::ObObj &value,
                                                      common::ObIAllocator &alloc,
                                                      const ObSQLSessionInfo &my_session)
{
  UNUSED(my_session);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    bool is_use_lob_locator = my_session.is_client_use_lob_locator();
    if (is_use_lob_locator && value.is_lob()) {
      ObString str;
      char *buf = nullptr;
      ObLobLocator *lob_locator = nullptr;
      if (OB_FAIL(value.get_string(str))) {
        STORAGE_LOG(WARN, "Failed to get string from obj", K(ret), K(value));
      } else if (OB_ISNULL(buf = static_cast<char*>(
                            alloc.alloc(str.length() + sizeof(ObLobLocator))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(str));
      } else if (FALSE_IT(lob_locator = reinterpret_cast<ObLobLocator *> (buf))) {
      } else if (OB_FAIL(lob_locator->init(str))) {
        STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(lob_locator));
      } else {
        value.set_lob_locator(*lob_locator);
        LOG_TRACE("return lob locator", K(*lob_locator), K(str));
      }
    } else if (!is_use_lob_locator && value.is_lob_locator()) {
      ObString payload;
      if (OB_ISNULL(value.get_lob_locator())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(value));
      } else if (OB_FAIL(value.get_lob_locator()->get_payload(payload))) {
        LOG_WARN("fail to get payload", K(ret));
      } else {
        value.set_lob_value(ObLongTextType,
                            payload.ptr(),
                            static_cast<int32_t>(payload.length()));
      }
    }
    LOG_TRACE("return data", K(is_use_lob_locator), K(value), K(lbt()));
  }
  return ret;
}

int ObExprOutputPack::process_lob_locator_results(common::ObObj& value,
                                                  common::ObIAllocator &alloc,
                                                  const ObSQLSessionInfo &my_session)
{
  int ret = OB_SUCCESS;
  // 1. if client is_use_lob_locator, return lob locator
  // 2. if client is_use_lob_locator, but not support outrow lob, return lob locator with inrow data
  //    refer to sz/aibo1m
  // 3. if client does not support use_lob_locator ,,return full lob data without locator header
  bool is_use_lob_locator = my_session.is_client_use_lob_locator();
  bool is_support_outrow_locator_v2 = my_session.is_client_support_lob_locatorv2();
  if (!(value.is_lob() || value.is_json() || value.is_lob_locator())) {
    // not lob types, do nothing
  } else if (is_use_lob_locator && value.is_lob() && lib::is_oracle_mode()) {
    // if does not have extern header, mock one
    ObLobLocatorV2 loc(value.get_string(), value.has_lob_header());
    if (loc.is_lob_locator_v1()) { // do nothing, lob locator version 1
    } else if (loc.is_valid()) { // lob locator v2
      if (!loc.has_lob_header()) { // consider 4.0 will return no lob header obj
        ObString data = value.get_string();
        ObTextStringResult new_tmp_lob(value.get_type(), true, &alloc);
        if (OB_FAIL(new_tmp_lob.init(data.length()))) {
          LOG_WARN("fail to init text string result", K(ret), K(new_tmp_lob), K(data.length()));
        } else if (OB_FAIL(new_tmp_lob.append(data))) {
          LOG_WARN("fail to append data", K(ret), K(new_tmp_lob), K(data.length()));
        } else {
          ObString res;
          new_tmp_lob.get_result_buffer(res);
          value.set_lob_value(value.get_type(), res.ptr(), res.length());
          value.set_has_lob_header(); // must has lob header
        }
      } else if (!loc.has_extern()) {  // if has header but without extern header
        if (!loc.has_inrow_data()) {
          // mock loc v2 with extern ? currently all temp lobs have extern field in oracle mode
          // should not come here
          OB_ASSERT(0);
        } else if (!is_support_outrow_locator_v2) {
          // convert to full extern temp lob locator
          ObString inrow_data;
          if (OB_FAIL(loc.get_inrow_data(inrow_data))) {
            LOG_WARN("Lob: get inrow data failed", K(ret), K(loc));
          } else {
            value.set_string(value.get_type(), inrow_data.ptr(), inrow_data.length());
            value.set_inrow();
            if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(value, alloc))) {
              LOG_WARN("fail to convert temp lob locator", K(ret), K(value));
            }
          }
        }
      } else if (!is_support_outrow_locator_v2 && !loc.has_inrow_data()) {
        if (OB_FAIL(ObTextStringIter::append_outrow_lob_fulldata(value, &my_session, alloc))) {
          LOG_WARN("Lob: convert lob to outrow failed", K(value), K(GET_MIN_CLUSTER_VERSION()));
        }
      }
    } else {
      OB_ASSERT(0);
    }
  } else if ((!is_use_lob_locator && lib::is_oracle_mode())
             || lib::is_mysql_mode()) {
    ObString raw_str = value.get_string();
    // remove locator header and read full lob data
    ObString data;
    ObLobLocatorV2 loc(value.get_string());
    if (loc.is_lob_locator_v1()) {
      if (OB_FAIL(convert_lob_locator_to_longtext(value, alloc, my_session))) {
        LOG_WARN("Lob: handle lob locator v1 failed", K(value), K(GET_MIN_CLUSTER_VERSION()));
      }
    } else { // lob locator v2
      ObTextStringIter instr_iter(value);
      if (OB_FAIL(instr_iter.init(0, &my_session, &alloc))) {
        LOG_WARN("init lob str inter failed", K(ret), K(value));
      } else if (OB_FAIL(instr_iter.get_full_data(data))) {
        LOG_WARN("Lob: init lob str iter failed ", K(value));
      } else {
        ObObjType dst_type = value.is_json() ? ObJsonType : ObLongTextType;
        // remove has lob header flag
        value.set_lob_value(dst_type, data.ptr(), static_cast<int32_t>(data.length()));
      }
    }
  } else { /* do nothing */ }
  return ret;
}

int ObExprOutputPack::process_sql_udt_results(common::ObObj& value,
                                              common::ObIAllocator &alloc,
                                              const ObSQLSessionInfo &my_session)
{
  int ret = OB_SUCCESS;
  if (!value.is_xml_sql_type()) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "not support udt type", K(ret), K(value.get_type()), K(value.get_udt_subschema_id()));
  } else {
    bool is_client_support_binary_xml = false; // client receive xml as json, like json
    if (value.is_null() || value.is_nop_value()) {
      // do nothing
    } else if (is_client_support_binary_xml) {
      // convert to udt client format
    } else {
      ObString data;
      ObLobLocatorV2 loc(value.get_string(), true);
      if (loc.is_null()) {
      } else {
        ObTextStringIter instr_iter(ObLongTextType, CS_TYPE_BINARY, value.get_string(), true);
        if (OB_FAIL(instr_iter.init(0, &my_session, &alloc))) {
          LOG_WARN("init lob str inter failed", K(ret), K(value));
        } else if (OB_FAIL(instr_iter.get_full_data(data))) {
          LOG_WARN("get xml full data failed", K(value));
        } else {
          value.set_udt_value(data.ptr(), data.length());
        }
      }
    }
  }
  return ret;
}

int ObExprOutputPack::convert_string_charset(const common::ObString &in_str,
                                             const ObCollationType in_cs_type,
                                             const ObCollationType out_cs_type,
                                             char *buf, int32_t buf_len, uint32_t &result_len)
{
  int ret = OB_SUCCESS;
  ret = ObCharset::charset_convert(in_cs_type, in_str.ptr(),
        in_str.length(),out_cs_type, buf, buf_len, result_len);
  if (OB_SUCCESS != ret) {
    int32_t str_offset = 0;
    int64_t buf_offset = 0;
    ObString question_mark = ObCharsetUtils::get_const_str(out_cs_type, '?');
    while (str_offset < in_str.length() && buf_offset + question_mark.length() <= buf_len) {
      int64_t offset = ObCharset::charpos(in_cs_type, in_str.ptr() + str_offset,
                                          in_str.length() - str_offset, 1);
      ret = ObCharset::charset_convert(in_cs_type, in_str.ptr() + str_offset, offset, out_cs_type,
                                       buf + buf_offset, buf_len - buf_offset, result_len);
      str_offset += offset;
      if (OB_SUCCESS == ret) {
        buf_offset += result_len;
      } else {
        MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
        buf_offset += question_mark.length();
      }
    }
    if (str_offset < in_str.length()) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("sizeoverflow", K(ret), K(in_str), KPHEX(in_str.ptr(), in_str.length()));
    } else {
      result_len = buf_offset;
      ret = OB_SUCCESS;
      LOG_WARN("charset convert failed", K(ret), K(in_cs_type), K(out_cs_type));
    }
  }
  return ret;
}

int ObExprOutputPack::try_encode_row(const ObExpr &expr, ObEvalCtx &ctx,
                                     ObSQLSessionInfo *session, common::ObIAllocator &alloc,
                                     ObOutputPackInfo *extra_info, char *buffer, char *bitmap,
                                     share::schema::ObSchemaGetterGuard *schema_guard,
                                     obmysql::MYSQL_PROTOCOL_TYPE encode_type,
                                     const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  for (int64_t column_idx = 1; OB_SUCC(ret) && column_idx < expr.arg_cnt_; ++column_idx) {
    ObDatum &datum = expr.locate_param_datum(ctx, column_idx);
    ObObj obj;
    if (OB_FAIL(datum.to_obj(obj, expr.args_[column_idx]->obj_meta_,
                             expr.args_[column_idx]->obj_datum_map_))) {
      LOG_WARN("failed to cast to obj", K(ret), K(column_idx));
    } else {
      if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == encode_type) {
        ObCastMode cast_mode = CM_NONE;
        if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session, cast_mode))) {
          LOG_WARN("failed to get default cast mode", K(ret));
        } else {
          cast_mode |= CM_WARN_ON_FAIL;
          if (obj.get_type() != extra_info->param_fields_.at(column_idx - 1).type_.get_type()) {
            ObCastCtx cast_ctx(&alloc, NULL, cast_mode, CS_TYPE_INVALID);
             if (OB_FAIL(common::ObObjCaster::to_type(extra_info->param_fields_.at(column_idx - 1).type_.get_type(),
                                                      cast_ctx,
                                                      obj,
                                                      obj))) {
              LOG_WARN("failed to cast object", K(ret), K(obj),
              K(obj.get_type()), K(extra_info->param_fields_.at(column_idx - 1).type_.get_type()));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (ob_is_string_tc(obj.get_type()) && CS_TYPE_INVALID != obj.get_collation_type()) {
          OZ(convert_string_value_charset(obj, alloc, *session));
        } else if (obj.is_clob_locator()
                  && OB_FAIL(convert_lob_value_charset(obj, alloc, *session))) {
          LOG_WARN("convert lob obj charset failed", K(ret));
        } else if (ob_is_text_tc(obj.get_type())
                  && OB_FAIL(convert_text_value_charset(obj, expr.obj_meta_.has_lob_header(), alloc, *session))) {
          LOG_WARN("convert text obj charset failed", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if ((obj.is_lob() || obj.is_lob_locator() || obj.is_json())
                   && OB_FAIL(process_lob_locator_results(obj, alloc, *session))) {
          LOG_WARN("convert lob locator to longtext failed", K(ret));
        } else if (obj.is_user_defined_sql_type() && OB_FAIL(process_sql_udt_results(obj, alloc, *session))) {
          LOG_WARN("convert udt to client format failed", K(ret), K(obj.get_udt_subschema_id()));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(encode_cell(obj, extra_info->param_fields_, buffer, len,
                              pos, bitmap, column_idx - 1/*for encode type expr*/,
                              session, schema_guard, encode_type))) {
      if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
        LOG_WARN("failed to encode obj", K(ret), K(column_idx));
      }
    }
  }
  return ret;
}

int ObExprOutputPack::process_oneline(const ObExpr &expr, ObEvalCtx &ctx, ObSQLSessionInfo *session,
                                      common::ObIAllocator &alloc, ObOutputPackInfo *extra_info,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  char *buffer = nullptr;
  int64_t len = 0;
  int64_t pos = 0;
  char *bitmap = nullptr;
  obmysql::MYSQL_PROTOCOL_TYPE encode_type;
  ObDatum &type_datum = expr.locate_param_datum(ctx, 0);
  encode_type = (type_datum.get_int() == 0)
              ? obmysql::MYSQL_PROTOCOL_TYPE::BINARY : obmysql::MYSQL_PROTOCOL_TYPE::TEXT;
  int tmp_ret = OB_SIZE_OVERFLOW;
  expr.cur_str_resvered_buf(ctx, buffer, len);
  if (OB_ISNULL(buffer) || len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get str res mem", K(ret), K(len));
  }
  //if try encode failed, refresh buffer and retry
  while(OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
    tmp_ret = OB_SUCCESS;
    if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == encode_type) {
      tmp_ret = reset_bitmap(buffer, len, expr.arg_cnt_ - 1, pos, bitmap);
    }
    if (OB_SUCCESS == tmp_ret) {
      tmp_ret = try_encode_row(expr, ctx, session, alloc, extra_info,
                              buffer, bitmap, schema_guard, encode_type, len, pos);
    }
    if (OB_SUCCESS != tmp_ret) {
      len *= 2;
      if (len >= MAX_PACK_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("encode row size overflow ", K(ret), K(len));
      } else if (OB_ISNULL(buffer = expr.get_str_res_mem(ctx, len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to get str res mem", K(ret), K(len));
      } else {
        //re encode from begin
        pos = 0;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_ret)) {
    LOG_WARN("failed to try encode row", K(ret));
  }
  if (OB_SUCC(ret)) {
    expr_datum.set_string(buffer, pos);
  }
  return ret;
}


}//namespace sql
}//namespace oceambase

