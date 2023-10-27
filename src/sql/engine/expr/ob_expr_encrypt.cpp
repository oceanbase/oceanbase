/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_encrypt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprDesEncrypt::ObExprDesEncrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_DES_ENCRYPT,
                         N_DES_ENCRYPT,
                         ONE_OR_TWO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprDesEncrypt::~ObExprDesEncrypt() {}

int ObExprDesEncrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 2 || param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(types_stack[0].get_collation_type(), len))) {
    LOG_WARN("fail to get mbmaxlen of the first param", K(ret));
  } else {
    len = len * types_stack[0].get_length();
    types_stack[0].set_calc_type(common::ObVarcharType);
    types_stack[0].set_calc_collation_type(types_stack[0].get_collation_type());
    types_stack[0].set_calc_collation_level(types_stack[0].get_collation_level());
    if (2 == param_num) {
      if (ob_is_integer_type(types_stack[1].get_type()) ||
          ObBitType == types_stack[1].get_type() ||
          ObYearType == types_stack[1].get_type()) {
        types_stack[1].set_calc_type(common::ObIntType);
        types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
        types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
      } else {
        types_stack[1].set_calc_type(common::ObVarcharType);
        types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
        types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
      }
    } else {}
    type.set_varbinary();
    type.set_length(len + 9);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprDesEncrypt::eval_des_encrypt_with_default(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    bool is_null = false;
    uint key_number = 0;
    struct DesKeySchedule keyschedule;
    DES_cblock ivec;
    //des-key-file is not supported, use default key
    MEMSET(&keyschedule, 0, sizeof(keyschedule));
    if (src.is_null()) {
      res.set_null();
      is_null = true;
    } else if (2 == expr.arg_cnt_) {
      ObDatum &key = expr.locate_param_datum(ctx, 1);
      if (key.is_null()) {
        key_number = 0;
      } else if (key.get_int() > 9 || key.get_int() < 0) {
        ObString func_name(N_DES_ENCRYPT);
        res.set_null();
        is_null = true;
        LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
        LOG_WARN("invalid param to procedure des_encrypt", K(ret), K(key.get_int()));
      } else {
        key_number = key.get_int();
      }
    }
    if (OB_SUCC(ret) && !is_null) {
      ObString src_str = src.get_string();
      if (src_str.empty()) {
        res.set_string(src_str);
      } else {
        size_t res_length = src_str.length() + (8 - src_str.length() % 8);
        char *res_buf = expr.get_str_res_mem(ctx, res_length + 1);
        if (OB_FAIL(ob_des_encrypt(ctx, src_str, keyschedule, key_number, res_buf))) {
          LOG_WARN("fail to do encrypt", K(ret));
        } else {
          res.set_string(res_buf, res_length + 1);
        }
      }
    }
  }
  return ret;
}

int ObExprDesEncrypt::eval_des_encrypt_batch_with_default(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size) {
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool has_key = (2 == expr.arg_cnt_);
  struct DesKeySchedule keyschedule;
  //des-key-file is not supported, use default key
  MEMSET(&keyschedule, 0, sizeof(keyschedule));
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else if (has_key) {
    if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("eval args_[1] failed", K(ret));
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector key_array;
    if (has_key) {
      key_array = expr.args_[1]->locate_expr_datumvector(ctx);
    } else {}
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else {
        ObString src_str = src_array.at(j)->get_string();
        uint key_number = 0;
        if (has_key) {
          if (key_array.at(j)->is_null()) {
            key_number = 0;
          } else {
            key_number = key_array.at(j)->get_int();
          }
        } else {}
        if (key_number > 9 || key_number < 0) {
          ObString func_name(N_DES_ENCRYPT);
          res_datum.at(j)->set_null();
          LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
          LOG_WARN("invalid param to procedure des_encrypt", K(ret), K(key_number));
        } else if (src_str.empty()) {
          res_datum.at(j)->set_string(src_str);
        } else {
          size_t res_length = src_str.length() + (8 - src_str.length() % 8);
          char *res_buf = expr.get_str_res_mem(ctx, res_length + 1, j);
          if (OB_FAIL(ob_des_encrypt(ctx, src_str, keyschedule, key_number, res_buf))) {
            LOG_WARN("fail to do encrypt", K(ret));
          } else {
            res_datum.at(j)->set_string(res_buf, res_length + 1);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprDesEncrypt::eval_des_encrypt_with_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    ObDatum &key = expr.locate_param_datum(ctx, 1);
    struct DesKeyBlock keyblock;
    struct DesKeySchedule keyschedule;
    if (src.is_null()) {
      res.set_null();
    } else if (key.is_null()) {
      ObString func_name(N_DES_ENCRYPT);
      res.set_null();
      LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
      LOG_WARN("invalid param to procedure des_encrypt", K(ret));
    } else if (src.get_string().empty()) {
      res.set_string(src.get_string());
    } else {
      const ObString key_str = key.get_string().empty() ? ObString(""): key.get_string();
      ObString src_str = src.get_string();
      size_t res_length = src_str.length() + (8 - src_str.length() % 8);
      char *res_buf = expr.get_str_res_mem(ctx, res_length + 1);
      DES_cblock ivec;
      MEMSET(&ivec, 0, sizeof(ivec));
      EVP_BytesToKey(EVP_des_ede3_cbc(), EVP_md5(), NULL,
            reinterpret_cast<const uchar*>(key_str.ptr()), static_cast<int>(key_str.length()),
            1, reinterpret_cast<uchar*>(&keyblock), ivec);
      DES_set_key_unchecked(&keyblock.key1,&keyschedule.ks1);
      DES_set_key_unchecked(&keyblock.key2,&keyschedule.ks2);
      DES_set_key_unchecked(&keyblock.key3,&keyschedule.ks3);
      if (OB_FAIL(ob_des_encrypt(ctx, src_str, keyschedule, 127, res_buf))) {
        LOG_WARN("fail to do encrypt", K(ret));
      } else {
        res.set_string(res_buf, res_length + 1);
      }
    }
  }
  return ret;
}

int ObExprDesEncrypt::eval_des_encrypt_batch_with_key(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  struct DesKeySchedule keyschedule;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[1] failed", K(ret));
  } else {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector key_array = expr.args_[1]->locate_expr_datumvector(ctx);
    struct DesKeyBlock keyblock;
    struct DesKeySchedule keyschedule;
    DES_cblock ivec;
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (key_array.at(j)->is_null()) {
        ObString func_name(N_DES_ENCRYPT);
        res_datum.at(j)->set_null();
        LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
        LOG_WARN("invalid param to procedure des_encrypt", K(ret));
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else {
        const ObString key_str = key_array.at(j)->get_string().empty() ? ObString(""): key_array.at(j)->get_string();
        ObString src_str = src_array.at(j)->get_string();
        size_t res_length = src_str.length() + (8 - src_str.length() % 8);
        char *res_buf = expr.get_str_res_mem(ctx, res_length + 1, j);
        MEMSET(&ivec, 0, sizeof(ivec));
        EVP_BytesToKey(EVP_des_ede3_cbc(), EVP_md5(), NULL,
              reinterpret_cast<const uchar*>(key_str.ptr()), static_cast<int>(key_str.length()),
              1, reinterpret_cast<uchar*>(&keyblock), ivec);
        DES_set_key_unchecked(&keyblock.key1,&keyschedule.ks1);
        DES_set_key_unchecked(&keyblock.key2,&keyschedule.ks2);
        DES_set_key_unchecked(&keyblock.key3,&keyschedule.ks3);
        if (OB_FAIL(ob_des_encrypt(ctx, src_str, keyschedule, 127, res_buf))) {
          LOG_WARN("fail to do encrypt", K(ret));
        } else {
          res_datum.at(j)->set_string(res_buf, res_length + 1);
        }
      }
    }
  }
  return ret;
}

int ObExprDesEncrypt::ob_des_encrypt(ObEvalCtx &ctx, const ObString &src, struct DesKeySchedule &keyschedule, uint key_number, char *res_buf) {
  int ret = OB_SUCCESS;
  size_t tail = 8 - src.length() % 8;
  size_t res_length = src.length() + tail;
  char *arg_buf = NULL;
  DES_cblock ivec;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  arg_buf = static_cast<char *>(calc_alloc.alloc(res_length));
  if (OB_ISNULL(arg_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else {
    MEMCPY(arg_buf, src.ptr(), src.length());
    MEMCPY(arg_buf + src.length(), "********", tail);
    arg_buf[res_length - 1] = static_cast<char>(tail);
    res_buf[0] = static_cast<char>(128 | key_number);
    memset(&ivec, 0, sizeof(ivec));
    DES_ede3_cbc_encrypt((const uchar*)(arg_buf),
                        reinterpret_cast<uchar*>(res_buf + 1),
                        res_length,
                        &keyschedule.ks1,
                        &keyschedule.ks2,
                        &keyschedule.ks3,
                        &ivec, true);
  }
  return ret;
}

int ObExprDesEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (2 == raw_expr.get_param_count() && common::ObIntType != raw_expr.get_param_expr(1)->get_result_type().get_type()) {
    rt_expr.eval_func_ = eval_des_encrypt_with_key;
    rt_expr.eval_batch_func_ = eval_des_encrypt_batch_with_key;
  } else {
    rt_expr.eval_func_ = eval_des_encrypt_with_default;
    rt_expr.eval_batch_func_ = eval_des_encrypt_batch_with_default;
  }
  return ret;
}


ObExprDesDecrypt::ObExprDesDecrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_DES_DECRYPT,
                         N_DES_DECRYPT,
                         ONE_OR_TWO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprDesDecrypt::~ObExprDesDecrypt() {}

int ObExprDesDecrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
   int64_t len = 0;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 2 || param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(ret), K(param_num));
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(types_stack[0].get_collation_type(), len))) {
    LOG_WARN("fail to get mbmaxlen of the first param", K(ret));
  } else {
    types_stack[0].set_calc_type(common::ObVarcharType);
    types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
    types_stack[0].set_calc_collation_level(types_stack[0].get_collation_level());
    if (2 == param_num) {
      types_stack[1].set_calc_type(common::ObVarcharType);
      types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
      types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
    }
    type.set_varbinary();
    len = len * types_stack[0].get_length();
    if (len < 9) {
      type.set_length(len);
    } else {
      type.set_length(len - 9);
    }
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprDesDecrypt::eval_des_decrypt(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    bool need_decrypt = false;
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    struct DesKeyBlock keyblock;
    struct DesKeySchedule keyschedule;
    DES_cblock ivec;
    ObString src_str;
    //get key and check if res should be null or empty
    if (src.is_null()) {
      res.set_null();
    } else {
      src_str = src.get_string();
      if (src_str.length() < 9 || src_str.length() % 8 != 1 || !(src_str.ptr()[0] & 128)) {
        //src is not encrypted
        res.set_string(src_str);
      } else if (1 == expr.arg_cnt_) {
        uint key_num = static_cast<uint>(src_str.ptr()[0] & 127);
        ObSQLSessionInfo *session = nullptr;
        if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(session));
        } else if (key_num > 9 || !session->has_user_super_privilege()) {
          ObString func_name(N_DES_DECRYPT);
          res.set_null();
          LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
          LOG_WARN("invalid param to procedure des_encrypt", K(ret), K(key_num));
        } else {
          //des-key-file is not supported, use default key
          MEMSET(&keyschedule, 0, sizeof(keyschedule));
          need_decrypt = true;
        }
      } else {
        ObDatum &key = expr.locate_param_datum(ctx, 1);
        if (key.is_null()) {
          ObString func_name(N_DES_DECRYPT);
          res.set_null();
          LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
          LOG_WARN("invalid param to procedure des_encrypt", K(ret));
        } else {
          const ObString key_str = key.get_string().empty() ? ObString(""): key.get_string();
          MEMSET(&ivec, 0, sizeof(ivec));
          EVP_BytesToKey(EVP_des_ede3_cbc(), EVP_md5(), NULL,
                reinterpret_cast<const uchar*>(key_str.ptr()), static_cast<int>(key_str.length()),
                1, reinterpret_cast<uchar*>(&keyblock), ivec);
          DES_set_key_unchecked(&keyblock.key1,&keyschedule.ks1);
          DES_set_key_unchecked(&keyblock.key2,&keyschedule.ks2);
          DES_set_key_unchecked(&keyblock.key3,&keyschedule.ks3);
          need_decrypt = true;
        }
      }
    }
    //do descrypt
    if (OB_SUCC(ret) && need_decrypt) {
      char* result_buf = expr.get_str_res_mem(ctx, src_str.length() + 1);
      uint tail = 0;
      MEMSET(&ivec, 0, sizeof(ivec));
      DES_ede3_cbc_encrypt(reinterpret_cast<const uchar*>(src_str.ptr() + 1),
                          reinterpret_cast<uchar*>(result_buf),
                          src_str.length() - 1,
                          &keyschedule.ks1,
                          &keyschedule.ks2,
                          &keyschedule.ks3,
                          &ivec, false);
      tail = static_cast<uint>(static_cast<uchar>(result_buf[src_str.length() - 2]));
      if (tail > 8) {
        res.set_null();
      } else {
        res.set_string(result_buf, src_str.length() - 1 - tail);
      }
    }
  }
  return ret;
}

int ObExprDesDecrypt::eval_des_decrypt_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  struct DesKeySchedule keyschedule;
  struct DesKeyBlock keyblock;
  DES_cblock ivec;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatumVector src_array;
  ObDatumVector key_array;
  //des-key-file is not supported, use default key
  MEMSET(&keyschedule, 0, sizeof(keyschedule));

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else {
    src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    if (2 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
        LOG_WARN("eval args_[1] failed", K(ret));
      } else {
        key_array = expr.args_[1]->locate_expr_datumvector(ctx);
      }
    } else {}
  }
  if (OB_SUCC(ret)) {
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      //get key and check if res should be null or empty
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else {
        const ObString src_str = src_array.at(j)->get_string();
        bool need_descrypt = false;
        if (src_str.length() < 9 || src_str.length() % 8 != 1 || !(src_str.ptr()[0] & 128)) {
          //src is not encrypted
          res_datum.at(j)->set_string(src_str);
        } else if (1 == expr.arg_cnt_) {
          uint key_num = static_cast<uint>(src_str.ptr()[0] & 127);
          ObSQLSessionInfo *session = nullptr;
          if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(session));
          } else if (key_num > 9 || !session->has_user_super_privilege()) {
            ObString func_name(N_DES_DECRYPT);
            res_datum.at(j)->set_null();
            LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
            LOG_WARN("invalid param to procedure des_encrypt", K(ret), K(key_num));
          } else {
            need_descrypt = true;
          }
        } else if (key_array.at(j)->is_null()) {
          ObString func_name(N_DES_DECRYPT);
          res_datum.at(j)->set_null();
          LOG_USER_WARN(OB_ERR_INVALID_PARAM_TO_PROCEDURE,func_name.length(),func_name.ptr());
          LOG_WARN("invalid param to procedure des_encrypt", K(ret));
        } else {
          const ObString key_str = key_array.at(j)->get_string().empty() ? ObString(""): key_array.at(j)->get_string();
          MEMSET(&ivec, 0, sizeof(ivec));
          EVP_BytesToKey(EVP_des_ede3_cbc(), EVP_md5(), NULL,
                reinterpret_cast<const uchar*>(key_str.ptr()), static_cast<int>(key_str.length()),
                1, reinterpret_cast<uchar*>(&keyblock), ivec);
          DES_set_key_unchecked(&keyblock.key1,&keyschedule.ks1);
          DES_set_key_unchecked(&keyblock.key2,&keyschedule.ks2);
          DES_set_key_unchecked(&keyblock.key3,&keyschedule.ks3);
          need_descrypt = true;
        }
        //do descrypt
        if (OB_SUCC(ret) && need_descrypt) {
          char *result_buf = expr.get_str_res_mem(ctx, src_str.length() + 1, j);
          uint tail = 0;
          MEMSET(&ivec, 0, sizeof(ivec));
          DES_ede3_cbc_encrypt(reinterpret_cast<const uchar*>(src_str.ptr() + 1),
                              reinterpret_cast<uchar*>(result_buf),
                              src_str.length() - 1,
                              &keyschedule.ks1,
                              &keyschedule.ks2,
                              &keyschedule.ks3,
                              &ivec, false);
          tail = static_cast<uint>(static_cast<uchar>(result_buf[src_str.length() - 2]));
          if (tail > 8) {
            res_datum.at(j)->set_null();
          } else {
            res_datum.at(j)->set_string(result_buf, src_str.length() - 1 - tail);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprDesDecrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_des_decrypt;
  rt_expr.eval_batch_func_ = eval_des_decrypt_batch;
  return ret;
}

ObExprEncrypt::ObExprEncrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ENCRYPT,
                         N_ENCRYPT,
                         ONE_OR_TWO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprEncrypt::~ObExprEncrypt() {}

int ObExprEncrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 2 || param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    for (int i = 0; i < param_num; ++i) {
      types_stack[i].set_calc_type(common::ObVarcharType);
      types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
      types_stack[i].set_calc_collation_level(types_stack[i].get_collation_level());
    }
    type.set_varbinary();
    type.set_length(13);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprEncrypt::eval_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    char salt[3];
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    bool need_encrypt = false;
    char *tmp_src = NULL;
    char tmp_res[14];
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();

    if (src.is_null()) {
      res.set_null();
    } else if (src.get_string().empty()) {
      res.set_string(src.get_string());
    } else if (1 == expr.arg_cnt_) {
      ObSQLSessionInfo *session = NULL;
      if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(session));
      } else {
        int64_t tv_sec = session->get_query_start_time()/1000000;
        salt[0] = bin_to_ascii(tv_sec & 0x3f);
        salt[1] = bin_to_ascii((tv_sec >> 5) & 0x3f);
        salt[2] = 0;
        need_encrypt = true;
      }
    } else {
      ObDatum &key = expr.locate_param_datum(ctx, 1);
      if (key.is_null() || key.get_string().length() < 2) {
        res.set_null();
      } else {
        MEMCPY(salt, key.get_string().ptr(), 2);
        salt[2] = 0;
        if (!check_key_valid(salt[0],salt[1])) {
          res.set_null();
        } else {
          need_encrypt = true;
        }
      }
    }
    if (OB_SUCC(ret) && need_encrypt) {
      tmp_src = static_cast<char *>(calc_alloc.alloc(src.get_string().length() + 1));
      if (OB_ISNULL(tmp_src)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem failed", K(ret));
      } else {
        size_t len = 0;
        char * res_buf = NULL;
        MEMCPY(tmp_src, src.get_string().ptr(), src.get_string().length());
        tmp_src[src.get_string().length()] = 0;
        DES_fcrypt(tmp_src, salt, tmp_res);
        len = STRLEN(tmp_res);
        res_buf = expr.get_str_res_mem(ctx, len);
        MEMCPY(res_buf, tmp_res, len);
        res.set_string(res_buf, len);
      }
    }
  }
  return ret;
}

int ObExprEncrypt::eval_encrypt_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size) {
  int ret = OB_SUCCESS;
  ObDatumVector src_array;
  ObDatumVector key_array;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  char *tmp_src = NULL;
  char *tmp_res = NULL;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  char salt[3];

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else {
    src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    if (2 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
        LOG_WARN("eval args_[1] failed", K(ret));
      } else {
        key_array = expr.args_[1]->locate_expr_datumvector(ctx);
      }
    } else {
      ObSQLSessionInfo *session = nullptr;
      if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(session));
      } else {
        int64_t timestamp = session->get_query_start_time();
        salt[0] = bin_to_ascii(timestamp & 0x3f);
        salt[1] = bin_to_ascii((timestamp >> 5) & 0x3f);
        salt[2] = 0;
      }
    }
  }
  if (OB_SUCC(ret)) {
    char *tmp_src = NULL;
    size_t cur_tmp_src_maxlen = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      bool need_encrypt = false;
      char tmp_res[14];
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else if (2 == expr.arg_cnt_) {
        if (key_array.at(j)->is_null() || (key_array.at(j)->get_string().length() < 2)) {
          res_datum.at(j)->set_null();
        } else {
          MEMCPY(salt, key_array.at(j)->get_string().ptr(), 2);
          if (!check_key_valid(salt[0],salt[1])) {
            res_datum.at(j)->set_null();
          } else {
            need_encrypt = true;
          }
        }
      } else {
        need_encrypt = true;
      }
      if (OB_SUCC(ret) && need_encrypt) {
        ObString src_str = src_array.at(j)->get_string();
        if (cur_tmp_src_maxlen <= src_str.length()) {
          tmp_src = static_cast<char *>(calc_alloc.alloc(src_str.length() + 1));
          if (OB_ISNULL(tmp_src)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem failed", K(ret));
          } else {
            cur_tmp_src_maxlen = src_str.length() + 1;
          }
        } else {}
          size_t len = 0;
          char * res_buf = NULL;
          MEMCPY(tmp_src, src_str.ptr(), src_str.length());
          tmp_src[src_str.length()] = 0;
          DES_fcrypt(tmp_src, salt, tmp_res);
          len = STRLEN(tmp_res);
          res_buf = expr.get_str_res_mem(ctx, len, j);
          MEMCPY(res_buf, tmp_res, len);
          res_datum.at(j)->set_string(res_buf, len);
      }
    }
  }
  return ret;
}

int ObExprEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_encrypt;
  rt_expr.eval_batch_func_ = eval_encrypt_batch;
  return ret;
}

inline char ObExprEncrypt::bin_to_ascii(int64_t c) {
  return static_cast<char> ((c)>= 38 ? ((c) - 38 + 'a') : (c) >= 12 ? ((c) - 12 + 'A'): (c) + '.');
}

inline bool ObExprEncrypt::check_key_valid(char salt0, char salt1) {
  return ((salt0 >= 'a' && salt0 <= 'z') || (salt0 >= 'A' && salt0 <= 'Z') || (salt0 >= '.' && salt0 <= '9')) &&
         ((salt1 >= 'a' && salt1 <= 'z') || (salt1 >= 'A' && salt1 <= 'Z') || (salt1 >= '.' && salt1 <= '9'));
}

ObExprEncode::ObExprEncode(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ENCODE,
                         N_ENCODE,
                         2,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprEncode::~ObExprEncode() {}

int ObExprEncode::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 0;
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type1.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail to get mbmaxlen of the first param", K(ret));
  } else {
    type1.set_calc_type(common::ObVarcharType);
    type1.set_calc_collation_type(type1.get_collation_type());
    type1.set_calc_collation_level(type1.get_collation_level());
    type2.set_calc_type(common::ObVarcharType);
    type2.set_calc_collation_type(type2.get_collation_type());
    type2.set_calc_collation_level(type2.get_collation_level());
    type.set_varbinary();
    type.set_length(type1.get_length() * mbmaxlen);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprEncode::eval_encode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    ObDatum &key = expr.locate_param_datum(ctx, 1);
    if (src.is_null() || key.is_null()) {
      res.set_null();
    } else {
      ObString src_str = src.get_string();
      ObString key_str = key.get_string();
      uint32_t password[2];
      ObCrypt crypt;
      if (src_str.empty()) {
        res.set_string(src_str);
      } else {
        char * res_buf = expr.get_str_res_mem(ctx, src_str.length());
        crypt.init(key_str);
        if (OB_FAIL(crypt.encode(src_str, res_buf))) {
          LOG_WARN("fail to encode string", K(ret));
        } else {
          res.set_string(res_buf, src_str.length());
        }
      }
    }
  }
  return ret;
}

int ObExprEncode::eval_encode_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint32_t password[2];
  ObCrypt crypt;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[1] failed", K(ret));
  } else if (!expr.args_[1]->is_batch_result()) {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatum &key = expr.locate_param_datum(ctx, 1);
    if (!key.is_null()) {
      ObString key_str = key.get_string();
      crypt.init(key_str);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (key.is_null() || src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else {
        ObString src_str = src_array.at(j)->get_string();
        char * res_buf = expr.get_str_res_mem(ctx, src_str.length(), j);
        if (OB_FAIL(crypt.encode(src_str, res_buf))) {
          LOG_WARN("fail to encode string", K(ret));
        } else {
          res_datum.at(j)->set_string(res_buf, src_str.length());
        }
        crypt.reinit();
      }
    }
  } else {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector key_array = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (key_array.at(j)->is_null() || src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else {
        ObString src_str = src_array.at(j)->get_string();
        ObString key_str = key_array.at(j)->get_string();

        char * res_buf = expr.get_str_res_mem(ctx, src_str.length(), j);
        crypt.init(key_str);
        if (OB_FAIL(crypt.encode(src_str, res_buf))) {
          LOG_WARN("fail to encode string", K(ret));
        } else {
          res_datum.at(j)->set_string(res_buf, src_str.length());
        }
      }
    }
  }
  return ret;
}

int ObExprEncode::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_encode;
  rt_expr.eval_batch_func_ = eval_encode_batch;
  return ret;
}

ObExprDecode::ObExprDecode(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_DECODE,
                         N_DECODE,
                         2,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprDecode::~ObExprDecode() {}

int ObExprDecode::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 0;
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type1.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail to get mbmaxlen of the first param", K(ret));
  } else {
    type1.set_calc_type(common::ObVarcharType);
    type1.set_calc_collation_type(type1.get_collation_type());
    type1.set_calc_collation_level(type1.get_collation_level());
    type2.set_calc_type(common::ObVarcharType);
    type2.set_calc_collation_type(type2.get_collation_type());
    type2.set_calc_collation_level(type2.get_collation_level());
    type.set_varbinary();
    type.set_length(type1.get_length() * mbmaxlen);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprDecode::eval_decode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &src = expr.locate_param_datum(ctx, 0);
    ObDatum &key = expr.locate_param_datum(ctx, 1);
    if (src.is_null() || key.is_null()) {
      res.set_null();
    } else {
      ObString src_str = src.get_string();
      ObString key_str = key.get_string();
      uint32_t password[2];
      ObCrypt crypt;
      if (src_str.empty()) {
        res.set_string(src_str);
      } else {
        char * res_buf = expr.get_str_res_mem(ctx, src_str.length());
        crypt.init(key_str);
        if (OB_FAIL(crypt.decode(src_str, res_buf))) {
          LOG_WARN("fail to decode string", K(ret));
        } else {
          res.set_string(res_buf, src_str.length());
        }
      }
    }
  }
  return ret;
}

int ObExprDecode::eval_decode_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint32_t password[2];
  ObCrypt crypt;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[1] failed", K(ret));
  } else if (!expr.args_[1]->is_batch_result()) {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatum &key = expr.locate_param_datum(ctx, 1);
    if (!key.is_null()) {
      ObString key_str = key.get_string();
      crypt.init(key_str);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (key.is_null() || src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else {
        ObString src_str = src_array.at(j)->get_string();
        char * res_buf = expr.get_str_res_mem(ctx, src_str.length(), j);
        if (OB_FAIL(crypt.decode(src_str, res_buf))) {
          LOG_WARN("fail to decode string", K(ret));
        } else {
          res_datum.at(j)->set_string(res_buf, src_str.length());
        }
        crypt.reinit();
      }
    }
  } else {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector key_array = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (key_array.at(j)->is_null() || src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (src_array.at(j)->get_string().empty()) {
        res_datum.at(j)->set_string(src_array.at(j)->get_string());
      } else {
        ObString src_str = src_array.at(j)->get_string();
        ObString key_str = key_array.at(j)->get_string();
        char * res_buf = expr.get_str_res_mem(ctx, src_str.length(), j);
        crypt.init(key_str);
        if (OB_FAIL(crypt.decode(src_str, res_buf))) {
          LOG_WARN("fail to decode string", K(ret));
        } else {
          res_datum.at(j)->set_string(res_buf, src_str.length());
        }
      }
    }
  }
  return ret;
}

int ObExprDecode::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_decode;
  rt_expr.eval_batch_func_ = eval_decode_batch;
  return ret;
}

void ObCrypt::init(ObString &key) {
  uint32_t nr=1345345333L, add=7, nr2=0x12345671L;
  if (!key.empty()) {
    uint32_t tmp = 0;
    const char *str_ptr = key.ptr();
    for (int64_t i = 0; i < key.length(); ++i) {
      if (' ' == str_ptr[i] || '\t' == str_ptr[i]) {
        // do nothing
      } else {
        tmp = static_cast<uint32_t>(static_cast<uchar>(str_ptr[i]));
        nr ^= (((nr & 63) + add) * tmp)+ (nr << 8);
        nr2 += (nr2 << 8) ^ nr;
        add += tmp;
      }
    }
  }
  rand_.init(nr & (((uint32_t) 1L << 31) -1L), nr2 & (((uint32_t) 1L << 31) -1L));
  for (int32_t i = 0 ; i <= 255; i++) {
    decode_buff_[i]= i;
  }
  for (int32_t i = 0 ; i<= 255 ; i++)
  {
    uint idx = static_cast<uint>(rand_.get_double() * 255.0);
    uchar a = decode_buff_[idx & 0xff];
    decode_buff_[idx & 0xff] = decode_buff_[i];
    decode_buff_[i] = a;
  }
  for (int32_t i = 0 ; i <= 255 ; i++) {
   encode_buff_[decode_buff_[i]] = i;
  }
  orig_rand_seed_[0] = rand_.seed1_;
  orig_rand_seed_[1] = rand_.seed2_;
  shift_ = 0;
}

int ObCrypt::encode(ObString &src, char *res) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(res));
  } else if (src.empty()) {
    res[0] = 0;
  } else {
    const char *str = src.ptr();
    uint idx = 0;
    for (size_t i = 0; i < src.length(); i++) {
      shift_ ^= static_cast<uint>(rand_.get_double() * 255.0);
      idx = static_cast<uint>(static_cast<uchar>(*str));
      *res = static_cast<char>(encode_buff_[idx & 0xff] ^ shift_);
      shift_ ^= idx;
      str ++;
      res ++;
    }
  }
  return ret;
}

int ObCrypt::decode(ObString &src, char *res) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(res));
  } else if (src.empty()) {
    res[0] = 0;
  } else {
    const char *str = src.ptr();
    uint idx = 0;
    for (size_t i = 0; i < src.length(); i++) {
      shift_ ^= static_cast<uint> (rand_.get_double() * 255.0);
      idx = static_cast<uint> (static_cast<uchar>(*str) ^ shift_);
      *res = decode_buff_[idx & 0xff];
      shift_ ^= static_cast<uint>(static_cast<uchar>(*res));
      str ++;
      res ++;
    }
  }
  return ret;
}

void ObCrypt::reinit() {
  shift_ = 0;
  rand_.seed1_ = orig_rand_seed_[0];
  rand_.seed2_ = orig_rand_seed_[1];
}

}
}
