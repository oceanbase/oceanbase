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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_dbms_crypto.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_encryption_util.h"
#include "ob_expr_extract.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

#define ENCRYPT_PAD_PKCS5 4096

#define ENCRYPT_CHAIN_CBC 256
#define ENCRYPT_CHAIN_ECB 768

#define ENCRYPT_AES128_MACRO_IN_DBMS_CRYPTO 6
#define ENCRYPT_AES192_MACRO_IN_DBMS_CRYPTO 7
#define ENCRYPT_AES256_MACRO_IN_DBMS_CRYPTO 8


namespace oceanbase
{
namespace sql
{

ObExprDbmsCryptoEncrypt::ObExprDbmsCryptoEncrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUC_SYS_DBMS_CRYPTO_ENCRYPT, 
                         N_DBMS_CRYPTO_ENCRYPT,
                         MORE_THAN_TWO,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         false,
                         INTERNAL_IN_ORACLE_MODE)
{
}
ObExprDbmsCryptoEncrypt::~ObExprDbmsCryptoEncrypt() {}

int ObExprDbmsCryptoEncrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack, 
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{ 
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;  
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num != 4)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    types_stack[0].set_calc_type(ObRawType);
    types_stack[1].set_calc_type(ObIntType);
    types_stack[2].set_calc_type(ObRawType);
    types_stack[3].set_calc_type(ObRawType);
    type.set_raw();
    type.set_collation_level(CS_LEVEL_NUMERIC);
  }
  return ret;
}

//---------------------------------------分割线
ObExprDbmsCryptoDecrypt::ObExprDbmsCryptoDecrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUC_SYS_DBMS_CRYPTO_DECRYPT, 
                         N_DBMS_CRYPTO_DECRYPT,
                         MORE_THAN_TWO,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         false,
                         INTERNAL_IN_ORACLE_MODE)
{
}
ObExprDbmsCryptoDecrypt::~ObExprDbmsCryptoDecrypt() {}

int ObExprDbmsCryptoDecrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num != 4)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    types_stack[0].set_calc_type(ObRawType);
    types_stack[1].set_calc_type(ObIntType);
    types_stack[2].set_calc_type(ObRawType);
    types_stack[3].set_calc_type(ObRawType);
    type.set_raw();
    type.set_collation_level(CS_LEVEL_NUMERIC);
  }
  return ret;
}

}
}
