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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_ENCRYPT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_ENCRYPT_H_
//#include <openssl/sha.h>
#include <openssl/opensslv.h>
#include <openssl/des.h>
#include "lib/random/ob_mysql_random.h"
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
struct DesKeyBlock
{
  DES_cblock key1, key2, key3;
};

struct DesKeySchedule
{
  DES_key_schedule ks1, ks2, ks3;
};

class ObExprDesEncrypt : public ObFuncExprOperator
{
public:
  ObExprDesEncrypt();
  explicit ObExprDesEncrypt(common::ObIAllocator& alloc);
  virtual ~ObExprDesEncrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_des_encrypt_with_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_des_encrypt_batch_with_key(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_des_encrypt_with_default(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_des_encrypt_batch_with_default(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int ob_des_encrypt(ObEvalCtx &ctx, const ObString &src, struct DesKeySchedule &keyschedule, uint key_number, char *res_buf);
  DISALLOW_COPY_AND_ASSIGN(ObExprDesEncrypt);
};

class ObExprDesDecrypt : public ObFuncExprOperator
{
public:
  ObExprDesDecrypt();
  explicit ObExprDesDecrypt(common::ObIAllocator& alloc);
  virtual ~ObExprDesDecrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_des_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_des_decrypt_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDesDecrypt);
};

class ObExprEncrypt : public ObFuncExprOperator
{
public:
  ObExprEncrypt();
  explicit ObExprEncrypt(common::ObIAllocator& alloc);
  virtual ~ObExprEncrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_encrypt_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static inline char bin_to_ascii(int64_t c);
  static inline bool check_key_valid(char salt0, char salt1);
  DISALLOW_COPY_AND_ASSIGN(ObExprEncrypt);
};

class ObCrypt {
public:
  void init(ObString &key);
  int encode(ObString &str, char *res);
  int decode(ObString &str, char *res);
  void reinit();
private:
  common::ObMysqlRandom rand_, orig_rand_;
  uint64_t orig_rand_seed_[2];
  uchar decode_buff_[256];
  uchar encode_buff_[256];
  uint shift_;
  const int32_t max_value_ = 0x3FFFFFFFL;
};

class ObExprEncode : public ObFuncExprOperator
{
public:
  ObExprEncode();
  explicit ObExprEncode(common::ObIAllocator& alloc);
  virtual ~ObExprEncode();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_encode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_encode_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEncode);
};

class ObExprDecode : public ObFuncExprOperator
{
public:
  ObExprDecode();
  explicit ObExprDecode(common::ObIAllocator& alloc);
  virtual ~ObExprDecode();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_decode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_decode_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDecode);
};

}
}





#endif
