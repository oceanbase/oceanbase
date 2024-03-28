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

#ifndef _OB_EXPR_OUTPUT_PACK_H_
#define _OB_EXPR_OUTPUT_PACK_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "common/ob_field.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/mysql/obsm_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace sql
{
class ObOutputPackInfo;
class ObExprOutputPack: public ObExprOperator
{
public:
  ObExprOutputPack();
  explicit ObExprOutputPack(common::ObIAllocator &alloc);
  virtual ~ObExprOutputPack();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_output_pack(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_output_pack_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size);
private:
  static const int64_t MAX_PACK_LEN = 2 * 128 * 1024 * 1024;
  static int encode_cell(const common::ObObj &cell,
                         const common::ObIArray<common::ObField> &param_fields,
                         char *buf, int64_t len, int64_t &pos, char *bitmap, int64_t column_num,
                         ObSQLSessionInfo *session,
                         share::schema::ObSchemaGetterGuard *schema_guard,
                         obmysql::MYSQL_PROTOCOL_TYPE encode_type);
  static int try_encode_row(const ObExpr &expr, ObEvalCtx &ctx, ObSQLSessionInfo *session,
                            common::ObIAllocator &alloc, ObOutputPackInfo *extra_info,
                            char *buffer, char *bitmap,
                            share::schema::ObSchemaGetterGuard *schema_guard,
                            obmysql::MYSQL_PROTOCOL_TYPE encode_type,
                            const int64_t len, int64_t &pos);
  static int reset_bitmap(char *result_buffer, const int64_t len,
                          const int64_t column_num, int64_t &pos, char *&bitmap);
  static int convert_string_value_charset(common::ObObj &value,
                                          common::ObIAllocator &alloc,
                                          const ObSQLSessionInfo &my_session);
  static int convert_lob_value_charset(common::ObObj &value,
                                       common::ObIAllocator &alloc,
                                       const ObSQLSessionInfo &my_session);
  static int convert_lob_locator_to_longtext(common::ObObj &value,
                                             common::ObIAllocator &alloc,
                                             const ObSQLSessionInfo &my_session);
  static int convert_text_value_charset(common::ObObj& value,
                                        bool res_has_lob_header,
                                        common::ObIAllocator &alloc,
                                        const ObSQLSessionInfo &my_session,
                                        sql::ObExecContext &exec_ctx);
  static int process_lob_locator_results(common::ObObj& value,
                                         common::ObIAllocator &alloc,
                                         const ObSQLSessionInfo &my_session,
                                         sql::ObExecContext &exec_ctx);

  static int convert_string_charset(const common::ObString &in_str,
                                    const ObCollationType in_cs_type,
                                    const ObCollationType out_cs_type,
                                    char *buf, int32_t buf_len, uint32_t &result_len);
  static int process_oneline(const ObExpr &expr, ObEvalCtx &ctx, ObSQLSessionInfo *session,
                             common::ObIAllocator &alloc, ObOutputPackInfo *extra_info,
                             share::schema::ObSchemaGetterGuard *schema_guard, ObDatum &expr_datum);
  DISALLOW_COPY_AND_ASSIGN(ObExprOutputPack);
};

struct ObOutputPackInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObOutputPackInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      param_fields_(&alloc)
  {}

  virtual ~ObOutputPackInfo() { param_fields_.destroy(); }

  //virtual int serialize(char *buf, const int64_t len, int64_t &pos) const;
  //virtual int deserialize(const char *buf, const int64_t len, int64_t &pos);
  //virtual int64_t get_serialize_size() const;

  static int init_output_pack_info(uint64_t extra,
                                   common::ObIAllocator *allocator,
                                   ObExpr &expr,
                                   const ObExprOperatorType type);

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  TO_STRING_KV(K_(param_fields));

  common::ObFixedArray<common::ObField, common::ObIAllocator> param_fields_;
};

}
}
#endif  /* _OB_EXPR_OUTPUT_PACK_H_ */
