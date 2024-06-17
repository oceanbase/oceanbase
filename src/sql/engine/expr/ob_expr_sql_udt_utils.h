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
 * This file is for define of expr sql udt utils
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_H_
#define OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_H_

#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

class ObSqlUdtNullBitMap final
{
public:
  ObSqlUdtNullBitMap() : bitmap_(NULL), bitmap_len_(0), ver_(0), pos_(0)
  {};
  ObSqlUdtNullBitMap(char *bitmap, uint32_t bitmap_len) :
    bitmap_(bitmap), bitmap_len_(bitmap_len), ver_(0), pos_(0)
  {};
  ObSqlUdtNullBitMap(ObString &bitmap_str) :
    bitmap_(bitmap_str.ptr()), bitmap_len_(bitmap_str.length()), ver_(0), pos_(0)
  {};
  ~ObSqlUdtNullBitMap(){};

  TO_STRING_KV(KP_(bitmap), K_(bitmap_len), K_(ver), K_(pos));

  bool is_valid();
  void set_bitmap(char *bitmap, uint32_t bitmap_len) {
    bitmap_ = bitmap;
    bitmap_len_ = bitmap_len;
    ver_ = 0;
    pos_ = 0;
  }
  int check_bitmap_pos(uint32_t pos, bool &is_set);
  int set_bitmap_pos(uint32_t pos);
  int reset_bitmap_pos(uint32_t pos);

  int check_current_bitmap_pos(bool &is_set);
  int set_current_bitmap_pos();
  int reset_current_bitmap_pos();
  int assign(ObSqlUdtNullBitMap &src, uint32_t pos, uint32_t bit_len);
  inline uint32_t &get_pos() { return pos_; };
  inline char *&get_bitmap_ptr() { return bitmap_; };
  inline uint32_t get_bitmap_bytelen() { return bitmap_len_; };


private:
  char *bitmap_;
  uint32_t bitmap_len_; // byte len
  uint32_t ver_; // reserved for inheritance
  uint32_t pos_;
};

// wrapper class to handle udt format(sql/pl) convert
class ObSqlUdtUtils final
{
public:
  // functions to flattern nested udt to a single object array
  static int ob_udt_flattern_pl_extend(const ObObj **flattern_objs,
                                       const int32_t &flattern_object_count,
                                       int32_t &pos,
                                       ObSqlUdtNullBitMap &nested_udt_bitmap,
                                       const ObObj *pl_ext_addr,
                                       ObExecContext &exec_context,
                                       ObSqlUDT &sql_udt);

  static int ob_udt_flattern_varray(const ObObj **flattern_objs,
                                    const int32_t &flattern_object_count,
                                    int32_t &pos,
                                    const ObObj *obj,
                                    ObExecContext &exec_context,
                                    ObSqlUDT &sql_udt);

  static int ob_udt_flattern_record(const ObObj **flattern_objs,
                                    const int32_t &flattern_object_count,
                                    int32_t &pos,
                                    ObSqlUdtNullBitMap &nested_udt_bitmap,
                                    const ObObj *obj,
                                    ObExecContext &exec_context,
                                    ObSqlUDT &sql_udt);

  // functions to reorder object array
  static int ob_udt_reordering_leaf_objects(const ObObj **flattern_objs,
                                            const ObObj **sorted_objs,
                                            const int32_t &flattern_object_count,
                                            ObSqlUDT &sql_udt);

  // functions to calc sql udt length (from nested pl extend objects)
  static int ob_udt_calc_total_len(const ObObj **sorted_objs,
                                   const int32_t &flattern_object_count,
                                   int64_t &sql_udt_total_len,
                                   ObSqlUDT &sql_udt);
  static int ob_udt_calc_sql_varray_length(const ObObj *cur_obj,
                                           int64_t &sql_udt_total_len,
                                           bool with_lob_header = true);

  // functions to convert objects (basic type, varray) to sql udt type
  static int ob_udt_convert_pl_varray_to_sql_varray(const ObObj *cur_obj,
                                                    char *buf,
                                                    const int64_t buf_len,
                                                    int64_t &pos,
                                                    bool with_lob_header = true);

  static int ob_udt_convert_sorted_objs_array_to_udf_format(const ObObj **sorted_objs,
                                                            char *buf,
                                                            const int64_t buf_len,
                                                            int64_t &pos,
                                                            ObSqlUDT &sql_udt);

  // functions to convert sql udt to string (only used in text protocal)
  static int covert_sql_udt_varray_to_string(ObStringBuffer &buf,
                                             ObString &udt_varray_buf,
                                             ObSqlUDTMeta &root_meta);

  static int convert_sql_udt_attributes_to_string(ObStringBuffer &buf,
                                                  ObString *attrs,
                                                  int64_t &pos,
                                                  sql::ObExecContext *exec_context,
                                                  ObSqlUDTMeta &root_meta,
                                                  ObSqlUdtNullBitMap &nested_udt_bitmap);

  static int rearrange_sql_udt_record(ObString &sql_udt_data,
                                      ObSqlUDTMeta udt_meta,
                                      common::ObIAllocator *allocator,
                                      ObSqlUdtNullBitMap &nested_udt_bitmap,
                                      ObString *&attrs,
                                      bool &is_null);

  static int convert_sql_udt_to_string(ObObj &sql_udt_obj,
                                       common::ObIAllocator *allocator,
                                       sql::ObExecContext *exec_context,
                                       ObSqlUDT &sql_udt,
                                       ObString &res_str);

  static bool ob_udt_util_check_same_type(ObObjType type1, ObObjType type2)
  {
    return (type1 == type2) || (ob_is_user_defined_type(type1) && ob_is_user_defined_type(type2));
  }

  static int ob_udt_build_nested_udt_bitmap_obj(ObObj &obj, ObSqlUdtNullBitMap &udt_bitmap)
  {
    int ret = OB_SUCCESS;
    if (udt_bitmap.is_valid()) {
      obj.set_hex_string_value(udt_bitmap.get_bitmap_ptr(), udt_bitmap.get_bitmap_bytelen());
    }
    return ret;
  }

  static int cast_pl_varray_to_sql_varray(common::ObIAllocator &res_allocator,
                                          ObString &res,
                                          const ObObj root_obj,
                                          bool with_lob_header = true);

  static int cast_pl_record_to_sql_record(common::ObIAllocator &tmp_allocator,
                                          common::ObIAllocator &res_allocator,
                                          sql::ObExecContext *exec_ctx,
                                          ObString &res,
                                          ObSqlUDT &sql_udt,
                                          const ObObj &root_obj);

  static int build_empty_record(sql::ObExecContext *exec_ctx, ObObj &result, uint64_t udt_id);
  static int cast_sql_udt_varray_to_pl_varray(sql::ObExecContext *exec_ctx,
                                              ObString &udt_varray_buf,
                                              ObSqlUDTMeta &udt_meta,
                                              ObObj &res_obj);

  static int cast_sql_udt_attributes_to_pl_record(sql::ObExecContext *exec_ctx,
                                                  ObString *attrs,
                                                  int64_t &pos,
                                                  ObSqlUDTMeta &udt_meta,
                                                  ObSqlUdtNullBitMap &nest_udt_bitmap,
                                                  ObObj &res_obj);

  static int cast_sql_record_to_pl_record(sql::ObExecContext *exec_ctx,
                                          ObObj &result,
                                          ObString &udt_data,
                                          ObSqlUDTMeta &udt_meta);

  static int get_sqludt_meta_by_subschema_id(sql::ObExecContext *exec_ctx,
                                             const uint16_t subschema_id,
                                             ObSqlUDTMeta &udt_meta);
};

class ObSqlUdtMetaUtils final
{

public:
  static int get_udt_meta_attr_info(ObSchemaGetterGuard *schema_guard,
                                    uint64_t tenant_id,
                                    const ObUDTTypeInfo *parent_udt_info,
                                    uint32_t &nested_udt_number,
                                    common::ObIArray<ObUDTTypeAttr *> &leaf_attr_meta);

  static int fill_udt_meta_attr_info(ObSchemaGetterGuard *schema_guard,
                                     ObSubSchemaCtx *subschema_ctx,
                                     uint64_t tenant_id,
                                     const common::ObIArray<ObUDTTypeAttr *> &src,
                                     ObSqlUDTAttrMeta *dst,
                                     const uint32 dst_cnt,
                                     bool is_leaf);

  // get udttypeinfo from schema by tenant id and udt id, get child udt defines recursively
  static int generate_udt_meta_from_schema(ObSchemaGetterGuard *schema_guard,
                                           ObSubSchemaCtx *subschema_ctx,
                                           common::ObIAllocator &allocator,
                                           uint64_t tenant_id,
                                           uint64_t udt_id,
                                           ObSqlUDTMeta &udt_meta);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_H_