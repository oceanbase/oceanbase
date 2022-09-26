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

// This file is for define of func json expr helper
#ifndef OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_

#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"

namespace oceanbase
{
namespace sql
{
class ObJsonExprHelper final
{
  class ObJsonPathCacheCtx : public ObExprOperatorCtx
  {
  public:
    ObJsonPathCacheCtx(common::ObIAllocator *allocator)
      : ObExprOperatorCtx(),
        path_cache_(allocator)
    {}
    virtual ~ObJsonPathCacheCtx() {}
    ObJsonPathCache *get_path_cache() { return &path_cache_; }
  private:
    ObJsonPathCache path_cache_;
  };
public:
  /*
  get json doc to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[out] is_null    the flag for null situation
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObArenaAllocator &allocator,
                          uint16_t index, ObIJsonBase*& j_base,
                          bool &is_null, bool need_to_tree=true);

  /*
  get json doc to JsonBase in old_typing_engine
  @param[in]  objs       the input arguments
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[out] is_null    the flag for null situation
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_doc(const ObObj *objs, common::ObIAllocator *allocator,
                          uint16_t index, ObIJsonBase*& j_base,
                          bool &is_null, bool need_to_tree = true);

  /*
  get json value to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[in]  to_bin     expect value is binary or not
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_val(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObIAllocator *allocator, uint16_t index,
                          ObIJsonBase*& j_base, bool to_bin = false);

  /*
  get json value to JsonBase in old_typing_engine
  @param[in]  data       the input argument
  @param[in]  is_bool    whether input data is JsonBoolean
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  to_bin     expect value is binary or not
  @param[out] j_base     the pointer to JsonBase
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_val(const common::ObObj &data, ObExprCtx &ctx,
                          bool is_bool, common::ObIAllocator *allocator,
                          ObIJsonBase*& j_base, bool to_bin = false);
 
  /*
  replace json_old with json_new in json_doc
  @param[in]  json_old   the old json node to be replaced
  @param[in]  json_new   the new json node to replace
  @param[in]  json_doc   the root json node of old json node
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int json_base_replace(ObIJsonBase *json_old, ObIJsonBase *json_new,
                               ObIJsonBase *&json_doc);

  static int find_and_add_cache(ObJsonPathCache* path_cache, ObJsonPath*& res_path,
                                ObString& path_str, int arg_idx, bool enable_wildcard);

  static ObJsonPathCache* get_path_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx);

  static int is_json_zero(const ObString& data, int& result);

  /*
  try to transfrom scalar data to jsonBase
  @param[in]  datum          the input datum
  @param[in]  type           the type of input argument
  @param[in]  allocator      the Allocator in context
  @param[in]  scale          the scale for json decimal
  @param[in]  ObTimeZoneInfo the timeZone info
  @param[out] j_base        the pointer to ObIJsonBase
  @param[in]  to_bin         whether convert to binary
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  template <typename T>
  static int transform_scalar_2jsonBase(const T &datum,
                                        ObObjType type,
                                        common::ObIAllocator *allocator,
                                        ObScale scale,
                                        const ObTimeZoneInfo *tz_info,
                                        ObIJsonBase*& j_base,
                                        bool to_bin)
  {
    int ret = OB_SUCCESS;
    void *buf = NULL;
    ObIJsonBase *json_node = NULL;

    switch (type) {
      case ObTinyIntType: {
        // mysql boolean type
        buf = allocator->alloc(sizeof(ObJsonInt));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonInt *)new (buf) ObJsonInt(datum.get_int());
        }
        break;
      }
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        buf = allocator->alloc(sizeof(ObJsonInt));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonInt *)new (buf) ObJsonInt(datum.get_int());
        }
        break;
      }
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        buf = allocator->alloc(sizeof(ObJsonUint));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonInt *)new (buf) ObJsonUint(datum.get_uint64());
        }
        break;
      }
      case ObDateTimeType:
      case ObTimestampType:
      case ObDateType:
      case ObTimeType: {
        ObTime ob_time;
        int64_t value = 0;
        ObJsonNodeType node_type;
        if (type == ObDateType) {
          node_type = ObJsonNodeType::J_DATE;
          value = datum.get_date();
          ob_time.mode_ = DT_TYPE_DATE;
          if (OB_FAIL(ObTimeConverter::date_to_ob_time(value, ob_time))) {}
        } else if (type == ObTimeType) {
          node_type = ObJsonNodeType::J_TIME;
          value = datum.get_time();
          ob_time.mode_ = DT_TYPE_TIME;
          if (OB_FAIL(ObTimeConverter::time_to_ob_time(value, ob_time))) {}
        } else if (type == ObDateTimeType) {
          node_type = ObJsonNodeType::J_DATETIME;
          value = datum.get_datetime();
          ob_time.mode_ = DT_TYPE_DATETIME;
          if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, tz_info, ob_time))) {}
        } else {
          node_type = ObJsonNodeType::J_TIMESTAMP;
          value = datum.get_timestamp();
          ob_time.mode_ = DT_TYPE_DATETIME;
          if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, tz_info, ob_time))) {}
        }

        if (OB_SUCC(ret)) {
          buf = allocator->alloc(sizeof(ObJsonDatetime));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            json_node = (ObJsonDatetime *)new (buf) ObJsonDatetime(node_type, ob_time);
          }
        }
        break;
      }
      case ObYearType: {
        uint8_t in_val = datum.get_year();
        int64_t full_year = 0;
        if (OB_FAIL(ObTimeConverter::year_to_int(in_val, full_year))) {
        } else {
          buf = allocator->alloc(sizeof(ObJsonInt));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            json_node = (ObJsonInt*)new(buf)ObJsonInt(full_year);
          }
        }
        break;
      }
      case ObFloatType:
      case ObDoubleType:
      case ObUFloatType:
      case ObUDoubleType: {
        buf = allocator->alloc(sizeof(ObJsonDouble));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonDouble *)new (buf) ObJsonDouble(datum.get_double());
        }
        break;
      }

      case ObUNumberType:
      case ObNumberType: {
        // won't waster much memory, do deep copy num
        number::ObNumber num;
        buf = allocator->alloc(sizeof(ObJsonDecimal));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(num.deep_copy_v3(datum.get_number(), *allocator))) {
          ;
        } else {
          // shadow copy
          json_node = (ObJsonDecimal *)new (buf) ObJsonDecimal(num, -1, scale);
        }
        break;
      }
      case ObHexStringType: {
        buf = allocator->alloc(sizeof(ObJsonOpaque));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonOpaque *)new (buf) ObJsonOpaque(datum.get_string(), type);
        }
        break;
      }
      case ObBitType: {
        const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
        int64_t pos = 0;
        char *bbuf = static_cast<char*>(allocator->alloc(BUF_LEN));
        if (OB_ISNULL(bbuf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          uint64_t in_val = datum.get_uint64();
          if (OB_FAIL(bit_to_char_array(in_val, scale, bbuf, BUF_LEN, pos))) {
          } else {
            common::ObString j_value(pos, bbuf);
            buf = allocator->alloc(sizeof(ObJsonOpaque));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              json_node = (ObJsonOpaque *)new(buf)ObJsonOpaque(j_value, type);
            }
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
      }
    }

    if (OB_SUCC(ret)) {
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = json_node;
      }
    }
  
    return ret;
  }
  /*
  try to transfrom from type which is_convertible_to_json to jsonBase
  @param[in]  datum          the input datum
  @param[in]  type           the type of input argument
  @param[in]  allocator      the Allocator in context
  @param[in]  cs_type        the collation type
  @param[out] j_base         the pointer to jsonBase
  @param[in]  to_bin         whether convert to binary
  @param[in]  deep_copy      whether deep copy input string from datum
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  template <typename T>
  static int transform_convertible_2jsonBase(const T &datum,
                                             ObObjType type,
                                             common::ObIAllocator *allocator,
                                             ObCollationType cs_type,
                                             ObIJsonBase*& j_base,
                                             bool to_bin,
                                             bool deep_copy = false)
  {
    int ret = OB_SUCCESS;
    void *buf = NULL;
    ObIJsonBase *json_node = NULL;

    switch (type) {
      case ObNullType: {
        buf = allocator->alloc(sizeof(ObJsonNull));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          json_node = (ObJsonNull *)new (buf) ObJsonNull();
        }
        break;
      }
      case ObVarcharType:
      case ObCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        ObString value;
        if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
          // should check collation first
          LOG_WARN("Invalid collation type for input string.", K(ret));
        } else if (deep_copy) {
          ret = deep_copy_ob_string(*allocator, datum.get_string(), value);
        } else {
          value = datum.get_string();
        }
  
        if (OB_SUCC(ret)) {
          uint64_t len = value.length();
          const char *ptr = value.ptr();
          buf = allocator->alloc(sizeof(ObJsonString));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            json_node = (ObJsonString *)new (buf) ObJsonString(ptr, len);
          }
        }

        break;
      }
      case ObJsonType: {
        ObString value;
        if (deep_copy) {
          if (OB_FAIL(deep_copy_ob_string(*allocator, datum.get_string(), value))) {}
        } else {
          value = datum.get_string();
        }
        if (OB_SUCC(ret)) {
          ObJsonInType to_type = to_bin ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, value, ObJsonInType::JSON_BIN, to_type, json_node))) {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
            LOG_WARN("fail to get json base", K(ret));
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
      }
    }

    if (OB_SUCC(ret)) {
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = json_node;
      }
    }

    return ret;
  }

  static bool is_convertible_to_json(ObObjType &type);
  static int is_valid_for_json(ObExprResType* types_stack, uint32_t index, const char* func_name);
  static int is_valid_for_json(ObExprResType& type, uint32_t index, const char* func_name);
  static int is_valid_for_path(ObExprResType* types_stack, uint32_t index);
  static void set_type_for_value(ObExprResType* types_stack, uint32_t index);
  static int ensure_collation(ObObjType type, ObCollationType cs_type);
  static ObJsonInType get_json_internal_type(ObObjType type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonExprHelper);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_