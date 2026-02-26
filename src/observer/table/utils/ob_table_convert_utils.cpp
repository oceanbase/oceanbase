/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_convert_utils.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/table/ob_table_context.h"
#include "object/ob_obj_type.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace table
{
// 1. check if obj is varchar or json
// 2. convert obj to json_bin
// 3. set obj to json_bin
int ObTableConvertUtils::convert_to_json_bin(ObIAllocator &allocator,
                                             const ObTableColumnInfo &column_info,
                                             ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObRawExprResType &column_type = column_info.type_;
  const ObCollationType &cs_type = column_type.get_collation_type();
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data version failed", K(ret));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
      || (data_version >= DATA_VERSION_4_4_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("json is not support", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "json in OBKV-Table");
  } else if (ob_is_json(obj.get_type()) || ob_is_null(obj.get_type())) {
    // do nothing
  } else if (!ob_is_varchar(obj.get_type(), cs_type)) {
    ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
    const char *schema_type_str = ob_obj_type_str(column_type.get_type());
    const char *obj_type_str = ob_obj_type_str(obj.get_type());
    LOG_USER_ERROR(OB_KV_COLUMN_TYPE_NOT_MATCH,
                   column_info.column_name_.length(),
                   column_info.column_name_.ptr(),
                   static_cast<int>(strlen(schema_type_str)),
                   schema_type_str,
                   static_cast<int>(strlen(obj_type_str)),
                   obj_type_str);
    LOG_WARN("object type mismatch with column type",
             K(ret),
             K(column_type),
             K(obj),
             K(obj.get_type()));
  } else {
    const ObString &json_str = obj.get_varchar();
    const ObJsonInType in_type = ObJsonInType::JSON_TREE;
    ObIJsonBase *json_bin = NULL;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                 json_str,
                                                 in_type,
                                                 ObJsonInType::JSON_BIN,
                                                 json_bin,
                                                 0,
                                                 ObJsonExprHelper::get_json_max_depth_config()))
        || OB_ISNULL(json_bin)) {
      LOG_WARN("fail to convert to json_bin", K(ret), K(json_str), K(in_type));
    } else {
      obj.set_json_value(ObJsonType, json_bin->get_data(), json_bin->get_data_length());
    }
  }
  return ret;
}

// 1. check if obj is JsonType
// 2. remove lob header
// 3. convert json_bin to json_text
int ObTableConvertUtils::convert_to_json_text(ObIAllocator &allocator, ObObj &json_obj)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data version failed", K(ret));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
      || (data_version >= DATA_VERSION_4_4_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("json is not support ", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "json in OBKV-Table");
  } else if (!ob_is_json(json_obj.get_type())) {
    ret = OB_KV_COLUMN_TYPE_NOT_MATCH;
    LOG_WARN("value is not a json type", K(ret), K(json_obj));
  } else if (json_obj.has_lob_header() && OB_FAIL(ObTableCtx::read_real_lob(allocator, json_obj))) {
    LOG_WARN("fail to read real json value", K(ret), K(json_obj));
  } else {
    const ObString &json_bin = json_obj.get_string();
    ObIJsonBase *json_tree = NULL;
    ObJsonBuffer j_buf(&allocator);
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                 json_bin,
                                                 ObJsonInType::JSON_BIN,
                                                 ObJsonInType::JSON_TREE,
                                                 json_tree))
        || OB_ISNULL(json_tree)) {
      LOG_WARN("fail to build json_base from json_bin", K(ret), K(json_bin));
    } else if (OB_FAIL(
                   json_tree->print(j_buf, /*is_quoted*/ false, json_tree->get_data_length()))) {
      LOG_WARN("fail to print json_text", K(ret), KP(json_tree));
    } else {
      json_obj.set_varchar(j_buf.ptr(), j_buf.length());
      json_obj.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
