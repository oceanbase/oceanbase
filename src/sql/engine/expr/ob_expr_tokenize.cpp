/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/ob_json_access_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_tokenize.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_struct.h"

namespace oceanbase
{
namespace sql
{
ObExprTokenize::ObExprTokenize(common::ObIAllocator &alloc)
    : ObStringExprOperator(alloc,
                           T_FUN_TOKENIZE,
                           N_TOKENIZE,
                           MORE_THAN_ZERO,
                           VALID_FOR_GENERATED_COL)
{
}

ObExprTokenize::~ObExprTokenize() {}

int ObExprTokenize::eval_tokenize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  ObIJsonBase *json_result = nullptr;
  TokenizeParam param;
  // ObTokenizeResult result;
  int64_t doc_len = 0;
  ObFTWordMap token_map;

  // check param num, which is checked in ObExprOperator::calc_result_typeN.
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Args count invalid.", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(parse_param(expr, ctx, temp_allocator, param))) {
    LOG_WARN("Fail to parse param", K(ret));
  } else if (OB_FAIL(tokenize_fulltext(param, param.output_mode_, temp_allocator, json_result))) {
    LOG_WARN("Fail to tokenize fulltext", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr,
                                                     ctx,
                                                     temp_allocator,
                                                     json_result,
                                                     expr_datum))) {
    LOG_WARN("fail to pack json result", K(ret));
  }

  return ret;
}

int ObExprTokenize::tokenize_fulltext(const TokenizeParam &param,
                                      TokenizeParam::OUTPUT_MODE mode,
                                      ObIAllocator &allocator,
                                      ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  storage::ObFTParseHelper tokenize_helper;
  const int64_t ft_word_bkt_cnt = MAX(param.fulltext_.length() / 10, 2);
  char *parser_name_buf = nullptr;
  int64_t doc_len = 0;
  ObFTWordMap token_map;

  if (TokenizeParam::OUTPUT_MODE::DEFAULT != mode && TokenizeParam::OUTPUT_MODE::ALL != mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid output mode", K(ret), K(mode));
  } else if (param.parser_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Empty parser name", K(ret), K(param.parser_name_));
  } else if (OB_ISNULL(parser_name_buf
                       = static_cast<char *>(allocator.alloc(OB_PLUGIN_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for ft_parser_name", K(ret));
  } else if (OB_FAIL(construct_ft_parser_inner_name(param.parser_name_,
                                                    parser_name_buf,
                                                    OB_PLUGIN_NAME_LENGTH))) {
    LOG_WARN("Fail to construct ft parser name", K(ret));
  } else if (OB_FAIL(tokenize_helper.init(&allocator, ObString::make_string(parser_name_buf)))) {
    LOG_WARN("Fail to init tokenize helper", K(ret));
  } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
    LOG_WARN("Fail to create token map", K(ret));
  } else if ((0 != param.fulltext_.length())
             && OB_FAIL(tokenize_helper.segment(param.cs_type_,
                                                param.fulltext_.ptr(),
                                                param.fulltext_.length(),
                                                doc_len,
                                                token_map))) {
    LOG_WARN("Fail to segment fulltext", K(ret));
  } else {
    switch (param.output_mode_) {
    case TokenizeParam::OUTPUT_MODE::DEFAULT: {
      if (OB_FAIL(tokenize_helper.make_token_array_json(token_map, result))) {
        LOG_WARN("Fail to construct json array", K(ret));
      } else {
        // pass
      }
      break;
    }
    case TokenizeParam::OUTPUT_MODE::ALL: {
      if (OB_FAIL(tokenize_helper.make_detail_json(token_map, doc_len, result))) {
        LOG_WARN("Fail to construct detaild json", K(ret));
      } else {
        // pass
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid output mode", K(ret), K(param.output_mode_));
    }
  }
  return ret;
}

ObExprTokenize::TokenizeParam ::TokenizeParam()
    : parser_name_(ObString(OB_DEFAULT_FULLTEXT_PARSER_NAME)), cs_type_(CS_TYPE_INVALID),
      fulltext_(), output_mode_(OUTPUT_MODE::DEFAULT)
{
}

int ObExprTokenize::TokenizeParam::parse_json_param(const ObIJsonBase *obj)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObIJsonBase *val;
  bool is_empty = false;
  if (OB_UNLIKELY(nullptr == obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Json param is null.", K(ret));
  } else if (ObJsonNodeType::J_OBJECT != obj->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Json args should be an object", K(ret));
  } else if (obj->element_count() == 0) {
    is_empty = true;
  } else if (OB_FAIL(obj->get_object_value(0, str, val))) {
    LOG_WARN("Failed to take para key from json object.", K(ret));
  }

  if (!is_empty && OB_SUCC(ret)) {
    if (0 == str.case_compare("CASE")) {
      if (ObJsonNodeType::J_STRING != val->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Json argument invalid", K(ret));
      } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("UPPER")) {
      } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("LOWER")) {
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Case indentifier not valid", K(ret));
      }
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "case indentifier");
    } else if (0 == str.case_compare("OUTPUT")) {
      if (ObJsonNodeType::J_STRING != val->json_type()) {
        LOG_WARN("Json argument invalid", K(ret));
        ret = OB_INVALID_ARGUMENT;
      } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("DEFAULT")) {
        output_mode_ = DEFAULT;
      } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("ALL")) {
        output_mode_ = ALL;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "output mode");
      }
    } else if (0 == str.case_compare("STOPWORDS")) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stopwords");
    } else if (0 == str.case_compare("ADDITIONAL-ARGS")) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "additional arguments");
    } else {
      LOG_WARN("Unsupported parameter", K(ret), K(str));
      ret = OB_INVALID_ARGUMENT;
    }
  }

  return ret;
}

int ObExprTokenize::parse_param(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                common::ObArenaAllocator &allocator,
                                TokenizeParam &param)
{
  int ret = OB_SUCCESS;

  ObDatum *fulltext_datum;
  ObDatum *parser_datum;
  ObDatum *parser_params_datum;

  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Args count invalid.", K(ret), K(expr.arg_cnt_));
  } else {
    if (OB_FAIL(expr.args_[0]->eval(ctx, fulltext_datum))) {
      LOG_WARN("Fail to eval fulltext.", K(ret));
    } else {
      if (fulltext_datum->is_null()) {
        // do nothing, return empty result
        param.fulltext_ = ObString::make_empty_string();
      } else {
        param.fulltext_ = fulltext_datum->get_string();
      }
      param.cs_type_ = expr.args_[0]->datum_meta_.cs_type_;

      if (OB_SUCC(ret) && expr.arg_cnt_ >= 2) {
        if (OB_FAIL(expr.args_[1]->eval(ctx, parser_datum))) {
          LOG_WARN("Fail to eval parser name.", K(ret));
        } else {
          if (parser_datum->is_null()) {
            param.parser_name_ = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
          } else {
            ObString name = parser_datum->get_string();
            param.parser_name_ = name.trim();
          }
        }
      } else {
      }

      if (OB_SUCC(ret) && expr.arg_cnt_ >= 3) {
        ObIJsonBase *base = nullptr;
        bool is_null = false;
        if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 2, base, is_null))) {
          LOG_WARN("Fail to get json doc", K(ret));
        } else {
          if (ObJsonNodeType::J_ARRAY != base->json_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid json type", K(ret));
          } else {
            for (uint64_t i = 0; OB_SUCC(ret) && i < base->element_count(); ++i) {
              ObIJsonBase *node = nullptr;
              if (OB_FAIL(base->get_array_element(i, node))) {
                LOG_WARN("Failed to get array element", K(ret));
                break;
              } else if (ObJsonNodeType::J_OBJECT != (node->json_type())) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("Argument of json array invalid", K(ret));
                break;
              } else if (OB_FAIL(param.parse_json_param(node))) {
                LOG_WARN("Failed to parse json object", K(ret));
                break;
              } else {
                // pass
              }
            } // for
          }
        }
      } else {
      }
    }
  }
  return ret;
}

int ObExprTokenize::construct_ft_parser_inner_name(const ObString &input_str,
                                                   char *&name_buf,
                                                   int64_t name_buf_size)
{
  int ret = OB_SUCCESS;
  // make an extract parser name
  share::ObPluginName plugin_name;
  storage::ObFTParser parser;

  if (OB_FAIL(plugin_name.set_name(input_str))) {
    LOG_WARN("Fail to set plugin name", K(ret));
  } else if (OB_FAIL(storage::OB_FT_PLUGIN_MGR.get_ft_parser(plugin_name, parser))) {
    LOG_WARN("Fail to get ft parser", K(ret));
  } else if (OB_FAIL(parser.serialize_to_str(name_buf, OB_PLUGIN_NAME_LENGTH))) {
    LOG_WARN("Fail to parse ft parser name", K(ret));
  }
  return ret;
}

int ObExprTokenize::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(param_num < 1 || param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString expr_name(N_TOKENIZE);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, expr_name.length(), expr_name.ptr());
  } else if (lib::is_oracle_mode()) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    // just okay
  }

  ObLength length = ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length();

  if (OB_SUCC(ret)) {
    // set res type
    type.set_json();
    type.set_length(length); // keep consistent with other json expr, maybe calc it later.

    // param type set, skip charset after first param
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (ob_is_string_type(types[i].get_type())) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

    // handle param
    if (param_num >= 2) {
      types[1].set_varchar();
    }

    if (param_num >= 3) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, 2, N_TOKENIZE))) {
        LOG_WARN("wrong type for json doc.", K(ret), K(types[2].get_type()));
      }
    }
  }

  return ret;
}

int ObExprTokenize::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK((rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 3));
  if (OB_SUCC(ret)) {
    // do register
    rt_expr.eval_func_ = eval_tokenize;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
