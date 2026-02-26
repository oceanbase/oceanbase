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

#define USING_LOG_PREFIX PL
#include "ob_dbms_hybrid_vector_mysql.h"
#include "src/share/hybrid_search/ob_hybrid_search_executor.h"

namespace oceanbase {
namespace pl {
using namespace common;
using namespace sql;

/*
FUNCTION SEARCH (IN table_name VARCHAR(65535),
                 IN search_params LONGTEXT)
RETURN JSON;
*/
int ObDBMSHybridVectorMySql::search(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(params.count() != 2) || !params.at(0).is_varchar() ||
      !params.at(1).is_text()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument types for search", K(ret));
  } else {
    ObString table_name = params.at(0).get_varchar();
    ObString search_params_str;
    if (OB_FAIL(params.at(1).get_string(search_params_str))) {
      LOG_WARN("fail to get search_params_str", K(ret));
    } else {
      oceanbase::share::ObHybridSearchExecutor executor;
      oceanbase::share::ObHybridSearchArg search_arg;
      search_arg.table_name_ = table_name;
      search_arg.search_params_ = search_params_str;
      search_arg.search_type_ = oceanbase::share::ObHybridSearchArg::SearchType::SEARCH;
      if (OB_FAIL(executor.init(ctx, search_arg))) {
        LOG_WARN("fail to init search arg", K(ret));
      } else {
        if (OB_FAIL(executor.execute_search(result))) {
          LOG_WARN("fail to execute hybrid search", K(ret), K(search_arg));
        }
      }
    }
  }
  return ret;
}

/*
FUNCTION GET_SQL (IN table_name VARCHAR(65535),
                   IN search_params LONGTEXT)
RETURN LONGTEXT;
*/
int ObDBMSHybridVectorMySql::get_sql(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(params.count() != 2) || !params.at(0).is_varchar() ||
      !params.at(1).is_text()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument types for get_sql", K(ret));
  } else {
    ObString table_name = params.at(0).get_varchar();
    ObString search_params_str;
    if (OB_FAIL(params.at(1).get_string(search_params_str))) {
      LOG_WARN("fail to get search_params_str", K(ret));
    } else {
      oceanbase::share::ObHybridSearchExecutor executor;
      oceanbase::share::ObHybridSearchArg arg;
      arg.table_name_ = table_name;
      arg.search_params_ = search_params_str;
      arg.search_type_ = oceanbase::share::ObHybridSearchArg::SearchType::GET_SQL;
      if (OB_FAIL(executor.init(ctx, arg))) {
        LOG_WARN("fail to init executor", K(ret));
      } else {
        ObString sql_result;
        if (OB_FAIL(executor.execute_get_sql(sql_result))) {
          LOG_WARN("fail to execute hybrid get_sql", K(ret), K(arg));
        } else {
          ObTextStringResult text_res(ObLongTextType, true, ctx.allocator_);
          if (OB_FAIL(text_res.init(sql_result.length()))) {
            LOG_WARN("Failed to init text res", K(ret), K(sql_result.length()));
          } else if (OB_FAIL(text_res.append(sql_result))) {
            LOG_WARN("Failed to append str to text res", K(ret), K(text_res));
          } else {
            ObString lob_str;
            text_res.get_result_buffer(lob_str);
            OX(result.set_lob_value(ObLongTextType, lob_str.ptr(),
                                    lob_str.length()));
            OX(result.set_has_lob_header());
          }
        }
      }
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase