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

#ifndef OCEANBASE_SHARE_OB_QUERY_TRANSLATOR_H_
#define OCEANBASE_SHARE_OB_QUERY_TRANSLATOR_H_

#include "ob_query_request.h"

namespace oceanbase
{
namespace share
{

class ObRequestTranslator
{
public :
  ObRequestTranslator(char *buf, int64_t buf_len, int64_t *pos, ObReqFromJson *req)
    : buf_(buf), buf_len_(buf_len), pos_(pos), req_(req), print_params_() {}

  int translate_from();
  int translate_table(const ObReqTable *table);
  // translate condition_items_ to string
  int translate_where();
  int translate_approx();
  int translate_limit();
  int translate_hints();
  char *buf_;
  int64_t buf_len_;
  int64_t *pos_;
  const ObReqFromJson *req_;
  ObObjPrintParams print_params_;
};


class ObQueryTranslator : public ObRequestTranslator
{
public :
  ObQueryTranslator(char *buf, int64_t buf_len, int64_t *pos, ObReqFromJson *req) :
    ObRequestTranslator(buf, buf_len, pos, req) {}
  int translate();
  int translate_select();
  // translate group_items_ to string
  int translate_group_by();
  // translate having_items_ to string
  int translate_having();
  // translate order_items_ to string
  int translate_order_by();
  int translate_order(const OrderInfo *order_info);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_QUERY_TRANSLATOR_H_