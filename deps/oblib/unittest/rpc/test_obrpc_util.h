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

#ifndef OCEANBASE_TEST_OBRPC_UTIL_H_
#define OCEANBASE_TEST_OBRPC_UTIL_H_
struct GconfDummpy {
  struct Dummpy {
    static oceanbase::ObString get_value_string() {
      return oceanbase::ObString();
    }
  };
  Dummpy _ob_ssl_invited_nodes;
  static oceanbase::ObAddr self_addr() {
    return oceanbase::ObAddr();
  }
};
GconfDummpy GCONF;
GconfDummpy GCTX;

#endif
