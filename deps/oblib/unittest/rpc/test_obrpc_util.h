/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
