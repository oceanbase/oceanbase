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

#include "io/easy_io.h"
#include <easy_test.h>
#include "util/easy_time.h"
#include "packet/easy_kfc_handler.h"
#include <sys/types.h>
#include <sys/wait.h>

///////////////////////////////////////////////////////////////////////////////////////////////////
TEST(easy_kfc_handler, easy_kfc_set_iplist)
{
  char* config = "10.1[7-8][1-3,985-5].4.1[1-3,5]6 role=server group=group1 port=80";
  easy_kfc_t* kfc = easy_kfc_create(config, 0);

  if (kfc) {
    easy_kfc_destroy(kfc);
  }
}
