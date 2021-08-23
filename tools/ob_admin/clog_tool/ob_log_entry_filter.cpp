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

#include <string.h>
#include <stdlib.h>
#include "lib/ob_errno.h"
#include "ob_log_entry_filter.h"

namespace oceanbase {

namespace clog {

int ObLogEntryFilter::parse(const char* str)
{
  int ret = OB_SUCCESS;
  char* ptr1 = NULL;
  char* saveptr1 = NULL;
  char* token1 = NULL;
  char buf[1024];
  char tmp[128];
  const char* TABLE_ID_STR = "table_id";
  const char* PARTITION_ID_STR = "partition_id";
  const char* LOG_ID_STR = "log_id";
  const char* TRANS_ID_STR = "trans_id";
  if (NULL != str) {
    strncpy(buf, str, sizeof(buf));
    for (ptr1 = buf;; ptr1 = NULL) {
      token1 = strtok_r(ptr1, ";", &saveptr1);
      if (NULL == token1) {
        break;
      } else {
        int i = 0;
        char* ptr2 = NULL;
        char* saveptr2 = NULL;
        char* token2 = NULL;
        for (i = 1, ptr2 = token1;; ptr2 = NULL, i++) {
          token2 = strtok_r(ptr2, "=", &saveptr2);
          if (NULL == token2) {
            break;
          } else if (1 == (i % 2)) {
            strncpy(tmp, token2, sizeof(tmp));
          } else {
            if (0 == strcmp(tmp, TABLE_ID_STR)) {
              table_id_ = atol(token2);
              is_table_id_valid_ = true;
            } else if (0 == strcmp(tmp, PARTITION_ID_STR)) {
              partition_id_ = atol(token2);
              is_partition_id_valid_ = true;
            } else if (0 == strcmp(tmp, LOG_ID_STR)) {
              log_id_ = atol(token2);
              is_log_id_valid_ = true;
            } else if (0 == strcmp(tmp, TRANS_ID_STR)) {
              trans_id_ = atol(token2);
              is_trans_id_valid_ = true;
            } else {
              // do nothing
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // end of namespace clog
}  // end of namespace oceanbase
