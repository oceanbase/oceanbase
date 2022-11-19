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

#include "ob_rs_test_utils.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

namespace oceanbase
{
namespace rootserver
{
void ob_parse_case_file(ObArenaAllocator &allocator, const char* case_file, json::Value *&root)
{
  json::Parser json_parser;
  ASSERT_EQ(OB_SUCCESS, json_parser.init(&allocator));
  FILE *fp = fopen(case_file, "r");
  ASSERT_TRUE(NULL != fp);
  char *content = NULL;
  size_t len = 0;
  ssize_t bytes_read = getdelim(&content, &len, '\0', fp);
  ASSERT_NE(-1, bytes_read);
  char *content_clone = (char*)allocator.alloc(len);
  ASSERT_TRUE(NULL != content_clone);
  memcpy(content_clone, content, len);
  root = NULL;
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(content_clone, len, root));

  if (NULL != content) {
    free(content);
  }
  if (NULL != fp)
  {
    fclose(fp);
  }
}

void ob_check_result(const char* base_dir, const char* casename)
{
  char output_file[512];
  snprintf(output_file, 512, "%s%s.tmp", base_dir, casename);
  char result_file[512];
  snprintf(result_file, 512, "%s%s.result", base_dir, casename);
  char cmd[512];
  snprintf(cmd, 512, "diff %s %s", result_file, output_file);
  printf("check results using\n%s\n", cmd);
  int ret = system(cmd);
  if (0 != ret) {
    printf("result file mismatch\n");
    printf("You can overwrite the old result using\n  cp %s %s\n", output_file, result_file);
  } else {
    printf("PASS\n");
  }
  ASSERT_EQ(0, ret);
}

} // end namespace rootserver
} // end namespace oceanbase
