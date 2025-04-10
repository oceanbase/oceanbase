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

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_util.h"
#include <vector>
#include <regex>

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
using namespace common;
using namespace share;

int ObTableLoadBackupUtil::get_column_ids_from_create_table_sql(const ObString &sql, ObIArray<int64_t> &column_ids)
{
  int ret = OB_SUCCESS;

  //split to lines
  ObArray<char *> lines;
  ObArenaAllocator allocator;
  lines.set_tenant_id(MTL_ID());
  allocator.set_tenant_id(MTL_ID());
  char *sql_str = nullptr;
  if (OB_ISNULL(sql_str = static_cast<char *>(allocator.alloc(sql.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    memcpy(sql_str, sql.ptr(), sql.length());
    sql_str[sql.length()] = '\0';
    char *save_ptr = nullptr;
    char *token = strtok_r(sql_str, "\n", &save_ptr);
    while (OB_SUCC(ret) && token != NULL) {
      if (OB_FAIL(lines.push_back(token))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        token = strtok_r(NULL, "\n", &save_ptr);
      }
    }
  }

  // get column lines and pk
  std::vector<std::string> column_lines;
  char *pk = nullptr;
  if (OB_SUCC(ret)) {
    for (int64_t i = 1; i < lines.count(); i ++) {
      char *pos = strcasestr(lines[i], "primary key");
      char *comment = strcasestr(lines[i], "comment ");
      if (pos != nullptr && comment == nullptr) {
        pk = lines[i];
        break;
      } else {
        column_lines.push_back(lines[i]);
      }
    }
  }

  // regex search column_ids and pk
  if (OB_SUCC(ret)) {
    std::vector<std::string> pks;
    if (pk != nullptr) {
      std::smatch m;
      std::string cur = pk;
      while (OB_SUCC(ret) && std::regex_search(cur, m, std::regex("`([^`]+)`"))) {
        pks.push_back(m[1].str());
        cur = m.suffix();
      }
    }

    std::vector<std::pair<std::string, int64_t>> column_defs;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_lines.size(); i ++) {
      std::smatch m;
      if (std::regex_search(column_lines[i], m, std::regex("`([^`]+)`"))) {
        std::string column_name = m[1].str();
        if (std::regex_search(column_lines[i], m, std::regex("id ([0-9]+)"))) {
          std::string id_str = m[1].str();
          char *endstr = nullptr;
          int64_t id = strtoll(id_str.c_str(), &endstr, 10);
          column_defs.push_back(std::make_pair(column_name, id));
        }
      }
    }

    // put pk first
    for (int64_t i = 0; OB_SUCC(ret) && i < pks.size(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < column_defs.size(); j++) {
        if (pks[i] == column_defs[j].first) {
          if (OB_FAIL(column_ids.push_back(column_defs[j].second))) {
            LOG_WARN("fail to push back", KR(ret));
          }
          break;
        }
      }
    }

    // add remain columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_defs.size(); i++) {
      bool flag = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); j++) {
        if (column_ids.at(j) == column_defs[i].second) {
          flag = true;
          break;
        }
      }
      if (!flag) {
        if (OB_FAIL(column_ids.push_back(column_defs[i].second))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
