//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_MEDIUM_LIST_CHECKER_H_
#define OB_STORAGE_COMPACTION_MEDIUM_LIST_CHECKER_H_
#include "/usr/include/stdint.h"
#include "lib/container/ob_iarray.h"
namespace oceanbase
{
namespace compaction
{
struct ObMediumCompactionInfo;
class ObExtraMediumInfo;
struct ObMediumListChecker
{
public:
  typedef common::ObIArray<compaction::ObMediumCompactionInfo*> MediumInfoArray;
  static int validate_medium_info_list(
    const ObExtraMediumInfo &extra_info,
    const MediumInfoArray *medium_info_array,
    const int64_t last_major_snapshot);
  static int check_continue(
    const MediumInfoArray &medium_info_array,
    const int64_t start_check_idx = 0);
  static int check_next_schedule_medium(
    const ObMediumCompactionInfo *next_schedule_info,
    const int64_t last_major_snapshot,
    const bool force_check = true);
private:
  static int check_extra_info(
    const ObExtraMediumInfo &extra_info,
    const int64_t last_major_snapshot);
  static int filter_finish_medium_info(
    const MediumInfoArray &medium_info_array,
    const int64_t last_major_snapshot,
    int64_t &next_medium_info_idx);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_LIST_CHECKER_H_
