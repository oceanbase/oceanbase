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

#include "ob_all_virtual_diag_index_scan.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

int ObAllVirtualDiagIndexScan::set_index_ids(const common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  common::ObRowkey start_key;
  common::ObRowkey end_key;
  const ObObj *start_key_obj_ptr = NULL;
  const ObObj *end_key_obj_ptr = NULL;
  int64_t index_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    start_key = ranges.at(i).start_key_;
    end_key = ranges.at(i).end_key_;
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      start_key_obj_ptr = start_key.get_obj_ptr();
      end_key_obj_ptr = end_key.get_obj_ptr();
      if (NULL != start_key_obj_ptr && NULL != end_key_obj_ptr) {
        if (start_key_obj_ptr[0].is_max_value() && end_key_obj_ptr[0].is_min_value()) {
          /* do nothing for always false range */
        } else if ((!start_key_obj_ptr[0].is_min_value() || !end_key_obj_ptr[0].is_max_value())
                   && start_key_obj_ptr[0] != end_key_obj_ptr[0]) {
          ret = OB_NOT_IMPLEMENT;
          SERVER_LOG(ERROR, "index id should be exact value", K(ret));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]) {
          if (ObIntType == start_key_obj_ptr[0].get_type()) {
            index_id = start_key_obj_ptr[0].get_int();
            if (OB_FAIL(add_var_to_array_no_dup(index_ids_, index_id))) {
              SERVER_LOG(WARN, "index id invalid", K(index_id), K(ret));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "index id invalid", K(ret));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "index id invalid", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "index id invalid", K(ret));
    }
  }
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */
