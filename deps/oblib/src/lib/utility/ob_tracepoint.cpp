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

#include "lib/utility/ob_tracepoint.h"

#define GLOBAL_ERRSIM_POINT_DEF(no, name, describe)                               \
oceanbase::common::NamedEventItem oceanbase::common::EventTable::name(      \
  no, #name, describe, oceanbase::common::EventTable::global_item_list());
#include "lib/utility/ob_tracepoint_def.h"
#undef GLOBAL_ERRSIM_POINT_DEF

bool &get_tp_switch()
{
  RLOCAL(bool, turn_off);
  return turn_off;
}


namespace oceanbase
{
namespace common
{

uint64_t TPSymbolTable::BKDRHash(const char *str)
{
  uint64_t seed = 131; // 31 131 1313 13131 131313 etc..
  uint64_t hash = 0;
  if (OB_ISNULL(str)) {
    LIB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "Input str should not be NULL");
  } else {
    while (*str) {
      hash = hash * seed + (*str++);
    }
  }
  return hash;
}

bool TPSymbolTable::SymbolEntry::find(const char* name)
{
  bool has_found = false;
  if (OB_ISNULL(name)) {
    LIB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "Input name should not be NULL");
  } else {
    while(!has_found) {
      int lock = TP_AL(&lock_);
      if (SETTING == lock) {
        // wait;
      } else if (OK == lock) {
        has_found = (STRNCMP(name_, name, sizeof(name_)) == 0);
        break;
      } else if (TP_BCAS(&lock_, lock, SETTING)) {
        strncpy(name_, name, sizeof(name_) - 1);
        name_[sizeof(name_) - 1] = '\0';
        has_found = true;
        TP_AS(&lock_, OK);
      }
      TP_RELAX();
    }
  }
  return has_found;
}

void** TPSymbolTable::do_get(const char* name)
{
  void** found_value = NULL;
  if (OB_ISNULL(name)) {
    LIB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "name should not be NULL");
  } else {
    for(uint64_t start_idx = BKDRHash(name), idx = 0;
        NULL == found_value && idx < SYMBOL_COUNT_LIMIT;
        idx++) {
      SymbolEntry* entry = symbol_table_ + (start_idx + idx) % SYMBOL_COUNT_LIMIT;
      if ((NULL != entry) && entry->find(name)) {
        found_value = &entry->value_;
      }
    }
  }
  return found_value;
}

}
}
