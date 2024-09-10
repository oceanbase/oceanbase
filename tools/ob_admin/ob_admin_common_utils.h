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

#ifndef _OB_ADMIN_COMMON_UTILS_H_
#define _OB_ADMIN_COMMON_UTILS_H_
#include "lib/restore/ob_storage_info.h"


namespace oceanbase
{
namespace tools
{

struct ObDumpMacroBlockContext final
{
public:
  ObDumpMacroBlockContext()
    : first_id_(-1), second_id_(-1), micro_id_(-1), tablet_id_(0), scn_(-1), offset_(0)
  {}
  ~ObDumpMacroBlockContext() = default;
  bool is_valid() const
  {
    return second_id_ >= 0 || STRLEN(object_file_path_) > 0 || (STRLEN(uri_) > 0 && STRLEN(storage_info_str_) > 0);
  }
  TO_STRING_KV(K(first_id_), K(second_id_), K(micro_id_), K_(tablet_id), K_(scn), K_(object_file_path), K_(uri), K_(prewarm_index));
  uint64_t first_id_;
  int64_t second_id_;
  int64_t micro_id_;
  uint64_t tablet_id_;
  int64_t scn_;
  int64_t offset_;
  char object_file_path_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char uri_[common::OB_MAX_URI_LENGTH] = {0};
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  char prewarm_index_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
};

class ObAdminCommonUtils {
public:
  static int dump_single_macro_block(const ObDumpMacroBlockContext &macro_context, const char *buf, const int64_t size);
  static int dump_shared_macro_block(const ObDumpMacroBlockContext &macro_context, const char *buf, const int64_t size);
};

} //namespace tools
} //namespace oceanbase
#endif  // _OB_ADMIN_COMMON_UTILS_H_