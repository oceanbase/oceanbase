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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_STRUCT_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_STRUCT_H_

#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{
enum class ObRedoLogMainType
{
  OB_REDO_LOG_INVALID = 0,
  OB_REDO_LOG_SYS = 1,
  OB_REDO_LOG_TENANT_STORAGE = 2,
  OB_REDO_LOG_SERVER_TENANT = 3,
  OB_REDO_LOG_MAX = 4
};

enum class ObRedoLogSubType
{
  OB_REDO_LOG_INVALID = 0,
  OB_REDO_LOG_NOP = 1,

  OB_REDO_LOG_CREATE_TENANT_PREPARE = 2,
  OB_REDO_LOG_CREATE_TENANT_COMMIT = 3,
  OB_REDO_LOG_CREATE_TENANT_ABORT = 4,
  OB_REDO_LOG_DELETE_TENANT_PREPARE = 5,
  OB_REDO_LOG_DELETE_TENANT_COMMIT = 6,
  OB_REDO_LOG_UPDATE_TENANT_UNIT = 7,
  OB_REDO_LOG_UPDATE_TENANT_SUPER_BLOCK = 8,

  OB_REDO_LOG_CREATE_LS = 9,
  OB_REDO_LOG_CREATE_LS_COMMIT = 10,
  OB_REDO_LOG_CREATE_LS_ABORT = 11,

  OB_REDO_LOG_UPDATE_LS = 12,
  OB_REDO_LOG_DELETE_LS = 13,

  OB_REDO_LOG_PUT_OLD_TABLET = 14, // discard from 4.1
  OB_REDO_LOG_DELETE_TABLET = 15,
  OB_REDO_LOG_UPDATE_TABLET = 16,
  OB_REDO_LOG_EMPTY_SHELL_TABLET = 17,

  OB_REDO_LOG_UPDATE_DUP_TABLE_LS = 18,

  OB_REDO_LOG_MAX
};

class ObIBaseStorageLogEntry
{
public:
  ObIBaseStorageLogEntry() {}
  virtual ~ObIBaseStorageLogEntry() = default;
  virtual bool is_valid() const = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;

  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObStorageLogParam final
{
  ObStorageLogParam();
  ObStorageLogParam(const int32_t cmd, ObIBaseStorageLogEntry *data);
  ~ObStorageLogParam() = default;

  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(cmd), KPC_(data), K_(disk_addr));

  int32_t cmd_;
  ObIBaseStorageLogEntry *data_;
  ObMetaDiskAddr disk_addr_;
};

}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_SLOG_OB_STORAGE_LOG_STRUCT_H_
