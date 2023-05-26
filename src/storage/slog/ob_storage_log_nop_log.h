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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_NOP_LOG_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_NOP_LOG_H_

#include <stdint.h>
#include "storage/slog/ob_storage_log_struct.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogNopLog : public ObIBaseStorageLogEntry
{
public:
  ObStorageLogNopLog();
  virtual ~ObStorageLogNopLog();

  int init(const int64_t tenant_id, const int64_t buffer_size);
  void destroy();

  int set_needed_size(const int64_t size);

  virtual bool is_valid() const override { return true; };
  virtual int serialize(char *buf, const int64_t limit, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t limit, int64_t &pos) override;
  virtual int64_t get_serialize_size() const override { return needed_size_; }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  int64_t get_fixed_serialize_len(const int64_t used_len);
private:
  bool is_inited_;
  char *buffer_;
  int64_t buffer_size_;
  int64_t needed_size_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_STORAGE_LOG_NOP_LOG_H_
