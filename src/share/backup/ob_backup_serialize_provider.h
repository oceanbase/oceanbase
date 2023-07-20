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

#ifndef OCEANBASE_SHARE_OB_BACKUP_SERIALIZE_PROVIEDER_H_
#define OCEANBASE_SHARE_OB_BACKUP_SERIALIZE_PROVIEDER_H_

#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{

class ObIBackupSerializeProvider
{
  OB_UNIS_VERSION_PV(); // pure virtual
public:
  virtual ~ObIBackupSerializeProvider() {}

  virtual bool is_valid() const = 0;
  // Get file data type
  virtual uint16_t get_data_type() const = 0;
  // Get file data version
  virtual uint16_t get_data_version() const = 0;
  virtual uint16_t get_compressor_type() const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};


// Wrapper backup serialize data with common backup header.
class ObBackupSerializeHeaderWrapper final : public ObIBackupSerializeProvider
{
public:
  // the only constructor.
  explicit ObBackupSerializeHeaderWrapper(ObIBackupSerializeProvider *serializer)
      : serializer_(serializer) {}
  virtual ~ObBackupSerializeHeaderWrapper() {}

  bool is_valid() const override;
  uint16_t get_data_type() const override;
  uint16_t get_data_version() const override;
  uint16_t get_compressor_type() const override;

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos) override;
  int64_t get_serialize_size() const override;

  TO_STRING_KV(K_(*serializer));

private:
  ObIBackupSerializeProvider *serializer_;
};

}
}

#endif