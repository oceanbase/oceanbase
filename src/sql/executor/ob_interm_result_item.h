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

#ifndef OCEANBASE_EXECUTOR_OB_INTERM_RESULT_ITEM_H_
#define OCEANBASE_EXECUTOR_OB_INTERM_RESULT_ITEM_H_

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
class ObScanner;
}
namespace sql {
class ObIIntermResultItem {
public:
  ObIIntermResultItem() : row_count_(0), data_len_(0)
  {}
  virtual ~ObIIntermResultItem()
  {}

  virtual bool in_memory() const = 0;
  virtual void reset() = 0;

  inline int64_t get_row_count() const
  {
    return row_count_;
  }
  inline int64_t get_data_len() const
  {
    return data_len_;
  }

  virtual int from_scanner(const common::ObScanner& scanner) = 0;
  // copy data to buffer
  virtual int copy_data(char* buf, const int64_t size) const = 0;

  VIRTUAL_TO_STRING_KV(K_(row_count), K_(data_len));

protected:
  int64_t row_count_;
  int64_t data_len_;

  DISALLOW_COPY_AND_ASSIGN(ObIIntermResultItem);
};

class ObDiskIntermResultItem;
class ObIntermResultItem : public ObIIntermResultItem {
  OB_UNIS_VERSION(1);

public:
  ObIntermResultItem(const char* label = common::ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_ITEM,
      uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  virtual ~ObIntermResultItem();

  virtual bool in_memory() const override
  {
    return true;
  }

  virtual void reset() override;
  int assign(const ObIntermResultItem& other);
  virtual int from_scanner(const common::ObScanner& scanner) override;
  int from_disk_ir_item(ObDiskIntermResultItem& disk_item);
  int to_scanner(common::ObScanner& scanner);
  inline bool is_valid() const
  {
    return data_len_ <= 0 || NULL != data_buf_;
  }

  inline const char* get_data_buf() const
  {
    return data_buf_;
  }

  virtual int copy_data(char* buf, const int64_t size) const override;

  INHERIT_TO_STRING_KV("iinterm_result", ObIIntermResultItem, KP_(data_buf));

private:
  common::ObArenaAllocator allocator_;
  char* data_buf_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermResultItem);
};

class ObDiskIntermResultItem : public ObIIntermResultItem {
public:
  ObDiskIntermResultItem();
  virtual ~ObDiskIntermResultItem();

  virtual bool in_memory() const override
  {
    return false;
  }

  int init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id_, const int64_t offset);
  bool is_inited() const
  {
    return fd_ >= 0;
  }

  virtual void reset() override;
  virtual int from_scanner(const common::ObScanner& scanner) override;
  virtual int copy_data(char* buf, const int64_t size) const override;

  static int get_timeout(int64_t& timeout_ms);

  INHERIT_TO_STRING_KV("iinterm_result", ObIIntermResultItem, K_(fd), K_(offset), K_(tenant_id));

private:
  uint64_t tenant_id_;
  int64_t fd_;
  int64_t dir_id_;
  int64_t offset_;
  DISALLOW_COPY_AND_ASSIGN(ObDiskIntermResultItem);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_EXECUTOR_OB_INTERM_RESULT_ITEM_H_
