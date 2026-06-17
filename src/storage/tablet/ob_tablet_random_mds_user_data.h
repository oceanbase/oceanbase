/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_USER_DATA
#define OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_USER_DATA

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace storage
{

class ObTabletRandomMdsUserData
{
  OB_UNIS_VERSION(1);
public:
  ObTabletRandomMdsUserData() : auto_random_size_(OB_INVALID_SIZE), is_active_(false) { }
  virtual ~ObTabletRandomMdsUserData() { reset(); }
  bool is_valid() const;
  int init_random_size(const int64_t auto_random_size);
  void reset();
  int assign(const ObTabletRandomMdsUserData &other);
  int get_random_part_data(int64_t &auto_random_size, bool &is_active) const;
  int set_random_size(const int64_t auto_random_size);
  int set_is_active(const bool is_active);
  bool is_active() {return is_active_;};
  TO_STRING_KV(K_(auto_random_size), K_(is_active));
public:
  int64_t auto_random_size_;
  bool is_active_;
};

struct ReadRandomDataAutoPartSizeOp
{
public:
  ReadRandomDataAutoPartSizeOp(int64_t &auto_random_size, bool is_active) : auto_random_size_(auto_random_size), is_active_(is_active) {}
  int operator()(const ObTabletRandomMdsUserData &data)
  {
    return data.get_random_part_data(auto_random_size_, is_active_);
  }
public:
  int64_t &auto_random_size_;
  bool is_active_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_USER_DATA
