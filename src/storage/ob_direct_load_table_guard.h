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

#pragma once

#include "storage/ob_i_table.h"

namespace oceanbase {

namespace share{
class SCN;
}

namespace storage {
class ObTablet;
class ObDDLKV;

class ObDirectLoadTableGuard {
private:
  static const int64_t MAX_RETRY_CREATE_MEMTABLE_TIME = 1LL * 1000LL * 1000LL; // 1 second

public:
  DISABLE_COPY_ASSIGN(ObDirectLoadTableGuard);
  ObDirectLoadTableGuard(ObTablet &tablet, const share::SCN &scn, const bool for_replay);
  ~ObDirectLoadTableGuard() { reset(); }
  void reset();
  void clear_write_ref(ObIArray<ObTableHandleV2> &table_handles);
  int prepare_memtable(ObDDLKV *&res_memtable);

  bool is_write_filtered() { return is_write_filtered_; }

  TO_STRING_KV(KP(this),
               K(has_acquired_memtable_),
               K(is_write_filtered_),
               K(for_replay_),
               K(ls_id_),
               K(tablet_id_),
               K(ddl_redo_scn_),
               K(table_handle_),
               K(construct_timestamp_));

private:
  void async_freeze_();
  int acquire_memtable_once_();
  int do_create_memtable_(ObLSHandle &ls_handle);
  int try_get_direct_load_memtable_for_write(ObLSHandle &ls_handle, bool &need_create_new_memtable);

private:
  bool has_acquired_memtable_;
  bool is_write_filtered_;
  const bool for_replay_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  const share::SCN &ddl_redo_scn_;
  ObTableHandleV2 table_handle_;
  int64_t construct_timestamp_;
};

}  // namespace storage
}  // namespace oceanbase
