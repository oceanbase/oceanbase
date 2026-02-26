/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

#ifndef OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_SERVICE_H_
#define OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_SERVICE_H_

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObDirectLoadAutoIncSeqData;
class ObDirectLoadAutoIncSeqService
{
  const static int INIT_NODE_MUTEX_NUM = 10243L;
public:
  static ObDirectLoadAutoIncSeqService &get_instance();
  static int get_start_seq(const share::ObLSID &ls_id,
                           const ObTabletID &tablet_id,
                           const int64_t step_size,
                           ObDirectLoadAutoIncSeqData &start_seq);
private:
  static int update_direct_load_auto_inc_seq(const ObLS &ls,
                                             const ObTabletID &tablet_id,
                                             ObDirectLoadAutoIncSeqData &new_seq);
private:
  int inner_get_start_seq(const share::ObLSID &ls_id,
                          const ObTabletID &tablet_id,
                          const int64_t step_size,
                          ObDirectLoadAutoIncSeqData &start_seq);
private:
  lib::ObMutex init_node_mutexs_[INIT_NODE_MUTEX_NUM];
};

} // namespace storage
} // namespace oceanbase

#endif /* OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_SERVICE_H_ */