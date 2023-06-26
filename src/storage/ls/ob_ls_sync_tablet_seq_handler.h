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

#ifndef OCEANBASE_STORAGE_LS_SYNC_TABLET_SEQ_HANDLER_
#define OCEANBASE_STORAGE_LS_SYNC_TABLET_SEQ_HANDLER_

#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase
{

namespace storage
{
class ObLS;

class ObLSSyncTabletSeqHandler : public logservice::ObIReplaySubHandler,
                                 public logservice::ObIRoleChangeSubHandler,
                                 public logservice::ObICheckpointSubHandler
{
public:
  ObLSSyncTabletSeqHandler() : is_inited_(false), ls_(nullptr) {}
  ~ObLSSyncTabletSeqHandler() { reset(); }

public:
  int init(ObLS *ls);
  void reset();
  // for replay
  int replay(const void *buffer,
             const int64_t nbytes,
             const palf::LSN &lsn,
             const share::SCN &scn) override final;

  // for role change
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

  // for checkpoint
  int flush(share::SCN &scn) override final;
  share::SCN get_rec_scn() override final;

private:
  bool is_inited_;
  ObLS *ls_;

};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_LS_SYNC_TABLET_SEQ_HANDLER_
