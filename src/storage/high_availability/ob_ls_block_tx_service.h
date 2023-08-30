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

#ifndef OCEANBASE_STORAGE_LS_BLOCK_TX_SERVICE_
#define OCEANBASE_STORAGE_LS_BLOCK_TX_SERVICE_

#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace storage
{

class ObLSBlockTxService : public logservice::ObIReplaySubHandler,
                           public logservice::ObIRoleChangeSubHandler,
                           public logservice::ObICheckpointSubHandler
{
public:
  ObLSBlockTxService();
  virtual ~ObLSBlockTxService();
  int init(storage::ObLS *ls);
  void destroy();
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override final;
  virtual share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &scn) override final;

public:
  //Not thread safe
  //Now only used for storage ha
  int ha_block_tx(const share::SCN &new_seq);
  int ha_kill_tx(const share::SCN &new_seq);
  int ha_unblock_tx(const share::SCN &new_seq);

private:
  int check_is_leader_();
  int check_seq_(const share::SCN &seq);
  int update_seq_(const share::SCN &seq);
private:
  bool is_inited_;
  ObMutex mutex_;
  share::SCN cur_seq_;
  storage::ObLS *ls_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBlockTxService);
};

}
}
#endif
