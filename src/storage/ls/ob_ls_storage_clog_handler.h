//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OB_STORAGE_STORAGE_LS_STORAGE_CLOG_HANDLER_H_
#define OB_STORAGE_STORAGE_LS_STORAGE_CLOG_HANDLER_H_

#include "logservice/ob_log_base_header.h"
#include "share/scn.h"
namespace oceanbase
{
namespace storage
{
class ObLS;

class ObLSStorageClogHandler : public logservice::ObIReplaySubHandler,
                                 public logservice::ObIRoleChangeSubHandler,
                                 public logservice::ObICheckpointSubHandler
{
public:
  ObLSStorageClogHandler() : is_inited_(false), ls_(nullptr) {}
  virtual ~ObLSStorageClogHandler() { reset(); }

public:
  int init(ObLS *ls);
  void reset();

  // for replay
  int replay(
      const void *buffer,
      const int64_t nbytes,
      const palf::LSN &lsn,
      const share::SCN &scn) override final;

  // for role change
  void switch_to_follower_forcedly() override final
  {
  }
  int switch_to_leader() override final
  {
    return OB_SUCCESS;
  }
  int switch_to_follower_gracefully() override final
  {
    return OB_SUCCESS;
  }
  int resume_leader() override final
  {
    return OB_SUCCESS;
  }

  // for checkpoint
  int flush(share::SCN &rec_scn) override final
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }
  share::SCN get_rec_scn() override final
  {
    return share::SCN::max_scn();
  }

private:
  virtual int inner_replay(
      const logservice::ObLogBaseHeader &base_header,
      const share::SCN &scn,
      const char *buffer,
      const int64_t buffer_size,
      int64_t &pos) = 0;

  bool is_inited_;

protected:
  ObLS *ls_;
};

class ObLSResvSnapClogHandler : public ObLSStorageClogHandler
{
protected:
  virtual int inner_replay(
      const logservice::ObLogBaseHeader &base_header,
      const share::SCN &scn,
      const char *buffer,
      const int64_t buffer_size,
      int64_t &pos) override final;
};

class ObMediumCompactionClogHandler : public ObLSStorageClogHandler
{
protected:
  virtual int inner_replay(
      const logservice::ObLogBaseHeader &base_header,
      const share::SCN &scn,
      const char *buffer,
      const int64_t buffer_size,
      int64_t &pos) override final;
};


} // namespace storage
} // namespace oceanbase

#endif
