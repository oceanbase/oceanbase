//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_STORAGE_STORAGE_CLOG_RECORDER_
#define OCEANBASE_STORAGE_STORAGE_CLOG_RECORDER_

#include <stdint.h>
#include "logservice/ob_append_callback.h"
#include "storage/meta_mem/ob_tablet_handle.h"
namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
} // namespace palf

namespace storage
{
class ObIStorageClogRecorder
{
protected:
  class ObStorageCLogCb : public logservice::AppendCb
  {
  public:
    ObStorageCLogCb(ObIStorageClogRecorder &recorder)
      : recorder_(recorder),
        update_version_(common::OB_INVALID_VERSION)
    {}
    virtual ~ObStorageCLogCb()
    {
      reset();
    }
    int init();
    void reset()
    {
      update_version_ = common::OB_INVALID_VERSION;
    }

    virtual int on_success() override;
    virtual int on_failure() override;
    virtual void reset_handle() {}

    void set_update_version(const int64_t update_version)
    {
      ATOMIC_SET(&update_version_, update_version);
    }
  private:
    ObIStorageClogRecorder &recorder_;
    int64_t update_version_;

    DISABLE_COPY_ASSIGN(ObStorageCLogCb);
  };
public:
  ObIStorageClogRecorder();
  virtual ~ObIStorageClogRecorder();

  int init(const int64_t max_saved_version, logservice::ObLogHandler *log_handler);
  virtual void destroy();
  void reset();

  // leader
  int try_update_for_leader(
      const int64_t update_version,
      ObIAllocator *allocator,
      const int64_t timeout_ts = 1000000);
  int64_t get_max_saved_version() const { return ATOMIC_LOAD(&max_saved_version_); }

  ObIStorageClogRecorder(const ObIStorageClogRecorder&) = delete;
  ObIStorageClogRecorder& operator=(const ObIStorageClogRecorder&) = delete;

  VIRTUAL_TO_STRING_KV(K(max_saved_version_), K(clog_scn_), KP(log_handler_));
protected:
  // follower, check update version
  int replay_clog(
      const int64_t update_version,
      const share::SCN &scn,
      const char *buf,
      const int64_t size,
      int64_t &pos);

  // clog callback
  void clog_update_fail();
  void clog_update_succ(const int64_t update_version, bool &finish_flag);

  virtual int inner_replay_clog(
      const int64_t update_version,
      const share::SCN &scn,
      const char *buf,
      const int64_t size,
      int64_t &pos) = 0;
  virtual int sync_clog_succ_for_leader(const int64_t update_version) = 0;
  virtual void sync_clog_failed_for_leader() = 0;
  // call prepare struct only once
  virtual int prepare_struct_in_lock(
      int64_t &update_version,
      ObIAllocator *allocator,
      char *&clog_buf,
      int64_t &clog_len) = 0;
  virtual int submit_log(
      const int64_t update_version,
      const char *clog_buf,
      const int64_t clog_len) = 0;
  virtual void free_struct_in_lock() = 0;

  int try_update_with_lock(
      const int64_t update_version,
      const char *clog_buf,
      const int64_t clog_len,
      const int64_t expire_ts);
  // lock
  OB_INLINE void wait_to_lock(const int64_t table_version);
  OB_INLINE void wait_for_logcb(const int64_t table_version);

  int write_clog(const char *buf, const int64_t buf_len);
  share::SCN get_log_scn() const { return clog_scn_; }

  int get_tablet_handle(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle);
  int replay_get_tablet_handle(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &scn,
      ObTabletHandle &tablet_handle);

protected:
  bool lock_;
  bool logcb_finish_flag_;
  ObStorageCLogCb *logcb_ptr_;
  logservice::ObLogHandler *log_handler_;
  int64_t max_saved_version_;
  share::SCN clog_scn_;
};

} // storage
} // oceanbase
#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_ */
