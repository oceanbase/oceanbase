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

#ifndef OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_
#define OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_

#include <stdint.h>

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_append_callback.h"
#include "storage/ob_storage_schema.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{

namespace logservice
{
class ObLogHandler;
} // namespace palf

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObMultiVersionSchemaService;
} // namespace share
} // namespace schema

namespace storage
{
class ObTablet;
class ObIMemtableMgr;
class ObTabletHandle;

class ObStorageSchemaRecorder
{

public:
  ObStorageSchemaRecorder();
  ~ObStorageSchemaRecorder();

  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t saved_schema_version,
      logservice::ObLogHandler *log_handler);
  void reset();
  void destroy();
  bool is_inited() const { return is_inited_; }
  bool is_valid() const
  {
    return is_inited_
        && ls_id_.is_valid()
        && tablet_id_.is_valid()
        && nullptr != log_handler_
        && max_saved_table_version_ >= 0;
  }

  // follower
  int replay_schema_log(const int64_t log_ts, const char *buf, const int64_t size, int64_t &pos);
  // leader
  int try_update_storage_schema(
      const int64_t table_id,
      const int64_t table_version,
      ObIAllocator &allocator,
      const int64_t timeout);

  ObStorageSchemaRecorder(const ObStorageSchemaRecorder&) = delete;
  ObStorageSchemaRecorder& operator=(const ObStorageSchemaRecorder&) = delete;
  int64_t get_max_sync_version() const { return ATOMIC_LOAD(&max_saved_table_version_); }
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id));

private:
  class ObStorageSchemaLogCb : public logservice::AppendCb
  {
  public:
    virtual int on_success() override;
    virtual int on_failure() override;

    void set_table_version(const int64_t table_version);

    ObStorageSchemaLogCb(ObStorageSchemaRecorder &recorder)
      : recorder_(recorder),
        table_version_(common::OB_INVALID_VERSION)
    {}
    virtual ~ObStorageSchemaLogCb() { clear(); }
    void clear();

    ObStorageSchemaLogCb(const ObStorageSchemaLogCb&) = delete;
    ObStorageSchemaLogCb& operator=(const ObStorageSchemaLogCb&) = delete;
  private:
    ObStorageSchemaRecorder &recorder_;
    int64_t table_version_;
  };

private:
  int prepare_schema(const int64_t table_id, int64_t &table_version);
  int get_expected_schema_guard(const int64_t table_id, int64_t &table_version);
  int submit_schema_log(const int64_t table_id);
  int generate_clog();
  int64_t calc_schema_log_size() const;
  int gen_log_and_submit(
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  void free_allocated_info();
  int try_update_with_lock(const int64_t table_id, const int64_t table_version, const int64_t expire_ts);
  int get_tablet_handle(ObTabletHandle &tablet_handle);
  int replay_get_tablet_handle(const int64_t log_ts, ObTabletHandle &tablet_handle);
  // clog callback
  void update_table_schema_fail();
  void update_table_schema_succ(const int64_t table_version, bool &finish_flag);
  OB_INLINE int dec_ref_on_memtable(const bool sync_finish);

  // lock
  OB_INLINE void wait_to_lock(const int64_t table_version);
  OB_INLINE void wait_for_logcb(const int64_t table_version);

  static const int64_t MAX_RETRY_TIMES = 10;

  bool is_inited_;
  bool lock_;
  bool logcb_finish_flag_;
  ObStorageSchemaLogCb *logcb_ptr_;
  int64_t max_saved_table_version_;
  char *clog_buf_;
  int64_t clog_len_;
  int64_t clog_ts_;

  share::schema::ObSchemaGetterGuard *schema_guard_;
  ObStorageSchema *storage_schema_;
  ObIAllocator *allocator_;

  logservice::ObLogHandler *log_handler_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
};

} // storage
} // oceanbase
#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_ */
