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

#ifndef OCEANBASE_ARCHIVE_OB_LS_META_RECORDER_H_
#define OCEANBASE_ARCHIVE_OB_LS_META_RECORDER_H_

#include <cstdint>
#include "lib/hash/ob_link_hashmap.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_archive_define.h"
#include "share/backup/ob_archive_struct.h"
#include "share/ob_ls_id.h"                    // ObLSID
#include "share/scn.h"                    // ObLSID
namespace oceanbase
{
namespace archive
{
typedef common::LinkHashValue<share::ObArchiveLSMetaType> RecordContextValue;
class ObArchiveRoundMgr;
struct RecordContext : public RecordContextValue
{
  share::SCN last_record_scn_;
  int64_t last_record_round_;
  int64_t last_record_piece_;
  int64_t last_record_file_;

  RecordContext();
  ~RecordContext();

  int set(const RecordContext &other);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(last_record_scn), K_(last_record_round),
      K_(last_record_piece), K_(last_record_file));
};

typedef common::ObLinkHashMap<share::ObArchiveLSMetaType, RecordContext> RecordContextMap;
class ObLSMetaRecorder
{
public:
  ObLSMetaRecorder();
  ~ObLSMetaRecorder();

public:
  int init(ObArchiveRoundMgr *round_mgr);
  void destroy();
  void handle();

private:
  int check_and_get_record_context_(const share::ObArchiveLSMetaType &type, RecordContext &context);
  int insert_or_update_record_context_(const share::ObArchiveLSMetaType &type, RecordContext &record_context);
  void clear_record_context_();
  int prepare_();
  bool check_need_delay_(const share::ObLSID &id, const ArchiveKey &key, const share::SCN &ts);
  int build_path_(const share::ObLSID &id,
      const ArchiveKey &key,
      const share::SCN &scn,
      const share::ObArchiveLSMetaType &type,
      const int64_t file_id,
      share::ObBackupPath &path,
      RecordContext &record_context);
  int generate_common_header_(char *buf,
      const int64_t header_size,
      const int64_t data_size,
      const share::SCN &scn);
  int do_record_(const ArchiveKey &key, const char *buf, const int64_t size, share::ObBackupPath &path);
  int make_dir_(const share::ObLSID &id,
      const ArchiveKey &key,
      const share::SCN &scn,
      const share::ObArchiveLSMetaType &type);
  void clear_();

private:
  bool inited_;
  ObArchiveRoundMgr *round_mgr_;
  char *buf_;
  RecordContextMap map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSMetaRecorder);
};

} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_LS_META_RECORDER_H_ */
