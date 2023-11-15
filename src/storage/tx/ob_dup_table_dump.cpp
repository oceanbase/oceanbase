// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "storage/tx/ob_dup_table_dump.h"

namespace oceanbase
{
namespace transaction
{

void ObDupTableLogDumpIterator::reset()
{
  str_arg_ = nullptr;
  iter_buf_ = nullptr;
  iter_buf_len_ = 0;
  iter_buf_pos_ = 0;

  big_segment_.reset();

  dup_tablet_map_.reset();
  dup_table_lease_map_.destroy();
  stat_log_.reset();
}

bool ObDupTableLogDumpIterator::is_inited()
{
  bool inited = false;
  inited = OB_NOT_NULL(str_arg_) && OB_NOT_NULL(iter_buf_) && big_segment_.is_active()
           && dup_tablet_map_.created() && dup_table_lease_map_.created();
  return inited;
}

int ObDupTableLogDumpIterator::init_with_log_buf(const char *buf,
                                                 const int64_t buf_len,
                                                 share::ObAdminMutatorStringArg *str_arg_ptr)
{
  int ret = OB_SUCCESS;

  logservice::ObLogBaseHeader base_header;
  int64_t replay_pos = 0;

  if (OB_ISNULL(buf) || buf_len < 0 || OB_ISNULL(str_arg_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), KP(str_arg_ptr));
  } else if (OB_FAIL(big_segment_.collect_one_part(buf, buf_len, replay_pos))) {
    if (OB_ITER_END == ret) {
      // need clear big_segment after collected all part for replay
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "Log entry is completed, can not merge new block", K(ret),
                    K(big_segment_));
    } else if (OB_START_LOG_CURSOR_INVALID == ret) {
      DUP_TABLE_LOG(INFO, "start replay from the middle of a big log entry", K(ret),
                    K(big_segment_));
    } else if (big_segment_.is_completed()) {
      ret = OB_ITER_END;
    }
  } else if (!dup_tablet_map_.created()
             && OB_FAIL(dup_tablet_map_.create(OB_SYS_TENANT_ID, 1024))) {
    DUP_TABLE_LOG(WARN, "create dup tablet map failed", K(ret), KPC(this),
                  K(dup_tablet_map_.created()));
  } else if (!dup_table_lease_map_.created()
             && OB_FAIL(dup_table_lease_map_.create(64, "DupTableDump"))) {
    DUP_TABLE_LOG(WARN, "create dup table lease map failed", K(ret), K(this),
                  K(dup_table_lease_map_.created()));
  } else {
    str_arg_ = str_arg_ptr;
    dup_tablet_map_.clear();
    dup_table_lease_map_.clear();
  }

  return ret;
}

int ObDupTableLogDumpIterator::dump_dup_table_log()
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (!big_segment_.is_completed()) {
      ret = OB_STATE_NOT_MATCH;
      DUP_TABLE_LOG(WARN, "need collect more parts of log entry", K(ret), K(big_segment_));
    } else {
      int64_t deser_pos = big_segment_.get_deserialize_buf_pos();
      const int64_t deser_buf_len = big_segment_.get_deserialize_buf_len();

      if (OB_NOT_NULL(str_arg_)) {
        share::ObAdminLogNormalDumper normal_writer;
        str_arg_->writer_ptr_ = &normal_writer;
        str_arg_->writer_ptr_->dump_key("###<DUP_TABLE_LOG>");
        str_arg_->writer_ptr_->start_object();
      }

      while (OB_SUCC(ret) && deser_pos < deser_buf_len) {
        DupTableLogEntryHeader entry_header;
        int64_t log_entry_size = 0;
        int64_t after_header_pos = 0;
        stat_log_.reset();
        if (OB_FAIL(entry_header.deserialize(big_segment_.get_deserialize_buf(), deser_buf_len,
                                             deser_pos))) {

          DUP_TABLE_LOG(WARN, "deserialize entry header failed", K(ret), K(deser_pos),
                        K(big_segment_), K(entry_header));
        } else if (OB_FAIL(serialization::decode_i64(big_segment_.get_deserialize_buf(),
                                                     deser_buf_len, deser_pos, &log_entry_size))) {
          DUP_TABLE_LOG(WARN, "decode log entry size failed", K(ret), K(log_entry_size),
                        K(big_segment_));
        } else if (OB_FALSE_IT(after_header_pos = deser_pos)) {
          // do nothing
        } else {
          switch (entry_header.entry_type_) {
          case DupTableLogEntryType::TabletChangeLog: {
            if (OB_FAIL(iter_tablet_log_(deser_pos + log_entry_size, deser_pos))) {
              DUP_TABLE_LOG(WARN, "iter dup table tablet log failed", K(ret), K(deser_pos),
                            K(log_entry_size));
            }
            break;
          }
          case DupTableLogEntryType::LeaseListLog: {
            if (OB_FAIL(iter_lease_log_(deser_pos + log_entry_size, deser_pos))) {
              DUP_TABLE_LOG(WARN, "iter dup table lease log failed", K(ret), K(deser_pos),
                            K(log_entry_size));
            }
            break;
          }
          case DupTableLogEntryType::DupTableStatLog: {
            if (OB_FAIL(iter_stat_log_(deser_pos + log_entry_size, deser_pos))) {
              DUP_TABLE_LOG(WARN, "iter dup table stat log failed", K(ret), K(deser_pos),
                            K(log_entry_size));
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            DUP_TABLE_LOG(WARN, "unexpected log entry type", K(ret), K(entry_header), K(deser_pos));
            break;
          }
          }
        }
      }
      if (OB_SUCC(ret)) {
        iter_buf_pos_ = deser_pos;
        if (OB_NOT_NULL(str_arg_)) {
          str_arg_->writer_ptr_->end_object();
        }
      }
    }
  }

  return ret;
}

int ObDupTableLogDumpIterator::iter_stat_log_(const int64_t deser_buf_len, int64_t &deser_pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = deser_pos;

  if (OB_FAIL(stat_log_.deserialize(big_segment_.get_deserialize_buf(), deser_buf_len, tmp_pos))) {
    DUP_TABLE_LOG(WARN, "deserialize stat log failed", K(ret), K(deser_pos), K(deser_buf_len),
                  K(stat_log_));
  } else if (OB_ISNULL(str_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid ObAdminMutatorStringArg", K(ret), KPC(this));
  } else {
    str_arg_->writer_ptr_->dump_key("##<STAT_LOG>");
    str_arg_->writer_ptr_->dump_string(to_cstring(stat_log_));
  }

  if (OB_SUCC(ret)) {
    deser_pos = tmp_pos;
  }

  return ret;
}

int ObDupTableLogDumpIterator::iter_tablet_log_(const int64_t deser_buf_len, int64_t &deser_pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = deser_pos;

  if (OB_ISNULL(str_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid ObAdminMutatorStringArg", K(ret), KPC(this));
  } else {
    str_arg_->writer_ptr_->dump_key("##<TABLET_LOG>");
    str_arg_->writer_ptr_->start_object();

    while (OB_SUCC(ret) && tmp_pos < deser_buf_len) {
      dup_tablet_map_.reuse();
      DupTabletLogBody dup_log_body(dup_tablet_map_);
      if (OB_FAIL(dup_log_body.deserialize(big_segment_.get_deserialize_buf(), deser_buf_len,
                                           tmp_pos))) {
        DUP_TABLE_LOG(WARN, "deserialize dup log body failed", K(ret), K(dup_log_body));
      } else {
        str_arg_->writer_ptr_->dump_key("DupTabletSet");

        str_arg_->writer_ptr_->start_object();

        str_arg_->writer_ptr_->dump_key("#(TabletSetAttribute)");
        str_arg_->writer_ptr_->dump_string(to_cstring(dup_tablet_map_.get_RO_attribute()));
        str_arg_->writer_ptr_->dump_key("#(RelatedTabletSetAttribute)");
        str_arg_->writer_ptr_->dump_string(to_cstring(dup_tablet_map_.get_RO_related_attribute()));
        str_arg_->writer_ptr_->dump_key("#(TabletCount)");
        str_arg_->writer_ptr_->dump_string(to_cstring(dup_tablet_map_.size()));
        str_arg_->writer_ptr_->dump_key("#(TabletIdList)");
        str_arg_->writer_ptr_->start_object();
        for (DupTabletChangeMap::const_iterator iter = dup_tablet_map_.begin();
             iter != dup_tablet_map_.end(); iter++) {
          str_arg_->writer_ptr_->dump_string(to_cstring(iter->first));
        }
        str_arg_->writer_ptr_->end_object();

        str_arg_->writer_ptr_->end_object();
      }
    }

    str_arg_->writer_ptr_->end_object();
  }
  if (OB_SUCC(ret)) {
    deser_pos = tmp_pos;
  }
  return ret;
}

int ObDupTableLogDumpIterator::iter_lease_log_(const int64_t deser_buf_len, int64_t &deser_pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = deser_pos;

  if (OB_ISNULL(str_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid ObAdminMutatorStringArg", K(ret), KPC(this));
  } else {
    str_arg_->writer_ptr_->dump_key("##<LEASE_LOG>");
    str_arg_->writer_ptr_->start_object();

    while (OB_SUCC(ret) && tmp_pos < deser_buf_len) {

      DupTableLeaseLogHeader lease_log_header;
      DupTableLeaderLeaseInfo leader_lease_info;
      DupTableDurableLeaseLogBody durable_lease_log_body(leader_lease_info.confirmed_lease_info_);
      if (OB_FAIL(lease_log_header.deserialize(big_segment_.get_deserialize_buf(), deser_buf_len,
                                               tmp_pos))) {
        DUP_TABLE_LOG(WARN, "deserialize lease log header failed", K(ret), K(lease_log_header),
                      K(tmp_pos), K(deser_buf_len));
      } else if (lease_log_header.is_durable_lease_log()) {
        if (OB_FAIL(durable_lease_log_body.deserialize(big_segment_.get_deserialize_buf(),
                                                       deser_buf_len, tmp_pos))) {
          DUP_TABLE_LOG(WARN, "deserialize leader lease info failed", K(ret));
        } else {
          str_arg_->writer_ptr_->start_object();
          str_arg_->writer_ptr_->dump_key("#(LeaseKey)");
          str_arg_->writer_ptr_->dump_string(to_cstring(lease_log_header));
          str_arg_->writer_ptr_->dump_key("#(ConfirmedLeaseInfo)");
          str_arg_->writer_ptr_->dump_string(to_cstring(leader_lease_info.confirmed_lease_info_));
          str_arg_->writer_ptr_->end_object();
        }
      }
    }

    str_arg_->writer_ptr_->end_object();
  }

  if (OB_SUCC(ret)) {
    deser_pos = tmp_pos;
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
