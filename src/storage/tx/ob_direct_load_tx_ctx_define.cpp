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

#include "storage/tx/ob_direct_load_tx_ctx_define.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{

namespace transaction
{

// OB_SERIALIZE_MEMBER(ObTxDirectLoadIncBatchKey, primary_tablet_id_, secondary_tablet_id_);

OB_SERIALIZE_MEMBER(ObTxDirectLoadIncBatchInfo,
                    batch_key_, /* 1 */
                    start_scn_ /* 2 */);
// flag_.val_ /* 3 */);

int ObTxDirectLoadIncBatchInfo::set_start_log_synced()
{
  int ret = OB_SUCCESS;

  if (!tmp_start_scn_.is_valid_and_not_min()
      || (start_scn_.is_valid_and_not_min() && start_scn_ != tmp_start_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "the tmp_start_scn is not valid in on_success", K(ret), KPC(this));
  } else {
    start_scn_ = tmp_start_scn_;
  }

  return ret;
}

int ObDLIBatchSet::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  const int64_t hash_count = ObTxDirectLoadBatchSet::size();
  const int64_t origin_pos = pos;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, hash_count))) {
    TRANS_LOG(WARN, "encode hash count failed", K(ret), K(hash_count), KP(buf), K(buf_len), K(pos));
  } else {
    ObTxDirectLoadBatchSet::const_iterator iter = ObTxDirectLoadBatchSet::begin();

    for (; iter != ObTxDirectLoadBatchSet::end() && OB_SUCC(ret); iter++) {
      if (OB_FAIL(iter->first.serialize(buf, buf_len, pos))) {
        TRANS_LOG(WARN, "serialize log key failed", K(ret), KP(buf), K(buf_len), K(pos));
      }
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "serialize failed,return to the origin_pos", K(ret), K(pos), K(origin_pos),
              KP(buf), K(buf_len));
    pos = origin_pos;
  }

  return ret;
}

int ObDLIBatchSet::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t hash_count = 0;
  const int64_t origin_pos = pos;
  const common::ObTabletID invalid_tablet_id(common::ObTabletID::INVALID_TABLET_ID);
  ObMemAttr mem_attr(MTL_ID(), "DLI_BATCH_HASH");
  if (!created() && OB_FAIL(create(32, mem_attr, mem_attr))) {
    TRANS_LOG(WARN, "create batch hash set failed", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, hash_count))) {
    TRANS_LOG(WARN, "decode hash count failed", K(ret), K(hash_count), KP(buf), K(data_len),
              K(pos));
  } else {
    for (int64_t i = 0; i < hash_count && OB_SUCC(ret); i++) {
      ObTxDirectLoadIncBatchInfo tmp_info;
      if (OB_FAIL(tmp_info.deserialize(buf, data_len, pos))) {
        TRANS_LOG(WARN, "deserialize direct load inc log key", K(ret), K(hash_count), KP(buf),
                  K(data_len), K(pos));
      } else if (!tmp_info.get_batch_key().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid deserialized log key", K(ret), K(tmp_info), K(i), K(hash_count),
                  KP(buf), K(data_len), K(pos));
      } else if (OB_FAIL(ObTxDirectLoadBatchSet::set_refactored(tmp_info))) {
        TRANS_LOG(WARN, "insert into hash set failed", K(ret), K(tmp_info), K(i), K(hash_count),
                  KP(buf), K(data_len), K(pos));
      }
    }
  }

  if (OB_SUCC(ret) && ObTxDirectLoadBatchSet::size() < hash_count) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "too few hash members after deserialized", K(ret),
              K(ObTxDirectLoadBatchSet::size()), K(hash_count), KP(buf), K(data_len), K(pos));
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "deserialize failed,return to the origin_pos", K(ret), K(pos), K(origin_pos),
              KP(buf), K(data_len));
    pos = origin_pos;
  }

  return ret;
}

int64_t ObDLIBatchSet::get_serialize_size() const
{
  int ret = OB_SUCCESS;

  const int64_t hash_count = ObTxDirectLoadBatchSet::size();
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(hash_count);

  ObTxDirectLoadBatchSet::const_iterator iter = ObTxDirectLoadBatchSet::begin();

  for (; iter != ObTxDirectLoadBatchSet::end() && OB_SUCC(ret); iter++) {
    total_size += iter->first.get_serialize_size();
  }
  return total_size;
}

int ObDLIBatchSet::before_submit_ddl_start(const ObDDLIncLogBasic &key, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "DLI_BATCH_HASH");
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (!created() && OB_FAIL(create(32, mem_attr, mem_attr))) {
    TRANS_LOG(WARN, "create batch hash set failed", K(ret), K(key));
  } else {
    ObTxDirectLoadIncBatchInfo batch_info(key);

    if (OB_FAIL(set_refactored(batch_info, 0))) {
      if (OB_HASH_EXIST != ret) {
        TRANS_LOG(WARN, "insert batch info into hash_set failed", K(ret), K(batch_info));
      } else {

        const ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
        if (OB_NOT_NULL(info_in_hashset)) {
          if ((info_in_hashset->is_ddl_start_logging()
               && info_in_hashset->get_tmp_start_scn() != start_scn)
              || (info_in_hashset->is_ddl_start_log_synced()
                  && info_in_hashset->get_start_scn() != start_scn)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "try to submit ddl start twice with the same key ", K(ret),
                      KPC(info_in_hashset), K(batch_info), K(start_scn));
          } else if (!start_scn.is_valid_and_not_min() && info_in_hashset->is_ddl_end_logging()) {
            ret = OB_EAGAIN;
            TRANS_LOG(WARN, "the last ddl end log is logging", K(ret), K(info_in_hashset),
                      K(batch_info), K(start_scn));
          } else if (OB_FAIL(set_refactored(batch_info, 1))) {
            TRANS_LOG(WARN, "overwrite existed batch_info failed", K(ret), KPC(info_in_hashset),
                      K(batch_info), K(start_scn));
          }

        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "a existed batch_info hasn't found", K(ret), K(batch_info),
                    KPC(info_in_hashset));
        }
      }
    }
  }
  return ret;
}

int ObDLIBatchSet::submit_ddl_start_succ(const ObDDLIncLogBasic &key, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {

    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(ERROR, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset), K(start_scn));
  } else {
    info_in_hashset->set_tmp_start_scn(start_scn);
    // info_in_hashset->clear_start_log_synced();
  }
  return ret;
}

int ObDLIBatchSet::sync_ddl_start_succ(const ObDDLIncLogBasic &key, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {

    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(ERROR, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset), K(start_scn));
  } else if (OB_FAIL(info_in_hashset->set_start_log_synced())) {
    TRANS_LOG(WARN, "set ddl start synced log failed", K(ret), K(batch_info), K(info_in_hashset),
              K(start_scn));
  }
  return ret;
}

int ObDLIBatchSet::sync_ddl_start_fail(const ObDDLIncLogBasic &key)
{
  int ret = OB_SUCCESS;

  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(ERROR, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset));
  } else {
    info_in_hashset->set_tmp_start_scn(share::SCN::invalid_scn());
  }

  return ret;
}

int ObDLIBatchSet::before_submit_ddl_end(const ObDDLIncLogBasic &key, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;


  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(WARN, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset),K(end_scn));
  } else if (info_in_hashset->is_ddl_end_logging()){
    ret = OB_ENTRY_EXIST;
    TRANS_LOG(WARN, "The DDL end is logging", K(ret), K(batch_info),KPC(info_in_hashset),K(end_scn));
  }

  return ret;
}

int ObDLIBatchSet::submit_ddl_end_succ(const ObDDLIncLogBasic &key, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;

  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(WARN, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset));
  } else {
    info_in_hashset->set_tmp_end_scn(end_scn);
  }
  return ret;
}

int ObDLIBatchSet::sync_ddl_end_succ(const ObDDLIncLogBasic &key, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObTxDirectLoadIncBatchInfo batch_info(key);
  if (OB_FAIL(erase_refactored(batch_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    TRANS_LOG(WARN, "erase from hash set failed", K(ret), K(batch_info), K(end_scn));
  }
  return ret;
}

int ObDLIBatchSet::sync_ddl_end_fail(const ObDDLIncLogBasic &key)
{

  int ret = OB_SUCCESS;
  ObTxDirectLoadIncBatchInfo batch_info(key);

  ObTxDirectLoadIncBatchInfo *info_in_hashset = get(batch_info);
  if (OB_ISNULL(info_in_hashset)) {

    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(ERROR, "a existed batch_info hasn't found", K(ret), K(batch_info),
              KPC(info_in_hashset));
  } else {
    info_in_hashset->set_tmp_end_scn(share::SCN::invalid_scn());
  }
  return ret;
}

int ObDLIBatchSet::remove_unlog_batch_info(const ObTxDirectLoadBatchKeyArray &batch_key_array)
{
  int ret = OB_SUCCESS;

  for (int i = 0; i < batch_key_array.count() && OB_SUCC(ret); i++) {

    ObTxDirectLoadIncBatchInfo batch_info(batch_key_array[i]);
    if (OB_FAIL(erase_refactored(batch_info))) {
      TRANS_LOG(WARN, "erase from hash set failed", K(ret), K(batch_info), K(i),
                K(batch_key_array.count()));
    }
  }
  return ret;
}

int ObDLIBatchSet::assign(const ObDLIBatchSet &other)
{
  int ret = OB_SUCCESS;

  ObMemAttr mem_attr(MTL_ID(), "DLI_BATCH_HASH");
  if (!other.created() || other.empty()) {
    reuse();
    // do nothing
  } else if (!created() && OB_FAIL(create(32, mem_attr, mem_attr))) {
    TRANS_LOG(WARN, "create batch hash set failed", K(ret));
  } else {
    reuse();
    ObDLIBatchSet::const_iterator iter = other.begin();

    while (iter != other.end() && OB_SUCC(ret)) {
      if (OB_FAIL(set_refactored(iter->first, 1))) {
        TRANS_LOG(WARN, "overwrite existed batch_info failed", K(ret), K(iter->first));
      }
      iter++;
    }
  }

  return ret;
}
} // namespace transaction

} // namespace oceanbase
