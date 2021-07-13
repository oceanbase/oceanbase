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

#ifndef OCEANBASE_SLOG_SIMPLE_OB_PARTITION_IMAGE_H_
#define OCEANBASE_SLOG_SIMPLE_OB_PARTITION_IMAGE_H_

#include "lib/list/ob_list.h"
#include "lib/file/file_directory_utils.h"
#include "lib/file/ob_file.h"
#include "lib/random/ob_random.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
using namespace common;
using namespace lib;
namespace blocksstable {
struct SimpleObPartition : public ObIBaseStorageLogEntry {
  int64_t table_id_;
  int64_t partition_id_;
  int64_t log_seq_num_;
  int64_t macro_block_cnt_;
  int64_t macro_blocks_[1024];
  bool operator==(const SimpleObPartition& partition);
  bool operator!=(const SimpleObPartition& partition);
  bool is_valid() const;
  static void build(int64_t table_id, int64_t partition_id, int64_t macro_block_cnt, SimpleObPartition& partition);
  TO_STRING_EMPTY();

public:
  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct SimpleObPartitionNode {
  SimpleObPartitionNode() : partition_(), next_(NULL)
  {}
  SimpleObPartition partition_;
  SimpleObPartitionNode* next_;
};

enum SimpleObPartitionOperation { ADD_PARTITION = 0, REMOVE_PARTITION = 1 };

class SimpleObPartitionImage : public ObIRedoModule {
public:
  SimpleObPartitionImage();
  virtual ~SimpleObPartitionImage();
  int init(const char* data_dir, ObBaseStorageLogger* redo_log);
  virtual int read_check_point();
  virtual int do_check_point();
  // replay the redo log.
  virtual int replay(const ObRedoModuleReplayParam& param) override;
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;
  virtual int enable_write_log() override
  {
    return common::OB_SUCCESS;
  }
  int add_partition(SimpleObPartition& partition);
  int add_partition_no_commit(SimpleObPartition& partition);
  int remove_partition(const int64_t table_id, const int64_t partition_id);
  bool exist_partition(const int64_t table_id, const int64_t partition_id, int64_t* log_seq_num = NULL);
  bool operator==(SimpleObPartitionImage& image);
  bool operator!=(SimpleObPartitionImage& image);

public:
  static const int64_t BUCKET_NUM = 1523;
  static int64_t hash(const int64_t table_id, const int64_t partition_id);
  int do_add_partition(const SimpleObPartition& partition);
  void do_remove_partition(const int64_t table_id, const int64_t partition_id);
  bool inited_;
  common::ObArenaAllocator allocator_;
  common::ObConcurrentFIFOAllocator pt_allocator_;
  common::ObBucketLock buckets_lock_;
  SimpleObPartitionNode** buckets_;
  char data_file_[OB_MAX_FILE_NAME_LENGTH];
  ObBaseStorageLogger* redo_log_;
};

bool SimpleObPartition::operator==(const SimpleObPartition& partition)
{
  bool bret = true;

  if (table_id_ != partition.table_id_) {
    bret = false;
  } else if (partition_id_ != partition.partition_id_) {
    bret = false;
  } else if (macro_block_cnt_ != partition.macro_block_cnt_) {
    bret = false;
  } else {
    for (int64_t i = 0; bret && i < macro_block_cnt_; i++) {
      if (macro_blocks_[i] != partition.macro_blocks_[i]) {
        bret = false;
      }
    }
  }

  return bret;
}

bool SimpleObPartition::operator!=(const SimpleObPartition& partition)
{
  return !(*this == partition);
}

bool SimpleObPartition::is_valid() const
{
  return true;
}

DEFINE_SERIALIZE(SimpleObPartition)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, table_id_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode ", "table_id_", table_id_, "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, partition_id_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode ", "partition_id_", partition_id_, "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, log_seq_num_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode ", "log_seq_num_", log_seq_num_, "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, macro_block_cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode ", "macro_block_cnt_", macro_block_cnt_, "ret", ret);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt_; ++i) {
      if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, macro_blocks_[i]))) {
        STORAGE_REDO_LOG(WARN, "Fail to encode ", "macro_block", macro_blocks_[i], "ret", ret);
      }
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(SimpleObPartition)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &table_id_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode table_id_", "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &partition_id_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode partition_id_", "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &log_seq_num_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode log_seq_num_", "ret", ret);
  } else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &macro_block_cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode macro_block_cnt_", "ret", ret);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt_; ++i) {
      if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &macro_blocks_[i]))) {
        STORAGE_REDO_LOG(WARN, "Fail to decode macro_block", "ret", ret);
      }
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(SimpleObPartition)
{
  int64_t len = 0;
  len += serialization::encoded_length(table_id_);
  len += serialization::encoded_length(partition_id_);
  len += serialization::encoded_length(log_seq_num_);
  len += serialization::encoded_length(macro_block_cnt_);

  for (int64_t i = 0; i < macro_block_cnt_; ++i) {
    len += serialization::encoded_length(macro_blocks_[i]);
  }

  len += ObRandom::rand(0, 100);

  return len;
}

void SimpleObPartition::build(
    int64_t table_id, int64_t partition_id, int64_t macro_block_cnt, SimpleObPartition& partition)
{
  partition.table_id_ = table_id;
  partition.partition_id_ = partition_id;
  partition.macro_block_cnt_ = macro_block_cnt;
  for (int64_t i = 0; i < partition.macro_block_cnt_; ++i) {
    partition.macro_blocks_[i] = (table_id % (partition_id * macro_block_cnt + 1) + i);
  }
}

SimpleObPartitionImage::SimpleObPartitionImage()
    : inited_(false), allocator_(ObModIds::TEST), buckets_(NULL), redo_log_(NULL)
{}

SimpleObPartitionImage::~SimpleObPartitionImage()
{
  if (NULL != buckets_) {
    SimpleObPartitionNode* iter = NULL;
    SimpleObPartitionNode* tmp = NULL;

    for (int64_t pos = 0; pos < BUCKET_NUM; ++pos) {
      for (iter = buckets_[pos]; iter != NULL; iter = tmp) {
        tmp = iter->next_;
        pt_allocator_.free(iter);
      }
    }
  }
}

int SimpleObPartitionImage::init(const char* data_dir, ObBaseStorageLogger* redo_log)
{
  int ret = OB_SUCCESS;
  void* tmp = NULL;

  if (inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice.");
  } else if (NULL == data_dir || NULL == redo_log) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments, ", "data_dir", data_dir, "redo_log", redo_log);
  } else if (OB_FAIL(redo_log->register_redo_module(OB_REDO_LOG_PARTITION, this))) {
    STORAGE_REDO_LOG(WARN, "Fail to register redo log, ", "ret", ret);
  } else if (OB_FAIL(buckets_lock_.init(BUCKET_NUM))) {
    STORAGE_REDO_LOG(WARN, "Fail to init buckets lock, ", K(ret));
  } else if (NULL == (tmp = allocator_.alloc(sizeof(SimpleObPartitionNode*) * BUCKET_NUM))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else if (OB_FAIL(pt_allocator_.init(1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024))) {
    STORAGE_REDO_LOG(WARN, "Fail to init pt allocator, ", K(ret));
  } else {
    buckets_ = new (tmp) SimpleObPartitionNode*[BUCKET_NUM];
    MEMSET(buckets_, 0, sizeof(SimpleObPartitionNode*) * BUCKET_NUM);
    snprintf(data_file_, OB_MAX_FILE_NAME_LENGTH, "%s/data", data_dir);
    redo_log_ = redo_log;
    inited_ = true;
  }

  return ret;
}

int SimpleObPartitionImage::read_check_point()
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_SUCCESS == FileDirectoryUtils::is_exists(data_file_, exist) && exist) {
    ObString str;
    ModuleArena allocator;
    SimpleObPartition partition;
    int64_t pos = 0;

    if (OB_SUCCESS != (ret = load_file_to_string(data_file_, allocator, str))) {
      STORAGE_REDO_LOG(WARN, "Fail to load file to string, ", "ret", ret);
    } else {
      while (OB_SUCCESS == ret && pos < str.length()) {
        if (OB_SUCCESS != (ret = partition.deserialize(str.ptr(), str.length(), pos))) {
          STORAGE_REDO_LOG(WARN, "Fail to deserialize partition, ", "ret", ret);
        } else if (OB_SUCCESS != (ret = do_add_partition(partition))) {
          STORAGE_REDO_LOG(WARN, "Fail to add partition, ", "ret", ret);
        }
      }
    }
  }

  return ret;
}

int SimpleObPartitionImage::do_check_point()
{
  int ret = OB_SUCCESS;
  ObFileAppender data_file;
  int64_t pos = 0;
  const int64_t len = 1024 * 16;
  char buf[len];

  if (OB_SUCCESS != (ret = data_file.open(ObString(0, (int32_t)strlen(data_file_), data_file_), false, true, true))) {
    ret = OB_IO_ERROR;
    STORAGE_REDO_LOG(WARN, "Fail to open checkpoint file, ", "ret", ret);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_NUM; ++i) {
      common::ObBucketWLockGuard guard(buckets_lock_, i);
      SimpleObPartitionNode* iter = NULL;
      for (iter = buckets_[i]; OB_SUCC(ret) && iter != NULL; iter = iter->next_) {
        pos = 0;
        if (OB_SUCCESS != (ret = iter->partition_.serialize(buf, len, pos))) {
          STORAGE_REDO_LOG(WARN, "Fail to serialize, ", "ret", ret);
        } else if (OB_SUCCESS != (ret = data_file.append(buf, pos, true))) {
          STORAGE_REDO_LOG(WARN, "Fail to append to data file, ", "ret", ret);
        }
      }
    }

    data_file.close();
  }

  return ret;
}

int SimpleObPartitionImage::replay(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const int64_t log_seq_num = param.log_seq_num_;
  const int64_t subcmd = param.subcmd_;
  const char* buf = param.buf_;
  const int64_t len = param.buf_len_;
  enum ObRedoLogMainType main_type;
  int32_t sub_type = 0;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
  UNUSED(log_seq_num);

  if (OB_REDO_LOG_PARTITION != main_type) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    switch (static_cast<enum SimpleObPartitionOperation>(sub_type)) {
      case ADD_PARTITION: {
        SimpleObPartition partition;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = partition.deserialize(buf, len, pos))) {
          STORAGE_REDO_LOG(WARN, "Fail to deserialize partition, ", "ret", ret);
        } else if (!exist_partition(partition.table_id_, partition.partition_id_)) {
          partition.log_seq_num_ = log_seq_num;
          if (OB_SUCCESS != (ret = do_add_partition(partition))) {
            STORAGE_REDO_LOG(WARN, "Fail to do add partition, ", "ret", ret);
          }
        } else {
          STORAGE_REDO_LOG(DEBUG,
              "Recovery add partition: ",
              "table_id",
              partition.table_id_,
              "partition_id",
              partition.partition_id_);
        }
        break;
      }
      case REMOVE_PARTITION: {
        int64_t pos = 0;
        int64_t table_id = 0;
        int64_t partition_id = 0;
        int64_t plog_seq_num = 0;
        if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, len, pos, &table_id))) {
          STORAGE_REDO_LOG(WARN, "Fail to decode table_id, ", "ret", ret);
        } else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, len, pos, &partition_id))) {
          STORAGE_REDO_LOG(WARN, "Fail to decode partition_id, ", "ret", ret);
        } else if (exist_partition(table_id, partition_id, &plog_seq_num)) {
          if (log_seq_num > plog_seq_num) {
            do_remove_partition(table_id, partition_id);
          }
        } else {
          STORAGE_REDO_LOG(DEBUG, "recovery remove partition: ", "table_id", table_id, "partition_id", partition_id);
        }

        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "Can not recognize the subcmd.");
        break;
      }
    }
  }

  return ret;
}

int SimpleObPartitionImage::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(subcmd);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(stream);
  return ret;
}

int SimpleObPartitionImage::add_partition(SimpleObPartition& partition)
{
  int ret = OB_SUCCESS;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, ADD_PARTITION);
  int64_t log_seq_num = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init.");
  } else {
    int64_t pos = hash(partition.table_id_, partition.partition_id_) % BUCKET_NUM;
    const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);
    common::ObBucketWLockGuard guard(buckets_lock_, pos);

    if (!exist_partition(partition.table_id_, partition.partition_id_)) {
      if (OB_SUCCESS != (ret = redo_log_->begin(OB_LOG_CS_CREATE_PARTITION))) {
        STORAGE_REDO_LOG(WARN, "Fail to start transaction, ", "ret", ret);
      } else if (OB_SUCCESS != (ret = redo_log_->write_log(subcmd, log_attr, partition))) {
        STORAGE_REDO_LOG(WARN, "Fail to write log, ", "ret", ret);
      } else if (OB_SUCCESS != (ret = redo_log_->commit(log_seq_num))) {
        STORAGE_REDO_LOG(WARN, "Fail to commit, ", "ret", ret);
      } else {
        partition.log_seq_num_ = log_seq_num;
        if (OB_SUCCESS != (ret = do_add_partition(partition))) {
          STORAGE_REDO_LOG(ERROR, "Fail to add partition, ", "ret", ret);
        }
      }

      if (OB_FAIL(ret)) {
        redo_log_->abort();
      }
    }
  }

  return ret;
}

int SimpleObPartitionImage::add_partition_no_commit(SimpleObPartition& partition)
{
  int ret = OB_SUCCESS;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, ADD_PARTITION);
  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init.");
  } else {
    const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);
    if (OB_SUCCESS != (ret = redo_log_->write_log(subcmd, log_attr, partition))) {
      STORAGE_REDO_LOG(WARN, "Fail to write log, ", "ret", ret);
    } else {
      if (OB_SUCCESS != (ret = do_add_partition(partition))) {
        STORAGE_REDO_LOG(WARN, "Fail to add partition, ", "ret", ret);
      }
    }
  }

  return ret;
}

int SimpleObPartitionImage::remove_partition(const int64_t table_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REMOVE_PARTITION);

  int64_t log_seq_num = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init.");
  } else {
    SimpleObPartition partition;
    SimpleObPartition::build(table_id, partition_id, 1, partition);
    const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);
    if (OB_SUCCESS != (ret = redo_log_->begin(OB_LOG_CS_DEL_PARTITION))) {
      STORAGE_REDO_LOG(WARN, "Fail to start transaction, ", "ret", ret);
    } else if (OB_SUCCESS != (ret = redo_log_->write_log(subcmd, log_attr, partition))) {
      STORAGE_REDO_LOG(WARN, "Fail to write log, ", "ret", ret);
    } else {
      int64_t pos = hash(table_id, partition_id) % BUCKET_NUM;
      common::ObBucketWLockGuard guard(buckets_lock_, pos);

      if (!exist_partition(table_id, partition_id)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_SUCCESS != (ret = redo_log_->commit(log_seq_num))) {
        STORAGE_REDO_LOG(WARN, "Fail to commit trans, ", "ret", ret);
      } else {
        do_remove_partition(table_id, partition_id);
      }
    }

    if (OB_FAIL(ret)) {
      redo_log_->abort();
    }
  }

  return ret;
}

bool SimpleObPartitionImage::exist_partition(const int64_t table_id, const int64_t partition_id, int64_t* log_seq_num)
{
  bool bret = false;
  int64_t pos = hash(table_id, partition_id) % BUCKET_NUM;
  SimpleObPartitionNode* iter = NULL;

  for (iter = buckets_[pos]; iter != NULL; iter = iter->next_) {
    if (iter->partition_.table_id_ == table_id && iter->partition_.partition_id_ == partition_id) {
      bret = true;
      if (NULL != log_seq_num) {
        *log_seq_num = iter->partition_.log_seq_num_;
      }
      break;
    }
  }

  return bret;
}

bool SimpleObPartitionImage::operator==(SimpleObPartitionImage& image)
{
  bool bret = true;
  int64_t i = 0;
  SimpleObPartitionNode* iter1 = NULL;
  SimpleObPartitionNode* iter2 = NULL;

  for (i = 0; bret && i < BUCKET_NUM; ++i) {
    for (iter1 = buckets_[i], iter2 = image.buckets_[i]; bret && iter1 != NULL && iter2 != NULL;
         iter1 = iter1->next_, iter2 = iter2->next_) {
      if ((iter1->partition_) != (iter2->partition_)) {
        bret = false;
        STORAGE_LOG(ERROR,
            "iter different, ",
            K(iter1->partition_.partition_id_),
            K(iter2->partition_.partition_id_),
            K(iter1->partition_.macro_block_cnt_),
            K(iter2->partition_.macro_block_cnt_));
      }
    }

    if (iter1 != NULL || iter2 != NULL) {
      bret = false;
    }
    OB_ASSERT(bret);
  }

  return bret;
}

bool SimpleObPartitionImage::operator!=(SimpleObPartitionImage& image)
{
  return !(*this == image);
}

int64_t SimpleObPartitionImage::hash(const int64_t table_id, const int64_t partition_id)
{
  return table_id + (partition_id << 16);
}

int SimpleObPartitionImage::do_add_partition(const SimpleObPartition& partition)
{
  int ret = OB_SUCCESS;
  int64_t pos = hash(partition.table_id_, partition.partition_id_) % BUCKET_NUM;
  SimpleObPartitionNode* iter = NULL;
  SimpleObPartitionNode* node = NULL;
  void* tmp = NULL;

  for (iter = buckets_[pos]; iter != NULL; iter = iter->next_) {
    if (iter->partition_.table_id_ == partition.table_id_ &&
        iter->partition_.partition_id_ == partition.partition_id_) {
      break;
    }
  }

  if (iter != NULL) {
    ret = OB_ENTRY_EXIST;
  } else if (NULL == (tmp = pt_allocator_.alloc(sizeof(SimpleObPartitionNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    node = new (tmp) SimpleObPartitionNode();
    node->partition_ = partition;
    node->next_ = NULL;
    for (iter = buckets_[pos]; iter != NULL && iter->next_ != NULL; iter = iter->next_) {}
    if (NULL == iter) {
      buckets_[pos] = node;
    } else {
      iter->next_ = node;
    }
  }

  return ret;
}

void SimpleObPartitionImage::do_remove_partition(const int64_t table_id, const int64_t partition_id)
{
  int64_t pos = hash(table_id, partition_id) % BUCKET_NUM;
  SimpleObPartitionNode* iter = NULL;
  SimpleObPartitionNode* prev = NULL;

  for (iter = buckets_[pos]; iter != NULL; iter = iter->next_) {
    if (iter->partition_.table_id_ == table_id && iter->partition_.partition_id_ == partition_id) {
      break;
    } else {
      prev = iter;
    }
  }

  if (iter != NULL) {
    if (NULL == prev) {
      buckets_[pos] = iter->next_;
    } else {
      prev->next_ = iter->next_;
    }
    pt_allocator_.free(iter);
  }
}

}  // namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_SLOG_SIMPLE_OB_PARTITION_IMAGE_H_
