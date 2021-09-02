// Copyright 2020 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@antgroup.com>

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_meta_store.h"
#include "share/backup/ob_backup_path.h"
#include "lib/restore/ob_storage.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace share {

ObBackupMeta::ObBackupMeta()
    : partition_meta_list_(), sstable_meta_list_(), table_key_list_(), partition_group_meta_(), pg_meta_index_()
{}

ObBackupMeta::~ObBackupMeta()
{}

void ObBackupMeta::reset()
{
  partition_meta_list_.reset();
  sstable_meta_list_.reset();
  table_key_list_.reset();
  partition_group_meta_.reset();
  pg_meta_index_.reset();
}

bool ObBackupMeta::is_valid() const
{
  bool valid = true;
  if (!partition_group_meta_.is_valid()) {
    valid = false;
  } else {
    for (int64_t i = 0; valid && i < partition_meta_list_.count(); ++i) {
      const ObPartitionStoreMeta* meta = partition_meta_list_.at(i);
      if (!meta->is_valid()) {
        valid = false;
        LOG_WARN("partition meta is not valid", K(*meta), K(i));
      }
    }
    for (int64_t i = 0; valid && i < sstable_meta_list_.count(); ++i) {
      const ObSSTableBaseMeta& meta = sstable_meta_list_.at(i);
      if (!meta.is_valid()) {
        valid = false;
        LOG_WARN("sstable meta is not valid", K(meta), K(i));
      }
    }
    for (int64_t i = 0; valid && i < table_key_list_.count(); ++i) {
      const ObITable::TableKey& table_key = table_key_list_.at(i);
      if (!table_key.is_valid()) {
        valid = false;
        LOG_WARN("table key is not valid", K(table_key), K(i));
      }
    }
  }
  return valid;
}

int64_t ObBackupMeta::get_total_macro_block_count() const
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < sstable_meta_list_.count(); ++i) {
    const ObSSTableBaseMeta& meta = sstable_meta_list_.at(i);
    cnt += meta.get_total_macro_block_count();
  }
  return cnt;
}

ObBackupMetaFileStore::ObBackupMetaFileStore()
    : is_inited_(false), pos_(-1), cur_mem_file_(), allocator_(), base_path_info_(), meta_file_path_list_()
{}

ObBackupMetaFileStore::~ObBackupMetaFileStore()
{}

int ObBackupMetaFileStore::init(const share::ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup meta file store init twice", KR(ret));
  } else if (OB_FAIL(get_meta_file_list(path_info, meta_file_path_list_))) {
    LOG_WARN("failed to get meta file list", KR(ret), K(path_info));
  } else {
    base_path_info_ = path_info;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupMetaFileStore::next(ObBackupMeta& backup_meta)
{
  int ret = OB_SUCCESS;
  backup_meta.reset();
  MemoryFile* mem_file = NULL;
  bool end_of_one_block = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup file store do not init", KR(ret));
  } else {
    do {
      mem_file = NULL;
      end_of_one_block = false;
      if (OB_FAIL(get_next_mem_file(mem_file))) {
        LOG_WARN("failed to get next buffer", KR(ret));
      } else if (OB_ISNULL(mem_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memory file should not be null", KR(ret), K(mem_file));
      } else if (OB_FAIL(may_need_parse_common_header(mem_file))) {
        LOG_WARN("failed to parse common header", KR(ret));
      } else if (OB_FAIL(parse_sealed_message(mem_file, backup_meta, end_of_one_block))) {
        LOG_WARN("failed to parse sealed message", KR(ret));
      } else if (mem_file->reader_.pos() == mem_file->end_pos_) {
        mem_file->reader_.advance(mem_file->padding_);
        mem_file->need_parse_common_header_ = true;
      }
    } while (OB_SUCC(ret) && !end_of_one_block);
  }
  return ret;
}

int ObBackupMetaFileStore::get_meta_file_list(
    const share::ObBackupBaseDataPathInfo& path_info, common::ObIArray<share::ObBackupPath>& file_name_list)
{
  int ret = OB_SUCCESS;
  bool find_one = false;
  ObBackupPath tmp_backup_path;
  ObArray<ObString> file_names;
  ObStorageUtil util(true /*need retry*/);
  ObArenaAllocator allocator(ObModIds::BACKUP);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup meta file store init twice", KR(ret));
  } else if (!path_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup base data path info is not valid", KR(ret), K(path_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, tmp_backup_path))) {
    LOG_WARN("failed to get inc backup path", KR(ret), K(path_info));
  } else if (OB_FAIL(util.list_files(
                 tmp_backup_path.get_obstr(), path_info.dest_.dest_.storage_info_, allocator, file_names))) {
    LOG_WARN("failed to list files", KR(ret), K(path_info));
  } else {
    ObBackupPath meta_file_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.size(); ++i) {
      meta_file_path.reset();
      const ObString& file_name = file_names.at(i);
      if (!file_name.prefix_match(OB_STRING_BACKUP_META_FILE)) {
        LOG_DEBUG("skip non meta data file", K(file_name));
      } else if (OB_FAIL(meta_file_path.init(tmp_backup_path.get_obstr()))) {
        LOG_WARN("failed to init meta file path", KR(ret), K(tmp_backup_path));
      } else if (OB_FAIL(meta_file_path.join(file_name))) {
        LOG_WARN("failed to init meta file path", KR(ret), K(file_name));
      } else if (OB_FAIL(file_name_list.push_back(meta_file_path))) {
        LOG_WARN("failed to push back meta file path", KR(ret), K(meta_file_path));
      } else {
        find_one = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (!find_one) {
        ret = OB_ERR_SYS;
        LOG_WARN("no meta data file exist", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupMetaFileStore::read_meta_file(
    const common::ObString& file_path, const common::ObString& storage_info, MemoryFile& mem_file)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true);
  char* buf = NULL;
  int64_t file_length = 0;
  int64_t read_length = 0;
  int64_t buffer_len = 0;
  int64_t task_id = -1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store do not init", KR(ret));
  } else if (OB_FAIL(util.get_file_length(file_path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", KR(ret), K(file_path), K(storage_info));
  } else if (file_length <= 0) {
    LOG_INFO("meta file is empty");
  } else if (FALSE_IT(buffer_len = file_length)) {
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buffer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(buffer_len));
  } else if (OB_FAIL(util.read_single_file(file_path, storage_info, buf, buffer_len, read_length))) {
    LOG_WARN("failed to read single file", KR(ret));
  } else if (read_length != buffer_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read size if not valid", KR(ret), K(read_length), K(buffer_len));
  } else if (OB_FAIL(get_task_id(file_path, task_id))) {
    LOG_WARN("failed to get task id", K(ret), K(file_path));
  } else {
    mem_file.reader_.assign(buf, buffer_len, 0);
    mem_file.need_parse_common_header_ = true;
    mem_file.task_id_ = task_id;
    LOG_INFO("read meta file", K(file_path), K(storage_info), K(mem_file));
  }
  return ret;
}

int ObBackupMetaFileStore::get_next_mem_file(MemoryFile*& mem_file)
{
  int ret = OB_SUCCESS;
  mem_file = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (cur_mem_file_.reader_.remain() > 0 && !cur_mem_file_.meet_file_end_mark_) {
    mem_file = &cur_mem_file_;
  } else {
    LOG_INFO("current memory file", K(cur_mem_file_.reader_));
    // clear memory used by previous meta file
    allocator_.clear();
    ++pos_;
    if (pos_ >= meta_file_path_list_.count()) {
      ret = OB_ITER_END;
      LOG_INFO("reach file list end", KR(ret), K(pos_));
    } else {
      MemoryFile tmp_mem_file;
      const share::ObBackupPath& meta_file_path = meta_file_path_list_.at(pos_);
      if (OB_FAIL(read_meta_file(meta_file_path.get_obstr(), base_path_info_.dest_.get_storage_info(), tmp_mem_file))) {
        LOG_WARN("failed to read meta file", KR(ret), K(meta_file_path), K(base_path_info_));
      } else if (OB_FAIL(cur_mem_file_.assign(tmp_mem_file))) {
        LOG_WARN("failed to assign mem file", KR(ret), K(tmp_mem_file));
      } else {
        mem_file = &cur_mem_file_;
      }
    }
  }
  return ret;
}

int ObBackupMetaFileStore::may_need_parse_common_header(MemoryFile*& mem_file)
{
  int ret = OB_SUCCESS;
  blocksstable::ObBufferReader& reader = mem_file->reader_;
  share::ObBackupCommonHeader common_header;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store do not init", KR(ret));
  } else if (!mem_file->need_parse_common_header_) {
    // do not need parse common header
  } else if (OB_FAIL(parse_common_header(reader, common_header))) {
    LOG_WARN("failed to parse common header", KR(ret));
  } else if (OB_FAIL(common_header.check_valid())) {
    LOG_WARN("failed to check common header", KR(ret));
  } else if (BACKUP_FILE_END_MARK == common_header.data_type_) {
    mem_file->meet_file_end_mark_ = true;
    LOG_INFO("file reach end mark", K(mem_file->reader_));
  } else if (common_header.data_length_ > reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer reader not enough", KR(ret));
  } else if (OB_FAIL(common_header.check_data_checksum(reader.current(), common_header.data_length_))) {
    LOG_WARN("failed to check data checksum", KR(ret), K(common_header));
  } else {
    mem_file->need_parse_common_header_ = false;
    mem_file->end_pos_ = reader.pos() + common_header.data_length_;
    mem_file->padding_ = common_header.align_length_;
  }
  return ret;
}

int ObBackupMetaFileStore::parse_common_header(
    blocksstable::ObBufferReader& reader, share::ObBackupCommonHeader& header)
{
  int ret = OB_SUCCESS;
  const ObBackupCommonHeader* common_header = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.get(common_header))) {
    LOG_WARN("failed to read backup common header", KR(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta file common header is null", KR(ret));
  } else {
    header = *common_header;
    LOG_INFO("parse common header success", K(header));
  }
  return ret;
}

int ObBackupMetaFileStore::parse_sealed_message(
    MemoryFile*& mem_file, ObBackupMeta& backup_meta, bool& end_of_one_block)
{
  int ret = OB_SUCCESS;
  end_of_one_block = false;
  int64_t pos = 0;
  ObBufferReader reader;
  ObBufferReader& mem_reader = mem_file->reader_;
  const ObBackupMetaHeader* meta_header = NULL;
  const int64_t offset = mem_reader.pos();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (mem_file->meet_file_end_mark_) {
    LOG_INFO("reach file end mark, no need continue parsing", K(mem_reader));
  } else if (OB_FAIL(reader.deserialize(mem_reader.current(), mem_file->end_pos_, pos))) {
    LOG_WARN("failed to deserialize buffer reader", KR(ret), K(reader));
  } else if (OB_FAIL(reader.get(meta_header))) {
    LOG_WARN("failed to read meta header", KR(ret), K(reader), K(mem_reader));
  } else if (OB_ISNULL(meta_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta header is null", KR(ret));
  } else if (OB_FAIL(meta_header->check_valid())) {
    LOG_WARN("meta_header is not valid", KR(ret));
  } else if (meta_header->data_length_ > reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer reader not enough", KR(ret));
  } else if (OB_FAIL(meta_header->check_data_checksum(reader.current(), meta_header->data_length_))) {
    LOG_WARN("meta header data checksum failed", KR(ret));
  } else {
    LOG_INFO("Parsed meta header", K(*meta_header), K(pos));
    switch (meta_header->meta_type_) {
      case PARTITION_GROUP_META: {
        if (OB_FAIL(parse_pg_meta(reader, backup_meta.partition_group_meta_))) {
          LOG_WARN("failed to parse pg meta", KR(ret), K(reader), K(meta_header));
        } else {
          backup_meta.pg_meta_index_.reset();
          backup_meta.pg_meta_index_.table_id_ = backup_meta.partition_group_meta_.pg_key_.get_table_id();
          backup_meta.pg_meta_index_.partition_id_ = backup_meta.partition_group_meta_.pg_key_.get_partition_id();
          backup_meta.pg_meta_index_.task_id_ = mem_file->task_id_;
          backup_meta.pg_meta_index_.data_length_ = pos;
          backup_meta.pg_meta_index_.meta_type_ = PARTITION_GROUP_META;
          backup_meta.pg_meta_index_.offset_ = offset;
          if (OB_FAIL(backup_meta.pg_meta_index_.check_valid())) {
            LOG_WARN(
                "failed to check pg meta index is valid", K(ret), K(backup_meta.pg_meta_index_), K(pos), K(offset));
          } else {
            end_of_one_block = true;
            LOG_INFO("meet pg meta, end of one block");
          }
        }
        break;
      }
      case PARTITION_META: {
        char* buf = NULL;
        ObPartitionStoreMeta* partition_meta = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObPartitionStoreMeta))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (FALSE_IT(partition_meta = new (buf) ObPartitionStoreMeta)) {
          // new
        } else if (OB_FAIL(parse_partition_meta(reader, *partition_meta))) {
          LOG_WARN("failed to parse partition meta", KR(ret), K(reader), K(meta_header));
        } else if (OB_FAIL(backup_meta.partition_meta_list_.push_back(partition_meta))) {
          LOG_WARN("failed to push back partition meta", KR(ret), K(partition_meta));
        }
        break;
      }
      case SSTABLE_METAS: {
        ObArray<ObSSTableBaseMeta> sstable_metas;
        if (OB_FAIL(parse_sstable_meta(reader, sstable_metas))) {
          LOG_WARN("failed to parse sstable meta", KR(ret), K(reader), K(meta_header));
        } else if (OB_FAIL(append(backup_meta.sstable_meta_list_, sstable_metas))) {
          LOG_WARN("failed to add array", KR(ret));
        }
        break;
      }
      case TABLE_KEYS: {
        ObArray<ObITable::TableKey> table_keys;
        if (OB_FAIL(parse_table_key_meta(reader, table_keys))) {
          LOG_WARN("failed to parse table key meta", KR(ret), K(reader), K(meta_header));
        } else if (OB_FAIL(append(backup_meta.table_key_list_, table_keys))) {
          LOG_WARN("failed to add array", KR(ret));
        }
        break;
      }
      case PARTITION_GROUP_META_INFO: {
        storage::ObBackupPGMetaInfo pg_meta_info;
        if (OB_FAIL(parse_pg_meta_info(reader, pg_meta_info))) {
          LOG_WARN("failed to parse pg meta", KR(ret), K(reader), K(meta_header));
        } else if (OB_FAIL(backup_meta.partition_group_meta_.deep_copy(pg_meta_info.pg_meta_))) {
          LOG_WARN("failed to deep copy pg meta", K(ret), K(pg_meta_info));
        } else {
          backup_meta.pg_meta_index_.reset();
          backup_meta.pg_meta_index_.table_id_ = pg_meta_info.pg_meta_.pg_key_.get_table_id();
          backup_meta.pg_meta_index_.partition_id_ = pg_meta_info.pg_meta_.pg_key_.get_partition_id();
          backup_meta.pg_meta_index_.task_id_ = mem_file->task_id_;
          backup_meta.pg_meta_index_.data_length_ = pos;
          backup_meta.pg_meta_index_.meta_type_ = PARTITION_GROUP_META_INFO;
          backup_meta.pg_meta_index_.offset_ = offset;
          if (OB_FAIL(backup_meta.pg_meta_index_.check_valid())) {
            LOG_WARN(
                "failed to check pg meta index is valid", K(ret), K(backup_meta.pg_meta_index_), K(pos), K(offset));
          } else {
            end_of_one_block = true;
            LOG_INFO("meet pg meta, end of one block");
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("meta header type do not match", KR(ret), K(meta_header));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mem_file->reader_.advance(pos))) {
        LOG_WARN("failed to advance reader", KR(ret), K(pos));
      }
    }
  }
  return ret;
}

int ObBackupMetaFileStore::parse_pg_meta(blocksstable::ObBufferReader& reader, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.read_serialize(pg_meta))) {
    LOG_WARN("failed to read serialize", KR(ret));
  } else {
    LOG_INFO("parse pg meta success", K(pg_meta));
  }
  return ret;
}

int ObBackupMetaFileStore::parse_partition_meta(
    blocksstable::ObBufferReader& reader, ObPartitionStoreMeta& partition_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.read_serialize(partition_meta))) {
    LOG_WARN("failed to read partition meta", KR(ret));
  } else {
    LOG_INFO("parse partition meta success", K(reader.pos()), K(partition_meta));
  }
  return ret;
}

int ObBackupMetaFileStore::parse_sstable_meta(
    blocksstable::ObBufferReader& reader, common::ObIArray<ObSSTableBaseMeta>& sstable_meta_array)
{
  int ret = OB_SUCCESS;
  sstable_meta_array.reset();
  int64_t table_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.read(table_count))) {
    LOG_WARN("failed to read table count", KR(ret));
  } else {
    LOG_INFO("get sstable meta count", K(table_count));
    ObSSTableBaseMeta meta(allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
      meta.reset();
      if (OB_FAIL(reader.read_serialize(meta))) {
        LOG_WARN("read sstable base meta failed", KR(ret), K(i));
      } else if (OB_FAIL(sstable_meta_array.push_back(meta))) {
        LOG_WARN("failed to push back sstable meta", KR(ret), K(meta));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("parse sstable meta success", K(reader.pos()), K(table_count), K(sstable_meta_array));
    }
  }
  return ret;
}

int ObBackupMetaFileStore::parse_table_key_meta(
    blocksstable::ObBufferReader& reader, common::ObIArray<ObITable::TableKey>& table_key_array)
{
  int ret = OB_SUCCESS;
  table_key_array.reset();
  int64_t table_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.read(table_count))) {
    LOG_WARN("failed to read table count", KR(ret));
  } else {
    LOG_INFO("get table key meta count", K(table_count));
    ObITable::TableKey table_key;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
      table_key.reset();
      if (OB_FAIL(reader.read_serialize(table_key))) {
        LOG_WARN("read table key meta failed", KR(ret), K(i));
      } else if (OB_FAIL(table_key_array.push_back(table_key))) {
        LOG_WARN("failed to push back table key", KR(ret), K(table_key));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("parse table key meta success", K(reader.pos()), K(table_count), K(table_key_array));
    }
  }
  return ret;
}

int ObBackupMetaFileStore::get_task_id(const ObString& file_path, int64_t& task_id)
{
  int ret = OB_SUCCESS;
  task_id = -1;
  const char split_flag = '_';
  const char end_flag = '/';
  bool found = false;
  int64_t pos = 0;
  int64_t tmp_task_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", K(ret));
  } else if (file_path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get task id get invalid argument", K(ret), K(file_path));
  } else {
    const char* str = file_path.ptr();
    for (int64_t i = file_path.length() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (split_flag == str[i]) {
        pos = i;
        found = true;
        break;
      } else if (end_flag == str[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get task id get unexpected result", K(ret), K(file_path), K(i), K(pos));
      }
    }

    if (OB_SUCC(ret)) {
      if (!found || pos + 1 >= file_path.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get task is get unexpected result", K(ret), K(file_path), K(pos));
      } else {
        for (int64_t i = pos + 1 /*skip '_' */; OB_SUCC(ret) && i < file_path.length(); ++i) {
          if (!isdigit(str[i]) && '\0' != str[i]) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("can not find digit number", K(ret), K(file_path), K(i));
          } else if ('\0' == str[i]) {
            break;
          } else {
            if (tmp_task_id > INT64_MAX / 10 || (tmp_task_id == INT64_MAX / 10 && (str[i] - '0') > INT64_MAX % 10)) {
              ret = OB_DECIMAL_OVERFLOW_WARN;
              LOG_WARN("task id is not valid", KR(ret), K(tmp_task_id));
            } else {
              tmp_task_id = tmp_task_id * 10 + (str[i] - '0');
            }
          }
        }
        if (OB_SUCC(ret)) {
          task_id = tmp_task_id;
        }
      }
    }
  }
  return ret;
}

int ObBackupMetaFileStore::parse_pg_meta_info(
    blocksstable::ObBufferReader& reader, storage::ObBackupPGMetaInfo& pg_meta_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta file store not init", KR(ret));
  } else if (OB_FAIL(reader.read_serialize(pg_meta_info))) {
    LOG_WARN("failed to read serialize", KR(ret));
  } else {
    LOG_INFO("parse pg meta info success");
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
