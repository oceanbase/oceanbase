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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_COS_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_COS_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "ob_i_storage.h"
#include "cos/ob_storage_cos_obj.h"

namespace oceanbase {
namespace common {

// Before using cos, you need to initialize cos enviroment.
// Thread safe guaranteed by user.
int init_cos_env();

// You need to clean cos resource when not use cos any more.
// Thread safe guaranteed by user.
void fin_cos_env();

class ObCosUtil : public ObIStorageUtil {
public:
  ObCosUtil()
  {}
  virtual ~ObCosUtil()
  {}

  int is_exist(const ObString &uri, const ObString &storage_info, bool &exist) override;

  int get_file_length(const ObString &uri, const ObString &storage_info, int64_t &file_length) override;

  int write_single_file(
      const ObString &uri, const ObString &storage_info, const char *buf, const int64_t size) override;

  // cos no dir
  int mkdir(const ObString &uri, const ObString &storage_info) override;

  int del_file(const ObString &uri, const ObString &storage_info) override;

  int update_file_modify_time(const ObString &uri, const ObString &storage_info) override;

  int list_files(const ObString &dir_path, const ObString &storage_info, common::ObIAllocator &allocator,
      common::ObIArray<ObString> &file_names) override;

  int del_dir(const ObString &uri, const ObString &storage_info) override;

  int get_pkeys_from_dir(
      const ObString &dir_path, const ObString &storage_info, common::ObIArray<common::ObPartitionKey> &pkeys) override;

  int delete_tmp_files(const ObString &dir_path, const ObString &storage_info) override;

  int is_empty_directory(
      const common::ObString &uri, const common::ObString &storage_info, bool &is_empty_directory) override;
  int is_tagging(const common::ObString &uri, const common::ObString &storage_info, bool &is_tagging) override;
  int get_file_meta(const ObString &uri, const ObString &storage_info, bool &exist, qcloud_cos::CosObjectMeta &meta);

  int check_backup_dest_lifecycle(
      const common::ObString &dir_path, const common::ObString &storage_info, bool &is_set_lifecycle) override;

  int list_directories(const ObString &dir_path, const ObString &storage_info, common::ObIAllocator &allocator,
      common::ObIArray<ObString> &directory_names) override;
};

// Random accesss object
class ObCosRandomAccessObject : public ObIStorageReader {
public:
  ObCosRandomAccessObject();

  virtual ~ObCosRandomAccessObject()
  {}

  int open(const ObString &uri, const ObString &storage_info) override;

  int pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size) override;

  int close() override;

  int64_t get_length() const override
  {
    return meta_.file_length;
  }

  bool is_opened() const override
  {
    return is_opened_;
  }

private:
  // basic cos object
  ObCosObject obj_;
  qcloud_cos::CosObjectMeta meta_;
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObCosRandomAccessObject);
};

// Over write object
class ObCosOverWriteObject : public ObIStorageWriter {
public:
  ObCosOverWriteObject();
  virtual ~ObCosOverWriteObject()
  {}

  int open(const ObString &uri, const ObString &storage_info) override;

  // append write, make sure no concurrent writer.
  int write(const char *buf, const int64_t size) override;

  int pwrite(const char *buf, const int64_t size, const int64_t offset) override;

  int close() override;

  // Return the amount of written data after open.
  // The returned value is undefined if have not opened before.
  int64_t get_length() const override
  {
    return file_length_;
  }

  bool is_opened() const override
  {
    return is_opened_;
  }

  // The returned value is undefined if have not opened before.
  int64_t get_object_size() const
  {
    return object_size_;
  }

private:
  // get the whole object from cos.
  int get_(char *buf, const int64_t buf_size);

  inline const ObString &bucket_name_string_() const
  {
    return obj_.bucket_name_string();
  }

  inline const ObString &object_name_string_() const
  {
    return obj_.object_name_string();
  }

private:
  // basic cos object
  ObCosObject obj_;
  // TODO: to rename to data_written_amount
  // The current value represents the amount of data written between open and close.
  int64_t file_length_;
  // The total object size in cos.
  int64_t object_size_;
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObCosOverWriteObject);
};

class ObCosMetaMgr : public ObIStorageMetaWrapper {
public:
  ObCosMetaMgr()
  {}
  virtual ~ObCosMetaMgr()
  {}

  int get(const common::ObString &uri, const common::ObString &storage_info, char *buf, const int64_t buf_size,
      int64_t &read_size);

  int set(const common::ObString &uri, const common::ObString &storage_info, const char *buf, const int64_t buf_size);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCosMetaMgr);
};

// ObCosSlice
class ObCosSlice {
public:
  enum Mask {
    OB_COS_SLICE_INVALID_MASK = 0,
    OB_COS_SLICE_ID_MASK = 0x01,
    OB_COS_MULTI_VERSION_MASK = 0x02,  // multi-version slice
  };

  struct Option {
    int64_t mask;
    int64_t sliceid;
    int64_t version;

    inline bool is_multi_version_slice() const
    {
      return 0 != (mask & Mask::OB_COS_MULTI_VERSION_MASK);
    }

    TO_STRING_KV(K(mask), K(sliceid), K(version));
  };

  // slice id marker in slice name.
  static constexpr const char *COS_SLICE_MARKER = "@part@_";
  // version marker in slice name.
  static constexpr const char *COS_VERSION_MAKER = "@version@_";
  // Concatenation to connect the slice id part and version part to a slice name.
  static constexpr const char *COS_SLICE_CONCATENATION = "-";

  ObCosSlice(const Option &option);

  ~ObCosSlice()
  {}

  // A valid slice name must include slice id, version is optional.
  static bool is_a_valid_slice_name(const ObString &path, bool &only);

  // Parse slice id and version from slice name.
  static int parse_slice_name(const ObString &path, ObCosSlice::Option &option);

  static int get_container_name(const ObString &slice_name, ObString &container_name, common::ObIAllocator &allocator);

  inline bool is_valid() const
  {
    return 0 != (option_.mask & Mask::OB_COS_SLICE_ID_MASK) && 0 < option_.sliceid;
  }

  inline bool is_multi_version_slice() const
  {
    return option_.is_multi_version_slice();
  }

  int build_slice_name(char *buffer, int64_t buff_size);

  TO_STRING_KV(K_(option));

private:
  Option option_;

  DISALLOW_COPY_AND_ASSIGN(ObCosSlice);
};

// ObCosContainer
class ObCosContainer : public ObCosBase {
public:
  struct Option {
    int64_t version;
    bool open_version = false;

    // threshold used to switch to next slice.
    int64_t threshold = 0;  // default 0

    TO_STRING_KV(K(version), K(open_version), K(threshold));
  };

  ObCosContainer() : ObCosBase()
  {}

  ~ObCosContainer()
  {}

  static inline int64_t get_start_slice_id()
  {
    // slice id start from 1.
    return 1LL;
  }

  static inline int64_t generate_next_slice_id(int64_t current_slice_id)
  {
    return current_slice_id + 1;
  }

  int build_bucket_and_container_name(const ObString &uri)
  {
    return build_bucket_and_object_name(uri);
  }

  const ObString &container_name_string() const
  {
    return object_name_string();
  }

  const qcloud_cos::CosStringBuffer &container_name() const
  {
    return object_name();
  }

  using ObCosBase::head_meta;

  // Query container meta, returned the slices and each slice length.
  int head_meta(bool &is_exist, qcloud_cos::CosObjectMeta &meta, common::ObIAllocator &allocator,
      common::ObIArray<ObString> &slice_names, common::ObIArray<int64_t> &slice_lengths);

  int head_meta(bool &is_exist, qcloud_cos::CosObjectMeta &meta) override;

  using ObCosBase::del;

  // delete all slices in container
  int del(int64_t &deleted_cnt) override;

  // list all slices in container
  int list_slices(bool &is_exist, common::ObIAllocator &allocator, common::ObIArray<ObString> &slice_names,
      common::ObIArray<int64_t> &slice_lengths);

  // return max slice id in container
  int find_max_slice_option(const int64_t version, bool &is_exist, int64_t &container_size,
      ObCosSlice::Option &max_slice_option, int64_t &last_slice_size);

  // let slice number in container not too much, limit 800
  static const int64_t MAX_SLICE_ID = 800;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCosContainer);
};

class ObCosWritableContainer : public ObIStorageWriter {
public:
  ObCosWritableContainer();

  ObCosWritableContainer(const ObCosContainer::Option &option);

  virtual ~ObCosWritableContainer()
  {}

  void set_option(const ObCosContainer::Option &option)
  {
    option_ = option;
  }

  int open(const ObString &uri, const ObString &storage_info) override;

  // append write
  int write(const char *buf, const int64_t size) override;

  int pwrite(const char *buf, const int64_t size, const int64_t offset) override;

  int close() override;

  int64_t get_length() const override
  {
    return length_;
  }

  bool is_opened() const override
  {
    return is_opened_;
  }

private:
  int64_t get_slice_mask() const
  {
    int64_t mask = ObCosSlice::Mask::OB_COS_SLICE_INVALID_MASK;
    mask |= ObCosSlice::Mask::OB_COS_SLICE_ID_MASK;
    if (option_.open_version) {
      mask |= ObCosSlice::Mask::OB_COS_MULTI_VERSION_MASK;
    }

    return mask;
  }

  int open_current_slice();

  int build_current_slice_name(char *slice_name_buff, int32_t buff_size);

  int switch_to_next_slice(const int64_t size);

  bool need_switch_next_slice(const int64_t size) const;

  const ObString &container_name_string() const
  {
    return container_.container_name_string();
  }

  const ObString &bucket_name_string() const
  {
    return container_.bucket_name_string();
  }

private:
  ObCosContainer::Option option_;
  bool is_opened_;
  int64_t length_;
  int64_t container_size_;
  int64_t last_slice_size_;
  ObCosContainer container_;
  ObCosOverWriteObject overwrite_obj_;
  int64_t current_slice_id_;
  // record the uri and storage info when open object.
  ObString uri_;
  ObString storage_info_;
  ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObCosWritableContainer);
};

// ObCosRandomAccessContainer
class ObCosRandomAccessContainer : public ObIStorageReader {
public:
  ObCosRandomAccessContainer();

  virtual ~ObCosRandomAccessContainer()
  {}

  int open(const ObString &uri, const ObString &storage_info) override;

  int pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size) override;

  int close() override;

  int64_t get_length() const override
  {
    return meta_.file_length;
  }

  bool is_opened() const override
  {
    return is_opened_;
  }

private:
  int build_slice_name(int64_t slice_idx, char *slice_name_buff, int32_t buff_size);

  int pread_from_slice(int64_t slice_idx, char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size);

private:
  ObCosContainer container_;
  bool is_opened_;
  qcloud_cos::CosObjectMeta meta_;
  ObArenaAllocator allocator_;
  ObArray<ObString> slice_names_array_;
  ObArray<int64_t> slice_lengths_array_;
  // record the uri and storage info when open object.
  ObString uri_;
  ObString storage_info_;

  DISALLOW_COPY_AND_ASSIGN(ObCosRandomAccessContainer);
};

// For a uri is either an object or container, however we do not it.
// ObCosRandomAccessReader is actually acted as an adapter. It will
// check which type the uri is, and then decides the corresponding reader.
class ObCosRandomAccessReader : public ObIStorageReader {
public:
  ObCosRandomAccessReader();

  virtual ~ObCosRandomAccessReader()
  {}

  int open(const ObString &uri, const ObString &storage_info) override;

  int pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size) override;

  int close() override;

  int64_t get_length() const override
  {
    int64_t length = -1;
    if (NULL != reader_) {
      length = reader_->get_length();
    }
    return length;
  }

  bool is_opened() const override
  {
    return NULL != reader_;
  }

private:
  ObCosRandomAccessContainer random_access_container_;
  ObCosRandomAccessObject random_access_object_;
  ObIStorageReader *reader_;
};

// Over write object
class ObCosAppender : public ObIStorageWriter {
public:
  ObCosAppender();
  virtual ~ObCosAppender()
  {}

  int open(const ObString &uri, const ObString &storage_info) override;

  // append write, make sure no concurrent writer.
  int write(const char *buf, const int64_t size) override;

  int pwrite(const char *buf, const int64_t size, const int64_t offset) override;

  int close() override;

  // Return the amount of written data after open.
  // The returned value is undefined if have not opened before.
  int64_t get_length() const override
  {
    int64_t length = -1;
    if (NULL != writer_) {
      length = writer_->get_length();
    }
    return length;
  }

  bool is_opened() const override
  {
    return NULL != writer_;
  }

  void set_obj_type(ObCosObjectType type)
  {
    obj_type_ = type;
  }

  void set_container_option(ObCosContainer::Option option)
  {
    writable_container_.set_option(option);
  }

private:
  ObCosWritableContainer writable_container_;
  ObCosOverWriteObject overwrite_obj_;
  ObIStorageWriter *writer_;

  ObCosObjectType obj_type_;

  DISALLOW_COPY_AND_ASSIGN(ObCosAppender);
};

}  // namespace common
}  // namespace oceanbase

#endif
