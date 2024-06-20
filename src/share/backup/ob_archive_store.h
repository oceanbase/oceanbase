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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_STORE_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_STORE_H_

#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_archive_struct.h"
#include "lib/hash/ob_hashset.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_serialize_provider.h"
#include "share/backup/ob_backup_store.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include <cstdint>

namespace oceanbase
{
namespace share
{

class ObExternArchiveDesc : public ObIBackupSerializeProvider
{
public:
  explicit ObExternArchiveDesc(uint16_t type, uint16_t version)
    : type_(type), version_(version) {}

  virtual ~ObExternArchiveDesc(){}

  // Get file data type
  uint16_t get_data_type() const override
  {
    return type_;
  }

  // Get file data version
  uint16_t get_data_version() const override
  {
    return version_;
  }

  // Get file data compress algorithm type, default none.
  uint16_t get_compressor_type() const override
  {
    return ObCompressorType::NONE_COMPRESSOR;
  }

  VIRTUAL_TO_STRING_KV(K_(type), K_(version));

private:
  uint16_t type_; // archive file type
  uint16_t version_; // file data version
};


// Define round start placeholder file content.
struct ObRoundStartDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION(1);
public:
  int64_t dest_id_;
  int64_t round_id_;
  SCN start_scn_; // archive start time of the round
  int64_t base_piece_id_;
  int64_t piece_switch_interval_; // unit: us

  ObRoundStartDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), 
    K_(start_scn), K_(base_piece_id), K_(piece_switch_interval));
};


// Define round end placeholder file content.
struct ObRoundEndDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION(1);
public:
  int64_t dest_id_;
  int64_t round_id_;
  SCN start_scn_; // archive start time of the round
  SCN checkpoint_scn_;
  int64_t base_piece_id_;
  int64_t piece_switch_interval_; // unit: ns

  ObRoundEndDesc();

  bool is_valid() const override;

  // assign from round start, set checkpoint_scn to '0'.
  int assign(const ObRoundStartDesc &round_start);

  bool operator < (const ObRoundEndDesc &other) const;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), 
    K_(start_scn), K_(checkpoint_scn), K_(base_piece_id), K_(piece_switch_interval));
};


// Define piece start placeholder file content.
struct ObPieceStartDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  SCN start_scn_;

  ObPieceStartDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), K_(piece_id), K_(start_scn));
};


// Define piece end placeholder file content.
struct ObPieceEndDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  SCN end_scn_;

  ObPieceEndDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), K_(piece_id), K_(end_scn));
};


// Define piece static attribute content, include the static attributes and history
// frozen pieces.
struct ObTenantArchivePieceInfosDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  // Static attributes of current piece.
  uint64_t tenant_id_;
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  int64_t incarnation_;
  int64_t dest_no_;
  ObArchiveCompatible compatible_;
  SCN start_scn_;
  SCN end_scn_;
  ObBackupPathString path_;

  // history frozen pieces, ordered by piece id desc.
  ObSArray<ObTenantArchivePieceAttr> his_frozen_pieces_;

  ObTenantArchivePieceInfosDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(tenant_id), K_(dest_id), K_(round_id),
    K_(piece_id), K_(incarnation), K_(dest_no), K_(start_scn), K_(end_scn), K_(compatible), K_(path), 
    K_(his_frozen_pieces));
};

struct ObExternPieceWholeInfo final 
{
  ObTenantArchivePieceAttr current_piece_;
  // history frozen pieces, ordered by piece id desc.
  ObArray<ObTenantArchivePieceAttr> his_frozen_pieces_;

  TO_STRING_KV(K_(current_piece), K_(his_frozen_pieces));
};

// Define single piece info file content.
struct ObSinglePieceDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  ObTenantArchivePieceAttr piece_;

  ObSinglePieceDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(piece));
};

struct ObSinglePieceDescComparator
{
  bool operator()(const ObSinglePieceDesc &lhs, const share::ObSinglePieceDesc &rhs) const
  {
    ObPieceKey lhs_key;
    ObPieceKey rhs_key;
    lhs_key.dest_id_ = lhs.piece_.key_.dest_id_;
    lhs_key.round_id_ = lhs.piece_.key_.round_id_;
    lhs_key.piece_id_ = lhs.piece_.key_.piece_id_;
    rhs_key.dest_id_ = rhs.piece_.key_.dest_id_;
    rhs_key.round_id_ = rhs.piece_.key_.round_id_;
    rhs_key.piece_id_ = rhs.piece_.key_.piece_id_;
    return lhs_key < rhs_key;
  }
};


// Define checkpoint file content.
struct ObPieceCheckpointDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  uint64_t tenant_id_;
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  int64_t incarnation_;
  ObArchiveCompatible compatible_;
  SCN start_scn_; // archive start time of the round
  SCN checkpoint_scn_; // archive end time of the round
  SCN max_scn_;
  SCN end_scn_;

  int64_t reserved_[7];

  ObPieceCheckpointDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), K_(piece_id),
    K_(incarnation), K_(compatible), K_(start_scn), K_(checkpoint_scn), K_(max_scn), K_(end_scn));
};


// Define piece inner placeholder file content.
struct ObPieceInnerPlaceholderDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  SCN start_scn_;
  SCN checkpoint_scn_;

  ObPieceInnerPlaceholderDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), K_(piece_id), K_(start_scn), K_(checkpoint_scn));
};


// Define single log stream file list
struct ObSingleLSInfoDesc final : public ObExternArchiveDesc
{
  struct OneFile
  {
    OB_UNIS_VERSION(1);

  public:
    int64_t file_id_;
    int64_t size_bytes_;

    bool operator < (const OneFile &other) const { return file_id_ < other.file_id_; }

    TO_STRING_KV(K_(file_id), K_(size_bytes));
  };

  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  ObLSID ls_id_;
  SCN start_scn_;
  SCN checkpoint_scn_;
  uint64_t min_lsn_;
  uint64_t max_lsn_;
  ObSArray<OneFile> filelist_;
  bool deleted_; // mark log stream deleted.

  ObSingleLSInfoDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id),
    K_(round_id), K_(piece_id), K_(ls_id), K_(checkpoint_scn), K_(min_lsn),
    K_(max_lsn), K_(filelist), K_(deleted));
};


// Define all log stream file list
struct ObPieceInfoDesc final : public ObExternArchiveDesc
{
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1); // virtual

public:
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  ObSArray<ObSingleLSInfoDesc> filelist_;

  ObPieceInfoDesc();

  bool is_valid() const override;

  INHERIT_TO_STRING_KV("ObExternArchiveDesc", ObExternArchiveDesc, K_(dest_id), K_(round_id), K_(piece_id), K_(filelist));
};


// Define archive store
class ObArchiveStore : public ObBackupStore
{
public:
  ObArchiveStore();
  void reset();

  // oss://archive/rounds/round_d[dest_id]r[round_id]_start.obarc
  int is_round_start_file_exist(const int64_t dest_id, const int64_t round_id, bool &is_exist) const;
  int read_round_start(const int64_t dest_id, const int64_t round_id, ObRoundStartDesc &desc) const;
  int write_round_start(const int64_t dest_id, const int64_t round_id, const ObRoundStartDesc &desc) const;

  // oss://archive/rounds/round_d[dest_id]r[round_id]_end.obarc
  int is_round_end_file_exist(const int64_t dest_id, const int64_t round_id, bool &is_exist) const;
  int read_round_end(const int64_t dest_id, const int64_t round_id, ObRoundEndDesc &desc) const;
  int write_round_end(const int64_t dest_id, const int64_t round_id, const ObRoundEndDesc &desc) const;

  // oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_start_20220601T120000
  int is_piece_start_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, bool &is_exist) const;
  int read_piece_start(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, ObPieceStartDesc &desc) const;
  int write_piece_start(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, const ObPieceStartDesc &desc) const;

  // oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_end_20220601T120000.obarc
  int is_piece_end_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, bool &is_exist) const;
  int read_piece_end(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, ObPieceEndDesc &desc) const;
  int write_piece_end(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const SCN &create_scn, const ObPieceEndDesc &desc) const;

  // oss://archive/d[dest_id]r[round_id]p[piece_id]/single_piece_info.obarc
  int is_single_piece_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const;
  int read_single_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObSinglePieceDesc &desc) const;
  int write_single_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObSinglePieceDesc &desc) const;
  // oss://[user_specified_path]/single_piece_info.obarc, FOR ADD RESTORE SOURCE ONLY
  int read_single_piece(ObSinglePieceDesc &desc);
  // oss://archive/d[dest_id]r[round_id]p[piece_id]/checkpoint/checkpoint_info.[file_id].obarc
  int is_piece_checkpoint_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, bool &is_exist) const;
  int read_piece_checkpoint(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, ObPieceCheckpointDesc &desc) const;
  int write_piece_checkpoint(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, const ObPieceCheckpointDesc &desc) const;
  // oss://[user_specified_path]/checkpoint/checkpoint_info.0.obarc
  int read_piece_checkpoint(ObPieceCheckpointDesc &desc) const;
  // oss://archive/d[dest_id]r[round_id]p[piece_id]/piece_d[dest_id]r[round_id]p[piece_id]_20220601T120000_20220602T120000.obarc
  int is_piece_inner_placeholder_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn,
    const SCN &end_scn, bool &is_exist) const;
  int read_piece_inner_placeholder(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn, const SCN &end_scn, ObPieceInnerPlaceholderDesc &desc) const;
  int write_piece_inner_placeholder(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn, const SCN &end_scn, const ObPieceInnerPlaceholderDesc &desc) const;

  // oss://archive/d[dest_id]r[round_id]p[piece_id]/[ls_id]/file_info.obarc
  int is_single_ls_info_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
      const ObLSID &ls_id, bool &is_exist) const;
  int read_single_ls_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObSingleLSInfoDesc &desc) const;
  int write_single_ls_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, const ObSingleLSInfoDesc &desc) const;

  // oss://[user_specified_path]/[s_id].file_info.obarc
  int read_single_ls_info(const ObLSID &ls_id, ObSingleLSInfoDesc &desc) const;

  // oss://archive/d[dest_id]r[round_id]p[piece_id]/file_info.obarc
  int is_piece_info_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const;
  int read_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObPieceInfoDesc &desc) const;
  int write_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObPieceInfoDesc &desc) const;

  // oss://archive/d[dest_id]r[round_id]p[piece_id]/tenant_archive_piece_infos.obarc
  int is_tenant_archive_piece_infos_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const;
  int read_tenant_archive_piece_infos(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObTenantArchivePieceInfosDesc &desc) const;
  int write_tenant_archive_piece_infos(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObTenantArchivePieceInfosDesc &desc) const;
  // oss://[user_specified_path]/tenant_archive_piece_infos.obarc
  int read_tenant_archive_piece_infos(ObTenantArchivePieceInfosDesc &desc) const;
  // oss://<random_dir>/tenant_archive_piece_infos.obarc, FOR ADD RESTORE SOURCE ONLY
  int is_tenant_archive_piece_infos_file_exist(bool &is_exist) const;
  // oss://archive/d[dest_id]r[round_id]p[piece_id]/[ls_id]/[file_id]
  int is_archive_log_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, const int64_t file_id, bool &is_exist) const;

  int get_all_round_ids(const int64_t dest_id, ObIArray<int64_t> &roundid_array);
  // If end file not exist, set checkpoint_scn in ObRoundEndDesc to 0.
  int get_all_rounds(const int64_t dest_id, ObIArray<ObRoundEndDesc> &roundids);
  int get_round_id(const int64_t dest_id, const SCN &scn, int64_t &round_id);
  int get_round_range(const int64_t dest_id, int64_t &min_round_id, int64_t &max_round_id);
  int get_piece_range(const int64_t dest_id, const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id);

  int get_all_piece_keys(ObIArray<ObPieceKey> &keys);
  // Get single piece info no matter the piece is frozen or not.
  // If the piece is frozen, then just read the single piece info file. Otherwise,
  // read the piece extend file to get the static piece attributes and read the
  // checkpoint info to get the dynamic piece attributes. Then, merge them together.
  int get_single_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, 
      bool &is_empty_piece, ObSinglePieceDesc &single_piece);
  int get_whole_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, 
      bool &is_empty_piece, ObExternPieceWholeInfo &whole_piece_info);
  // independent of piece dir name, ONLY FOR ADD RESTORE SOURCE
  int get_single_piece_info(bool &is_empty_piece, ObSinglePieceDesc &single_piece);
  int get_whole_piece_info(bool &is_empty_piece, ObExternPieceWholeInfo &whole_piece_info);

  // Get pieces needed in the specific interval indicated by 'start_scn' and 'end_scn'.
  // Return OB_ENTRY_NOT_EXIST if cannot find enough pieces.
  int get_piece_paths_in_range(const SCN &start_scn, const SCN &end_scn, ObIArray<share::ObRestoreLogPieceBriefInfo> &pieces);

  // Get archive file range in one piece
  // return OB_ENTRY_NOT_EXIST if no file exist
  int get_file_range_in_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, int64_t &min_file_id, int64_t &max_file_id);

  // Get each file id and size for specific log stream under piece.
  int get_file_list_in_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObIArray<ObSingleLSInfoDesc::OneFile> &filelist) const;

  int get_max_checkpoint_scn(const int64_t dest_id, int64_t &round_id, int64_t &piece_id, SCN &max_checkpoint_scn);
  int get_round_max_checkpoint_scn(const int64_t dest_id, const int64_t round_id, int64_t &piece_id, SCN &max_checkpoint_scn);
  int get_piece_max_checkpoint_scn(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, SCN &max_checkpoint_scn);

private:

  class ObPieceRangeFilter : public ObBaseDirEntryOperator
  {
  public:
    ObPieceRangeFilter();
    virtual ~ObPieceRangeFilter() {}
    int init(ObArchiveStore *store, const int64_t dest_id, const int64_t round_id);
    int func(const dirent *entry) override;

    ObArray<int64_t> &result() { return pieces_; }

  private:
    bool is_inited_;
    ObArchiveStore *store_;
    int64_t dest_id_;
    int64_t round_id_;

    ObArray<int64_t> pieces_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObPieceRangeFilter);
  };

  class ObRoundFilter : public ObBaseDirEntryOperator
  {
    public:
    ObRoundFilter();
    virtual ~ObRoundFilter() {}
    int init(ObArchiveStore *store);
    int func(const dirent *entry) override;

    ObArray<ObRoundEndDesc> &result() { return rounds_; }

    TO_STRING_KV(K_(is_inited), K_(*store), K_(rounds));

  private:
    bool is_inited_;
    ObArchiveStore *store_;
    ObArray<ObRoundEndDesc> rounds_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObRoundFilter);
  };

  class ObPieceFilter : public ObBaseDirEntryOperator
  {
    public:
    ObPieceFilter();
    virtual ~ObPieceFilter() {}
    int init(ObArchiveStore *store);
    int func(const dirent *entry) override;

    ObArray<ObPieceKey> &result() { return piece_keys_; }

    TO_STRING_KV(K_(is_inited), K_(*store), K_(piece_keys));

  private:
    bool is_inited_;
    ObArchiveStore *store_;
    ObArray<ObPieceKey> piece_keys_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObPieceFilter);
  };

  class ObLocateRoundFilter : public ObBaseDirEntryOperator
  {
  public:
    ObLocateRoundFilter();
    virtual ~ObLocateRoundFilter() {}
    int init(ObArchiveStore *store, const SCN &scn);
    int func(const dirent *entry) override;

    ObArray<int64_t> &result() { return rounds_; }

    TO_STRING_KV(K_(is_inited), K_(*store), K_(scn), K_(rounds));

  private:
    bool is_inited_;
    ObArchiveStore *store_;
    SCN scn_;

    ObArray<int64_t> rounds_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObLocateRoundFilter);
  };

  class ObRoundRangeFilter : public ObBaseDirEntryOperator
  {
  public:
    ObRoundRangeFilter();
    virtual ~ObRoundRangeFilter() {}
    int init(ObArchiveStore *store, const int64_t dest_id);
    int func(const dirent *entry) override;

    ObArray<int64_t> &result() { return rounds_; }

    TO_STRING_KV(K_(is_inited), K_(*store), K_(rounds));

  private:
    bool is_inited_;
    ObArchiveStore *store_;
    int64_t dest_id_;

    ObArray<int64_t> rounds_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObRoundRangeFilter);
  };

  class ObLSFileListOp : public ObBaseDirEntryOperator
  {
  public:
    ObLSFileListOp();
    virtual ~ObLSFileListOp() {}
    int init(const ObArchiveStore *store, ObIArray<ObSingleLSInfoDesc::OneFile> *filelist);
    bool need_get_file_size() const override { return true; }
    int func(const dirent *entry) override;

    TO_STRING_KV(K_(is_inited), KPC(store_), KPC(filelist_));

  private:
    bool is_inited_;
    const ObArchiveStore *store_;

    ObIArray<ObSingleLSInfoDesc::OneFile> *filelist_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObLSFileListOp);
  };

  DISALLOW_COPY_AND_ASSIGN(ObArchiveStore);
};


}
}

#endif
