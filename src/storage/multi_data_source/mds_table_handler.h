#ifndef SRC_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HANDLER_H
#define SRC_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HANDLER_H
#include "lib/lock/ob_small_spin_lock.h"
#include "mds_table_handle.h"
#include "mds_table_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObTabletPointer;
namespace mds
{

class ObMdsTableHandler
{
public:
  ObMdsTableHandler() : is_written_(false) {}
  ~ObMdsTableHandler();
  ObMdsTableHandler(const ObMdsTableHandler &) = delete;
  ObMdsTableHandler &operator=(const ObMdsTableHandler &);// value sematic for tablet ponter deep copy
  int get_mds_table_handle(mds::MdsTableHandle &handle,
                           const ObTabletID &tablet_id,
                           const share::ObLSID &ls_id,
                           const bool not_exist_create,
                           ObTabletPointer *pointer);
  int try_gc_mds_table();
  int try_release_nodes_below(const share::SCN &scn);
  void reset() { this->~ObMdsTableHandler(); }
  void set_mds_written() { ATOMIC_CAS(&(is_written_), false, true); }
  void mark_removed_from_t3m(ObTabletPointer *pointer);
  bool is_mds_written() const { return ATOMIC_LOAD(&(is_written_)); }
  TO_STRING_KV(K_(mds_table_handle));
private:
  MdsTableMgrHandle mds_table_mgr_handle_;// mgr handle destroy after table handle destroy
  MdsTableHandle mds_table_handle_;// primary handle, all other handles are copied from this one
  bool is_written_;
  mutable MdsLock lock_;
};

}
}
}
#endif