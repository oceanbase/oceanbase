extern void mod_report(format_t* f);
extern void* mod_alloc(int64_t sz, int mod);
extern void mod_free(void* p);
extern void* salloc(int64_t sz);
extern void sfree(void* p);
extern const char* mem_freelists_str();
extern void init_mem_freelists();
extern void refresh_mem_freelists();
enum {
#define MOD_DEF(name) MOD_ ## name, //keep
#include "mod_define.h"
#undef MOD_DEF
};
