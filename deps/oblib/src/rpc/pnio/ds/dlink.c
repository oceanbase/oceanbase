extern inline bool dlink_is_empty(dlink_t* n);
extern inline void dlink_init(dlink_t* n);
extern inline void __dlink_insert(dlink_t* prev, dlink_t* next, dlink_t* n);
extern inline void __dlink_delete(dlink_t* prev, dlink_t* next);
extern inline void dlink_insert(dlink_t* head, dlink_t* n);
extern inline void dlink_delete(dlink_t* n);;
