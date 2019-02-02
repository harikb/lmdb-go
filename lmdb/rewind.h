#include "lmdb.h"

typedef struct REW_env {
    MDB_env* mdb_log_env;
    MDB_dbi mdb_log_dbi;
    MDB_cursor* mdb_log_cursor;
    char* path;
    unsigned int last_durable_sequence;
    unsigned int current_sequence;
}REW_env;

int rew_env_create(MDB_env** env);
int re_env_create(MDB_env** env);
int re_env_open(MDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
int re_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn);
int re_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int re_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
int re_txn_commit(MDB_txn *txn);
int re_txn_abort(MDB_txn *txn);
void re_dbi_close(MDB_env *env, MDB_dbi dbi);
void re_env_close(MDB_env *env);
