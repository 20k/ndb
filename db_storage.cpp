#include "db_storage.hpp"
#include <lmdb.h>
#include <assert.h>
#include <stdexcept>
#include <vector>

struct db_tx
{
    MDB_txn* parent_transaction = nullptr;
    MDB_txn* transaction;

    bool read_only = false;

    db_tx(MDB_env* env, bool _read_only) : read_only(_read_only)
    {
        if(const int rc = mdb_txn_begin(env, parent_transaction, read_only ? MDB_RDONLY : 0, &transaction) != 0)
        {
            throw std::runtime_error("Bad Transaction (db_storage.cpp)");
        }
    }

    ~db_tx()
    {
        if(read_only)
        {
            mdb_txn_abort(transaction);
        }
        else
        {
            mdb_txn_commit(transaction);
        }
    }

    MDB_txn* get()
    {
        return transaction;
    }
};

struct db_read : db_tx
{
    db_read(MDB_env* env) : db_tx(env, true) {}
};

struct db_read_write : db_tx
{
    db_read_write(MDB_env* env) : db_tx(env, false) {}
};

struct db_backend
{
    MDB_env* env = nullptr;
    std::string storage;
    int db_count = 0;

    std::vector<MDB_dbi> dbis;

    db_backend(const std::string& _storage, int _db_count) : storage(_storage), db_count(_db_count)
    {
        assert(mdb_env_create(&env) == 0);

        mdb_env_set_maxdbs(env, 50);

        ///10000 MB
        mdb_env_set_mapsize(env, 1024ull * 1024ull * 10000ull);

        assert(mdb_env_open(env, storage.c_str(), 0, 0) == 0);

        dbis.resize(db_count);

        for(int i=0; i < db_count; i++)
        {
            db_read_write tx(env);

            assert(mdb_dbi_open(tx.get(), std::to_string(i).c_str(), MDB_CREATE, &dbis[i]) == 0);
        }
    }

    ~db_backend()
    {
        mdb_env_close(env);
    }
};

void db_tests()
{

}
