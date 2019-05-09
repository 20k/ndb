#include "db_storage.hpp"
#include <lmdb.h>
#include <assert.h>
#include <stdexcept>
#include <vector>
#include <string_view>
#include <optional>

struct db_tx
{
    MDB_txn* parent_transaction = nullptr;
    MDB_txn* transaction = nullptr;

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

struct db_data
{
    MDB_cursor* cursor = nullptr;
    std::string_view data;

    db_data(std::string_view _data, MDB_cursor* _cursor) : cursor(_cursor), data(_data){}
    ~db_data(){mdb_cursor_close(cursor);}
};

struct db_tx_read
{
    MDB_dbi dbi;

    db_tx_read(MDB_dbi _dbi) : dbi(_dbi){}

    std::optional<db_data> read_tx(const db_tx& tx, std::string_view skey)
    {
        MDB_val key = {skey.size(), const_cast<void*>((const void*)skey.data())};

        MDB_val data;

        MDB_cursor* cursor = nullptr;

        if(mdb_cursor_open(tx.transaction, dbi, &cursor) != 0)
            throw std::runtime_error("Bad Cursor");

        if(mdb_cursor_get(cursor, &key, &data, MDB_SET_KEY) != 0)
        {
            mdb_cursor_close(cursor);

            return std::nullopt;
        }

        return db_data({(const char*)data.mv_data, data.mv_size}, cursor);
    }
};

struct db_tx_read_write : db_tx_read
{
    db_tx_read_write(MDB_dbi _dbi) : db_tx_read(_dbi){}

    void write_tx(const db_tx& tx, std::string_view skey, std::string_view sdata)
    {
        MDB_val key = {skey.size(), const_cast<void*>((const void*)skey.data())};
        MDB_val data = {sdata.size(), const_cast<void*>((const void*)sdata.data())};

        if(mdb_put(tx.transaction, dbi, &key, &data, 0) != 0)
        {
            throw std::runtime_error("Write error");
        }
    }
};

struct db_read : db_tx
{
    db_tx_read mread;

    db_read(MDB_env* _env, MDB_dbi _dbi) : db_tx(_env, true), mread(_dbi) {}

    std::optional<db_data> read(std::string_view skey)
    {
        return mread.read_tx(*this, skey);
    }
};

struct db_read_write : db_tx
{
    db_tx_read_write mwrite;

    db_read_write(MDB_env* _env, MDB_dbi _dbi) : db_tx(_env, false), mwrite(_dbi) {}

    void write(std::string_view skey, std::string_view sdata)
    {
        return mwrite.write_tx(*this, skey, sdata);
    }
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
        mdb_env_set_mapsize(env, 10485760ull * 10000ull);

        assert(mdb_env_open(env, storage.c_str(), 0, 0) == 0);

        dbis.resize(db_count);

        for(int i=0; i < db_count; i++)
        {
            db_tx tx(env, false);

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
