#include "db_storage.hpp"
#include <liblmdb/lmdb.h>
#include <assert.h>
#include <stdexcept>
#include <vector>
#include <string_view>
#include <optional>
#include <sstream>
#include <direct.h>

#define CHECK_THROW(x) if(const int rc = x) { throw std::runtime_error("DB Error " + std::to_string(rc));}
#define CHECK_ASSERT(x) if(const int rc = x) {printf("DB Error %i %s" + rc, #x); assert(false && #x);}

inline
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems)
{
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

inline
std::vector<std::string> split(const std::string &s, char delim)
{
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

struct db_backend
{
    MDB_env* env = nullptr;
    std::string storage;
    int db_count = 0;

    std::vector<MDB_dbi> dbis;

    db_backend(const std::string& _storage, int _db_count) : storage(_storage), db_count(_db_count)
    {
        std::vector<std::string> dirs = split(storage, '/');

        for(auto& i : dirs)
        {
            if(i == ".")
                continue;

            _mkdir(i.c_str());
        }

        CHECK_ASSERT(mdb_env_create(&env));

        mdb_env_set_maxdbs(env, 50);

        ///10000 MB
        mdb_env_set_mapsize(env, 10485760ull * 10000ull);

        CHECK_ASSERT(mdb_env_open(env, storage.c_str(), 0, 0));

        dbis.resize(db_count);

        for(int i=0; i < db_count; i++)
        {
            MDB_txn* transaction = nullptr;

            CHECK_ASSERT(mdb_txn_begin(env, nullptr, 0, &transaction));

            CHECK_ASSERT(mdb_dbi_open(transaction, std::to_string(i).c_str(), MDB_CREATE, &dbis[i]));

            mdb_txn_commit(transaction);
        }
    }

    ~db_backend()
    {
        mdb_env_close(env);
    }

    MDB_dbi get_db(int id) const
    {
        if(id < 0 || id >= (int)dbis.size())
            throw std::runtime_error("Bad db id");

        return dbis[id];
    }
};

struct db_tx
{
    MDB_txn* parent_transaction = nullptr;
    MDB_txn* transaction = nullptr;

    bool read_only = false;

    db_tx(const db_backend& db, bool _read_only) : read_only(_read_only)
    {
        CHECK_THROW(mdb_txn_begin(db.env, parent_transaction, read_only ? MDB_RDONLY : 0, &transaction));
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

        CHECK_THROW(mdb_cursor_open(tx.transaction, dbi, &cursor));

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

        CHECK_THROW(mdb_put(tx.transaction, dbi, &key, &data, 0));
    }

    void del_tx(const db_tx& tx, std::string_view skey)
    {
        MDB_val key = {skey.size(), const_cast<void*>((const void*)skey.data())};

        CHECK_THROW(mdb_del(tx.transaction, dbi, &key, nullptr));
    }
};

struct db_read : db_tx
{
    db_tx_read mread;

    db_read(const db_backend& db, int db_id) : db_tx(db, true), mread(db.get_db(db_id)) {}

    std::optional<db_data> read(std::string_view skey)
    {
        return mread.read_tx(*this, skey);
    }
};

struct db_read_write : db_tx
{
    db_tx_read_write mwrite;

    db_read_write(const db_backend& db, int db_id) : db_tx(db, false), mwrite(db.get_db(db_id)) {}

    std::optional<db_data> read(std::string_view skey)
    {
        return mwrite.read_tx(*this, skey);
    }

    void write(std::string_view skey, std::string_view sdata)
    {
        return mwrite.write_tx(*this, skey, sdata);
    }

    void del(std::string_view skey)
    {
        return mwrite.del_tx(*this, skey);
    }
};

void db_tests()
{
    db_backend simple_test("./test_db", 1);

    {
        {
            db_read_write write_tx(simple_test, 0);

            write_tx.write("key_1", "mydataboy");
        }

        {
            db_read read_tx(simple_test, 0);

            auto opt = read_tx.read("key_1");

            assert(opt.has_value() && opt.value().data == "mydataboy");
        }
    }
}
