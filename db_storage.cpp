#include "db_storage.hpp"
#include <lmdb.h>
#include <assert.h>
#include <stdexcept>
#include <vector>
#include <string_view>
#include <optional>
#include <sstream>
#include <iostream>

#ifdef PERF
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif // PERF

#define CHECK_THROW(x) if(const int rc = x) { std::cout << rc << std::endl; throw std::runtime_error("DB Error " + std::to_string(rc));}
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
        /*std::vector<std::string> dirs = split(storage, '/');

        for(auto& i : dirs)
        {
            if(i == ".")
                continue;

            _mkdir(i.c_str());
        }*/

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

            CHECK_ASSERT(mdb_txn_commit(transaction));
        }
    }

    ~db_backend()
    {
        for(int i=0; i < (int)dbis.size(); i++)
        {
            mdb_dbi_close(env, dbis[i]);
        }

        mdb_env_close(env);
    }

    MDB_dbi get_db(int id) const
    {
        if(id < 0 || id >= (int)dbis.size())
            throw std::runtime_error("Bad db id");

        return dbis[id];
    }
};

MDB_txn*& get_parent_transaction()
{
    static thread_local MDB_txn* parent = nullptr;

    return parent;
}

bool& db_abort_in_flight()
{
    static thread_local bool db_abort;

    return db_abort;
}

db_exception::db_exception()
{
    db_abort_in_flight() = true;
}

db_exception::~db_exception()
{
    db_abort_in_flight() = false;
}

db_tx::db_tx(const db_backend& db, bool _read_only) : read_only(_read_only)
{
    MDB_txn*& parent = get_parent_transaction();

    CHECK_THROW(mdb_txn_begin(db.env, nullptr, read_only ? MDB_RDONLY : 0, &transaction));

    last_parent_transaction = parent;
    parent = transaction;
}

db_tx::~db_tx()
{
    if(db_abort_in_flight())
    {
        mdb_txn_abort(transaction);
    }
    else
    {
        CHECK_ASSERT(mdb_txn_commit(transaction));
    }

    MDB_txn*& parent = get_parent_transaction();

    parent = last_parent_transaction;
}

MDB_txn* db_tx::get()
{
    return transaction;
}

db_data::db_data(std::string_view _data, MDB_cursor* _cursor) : cursor(_cursor), data(_data){}

db_data::db_data(db_data&& other)
{
    data = other.data;
    cursor = other.cursor;

    other.cursor = nullptr;
}

db_data::~db_data()
{
    if(cursor == nullptr)
        return;

    mdb_cursor_close(cursor);
}

std::optional<db_data> read_tx(MDB_dbi dbi, const db_tx& tx, std::string_view skey)
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

void write_tx(MDB_dbi dbi, const db_tx& tx, std::string_view skey, std::string_view sdata)
{
    MDB_val key = {skey.size(), const_cast<void*>((const void*)skey.data())};
    MDB_val data = {sdata.size(), const_cast<void*>((const void*)sdata.data())};

    CHECK_THROW(mdb_put(tx.transaction, dbi, &key, &data, 0));
}

void del_tx(MDB_dbi dbi, const db_tx& tx, std::string_view skey)
{
    MDB_val key = {skey.size(), const_cast<void*>((const void*)skey.data())};

    CHECK_THROW(mdb_del(tx.transaction, dbi, &key, nullptr));
}

db_read::db_read(const db_backend& db, int _db_id) : dtx(db, true), dbid(db.get_db(_db_id)) {}
db_read::db_read(const db_backend& db, int _db_id, bool is_read_only) : dtx(db, is_read_only), dbid(db.get_db(_db_id)) {}

std::optional<db_data> db_read::read(std::string_view skey)
{
    return read_tx(dbid, dtx, skey);
}

db_read_write::db_read_write(const db_backend& db, int _db_id) : db_read(db, _db_id, false) {}

void db_read_write::write(std::string_view skey, std::string_view sdata)
{
    return write_tx(dbid, dtx, skey, sdata);
}

void db_read_write::del(std::string_view skey)
{
    return del_tx(dbid, dtx, skey);
}

struct dtime
{
    #ifdef PERF
    LARGE_INTEGER freq;
    LARGE_INTEGER start;
    #endif // PERF

    dtime()
    {
        #ifdef PERF
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);
        #endif // PERF
    }

    ~dtime()
    {
        #ifdef PERF
        LARGE_INTEGER fin;

        QueryPerformanceCounter(&fin);

        size_t diff = fin.QuadPart - start.QuadPart;

        std::cout << "Timer ms " << (double)diff / ((double)freq.QuadPart / 1000.) << std::endl;
        #endif // PERF
    }
};

void db_tests()
{
    db_backend simple_test("./test_db", 3);

    {
        {
            dtime tm;

            db_read_write write_tx(simple_test, 0);

            write_tx.write("key_1", "mydataboy");
        }

        {
            dtime tm;

            db_read read_tx(simple_test, 0);

            auto opt = read_tx.read("key_1");

            assert(opt.has_value() && opt.value().data == "mydataboy");
        }

        {
            db_read_write write_tx(simple_test, 0);

            write_tx.del("key_1");
        }

        {
            db_read read_tx(simple_test, 0);

            assert(!read_tx.read("key_1").has_value());
        }
    }

    {
        {
            dtime tm;

            db_read_write write_tx(simple_test, 1);

            for(int i=0; i < 1000*1000; i++)
            {
                write_tx.write(std::to_string(i), "dfdf");
            }
        }
    }

    ///check db polymorphism
    {
        dtime tm;

        {
            db_read_write write_tx(simple_test, 2);

            write_tx.write("test_key", "1234");
        }

        {
            db_read_write read_tx(simple_test, 2);

            db_read& poly = read_tx;

            auto opt = poly.read("test_key");

            assert(opt.has_value() && opt.value().data == "1234");
        }
    }

    /*
    ///test parent_txns
    {
        db_read write_tx(simple_test, 0);

        write_tx.write("key_2", "somedatahere");

        db_read read_tx(simple_test, 0);

        auto opt = read_tx.read("key_2");

        assert(opt.has_value());
    }*/

    printf("Tests success");
}

int& get_num_dbs()
{
    static int num = 1;

    return num;
}

void set_num_dbs(int num)
{
    get_num_dbs() = num;
}

db_backend& get_db()
{
    static db_backend bck("./prod_db", get_num_dbs());

    return bck;
}
