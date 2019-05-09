#ifndef DB_STORAGE_HPP_INCLUDED
#define DB_STORAGE_HPP_INCLUDED

#include <optional>
#include <string_view>

typedef unsigned int MDB_dbi;
typedef struct MDB_txn MDB_txn;
typedef struct MDB_cursor MDB_cursor;

struct db_backend;

///so, lmdb will abort committed children if you abort their parent transaction
///if I can find some way to throw a special exception that aborts ongoing transactions
///rather than having to cancel them manually, might save me a LOT of error checking

struct db_exception : public std::exception
{
    db_exception();
    ~db_exception();

    const char* what() const noexcept
    {
        return "Db Exception";
    }

};

struct db_data
{
    MDB_cursor* cursor = nullptr;
    std::string_view data;

    db_data(std::string_view _data, MDB_cursor* _cursor);
    ~db_data();
};

struct db_tx
{
    MDB_txn* last_parent_transaction = nullptr;
    MDB_txn* transaction = nullptr;

    bool read_only = false;

    db_tx(const db_backend& db, bool _read_only);
    ~db_tx();

    db_tx(const db_tx&) = delete; // no copies
    db_tx& operator=(const db_tx&) = delete; // no self-assignments
    db_tx(db_tx&&) = delete; // WHY?
    db_tx& operator=(db_tx&&) = delete; // WHY?

    MDB_txn* get();
};

struct db_read
{
    db_tx dtx;
    MDB_dbi dbid;

    db_read(const db_backend& db, int _db_id);

    std::optional<db_data> read(std::string_view skey);
};

struct db_read_write
{
    db_tx dtx;
    MDB_dbi dbid;

    db_read_write(const db_backend& db, int _db_id);

    std::optional<db_data> read(std::string_view skey);
    void write(std::string_view skey, std::string_view sdata);
    void del(std::string_view skey);
};

void db_tests();

db_backend& get_db();

#endif // DB_STORAGE_HPP_INCLUDED
