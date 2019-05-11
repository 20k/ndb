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

///parent/child transactions are hard to get right. It only works for write transactions
///which means that if i'm in a read and then I commit a write child, i'd have to abort the parent read
///create a new tx for the parent, child my write to that, and then make all children write transactions
///additionally each transaction can only have one child, so would need to basically create a linked list of transactions
///not 100% sure what it'd be like for perf because might end up with very long transaction chains
///(due to using as fine a grain of transactions as possible)

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

    db_data(const db_data&) = delete;
    db_data& operator=(const db_data&) = delete;
    db_data& operator=(db_data&&) = delete;

    db_data(db_data&&);
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

    db_tx(const db_tx&) = delete;
    db_tx& operator=(const db_tx&) = delete;
    db_tx(db_tx&&) = delete;
    db_tx& operator=(db_tx&&) = delete;

    MDB_txn* get();
};

struct db_read
{
    db_tx dtx;
    MDB_dbi dbid;

    db_read(const db_backend& db, int _db_id);
    db_read(const db_backend& db, int _db_id, bool is_read_only); ///HACK

    std::optional<db_data> read(std::string_view skey);
};

///this needs to inherit from db_read so we can use it polymorphically
struct db_read_write : db_read
{
    db_read_write(const db_backend& db, int _db_id);

    void write(std::string_view skey, std::string_view sdata);
    void del(std::string_view skey);
};

void db_tests();

void set_num_dbs(int num);
db_backend& get_db();

#endif // DB_STORAGE_HPP_INCLUDED
