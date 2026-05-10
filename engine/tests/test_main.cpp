#include "btree.h"
#include "buffer_pool_manager.h"
#include "database.h"
#include "page.h"
#include "record.h"
#include "storage_manager.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

namespace {

int g_pass = 0;
int g_fail = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        if (cond) {                                                       \
            ++g_pass;                                                     \
        } else {                                                          \
            ++g_fail;                                                     \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":"   \
                      << __LINE__ << std::endl;                           \
        }                                                                 \
    } while (0)

void TestPage() {
    Page page(7);
    CHECK(page.GetPageId() == 7u);
    CHECK(!page.IsDirty());

    const char* msg = "hello";
    std::memcpy(page.GetData(), msg, std::strlen(msg));
    page.SetDirty(true);

    CHECK(page.IsDirty());
    CHECK(std::strncmp(page.GetData(), "hello", 5) == 0);
}

void TestStorageManager() {
    const std::string path = "test_storage.db";
    std::remove(path.c_str());

    {
        StorageManager sm(path);

        page_id_t a = sm.AllocatePage();
        page_id_t b = sm.AllocatePage();
        CHECK(b == a + 1);

        Page p(a);
        std::memcpy(p.GetData(), "alpha", 5);
        CHECK(sm.WritePage(p));

        Page q(b);
        std::memcpy(q.GetData(), "beta", 4);
        CHECK(sm.WritePage(q));

        auto pa = sm.ReadPage(a);
        CHECK(std::strncmp(pa->GetData(), "alpha", 5) == 0);

        auto pb = sm.ReadPage(b);
        CHECK(std::strncmp(pb->GetData(), "beta", 4) == 0);
    }

    {
        StorageManager sm(path);
        auto pa = sm.ReadPage(0);
        CHECK(std::strncmp(pa->GetData(), "alpha", 5) == 0);
    }

    std::remove(path.c_str());
}

void TestBTreeSplitAndSearch() {
    const std::string path = "test_btree.db";
    std::remove(path.c_str());

    StorageManager sm(path);
    BufferPoolManager bpm(50, &sm);
    BTree tree(&bpm);

    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        Record r;
        r.AddValue(i);
        r.AddValue(i * 10);
        CHECK(tree.Insert(i, r));
    }

    for (int i = 0; i < N; ++i) {
        Record r;
        CHECK(tree.Search(i, r));
    }

    auto range = tree.RangeScan(10, 19);
    CHECK(range.size() == 10u);

    CHECK(tree.Delete(50));
    Record r;
    CHECK(!tree.Search(50, r));

    std::remove(path.c_str());
}

void TestSqlEndToEnd() {
    const std::string path = "test_sql.db";
    std::remove(path.c_str());

    Database db(path);
    CHECK(db.ExecuteQuery("CREATE TABLE t (id INT, val INT)"));
    for (int i = 0; i < 10; ++i) {
        std::string sql = "INSERT INTO t VALUES (" + std::to_string(i) + ", " +
                          std::to_string(i * 2) + ")";
        CHECK(db.ExecuteQuery(sql));
    }
    CHECK(db.ExecuteQuery("SELECT * FROM t"));
    CHECK(db.GetLastResults().size() == 10u);

    CHECK(db.ExecuteQuery("SELECT * FROM t WHERE id = 4"));
    CHECK(db.GetLastResults().size() == 1u);

    CHECK(db.ExecuteQuery("DELETE FROM t WHERE id = 4"));
    CHECK(db.ExecuteQuery("SELECT * FROM t"));
    CHECK(db.GetLastResults().size() == 9u);

    CHECK(db.ExecuteQuery("SELECT * FROM t WHERE id = 4"));
    CHECK(db.GetLastResults().empty());

    std::remove(path.c_str());
}

}  // namespace

int main() {
    TestPage();
    TestStorageManager();
    TestBTreeSplitAndSearch();
    TestSqlEndToEnd();

    std::cout << "Passed: " << g_pass << ", Failed: " << g_fail << std::endl;
    return g_fail == 0 ? 0 : 1;
}
