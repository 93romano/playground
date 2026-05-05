#pragma once
#include "storage_manager.h"
#include "buffer_pool_manager.h"
#include "btree.h"
#include "sql_parser.h"
#include <unordered_map>
#include <memory>

struct Table {
    std::string name;
    std::vector<Column> columns;
    std::unique_ptr<BTree> index;
};

class Database {
public:
    explicit Database(const std::string& db_file);
    ~Database() = default;

    bool ExecuteQuery(const std::string& sql);
    std::vector<Record> GetLastResults() const { return last_results_; }

private:
    std::unique_ptr<StorageManager> storage_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<SQLParser> parser_;
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    std::vector<Record> last_results_;

    bool ExecuteSelect(const Query& query);
    bool ExecuteInsert(const Query& query);
    bool ExecuteCreateTable(const Query& query);
    
    bool EvaluateCondition(const Record& record, const Condition& condition, const Table& table);
    int GetColumnIndex(const std::string& column_name, const Table& table);
    Value GetRecordValue(const Record& record, int column_index);
};