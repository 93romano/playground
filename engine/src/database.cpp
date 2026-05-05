#include "database.h"
#include <iostream>

Database::Database(const std::string& db_file) {
    storage_manager_ = std::make_unique<StorageManager>(db_file);
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(50, storage_manager_.get());
    parser_ = std::make_unique<SQLParser>();
}

bool Database::ExecuteQuery(const std::string& sql) {
    auto query = parser_->Parse(sql);
    if (!query) {
        std::cerr << "Failed to parse query: " << sql << std::endl;
        return false;
    }

    last_results_.clear();

    switch (query->type) {
        case QueryType::SELECT:
            return ExecuteSelect(*query);
        case QueryType::INSERT:
            return ExecuteInsert(*query);
        case QueryType::CREATE_TABLE:
            return ExecuteCreateTable(*query);
        default:
            std::cerr << "Unsupported query type" << std::endl;
            return false;
    }
}

bool Database::ExecuteSelect(const Query& query) {
    auto table_it = tables_.find(query.table_name);
    if (table_it == tables_.end()) {
        std::cerr << "Table not found: " << query.table_name << std::endl;
        return false;
    }

    Table& table = *table_it->second;
    
    if (query.conditions.empty()) {
        auto all_records = table.index->RangeScan(INT_MIN, INT_MAX);
        for (const auto& record : all_records) {
            bool matches = true;
            for (const auto& condition : query.conditions) {
                if (!EvaluateCondition(record, condition, table)) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                last_results_.push_back(record);
            }
        }
    } else {
        for (const auto& condition : query.conditions) {
            if (condition.column == "id" && condition.op == "=") {
                int key = std::get<int>(condition.value);
                Record record;
                if (table.index->Search(key, record)) {
                    bool matches = true;
                    for (const auto& cond : query.conditions) {
                        if (!EvaluateCondition(record, cond, table)) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        last_results_.push_back(record);
                    }
                }
                break;
            }
        }
    }

    return true;
}

bool Database::ExecuteInsert(const Query& query) {
    auto table_it = tables_.find(query.table_name);
    if (table_it == tables_.end()) {
        std::cerr << "Table not found: " << query.table_name << std::endl;
        return false;
    }

    Table& table = *table_it->second;
    
    if (query.values.size() != table.columns.size()) {
        std::cerr << "Value count doesn't match column count" << std::endl;
        return false;
    }

    Record record(query.values);
    
    int key = std::get<int>(query.values[0]);
    
    return table.index->Insert(key, record);
}

bool Database::ExecuteCreateTable(const Query& query) {
    if (tables_.find(query.table_name) != tables_.end()) {
        std::cerr << "Table already exists: " << query.table_name << std::endl;
        return false;
    }

    auto table = std::make_unique<Table>();
    table->name = query.table_name;
    table->columns = query.table_columns;
    table->index = std::make_unique<BTree>(buffer_pool_manager_.get());

    tables_[query.table_name] = std::move(table);
    
    std::cout << "Table created: " << query.table_name << std::endl;
    return true;
}

bool Database::EvaluateCondition(const Record& record, const Condition& condition, const Table& table) {
    int column_index = GetColumnIndex(condition.column, table);
    if (column_index == -1) {
        return false;
    }

    Value record_value = GetRecordValue(record, column_index);
    
    if (condition.op == "=") {
        return record_value == condition.value;
    } else if (condition.op == ">") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(condition.value)) {
            return std::get<int>(record_value) > std::get<int>(condition.value);
        } else if (std::holds_alternative<double>(record_value) && std::holds_alternative<double>(condition.value)) {
            return std::get<double>(record_value) > std::get<double>(condition.value);
        }
        return false;
    } else if (condition.op == "<") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(condition.value)) {
            return std::get<int>(record_value) < std::get<int>(condition.value);
        } else if (std::holds_alternative<double>(record_value) && std::holds_alternative<double>(condition.value)) {
            return std::get<double>(record_value) < std::get<double>(condition.value);
        }
        return false;
    }
    
    return false;
}

int Database::GetColumnIndex(const std::string& column_name, const Table& table) {
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == column_name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

Value Database::GetRecordValue(const Record& record, int column_index) {
    return record.GetValue(column_index);
}