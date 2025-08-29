#pragma once
#include "record.h"
#include <string>
#include <vector>
#include <memory>

enum class QueryType {
    SELECT,
    INSERT,
    DELETE,
    CREATE_TABLE,
    UNKNOWN
};

struct Column {
    std::string name;
    std::string type;
};

struct Condition {
    std::string column;
    std::string op;
    Value value;
};

struct Query {
    QueryType type{QueryType::UNKNOWN};
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<Value> values;
    std::vector<Condition> conditions;
    std::vector<Column> table_columns;
};

class SQLParser {
public:
    SQLParser() = default;
    ~SQLParser() = default;

    std::unique_ptr<Query> Parse(const std::string& sql);

private:
    std::vector<std::string> Tokenize(const std::string& sql);
    std::string ToUpper(const std::string& str);
    bool IsNumber(const std::string& str);
    Value ParseValue(const std::string& str);
    
    std::unique_ptr<Query> ParseSelect(const std::vector<std::string>& tokens);
    std::unique_ptr<Query> ParseInsert(const std::vector<std::string>& tokens);
    std::unique_ptr<Query> ParseCreateTable(const std::vector<std::string>& tokens);
};