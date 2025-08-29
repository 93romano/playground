#include "sql_parser.h"
#include <sstream>
#include <algorithm>
#include <cctype>

std::unique_ptr<Query> SQLParser::Parse(const std::string& sql) {
    auto tokens = Tokenize(sql);
    if (tokens.empty()) {
        return nullptr;
    }
    
    std::string command = ToUpper(tokens[0]);
    
    if (command == "SELECT") {
        return ParseSelect(tokens);
    } else if (command == "INSERT") {
        return ParseInsert(tokens);
    } else if (command == "CREATE") {
        return ParseCreateTable(tokens);
    }
    
    return nullptr;
}

std::vector<std::string> SQLParser::Tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::istringstream iss(sql);
    std::string token;
    
    while (iss >> token) {
        if (token.back() == ',' || token.back() == ';') {
            std::string main_token = token.substr(0, token.length() - 1);
            if (!main_token.empty()) {
                tokens.push_back(main_token);
            }
            tokens.push_back(std::string(1, token.back()));
        } else if (token.front() == '(' || token.back() == ')') {
            if (token.front() == '(') {
                tokens.push_back("(");
                token = token.substr(1);
            }
            if (token.back() == ')') {
                std::string main_token = token.substr(0, token.length() - 1);
                if (!main_token.empty()) {
                    tokens.push_back(main_token);
                }
                tokens.push_back(")");
            } else if (!token.empty()) {
                tokens.push_back(token);
            }
        } else {
            tokens.push_back(token);
        }
    }
    
    return tokens;
}

std::unique_ptr<Query> SQLParser::ParseSelect(const std::vector<std::string>& tokens) {
    auto query = std::make_unique<Query>();
    query->type = QueryType::SELECT;
    
    size_t i = 1;
    
    while (i < tokens.size() && ToUpper(tokens[i]) != "FROM") {
        if (tokens[i] != ",") {
            query->columns.push_back(tokens[i]);
        }
        i++;
    }
    
    if (i < tokens.size() && ToUpper(tokens[i]) == "FROM") {
        i++;
        if (i < tokens.size()) {
            query->table_name = tokens[i];
            i++;
        }
    }
    
    if (i < tokens.size() && ToUpper(tokens[i]) == "WHERE") {
        i++;
        while (i + 2 < tokens.size()) {
            Condition condition;
            condition.column = tokens[i];
            condition.op = tokens[i + 1];
            condition.value = ParseValue(tokens[i + 2]);
            query->conditions.push_back(condition);
            
            i += 3;
            if (i < tokens.size() && ToUpper(tokens[i]) == "AND") {
                i++;
            } else {
                break;
            }
        }
    }
    
    return query;
}

std::unique_ptr<Query> SQLParser::ParseInsert(const std::vector<std::string>& tokens) {
    auto query = std::make_unique<Query>();
    query->type = QueryType::INSERT;
    
    size_t i = 1;
    if (i < tokens.size() && ToUpper(tokens[i]) == "INTO") {
        i++;
        if (i < tokens.size()) {
            query->table_name = tokens[i];
            i++;
        }
    }
    
    if (i < tokens.size() && ToUpper(tokens[i]) == "VALUES") {
        i++;
        if (i < tokens.size() && tokens[i] == "(") {
            i++;
            while (i < tokens.size() && tokens[i] != ")") {
                if (tokens[i] != ",") {
                    query->values.push_back(ParseValue(tokens[i]));
                }
                i++;
            }
        }
    }
    
    return query;
}

std::unique_ptr<Query> SQLParser::ParseCreateTable(const std::vector<std::string>& tokens) {
    auto query = std::make_unique<Query>();
    query->type = QueryType::CREATE_TABLE;
    
    size_t i = 1;
    if (i < tokens.size() && ToUpper(tokens[i]) == "TABLE") {
        i++;
        if (i < tokens.size()) {
            query->table_name = tokens[i];
            i++;
        }
    }
    
    if (i < tokens.size() && tokens[i] == "(") {
        i++;
        while (i + 1 < tokens.size() && tokens[i] != ")") {
            if (tokens[i] != ",") {
                Column col;
                col.name = tokens[i];
                col.type = tokens[i + 1];
                query->table_columns.push_back(col);
                i += 2;
            } else {
                i++;
            }
        }
    }
    
    return query;
}

std::string SQLParser::ToUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

bool SQLParser::IsNumber(const std::string& str) {
    if (str.empty()) return false;
    
    size_t start = 0;
    if (str[0] == '-' || str[0] == '+') start = 1;
    if (start >= str.length()) return false;
    
    bool has_dot = false;
    for (size_t i = start; i < str.length(); ++i) {
        if (str[i] == '.') {
            if (has_dot) return false;
            has_dot = true;
        } else if (!std::isdigit(str[i])) {
            return false;
        }
    }
    
    return true;
}

Value SQLParser::ParseValue(const std::string& str) {
    if (IsNumber(str)) {
        if (str.find('.') != std::string::npos) {
            return std::stod(str);
        } else {
            return std::stoi(str);
        }
    }
    
    if (str.length() >= 2 && str.front() == '\'' && str.back() == '\'') {
        return str.substr(1, str.length() - 2);
    }
    
    return str;
}