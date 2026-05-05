#pragma once
#include <string>
#include <vector>
#include <variant>
#include <cstring>

using Value = std::variant<int, double, std::string>;

class Record {
public:
    Record() = default;
    explicit Record(const std::vector<Value>& values);
    
    void AddValue(const Value& value) { values_.push_back(value); }
    const std::vector<Value>& GetValues() const { return values_; }
    Value GetValue(size_t index) const;
    
    size_t GetSize() const;
    void Serialize(char* data) const;
    static Record Deserialize(const char* data, size_t& offset);
    
    std::string ToString() const;

private:
    std::vector<Value> values_;
};