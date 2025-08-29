#include "record.h"
#include <sstream>

Record::Record(const std::vector<Value>& values) : values_(values) {}

Value Record::GetValue(size_t index) const {
    if (index >= values_.size()) {
        return 0;
    }
    return values_[index];
}

size_t Record::GetSize() const {
    size_t size = sizeof(size_t);
    for (const auto& value : values_) {
        size += sizeof(uint8_t);
        std::visit([&size](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int> || std::is_same_v<T, double>) {
                size += sizeof(T);
            } else {
                size += sizeof(size_t) + v.length();
            }
        }, value);
    }
    return size;
}

void Record::Serialize(char* data) const {
    size_t offset = 0;
    
    size_t count = values_.size();
    std::memcpy(data + offset, &count, sizeof(count));
    offset += sizeof(count);
    
    for (const auto& value : values_) {
        std::visit([&](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int>) {
                uint8_t type = 0;
                std::memcpy(data + offset, &type, sizeof(type));
                offset += sizeof(type);
                std::memcpy(data + offset, &v, sizeof(v));
                offset += sizeof(v);
            } else if constexpr (std::is_same_v<T, double>) {
                uint8_t type = 1;
                std::memcpy(data + offset, &type, sizeof(type));
                offset += sizeof(type);
                std::memcpy(data + offset, &v, sizeof(v));
                offset += sizeof(v);
            } else {
                uint8_t type = 2;
                std::memcpy(data + offset, &type, sizeof(type));
                offset += sizeof(type);
                size_t len = v.length();
                std::memcpy(data + offset, &len, sizeof(len));
                offset += sizeof(len);
                std::memcpy(data + offset, v.c_str(), len);
                offset += len;
            }
        }, value);
    }
}

Record Record::Deserialize(const char* data, size_t& offset) {
    Record record;
    
    size_t count;
    std::memcpy(&count, data + offset, sizeof(count));
    offset += sizeof(count);
    
    for (size_t i = 0; i < count; ++i) {
        uint8_t type;
        std::memcpy(&type, data + offset, sizeof(type));
        offset += sizeof(type);
        
        if (type == 0) {
            int value;
            std::memcpy(&value, data + offset, sizeof(value));
            offset += sizeof(value);
            record.AddValue(value);
        } else if (type == 1) {
            double value;
            std::memcpy(&value, data + offset, sizeof(value));
            offset += sizeof(value);
            record.AddValue(value);
        } else {
            size_t len;
            std::memcpy(&len, data + offset, sizeof(len));
            offset += sizeof(len);
            std::string value(data + offset, len);
            offset += len;
            record.AddValue(value);
        }
    }
    
    return record;
}

std::string Record::ToString() const {
    std::ostringstream oss;
    oss << "(";
    for (size_t i = 0; i < values_.size(); ++i) {
        if (i > 0) oss << ", ";
        std::visit([&oss](const auto& v) {
            oss << v;
        }, values_[i]);
    }
    oss << ")";
    return oss.str();
}