#include "database.h"
#include <iostream>

int main() {
    std::cout << "Simple Database Engine - Full Demo" << std::endl;
    
    Database db("demo.db");
    
    std::cout << "\n=== Creating Table ===" << std::endl;
    db.ExecuteQuery("CREATE TABLE users (id INT, name VARCHAR, age INT)");
    
    std::cout << "\n=== Inserting Data ===" << std::endl;
    std::cout << "Inserting record 1..." << std::endl;
    db.ExecuteQuery("INSERT INTO users VALUES (1, 'Alice', 25)");
    std::cout << "Inserting record 2..." << std::endl;
    db.ExecuteQuery("INSERT INTO users VALUES (2, 'Bob', 30)");
    std::cout << "Inserting record 3..." << std::endl;
    db.ExecuteQuery("INSERT INTO users VALUES (3, 'Charlie', 35)");
    
    std::cout << "\n=== Selecting All Records ===" << std::endl;
    if (db.ExecuteQuery("SELECT * FROM users")) {
        auto results = db.GetLastResults();
        std::cout << "Found " << results.size() << " records:" << std::endl;
        for (const auto& record : results) {
            std::cout << "  " << record.ToString() << std::endl;
        }
    }
    
    std::cout << "\n=== Selecting with WHERE Clause ===" << std::endl;
    if (db.ExecuteQuery("SELECT * FROM users WHERE id = 2")) {
        auto results = db.GetLastResults();
        std::cout << "Found " << results.size() << " records:" << std::endl;
        for (const auto& record : results) {
            std::cout << "  " << record.ToString() << std::endl;
        }
    }

    std::cout << "\n=== Deleting Record id = 2 ===" << std::endl;
    db.ExecuteQuery("DELETE FROM users WHERE id = 2");
    if (db.ExecuteQuery("SELECT * FROM users")) {
        auto results = db.GetLastResults();
        std::cout << "After delete, found " << results.size() << " records:" << std::endl;
        for (const auto& record : results) {
            std::cout << "  " << record.ToString() << std::endl;
        }
    }

    std::cout << "\n=== Bulk Insert (BTree split) ===" << std::endl;
    db.ExecuteQuery("CREATE TABLE big (id INT, val INT)");
    for (int i = 0; i < 50; ++i) {
        std::string sql = "INSERT INTO big VALUES (" + std::to_string(i) + ", " +
                          std::to_string(i * 10) + ")";
        db.ExecuteQuery(sql);
    }
    if (db.ExecuteQuery("SELECT * FROM big")) {
        auto results = db.GetLastResults();
        std::cout << "Bulk table now contains " << results.size() << " records" << std::endl;
    }
    if (db.ExecuteQuery("SELECT * FROM big WHERE id = 37")) {
        auto results = db.GetLastResults();
        std::cout << "Lookup id=37 returned " << results.size() << " record(s)";
        if (!results.empty()) std::cout << ": " << results[0].ToString();
        std::cout << std::endl;
    }

    std::cout << "\n=== Database Demo Complete ===" << std::endl;
    return 0;
}