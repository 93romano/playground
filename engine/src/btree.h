#pragma once
#include "page.h"
#include "buffer_pool_manager.h"
#include "record.h"
#include <vector>
#include <memory>

constexpr int BTREE_ORDER = 4;

struct BTreeNode {
    bool is_leaf{false};
    std::vector<int> keys;
    std::vector<page_id_t> children;
    std::vector<Record> records;
    page_id_t next_leaf{INVALID_PAGE_ID};
    
    static constexpr page_id_t INVALID_PAGE_ID = static_cast<page_id_t>(-1);
};

class BTree {
public:
    explicit BTree(BufferPoolManager* buffer_pool_manager);
    ~BTree() = default;

    bool Insert(int key, const Record& record);
    bool Search(int key, Record& record);
    bool Delete(int key);
    
    std::vector<Record> RangeScan(int start_key, int end_key);

private:
    BufferPoolManager* buffer_pool_manager_;
    page_id_t root_page_id_{BTreeNode::INVALID_PAGE_ID};

    BTreeNode* GetNode(page_id_t page_id);
    void SerializeNode(const BTreeNode& node, Page* page);
    BTreeNode DeserializeNode(Page* page);
    
    page_id_t CreateNewNode(bool is_leaf);
    bool InsertIntoLeaf(BTreeNode& leaf, int key, const Record& record);
    bool InsertIntoInternal(BTreeNode& internal, int key, page_id_t child_page_id);
    
    void SplitLeafNode(page_id_t leaf_page_id, int key, const Record& record);
    void SplitInternalNode(page_id_t internal_page_id, int key, page_id_t child_page_id);
    
    page_id_t FindLeafPage(int key);
    int FindKeyIndex(const std::vector<int>& keys, int key);
};