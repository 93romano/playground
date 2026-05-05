#include "btree.h"
#include <algorithm>
#include <cstring>

BTree::BTree(BufferPoolManager* buffer_pool_manager) 
    : buffer_pool_manager_(buffer_pool_manager) {
    root_page_id_ = CreateNewNode(true);
}

bool BTree::Insert(int key, const Record& record) {
    if (root_page_id_ == BTreeNode::INVALID_PAGE_ID) {
        root_page_id_ = CreateNewNode(true);
    }

    std::vector<page_id_t> path;
    page_id_t current = root_page_id_;
    while (true) {
        Page* page = buffer_pool_manager_->FetchPage(current);
        if (!page) return false;
        BTreeNode node = DeserializeNode(page);
        buffer_pool_manager_->UnpinPage(current, false);
        if (node.is_leaf) break;
        path.push_back(current);
        int idx = FindKeyIndex(node.keys, key);
        current = node.children[idx];
    }
    page_id_t leaf_page_id = current;

    Page* leaf_page = buffer_pool_manager_->FetchPage(leaf_page_id);
    if (!leaf_page) return false;
    BTreeNode leaf = DeserializeNode(leaf_page);

    auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
    if (it != leaf.keys.end() && *it == key) {
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
        return false;
    }

    if (leaf.keys.size() < static_cast<size_t>(BTREE_ORDER - 1)) {
        InsertIntoLeaf(leaf, key, record);
        SerializeNode(leaf, leaf_page);
        buffer_pool_manager_->UnpinPage(leaf_page_id, true);
        return true;
    }

    int idx = it - leaf.keys.begin();
    leaf.keys.insert(leaf.keys.begin() + idx, key);
    leaf.records.insert(leaf.records.begin() + idx, record);

    int mid = leaf.keys.size() / 2;
    page_id_t new_leaf_page_id = CreateNewNode(true);
    Page* new_leaf_page = buffer_pool_manager_->FetchPage(new_leaf_page_id);
    BTreeNode new_leaf = DeserializeNode(new_leaf_page);

    new_leaf.keys.assign(leaf.keys.begin() + mid, leaf.keys.end());
    new_leaf.records.assign(leaf.records.begin() + mid, leaf.records.end());
    new_leaf.next_leaf = leaf.next_leaf;
    leaf.keys.resize(mid);
    leaf.records.resize(mid);
    leaf.next_leaf = new_leaf_page_id;

    int promote_key = new_leaf.keys.front();
    SerializeNode(leaf, leaf_page);
    SerializeNode(new_leaf, new_leaf_page);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);

    page_id_t right_child = new_leaf_page_id;

    while (!path.empty()) {
        page_id_t parent_id = path.back();
        path.pop_back();

        Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
        if (!parent_page) return false;
        BTreeNode parent = DeserializeNode(parent_page);

        if (parent.keys.size() < static_cast<size_t>(BTREE_ORDER - 1)) {
            InsertIntoInternal(parent, promote_key, right_child);
            SerializeNode(parent, parent_page);
            buffer_pool_manager_->UnpinPage(parent_id, true);
            return true;
        }

        InsertIntoInternal(parent, promote_key, right_child);

        int pmid = parent.keys.size() / 2;
        int new_promote = parent.keys[pmid];

        page_id_t new_internal_id = CreateNewNode(false);
        Page* new_internal_page = buffer_pool_manager_->FetchPage(new_internal_id);
        BTreeNode new_internal = DeserializeNode(new_internal_page);

        new_internal.keys.assign(parent.keys.begin() + pmid + 1, parent.keys.end());
        new_internal.children.assign(parent.children.begin() + pmid + 1, parent.children.end());
        parent.keys.resize(pmid);
        parent.children.resize(pmid + 1);

        SerializeNode(parent, parent_page);
        SerializeNode(new_internal, new_internal_page);
        buffer_pool_manager_->UnpinPage(parent_id, true);
        buffer_pool_manager_->UnpinPage(new_internal_id, true);

        promote_key = new_promote;
        right_child = new_internal_id;
    }

    page_id_t new_root_id = CreateNewNode(false);
    Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
    BTreeNode new_root;
    new_root.is_leaf = false;
    new_root.keys.push_back(promote_key);
    new_root.children.push_back(root_page_id_);
    new_root.children.push_back(right_child);
    SerializeNode(new_root, new_root_page);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    root_page_id_ = new_root_id;

    return true;
}

bool BTree::Search(int key, Record& record) {
    if (root_page_id_ == BTreeNode::INVALID_PAGE_ID) {
        return false;
    }
    
    page_id_t leaf_page_id = FindLeafPage(key);
    Page* leaf_page = buffer_pool_manager_->FetchPage(leaf_page_id);
    if (!leaf_page) return false;
    
    BTreeNode leaf = DeserializeNode(leaf_page);
    
    auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
    if (it != leaf.keys.end() && *it == key) {
        int index = it - leaf.keys.begin();
        record = leaf.records[index];
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
        return true;
    }
    
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
}

bool BTree::Delete(int key) {
    if (root_page_id_ == BTreeNode::INVALID_PAGE_ID) {
        return false;
    }
    
    page_id_t leaf_page_id = FindLeafPage(key);
    Page* leaf_page = buffer_pool_manager_->FetchPage(leaf_page_id);
    if (!leaf_page) return false;
    
    BTreeNode leaf = DeserializeNode(leaf_page);
    
    auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
    if (it != leaf.keys.end() && *it == key) {
        int index = it - leaf.keys.begin();
        leaf.keys.erase(it);
        leaf.records.erase(leaf.records.begin() + index);
        
        SerializeNode(leaf, leaf_page);
        buffer_pool_manager_->UnpinPage(leaf_page_id, true);
        return true;
    }
    
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
}

std::vector<Record> BTree::RangeScan(int start_key, int end_key) {
    std::vector<Record> results;
    
    page_id_t leaf_page_id = FindLeafPage(start_key);
    
    while (leaf_page_id != BTreeNode::INVALID_PAGE_ID) {
        Page* leaf_page = buffer_pool_manager_->FetchPage(leaf_page_id);
        if (!leaf_page) break;
        
        BTreeNode leaf = DeserializeNode(leaf_page);
        
        for (size_t i = 0; i < leaf.keys.size(); ++i) {
            if (leaf.keys[i] >= start_key && leaf.keys[i] <= end_key) {
                results.push_back(leaf.records[i]);
            } else if (leaf.keys[i] > end_key) {
                buffer_pool_manager_->UnpinPage(leaf_page_id, false);
                return results;
            }
        }
        
        page_id_t next_page_id = leaf.next_leaf;
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
        leaf_page_id = next_page_id;
    }
    
    return results;
}

page_id_t BTree::CreateNewNode(bool is_leaf) {
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (!new_page) return BTreeNode::INVALID_PAGE_ID;
    
    BTreeNode node;
    node.is_leaf = is_leaf;
    SerializeNode(node, new_page);
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_page_id;
}

page_id_t BTree::FindLeafPage(int key) {
    page_id_t current_page_id = root_page_id_;
    
    while (current_page_id != BTreeNode::INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (!page) return BTreeNode::INVALID_PAGE_ID;
        
        BTreeNode node = DeserializeNode(page);
        
        if (node.is_leaf) {
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return current_page_id;
        }
        
        int index = FindKeyIndex(node.keys, key);
        page_id_t next_page_id = node.children[index];
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        current_page_id = next_page_id;
    }
    
    return BTreeNode::INVALID_PAGE_ID;
}

bool BTree::InsertIntoLeaf(BTreeNode& leaf, int key, const Record& record) {
    auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
    if (it != leaf.keys.end() && *it == key) {
        return false;
    }
    
    int index = it - leaf.keys.begin();
    leaf.keys.insert(it, key);
    leaf.records.insert(leaf.records.begin() + index, record);
    return true;
}

bool BTree::InsertIntoInternal(BTreeNode& internal, int key, page_id_t child_page_id) {
    auto it = std::upper_bound(internal.keys.begin(), internal.keys.end(), key);
    int index = it - internal.keys.begin();
    
    internal.keys.insert(it, key);
    internal.children.insert(internal.children.begin() + index + 1, child_page_id);
    return true;
}

int BTree::FindKeyIndex(const std::vector<int>& keys, int key) {
    auto it = std::upper_bound(keys.begin(), keys.end(), key);
    return it - keys.begin();
}

void BTree::SerializeNode(const BTreeNode& node, Page* page) {
    char* data = page->GetData();
    size_t offset = 0;
    
    std::memcpy(data + offset, &node.is_leaf, sizeof(node.is_leaf));
    offset += sizeof(node.is_leaf);
    
    size_t key_count = node.keys.size();
    std::memcpy(data + offset, &key_count, sizeof(key_count));
    offset += sizeof(key_count);
    
    for (int key : node.keys) {
        std::memcpy(data + offset, &key, sizeof(key));
        offset += sizeof(key);
    }
    
    if (!node.is_leaf) {
        for (page_id_t child : node.children) {
            std::memcpy(data + offset, &child, sizeof(child));
            offset += sizeof(child);
        }
    } else {
        for (const auto& record : node.records) {
            record.Serialize(data + offset);
            offset += record.GetSize();
        }
        std::memcpy(data + offset, &node.next_leaf, sizeof(node.next_leaf));
    }
}

BTreeNode BTree::DeserializeNode(Page* page) {
    BTreeNode node;
    const char* data = page->GetData();
    size_t offset = 0;
    
    std::memcpy(&node.is_leaf, data + offset, sizeof(node.is_leaf));
    offset += sizeof(node.is_leaf);
    
    size_t key_count;
    std::memcpy(&key_count, data + offset, sizeof(key_count));
    offset += sizeof(key_count);
    
    node.keys.resize(key_count);
    for (size_t i = 0; i < key_count; ++i) {
        std::memcpy(&node.keys[i], data + offset, sizeof(int));
        offset += sizeof(int);
    }
    
    if (!node.is_leaf) {
        node.children.resize(key_count + 1);
        for (size_t i = 0; i < key_count + 1; ++i) {
            std::memcpy(&node.children[i], data + offset, sizeof(page_id_t));
            offset += sizeof(page_id_t);
        }
    } else {
        node.records.reserve(key_count);
        for (size_t i = 0; i < key_count; ++i) {
            node.records.push_back(Record::Deserialize(data, offset));
        }
        std::memcpy(&node.next_leaf, data + offset, sizeof(node.next_leaf));
    }
    
    return node;
}