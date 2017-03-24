#include "bnode_inner.h"
#include <vector>

using namespace std;

VALUETYPE Bnode_inner::merge(Bnode_inner* rhs, int parent_idx) {
    assert(rhs->parent == parent); // can only merge siblings
    assert(rhs->num_values > 0);
    // TODO: Implement this
    assert(rhs->getNumChildren() + this->getNumChildren() <= BTREE_FANOUT);
    VALUETYPE retVal = rhs->get(0);
    VALUETYPE parent_val = this->parent->get(parent_idx);
    int base_addr = num_children;
    this->insert(parent_val);
    for (int i = 0; i < rhs->num_children - 1; ++i){
        insert(rhs->getChild(i), base_addr + i);
        insert(rhs->get(i));
    }
    insert(rhs->getChild(rhs->num_children - 1), rhs->num_children);
    rhs->clear();
    // delete rhs;
    return retVal;
}

VALUETYPE Bnode_inner::redistribute(Bnode_inner* rhs, int parent_idx) {
    assert(rhs->parent == parent); // inner node redistribution should only happen with siblings
    assert(parent_idx >= 0);
    // TODO: Implement this
    assert(parent_idx < parent->getNumValues());
    vector<Bnode*> all_children(children, children + num_values);
    vector<VALUETYPE> all_values(values, values + num_values);
    VALUETYPE update_value = rhs->get(0);    
    all_values.push_back(this->parent->get(parent_idx)); // from parent
    for(unsigned int i = 0; i < rhs->getNumChildren(); i++){
        all_children.push_back(rhs->getChild(i));
        if(i != 0 && i < rhs->getNumValues())
            all_values.push_back(rhs->get(i));
    }
    rhs->clear();
    this->clear();
    insert(all_children[0], 0);
    for(unsigned int i = 0; i < all_values.size(); i++){
        if(i < all_values.size() / 2)
        {
            insert(all_values[i]);
            insert(all_children[i + 1], i + 1);
        } else{
            rhs->insert(all_children[i + 1], i + 1);
            rhs->insert(all_values[i]);
        }
    }
    rhs->insert(all_children[all_children.size() - 1], all_children.size() - 1);
    assert((this->num_values >= (BTREE_FANOUT / 2)) && (rhs->num_values >= (BTREE_FANOUT / 2)));
    this->parent->replace_value(update_value, parent_idx);
    return update_value;
}

Bnode_inner* Bnode_inner::split(VALUETYPE& output_val, VALUETYPE insert_value, Bnode* insert_node) {
    assert(num_values == BTREE_FANOUT - 1); // only split when it's full!

    // Populate an intermediate array with all the values/children before splitting - makes this simpler
    vector<VALUETYPE> all_values(values, values + num_values);
    vector<Bnode*> all_children(children, children + num_children);

    // Insert the value that created the split
    int ins_idx = find_value_gt(insert_value);
    all_values.insert(all_values.begin()+ins_idx, insert_value);
    all_children.insert(all_children.begin()+ins_idx+1, insert_node);

    // Do the actual split into another node
    Bnode_inner* split_node = new Bnode_inner;

    assert(all_values.size() == BTREE_FANOUT);
    assert(all_children.size() == BTREE_FANOUT + 1);

    // Give the first BTREE_FANOUT/2 values to this bnode
    clear();
    for (int i = 0; i < BTREE_FANOUT/2; ++i)
        insert(all_values[i]);
    for (int i = 0, idx = 0; i < (BTREE_FANOUT/2) + 1; ++i, ++idx) {
        insert(all_children[i], idx);
        all_children[i] -> parent = this;
    }

    // Middle value should be pushed to parent
    output_val = all_values[BTREE_FANOUT/2];

    // Give the last BTREE/2 values to the new bnode
    for (int i = (BTREE_FANOUT/2) + 1; i < all_values.size(); ++i)
        split_node->insert(all_values[i]);
    for (int i = (BTREE_FANOUT/2) + 1, idx = 0; i < all_children.size(); ++i, ++idx) {
        split_node->insert(all_children[i], idx);
        all_children[i] -> parent = split_node;
    }

    // I like to do the asserts :)
    assert(num_values == BTREE_FANOUT/2);
    assert(num_children == num_values+1);
    assert(split_node->getNumValues() == BTREE_FANOUT/2);
    assert(split_node->getNumChildren() == num_values + 1);

    split_node->parent = parent; // they are siblings

    return split_node;
}

bool Bnode_inner::hasLeft(){
    if (this->parent){
        int idx = this->parent->find_child(this);
        if (idx != 0)
            return true;
    }
    return false;
}

bool Bnode_inner::hasRight(){
    if (this->parent){
        int idx = this->parent->find_child(this);
        if (idx != this->parent->getNumChildren() - 1)
            return true;
    }
    return false;
}
Bnode_inner* Bnode_inner::getRight(){
    if(this->hasRight()){
        int idx = parent->find_child(this);
        return (Bnode_inner*)parent->getChild(idx + 1);
    }
    else 
        return nullptr;
}

Bnode_inner* Bnode_inner::getLeft(){
    if(this->hasLeft()){
        int idx = parent->find_child(this);
        return (Bnode_inner*)parent->getChild(idx - 1);
    }
    else 
        return nullptr;
}