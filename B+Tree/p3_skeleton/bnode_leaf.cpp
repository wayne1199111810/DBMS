#include "bnode_leaf.h"
#include "bnode_inner.h"
#include <vector>

using namespace std;

Bnode_leaf::~Bnode_leaf() {
    // Remember to deallocate memory!!

}

VALUETYPE Bnode_leaf::merge(Bnode_leaf* rhs) {
    assert(num_values + rhs->getNumValues() < BTREE_LEAF_SIZE);
    assert(rhs->num_values > 0);
    VALUETYPE retVal;
    Bnode_inner* rhs_parent = rhs->parent;
    Bnode_inner* parent = this->parent;
    if (rhs_parent == parent) {
        retVal = parent->get(parent->find_child(this));
    }else {
        int idx = parent->find_child(this);
        while(parent != rhs_parent)
        {
            idx = parent->parent->find_child(parent);
            parent = parent->parent;
            rhs_parent = rhs_parent->parent;
        }
        retVal = parent->get(idx);
    }
    this->next = rhs->next;
    if (next) next->prev = this;
    for (int i = 0; i < rhs->getNumValues(); ++i)
        insert(rhs->getData(i));
    rhs->clear();
    return retVal;
}

VALUETYPE Bnode_leaf::redistribute(Bnode_leaf* rhs) {
    // TODO: Implement this
    assert(rhs->num_values > 0 || this->num_values > 0);
    vector<VALUETYPE> all_values = vector<VALUETYPE>(0);
    for (int i = 0; i < num_values; i++){
        all_values.push_back(get(i));
    }
    for (int i = 0; i < rhs->num_values; i++){
        all_values.push_back(rhs->get(i));
    }
    rhs->clear();
    this->clear();
    for(unsigned int i = 0; i < all_values.size(); i++){
        if(i < all_values.size() / 2)
            insert(all_values[i]);
        else
            rhs->insert(all_values[i]);
    }
    return rhs->get(0);
}

Bnode_leaf* Bnode_leaf::split(VALUETYPE insert_value) {
    // TODO: Implement this
    assert(num_values == BTREE_LEAF_SIZE);
    
    vector<Data*> all_values(0);
    bool inserted = false;
    for(int i = 0; i < getNumValues(); i++)
        all_values.push_back(getData(i));
    
    // Insert the value that created the split
    Bnode_leaf* rhs = new Bnode_leaf();
    clear();
    for (int i = 0; i < BTREE_LEAF_SIZE/2; ++i)
        insert(all_values[i]);
    for (int i = (BTREE_LEAF_SIZE/2); i < all_values.size(); ++i)
        rhs->insert(all_values[i]);
    if(rhs->get(0) > insert_value)
        this->insert(insert_value);
    else
        rhs->insert(insert_value);

    if(this->next)
        this->next->prev = rhs;
    rhs->next = this->next;
    this->next = rhs;
    rhs->prev = this;
    rhs->parent = parent;
    return rhs;
}