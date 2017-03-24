#include "bnode_leaf.h"
#include <vector>

using namespace std;

Bnode_leaf::~Bnode_leaf() {
    // Remember to deallocate memory!!

}

VALUETYPE Bnode_leaf::merge(Bnode_leaf* rhs) {
    assert(num_values + rhs->getNumValues() < BTREE_LEAF_SIZE);
    assert(rhs->num_values > 0);
    VALUETYPE retVal = rhs->get(0);
    Bnode_leaf* save = next;
    next = next->next;
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
    Bnode_leaf* right = new Bnode_leaf();
    for(int i = 0; i < BTREE_LEAF_SIZE/2 ; i++)
    {

        right->insert(this->get(i + BTREE_LEAF_SIZE/2));
        this->remove(this->get(i + BTREE_LEAF_SIZE/2));
    }
    if(right->get(0) > insert_value)
        this->insert(insert_value);
    else
        right->insert(insert_value);
    right->next = this->next;
    this->next = right;
    right->prev = this;
    right->parent = parent;
    return right;
}


