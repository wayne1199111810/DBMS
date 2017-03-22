#include "bnode_leaf.h"

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
    if(this->num_values == BTREE_LEAF_SIZE - 1)
    {
        rhs->insert(this->getData(num_values - 1));
    }
    else
    {
        this->insert(rhs->getData(0));
    }
    return rhs->get(0);

}

Bnode_leaf* Bnode_leaf::split(VALUETYPE insert_value) {
    // TODO: Implement this
    assert(num_values == BTREE_LEAF_SIZE);
    bnode_inner* parent = new Bnode_inner();
    Bnode_leaf* right = new Bnode_leaf();
    for(i = 0; i < BTREE_LEAF_SIZE/2 ; i++)
    {
        right->insert(this->get(i + BTREE_LEAF_SIZE/2));
        this->remove(this->get(i + BTREE_LEAF_SIZE/2));
    }
    if(right->get(0) == this->get(BTREE_LEAF_SIZE/2 - 1) || right->get(0) == insert_value) 
        this
    else if(right->get(0) > insert_value)
        this->insert(insert_value);
    else
        right->insert(insert_value);
    right->next = this->next;
    this->next = right;
    right->prev = this;
    return right;
    // return nullptr;
}


