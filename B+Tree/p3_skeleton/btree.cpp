#include "btree.h"
#include "bnode.h"
#include "bnode_inner.h"
#include "bnode_leaf.h"
#include <cassert>

using namespace std;

const int LEAF_ORDER = BTREE_LEAF_SIZE/2;
const int INNER_ORDER = (BTREE_FANOUT-1)/2;

void updatePrevNextOnDelete(Bnode_leaf*);

Btree::Btree() : root(new Bnode_leaf), size(0) {
    // Fill in here if needed
}

Btree::~Btree() {
    // Don't forget to deallocate memory
}


bool Btree::insert(VALUETYPE value) {
    if(search(value) != nullptr)
        return false;
    Bnode_leaf* leaf = findLeafBy(value);
    Bnode_inner* parent = leaf->parent;
    if (leaf->getNumValues() < BTREE_LEAF_SIZE){
        leaf->insert(value);
    }else{
        newParentIfNotHaveOne(leaf); // leaf == root
        parent = leaf->parent;
        Bnode* rhs = leaf->split(value);
        VALUETYPE update_value = ((Bnode_leaf*)rhs)->get(0);
        int idx = parent->find_child(leaf) + 1;
        while(parent->getNumChildren() == BTREE_FANOUT){
            newParentIfNotHaveOne(parent);
            VALUETYPE ouput_value;
            rhs = parent->split(ouput_value, update_value, rhs);
            update_value = ouput_value;
            idx = parent->parent->find_child(parent) + 1;
            parent = parent->parent;
        }
        parent->insert(update_value);
        parent->insert(rhs, idx);
    }
    if(parent != nullptr && !parent->parent)
        root = parent;
    return true;
}

void Btree::newParentIfNotHaveOne(Bnode* node){
    if(!node->parent) {
        node->parent = new Bnode_inner();
        node->parent->insert(node, 0);
    }
}

Bnode_leaf* Btree::findLeafBy(VALUETYPE value){
    Bnode* current = root;
    Bnode_inner* inner = dynamic_cast<Bnode_inner*>(current);
    while(inner){
        int idx = inner->find_value_gt(value);
        current = ((Bnode_inner*)current)->getChild(idx);
        inner = dynamic_cast<Bnode_inner*>(current);
    }
    return dynamic_cast<Bnode_leaf*>(current);
}

bool Btree::remove(VALUETYPE value) {
    // TODO: Implement this
    Bnode_leaf* leaf = findLeafBy(value);

    if(!leaf->remove(value))
        return false;
    Bnode_inner* parent = leaf->parent;
    if (leaf == root){
        return true;
    }else if (leaf->getNumValues() < BTREE_LEAF_SIZE / 2){
        if (leaf->next && leaf->next->getNumValues() > BTREE_LEAF_SIZE / 2){
            updateByRedistribute(leaf);
        } else if (leaf->prev && leaf->prev->getNumValues() > BTREE_LEAF_SIZE / 2){
            
            updateByRedistribute(leaf->prev);
        } else {
            if (leaf->getNumValues() == 0){
                if (parent->find_child(leaf) != parent->getNumValues())
                    parent->remove_value(parent->find_child(leaf));
                parent->remove_child(parent->find_child(leaf));
                updatePrevNextOnDelete(leaf);
                delete leaf;
            } else if (leaf->next && leaf->next->getNumValues() + leaf->getNumValues() <= BTREE_LEAF_SIZE){
                parent = leaf->next->parent;
                updateParentWithMerge(leaf);
            } else if (leaf->prev && leaf->prev->getNumValues() + leaf->getNumValues() <= BTREE_LEAF_SIZE){
                updateParentWithMerge(leaf->prev);
            }
            cleanTreeByRemove(parent);
        }
    }
    return true;
}

void Btree::updateParentWithMerge(Bnode_leaf* node){
    Bnode_leaf* rhs = node->next;
    Bnode_inner* rhs_parent = rhs->parent;
    Bnode_inner* parent = node->parent;
    VALUETYPE remove_value = node->merge(node->next);

    VALUETYPE update_value = rhs_parent->get(0);
    rhs_parent->remove_child(rhs_parent->find_child(rhs));
    while(-1 == rhs_parent->find_value(remove_value)){
        rhs_parent = rhs_parent->parent;
    }
    if (rhs_parent == parent){
        parent->remove_value(parent->find_value(remove_value));
    } else {
        rhs_parent->replace_value(update_value, rhs_parent->find_value(remove_value));
    }
    delete rhs;
}

void Btree::updateByRedistribute(Bnode_leaf* leaf){
    Bnode_inner* parent = leaf->parent;
    Bnode_leaf* rhs = leaf->next;
    Bnode_inner* rhs_parent = rhs->parent;
    VALUETYPE update_value = leaf->redistribute(leaf->next);
    int idx = parent->find_child(leaf);
    while(parent != rhs_parent){
        if (!parent->parent)
        {
            root = parent;
            break;
        }        
        idx = parent->parent->find_child(parent);
        parent = parent->parent;
        rhs_parent = rhs_parent->parent;
    }
    parent->replace_value(update_value, idx);
}

void Btree::cleanTreeByRemove(Bnode_inner* node){
    while(node != nullptr && !node->isValid()){
        // Bnode_inner* rhs;
        VALUETYPE update_value;

        if (node->parent){
            int idx = node->parent->find_child(node);
            if (node->hasRight() && node->getRight()->getNumValues() > (BTREE_FANOUT / 2) ){
                update_value = node->redistribute(node->getRight(), idx);
            } else if (node->hasLeft() && node->getLeft()->getNumValues() > (BTREE_FANOUT / 2) ){
                update_value = node->getLeft()->redistribute(node, idx - 1);
            }
            else if (node->hasRight()) {
                update_value = node->merge(node->getRight(), idx);
            } 
            else if (node->hasLeft()) {
                node = node->getLeft();
                update_value = node->merge(node->getRight(), idx - 1);
            }
            if (node->parent == root && node->parent->getNumChildren() == 1){
                root = node;
                node->parent = nullptr;
            }
        } else{
            if (node->getNumChildren() == 1){
                root = node->getChild(0);
                root->parent = nullptr;
                delete node;
            }
        }
        node = node->parent;
    }
}

vector<Data*> Btree::search_range(VALUETYPE begin, VALUETYPE end) {
    std::vector<Data*> returnValues;
    // TODO: Implement this
    // Have not reached a leaf node yet
    Bnode* current = root;
    Bnode_inner* inner = dynamic_cast<Bnode_inner*>(current);
    while (inner)
    {
        int idx = inner->find_value_gt(begin);
        current = inner->getChild(idx);
        inner = dynamic_cast<Bnode_inner*>(current);
    }
    Bnode_leaf* leaf = dynamic_cast<Bnode_leaf*>(current);
    while(leaf){
        for (int i = 0; i < leaf->getNumValues(); i++)
        {
            if (leaf->get(i) > end)
            {
                leaf = nullptr;
                break;
            } else if (leaf->get(i) >= begin) {
                returnValues.push_back(leaf->getData(i));
            }
        }
        if (leaf != nullptr)
            leaf = leaf->next;
    }
    return returnValues;
}


//
// Given code
//
Data* Btree::search(VALUETYPE value) {
    assert(root);
    Bnode* current = root;

    // Have not reached a leaf node yet
    Bnode_inner* inner = dynamic_cast<Bnode_inner*>(current);
    // A dynamic cast <T> will return a nullptr if the given input is polymorphically a T
    //                    will return a upcasted pointer to a T* if given input is polymorphically a T
    while (inner) {
        int find_index = inner->find_value_gt(value);
        current = inner->getChild(find_index);
        inner = dynamic_cast<Bnode_inner*>(current);
    }

    // Found a leaf node
    Bnode_leaf* leaf = dynamic_cast<Bnode_leaf*>(current);
    assert(leaf);
    for (int i = 0; i < leaf->getNumValues(); ++i) {
        if (leaf->get(i) > value) return nullptr; // passed the possible location
        if (leaf->get(i) == value) return leaf->getData(i);
    }

    // reached past the possible values - not here
    return nullptr;
}

void updatePrevNextOnDelete(Bnode_leaf* leaf){
    if(leaf->prev && leaf->next)
    {  
        leaf->prev->next = leaf->next;
        leaf->next->prev = leaf->prev;
    } else if (!leaf->prev && leaf->next) {
        leaf->next->prev = nullptr;
    } else if (leaf->prev && !leaf->next){
        leaf->prev->next = nullptr;
    }
}
/*
void Btree::printLeaf(){
    assert(root);
    Bnode* current = root;
    Bnode_inner* inner = dynamic_cast<Bnode_inner*>(current);
    while (inner) {
        current = inner->getChild(0);
        inner = dynamic_cast<Bnode_inner*>(current);
    }
    Bnode_leaf* leaf = dynamic_cast<Bnode_leaf*>(current);
    while(leaf)
    {
        leaf = leaf->next;
    }
}
*/