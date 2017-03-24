#include "btree.h"
#include "bnode.h"
#include "bnode_inner.h"
#include "bnode_leaf.h"
#include <iostream>

#include <cassert>

using namespace std;

const int LEAF_ORDER = BTREE_LEAF_SIZE/2;
const int INNER_ORDER = (BTREE_FANOUT-1)/2;

Btree::Btree() : root(new Bnode_leaf), size(0) {
    // Fill in here if needed
}

Btree::~Btree() {
    // Don't forget to deallocate memory
}

bool Btree::insert(VALUETYPE value) {
    // cout << "insert: " << value << endl;
    if(search(value) != nullptr)
        return false;
    Bnode_leaf* leaf = findLeafBy(value);
    Bnode_inner* parent = leaf->parent;
    if (leaf->getNumValues() < BTREE_LEAF_SIZE){
        leaf->insert(value);
        // cout << "inside leaf";
    }else{
        newParentIfNotHaveOne(leaf); // leaf == root
        parent = leaf->parent;
        // cout << "value: " << value << endl;
        Bnode* rhs = leaf->split(value);
        VALUETYPE update_value = ((Bnode_leaf*)rhs)->get(0);
        while(parent->getNumChildren() == BTREE_FANOUT){
            newParentIfNotHaveOne(parent);
            VALUETYPE ouput_value;
            rhs = parent->split(ouput_value, update_value, rhs);
            update_value = ouput_value;
            parent = parent->parent;
        }
        Bnode_inner* node = (Bnode_inner*)rhs;
        // cout << "6" << endl;
        parent->insert(update_value);
        int index = parent->find_value_gt(update_value);
        // cout << "7" << endl;
        parent->insert(rhs, index);
    }
    if(parent != nullptr && !parent->parent)
        root = parent;
    // cout << *this <<endl;
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
    // cout << "remove_value: " << value << endl;
    if(search(value) == nullptr)
    {
        // cout << "not found: " << value << endl;
        return false;
    }

    Bnode_leaf* leaf = findLeafBy(value);
    Bnode_inner* parent = leaf->parent;
    bool remove_valid = leaf->remove(value);
    assert(remove_valid);
    cout << "remove value: " << value << endl;
    if (leaf == root){
        return true;
    }else if (leaf->getNumValues() < BTREE_LEAF_SIZE / 2){
        VALUETYPE update_value;
        if (leaf->next && leaf->next->getNumValues() > BTREE_LEAF_SIZE / 2){
            cout << "3" << endl;
            update_value = leaf->redistribute(leaf->next);
            
            updateByRedistribute(update_value, leaf);
        } else if (leaf->prev && leaf->prev->getNumValues() > BTREE_LEAF_SIZE / 2){
            cout << "4" << endl;
            update_value = leaf->prev->redistribute(leaf);
            updateByRedistribute(update_value, leaf->prev);
        } else {
                cout << "5" << endl;
            if (leaf->next && leaf->next->getNumValues() != BTREE_LEAF_SIZE){
                cout << "6" << endl;
                parent = leaf->next->parent;
                updateParentWithMerge(leaf);
            } else if (leaf->prev && leaf->getNumValues() != 0){
                cout << "7" << endl;
                updateParentWithMerge(leaf->prev);
                leaf = leaf->prev;
            } else if (leaf->getNumValues() == 0){
                int idx = parent->find_child(leaf);
                cout << "idx: " << idx << endl;
                removeChildValueOf(parent, idx);
            }
            cout << "8" << endl;
            cleanTreeByRemove(parent);
        }
    }
    return true;
}

void Btree::removeChildValueOf(Bnode_inner* node, int idx){
    if (node->getNumChildren() - 1 == idx){
        node->remove_child(idx);
        idx -= 1;
    } else{
        node->remove_child(idx);
    }
    if (node->getNumValues() != 0)
        node->remove_value(idx);
}

void Btree::updateParentWithMerge(Bnode_leaf* node){
    Bnode_leaf* rhs = node->next;
    Bnode_inner* rhs_parent = rhs->parent;
    Bnode_inner* parent = node->parent;
    VALUETYPE remove_value = node->merge(node->next);
    VALUETYPE update_value = node->get(node->getNumValues() - 1);
    int idx;
    if (parent && rhs_parent == parent){
        idx = rhs_parent->find_child(rhs);
        removeChildValueOf(rhs_parent, idx);
        delete rhs;
    } else if (parent) {
        removeChildValueOf(rhs_parent, 0);
        while (parent != rhs_parent){
            assert(rhs_parent->getNumChildren() > 0);
            // cout << "replace_value " << endl;
            idx = parent->parent->find_child(parent);
            parent = parent->parent;
            rhs_parent = rhs_parent->parent;
        }
        parent->replace_value(update_value, idx);
    }
    // cout << "updateParentWithMerge";
}

void Btree::updateByRedistribute(VALUETYPE value, Bnode_leaf* leaf){
    // cout << "after redistribute: " << value << endl;
    Bnode_inner* parent = leaf->parent;
    Bnode_leaf* rhs = leaf->next;
    Bnode_inner* rhs_parent = rhs->parent;
    if (!parent) return;
    VALUETYPE smallest = leaf->get(0);
    int idx = parent->find_child(leaf);
    if (parent && parent == rhs_parent){
        parent->replace_value(value, idx);
    }
    while(parent && parent != rhs_parent){
        if(idx < parent->getNumChildren() - 1 && parent->get(idx) < value)
            parent->replace_value(value, idx);
        if (!parent->parent)
        {
            root = parent;
            break;
        }        
        idx = parent->parent->find_child(parent);
        parent = parent->parent;
        rhs_parent = rhs_parent->parent;
    }
    // cout << "Finsish update redistribute" << endl;
}

void Btree::cleanTreeByRemove(Bnode_inner* node){
    cout << "cleanTreeByRemove " <<  node->getNumValues() << endl;
    while(node != nullptr && (node->getNumChildren() <= (BTREE_FANOUT / 2))){
        cout << "8" << endl;
        // Bnode_inner* rhs;
        VALUETYPE update_value;
        if (node->parent){
            int idx = node->parent->find_child(node);

            if (node->hasRight() && node->getRight()->getNumChildren() > (BTREE_FANOUT / 2 + 1) ){
                cout << "11" << endl;
                update_value = node->redistribute(node->getRight(), idx);
                node->parent->replace_value(update_value, idx);
            } else if (node->hasLeft() && node->getLeft()->getNumChildren() > (BTREE_FANOUT / 2 + 1) ){
                cout << "12" << endl;
                update_value = node->getLeft()->redistribute(node, idx - 1);
                node->parent->replace_value(update_value, idx - 1);
            }

            // if (idx == node->parent->getNumChildren() - 1){
            //     cout << "9" << endl;
            //     rhs = node;
            //     node = (Bnode_inner*)node->parent->getChild(idx--);
            // } else{
            //     cout << "10" << endl;
            //     rhs = (Bnode_inner*)node->parent->getChild(idx + 1);
            // }
            // if (rhs->getNumChildren() > (BTREE_FANOUT / 2 + 1)){
            //     cout << "11" << endl;
            //     update_value = node->redistribute(rhs, idx);
            //     node->parent->replace_value(update_value, idx);
            // } 
            else if (node->hasRight()) {
                cout << "13" << endl;
                cout << node->getNumChildren() << ", " << node->getRight()->getNumChildren() << endl;
                cout << "right merge";
                update_value = node->merge(node->getRight(), idx);
                cout << "after merge";
                node->print(cout);
                cout << endl;
                removeChildValueOf(node->parent, idx + 1);
            } 
            else if (node->hasLeft()) {
                cout << "14" << endl;
                update_value = node->getLeft()->merge(node, idx - 1);
                cout << "leaft merge";
                node->print(cout);
                cout << endl;
                removeChildValueOf(node->parent, idx);
            }
            cout << "15: " << idx << ", " << node->parent->getNumValues() << endl;
            if (node->parent == root && node->parent->getNumChildren() == 1){
                root = node;
                node->parent = nullptr;
            }

        } else{
            cout << "no node->parent" << endl;
            if (node->getNumChildren() == 1){
                cout << " one child" << endl;
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