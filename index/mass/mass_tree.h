/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _mass_tree_h_
#define _mass_tree_h_

#include "mass_node.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct mass_tree
{
  mass_node *root;
}mass_tree;

mass_tree* new_mass_tree();
void free_mass_tree(mass_tree *mt);
int mass_tree_put(mass_tree *mt, const void *key, uint32_t len, void *val);
void* mass_tree_get(mass_tree *mt, const void *key, uint32_t len);

#ifdef Test

void mass_tree_validate(mass_tree *mt);

#endif // Test
#ifdef __cplusplus
}
#endif
#endif /* _mass_tree_h_ */
