//
// Created by tawnysky on 2020/9/8.
//

#pragma once

#include <cstdint>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <limits>
#include <iostream>

// Epsilon for double comparison
#define EPS 1e-12

// Epsilon for making segmentation
#define EPSILON 8

// Additional reserved space for left buffer and nodes
// Also represents insert/delete times in inplace_node
#define RESERVE (256 * EPSILON)

