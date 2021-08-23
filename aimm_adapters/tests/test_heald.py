from pathlib import Path

import pytest
import os


from ..heald_labview import HealdLabViewTree
from tiled.client import from_tree
from tiled.trees.in_memory import Tree

@pytest.mark.parametrize(
    "directory, filename, expected_size",
    [
     ("../files/", "test_data.01", (2, 4))
     ],
    )
def test_heald_labview(directory, filename, expected_size):
    tree = Tree({"A": HealdLabViewTree.from_directory(directory)})
    client = from_tree(tree)    
    arr = client["A"][filename].read()
    assert arr.shape == expected_size
    
    