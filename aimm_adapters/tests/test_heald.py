import pytest

from pathlib import Path
from ..heald_labview import HealdLabViewTree
from tiled.client import from_tree
from tiled.trees.in_memory import Tree


@pytest.mark.parametrize(
    "filename, expected_size",
    [("test_data.01", (2, 4))],
)
def test_heald_labview(filename, expected_size):
    here = Path(__file__).parent  # directory containing this module
    files = here / ".." / "files"
    tree = Tree({"A": HealdLabViewTree.from_directory(files)})
    client = from_tree(tree)
    arr = client["A"][filename].read()
    assert arr.shape == expected_size
