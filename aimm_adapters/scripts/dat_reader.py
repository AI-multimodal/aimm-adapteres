from pathlib import Path

import numpy as np
import pandas as pd


def readdatfiles(folderpath, dataset_name):
    """
    Extract data and metadata information from a collection of dat files inside a specified folder


    Parameters
    ----------
    folderpath : string
        path to folder with dat files.
    dataset_name : string
        name of the dataset to be used in the aimm server.

    Returns
    -------
    metadata_collection : dict
        Collection of metadata captured from files. Each entry is represented as {filename:dict}.
    data_collection : dict
        Collection of data captured from files. Each entry is represented as {filename:dataframe}.

    """

    path = Path(folderpath)
    filepaths = sorted(path.iterdir())

    data_collection = {}
    metadata_collection = {}
    for filepath in filepaths:
        if filepath.suffix == ".dat":
            with open(filepath) as file:
                lines = file.readlines()

                metadata = {}
                data = []

                metadata["dataset"] = dataset_name
                metadata["fname"] = filepath.name
                for line in lines:
                    line = line.rstrip()
                    if line[0] == "#":
                        # Metadata
                        meta_raw = line[2:]
                        if ": " in meta_raw:
                            # Builds metadata
                            meta_split = meta_raw.split(": ")
                            keys = meta_split[0].split(".")
                            if keys[0].lower() not in metadata:
                                metadata[keys[0].lower()] = {}

                            if len(meta_split) > 1:
                                metadata[keys[0].lower()][keys[1].lower()] = meta_split[
                                    1
                                ]
                            else:
                                metadata[keys[0].lower()][keys[1].lower()] = None

                        else:
                            # Builds column names in dataframe
                            meta_raw = " ".join(
                                meta_raw.split()
                            )  # Remove unwanted white spaces
                            columns = meta_raw.split()

                    else:
                        # Data
                        sample = line.split()
                        sample = list(map(float, sample))
                        data.append(sample)

                metadata_collection[filepath.stem] = metadata

                df = pd.DataFrame(np.array(data), columns=columns)
                data_collection[filepath.stem] = df

    return data_collection, metadata_collection


def get_heald_data(parent_node, dataset_name, data_collection, metadata_collection):
    """
    Navigates a tiled tree and searches for the nodes that meet a criteria that is compatible in the aimm server

    Parameters
    ----------
    parent_node : tiled.client.node.Node
        Root node of the tree.
    dataset_name : str
        name of the dataset that will be used in the aimm server. This will be added to the metadata of each sample
    data_collection : dict
        container where the data will be saved recursively.
    metadata_collection : dict
        container where the metadadata will be saved recursively.

    Returns
    -------
    data_collection : dict
        container where all the data was be saved.
    metadata_collection : dict
        container where all the metadata was be saved.

    """

    from tiled.client.node import Node

    for child_node in parent_node:
        if isinstance(parent_node[child_node], Node):
            data_collection, metadata_collection = get_heald_data(
                parent_node[child_node],
                dataset_name,
                data_collection,
                metadata_collection,
            )
        else:
            if "common" in parent_node[child_node].metadata:
                if (
                    parent_node[child_node].metadata["common"]["element"]["symbol"]
                    is not None
                ):
                    path = parent_node[child_node].path
                    path_name = "-".join(path)

                    data_collection[path_name] = parent_node[child_node].read()
                    metadata_collection[path_name] = dict(
                        parent_node[child_node].metadata
                    )
                    metadata_collection[path_name]["dataset"] = dataset_name
                    metadata_collection[path_name]["sample"] = {"name": child_node}
                    metadata_collection[path_name]["fname"] = path_name

    return data_collection, metadata_collection
