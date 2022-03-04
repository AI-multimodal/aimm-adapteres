import json
import os
from pathlib import Path

import pandas as pd
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter


def get_unique_keywords_from_json(filepath, tracked_set, start=False, count=False):

    data = None
    with open(filepath) as json_file:
        if filepath.suffix[1:] == "json":
            data = json.load(json_file)

            if type(data) == list:
                for item in data:
                    column_set = set(item.keys())
                    if not count:
                        if start:
                            tracked_set = column_set.copy()
                            start = False
                        else:
                            tracked_set = tracked_set | column_set
                    else:
                        for set_name in column_set:
                            if set_name not in tracked_set:
                                tracked_set[set_name] = 0
                            tracked_set[set_name] += 1

    return tracked_set, start


def iter_unique_keys_json_files():
    path = Path("../files/")
    # start = True
    # tracked_set = set()

    tracked_dict = dict()
    for filepath in path.iterdir():
        # tracked_set, start = get_unique_keywords_from_json(filepath, tracked_set, start=start)
        tracked_dict, start = get_unique_keywords_from_json(
            filepath, tracked_dict, count=True
        )

    # print(tracked_set)
    sorted_list = list(
        sorted(tracked_dict.items(), key=lambda item: item[1], reverse=True)
    )
    print(sorted_list)


def parse_json(filepath):
    metadata = {}
    df = None

    if filepath.suffix[1:] == "json":
        with open(filepath) as json_file:
            fields = json.load(json_file)
            if type(fields) == list:
                for item in fields:
                    if "Energy" not in item:
                        metadata.update(item)
                    else:
                        columns = list(item.keys())
                        metadata["columns"] = columns
                        # transpose data to sort it by sample
                        column_data = [item[column] for column in item]
                        data = list(map(list, zip(*column_data)))
                        df = pd.DataFrame(data, columns=columns)

                        df = df.rename(
                            {"Energy": "energy", "mu_flat": "mutrans"}, axis="columns"
                        )
                        metadata["Translation"] = {
                            "energy": "Energy",
                            "mutrans": "mu_flat",
                        }

                if df is not None:
                    metadata["fname"] = filepath.name
                    metadata["provenance"] = {"source_id": "aimm_ncm_eli"}
                    metadata["voltage"] = metadata.pop("Voltage")
                    metadata["name"] = (
                        f"{metadata['element']}-{metadata['edge']}-"
                        f"cycle{int(metadata['cycle']):d}-"
                        f"{float(metadata['voltage']):0.1f}V"
                    )

    return df, metadata


def build_reader(filepath):
    df, metadata = parse_json(filepath)
    return DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)


class EliJsonTree(MapAdapter):
    @classmethod
    def from_directory(cls, directory):
        mapping = {
            filename: build_reader(Path(directory, filename))
            for filename in os.listdir(directory)
        }
        return cls(mapping)
