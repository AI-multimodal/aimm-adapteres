"""
Use with server configuration like this:

# config.yml
trees:
  - path: "/"
    tree: aimm_readers.heald_labview:HealdLabViewTree
    args:
      directory: path/to/files  # your directory here

and start a server like this:

tiled serve config config.yml
"""

import os
from pathlib import Path

import pandas as pd
from enum import Enum
from tiled.trees.in_memory import Tree
from tiled.readers.dataframe import DataFrameAdapter
from tiled.server.object_cache import with_object_cache


class ParsingCase(Enum):
    column = 1
    user = 2
    scan = 3
    amplifier = 4
    analog = 5
    mono = 6
    id_info = 7
    slit = 8
    motor = 9
    panel = 10
    beamline = 11
    xia = 12
    shutter = 13


def parse_heald_labview(file):
    lines = file.readlines()

    parsing_case = 0
    data = []
    comment_lines = []
    meta_dict = {}

    for line in lines:
        line = line.rstrip()
        # Parse comments as metadata
        if line[0] == "#":
            if len(line) > 2:
                # The next line after the Column Headinds tag is the only line
                # that does not include a white space after the comment/hash symbol
                if parsing_case == ParsingCase.column:
                    line = line[1:]
                else:
                    line = line[2:]

                # Add additional cases to parse more information from the
                # header comments
                # Start reading the name of the upcoming block of information

                if line == "Column Headings:":
                    parsing_case = ParsingCase.column  # Create headers for dataframe
                    continue
                elif line == "User Comment:":
                    comment_lines = []
                    parsing_case = ParsingCase.user
                    continue
                elif line == "Scan config:":
                    comment_lines = []
                    parsing_case = ParsingCase.scan
                    continue
                elif line == "Amplifier Sensitivities:":
                    comment_lines = []
                    parsing_case = ParsingCase.amplifier
                    continue
                elif line.find("Analog Input Voltages") != -1:
                    comment_lines = []
                    parsing_case = ParsingCase.analog
                    continue
                elif line == "Mono Info:":
                    comment_lines = []
                    parsing_case = ParsingCase.mono
                    continue
                elif line == "ID Info:":
                    comment_lines = []
                    parsing_case = ParsingCase.id_info
                    continue
                elif line == "Slit Info:":
                    comment_lines = []
                    parsing_case = ParsingCase.slit
                    continue
                elif line == "Motor Positions:":
                    comment_lines = []
                    parsing_case = ParsingCase.motor
                    continue
                elif line.find("LabVIEW Control Panel") != -1:
                    comment_lines = []
                    parsing_case = ParsingCase.panel
                elif line.find("Beamline") != -1:
                    parsing_case = ParsingCase.beamline
                elif line.find("XIA Filters:") != -1:
                    parsing_case = ParsingCase.xia
                    continue
                elif line.find("XIA Shutter Unit:") != -1:
                    parsing_case = ParsingCase.shutter
                    continue

                # Reads the following lines to parse a block of information
                # with a specific format
                if parsing_case == ParsingCase.column:
                    line = line.replace("*", " ")
                    line = line.replace("tempeXMAP4", "tempe        XMAP4")

                    headers = [term.lstrip() for term in line.split("  ") if term]
                    meta_dict["Columns"] = headers
                    parsing_case = 0
                elif parsing_case == ParsingCase.user:
                    line = " ".join(line.split())  # Remove unwanted white spaces
                    comment_lines.append(line)
                    meta_dict["UserComment"] = comment_lines
                elif parsing_case == ParsingCase.scan:
                    line = " ".join(line.split())  # Remove unwanted white spaces
                    comment_lines.append(line)
                    meta_dict["ScanConfig"] = comment_lines
                elif parsing_case == ParsingCase.amplifier:
                    comment_lines = line.split("  ")
                    amplifier_dict = {}
                    for element in comment_lines:
                        key, value = element.split(": ", 1)
                        amplifier_dict[key] = value
                    meta_dict["AmplifierSensitivities"] = amplifier_dict
                elif parsing_case == ParsingCase.analog:
                    comment_lines = line.split("  ")
                    analog_dict = {}
                    for element in comment_lines:
                        key, value = element.split(": ", 1)
                        analog_dict[key] = value
                    meta_dict["AnalogInputVoltages"] = analog_dict
                elif parsing_case == ParsingCase.mono:
                    comment_lines = line.split("; ")
                    mono_dict = {}
                    for element in comment_lines:
                        key, value = element.split(": ", 1)
                        mono_dict[key] = value
                    meta_dict["MonoInfo"] = mono_dict
                elif parsing_case == ParsingCase.id_info:
                    comment_lines = line.split("  ")
                    meta_dict["IDInfo"] = comment_lines
                elif parsing_case == ParsingCase.slit:
                    comment_lines.append(line)
                    meta_dict["SlitInfo"] = comment_lines
                elif parsing_case == ParsingCase.motor:
                    comment_lines.append(line)
                    meta_dict["MotorPositions"] = comment_lines
                elif parsing_case == ParsingCase.panel:
                    comment_lines = line.split("; ")
                    meta_dict["File"] = comment_lines
                elif parsing_case == ParsingCase.beamline:
                    meta_dict["Beamline"] = line
                elif parsing_case == ParsingCase.xia:
                    line = line.replace("OUT", "OUT ")
                    comment_lines = line.split("  ")
                    xia_dict = {}
                    for element in comment_lines:
                        key, value = element.split(": ", 1)
                        xia_dict[key] = value
                    meta_dict["XIAFilter"] = xia_dict
                elif parsing_case == ParsingCase.shutter:
                    line = line.replace("OUT", "OUT ")
                    comment_lines = line.split("  ")
                    shutter_dict = {}
                    for element in comment_lines:
                        key, value = element.split(": ", 1)
                        shutter_dict[key] = value
                    meta_dict["XIAShutterUnit"] = shutter_dict
            else:
                parsing_case = 0
                continue
        # Parse data
        else:
            line = " ".join(line.split())  # Remove unwanted white spaces
            sample = line.split()
            sample = list(map(float, sample))
            data.append(sample)
    df = pd.DataFrame(data, columns=headers)
    return df, meta_dict


def build_reader(filepath):
    with open(filepath) as file:
        df, metadata = parse_heald_labview(file)
    return DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)


def is_candidate(filename):
    filename_ext = filename.split(".")
    return filename_ext[-1].isnumeric()


def iter_subdirectory(mapping, path):
    experiment_group = {}
    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            sub_mapping = {}
            mapping[filepath.name] = Tree(sub_mapping)
            sub_mapping = iter_subdirectory(sub_mapping, filepath)
            continue
        if filepath.suffix[1:].isnumeric():
            if filepath.stem not in mapping:
                experiment_group[filepath.stem] = {}
                mapping[filepath.stem] = Tree(experiment_group[filepath.stem])
            cache_key = (Path(__file__).stem, filepath)
            experiment_group[filepath.stem][filepath.name] = with_object_cache(
                cache_key, build_reader, filepath
            )

    return mapping


def subdirectory_handler(path):
    mapping = {}
    heald_tree = Tree(mapping)
    mapping = iter_subdirectory(mapping, path)
    return heald_tree


def normalize_dataframe(df):
    keywords = ["Mono Energy", "I0", "It", "Iref"]
    column_names = set(df.columns.values.tolist())
    norm_df = pd.DataFrame()
    for key in keywords:
        if key in column_names:
            norm_df[key] = df[key]

    return norm_df


class HealdLabViewTree(Tree):
    @classmethod
    def from_directory(cls, directory):
        mapping = {
            filename: build_reader(Path(directory, filename))
            for filename in os.listdir(directory)
            if is_candidate(filename)
        }
        return cls(mapping)


class RIXSImagesAndTable(Tree):
    @classmethod
    def from_directory(cls, directory):
        import tifffile
        from tiled.readers.tiff_sequence import TiffSequenceReader

        mapping = {
            name: Tree(
                {
                    "table": build_reader(Path(directory, name)),
                    "images": TiffSequenceReader(
                        tifffile.TiffSequence(f"{Path(directory,name)}.Eiger/*")
                    ),
                }
            )
            for name in os.listdir(directory)
            if not os.path.isdir(Path(directory, name))
        }
        return cls(mapping)


class NormalizedReader:
    def __init__(self, filepath):
        cache_key = (
            Path(__file__).stem,
            filepath,
        )  # exact same key you used for build_reader
        # Make an UNnoramlized reader first.
        # Use the cache so that this unnormalized reader can be shared across a normalized tree and an unnormalized tree.
        self._unnormalized_reader = with_object_cache(cache_key, build_reader, filepath)
        # self._unnormalized_reader = with_object_cache(cache_key, subdirectory_handler, filepath)

    def read(self):
        result = self._unnormalized_reader.read()
        # Make changes to result (altering column names, removing extraneous columns) and then return it.
        return normalize_dataframe(result)
