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
from collections import defaultdict
from enum import Enum
from pathlib import Path

import pandas as pd
import xraydb
from tiled.readers.dataframe import DataFrameAdapter
from tiled.server.object_cache import with_object_cache
from tiled.trees.in_memory import Tree


def mangle_dup_names(names):
    d = defaultdict(int)

    out = []

    for x in names:
        count = d[x]
        if count == 0:
            out.append(x)
        else:
            out.append(f"{x}.{count}")
        d[x] += 1

    return out


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


def parse_heald_labview(file, no_device=False):
    lines = file.readlines()

    parsing_case = 0
    headers = []
    data = []
    comment_lines = []
    meta_dict = {}
    first_line = True

    for line in lines:
        line = line.rstrip()
        # Parse comments as metadata
        if line[0] == "#":
            if len(line) > 2:
                # The next line after the Column Headinds tag is the only line
                # that does not include a white space after the comment/hash symbol
                if parsing_case == ParsingCase.column or first_line:
                    line = line[1:]
                    first_line = False
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
                    line = line.replace("scatter_Sum XMAP4", "scatter_Sum        XMAP4")
                    line = line.replace("Stats1:TS20-", "Stats1:T        S20-")

                    if not no_device:
                        headers = [term.lstrip() for term in line.split("  ") if term]
                    else:
                        for term in line.split("  "):
                            if term:
                                term = term.lstrip()
                                index_list = find_char_indexes(term, ":")
                                if len(index_list) == 0:
                                    headers.append(term)
                                else:
                                    lower_dev_names = set(["pncaux", "pncid"])
                                    if (
                                        term[: index_list[0]].isupper()
                                        or term[: index_list[0]] in lower_dev_names
                                    ):
                                        temp_term = term[
                                            index_list[0] + 1 :  # noqa: E203
                                        ]
                                    else:
                                        temp_term = term[: index_list[-1]]
                                    headers.append(temp_term)

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

    headers = mangle_dup_names(headers)
    df = pd.DataFrame(data, columns=headers)

    return df, meta_dict


def find_char_indexes(word, char):
    return [i for i, val in enumerate(word) if val == char]


def build_reader(filepath, no_device=False):
    with open(filepath) as file:
        df, metadata = parse_heald_labview(file, no_device)
    return DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)


def is_candidate(filename):
    filename_ext = filename.split(".")
    return filename_ext[-1].isnumeric()


def iter_subdirectory(mapping, path, normalize=False):
    experiment_group = {}
    filepaths = sorted(path.iterdir())
    for i in range(len(filepaths)):
        if filepaths[i].name.startswith("."):
            # Skip hidden files.
            continue
        if not filepaths[i].is_file():
            # Explore subfolder for more labview files recursively
            sub_mapping = {}
            sub_mapping = iter_subdirectory(sub_mapping, filepaths[i], normalize)
            if sub_mapping:
                mapping[filepaths[i].name] = Tree(sub_mapping)
            continue
        if filepaths[i].suffix[1:].isnumeric():
            if filepaths[i].stem not in experiment_group:
                experiment_group[filepaths[i].stem] = {}
                if not normalize:
                    mapping[filepaths[i].stem] = Tree(
                        experiment_group[filepaths[i].stem]
                    )
            if normalize:
                norm_node = NormalizedReader(filepaths[i]).read()
                if norm_node is not None:
                    experiment_group[filepaths[i].stem][filepaths[i].name] = norm_node
            else:
                cache_key = (Path(__file__).stem, filepaths[i])
                experiment_group[filepaths[i].stem][
                    filepaths[i].name
                ] = with_object_cache(cache_key, build_reader, filepaths[i])

        # For a normalized tree, experiments files are grouped, filtered and saved
        # temporarily. Once all files of ine experiments are read, it checks if there
        # are remaining files that passed the filtering phase. The result is saved in
        # the main tree. This avoids the generation of empty nodes in the final version
        # of the tree.
        if normalize:
            if filepaths[i].stem in experiment_group:
                if i == len(filepaths) - 1:
                    if len(experiment_group[filepaths[i].stem]) != 0:
                        mapping[filepaths[i].stem] = Tree(
                            experiment_group[filepaths[i].stem]
                        )
                elif filepaths[i].stem != filepaths[i + 1].stem:
                    if len(experiment_group[filepaths[i].stem]) != 0:
                        mapping[filepaths[i].stem] = Tree(
                            experiment_group[filepaths[i].stem]
                        )

    return mapping


def subdirectory_handler(path):
    mapping = {}
    heald_tree = Tree(mapping)
    mapping = iter_subdirectory(mapping, path)
    return heald_tree


def normalized_subdirectory_handler(path):
    mapping = {}
    heald_tree = Tree(mapping)
    mapping = iter_subdirectory(mapping, path, normalize=True)
    return heald_tree


def normalize_dataframe(df):
    energy = "Mono Energy"
    keywords = {
        "time": ["Scaler preset time", "None"],
        "i0": ["I0", "IO", "I-0"],
        "it": ["IT", "I1", "I", "It", "Trans"],
        "ir": ["Iref", "IRef", "I2", "IR", "IREF", "DiodeRef", "Cal(Iref)", "Ref"],
        "if": [
            "Ifluor",
            "IF",
            "If",
            "Cal Diode",
            "Cal-diode",
            "CalDiode",
            "Cal_Diode",
            "Cal_diode",
            "Canberra",
        ],
    }
    column_names = set(df.columns.values.tolist())
    norm_df = None
    if energy in column_names:
        norm_df = pd.DataFrame()
        norm_df["energy"] = df[energy]
        for key, value in keywords.items():
            if key != "time":
                counter = 0
                for name in value:
                    if name in column_names:
                        norm_df[key] = df[name]
                        break
                    counter += 1

                    # Reached the end of the list and found nothing for one variable
                    # Must return None because it does not meet the XDI standards
                    # Uncomment the next lines to make the normalized filter more strict
                    # if counter == len(value):
                    #     norm_df = None
                    #     return norm_df

    return norm_df


def parse_element_name(filepath, df, metadata):

    element_name = None
    if "energy" in set(df.keys()):
        energy = df["energy"]
        if len(energy) > 1:
            min_max = [min(energy), max(energy)]

            element_list = {}
            # Find if the edge value of an element in xrayDB is inside the range of
            # Mono Energy values of the current file
            # An element in XrayDB can contain more than one edge, each one identified by
            # a unique IUPAC symbol
            for i in range(1, 99):
                current_element = xraydb.atomic_symbol(i)
                edges = xraydb.xray_edges(current_element)
                for key in edges:
                    if (
                        edges[key].energy >= min_max[0]
                        and edges[key].energy <= min_max[1]
                    ):
                        element_list[current_element] = [
                            i,
                            current_element,
                            key,
                            edges[key].energy,
                            False,
                        ]
                        break

            # Find if the matching elements are named in the parsed metadata
            # Must considered cases with none or multiple matches
            match_counter = 0
            found_key = ""
            element_match = {}
            reference = None

            if "UserComment" in metadata:
                for line in metadata["UserComment"]:
                    for key, values in element_list.items():
                        if values[1] in line:
                            if "iref" in line:
                                reference = values[1]

                            if not element_list[key][4]:
                                element_list[key][4] = True
                                element_match[key] = element_list[key]
                                found_key = key
                                match_counter += 1
                                break

            if element_list and not element_match:
                for key, values in element_list.items():
                    if key in filepath.stem:
                        element_list[key][4] = True
                        element_match[key] = element_list[key]
                        found_key = key
                        match_counter += 1

            if match_counter == 0:
                element_name = None
            elif match_counter == 1:
                element_name = element_list[found_key][1]
            else:
                if reference is not None:
                    if reference in element_match:
                        element_match.pop(reference, None)
                        key_list = list(element_match.keys())
                        if len(key_list) == 1:
                            element_name = element_match[key_list[0]][1]

    return element_name


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
        # Use the cache so that this unnormalized reader can be shared across
        # a normalized tree and an unnormalized tree.
        self._unnormalized_reader = with_object_cache(
            cache_key, build_reader, filepath, no_device=True
        )
        self._current_filepath = filepath

    def read(self):
        result = self._unnormalized_reader.read()
        # Make changes to result (altering column names, removing extraneous columns)
        # and then return it.
        norm_df = normalize_dataframe(result)
        if norm_df is None:
            return norm_df

        # norm_metadata = {'Columns':self._unnormalized_reader.metadata['Columns']}
        element_name = parse_element_name(
            self._current_filepath, norm_df, self._unnormalized_reader.metadata
        )
        norm_metadata = {
            "Columns": list(norm_df.columns),
            "Element_symbol": element_name,
        }
        return DataFrameAdapter.from_pandas(
            norm_df, metadata=norm_metadata, npartitions=1
        )
