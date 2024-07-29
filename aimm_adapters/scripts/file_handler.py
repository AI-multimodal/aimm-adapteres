from enum import Enum
from pathlib import Path

import pandas as pd
import xraydb

_EDGE_ENERGY_DICT = {
    xraydb.atomic_symbol(i): [i, xraydb.xray_edges(i)] for i in range(1, 99)
}


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


def parse_columns(file, no_device=False):
    # Abbreviated parsing method that is based on the method used in heald_labview.py.
    # It focuses on extracting the metadata for the column names to be used in more
    # detailed analysis.
    #
    # Used for testing purposes only.
    # Mainly to compare the structure of the columns and column names.

    lines = file.readlines()

    parsing_case = 0
    parsed_columns = []
    data_size = 0
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

                # Start reading the name of the upcoming block of information
                if line == "Column Headings:":
                    parsing_case = ParsingCase.column  # Create headers for dataframe
                    continue

                # Reads the following lines to parse a block of information
                # with a specific format
                if parsing_case == ParsingCase.column:
                    line = line.replace("*", " ")
                    line = line.replace("tempeXMAP4", "tempe        XMAP4")
                    line = line.replace("scatter_Sum XMAP4", "scatter_Sum        XMAP4")
                    line = line.replace("Stats1:TS20-", "Stats1:T        S20-")
                    line = line.replace("Mono Energy (alt)", "Mono Energy")

                    if not no_device:
                        parsed_columns = [
                            term.lstrip() for term in line.split("  ") if term
                        ]
                    else:
                        for term in line.split("  "):
                            if term:
                                term = term.lstrip()
                                ch_list = find_char_indexes(term, ":")
                                if len(ch_list) == 0:
                                    parsed_columns.append(term.lstrip())
                                else:
                                    lower_dev_names = set(
                                        ["pncaux", "pncid", "s20ptc10"]
                                    )
                                    if (
                                        term[: ch_list[0]].isupper()
                                        or term[: ch_list[0]] in lower_dev_names
                                    ):
                                        temp_term = term[ch_list[0] + 1 :]  # noqa: E203
                                    else:
                                        temp_term = term[: ch_list[-1]].lstrip()
                                    parsed_columns.append(temp_term)

            else:
                parsing_case = 0
                continue

        # Parse data
        else:
            if parsing_case == ParsingCase.column:
                line = " ".join(line.split())  # Remove unwanted white spaces
                sample = line.split()
                try:
                    sample = list(map(float, sample))
                except ValueError:
                    print(file.name)
                data_size = len(sample)
                break

    return parsed_columns, data_size


def parse_labview_file(file, no_device=False):
    # This method is a duplicate from the method used in heald_labview:parse_heald_labview.py
    # This mehtod is used to test new ideas that can expand and improve the parsing strategy
    # of heald's dataset

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
    df = pd.DataFrame(data, columns=headers)

    return df, meta_dict


def find_char_indexes(word, char):
    return [i for i, val in enumerate(word) if val == char]


def iter_subdirectory_handler(mapping, path):
    # Recursively, creates a tree-like dictionary by grouping files based on the
    # name of experiment. The end nodes save the information of each file contaning
    # a list with the name of the columns and the size of the list and the size of
    # the columns in the data section. This function is to be used to check that
    # all files have a matching and stable structure in size

    test_group = set()
    name_set = set()
    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            mapping[filepath.name] = {}
            mapping[filepath.name] = iter_subdirectory_handler(
                mapping[filepath.name], filepath
            )
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                columns, column_number = parse_columns(file)

            if filepath.stem in test_group:
                for element in columns:
                    if element not in name_set:
                        name_set.add(element)
                        mapping[filepath.stem][0].append(element)
                        mapping[filepath.stem][2][0] = len(mapping[filepath.stem][0])
                        print(filepath.name)

                mapping[filepath.stem][1].append(filepath.name)

                if column_number != mapping[filepath.stem][2][1]:
                    print(column_number)

                # print(filepath.stem)
            else:
                test_group.add(filepath.stem)
                file_list = [filepath.name]
                mapping[filepath.stem] = [
                    columns,
                    file_list,
                    [column_number, len(columns)],
                ]
                name_set = set(columns)
            # print(filepath.stem)

    return mapping


def iter_subdirectory_handler_v2(mapping, path):
    # Recursively, creates a tree-like dictionary by grouping files based on the
    # name of experiment and similarities between column names

    test_group = set()

    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            mapping[filepath.name] = {}
            mapping[filepath.name] = iter_subdirectory_handler_v2(
                mapping[filepath.name], filepath
            )
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                columns, column_number = parse_columns(file)
                column_key = tuple(columns)

            if filepath.stem not in test_group:
                test_group.add(filepath.stem)
                mapping[filepath.stem] = {}
                file_value = [filepath.name]
                mapping[filepath.stem][column_key] = file_value
            else:
                if column_key in mapping[filepath.stem].keys():
                    mapping[filepath.stem][column_key].append(filepath.name)
                else:
                    file_value = [filepath.name]
                    mapping[filepath.stem][column_key] = file_value

    return mapping


def iter_subdirectory_handler_v3(mapping, path, keyword):
    # Improved recursive method. Creates a tree-like dictionary by grouping files
    # based on the use of a specific keyword.

    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            mapping[filepath.name] = {}
            mapping[filepath.name] = iter_subdirectory_handler_v3(
                mapping[filepath.name], filepath, keyword
            )
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                is_column = find_in_file(file, keyword)

            if filepath.stem not in mapping:
                mapping[filepath.stem] = {keyword: [], "None": []}
            if is_column:
                mapping[filepath.stem][keyword].append(filepath.name)
            else:
                mapping[filepath.stem]["None"].append(filepath.name)

    return mapping


def iter_count_keyword(path, keyword):
    # Recursively, counts the number of times that a keyword is used in all the files
    # of the dataset

    counter = 0
    total = 0
    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            temp_counter, temp_total = iter_count_keyword(filepath, keyword)
            counter += temp_counter
            total += temp_total
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                column_names, column_size = parse_columns(file)
                column_set = set(column_names)
                if keyword in column_set:
                    counter += 1
                total += 1
    return counter, total


def iter_unique_keywords(path, tracked_set, start=False, count=False, collection=None):
    # Recursively, navigates through subfolders and labview files and finds
    # unique keywords that used in the columns names throughout the entire dataset

    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            tracked_set, start, collection = iter_unique_keywords(
                filepath, tracked_set, start, count, collection
            )
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                column_names, column_size = parse_columns(file, no_device=True)

                column_set = set(column_names)
                if "Mono Energy" in column_set and column_size > 0:
                    if not count:
                        # if (
                        #     "I0" not in column_set
                        #     and "IO" not in column_set
                        #     and "I-0" not in column_set
                        # ):
                        #     collection_names = ",".join(column_names)
                        #     collection.add(collection_names)
                        #     print("Not Unique: ", filepath)
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

    return tracked_set, start, collection


def iter_dictionary_read(dict_input, level, str_buffer):
    # Recursively, reads a dictionary and pass it to a buffer

    spacing = "--"

    for key, value in dict_input.items():
        str_buffer += "    " * level + "|" + spacing + " "
        str_buffer += str(key) + ":"
        if type(value) is dict:
            str_buffer += "\n"
            str_buffer = iter_dictionary_read(value, level + 1, str_buffer)
        else:
            str_buffer += str(value) + "\n"

    return str_buffer


def iter_element_name_parse(path):

    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            iter_element_name_parse(filepath)
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                df, metadata = parse_labview_file(file)

            if not df.empty:
                element_name, edge_symbol = parse_element_name(filepath, df, metadata)
                print(element_name, edge_symbol)


def write_file_structure(keyword):
    # Navigates through all the subfolders in the dataset, finds the compatible files,
    # creates a tree structure with their information and writes it into a file.

    print("Generating file...")
    mapping = {}
    # mapping = iter_subdirectory_handler_v2(mapping, Path("../files/"))
    mapping = iter_subdirectory_handler_v3(mapping, Path("../files/"), keyword)
    string_buffer = iter_dictionary_read(mapping, 0, "")

    # with open("labview_file_tree.txt", "w") as file:
    filename = "files/" + keyword + "_file_tree.txt"
    with open(filename, "w") as file:
        file.write(string_buffer)

    print("Done!!")


def find_in_file(file, keyword):
    column_names, column_size = parse_columns(file)
    column_set = set(column_names)
    if keyword in column_set:
        return True
    return False


def find_unique_keywords():
    tracked_set = set()
    collection = set()
    tracked_set, start, collection = iter_unique_keywords(
        Path("../files/"), tracked_set, start=True, collection=collection
    )
    tracked_list = list(tracked_set)
    tracked_list.sort()
    print(tracked_list, len(tracked_list))
    print(collection)


def count_unique_words():
    tracked_dict = dict()
    tracked_dict, start, collection = iter_unique_keywords(
        Path("../files/"), tracked_dict, count=True
    )
    sorted_list = list(
        sorted(tracked_dict.items(), key=lambda item: item[1], reverse=True)
    )
    print(sorted_list)


def parse_element_name(filepath, df, metadata):

    element_name = None
    edge_symbol = None
    if "Mono Energy" in set(df.keys()):
        energy = df["Mono Energy"]
        if len(energy) > 1:
            min_max = [min(energy), max(energy)]

            element_list = {}
            # Find if the edge value of an element in xrayDB is inside the range of
            # Mono Energy values of the current file
            # An element in XrayDB can contain more than one edge, each one identified by
            # a unique IUPAC symbol
            for current_element, values in _EDGE_ENERGY_DICT.items():
                edges = values[1]
                # Most of the cases are solved with a 'K' edge value. This is added to
                # improve computing time
                if "K" in edges:
                    if (
                        edges["K"].energy >= min_max[0]
                        and edges["K"].energy <= min_max[1]
                    ):
                        element_list[current_element] = [
                            values[0],
                            current_element,
                            "K",
                            edges["K"].energy,
                            False,
                        ]
                else:
                    for key in edges:
                        if (
                            edges[key].energy >= min_max[0]
                            and edges[key].energy <= min_max[1]
                        ):
                            element_list[current_element] = [
                                values[0],
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
                # print("No match", filepath)
                element_name = None
                edge_symbol = None
            elif match_counter == 1:
                element_name = element_list[found_key][1]
                edge_symbol = element_list[found_key][2]
            else:
                if reference is not None:
                    if reference in element_match:
                        element_match.pop(reference, None)
                        key_list = list(element_match.keys())
                        if len(key_list) == 1:
                            element_name = element_match[key_list[0]][1]
                            edge_symbol = element_match[key_list[0]][2]

    return element_name, edge_symbol


if __name__ == "__main__":
    # find_unique_keywords()
    count_unique_words()

    # filename = Path("../files/")
    # parse_element_name(filename)
    # iter_element_name_parse(filename)
