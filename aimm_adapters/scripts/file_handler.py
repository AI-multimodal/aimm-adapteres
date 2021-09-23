from pathlib import Path

from enum import Enum


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


def parse_columns(file):
    lines = file.readlines()

    parsing_case = 0
    parsed_columns = []
    data_size = 0

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

                # Start reading the name of the upcoming block of information
                if line == "Column Headings:":
                    parsing_case = ParsingCase.column  # Create headers for dataframe
                    continue

                # Reads the following lines to parse a block of information
                # with a specific format
                if parsing_case == ParsingCase.column:
                    line = line.replace("*", " ")
                    line = line.replace("tempeXMAP4", "tempe        XMAP4")

                    parsed_columns = [term for term in line.split("  ") if term]
                    parsing_case = 0
                    break
            else:
                parsing_case = 0
                continue

        # Parse data
        else:
            line = " ".join(line.split())  # Remove unwanted white spaces
            sample = line.split()
            try:
                sample = list(map(float, sample))
            except ValueError:
                print(file.name)
            data_size = len(sample)
            break

    return parsed_columns, data_size


def iter_subdirectory_handler(mapping, path):
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


def iter_dictionary_read(dict_input, level, str_buffer):
    spacing = "--"

    for key, value in dict_input.items():
        str_buffer += "    " * level + "|" + spacing + " "
        str_buffer += str(key) + ":"
        if type(value) == dict:
            str_buffer += "\n"
            str_buffer = iter_dictionary_read(value, level + 1, str_buffer)
        else:
            str_buffer += str(value) + "\n"

    return str_buffer


def write_file_structure():

    print("Generating file...")
    mapping = {}
    mapping = iter_subdirectory_handler_v2(mapping, Path("../files/"))
    string_buffer = iter_dictionary_read(mapping, 0, "")

    with open("labview_file_tree.txt", "w") as file:
        file.write(string_buffer)

    print("Done!!")
