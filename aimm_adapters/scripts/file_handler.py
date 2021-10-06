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


def parse_columns(file, no_device=False):
    # Abbreviated parsing method that is ased on the method used in heald_labview.py.
    # It focuses on extracting the metadata for the column names to be used in more
    # detailed analysis.

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

                    if not no_device:
                        parsed_columns = [
                            term.lstrip() for term in line.split("  ") if term
                        ]
                    else:
                        parsed_columns = []
                        for term in line.split("  "):
                            if term:
                                found_index = term.find(":")
                                if found_index != -1:
                                    temp_term = term[found_index + 1 :]  # noqa: E261
                                    parsed_columns.append(temp_term)
                                else:
                                    parsed_columns.append(term.lstrip())
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


def iter_count_keword(path, keyword):
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
            temp_counter, temp_total = iter_count_keword(filepath, keyword)
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


def iter_unique_keywords(path, tracked_set, start=False, count=False):
    # Recursively, navigates through subfolders and labview files and finds
    # unique keywords that used in the columns names throughout the entire dataset

    for filepath in path.iterdir():
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if not filepath.is_file():
            # Explore subfolder for more labview files recursively
            tracked_set, start = iter_unique_keywords(
                filepath, tracked_set, start, count
            )
            continue
        if filepath.suffix[1:].isnumeric():
            with open(filepath) as file:
                column_names, column_size = parse_columns(file, no_device=True)
                column_set = set(column_names)
                if "Mono Energy" in column_set:
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


def iter_dictionary_read(dict_input, level, str_buffer):
    # Recursively, reads a dictionary and pass it to a buffer

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
    tracked_set, start = iter_unique_keywords(
        Path("../files/"), tracked_set, start=True
    )
    tracked_list = list(tracked_set)
    tracked_list.sort()
    print(tracked_list)


def count_unique_words():
    tracked_dict = dict()
    tracked_dict, start = iter_unique_keywords(
        Path("../files/"), tracked_dict, count=True
    )
    sorted_list = list(
        sorted(tracked_dict.items(), key=lambda item: item[1], reverse=True)
    )
    print(sorted_list)


if __name__ == "__main__":
    find_unique_keywords()
