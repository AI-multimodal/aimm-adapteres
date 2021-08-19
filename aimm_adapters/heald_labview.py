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

                # Reads the following lines to parse a block of information
                # with a specific format
                if parsing_case == ParsingCase.column:
                    line = line.replace("XMAP12:DT Corr I0", "XMAP12:DT_Corr_I0")
                    line = " ".join(line.split())  # Remove unwanted white spaces
                    headers = line.split()
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
                    comment_lines = line.split("   ")
                    meta_dict["SlitInfo"] = comment_lines
                elif parsing_case == ParsingCase.motor:
                    comment_lines = line.split("  ")
                    meta_dict["MotorPositions"] = comment_lines
                elif parsing_case == ParsingCase.panel:
                    comment_lines = line.split("; ")
                    meta_dict["File"] = comment_lines
                elif parsing_case == ParsingCase.beamline:
                    meta_dict["Beamline"] = line
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
    "An another approach"
    with open(filepath) as file:
        metadata, df = parse_heald_labview(file)
    return DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)


def is_candidate(filename):
    filename_ext = filename.split(".")
    if filename_ext[-1].isnumeric():
        return True
    return False


class HealdLabViewTree(Tree):
    @classmethod
    def from_directory(cls, directory):
        mapping = {
            filename: build_reader(Path(directory, filename))
            for filename in os.listdir(directory)
            if is_candidate(filename)
        }
        return super()(mapping)
