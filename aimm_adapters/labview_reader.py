from enum import Enum
import pandas as pd


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


class LabviewFileReader:

    structure_family = "dataframe"

    def __init__(self, path):
        self._file = open(path, "r")

    def parse_file(self):
        lines = self._file.readlines()

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
                    comment_lines = []
                    if line == "Column Headings:":
                        parsing_case = (
                            ParsingCase.column
                        )  # Create headers for dataframe
                        continue
                    elif line == "User Comment:":
                        parsing_case = ParsingCase.user
                        continue
                    elif line == "Scan config:":
                        parsing_case = ParsingCase.scan
                        continue
                    elif line == "Amplifier Sensitivities:":
                        parsing_case = ParsingCase.amplifier
                        continue
                    elif line.find("Analog Input Voltages") != -1:
                        parsing_case = ParsingCase.analog
                        continue
                    elif line == "Mono Info:":
                        parsing_case = ParsingCase.mono
                        continue
                    elif line == "ID Info:":
                        parsing_case = ParsingCase.id_info
                        continue
                    elif line == "Slit Info:":
                        parsing_case = ParsingCase.slit
                        continue
                    elif line == "Motor Positions:":
                        parsing_case = ParsingCase.motor
                        continue
                    elif line.find("LabVIEW Control Panel") != -1:
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
                        meta_dict["AmplifierSensitivities"] = comment_lines
                    elif parsing_case == ParsingCase.analog:
                        comment_lines = line.split("  ")
                        meta_dict["AnalogInputVoltages"] = comment_lines
                    elif parsing_case == ParsingCase.mono:
                        comment_lines = line.split("; ")
                        meta_dict["MonoInfo"] = comment_lines
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
