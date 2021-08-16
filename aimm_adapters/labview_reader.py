import pandas as pd


class LabviewFileReader:

    structure_family = "dataframe"

    def __init__(self, path):
        self._file = open(path, "r")

    def parse_file(self):
        Lines = self._file.readlines()

        parsing_case = 0
        data = []
        comment_lines = []
        meta_dict = {}
        for line in Lines:
            line = line.rstrip()
            # Parse comments as metadata
            if line[0] == "#":
                if len(line) > 2:
                    line = line[2:]

                    # Add additional cases to parse more information from the
                    # header comments
                    # Start reading the name of the upcoming block of information
                    comment_lines = []
                    if line == "Column Headings:":
                        parsing_case = 1  # Create headers for dataframe
                        continue
                    elif line == "User Comment:":
                        parsing_case = 2
                        continue
                    elif line == "Scan config:":
                        parsing_case = 3
                        continue
                    elif line == "Amplifier Sensitivities:":
                        parsing_case = 4
                        continue
                    elif line.find("Analog Input Voltages") != -1:
                        parsing_case = 5
                        continue
                    elif line == "Mono Info:":
                        parsing_case = 6
                        continue
                    elif line == "ID Info:":
                        parsing_case = 7
                        continue
                    elif line == "Slit Info:":
                        parsing_case = 8
                        continue
                    elif line == "Motor Positions:":
                        parsing_case = 9
                        continue
                    elif line.find("LabVIEW Control Panel") != -1:
                        parsing_case = 10
                    elif line.find("Beamline") != -1:
                        parsing_case = 11
                    # Reads the following lines for a specific block of information
                    if parsing_case == 1:
                        line = line.replace("XMAP12:DT Corr I0", "XMAP12:DT_Corr_I0")
                        line = " ".join(line.split())  # Remove unwanted white spaces
                        headers = line.split()
                        meta_dict["Columns"] = headers
                        parsing_case = 0
                    elif parsing_case == 2:
                        line = " ".join(line.split())  # Remove unwanted white spaces
                        comment_lines.append(line)
                        meta_dict["UserComment"] = comment_lines
                    elif parsing_case == 3:
                        line = " ".join(line.split())  # Remove unwanted white spaces
                        comment_lines.append(line)
                        meta_dict["ScanConfig"] = comment_lines
                    elif parsing_case == 4:
                        comment_lines = line.split("  ")
                        meta_dict["AmplifierSensitivities"] = comment_lines
                    elif parsing_case == 5:
                        comment_lines = line.split("  ")
                        meta_dict["AnalogInputVoltages"] = comment_lines
                    elif parsing_case == 6:
                        comment_lines = line.split("; ")
                        meta_dict["MonoInfo"] = comment_lines
                    elif parsing_case == 7:
                        comment_lines = line.split("  ")
                        meta_dict["IDInfo"] = comment_lines
                    elif parsing_case == 8:
                        comment_lines = line.split("   ")
                        meta_dict["SlitInfo"] = comment_lines
                    elif parsing_case == 9:
                        comment_lines = line.split("  ")
                        meta_dict["MotorPositions"] = comment_lines
                    elif parsing_case == 10:
                        comment_lines = line.split("; ")
                        meta_dict["File"] = comment_lines
                    elif parsing_case == 11:
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