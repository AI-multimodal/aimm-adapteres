import pathlib
import re

import pandas as pd


def parse_charge(charge_params):
    if "C" in charge_params:
        state = "C"
    else:
        state = "DC"

    cycle = int(charge_params[:-1])

    charge = {"cycle": cycle, "state": state}

    return charge


def read_electrochem(c, data_path):
    c_uid = c["uid"]

    files = list(data_path.glob("*.csv"))

    data = []
    metadata = []

    extra_meta = {
        "NCM622": {"loading_mass": 0.1638, "current": 30},
        "NCM712": {"loading_mass": 0.1661, "current": 30},
        "NCM712-Al": {"loading_mass": 0.1568, "current": 30},
        "NCM811": {"loading_mass": 0.1675, "current": 30},
        "NCMA": {"loading_mass": 0.1671, "current": 30},
    }

    units = {"units": {"loading_mass": "g", "current": "mA/g"}}

    for file in files:
        fname = file.name
        print(fname)

        if file.stem == "811-Al":
            sample_name = "NCMA"
        else:
            sample_name = "NCM" + file.stem

        with open(file, "r") as f:
            raw_df = pd.read_csv(f, sep=",", header=[0, 1])

            for i in range(0, raw_df.shape[1], 2):
                columns = [
                    raw_df.iloc[:, i].name[0],
                    raw_df.iloc[:, i + 1].name[0],
                ]  # To save in the metadata
                edited_columns = [
                    columns[0].split()[0].lower(),
                    columns[1].split()[0].lower(),
                ]  # No units; just the names.
                # To use in the dataframes used in tiled
                table_units = {
                    edited_columns[0]: re.sub("[()]", "", columns[0].split()[1]),
                    edited_columns[1]: re.sub("[()]", "", columns[1].split()[1]),
                }

                df = pd.DataFrame(
                    {
                        edited_columns[0]: raw_df.iloc[:, i],
                        edited_columns[1]: raw_df.iloc[:, i + 1],
                    }
                )
                df = df.dropna()

                charge = parse_charge(raw_df.iloc[:, i].name[1])
                meta = {
                    "dataset": "nmc_electrochem",
                    "fname": fname,
                    "facility": {"name": "ALS"},
                    "beamline": {"name": "8.0.1"},
                    "sample": {"name": sample_name},
                    "charge": charge,
                }

                meta.update(extra_meta[sample_name])
                meta.update(units)
                meta["units"].update(table_units)

                c_uid.write_dataframe(df, meta, specs=["electrochemistry"])

                data.append(df)
                metadata.append(meta)

    return data, metadata


if __name__ == "__main__":

    data_path = pathlib.Path(
        "C:/Users/jmaruland/Documents/GitHub/aimm-adapters/aimm_adapters/files"
    )

    data, metadata = read_electrochem(data_path)
