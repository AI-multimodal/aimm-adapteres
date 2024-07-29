"""
This scripts contians all the ingestion methods that have been used to add datasets to aimmdb.


"""

import json
import pathlib
import re

import pandas as pd
from heald_labview import mangle_dup_names
from tiled.queries import Key


# NOTE this hardcodes BM prefix
def parse_filename(name):
    """
    Extract the experiment parameters of a filename in Wanli's data.

    Parameters
    ----------
    name : string
        Name of the file.

    Returns
    -------
    sample : string
        Nametag of the sample used in aimmdb for the experiment in the file.
    charge : dict
        Cycle, voltage, state values of this experiment.

    """

    if "622" in name:
        sample = "BM_NCM622"
    elif "NCMA" in name:
        sample = "BM_NCMA"
    elif "712_Al_free" in name:
        sample = "BM_NCM712"
    elif "712" in name:
        sample = "BM_NCM712-Al"
    elif "811" in name:
        sample = "BM_NCM811"
    else:
        raise KeyError(f"unable to parse sample from {name}")

    if sample == "Ni_metal":
        charge = None
    elif "Pristine" in name:
        charge = (0, 0.0, "DC")
    else:
        if "1st" in name:
            cycle = 1
        elif "2nd" in name:
            cycle = 2
        elif "10th" in name:
            cycle = 10
        else:
            raise KeyError(f"unable to parse cycle from {name}")

        voltage_str = re.search("(\d*)V", name)[0]  # noqa: W605, E261
        if voltage_str == "43V":
            voltage = 4.3
            state = "C"
        elif voltage_str == "48V":
            voltage = 4.8
            state = "C"
        elif voltage_str == "3V":
            voltage = 3.0
            state = "DC"
        else:
            raise KeyError(f"unable to parse voltage from {voltage_str}")

        charge = (cycle, voltage, state)

    if charge:
        keys = ["cycle", "voltage", "state"]
        charge = dict(zip(keys, charge))
    return sample, charge


def parse_filename_gihyeok(name):
    """
    Extract the experiment parameters of a filename in Gihyeok's data.

    Parameters
    ----------
    name : string
        Name of the file.

    Returns
    -------
    sample : string
        Nametag of the sample used in aimmdb for the experiment in the file.
    symbol : string
        Element symbol extracted from the file name.

    """

    if "622" in name:
        sample = "BM_NCM622"
    elif "NCMA" in name:
        sample = "BM_NCMA"
    elif "712_Al-doped" in name:
        sample = "BM_NCM712-Al"
    elif "712" in name:
        sample = "BM_NCM712"
    elif "811" in name:
        sample = "BM_NCM811"
    else:
        raise KeyError(f"unable to parse sample from {name}")

    if "Co" in name:
        symbol = "Co"
    elif "Mn" in name:
        symbol = "Mn"
    elif "Ni" in name:
        symbol = "Ni"
    elif "O" in name:
        symbol = "O"
    else:
        raise KeyError(f"unable to parse symbol from {name}")

    return sample, symbol


def parse_filename_nslsii(name):
    """
    Extract the experiment parameters of a filename in Eli's first batch of data.

    Parameters
    ----------
    name : string
        Name of the file.

    Returns
    -------
    sample : string
        Nametag of the sample used in aimmdb for the experiment in the file.
    charge : dict
        Cycle, voltage, state values of this experiment.

    """

    if "622" in name:
        sample = "BM_NCM622"
    elif "NCMA" in name or "NMCA" in name:
        sample = "BM_NCMA"
    elif "712" in name or "721" in name:
        sample = "BM_NCM712-Al"
    elif "811" in name:
        sample = "BM_NCM811"
    else:
        raise KeyError(f"unable to parse sample from {name}")

    if "pristine" in name or "Pristine" in name:
        charge = (0, 0.0, "DC")
    else:
        if "1st" in name:
            cycle = 1
        elif "2nd" in name:
            cycle = 2
        elif "10th" in name:
            cycle = 10
        else:
            # raise KeyError(f"unable to parse cycle from {name}")
            cycle = 1  # In Eli's dataset, files with no cycle value in the filename
            # are assumed to be cycle = 1

        if "4_3" in name or "43C" in name:
            voltage = 4.3
            state = "C"
        elif "4_8" in name or "48C" in name:
            voltage = 4.8
            state = "C"
        elif "3V" in name or "3_0" in name or "30DC" in name:
            voltage = 3.0
            state = "DC"
        else:
            raise KeyError(f"unable to parse voltage from {name}")

        charge = (cycle, voltage, state)

    if charge:
        keys = ["cycle", "voltage", "state"]
        charge = dict(zip(keys, charge))
    return sample, charge


def get_experiment_params(name):
    """
    Extract the experiment parameters of a filename in Eli's second batch of data.
    This method uses a dictionary with predefined keys to link an experiment configuration
    with the right file.

    Parameters
    ----------
    name : string
        Name of the file.

    Returns
    -------
    sample : string
        Nametag of the sample used in aimmdb for the experiment in the file.
    charge : dict
        Cycle, voltage, state values of this experiment.

    """

    experiment_to_params_map = {
        "1-1": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 1, "voltage": 3.0, "state": "DC"},
        },
        "1-2": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 10, "voltage": 4.8, "state": "C"},
        },
        "1-3": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 1, "voltage": 3.0, "state": "DC"},
        },
        "1-4": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 10, "voltage": 3.0, "state": "DC"},
        },
        "2-1": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 2, "voltage": 4.8, "state": "C"},
        },
        "2-2": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 1, "voltage": 4.8, "state": "C"},
        },
        "2-3": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 10, "voltage": 4.8, "state": "C"},
        },
        "2-4": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 1, "voltage": 3.0, "state": "DC"},
        },
        "3-1": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 0, "voltage": 0.0, "state": "DC"},
        },
        "3-2": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 2, "voltage": 4.8, "state": "C"},
        },
        "3-3": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 2, "voltage": 4.8, "state": "C"},
        },
        "3-4": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 2, "voltage": 4.8, "state": "C"},
        },
        "4-1": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 2, "voltage": 4.3, "state": "C"},
        },
        "4-2": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 1, "voltage": 4.8, "state": "C"},
        },
        "4-3": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 10, "voltage": 3.0, "state": "DC"},
        },
        "4-4": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 1, "voltage": 4.8, "state": "C"},
        },
        "5-1": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 1, "voltage": 4.3, "state": "C"},
        },
        "5-2": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 2, "voltage": 4.3, "state": "C"},
        },
        "5-3": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 1, "voltage": 4.8, "state": "C"},
        },
        "5-4": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 10, "voltage": 3.0, "state": "DC"},
        },
        "6-1": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 1, "voltage": 3.0, "state": "DC"},
        },
        "6-2": {
            "sample": "BM_NCM712-Al",
            "charge": {"cycle": 2, "voltage": 4.8, "state": "C"},
        },
        "6-3": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 10, "voltage": 4.8, "state": "C"},
        },
        "6-4": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 0, "voltage": 0.0, "state": "DC"},
        },
        "7-1": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 2, "voltage": 4.3, "state": "C"},
        },
        "7-2": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 1, "voltage": 4.3, "state": "C"},
        },
        "7-3": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 0, "voltage": 0.0, "state": "DC"},
        },
        "7-4": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 1, "voltage": 4.3, "state": "C"},
        },
        "8-1": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 0, "voltage": 0.0, "state": "DC"},
        },
        "8-2": {
            "sample": "BM_NCM811",
            "charge": {"cycle": 1, "voltage": 4.3, "state": "C"},
        },
        "8-3": {
            "sample": "BM_NCMA",
            "charge": {"cycle": 10, "voltage": 4.8, "state": "C"},
        },
        "8-4": {
            "sample": "BM_NCM622",
            "charge": {"cycle": 10, "voltage": 4.8, "state": "C"},
        },
    }

    split_name = name.split()
    experiment_number = split_name[0]
    experiment_params = experiment_to_params_map[experiment_number]

    return experiment_params["sample"], experiment_params["charge"]


def read_header(f):
    """
    Reads the column names of the data in file

    Parameters
    ----------
    f : list[string]
        Lines read fromn the file.

    Returns
    -------
    header : list[string]
        List of the column names in the file.

    """

    header = ""
    for line in f:
        if line.startswith("Time (s)"):
            line = line.rstrip()
            header = line.split("\t")
            return header


def read_wanli(f):
    """
    Reads the data of a file and generates a dataframe with the designated names

    Parameters
    ----------
    f : list[string]
        Lines read fromn the file.

    Returns
    -------
    df : pandas.dataframe
        dataframe with data collected from an input file.

    """

    names = read_header(f)
    names = mangle_dup_names(names)
    df = pd.read_csv(f, sep="\t", names=names)

    translation = {
        "Mono Energy": "energy",
        "Counter 0": "i0",
        "Counter 1": "tey",
        "Counter 2": "tfy",
        # "Counter 3": "i0",
    }
    df = df.rename(columns=translation)[list(translation.values())]

    df["mu_tfy"] = df["tfy"] / df["i0"]
    df["mu_tey"] = df["tey"] / df["i0"]

    return df


def read_eli_txt(f):
    """
    Reads txt files from Eli's data that contains metadata only

    Parameters
    ----------
    f : file
        file with raw text.

    Returns
    -------
    metadata : dict
        structured information about an experiment extracted from a text file.

    """

    lines = f.readlines()

    metadata = {}

    for line in lines:
        line = line.rstrip()

        if line[0] == "#":
            # Metadata
            meta_raw = line[2:]
            if "Column" in meta_raw:
                break  # No more relevant information after this line
            else:
                if ": " in meta_raw:
                    # Builds metadata
                    meta_split = meta_raw.split(": ", 1)
                    keys = meta_split[0].split(".")
                    if keys[0].lower() not in metadata:
                        metadata[keys[0].lower()] = {}

                    metadata[keys[0].lower()][keys[1].lower()] = meta_split[1]

    return metadata


def read_eli_dat(f):
    """
    Reads .dat files from Eli's dataset and converts

    Parameters
    ----------
    f : file
        file object to parse as dataframe.

    Returns
    -------
    df : pandas.dataframe
        Parsed data of file as a dataframe with the proper translated column names.

    """

    df = pd.read_csv(f, sep=",", index_col=0)
    df.columns = ["energy", "mutrans", "mufluor", "murefer"]
    return df


def ingest_aimm_ncm_wanli(c, sample_ids, data_path):
    """
    Given a specific path, finds and ingest the txt files to aimmdb.
    This method works for the files provided by Wanli.

    Parameters
    ----------
    c : tiled.client
        client object connected to aimmdb.
    sample_ids : dict[string:string]
        key/value pairs of the sample names and their unique ids assigned in aimmdb.
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    None.

    """

    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.name
        print(fname)

        try:
            sample_name, charge = parse_filename(fname)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]

        with open(file, "r") as f:
            df = read_wanli(f)

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "charge": charge,
            "facility": {"name": "ALS"},
            "beamline": {"name": "8.0.1"},
            "element": {"symbol": "Ni", "edge": "L3"},
            "sample_id": sample_id,
        }

        results = c_uid.search(Key("fname") == fname)
        if len(results) != 0:
            uid = results.values().first().item["id"]
            del c_uid[uid]
            c_uid.write_dataframe(
                df, metadata, specs=["XAS_TEY", "XAS_TFY", "HasBatteryChargeData"]
            )
        else:
            print(f"{fname} is not part of the database")


def ingest_aimm_ncm_eli(c, sample_ids, data_path):
    """
    Given a specific path, finds and ingest the .txt and .dat files to aimmdb.
    This method works for the files provided by Eli.

    Parameters
    ----------
    c : tiled.client
        client object connected to aimmdb..
    sample_ids : dict[string:string]
        key/value pairs of the sample names and their unique ids assigned in aimmdb.
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    None.

    """

    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.stem  # source of data comes from two different files .dat and .txt
        print(fname)

        try:
            # sample_name, charge = parse_filename_nslsii(fname)
            sample_name, charge = get_experiment_params(fname)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]

        with open(file, "r") as f:
            meta = read_eli_txt(f)
            meta["beamline"]["name"] = "ISS"
            meta["sample"]["name"] = sample_name

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "charge": charge,
            "sample_id": sample_id,
        }

        metadata.update(meta)

        dat_file = data_path / (fname + ".dat")
        with open(dat_file, "r") as f:
            df = read_eli_dat(f)

        c_uid.write_dataframe(df, metadata, specs=["XAS", "HasBatteryChargeData"])


def ingest_aimm_ncm_gihyeok(c, sample_ids, data_path):
    """
    Given a specific path, finds and ingest the txt files to aimmdb.
    This method works for the files provided by Gihyeok.

    Parameters
    ----------
    c : tiled.client
        client object connected to aimmdb..
    sample_ids : dict[string:string]
        key/value pairs of the sample names and their unique ids assigned in aimmdb.
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    None.

    """

    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.name
        print(fname)

        try:
            sample_name, symbol = parse_filename_gihyeok(fname)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]

        with open(file, "r") as f:
            df = pd.read_csv(f, sep="\t")

        new_columns = ["energy", "normfluor"]

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "facility": {"name": "ALS"},
            "beamline": {"name": "8.0.1"},
            "element": {"symbol": symbol, "edge": "L3"},
            "sample_id": sample_id,
        }

        for column in df.columns:
            if column != "Energy":
                sub_df = df[["Energy", column]].copy()
                sub_df.columns = new_columns

                if "Pristine" in column:
                    charge = (0, 0.0, "DC")
                else:
                    if "1st" in column:
                        cycle = 1
                    elif "2nd" in column:
                        cycle = 2
                    elif "10th" in column:
                        cycle = 10
                    else:
                        raise KeyError(f"unable to parse cycle from {column}")

                    if "3.0" in column:
                        voltage = 3.0
                        state = "DC"
                    elif "4.8" in column:
                        voltage = 4.8
                        state = "C"
                    else:
                        raise KeyError(f"unable to parse voltage from {column}")

                    charge = (cycle, voltage, state)

                keys = ["cycle", "voltage", "state"]
                charge = dict(zip(keys, charge))
                metadata["charge"] = charge

                c_uid.write_dataframe(
                    sub_df, metadata, specs=["XAS", "HasBatteryChargeData"]
                )


def get_sample_charge(name):
    """
    Given a specific filename from Gihyeok's datafinds the correct sample name

    Parameters
    ----------
    name : string
        Name of the file.

    Returns
    -------
    sample : string
        Nametag of the sample used in aimmdb for the experiment in the file.

    """

    file_to_sample_map = {
        "BM_NCM622": [
            "Sigscan71987",
            "Sigscan71991",
            "Sigscan71996",
            "Sigscan72001",
            "Sigscan72005",
            "Sigscan72009",
            "Sigscan71970",
            "Sigscan71974",
        ],
        "BM_NCMA": [
            "Sigscan71986",
            "Sigscan71990",
            "Sigscan71995",
            "Sigscan71999",
            "Sigscan72004",
            "Sigscan72008",
            "Sigscan71969",
            "Sigscan71973",
        ],
        "BM_NCM811": [],
        "BM_NCM712-Al": [
            "Sigscan71983",
            "Sigscan71988",
            "Sigscan71993",
            "Sigscan71997",
            "Sigscan72002",
            "Sigscan72006",
            "Sigscan72010",
            "Sigscan71971",
        ],
        "BM_NCM712": [
            "Sigscan71294",
            "Sigscan71295",
            "Sigscan71296",
            "Sigscan71297",
            "Sigscan71312",
            "Sigscan71313",
            "Sigscan71314",
            "Sigscan71315",
            "Sigscan71985",
            "Sigscan71989",
            "Sigscan71994",
            "Sigscan71998",
            "Sigscan72003",
            "Sigscan72007",
            "Sigscan71968",
            "Sigscan71972",
        ],
    }

    file_to_charge_map = {
        "Pristine": [
            "Sigscan71294",
            "Sigscan72001",
            "Sigscan72002",
            "Sigscan72003",
            "Sigscan72004",
        ],
        "1st_43V": [
            "Sigscan71295",
            "Sigscan72005",
            "Sigscan72006",
            "Sigscan72007",
            "Sigscan72008",
        ],
        "1st_48V": [
            "Sigscan71296",
            "Sigscan72009",
            "Sigscan72010",
            "Sigscan71968",
            "Sigscan71969",
        ],
        "1st_3V": [
            "Sigscan71297",
            "Sigscan71970",
            "Sigscan71971",
            "Sigscan71972",
            "Sigscan71973",
        ],
        "2nd_43V": [
            "Sigscan71312",
            "Sigscan71983",
            "Sigscan71985",
            "Sigscan71986",
            "Sigscan71974",
        ],
        "2nd_48V": [
            "Sigscan71313",
            "Sigscan71987",
            "Sigscan71988",
            "Sigscan71989",
            "Sigscan71990",
        ],
        "10th_48V": [
            "Sigscan71314",
            "Sigscan71991",
            "Sigscan71993",
            "Sigscan71994",
            "Sigscan71995",
        ],
        "10th_3V": [
            "Sigscan71315",
            "Sigscan71996",
            "Sigscan71997",
            "Sigscan71998",
            "Sigscan71999",
        ],
    }

    str_sample = None
    str_charge = None

    fname = name.split("-")[0].capitalize()

    for key, value in file_to_sample_map.items():
        if fname in value:
            str_sample = key
            break

    for key, value in file_to_charge_map.items():
        if fname in value:
            str_charge = key
            break

    if str_sample is not None and str_charge is not None:
        return str_sample + "_" + str_charge


def ingest_aimm_ncm_gihyeok_sigscan(c, sample_ids, data_path):
    """
    Given a specific path, finds and ingest the txt files to aimmdb.
    This method works for the files provided by Gihyeok for Co data.

    Parameters
    ----------
    c : tiled.client
        client object connected to aimmdb..
    sample_ids : dict[string:string]
        key/value pairs of the sample names and their unique ids assigned in aimmdb.
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    None.

    """

    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.name
        print(fname)

        file_params = get_sample_charge(file.stem)
        print(file_params)

        try:
            sample_name, charge = parse_filename(file_params)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]
        print(sample_name, sample_id)

        with open(file, "r") as f:
            df = read_wanli(f)

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "charge": charge,
            "facility": {"name": "ALS"},
            "beamline": {"name": "8.0.1"},
            "element": {"symbol": "Co", "edge": "L3"},
            "sample_id": sample_id,
        }

        c_uid.write_dataframe(
            df, metadata, specs=["XAS_TEY", "XAS_TFY", "HasBatteryChargeData"]
        )


def ingest_aimm_ncm_gihyeok_oxygen(c, sample_ids, data_path):
    """
    Given a specific path, finds and ingest the txt files to aimmdb.
    This method works for the files provided by Gihyeok with Oxigen data.

    Parameters
    ----------
    c : tiled.client
        client object connected to aimmdb..
    sample_ids : dict[string:string]
        key/value pairs of the sample names and their unique ids assigned in aimmdb.
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    None.

    """

    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.name
        print(fname)

        try:
            sample_name, symbol = parse_filename_gihyeok(fname)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]

        with open(file, "r") as f:
            df = pd.read_csv(f, sep="\t")

        new_columns = ["energy", "normfluor"]

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "facility": {"name": "ALS"},
            "beamline": {"name": "8.0.1"},
            "element": {"symbol": symbol, "edge": "K"},
            "sample_id": sample_id,
        }

        for i in range(0, len(df.columns), 2):
            sub_df = df[[df.columns[i], df.columns[i + 1]]].copy()
            sub_df.columns = new_columns
            sub_df = sub_df.dropna()

            column = df.columns[i + 1]
            if "Pristine" in column:
                charge = (0, 0.0, "DC")
            else:
                if "1st" in column:
                    cycle = 1
                elif "2nd" in column:
                    cycle = 2
                elif "10th" in column:
                    cycle = 10
                else:
                    raise KeyError(f"unable to parse cycle from {column}")

                if "3.0" in column:
                    voltage = 3.0
                    state = "DC"
                elif "4.3" in column:
                    voltage = 4.3
                    state = "C"
                elif "4.8" in column:
                    voltage = 4.8
                    state = "C"
                else:
                    raise KeyError(f"unable to parse voltage from {column}")

                charge = (cycle, voltage, state)

            keys = ["cycle", "voltage", "state"]
            charge = dict(zip(keys, charge))
            metadata["charge"] = charge

            c_uid.write_dataframe(
                sub_df, metadata, specs=["XAS", "HasBatteryChargeData"]
            )


def read_aimm_ncm_vasp(data_path):
    """
    Given a specific path, finds and parses simulation data to aimmdb.
    This method works for the file provided by Yimming with VASP data.
    The resulting data and metadata are related by key

    Parameters
    ----------
    data_path : pathlib.Path
        Path to the folder with the files.

    Returns
    -------
    data : dict[string:dict]
        Data collected from json file.
    meta : dict[string:dict]
        Metadata representing every sample in data.

    """

    file = data_path / "xas_nmc_vasp.json"
    if file.is_file():
        with open(file, "r") as f:
            file_data = json.load(f)
            data = {}
            meta = {}
            if file_data["energy"].keys() == file_data["intensity"].keys():
                for key, values in file_data["energy"].items():
                    data[key] = {
                        "energy": values,
                        "mutrans": file_data["intensity"][key],
                    }

                    meta[key] = {
                        "dataset": "sim_nmc",
                        "fname": file.name,
                        "facility": {"name": "CNM"},
                        "beamline": {"name": "Simulation"},
                    }
            return data, meta


def ingest_aimm_ncm_sim(c, data_path, input_type):

    file_types = {
        "xanes": {
            "fname": "HJ_NMC_XANES.json",
            "structure_index": "structure_id",
            "sim_code": "xanes",
        },
        "feff": {
            "fname": "nmc_feff_xas_v3_structure.json",
            "structure_index": "structure_index",
            "sim_code": "feff",
        },
        "vasp": {
            "fname": "nmc_vasp_xas_v3_structure.json",
            "structure_index": "structure_index",
            "sim_code": "vasp",
        },
    }

    if input_type not in file_types:
        raise "wrong input type for reading file"

    c_uid = c["uid"]
    file = data_path / file_types[input_type]["fname"]
    if file.is_file():
        with open(file, "r") as f:
            file_data = json.load(f)

            metadata_list = []
            df_list = []
            for i in range(len(file_data)):
                if len(file_data[i]["spectra"]) > 0:
                    try:
                        structure_id = (
                            c["dataset"]["nmc_sim_structure"]["uid"]
                            .search(
                                Key("structure_index")
                                == file_data[i][
                                    file_types[input_type]["structure_index"]
                                ]
                            )
                            .keys()
                            .first()
                        )  # Get unique id generated in the server for the structure
                        print(
                            f"{i} - {len(file_data)} - {file_data[i]['structure_index']} - {structure_id}"
                        )
                    except KeyError:
                        print(f"failed to extract structure from id: {i}")

                    if structure_id is not None:
                        for j in range(len(file_data[i]["spectra"])):
                            metadata = {}
                            metadata["dataset"] = "nmc_sim"
                            metadata["element"] = {
                                "edge": file_data[i]["spectra"][j]["edge"],
                                "symbol": file_data[i]["spectra"][j]["absorbing_atom"],
                            }
                            metadata["fname"] = file.name
                            metadata["facility"] = {"name": "CNM"}
                            metadata["beamline"] = {"name": "Simulation"}
                            metadata["spectra_path"] = {"id": i, "spectra_id": j}
                            metadata["structure_id"] = structure_id
                            metadata["sample"] = {"name": file_data[i]["composition"]}
                            metadata["sim_code"] = file_types[input_type]["sim_code"]

                            df = pd.DataFrame(
                                {
                                    "energy": file_data[i]["spectra"][j]["energy"],
                                    "mutrans": file_data[i]["spectra"][j]["mutrans"],
                                }
                            )

                            c_uid.write_dataframe(df, metadata, specs=["simulation"])
                            metadata_list.append(metadata)
                            df_list.append(df)
                            print(f"{file.name}: id: {i} - spectra_id: {j}")

                    else:
                        print(f"None structure: {i} - {file_data[i]['structure_id']}")


def ingest_aimm_structures(c, data_path):
    c_uid = c["uid"]
    file = data_path / "nmc_structure_v3.json"
    if file.is_file():
        with open(file, "r") as f:
            file_data = json.load(f)

            structures = []
            df = pd.DataFrame()
            for i in range(len(file_data["structure_index"])):
                struct_dict = {}
                struct_dict["dataset"] = "nmc_sim_structure"
                struct_dict["structure_index"] = file_data["structure_index"][str(i)]
                struct_dict["sample"] = {"name": file_data["composition"][str(i)]}
                struct_dict["structure"] = file_data["structure"][str(i)]

                struct_dict["fname"] = file.name
                struct_dict["facility"] = {"name": "CNM"}
                struct_dict["beamline"] = {"name": "Simulation"}

                structures.append(struct_dict)

                c_uid.write_dataframe(df, structures[i], specs=["structure"])
                print(file.name, i)


def ingest_aimm_ncm_sim_xanes(c, data_path):
    c_uid = c["uid"]
    file = data_path / "HJ_NMC_XANES.json"
    if file.is_file():
        with open(file, "r") as f:
            file_data = json.load(f)

            metadata_list = []
            df_list = []
            for i in range(len(file_data)):
                try:
                    structure_id = (
                        c["dataset"]["nmc_sim_structure"]["uid"]
                        .search(Key("structure_index") == file_data[i]["structure_id"])
                        .keys()
                        .first()
                    )  # Get unique id generated in the server for the structure
                    print(
                        f"{i} - {len(file_data)} - {file_data[i]['structure_id']} - {structure_id}"
                    )
                except KeyError:
                    print(f"failed to extract structure from id: {i}")

                if structure_id is not None:
                    for j in range(len(file_data[i]["spectra"])):
                        metadata = {}
                        metadata["dataset"] = "nmc_sim"
                        metadata["element"] = {
                            "edge": file_data[i]["spectra"][j]["edge"],
                            "symbol": file_data[i]["spectra"][j]["absorbing_atom"],
                        }
                        metadata["fname"] = file.name
                        metadata["facility"] = {"name": "CNM"}
                        metadata["beamline"] = {"name": "Simulation"}
                        metadata["spectra_path"] = {"id": i, "spectra_id": j}
                        metadata["structure_id"] = structure_id
                        metadata["sample"] = {"name": file_data[i]["composition"]}

                        df = pd.DataFrame(
                            {
                                "energy": file_data[i]["spectra"][j]["energy"],
                                "mutrans": file_data[i]["spectra"][j]["mutrans"],
                            }
                        )

                        c_uid.write_dataframe(df, metadata, specs=["simulation"])
                        metadata_list.append(metadata)
                        df_list.append(df)
                        print(f"{file.name}: id: {i} - spectra_id: {j}")
                else:
                    print(f"None structure: {i} - {file_data[i]['structure_id']}")


def update_alignment_ncm_wanli(c):

    alignments = {
        "FVHEqkxTqz8": {"name": "BM_NCM622", "offset": -0.278},
        "SNH7Dg7PR9h": {"name": "BM_NCM712-Al", "offset": -0.278},
        "f6pVatZS3D9": {"name": "BM_NCMA", "offset": -0.278},
        "4FToXZNyQBr": {"name": "BM_NCM712", "offset": -1.135},
    }

    for key, value in alignments.items():
        spectra = c["dataset"]["nmc"]["element"]["Ni"]["edge"]["L3"]["sample"][key][
            "uid"
        ].search(Key("facility.name") == "ALS")
        for spectra_key, spectra_value in spectra.items():
            metadata = dict(spectra_value.metadata)
            if "energy_offset" not in metadata:
                # Remove auto-generated entries in metadata of old node.
                # Server will create new entries for new node
                metadata.pop("_tiled")
                metadata.pop("sample")

                metadata["energy_offset"] = value["offset"]

                df = spectra_value.read()
                df["energy"] = df["energy"] + value["offset"]

                if "tey" in df.columns:
                    specs = ["XAS_TEY", "XAS_TFY", "HasBatteryChargeData"]
                else:
                    specs = ["XAS", "HasBatteryChargeData"]

                old_uid = spectra_value.uid

                del c["uid"][spectra_key]

                c["uid"].write_dataframe(df, metadata, specs=specs)

                new_node = (
                    c["uid"].search(Key("fname") == metadata["fname"]).values().first()
                )

                print(f"Node updated from {old_uid} to {new_node.uid}")
            else:
                print(f"Skipped: alignment has been updated previously - {spectra_key}")


def ingest_aimm_ncm_gihyeok_811(c, sample_ids, data_path):
    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    for file in files:
        fname = file.name
        print(fname)

        try:
            sample_name, charge = parse_filename_nslsii(fname)
        except KeyError:
            print(f"failed to extract sample from {fname}")
            continue

        sample_id = sample_ids[sample_name]

        with open(file, "r") as f:
            df = read_wanli(f)

        metadata = {
            "dataset": "nmc",
            "fname": fname,
            "charge": charge,
            "facility": {"name": "ALS"},
            "beamline": {"name": "8.0.1"},
            "element": {"symbol": "Ni", "edge": "L3"},
            "sample_id": sample_id,
        }

        if charge["cycle"] == 1 or charge["cycle"] == 10:
            metadata["energy_offset"] = 0.176
        elif charge["cycle"] == 2:
            metadata["energy_offset"] = -0.055

        if "energy_offset" in metadata:
            df["energy"] = df["energy"] + metadata["energy_offset"]

        c_uid.write_dataframe(
            df, metadata, specs=["XAS_TEY", "XAS_TFY", "HasBatteryChargeData"]
        )

        new_node = c_uid.search(Key("fname") == fname).values().first()
        print(f"{new_node.uid}")


def validate_files(c, data_path):
    c_uid = c["uid"]
    files = list(data_path.glob("*.txt"))

    counter = 0
    for file in files:
        fname = file.name
        print(fname)

        results = c_uid.search(Key("fname") == fname)
        if len(results) != 0:
            counter += 1
            print(results.values().first().item["id"])
        else:
            print("Not found")
    print(f"total files found: {counter}")


if __name__ == "__main__":

    sample_ids = {
        "BM_NCM622": "FVHEqkxTqz8",
        "BM_NCM712-Al": "SNH7Dg7PR9h",
        "BM_NCMA": "f6pVatZS3D9",
        "BM_NCM811": "3pm2g7NXT4R",
        "BM_NCM712": "4FToXZNyQBr",
    }

    data_path = pathlib.Path(
        "C:/Users/jmaruland/Documents/GitHub/aimm-adapters/aimm_adapters/files"
    )
    # ingest_aimm_ncm_gihyeok_811(sample_ids, data_path)
    # file_path = data_path / "nmc_structures_12162022.json"

    # file_path = data_path / "HJ_NMC_XANES.json"
    # with open(file_path, "r") as f:
    #     file_xanes_xas_structure = json.load(f)

    # structure_result = ingest_aimm_structures(data_path)
    # ingest_aimm_ncm_feff(data_path)
