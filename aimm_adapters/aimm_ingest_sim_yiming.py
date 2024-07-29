import json

import pandas as pd
from uid import uid


def ingest_aimm_nmc_vasp(c, folder_path):
    """
    Reads a json file (nmc_vasp_xas.json) and write all the spectra to aimmdb (tiled server)

    Parameters
    ----------
    c : tiled.client.container.Container
        Tiled client container set on root.
    folder_path : pathlib.Path
        path of directory where the vasp file is being stored.

    Returns
    -------
    None.

    """

    filename = "nmc_vasp_xas.json"
    file_path = folder_path / filename

    if file_path.is_file():
        with open(file_path, "r") as f:
            file_data = json.load(f)

        if "nmc_sim_vasp" not in c:
            c.create_container(key="nmc_sim_vasp")
        vasp_c = c["nmc_sim_vasp"]
        for i in range(len(file_data["energy"])):
            key = str(i)
            metadata = {}
            metadata["dataset"] = "nmc_sim_vasp"
            metadata["fname"] = file_path.name
            metadata["facility"] = {"name": "CNM"}
            metadata["beamline"] = {"name": "Simulation"}
            metadata["sample"] = {
                "name": file_data["composition"][key]
            }  # changed from composition to sample.name to meet aimmdb format
            metadata["absorbing_atom_idx"] = file_data["absorbing_atom_idx"][
                key
            ]  # Last time, this was defined as element name/symbol.
            # Is an id enpough to save the spectra in aimmdb? # noqa: E116
            metadata["bond_length"] = file_data["bond_length"][key]
            metadata["oxidation_state"] = file_data["oxidation_state"][key]
            metadata["structure"] = file_data["structure"][key]

            df = pd.DataFrame(
                {
                    "energy": file_data["energy"][key],
                    "intensity": file_data["intensity"][key],
                }
            )  # Last time, dataframe was created used mutrans. The new version uses intensity
            vasp_c.write_dataframe(
                df, metadata=metadata, key=uid(), specs=["simulation"]
            )

            print(f"{file_path.name}: id: {i}")
