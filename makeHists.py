# library imports
from collections import defaultdict
import copy
import csv
import dask
from dask.distributed import Client
import awkward as ak
import datetime
import fsspec
import hist
from itertools import product
import json
import numpy as np
import os
import re
import time
import vector
import shutil
import glob
import yaml
from coffea.util import save

vector.register_awkward()
import zipfile

# coffea imports
from coffea.nanoevents import NanoAODSchema
from pfnanoschema import PFNanoSchema
from coffea.processor.accumulator import set_accumulator, defaultdict_accumulator, dict_accumulator
import coffea.processor as processor
from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)

NanoAODSchema.warn_missing_crossrefs = False

fsspec.config.conf["xrootd"] = {"timeout": 5}

####################################################################################################
# HELPER FUNCTIONS FOR PROCESSOR

def process_histograms(dataset_runnable, config, client):
    """Runs the Dask-based histogram processing."""
    tstart = time.time()

    if config["run_locally"]:
        executor = processor.IterativeExecutor(compression=None)
    else: 
        executor = processor.DaskExecutor(client=client, compression=None)
        
    run = processor.Runner(
        executor=executor, 
        chunksize=250_000,
        skipbadfiles=True,
        xrootdtimeout=5,
        schema=PFNanoSchema,
        #savemetrics=True,
    )

    #hist_result, report = run(
    hist_result = run(
        dataset_runnable, 
        processor_instance=MakeHists(
            config=config
        )
    )
    print(f"{time.time() - tstart:.1f}s to process")

    return hist_result  # [0]


def load_config(config_path="config.yaml"):
    """Loads YAML configuration and saves a timestamped copy."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if config["process_all_datasets"]:
        dataset_string = "All"
    else:
        dataset_string = "_".join(config["dataset_names"])

    return config, timestamp


def load_dataset(json_filename, process_all_datasets, dataset_names, n_files):
    """Loads JSON dataset and filters files based on n_files limit. Also returns modified list dataset_names"""
    with open(json_filename, "r") as f:
        dataset = json.load(f)

    if process_all_datasets:
        dataset_names = list(dataset.keys())

    dataset_dict = {}
    for dataset_name in dataset_names:
        if n_files == -1:
            dataset_dict[dataset_name] = {"files": dataset[dataset_name]["files"]}
        else:
            # Use dictionary slicing for efficiency
            dataset_dict[dataset_name] = {
                "files": dict(list(dataset[dataset_name]["files"].items())[:n_files])
            }

    return dataset_dict, dataset_names


def save_histogram(hist_result, process_all_datasets, dataset_names, timestamp, label=None):
    """Saves the histogram to a pickle file."""
    if process_all_datasets:
        dataset_string = "All"
    else:
        dataset_string = "_".join(dataset_names)
    if label is not None: 
        filename = f"results/hist_result_{dataset_string}_{timestamp}_{label}.pkl"
    else:
        filename = f"results/hist_result_{dataset_string}_{timestamp}.pkl"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    save(hist_result, filename)
    print(f"Histogram saved as {filename}")


# ###################################################################################################
# # DEFINE COFFEA PROCESSOR
class MakeHists(processor.ProcessorABC):
    def __init__(
        self, config=None
    ):
        self.config = config

        # Define axes for histograms # TODO: maybe move this into a dictionary elsewhere
        # String based axes
        self.dataset_axis = hist.axis.StrCategory(
            [], growth=True, name="dataset", label="Primary dataset"
        )
        self.mult_axis = hist.axis.Regular(201, -0.5, 200.5, name="mult", label=r"$N_{obj}$")
        self.pt_axis = hist.axis.Regular(1000, 0, 5000, name="pt", label=r"$p_{T}$ [GeV]")
        self.eta_axis = hist.axis.Regular(500, -5, 5, name="eta", label=r"$\eta$")
        self.phi_axis = hist.axis.Regular(400, -4, 4, name="phi", label=r"$\phi$")
        self.met_axis = hist.axis.Regular(100, 0, 1000, name="met", label=r"$p^{miss}_{T} [GeV]$")
        self.ht_axis = hist.axis.Regular(200, 0, 4000, name="ht", label=r"$H_{T}$ [GeV]")
        self.mass_axis = hist.axis.Regular(12000, 0, 4000, name="mass", label=r"$m_{obj_{1},obj_{2}}$ [GeV]")
        self.deltaR_axis = hist.axis.Regular(300, 0, 6.5, name="deltaR", label=r"$\Delta R$ between $obj_1$ and $obj_2$")
        self.deta_axis = hist.axis.Regular(500, -5, 5, name="deta", label=r"delta eta")
        self.dphi_axis = hist.axis.Regular(600, -np.pi*2, np.pi*2, name="dphi", label=r"delta phi")        
        self.neHEF_axis  = hist.axis.Regular(100, 0, 1, name="neHEF", label=r"Neutral Hadron Energy Fraction")
        self.neEmEF_axis = hist.axis.Regular(100, 0, 1, name="neEmEF", label=r"Neutral EM Energy Fraction")
        self.muEF_axis   = hist.axis.Regular(100, 0, 1, name="muEF",   label=r"Muon Energy Fraction")
        self.chEmEF_axis = hist.axis.Regular(100, 0, 1, name="chEmEF", label=r"Charged EM Energy Fraction")
        self.chHEF_axis  = hist.axis.Regular(100, 0, 1, name="chHEF",  label=r"Charged Hadron Energy Fraction")
        self.nConstituents_axis = hist.axis.Regular(201, -0.5, 200.5, name="nConstituents", label=r"$N_{\mathrm{constituents}}$")


    def _create_hist_1d(
        self, hist_dict, dataset_axis, observable_axis, hist_name
    ):
        """Creates a 1D histogram and adds it to the provided histogram dictionary."""
    
        h = hist.Hist(
            dataset_axis, observable_axis, storage="weight", label="nEvents"
        )
    
        hist_dict[f"{hist_name}"] = h
    
        return hist_dict

    
    def _initialize_1d_hist_dict(self, hist_dict, axis_map):
        """Initializes histogram objects for all 1d histograms specified in config and stores them in hist_dict"""
    
        config = self.config
    
        for histogram_group, histograms in (
            config["histograms_1d"].items() if config["histograms_1d"] else []
        ): 
            for histogram in histograms if histograms else []:
    
                # iterate through reconstruction levels
                for reconstruction_level in (
                    config["objects"] if config["objects"] else []
                ):  # Loop over different object reconstruction levels
                    if histogram_group == "per_event":
                        hist_name = f"{reconstruction_level}_{histogram}"
                        hist_dict = self._create_hist_1d(
                            hist_dict, self.dataset_axis,
                            axis_map[histogram], hist_name=hist_name
                        )
                        
                    elif histogram_group == "per_object_type":
                        for obj in config["objects"][reconstruction_level]:
                            hist_name = f"{obj}_{histogram}"
                            hist_dict = self._create_hist_1d(
                                hist_dict, self.dataset_axis,
                                axis_map[histogram], hist_name=hist_name
                            )
                            
                    elif histogram_group == "per_object":
                        for obj in config["objects"][reconstruction_level]:
                            for i in range(config["objects_max_i"][obj]):
                                hist_name = f"{obj}_{i}_{histogram}"
                                hist_dict = self._create_hist_1d(
                                    hist_dict, self.dataset_axis,
                                    axis_map[histogram], hist_name=hist_name
                                )
    
        return hist_dict
    
    def initialize_hist_dict(self, hist_dict):
        """Initializes histogram objects for all histograms specified in config and stores them in hist_dict"""
        axis_map = {
            "ht": self.ht_axis,
            "met": self.met_axis,
            "mult": self.mult_axis,
            "pt": self.pt_axis,
            "eta": self.eta_axis,
            "phi": self.phi_axis,
            "mass": self.mass_axis,
            "deltaR": self.deltaR_axis,
            "dphi": self.dphi_axis,  
            "deta": self.deta_axis,  
            "neHEF": self.neHEF_axis,
            "neEmEF": self.neEmEF_axis,
            "muEF": self.muEF_axis,
            "chEmEF": self.chEmEF_axis,
            "chHEF": self.chHEF_axis,
            "nConstituents": self.nConstituents_axis,
        }
    
        # shortcut for config
        config = self.config
    
        # initialize 1d histograms
        hist_dict = self._initialize_1d_hist_dict(hist_dict, axis_map)
    
        return hist_dict

    def get_required_observables(self):
        """
        Combs through the observables needed to fill the requested histograms in self.config and returns 
        a dictionary that lists which observables need to be calculated."""
    
        required_observables = {
            "per_event": set(),
            "per_object_type": set(),
            "per_object": set(),
        }
    
        # get 1d histogram observables
        for category, hist_list in (
            self.config.get("histograms_1d", {}).items() if self.config["histograms_1d"] else []
        ):
            if hist_list:
                required_observables[category].update(hist_list)
    
        # hold on to mult for unflattening arrays
        required_observables["per_object_type"].add("mult")
    
        return required_observables

    def _get_per_event_hist_values(self, reconstruction_level, histogram, events_trig):
        level_map = {
            "reco": {
                "ht": lambda e: ak.sum(e.Jet.pt, axis=1),
                "met": lambda e: e.MET.pt,
                "mult": lambda e: (
                    ak.num(e.Jet)
                    + ak.num(e.Electron)
                    + ak.num(e.Photon)
                    + ak.num(e.Muon)
                ),
                "pt": lambda e: (
                    ak.sum(e.Jet.pt, axis=1)
                    + ak.sum(e.Electron.pt, axis=1)
                    + ak.sum(e.Photon.pt, axis=1)
                    + ak.sum(e.Muon.pt, axis=1)
                ),
            },
        }
    
        hist_func = level_map.get(reconstruction_level, {}).get(histogram)
        return hist_func(events_trig) if hist_func else None

    def _calculate_per_event_observables(self, observables, events):
    
        # initialize per-event observable dictionary
        per_event_dict = {}                
        # get every other observable
        for observable in observables:
            for reconstruction_level in self.config["objects"] if self.config["objects"] else []:
                if reconstruction_level not in per_event_dict.keys():
                    per_event_dict[reconstruction_level] = {}
                per_event_dict[reconstruction_level][observable] = (
                    self._get_per_event_hist_values(reconstruction_level, observable, events)
                )
        return per_event_dict

    def _get_per_object_type_hist_values(self, objects, histogram):
        """
        Retrieve histogram values based on reconstruction level and histogram type. 
        Uses a dictionary lookup with lambda functions to avoid unnecessary computations.
        """
        
        level_map = {
            "ht": lambda objects: ak.sum(objects.pt, axis=1),
            "mult": lambda objects: ak.num(objects),
            "pt": lambda objects: ak.flatten(objects.pt),
            "eta": lambda objects: ak.flatten(objects.eta),
            "phi": lambda objects: ak.flatten(objects.phi),
            "neHEF": lambda objects: ak.flatten(objects.neHEF),
            "neEmEF": lambda objects: ak.flatten(objects.neEmEF),
            "muEF": lambda objects: ak.flatten(objects.muEF),
            "chEmEF": lambda objects: ak.flatten(objects.chEmEF),
            "chHEF": lambda objects: ak.flatten(objects.chHEF),
            "nConstituents": lambda objects: ak.flatten(objects.nConstituents),
        }
        return level_map.get(histogram)(objects)

    def _get_per_object_hist_values(self, objects, i, histogram):
        """
        Retrieve histogram values based on reconstruction level and histogram type. 
        Uses a dictionary lookup with lambda functions to avoid unnecessary computations.
        """
        
        level_map = {
            "pt": lambda objects: ak.flatten(objects.pt[:, i : i + 1]),
            "eta": lambda objects: ak.flatten(objects.eta[:, i : i + 1]),
            "phi": lambda objects: ak.flatten(objects.phi[:, i : i + 1]),
            "counts": lambda objects: ak.num(
                objects.pt[:, i : i + 1], axis=1
            ),  # counts is used to broadcast per-event observables while making 2d arrays
        }
        return level_map.get(histogram)(objects)

    def _clean_objects(self, objects, cuts, reconstruction_level=None, update_cutflow=False, object_name=None, min_pass=1):
        """Apply upper and lower-bound cuts on different object variables based on reconstruction level."""

        # figure out list of branches (common across regions)
        all_branches = [set(region_cuts.keys()) for region_cuts in cuts.values()]
        first = next(iter(all_branches))
        for i, brs in enumerate(all_branches[1:], start=1):
            if brs != first:
                raise ValueError(
                    f"Inconsistent cut branches between regions: "
                    f"expected {first}, got {brs} in region index {i}"
                )

        common_branches = list(next(iter(cuts.values())).keys())
    
        bx_mask = ak.ones_like(objects.pt, dtype=bool)
        
        # initialize container for masks in different regions
        masks_cumulative = []
        for i, (region, region_cuts) in enumerate(cuts.items()):
            masks_cumulative.append(bx_mask) # start with bx mask for each object

        # iterate through and apply cuts
        for br in common_branches:

            masks_separate = []
            for i, (region, region_cuts) in enumerate(cuts.items()):
                cut = region_cuts[br]

                # get values
                if cut and hasattr(objects, br):
                    values = getattr(objects, br)

                # get mask for cuts
                if isinstance(cut[0], list) or isinstance(cut[0], tuple):
                    submask = ak.zeros_like(values, dtype=bool)
                    for lower, upper in cut:
                        lower_cut = lower if lower is not None else float("-inf")
                        upper_cut = upper if upper is not None else float("inf")
                        submask = submask | ((values >= lower_cut) & (values <= upper_cut))
                else:
                    lower_cut = cut[0] if cut[0] is not None else float("-inf")
                    upper_cut = cut[1] if cut[1] is not None else float("inf")
                    submask = (values >= lower_cut) & (values <= upper_cut)

                masks_cumulative[i] = masks_cumulative[i] & submask
                masks_separate.append(submask)

        # apply cuts per region
        cleaned_objects_per_region = []
        for i, mask in enumerate(masks_cumulative):
            # apply cumulative mask for this region
            objs = objects[mask]
        
            # attach the region index
            region_arr = ak.full_like(objs.pt, i, dtype=np.uint8)
            objs = ak.with_field(objs, region_arr, "region")
        
            cleaned_objects_per_region.append(objs)
        
        cleaned_objects = ak.concatenate(cleaned_objects_per_region, axis=1) if cleaned_objects_per_region else objects[:0]
    
        # make sure objects are re-sorted by pT after concatenation if there is more than one region
        if len(cuts.items())>1:
            sorted_indices = ak.argsort(cleaned_objects.pt, ascending=False)
            cleaned_objects = cleaned_objects[sorted_indices]
    
        return cleaned_objects

    def _calculate_object_observables(self, observables, events, object_cuts):
    
        # initialize dicts
        per_object_type_dict = {}
        per_object_dict = {}
    
        # calculate per-object-type and per-object observables
        for reconstruction_level, object_types in (
            self.config["objects"].items() if self.config["objects"] else []
        ):
            per_object_type_dict.setdefault(reconstruction_level, {})
            per_object_dict.setdefault(reconstruction_level, {})
    
            for object_type in object_types or []:
                per_object_type_dict[reconstruction_level].setdefault(object_type, {})
                per_object_dict[reconstruction_level].setdefault(object_type, {})
    
                # Get raw objects
                objects_uncleaned = getattr(events, object_type)
    
                # -------------------------
                # Per-object-type observables
                # -------------------------
    
                # get per-object cleaned objects if we need them
                if ( any(obs not in {"ht", "mult"} for obs in per_object_type_dict)):
                    
                    # clean objects for per_object_type (non-ht and non-mult is always per-object cleaning)
                    print("object type = ", object_type)
                    if object_type in object_cuts:
                        objects = self._clean_objects(
                            objects_uncleaned, object_cuts[object_type], reconstruction_level
                        )
                    else:
                        objects = objects_uncleaned
                        
                    # always keep mult so what we can do broadcasting
                    per_object_type_dict[reconstruction_level][object_type]["mult-for-broadcast"] = (
                        self._get_per_object_type_hist_values(objects, "mult")
                    )
                    
                # fill histograms for non-ht and non-mult per-object-type variables
                for observable in observables["per_object_type"]:
    
                    per_object_type_dict[reconstruction_level][object_type][observable] = self._get_per_object_type_hist_values(
                        objects, observable
                    )
                        
                # -------------------------
                # Per-object observables (loop over indices)
                # -------------------------
                for i in range(self.config["objects_max_i"][object_type]):
    
                    # fill in observable values
                    for observable in observables["per_object"]:
                        per_object_dict[reconstruction_level][object_type][f"{observable}_{i}"] = self._get_per_object_hist_values(
                            objects,
                            i,
                            observable,
                        )
    
        return per_object_type_dict, per_object_dict

    def calculate_observables(self, observables, events, object_cuts):
        """
        Uses the dictionary observables to calculate each listed observable from events and stores 
        the results in observable_dict, which is returned.
        """
    
        observable_dict = {
            "per_event": self._calculate_per_event_observables(
                observables["per_event"], 
                events, 
            ),
        }
    
        per_object_type_dict, per_object_dict = self._calculate_object_observables(
            observables, 
            events, 
            object_cuts, 
        )
        observable_dict["per_object_type"] = per_object_type_dict
        observable_dict["per_object"] = per_object_dict
        
        return observable_dict

    def _fill_hist_1d(
        self,
        hist_dict,
        hist_name,
        dataset,
        observable,
        observable_name,
        object_name=None,
    ):
        """Fills a 1D histogram and adds it to the provided histogram dictionary."""
    
        kwargs = {observable_name: observable, "dataset": dataset}
    
        if object_name is not None:
            kwargs["object"] = object_name
    
        hist_dict[f"{hist_name}"].fill(**kwargs)
    
        return hist_dict

    def _fill_per_event_hist(self, hist_dict, histogram, observable_calculations, dataset):
        """Fills specified 1d per-event histogram object with values from observable_calculations"""
        config = self.config

        # iterate through reconstruction levels
        for reconstruction_level in (
            config["objects"] if config["objects"] else []
        ):
            print("Reconstruction level: ", reconstruction_level)
            self._fill_hist_1d(
                hist_dict,
                reconstruction_level + "_" + histogram,
                dataset,
                observable_calculations["per_event"][reconstruction_level][histogram],
                histogram,
            )

        return hist_dict

    def _fill_per_object_type_hist(
        self, hist_dict, histogram, observable_calculations, dataset, object_type, reconstruction_level
    ):
        config = self.config
                
        self._fill_hist_1d(
            hist_dict,
            object_type + "_" + histogram,
            dataset,
            observable_calculations["per_object_type"][reconstruction_level][
                object_type
            ][histogram],
            histogram,
        )

        return hist_dict

    def _fill_per_object_hist(
        self, hist_dict, histogram, observable_calculations, dataset, trigger_path, object_type, reconstruction_level
    ):
        config = self.config

        for i in range(config["objects_max_i"][object_type]):
                             
            self._fill_hist_1d(
                hist_dict,
                object_type + "_" + str(i) + "_" + histogram,
                dataset,
                observable_calculations["per_object"][reconstruction_level][
                    object_type
                ][f"{histogram}_{i}"],
                histogram,
            )

        return hist_dict


    def fill_hist_dict(self, hist_dict, observable_calculations, dataset):
        config = self.config

        # fill 1d histograms
        for histogram_group, histograms in (
            config["histograms_1d"].items() if config["histograms_1d"] else []
        ):
            print("Histogram group: ", histogram_group)

            # fill event level histograms
            if histogram_group == "per_event":  
                for histogram in histograms if histograms else []:
                    print("Histogram type: ", histogram)
                    hist_dict = self._fill_per_event_hist(
                        hist_dict, histogram, observable_calculations, dataset
                    )

            # fill object level histograms
            if histogram_group == "per_object_type" or histogram_group == "per_object":
                for reconstruction_level, object_types in (
                    config["objects"].items() if config["objects"] else []
                ):
                    for object_type in object_types:
                        print("Object type:", object_type)
                        for histogram in histograms:
                            if histogram_group == "per_object_type":
                                print("Histogram type: ", histogram)
                                hist_dict = self._fill_per_object_type_hist(
                                    hist_dict, histogram, observable_calculations, 
                                    dataset, object_type, reconstruction_level
                                )
                            
                            if histogram_group == "per_object":
                                print("Histogram type: ", histogram)
                                hist_dict = self._fill_per_object_hist(
                                    hist_dict, histogram, observable_calculations, 
                                    dataset, object_type, reconstruction_level
                                )
                        
        return hist_dict

    def process(self, events):
        dataset = events.metadata["dataset"]
        self._current_dataset = dataset

        hist_dict = {}

        # Check that the objects you want to run on match the available fields in the data
        for object_type in self.config["objects"] if self.config["objects"] else []:
            for my_object in (
                self.config["objects"][object_type] if self.config["objects"][object_type] else []
            ):
                assert my_object in events.fields, (
                    f"Error: {my_object} not in available fields: {events.fields}"
                )

        hist_dict = self.initialize_hist_dict(hist_dict)
        required_observables = self.get_required_observables()
        observable_calculations = self.calculate_observables(required_observables, events, self.config["object_cleaning"])
        hist_dict = self.fill_hist_dict(hist_dict, observable_calculations, dataset)

        return_dict = {
            "hists": hist_dict,
        }

        return return_dict

    def postprocess(self, accumulator):
        return accumulator


###################################################################################################
# DEFINE MAIN FUNCTION


def main():
    """Main script execution."""

    config, timestamp = load_config()
    
    client = None 
    if not config["run_locally"]:  # set up dask
        client = Client("tls://localhost:8786")

    dataset_skimmed, dataset_names = load_dataset(
        config["json_filename"],
        config["process_all_datasets"],
        config["dataset_names"],
        config["n_files"],
    )
    config["dataset_names"] = dataset_names

    total_n_files = sum(
        [len(dataset_skimmed[dataset_name]["files"]) for dataset_name in dataset_names]
    )
    print(f"Processing {total_n_files} files")

    hist_result = process_histograms(dataset_skimmed, config, client)
    print(hist_result)

    if config["save_hists"]:
        save_histogram(
            hist_result, config["process_all_datasets"], config["dataset_names"], timestamp
        )

        config_path="config.yaml"
        if config["process_all_datasets"]: dataset_string = "All"
        else: dataset_string = "_".join(config["dataset_names"])
        backup_path = f"results/config_{dataset_string}_{timestamp}.yaml"
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        shutil.copy(config_path, backup_path)

    print("Finished")


###################################################################################################
# RUN SCRIPT
if __name__ == "__main__":
    main()
