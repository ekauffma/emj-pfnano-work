from coffea.nanoevents.methods import base, vector
from coffea.nanoevents.schemas.base import BaseSchema, zip_forms, nest_jagged_forms


class PFNanoSchema(BaseSchema):
    
    def __init__(self, base_form):
        super().__init__(base_form)
        old_style_form = {
            k: v for k, v in zip(self._form["fields"], self._form["contents"])
        }
        output = self._build_collection(old_style_form)
        self._form["fields"] = [k for k in output.keys()]
        self._form["contents"] = [v for v in output.values()]
        
    def _build_collection(self, form):
        collection_layout = [
            {"name": "Jet", "type": "PtEtaPhiMLorentzVector"},
            {"name": "JetCHS", "type": "PtEtaPhiMLorentzVector"},
            {"name": "SubJet", "type": "PtEtaPhiMLorentzVector"},
            {"name": "FatJet", "type": "PtEtaPhiMLorentzVector"},
            {"name": "JetPFCands", "type": "PtEtaPhiMLorentzVector"},
            {"name": "JetCHSPFCands", "type": "PtEtaPhiMLorentzVector"},
            {"name": "PFCands", "type": "PtEtaPhiMLorentzVector"},
            {"name": "GenJet", "type": "PtEtaPhiMLorentzVector"},
            {"name": "GenJetCands", "type": "PtEtaPhiMLorentzVector"},
            {"name": "GenPart", "type": "PtEtaPhiMLorentzVector"},
        ]
        type_components = {
            "ThreeVector": {"x": "x", "y": "y", "z": "z"},
            "PtEtaPhiMLorentzVector": {
                "Pt": "pt",
                "Eta": "eta",
                "Phi": "phi",
                "M": "mass",
            },
            "LorentzVector": {"Px": "x", "Py": "y", "Pz": "z", "E": "t"},
        }

        def create_collection(collection_dict, prefix=""):
            # Recursively creating the nested elements first
            if "typed_fields" in collection_dict:
                for field_dict in collection_dict["typed_fields"]:
                    create_collection(field_dict)
            if "nested" in collection_dict:
                for nested_collection in collection_dict["nested"].values():
                    create_collection(nested_collection, prefix="__nested")

            # Reducing the collections to matching items with matching prefixes
            name = collection_dict["name"]
            key_list = [x for x in form.keys() if x.startswith(name + "_")]

            def reduce_key(k):
                k = k.replace(name + "_", "")
                if "type" not in collection_dict:
                    return k
                else:
                    return type_components[collection_dict["type"]].get(k, k)

            if "type" in collection_dict:
                form[prefix + name] = zip_forms(
                    {reduce_key(k): form.pop(k) for k in key_list},
                    prefix + name,
                    collection_dict["type"],
                )
            else:
                form[prefix + name] = form.pop(name)

            # Creating the nested jagged forms
            if "nested" in collection_dict:
                for fold_def, fold_entry in collection_dict["nested"].items():
                    nest_jagged_forms(
                        form[name],
                        form.pop("__nested" + fold_entry["name"]),
                        fold_def,
                        fold_entry["name"].replace(name + "_", ""),
                    )

        for collection in collection_layout:
            create_collection(collection)
            
        return form


    @classmethod
    def behavior(cls):
        behavior = {}
        behavior.update(base.behavior)
        behavior.update(vector.behavior)
        return behavior
