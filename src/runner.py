import asyncio
import os
from dotenv import load_dotenv
import clr
from System.Collections.Generic import List, KeyValuePair

clr.AddReference("csharp\RenTechLaunchConfig.dll")
from System.RenTechLaunchConfig import WebConfigurator

load_dotenv()


class Runner:
    def __init__(self, namespace, part_number, profile):
        self.namespace = namespace
        self.part_number = part_number
        self.profile = profile

        key = os.getenv("API_KEY")
        self.configurator = WebConfigurator(
            "RENAISSANCETECH_DEM",
            "RENAISSANCETECH_DEM",
            "https://configurator.inforcloudsuite.com/api/v3/ProductConfigurator.svc",
            key,
        )

    def _dict_to_kvp_list(py_dict):
        kvp_list = List[KeyValuePair[str, str]]()
        for key, value in py_dict.items():
            kvp = KeyValuePair[str, str](str(key), str(value))
            kvp_list.Add(kvp)
        return kvp_list

    async def send_to_config(self, variant):
        loop = asyncio.get_event_loop()
        summary = await loop.run_in_executor(
            self.configurator.RunBackgroundConfiguration(
                self.namespace,
                self.part_number,
                self.profile,
                self.dict_to_kvp_list(variant),
            )
        )

        output = {
            "result": summary.Result,
            "details": summary.SummaryDetails,
            "message": summary.Message,
            "variant_key": summary.VariantKey,
        }

        return output
