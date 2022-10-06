from __future__ import annotations
import json
from typing import Any


class Query:
    def __init__(
        self,
        model: str,
        view: str,
        fields: list[str],
        pivots: list[str] = None,
        filters: dict[str:str] = None,
        properties: dict[str:str] = None,
    ) -> None:
        self.model = model
        self.view = view
        self.fields = fields
        self.pivots = pivots
        self.filters = filters
        self.properties = properties

    @classmethod
    def from_dict(cls, body: dict[str:Any]) -> "Query":
        cls.model = body.get("model")
        cls.view = body.get("view")
        cls.fields = body.get("fields")
        cls.pivots = body.get("pivots")
        cls.filters = body.get("filters")
        cls.properties = body
        return cls

    @classmethod
    def to_json(cls):
        data = cls.properties
        data["model"] = cls.model
        data["view"] = cls.view
        data["fields"] = cls.fields
        data["pivots"] = cls.pivots
        data["filters"] = cls.filters
        json_data = json.dumps(data, indent=4)
        return json_data


class Job:
    def __init__(self) -> None:
        self.name = None
        self.depends_on = []
        self._dependent_data = {}
        self.required_detail = []

    def is_valid(self, plan: dict[str:str]) -> tuple[bool, str]:
        valid = True
        missing = []
        error = None
        for _, key in enumerate(self.required_detail):
            if key not in plan:
                valid = False
                missing.append(key)

        if not valid:
            error = ", ".join(missing) + " not found in plan"
        return valid, error

    def get_dependents(self, store: dict[str:Any]):
        missing = []
        error = False
        for job_name in self.depends_on:
            if job_name in store:
                self._dependent_data[job_name] = store[job_name]
            else:
                error = True
                missing.append(job_name)
                # couldn't get dependent job_name data
        if error:
            error = f"{self.name} depends on these missing jobs" + ",".join(missing)

    def run_from_plan(self, plan: dict[str:str]) -> tuple[bool, Any]:
        # this is the default behavior of the abstract implementation
        return False, plan
