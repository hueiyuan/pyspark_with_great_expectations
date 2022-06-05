from dataclasses import dataclass


@dataclass
class DataAssetName:
    """
      Define validation data asset name
    """
    table_name: str
    dt: str

    def __repr__(self):
        return f"{self.table_name}_{self.dt}"
