### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
    )

input_urls = [
    'https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_daily_200101.nc?download=1',
    'https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_daily_200102.nc?download=1',
]

pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(cache='temp/cache')
    | OpenWithXarray()
    | StoreToZarr(
        store_name="test_leap_dataset.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_root=target_root,
    )
)