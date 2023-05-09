### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
    )
transform_dict = {}

# for testing
# years = range(2001, 2003)
years = range(2001, 2021)

#trying to avoid time out errors
# see https://stackoverflow.com/a/73746020
# from aiohttp import ClientTimeout
# open_kwargs = {"client_kwargs":{"timeout": ClientTimeout(total=5000, connect=1000)}}

## Monthly version

input_urls = [
    f'https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_monthly_{year}.nc?download=1' for year in years
]

pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()#open_kwargs=open_kwargs
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_monthly.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)
transform_dict['METAFLUX_GPP_RECO_monthly'] = transforms


## daily version
input_urls = [f"https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_daily_{year}{month:02}.nc?download=1" for year in years for month in range(1,12) ]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()#open_kwargs=open_kwargs
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_daily.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)
transform_dict['METAFLUX_GPP_RECO_daily'] = transforms

