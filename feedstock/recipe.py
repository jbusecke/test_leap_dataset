
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, 
    OpenWithXarray, 
    StoreToZarr
    )

years = range(2001, 2022)
months = range(1, 13)
dataset_url = 'https://zenodo.org/record/7761881/files'
transform_dict = {}
## Monthly version


input_urls = [
    f'{dataset_url}/METAFLUX_GPP_RECO_monthly_{y}.nc' for y in years
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
input_urls = [
    f"{dataset_url}/METAFLUX_GPP_RECO_daily_{y}{m:02}.nc" for y in years for m in months
]
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