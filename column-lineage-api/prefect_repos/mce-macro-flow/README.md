# MCEMacroFlow

This replicates the process of fetching an MCE report, copying into `MCE Macro v2.3.xlsx` and copying the result.

## Flow

### Parameters

| Parameter     | Description                                 |
|---------------|---------------------------------------------|
| request_id    | Unique string provided from upstream caller |
| bucket_name   | Name of S3 Bucket with Uploaded File        |
| file_location | Location of Template file                   |
| env           | Prod or dev flag                            |

### Template Parameters

**mce_macro_v1.xlsx**

**upload_type** - `MCE_MACRO`


| Parameter     | Description                |
|---------------|----------------------------|
| Customer Name | Name of Customer, Optional |


## Development

See Notebook at http://172.18.138.27:8090/notebooks/flows/mce-macro/mce-macro-flow.ipynb

The notebook directory includes ``common`` as well as ``egrid.json`` 
