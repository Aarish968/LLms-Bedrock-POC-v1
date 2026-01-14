# common_serial_resolution

This repository contains the code necessary to resolve a Cisco Serial Number to a Cisco Instance Id

The actual implementation and logic is contained in the stored procedure `serial_resolution_v2.sql`

This repository handles loading, calling, fetching and interpreting the results of the stored procedure.

Regardless of whether this is the end goal or not, each time we resolve serial numbers, we need to run several side effects
and provide an audit Excel spreadsheet.

## Usage

```python
from common_serial_resolution import run_serial_resolution
from tempfile import NamedTemporaryFile
from pathlib import Path


with NamedTemporaryFile(delete_on_close=False, delete=False) as temp_file_obj:
    tmp_file = Path(temp_file_obj.name)

audit_data = run_serial_resolution(
    get_engine=settings.get_engine, serial_numbers=['FDO12345678', 'FDO87654321'], request_id=123, dc_engagement_id=456,
    comment="Triggered by the API", requestor="user@cisco.com", excel_file_path=tmp_file, s3_client=, env=env
    )
```
