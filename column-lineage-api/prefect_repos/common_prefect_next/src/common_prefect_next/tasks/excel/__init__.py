try:
    import pandas as pd
    import openpyxl
    import xlsxwriter
    from .excel import write_to_excel, write_to_excel_workbook
except ImportError:
    warning = (
        "The 'pandas', 'openpyxl', and 'xlsxwriter' libraries are required for this module. "
        "Please install them selecting installing the 'excel' extra: "
        "`uv add common-prefect-next[excel]`."
    )
    import warnings

    warnings.warn(warning, stacklevel=2)
    raise ImportError(warning) from None
