
# Data Extraction Process

This script reads extraction information from a JSON file, initializes a `DataExtractor` object, and executes the extraction process.
It will produce smaller version of the full database extraction files. It needs a sample file that will provide guidance for the
important keys/codes to be included in the smaller file versions.

The user can adjust the extraction process from the setup of the JSON file, which will instuct the sequence of the
extraction process.

## Install Dependencies:

   No dependancies need to be installed apart from the built-in python module.

## Description
The script performs the following steps:

1. Reads extraction information from a JSON file (extraction_info.json).
2. Initializes a DataExtractor object with the provided information.
3. Executes the extraction process based on the sequence specified in the JSON file.

## Requirements
* Python >= 3.7

## JSON File Structure
Ensure that the extraction_info.json file follows the required structure:
```
{
  "main": {
    "sample_filename": "SAMPLE.CSV",
    "main_key_column": "\"CUSTOMER_CODE\""
  },
  "extraction_info": [
    {
      "input": "./indata_files/CUSTOMER.CSV",
      "output": "./outdata_files/OUT_CUSTOMER.CSV",
      "key_column": "\"CUSTOMER_CODE\""
    },
    {
      "input": "./indata_files/INVOICE.CSV",
      "output": "./outdata_files/OUT_INVOICE.CSV",
      "key_column": "\"CUSTOMER_CODE\"",
      "relevant_keys": ["\"INVOICE_CODE\""]
    },
    {
      "input": "./indata_files/INVOICE_ITEM.CSV",
      "output": "./outdata_files/OUT_INVOICE_ITEM.CSV",
      "key_column": "\"INVOICE_CODE\""
    }
  ]
}
```
Make sure to update the JSON file with your specific data extraction configurations.

