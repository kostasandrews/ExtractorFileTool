import json
from typing import List, Dict, Union, Callable, Optional


class SimpleDictReader:
    def __init__(self, file: str, fieldnames: List[str], delimiter: str = ',', quotechar: str = '"') -> None:
        """
        Initializes a SimpleDictReader instance.

        :param file: (str) The file-like object to read from.
        :param fieldnames: (List[str]) List of field names for the CSV header.
        :param delimiter: (str) The character used to separate fields (default is ',').
        :param quotechar: (str) The character used for quoting fields (default is '"').
        :return: None
        """
        self.file = file
        self.fieldnames = fieldnames
        self.delimiter = delimiter
        self.quotechar = quotechar

    def _parse_row(self, row: str) -> Dict[str, str]:
        """
        Parses a row from the CSV file into a dictionary.

        :param row: (str) The string representing a row of data.
        :return: Dict[str, str] A dictionary representing the parsed row.
        """

        # Split the row based on the delimiter
        row = row.rstrip('\n')
        values = row.split(self.delimiter)

        # Create a dictionary by pairing fieldnames with corresponding values
        return dict(zip(self.fieldnames, values))

    def __iter__(self) -> 'SimpleDictReader':
        return self

    def __next__(self) -> Optional[Dict[str, str]]:
        """
       Reads the next row from the CSV file and returns it as a dictionary.

       :return: Optional[Dict[str, str]]: A dictionary representing the next row of data, or None if the end of the
                file is reached.
       """

        # Read a line from the file
        line = next(self.file, None)

        # If the line is None, we've reached the end of the file
        if line is None:
            raise StopIteration

        # Parse the line into a dictionary
        row_dict = self._parse_row(line)

        return row_dict


class SimpleDictWriter:
    def __init__(self, file: str, fieldnames: List[str], delimiter: str = ',', quotechar: str = '"') -> None:
        """
        Initializes a SimpleDictWriter instance.

        :param file : (str) The file-like object to write to.
        :param fieldnames : (List[str]) List of field names for the CSV header.
        :param delimiter : (str) The character used to separate fields (default is ',').
        :param quotechar : (str) The character used for quoting fields (default is '"').
        :return: None

        """
        self.file = file
        self.fieldnames = fieldnames
        self.delimiter = delimiter
        self.quotechar = quotechar

    def _format_row(self, row: dict) -> str:
        """
        Formats a row for writing to the CSV file.

        :param row : (dict) The dictionary representing a row of data.
        :return: (str) The formatted row as a string.
        """
        # Format each value, adding quotes if needed
        formatted_values = [f'{self.quotechar}{row[field]}{self.quotechar}' if '"' not in row[field] else f'{row[field]}' for field in self.fieldnames]

        # Join the formatted values with the delimiter
        return self.delimiter.join(formatted_values)

    def writerow(self, row: dict) -> None:
        """
        Writes a row to the CSV file.

        :param row : (dict) The dictionary representing a row of data.
        :return: None
        """

        # Format the row and write it to the file
        formatted_row = self._format_row(row)
        self.file.write(formatted_row + '\n')


class DataExtractor:

    def __init__(
            self,
            sample_file: str,
            key_column: str,
            preprocess_files: bool = False,
            files: Union[List[str], None] = None
    ) -> None:
        """
        Initializes a DataExtractor instance. This the main tool for creating the extraction process.

        :param sample_file: (str) The file that has the preselected codes of interest.
        :param key_column: (str) The column name that will be used for matching.
        :param preprocess_files: (bool) A boolean flag for preprocessing the files. Formatting the data.
        :param files: A list of files to be preprocessed.
        :return: None
        """
        self.sample_file = sample_file

        # Preprocess the data so they adhere to the double quote guidelines.
        if preprocess_files and isinstance(files, list):
            self.preprocess_data(files)

        # Get Codes of interest.
        self.codes = self.read_codes(key_column)
        self.relevant_keys = {}

    def read_codes(self, key_column: str, file: Union[str, None] = None) -> List[str]:
        """
        Reads customer codes from the specified CSV file or the default sample file.

        :param key_column : (str) The column containing the customer codes.
        :param file : (Union[str, None]) The CSV file path. If None, uses the default sample file.

        :return: List[str] A list of customer codes.
        """

        if file is None:
            file = self.sample_file

        with open(self.sample_file, 'r', encoding='utf-8') as file:
            header = file.readline().strip().split(',')
            reader = SimpleDictReader(file, header)
            customers = [row[key_column] for row in reader]
            return customers

    def extract_data(
            self, input_file: str, output_file: str, key_column: str, record_relevant_keys: Union[list, None] = None
    ) -> Union[Dict[str, List[str]], None]:
        """
        Extracts data from the input file based on the specified key column and writes it to the output file.

        :param input_file : (str) The path to the input CSV file.
        :param output_file : (str) The path to the output CSV file.
        :param key_column : (str) The column used as the key for extraction.
        :param record_relevant_keys : (Union[List[str], None]) List of keys to record in relevant_keys. If None, no recording.

        Returns:
            Union[Dict[str, List[str]], None]: A dictionary containing relevant keys and their values, or None if no relevant keys were recorded.
        """

        # Processing both reading and writing operations at the same time.
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            # Get headers
            header = infile.readline().strip().split(',')

            # Reader object
            reader = SimpleDictReader(infile, header)

            # Writer Object
            writer = SimpleDictWriter(outfile, header)

            # Manually format the header with double quotes
            writer.writerow({key: key for key in writer.fieldnames})

            # Initialize key value pairs for relevant_keys
            if record_relevant_keys:
                self.relevant_keys = {key: [] for key in record_relevant_keys}

            for row in reader:

                code = row[key_column]

                # Matching the keys of interest.
                if code in self.codes:

                    # Record any relevant keys.
                    if record_relevant_keys:
                        for key in record_relevant_keys:
                            if key in row.keys():
                                self.relevant_keys[key].append(row[key])
                    writer.writerow(row)

            return self.relevant_keys if self.relevant_keys else None

    @staticmethod
    def preprocess_data(files: List[str]) -> None:
        """
        This is a static method it preprocesses the specified files by replacing various quote characters with
        double quotes.

        :param files: (List[str]) A list of file paths to be processed.
        :return: None
        """

        for file in files:
            with open(file, 'r+', encoding='utf-8') as f:
                # Read all lines into a list
                lines = f.readlines()

                # Replace all other type of quotes with double quotes for each line.

                quote_mapping = {
                    "'": '"',
                    '”': '"',
                    '\u201C': '"',
                    '\u201E': '"',
                    '\u201D': '"',
                    '\u0022': '"'
                }

                modified_lines = [line.translate(str.maketrans(quote_mapping)) for line in lines]

                # Move the file cursor to the beginning and truncate the file
                f.seek(0)
                f.truncate()

                # Write to the file
                f.writelines(modified_lines)


def process_extraction_info(file: str) -> None:
    """
        Reads extraction information from a JSON file, initializes a DataExtractor, and executes the extraction process.

        :param file: (str) The JSON file that will lead the xtraction process.
        :return: None
        """

    # Read extraction information from the JSON file.
    with open(file, 'r') as json_file:
        data = json.load(json_file)
        extraction_info = data.get("extraction_info")
        infiles = [i.get('input') for i in extraction_info]
        sample_file, main_key_column = data.get("main").values()

    # Initialize DataExtractor with the customer sample file
    data_extractor = DataExtractor(sample_file, main_key_column, preprocess_files=True, files=infiles)

    # Execute the extraction process according to the provided sequence.
    codes = None
    for extraction in extraction_info:
        if codes:
            data_extractor.codes = codes.get(extraction['key_column'])
        codes = data_extractor.extract_data(
            extraction['input'], extraction['output'], extraction['key_column'], extraction.get('relevant_keys')
        )


if __name__ == "__main__":

    # Call the function to execute the data extraction process.
    file = 'extraction_info.json'
    process_extraction_info(file)
