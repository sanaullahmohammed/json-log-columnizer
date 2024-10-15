# JSON Log Columnizer

## Overview
The JSON Log Columnizer project is designed to process JSON objects from a specified log file and convert them into a columnar format after flattening the JSON structure. This enables easier analysis and manipulation of the data by transforming complex nested JSON objects into a more accessible table-like format.

## Features

- **JSON Flattening**: Converts nested JSON objects into a flat structure for easier data handling.
- **Columnar File Output**: Outputs the processed data in a columnar format, suitable for data analysis and reporting.
- **Multithreading Support**: Utilizes multithreading to improve performance by concurrently mining, refining, and storing data.
- **Concurrent Queues**: Implements a thread-safe concurrent queue to facilitate communication between threads.


## Components

### Miner:
Responsible for reading the JSON log file and extracting data.
Pushes the raw JSON objects into a concurrent queue for further processing.

### Refiner:
Takes the raw JSON objects from the miner and flattens them into a columnar format.
Performs any necessary transformations or cleaning of the data before passing it to the storer.

### Storer:
Receives the refined columnar data and writes it to the specified results directory.
Ensures the data is saved in a structured and accessible manner.

## Usage

### Prerequisites
Ensure you have the .NET runtime installed on your machine.

## Running the Application

### Clone the Repository:

```bash
git clone [https://github.com/yourusername/JsonLogColumnizer.git](https://github.com/sanaullahmohammed/JsonLogColumnizer.git)
cd JsonLogColumnizer
```

### Build the Project:

Use your preferred IDE or the command line to build the project.

### Execute the Program:

Run the program from the command line with the following syntax:

```bash
```


## Contributing
Contributions are welcome! If you would like to contribute, please fork the repository and create a pull request with your changes.

## License
This project is licensed under the MIT License - see the LICENSE file for details.
