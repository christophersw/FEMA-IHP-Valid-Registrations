# OpenFEMA Dataset: Individuals and Households Program - Valid Registrations - v1

This is a simple tool (written in Golang) to parse the [OpenFEMA Dataset: Individuals and Households Program - Valid Registrations - v1](https://www.fema.gov/openfema-data-page/individuals-and-households-program-valid-registrations-v1) to a SQLite database.

**Please check to commit records to see when this was updated.**

### Usage
This tool is written in Golang and can be run from the command line with `go run main.go`.

It looks for `IHP.csv` in the root folder. This CSV file is downloaded (manually) from the [FEMA](https://www.fema.gov/openfema-data-page/individuals-and-households-program-valid-registrations-v1) and contains the data for the IHP Valid Registrations.

This tool reads the CSV line-by-line and parses the data into a SQLite database.