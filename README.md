# File Retriever

A tool to connect to and interact with servers via FTP/SFTP clients. 

### Usage
The file_retriever tool can be imported and used in other applications or used with its CLI. 

#### Commands
The following information is also available using `fetch --help`

##### Get Vendor Files
Retrieves files from one or more vendor servers via FTP/SFTP

`$ fetch vendor-files`

Options:
    `-v, --vendor`:
        The vendor whose server you would like to connect to and retrieve files from.
        Use `-v all` to retrieve files from all configured vendors.
        This option can be repeated: 
            eg. `$ fetch vendor-files -v eastview -v leila`
    `-d, --days`: 
        The number of days to go back to search for files
    `-h, --hours`:
        The number of hours to go back to search for files
    `-m, --minutes`
        The number of minutes to go back to search for files

##### Get Daily Vendor Files
Retrieves files updated within last day from servers of all configured vendors 
`$ fetch daily-vendor-files`

##### Get List of Configured Vendors
Lists all vendors whose servers are available to connect to
`$ fetch available-vendors`
