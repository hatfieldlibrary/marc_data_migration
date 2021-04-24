###### Record Migration Tools

Python modules created for migration of Pacific Northwest College of Art records to 
Alma (Orbis-Cascade Alliance SLIS).

```
usage: processor.py [-h] [-db database name] [-r] [-comp] [-nt] [-t] [-m]
                    [-so] [-oc] [-d]
                    source

Process marc records.

positional arguments:
  source                Required: the path to the marc file to be processed

optional arguments:
  -h, --help            show this help message and exit
  -db database name, --use-database database name
                        Postgres database name to be used instead of OCLC API
  -r, --replace-fields  Replace fields with fields from the OCLC record.
  -comp, --compare_oclc_numbers
                        This utility retrieves OCLC records and compares oclc
                        numbers inthe response and the original input file.
                        Logs the discrepancies for analysis.
  -nt, --no-title-check
                        Skip the fuzzy title match on 245a before updating
                        records
  -t, --track-fields    Create an audit log of modifed fields.
  -m, --track-title-matches
                        Create a log of fuzzy title matches.
  -so, --save-oclc      Save records from OCLC to local xml file during
                        replacement task.
  -oc, --oclc-records   Only download marcxml for all records with OCLC
                        number, no other tasks performed.
  -d, --duplicates      Checks the source file for duplicate OCLC numbers in
                        the database.
                        
Example:
process.py --replace-fields --track-fields --track-title-matches /home/user/marcfile
