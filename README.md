###### Record Migration Tools

Python modules created for migration of Pacific Northwest College of Art records to 
Alma (Orbis-Cascade Alliance SLIS).

```
usage: processor.py [-h] [-r] [-pm] [-db database name] [-di] [-comp] [-nt]
                    [-t] [-m] [-so] [-oc] [-d]
                    source

Process marc records.

positional arguments:
  source                Required: the path to the marc file to be processed

optional arguments:
  -h, --help            show this help message and exit
  -r, --replace-fields  Replace fields with fields from the OCLC record.
  -pm, --perfect-match  Perfect OCLC title match will be required with lower
                        fuzzy matchratios written to seperate files.
  -db database name, --use-database database name
                        Postgres database name to use instead of the OCLC API
  -di, --database-insert
                        Insert record into database while replacing fields
                        with OCLC API data. Requires --use-database flag with
                        database name.
  -comp, --compare_oclc_numbers
                        Retrieve OCLC records and compare oclc numbers inthe
                        response and with the original input file. Logs any
                        the discrepancies for analysis.
  -nt, --no-title-check
                        Skip the fuzzy title match on 245a before updating
                        records. You probably do not want to do this
  -t, --track-fields    Create an audit log of modified fields.
  -m, --track-title-matches
                        Create audit log of fuzzy title matches.
  -so, --save-oclc      Save records from OCLC to local xml file during
                        replacement task.
  -oc, --oclc-records   Only download marcxml from OCLC number, no other tasks
                        performed.
  -d, --duplicates      Checks for duplicate OCLC numbers in the database.
          
Example:

process.py --replace-fields --track-fields --track-title-matches /home/user/marcfile
