###### Record Migration Tools

Python modules created for migration of Pacific Northwest College of Art records to 
Alma (Orbis-Cascade Alliance SLIS).

```
usage: processor.py [-h] [-db database name] [-r] [-nt] [-t] [-m] [-so] [-oc]
                    source

Process marc records.

positional arguments:
  source                Path to the marc file that will be processed

optional arguments:
  -h, --help            show this help message and exit
  -db database name, --use-database database name
                        Postgres database name to be used instead of OCLC API
  -r, --replace-fields  Replace fields with OCLC data
  -nt, --no-title-check
                        Skip the fuzzy title match on 245a before updating
                        records
  -t, --track-fields    Create an audit log of modifed fields.
  -m, --track-title-matches
                        Create a log of fuzzy title matches.
  -so, --save-oclc      Save records from OCLC to local xml file during
                        replacement.
  -oc, --oclc-records   Download marcxml for all records with OCLC number.
  
Example:
process.py --replace-fields --track-fields --track-title-matches /home/user/marcfile
