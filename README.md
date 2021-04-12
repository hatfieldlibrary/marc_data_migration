###### Record Migration Tools

Python modules created for migration of Pacific Northwest College of Art records to 
Alma (Orbis-Cascade Alliance SLIS).

```positional arguments:
source                path to the marc file that will be processed

optional arguments:
-h, --help            show this help message and exit
-db database name, --use-database database name
         postgres database name to be used instead of OCLC API
-nt, --no-title-check
         skip the fuzzy title match on 245a
-r, --replace-fields  replace fields with OCLC data
-t, --track-fields    Create an audit log of modifed fields.
-m, --track-title-matches
Create a log of fuzzy title matches.
-so, --save-oclc      Save records from OCLC to local xml file for reuse.
-oc, --oclc-records   just download the OCLC marcxml records```

Example:
process.py /home/user/marcfile --replace-fields --track-fields --track-title-matches
