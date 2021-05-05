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
                        fuzzy match ratios written to seperate files.
  -db database name, --use-database database name
                        Provide name of the postgres database name to use
                        instead of the OCLC API. This significantly speeds up
                        processing.
  -di, --database-insert
                        Insert records into database while replacing fields
                        with OCLC API data. Requires --use-database flag with
                        database name.
  -comp, --compare_oclc_numbers
                        Retrieve OCLC records and compare oclc numbers in the
                        response and with the original input file. Logs any
                        the discrepancies for analysis.
  -nt, --no-title-check
                        Skip the fuzzy title match on 245a before updating
                        records. You probably do not want to do this.
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
```

This utility was written to correct several problems in records that were exported from the Alexandria ILS.
These problems included bad character encoding and invalid control fields. The primary goal is to prepare records
for addition to the Alma (ExLibris) Network Zone. This utility can also move existing data to
local fields that will be maintained after Alma ingest. 

When the `--replace-fields` argument is provided, the OCLC API will be queried (an OCLC developer key is required). 
The utility will  update any field in the `substitutions` list with corresponding data from the OCLC record.

Using the OCLC API is time-consuming. Running with the `--database-name` and `--database-insert` arguments will insert
OCLC  data into a postgres database table. Subsequent executions will run against the database when the  
`--database-name` argument is provided. Highly recommended if you need to do this more than one time!

Updated records are output to `updated-records`.  Unmodified records are written to `unmodified-records`.

# Matching Records

When the `--perfect-match` argument is provided, only records with a perfect match on the OCLC 245(a)(b) subfields
are written to the `updated-records` file. Otherwise, the utility uses a fuzzy match algorithm and writes
any record that meets the threshold requirement to `updated-records`.  An audit file can be used to assess the
accuracy of fuzzy record matching.

If you do use the `--perfect-match` option, updated records with imperfect matches on 245(a)(b) are
written to `fuzzy-modified-records`. These records can be immediately reviewed, or loaded separately into
the Alma Institution Zone and reviewed as part of a cleanup project. A `fuzzy-match-passed` or `fuzzy-match-failed`
label is added to the local 962 field to make that work easier. (Note that records failing the fuzzy match threshold
are often valid matches because of variations in cataloging.)

# Output Files

## updated-records
Records that are updated with OCLC data.

## unmodified-records
Original input records that are not updated with OCLC data.

## bad-records
Records that could not be processed by pymarc because of errors in the original marc record.

## fuzzy-modified-records
Records that have been updated with OCLC data without an exact match on the 245 fields. See audit log.

## fuzzy-original-records
The original input records for comparison with fuzzy-modified-records.

# Audit files

## title-fuzzy-match
A tab-delimited text file with information on fuzzy match records: original title, oclc title, titles normalized for 
comparison, fuzzy match ratio, pass/fail result,  oclc number. 

## fields_audit
A tab-delimited file with all field replacements: oclc number, tag, new value, original value.

# OCLC XML

## oclc
When the --save-oclc argument is used, marcxml written to this file.
