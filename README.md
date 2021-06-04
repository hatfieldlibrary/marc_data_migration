# Record Modification Tool

Python modules created for migration of Pacific Northwest College of Art (PNCA) records to 
Alma (Orbis-Cascade Alliance SLIS). Uses [pymarc](https://gitlab.com/pymarc/pymarc).

The main features are:

* Replacing fields with OCLC data based on OCLC number lookup and exact or fuzzy
matching on the item title.
* Modifying existing records with specific transformation rules.
* Creating plugins that define the rules for transformations.

I created a plugin module for our PNCA data and a sample starter plugin for adding a new record update policy.

```
usage: processor.py [-h] [-p module name] [-m] [-r] [-pm] [-db database name]
                    [-di] [-nt] [-t] [-tm] [-so] [-oc] [-ccf] [-d] [-dupt]
                    [-dupm] [-comp]
                    source

Process marc records.

positional arguments:
  source                Required: the path to the marc file to be processed

optional arguments:
  -h, --help            show this help message and exit
  -p module name, --plugin module name
                        The plugin module used for record modifications.
                        Example: processors.plugins.pnca.pnca_policy
  -m, --modify-recs     Just modify records using the provided plugin.
  -r, --replace-fields  Replace fields with fields from the OCLC record.
  -pm, --perfect-match  Perfect OCLC title match will be required; records
                        with lower fuzzy match ratios are written to a
                        separate file.
  -db database name, --use-database database name
                        Provide name of the postgres database name to use
                        instead of the OCLC API. This significantly speeds up
                        processing.
  -di, --database-insert
                        Insert records into database while replacing fields
                        with OCLC API data. Requires --use-database flag with
                        database name.
  -nt, --no-title-check
                        Skip the fuzzy title match on 245 fields before
                        updating records. You probably do not want to do this.
  -t, --track-fields    Create an audit log of modified fields.
  -tm, --track-title-matches
                        Create audit log of fuzzy title matches.
  -so, --save-oclc      Save records from OCLC to local xml file during while
                        running the replacement task.
  -oc, --oclc-records   Only download marcxml from OCLC number, no other tasks
                        performed.
  -ccf, --check-control-field-db
                        Reports duplicate 001/003 combinations.
  -d, --duplicates      Checks for duplicate OCLC numbers in the database.
  -dupt, --check-duplicate-title
                        Check for duplicate 245 fields.
  -dupm, --check-duplicate-main
                        Check for duplicate main entry fields.
  -comp, --compare_oclc_numbers
                        Retrieve OCLC records and compare oclc numbers in the
                        response and with the original input file. Logs the
                        discrepancies for analysis.
                        
Example:

process.py --replace-fields --plug-in processors.plugins.lib.lib_policy --track-fields 
--track-title-matches /home/user/marcfile 
```

We are using this utility to correct problems in records that were exported from the Alexandria ILS.
The problems included bad character encoding and invalid control fields.

When you provide the `--replace-fields` argument, the OCLC API is queried (an OCLC developer key is required). 
The utility will update any field in the `substitutions` list with corresponding data from the OCLC record.

Using the OCLC API is time-consuming. Running with the `--database-name` and `--database-insert` arguments will insert
OCLC  data into a postgres database table. Subsequent executions will run against the database 
when the `--database-name` argument is provided. Highly recommended if you need to do this more than one time!

Updated records are output to `updated-records`.  Unmodified records are written to `unmodified-records`.

# Matching Records

When the `--perfect-match` argument is used, only records with a perfect match on the OCLC 245(a)(b) subfields
are written to the `updated-records` file. Otherwise, the utility uses a fuzzy match algorithm. It writes
any record that meets the fuzzy match threshold requirement to `updated-records`.  An audit file can be used to assess the
accuracy of fuzzy record matching.

If you use the `--perfect-match` option, updated records with **imperfect matches** on 245(a)(b) are
written to `fuzzy-modified-records`. These records can be immediately reviewed, or loaded separately into
the Alma _Institution Zone_ and reviewed as part of a cleanup project. A `fuzzy-match-passed` or `fuzzy-match-failed`
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
