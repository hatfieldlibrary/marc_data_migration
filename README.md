# Record Modification Tool

Python modules created for migration of Pacific Northwest College of Art (PNCA) records to 
Alma (Orbis-Cascade Alliance SLIS). Uses [pymarc](https://gitlab.com/pymarc/pymarc).

The features are:

* Plugins that define rules for record transformations.
* A module for updating records with data from the OCLC Worldcat API.
* Functions for generating reports.

```
usage: processor.py [-h] [-p module name] [-m] [-r] [-pm] [-db database name]
                    [-di] [-nt] [-dft] [-t] [-tm] [-so] [-oc] [-ccf] [-d]
                    [-dupt] [-dupm] [-comp]
                    source

Process marc records.

positional arguments:
  source                Required: the path to the marc file to be processed

optional arguments:
  -h, --help            show this help message and exit
  -p module name, --plugin plugin name
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
  -dft, --do-fuzzy-test
                        This option adds an additional test of fuzzy match
                        records when the OCLC number was found based only on
                        the 003 label.
  -t, --track-fields    Create an audit log of modified fields.
  -tm, --track-title-matches
                        Create audit log of fuzzy title matches.
  -so, --save-oclc      Save records from OCLC to local xml file during while
                        running the replacement task.
  -oc, --oclc-records   Only download marcxml from OCLC, no other tasks
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

process.py --replace-fields --perfect-match --plug-in processors.plugins.lib.lib_policy --track-fields 
--track-title-matches --do-fuzzy-test --db database_name /home/user/marcfile 
```

## Plugins

You can define rules for modifying records using an `UpdatePolicy` plugin and providing it at runtime using
the `--plug-in` argument. This package includes the plugin we developed for our migration into Alma and a 
sample starter plugin.

## Reports
You can run a number of reports that we developed to analyze errors in our records and review
the output of OCLC field substitution. The analysis of OCLC title matches includes metrics to help with
determining accuracy.

## OCLC API Record Harvesting
You can harvest OCLC records in two ways. The method used in package adds records to a postgres database using 
the `--database-insert` option. You can use the database for subsequent processing by adding the `-db` flag and 
providing the database name (and optionally the password). This is recommended to speed up processing.

If you like, you can also write OCLC records to a MARCXML file.  

## Updating with OCLC Data
If your records require this step, you can update and/or add new OCLC record fields. For large projects, this 
will require and OCLC API developer key (path to key defined in `proccessor.py`). Use 
the `--replace-fields` argument and additional arguments such as `--perfect-match`, `--track-title-matches`, and
`--db`.

If you replace fields you will probably want to review and update the `substitution_array` defined in 
`replace_configuration.py`. This list determines which fields get updated with OCLC data. There are two
replacement strategies available: `replace_and_add` and `replace_only`.  The obvious difference is that
`replace_and_add` (default) will add new fields to the record. The `replace_only` strategy simply updates 
existing fields.

The record locations used for the OCLC number in order of precedence are:

* 035 with OCoLC label
* 001 value with an OCLC prefix
* 001/003 combination with an OCoLC label, and an 001 value that does not have an OCLC prefix

When using the `--perfect-match` argument, only records with perfect matches on OCLC 245(a)(b)
get written to the `updated-records` file. For imperfect matches, the program updates the record with OCLC
data, but writes the output to a `fuzzy-updated-records` file for later review. A `fuzzy-match-passed` 
or `fuzzy-match-failed` label gets added to the 962 field so these records can be found for review
after records are loaded into the system. To assist with the later review, you can
add the `--track-title-matches` argument.  This generates a tab-delimited audit file with accuracy metrics
based on Levenshtein Distance and Jaccard Similarity. Sorting on these metrics can be useful.

The `--do-fuzzy-test` argument is a special case that might be helpful elsewhere. In our data set, 
OCLC numbers for 001 values without an OCLC prefix ('ocn', 'ocm', or 'on') were highly inaccurate. 
The `--do-fuzzy-test` argument triggers a secondary evaluation that excludes 
records that do not meet an accuracy threshold (Levenshtein Distance). Records that do not meet the 
threshold get written to the `non-modified` records file without an OCLC update after the program replaces 
the OCLC 001/003 values with a unique local identifier provided by the plugin's `set_local_id()` 
method. This sort of fine-tuning is obviously dependent the results you see in your own data.

Records that do not have an OCLC match (or are rejected by `--do-fuzzy-test` mentioned above) get written 
to a `non-updated-records` file.

When not using the `--perfect-match` you get quite different results.  The program does not write records to the 
`fuzzy-updated-records` file.  Instead, records get updated and added to the `updated-records` file when they meet or
exceed an accuracy threshold (Levenshtein Distance). All other records get written to the 
`non-updated-records` file. This may be useful when the data is clean, and you are confident that you can set
a threshold that excludes any misfits.

# Output Files

## updated-records
Records that are updated with OCLC data.

## updated-online
Records for online resources that are updated with OCLC data.

## non-updated-records
Original input records that are not updated with OCLC data.

## non-updated-online
Original input records for online resources that are not updated with OCLC data.

## bad-records
Records that could not be processed by pymarc because of errors in the original marc record.

## fuzzy-updated-records
Records that have been updated with OCLC data but lack an exact match on the 245 fields. 

## fuzzy-online-records
Records for electronic resources that have been updated with OCLC data but lack an exact match on the 245 fields. 

## fuzzy-original-records
The original input records for comparison with fuzzy-updated-records.

# Audit files

## title-fuzzy-match
A tab-delimited text file with information on fuzzy match records: Levenshtein Distance, Jaccard Similarity,
original title, oclc title, pass/fail result, oclc number. 

## fields-audit
A tab-delimited file recording all field replacements: oclc number, tag, new value, original value.

## mat-type-analysis
Identifies conflict between call number and location (300) fields. Idiosyncratic, so uses a plugin method. 
Tab-delimited.

## field-035-details
Captures subfield "z", duplicate or missing "a"

## duplicate_title_fields
Reports all records with multiple 245 fields

## duplicate_100_fields
Reports all records with multiple 1xx fields

## missing-245a
Records that have no 245(a) value.

# Harvest

## oclc
OCLC MARCXML written to this file when `--save-oclc` or `--oclc-records` are used.
