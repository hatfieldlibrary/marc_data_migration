# Record Modification Tool

Python modules created for migration of Pacific Northwest College of Art (PNCA) records exported from Alexandria to 
Alma (Orbis-Cascade Alliance SLIS). Uses [pymarc](https://gitlab.com/pymarc/pymarc).

The features are:

* Plugins that define rules for record transformations.
* A module for updating records with data from the OCLC Worldcat API.
* Functions for generating reports.

```
usage: processor.py [-h] [-p plugin name] [-m] [-r] [-pm]
                    [-database database name] [-udb] [-adb] [-nt] [-dft] [-t]
                    [-tm] [-so] [-oc] [-ccf] [-d] [-dupt] [-dupm] [-comp]
                    source

Process marc records.

positional arguments:
  source                Required: the path to the marc file to be processed

optional arguments:
  -h, --help            show this help message and exit
  -p plugin name, --plugin plugin name
                        The plugin module used for record modifications.
                        Example: processors.plugins.pnca.pnca_policy
  -m, --modify-recs     Just modify records using the provided plugin.
  -r, --replace-fields  Replace fields with fields from the OCLC record.
  -pm, --perfect-match  Perfect OCLC title match will be required; records
                        with lower fuzzy match ratios are written to a
                        separate file.
  -database database name, --database-name database name
                        Provide name of the postgres database name to use
                        instead of the OCLC API. This significantly speeds up
                        processing.
  -udb, --use-database  While replacing fields with OCLC API data insert
                        records into the provided database. Requires --use-
                        database flag with database name.
  -adb, --add-to-database
                        While replacing fields with OCLC API data insert
                        records into the provided database. Requires --use-
                        database flag with database name.
  -nt, --no-title-check
                        Skip the title match on 245 fields before updating
                        records. You probably do not want to do this.
  -dft, --do-fuzzy-test
                        This option adds an additional test of fuzzy match
                        records when the OCLC number was found based only on
                        the 003 label.
  -t, --track-fields    Create an audit log of modified fields.
  -tm, --track-title-matches
                        Create audit log of fuzzy title matches that includes
                        match accuracy metrics.
  -so, --save-oclc      Save records from OCLC to local xml file during while
                        running the replacement task.
  -oc, --oclc-records   Only download marcxml from OCLC, no other tasks
                        performed.
  -ccf, --check-duplicate-control-field
                        Reports duplicate 001/003 combinations. You must
                        provide a test database name.
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

process.py --replace-fields --perfect-match --plug-in processors.plugins.lib.lib_policy 
--track-fields  --track-title-matches --do-fuzzy-test --db database_name /home/user/marcfile 
```

## Plugins

You can define rules for modifying records using an `UpdatePolicy` plugin and providing it at runtime using
the `--plug-in` argument. This package includes the plugin we developed for our migration into Alma and a 
sample starter plugin.

If you want to modify a record using the plugin, without updating fields with OCLC data, use the `--modify-recs` 
argument.

## Reports
You can run a number of reports that we developed to analyze errors in our records and review
the output of OCLC field substitution. The analysis of OCLC title matches includes metrics to help with
determining accuracy.

Some optional reports are only available when using the
`--replace-fields` argument to update records with OCLC data: `--track-fields`,
`--track-title-matches`. Other optional reports can be run independently: `--check-control-field-db`, `--duplicates`,
`--check-duplicate-main`, and `--compare_oclc_numbers`. Some of these reports use a relational database table.

These reports are admittedly ad hoc and may not be useful for other projects.  See `reporting` directory for details.


## OCLC API Record Harvesting
You can harvest OCLC records in two ways. The common and most useful approach adds records to a postgres database 
using the `--add-to-database` option. You can then use the database for subsequent processing by adding the 
`--use-database` argument and providing the database name via the `--database-name` argument (and optionally the 
password). This is recommended to speed up processing.

If you like, you can also write OCLC records to a MARCXML file.  

## Updating with OCLC Data
If your records need a serious fix, you can update fields and/or add new fields using data retrieved 
from OCLC. For larger projects, this will require an OCLC API developer key (the path to key is defined 
in `proccessor.py`). Use the `--replace-fields` argument and additional arguments such as `--perfect-match`, 
`--track-title-matches`, and `--use-database`.

If you decide to replace fields, you should review and possibly update the `substitution_array` defined in 
`replace_configuration.py`. This list determines which fields get updated with OCLC data. Also, there are two
replacement strategies available: `replace_and_add` and `replace_only`.  The obvious difference is that
`replace_and_add` (default) will add new fields to the record. The `replace_only` strategy simply updates 
existing fields.

The record locations used for the OCLC number in order of precedence are:

* 035 with OCoLC label
* 001 value with an OCLC prefix
* 001/003 combination with an OCoLC label, and an 001 value that does not have an OCLC prefix

When using the `--perfect-match` argument, only records with perfect matches on OCLC 245(a)(b)
get written to the `updated-records` file. For imperfect matches, the program updates the record with OCLC
data and writes the output to a `fuzzy-updated-records` file for later review. It also adds a `fuzzy-match-passed` 
or `fuzzy-match-failed` label to the 962 field so records can be reviewed later,
after they are loaded into the system.  If you
add the `--track-title-matches` argument, the program generates a tab-delimited audit file with accuracy metrics
based on Levenshtein Distance and Jaccard Similarity. Sorting on these metrics can be useful during view
and for identifying problems early.

The `--do-fuzzy-test` argument is a special case that might be helpful in some cases. In our data set, 
OCLC numbers for 001 values without an OCLC prefix ('ocn', 'ocm', or 'on') were inaccurate. 
We used the `--do-fuzzy-test` argument to trigger a secondary evaluation that excludes 
records below an accuracy threshold (Levenshtein Distance). Records that do not meet the 
threshold get written to the `non-modified` records file with no OCLC update. Since you probably want to
remove the invalid OCLC number from the record, the program replaces 
the OCLC 001/003 values with a unique local identifier provided by the plugin's `set_local_id()` 
method. The exact nature of this fine-tuning is dependents your data and requirements, which is why it's delegated
in your plugin.

Records that do not have an OCLC match (or are rejected by `--do-fuzzy-test` mentioned above) get written 
to a `non-updated-records` file.

When not using the `--perfect-match` you get different results.  The program does not write records to a 
`fuzzy-updated-records` file.  Instead, records get updated and added to the `updated-records` file when they meet or
exceed an accuracy threshold (Levenshtein Distance). All other records get written to the 
`non-updated-records` file. This may be useful when the data is clean, and you have a threshold fuzzy match ratio
 that excludes any misfits.

If you want to do record transformations at the same time that you update records with OCLC data, you can provide 
the plugin using the `--plug-in` argument.  Plugin transformations will be applied after OCLC updates.

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
