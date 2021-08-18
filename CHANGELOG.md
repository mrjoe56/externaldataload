## Version 1.11
* rare data migration (added param and fields)
* temporarily non-recallable tag: do not set on volunteer records that are linked to multiple panels
## Version 1.10
* included UCL and CNS data for upload

## Version 1.9
* created stub to load HLQ data
* allow multiple disease records of the same disease but different notes

## Version 1.8
* bug fix site mapping: check site alias (STRIDES and IBD) before ODS code

## Version 1.7
* further update to 1.6

## Version 1.6
* ensure one and only one address is flagged as 'primary'
* avoid address duplication with comparison on lowercase and letters and numbers only

## Version 1.5
* updated withdrawal procedure to include STRIDES

## Version 1.4
* added data field 'genotypic_sex'

## Version 1.3
* added Pack ID MH (EDGI and GLAD)
* updated error reporting

## Version 1.2
* added EDGI data
* set/unset non-recallable reason with 'temporarily non-recallable' tag
* added checks on DOB and surname

## Version 1.1
* added GLAD data
* added missing data field for NAFLD
* do not load email addresses with invalid format
* on update keep old surname as former surname under the contact identifiers
* set/unset temporarily non-recallable tag
* use consent date for recruitment case (insetead of current date)

## Version 1.0
* initial version
