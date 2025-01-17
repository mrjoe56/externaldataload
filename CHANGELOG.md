## version 1.61
* update (some of the) data for volunteers with statuses other than pending and active who did not ask their data to be destroyed
* include links for panel/consent and packID/consent (newly added data and migration - latter can be removed for performance reasons after first upload)

## version 1.60
* give warning if any of the headers in the upload file is not part of the default mapping
* added "cih_type_leicester" for Leicester data load (one-off, use datasource imid or nafld)

## version 1.59
*Fixed bug in nhs validation function  (Requires version 2.57 of nihrbackbone) (https://www.wrike.com/open.htm?id=1294854474)

## version 1.58
* Added NHS number validation to importdemographics csv (Requires version 2.57 of nihrbackbone) (https://www.wrike.com/open.htm?id=1294854474)

## version 1.57
* added new fields for assent upload (PIBD)

## version 1.56
* fixed version 1.55

## version 1.55
* addAddress: fixed is_primary issue (as v1.54 for email and phone)
* https://www.wrike.com/open.htm?id=1172459680 -
  for CYP guardian address was not added to second/third child if spelt slightly different and not duplicated on guardian record

## version 1.54
* addEmail, addPhone: use API call instead of direct insert (is_primary previously incorrectly set in some cases)

## version 1.53
* Do not overwrite first name if already stored under contact identifier 'Former/alternative surname'

## version 1.52
* include old ODS codes for site mapping

## version 1.51
* do not add multiple DCYPHR IDs to a volunteer record

## version 1.50
* PIBD/CYP: do not add relationship if inactive one already exists to the same guardian

## version 1.49
* adding historic rare disease identifiers for data upload: WGSID, EGSID, Illumina ID

## version 1.48
* DOB check updated

## version 1.47
* bug fix v1.46

## version 1.46
* Allow any DOB (must not be before consent date)

## version 1.45
CYP: create assent activity for ages 3-5 without assent version

## Version 1.44
* CYP: do not create duplicate guardian records, guardian records are mapped on name and email address (or DOB, if provided)

## Version 1.43
* Updated names of participationinstudies backbone functions to be volunteerparticipationinstudies to match changes (https://www.wrike.com/open.htm?id=1093689558)

## Version 1.42
* CYP: add panel without site

## Version 1.41
* Updated names of functions in LoadAssent to match backbone and made it so assent sends status as correct by default

## Version 1.40
* EH: added disable/enableFullGroupMode around query, perhaps linked to error reported by Carola
* update HLQ data load

## Version 1.39
* upload consent data for slam volunteers with status 'consent outdated'

## Version 1.38
* added new consent field 'opt in to Gel NGRL'
* added HLQ data fields
* added CYP data load (guardian and volunteer data as PIBD)

## Version 1.37
* added new field 'consent_details'

## Version 1.36
* renamed cih_type_covid-cns_id to cih_type_covid_cns_id
* updated findGuardian - to be used for CYP

## Version 1.35
* updates to support PIBD data load (folder name change, Inception panel)

## Version 1.34
* include panel and ID into subject line for consent
* upload of SLaM BioResource data

## Version 1.33
* fix to previous version

## Version 1.32
* added generic functionality to upload activities and recruitment case activities

## Version 1.31
* NHS number not to be updated

## Version 1.30
* PIBD data

## Version 1.29
* included guardian
* pibd: add guardian relationship

## Version 1.28
* updated addAlias: fixed duplication error (wrike https://www.wrike.com/open.htm?id=814123304)

## Version 1.27
* upload of HLQ data (using participant ID as identifier)
* upload medication data (starfish migration - free text - and drug families)

## Version 1.26
* CPMS accrual activity added for IBD volunteers

## Version 1.25
* Newcastle BioResource (NCL) data upload

## Version 1.24
* CNS data upload (included COVID CNS ID)

## Version 1.23
* addRecruitmentCaseActivity - avoid duplicates (without datetime)

## Version 1.22
* added HLQ fields willing_to_give_blood, willing_commercial, willing_to_travel

## Version 1.21
* rare migration: allow multiple activities with different dates

## Version 1.20
* included data upload for Newcastle BioResource
* rare migration: moved CPMS activity back to recruitment case; no error checking on DOB

## Version 1.19
* rare migration: upload data for non active volunteers

## Version 1.18
* rare migration: fixed redundant

## Version 1.17
* rare migration: fixed opted_out_of_gel_main; only apply redundant/withdrawn activity/status to records not already on orca; allow panel without site
* CPMS accrual added as stand alone activity

## Version 1.16
* rare migration: added CPMS accrual and redundant activities
* notes: avoid duplicates (exact match only)

## Version 1.15
* added notes_date and cih_type_hospital_number

## Version 1.14
* rare migration: only add temporarily-non-recallable flag for new volunteers

## Version 1.13
* updated rare alias types

## Version 1.12
* included IMID and GSTT data for upload
* fct "processDeceased" migrated from Backbone and updated to allow flagging a volunteer as deceased without a date of death provided

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
