<?php
use CRM_Externaldataload_ExtensionUtil as E;
// accept no time limit
set_time_limit(0);

/**
 * Class for National BioResource CSV Importer, demographics import
 *
 * @author Carola Kanz
 * @date 11/02/2020
 * @license AGPL-3.0
 */
class CRM_Externaldataload_NihrImportDemographicsCsv
{

  private $_csvFile = NULL;
  private $_importId = NULL;
  private $_separator = NULL;
  private $_csv = NULL;
  private $_columnHeaders = [];
  private $_dataSource = NULL;
  private $_imported = NULL;
  private $_failed = NULL;
  private $_read = NULL;
  private $_originalFileName = NULL;
  private $_createRecord = NULL;

  /**
   * CRM_Externaldataload_NihrImportDemographicsCsv constructor.
   *
   * @param string $csvFileName
   * @param string $separator
   * @param bool $firstRowHeaders
   * @param string $originalFileName
   * @param string $context
   *
   * @throws Exception when error in logMessage
   */

  public function __construct($csvFileName, $additional_parameter = [])
  {
    if (isset($additional_parameter['separator'])) {
      $this->_separator = $additional_parameter['separator'];
    } else {
      $this->_separator = ';';
    }

    if (isset($additional_parameter['dataSource'])) {
      $this->_dataSource = $additional_parameter['dataSource'];
    } else {
      $this->_dataSource = "";
    }

    if (isset($additional_parameter['createRecord'])) {
      $this->_createRecord = $additional_parameter['createRecord'];
    } else {
      $this->_createRecord = 1;
    }

    $this->_logger = new CRM_Nihrbackbone_NihrLogger('nbrcsvimport_' . $this->_dataSource . '_' . date('Ymdhis'));
    $this->_failed = 0;
    $this->_imported = 0;
    $this->_read = 0;
    $this->_importId = uniqid(rand());
    $this->_csvFile = $csvFileName;
  }


  /**
   * Method to check if the import data is valid
   *
   * @return bool
   * @throws
   */
  public function validImportData()
  {
    // already checked that file exists

    // open
    $this->_csv = fopen($this->_csvFile, 'r');
    if (!$this->_csv) {
      $message = E::ts('Could not open csv file ') . $this->_csvFile . E::ts(' in ') . __METHOD__ . E::ts(', import aborted.');
      CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'error');
      return FALSE;
    }

    // is there any data?
    $data = fgetcsv($this->_csv, 0, $this->_separator);
    if (!$data || empty($data)) {
      $message = E::ts('No data in csv file ') . $this->_csvFile . E::ts(', no data imported');
      CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'warning');
      fclose($this->_csv);
      return FALSE;
    }

    // read headers
    foreach ($data as $key => $value) {
      $this->_columnHeaders[$key] = $value;
      // todo validate if i have all headers
    }

    return TRUE;
  }

  /**
   * Method to process import
   *
   * @param string $recallGroup
   * @return array|void
   * @throws Exception
   */
  public function processImport($recallGroup = NULL)
  {
    // get mapping
    $this->getMapping();

    // check if mapping contains mandatory columns according to source given
    if (($this->_dataSource == 'ucl' && !isset($this->_mapping['cih_type_ucl_local'])) ||
      ($this->_dataSource == 'gstt' && !isset($this->_mapping['cih_type_gstt'])) ||
      ($this->_dataSource == 'ncl' && !isset($this->_mapping['cih_type_newcastle']) &&
        !isset($this->_mapping['cih_type_packid']) && !isset($this->_mapping['cih_type_newcastle_local']) )||
      ($this->_dataSource == 'ibd' && !isset($this->_mapping['pat_bio_no'])) ||
      ($this->_dataSource == 'strides' && !isset($this->_mapping['cih_type_strides_pid']) &&
        !isset($this->_mapping['cih_type_pack_id_din'])) ||
      ($this->_dataSource == 'nafld' && !isset($this->_mapping['cih_type_packid'])) ||
      ($this->_dataSource == 'imid' && !isset($this->_mapping['cih_type_packid'])) ||
      ($this->_dataSource == 'glad' && !isset($this->_mapping['cih_type_glad_id'])) ||
      ($this->_dataSource == 'edgi' && !isset($this->_mapping['cih_type_edgi_id'])) ||
      ($this->_dataSource == 'slam' && !isset($this->_mapping['cih_type_slam'])) ||
      ($this->_dataSource == 'hlq' && !isset($this->_mapping['cih_type_participant_id'])) ||
      ($this->_dataSource == 'rare_migration' && !isset($this->_mapping['cih_type_rare_migration_id']))
    ) {
      $this->_logger->logMessage('ID column missing for ' . $this->_dataSource . ' data not loaded', 'ERROR');
    } elseif (!isset($this->_mapping['panel'])) {
      // todo check on panel, centre and site
      $this->_logger->logMessage('panel missing for ' . $this->_dataSource . ' data not loaded', 'ERROR');
    } else {
      $this->importDemographics();
    }
    fclose($this->_csv);

    // return messages in return values
    return $this->setJobReturnValues();
  }

  /**
   * Method  to fill an array with the messages of the job (so they can be shown as returnValues in the scheduled job log)
   * @return array
   */
  private function setJobReturnValues()
  {
    $result = [];
    try {
      $apiMessages = civicrm_api3('NbrImportLog', 'get', [
        'import_id' => $this->_importId,
        'sequential' => 1,
        'options' => ['limit' => 0],
      ]);
      foreach ($apiMessages['values'] as $message) {
        $result[] = $message['message_type'] . ": " . $message['message'];
      }
    } catch (CiviCRM_API3_Exception $ex) {
    }
    return $result;
  }

  /**
   * Method to get the data mapping based on the name of the csv file
   */
  private function getMapping()
  {
    $container = CRM_Extension_System::singleton()->getFullContainer();
    $resourcePath = $container->getPath('externaldataload') . '/resources/';
    /* $mappingFile = $resourcePath . DIRECTORY_SEPARATOR . $this->_dataSource . "_mapping.json";
    if (!file_exists($mappingFile)) {
      $mappingFile = $resourcePath . DIRECTORY_SEPARATOR . "default_mapping.json";
    } */

    // use default mapping file for all projects
    $mappingFile = $resourcePath . DIRECTORY_SEPARATOR . "default_mapping.json";

    $mappingJson = file_get_contents($mappingFile);
    $this->_mapping = json_decode($mappingJson, TRUE);
  }

  /**
   * Method to process the participation import (participation id)
   *
   * @throws
   */
  private function importDemographics()
  {
    $this->_logger->logMessage('file: ' . $this->_csvFile . '; dataSource: ' . $this->_dataSource . '; separator: ' . $this->_separator);

    while (!feof($this->_csv)) {
      $data = fgetcsv($this->_csv, 0, $this->_separator);
      if ($data) {
        // map data based on filename
        $data = $this->applyMapping($data);
        // format data (e.g. mixed case, trim...)
        $data = $this->formatData($data);

        CRM_Core_DAO::disableFullGroupByMode();
        // add volunteer or update data of existing volunteer
        [$contactId, $dataStored, $new_volunteer, $project_identifier] = $this->addContact($data);
        CRM_Core_DAO::reenableFullGroupByMode();
        // data is not stored if no local identifier is given or if the existing volunteer has a status
        // other than active or pending
        if ($dataStored) {
          $this->addEmail($contactId, $data);
          $this->addAddress($contactId, $data);
          //$this->addPhone($contactId, $data, 'phone_home', Civi::service('nbrBackbone')->getHomeLocationTypeId(), CRM_Nihrbackbone_BackboneConfig::singleton()->getPhonePhoneTypeId());
          //$this->addPhone($contactId, $data, 'phone_work', Civi::service('nbrBackbone')->getWorkLocationTypeId() , CRM_Nihrbackbone_BackboneConfig::singleton()->getPhonePhoneTypeId());
          //$this->addPhone($contactId, $data, 'phone_mobile', Civi::service('nbrBackbone')->getHomeLocationTypeId(), CRM_Nihrbackbone_BackboneConfig::singleton()->getMobilePhoneTypeId());

          if (isset($data['contact_category']) && $data['contact_category'] == 'Phone') {
            $this->addPhone($contactId, $data, 'phone', $data['contact_location'], $data['contact_phone_type'], $data['is_primary']);
          }

          $this->addNote($contactId, $data['notes'], $data['notes_date']);

          if ($data['panel'] <> '' || $data['site'] <> '' || $data['centre'] <> '') {
            $this->addPanel($contactId, $data['panel'], $data['site'], $data['centre'], $data['source'], $this->_dataSource);
          }

          // *** Aliases ***
          if (!empty($data['cih_type_nhs_number'])) {
            $this->addAlias($contactId, 'cih_type_nhs_number', $data['cih_type_nhs_number'], 0);
          }
          if (!empty($data['cih_type_packid'])) {
            $this->addAlias($contactId, 'cih_type_packid', $data['cih_type_packid'], 2);
          }
          if (!empty($data['cih_type_ibd_id'])) {
            $this->addAlias($contactId, 'cih_type_ibd_id', $data['cih_type_ibd_id'], 2);
          }
          if (!empty($data['cih_type_covid_cns_id'])) {
            $this->addAlias($contactId, 'cih_type_covid_cns_id', $data['cih_type_covid_cns_id'], 2);
          }

          // STRIDES
          if (!empty($data['cih_type_strides_pid'])) {
            $this->addAlias($contactId, 'cih_type_strides_pid', $data['cih_type_strides_pid'], 2);
          }
          if (!empty($data['cih_type_pack_id_din'])) {
            $this->addAlias($contactId, 'cih_type_pack_id_din', $data['cih_type_pack_id_din'], 2);
          }
          if (!empty($data['cih_type_blood_donor_id'])) {
            $this->addAlias($contactId, 'cih_type_blood_donor_id', $data['cih_type_blood_donor_id'], 2);
          }

          // NAFLD
          if (!empty($data['cih_type_nafld_br'])) {
            $this->addAlias($contactId, 'cih_type_nafld_br', $data['cih_type_nafld_br'], 2);
          }

          // GLAD
          if (!empty($data['cih_type_glad_id'])) {
            $this->addAlias($contactId, 'cih_type_glad_id', $data['cih_type_glad_id'], 2);
          }
          if (!empty($data['cih_type_slam'])) {
            $this->addAlias($contactId, 'cih_type_slam', $data['cih_type_slam'], 2);
          }
          if (!empty($data['cih_type_pack_id_mh'])) {
            $this->addAlias($contactId, 'cih_type_pack_id_mh', $data['cih_type_pack_id_mh'], 2);
          }

          // EDGI (incl. slam ID and pack ID MH above)
          if (!empty($data['cih_type_edgi_id'])) {
            $this->addAlias($contactId, 'cih_type_edgi_id', $data['cih_type_edgi_id'], 2);
          }

          if (isset($data['previous_names']) && !empty($data['previous_names'])) {
            $this->addAlias($contactId, 'cih_type_former_surname', $data['previous_names'], 2);
          }

          // Rare migration
          if (!empty($data['cih_type_rare_migration_id'])) {
            $this->addAlias($contactId, 'cih_type_rare_migration_id', $data['cih_type_rare_migration_id'], 0);
          }
          $aliases = array(
            'cih_type_bridge_id',
            'cih_type_genetics_dept_number',
            'cih_type_oc_subject_id',
            'cih_type_pedigree_number',
            'cih_type_rare_study_specific_id',
            'cih_type_family_id',
            'cih_type_bpd_cs_id',
            'cih_type_cpms_id',
            'cih_type_gold_id',
            'cih_type_igan_id',
            'cih_type_imperial_oc_id',
            'cih_type_medscinet_id',
            'cih_type_mendelian_id',
            'cih_type_radar_id',
            'cih_type_thrombogenomics_id',
            'cih_type_catgo_pack_id',
            'cih_type_cuh_pathology_id',
            'cih_type_gel_id',
            'cih_type_hospital_number'
          );

          foreach ($aliases as &$alias) {
            if (isset($data[$alias]) && !empty($data[$alias])) {
              $this->addAlias($contactId, $alias, $data[$alias], 2);
            }
          }

          // *** Diseases ***
          $this->addDisease($contactId, $data['family_member'], $data['disease'], $data['diagnosis_year'], $data['diagnosis_age'], $data['disease_notes'], $data['taking_medication']);

          // *** Medication data ***
          if ((isset($data['medication_starfish_data']) && !empty($data['medication_starfish_data'])) ||
            (isset($data['medication_drug_family']) && !empty($data['medication_drug_family'])) ||
            (isset($data['medication_name']) && !empty($data['medication_name']))) {
            $this->addMedication($contactId, $data['medication_starfish_data'], $data['medication_drug_family'],
              $data['medication_name'], $data['medication_date']);
          }


          // *** add source specific identifiers and data *********************************************************
          switch ($this->_dataSource) {
            case "ibd":
            case "pibd":
              if ($data['diagnosis'] <> '') {
                $this->addDisease($contactId, 'family_member_self', $data['diagnosis'], '', '', '', '');
              }
              if ($data['cih_type_ibdgc_number'] <> '') {
                $this->addAlias($contactId, 'cih_type_ibdgc_number', $data['cih_type_ibdgc_number'], 1);
              }
              if ($data['cih_type_guardian_id'] <> '') {
                $this->addAlias($contactId, 'cih_type_guardian_id', $data['cih_type_guardian_id'], 2);
              }

              if (isset($data['guardian_of']) and $data['guardian_of'] <> '') {
                // create link to guardian record
                $this->addRelationship($contactId, $data['guardian_of'], 'nbr_guardian_of');
              }
              break;

            case "cyp":
              if ($data['cih_type_dcyphr_id'] <> '') {
                $this->addAlias($contactId, 'cih_type_dcyphr_id', $data['cih_type_dcyphr_id'], 2);
              }
              if ($data['cih_type_guardian_id'] <> '') {
                $this->addAlias($contactId, 'cih_type_guardian_id', $data['cih_type_guardian_id'], 2);
              }

              if (isset($data['guardian_of']) and $data['guardian_of'] <> '') {
                // create link to guardian record
                $this->addRelationship($contactId, $data['guardian_of'], 'nbr_guardian_of');
              }
              break;

            case "ucl":
              $this->addAlias($contactId, 'cih_type_ucl_local', $data['cih_type_ucl_local'], 0);
              $this->addAlias($contactId, 'cih_type_ucl', $data['cih_type_ucl'], 0);

              break;

            case "gstt":
              $this->addAlias($contactId, 'cih_type_gstt', $data['cih_type_gstt'], 0);
              break;

            case "ncl":
              $this->addAlias($contactId, 'cih_type_newcastle', $data['cih_type_newcastle'], 2);
              $this->addAlias($contactId, 'cih_type_newcastle_local', $data['cih_type_newcastle_local'], 2);
              break;
            }

          // *** all recruitment information is stored in one recruitment case *************************
          // *** regardless if volunteer is new (might be missing in existing record) - check if recruitment
          // *** case exists and retrieve ID
          //$caseId = CRM_Nihrbackbone_NbrRecruitmentCase::getActiveRecruitmentCaseId($contactId);
          if ($data['contact_sub_type'] <> 'nbr_guardian') {
            $caseID = $this->createRecruitmentCase($contactId, $data['consent_date']);


            // add consent to recruitment case
            // NOTE: status 'not valid' is set for IBD - call might need to be updated for other projects
            if (isset($data['consent_version']) && $data['consent_version'] <> '') {
              $nbrConsent = new CRM_Externaldataload_LoadConsent();
              $consent_status = 'consent_form_status_correct';
              if ($this->_dataSource == 'ibd' ||
                ($this->_dataSource == 'pibd' && $data['consent_type'] == 'consent_type_face_to_face')) {
                $consent_status = 'consent_form_status_not_valid';
              }
              $subject = "Consent " . $data['panel'] . " $project_identifier";
              $nbrConsent->addConsent($contactId, $caseID, $consent_status, $subject, $data, $this->_logger);
            }

            // assent (CYP only)
            if (isset($data['assent_version']) &&
              (isset($data['assent_date']) && $data['assent_date'] <> '')) {
              $nbrAssent = new CRM_Externaldataload_LoadAssent();
              //$subject = "Assent " . $data['panel'] . " $project_identifier";
              $subject = "Assent";
              $nbrAssent->addAssent($contactId, $caseID, $subject, $data, $this->_logger);
            }

            // migrate paper questionnaire flag (IBD)
            if (isset($data['nihr_paper_hlq']) && $data['nihr_paper_hlq'] == 'Yes') {
              $this->addRecruitmentCaseActivity($contactId, 'nihr_paper_hlq', '', '', 'Completed', '', $caseID);
            }

            // migrate spine lookup data
            if (isset($data['spine_lookup']) && $data['spine_lookup'] <> '') {
              $this->addRecruitmentCaseActivity($contactId, 'spine_lookup', $data['spine_lookup'], '', 'Completed', '', $caseID);
            }
            // migrate date ibd questionnaire data loaded
            if (isset($data['ibd_questionnaire_data_loaded']) && $data['ibd_questionnaire_data_loaded'] <> '') {
              $this->addRecruitmentCaseActivity($contactId, 'ibd_questionnaire_data_loaded', $data['ibd_questionnaire_data_loaded'], '', 'Completed', '', $caseID);
            }

            // migrate CPMS accrual activity (rare data migration) + for IBD volunteers
            if (isset($data['cpms_accrual_date']) && $data['cpms_accrual_date'] <> '') {
              if ($this->_dataSource == 'rare_migration') {
                $this->addRecruitmentCaseActivity($contactId, 'nihr_cpms_accrual', $data['cpms_accrual_date'], 'Rares', 'Completed', '', $caseID);
              } elseif ($this->_dataSource == 'ibd') {
                $this->addRecruitmentCaseActivity($contactId, 'nihr_cpms_accrual', $data['cpms_accrual_date'], 'IBD', 'Arrange', '', $caseID);
              }
            }
          }

          // gdpr request - very likely only used for migration
          if (isset($data['gdpr_request_received']) && $data['gdpr_request_received'] <> '') {
            $this->addActivity($contactId, 'nihr_gdpr_request_received', $data['gdpr_request_received'], '', 'Completed', '');
          }
          if (isset($data['gdpr_sent_to_nbr']) && $data['gdpr_sent_to_nbr'] <> '') {
            $this->addActivity($contactId, 'nihr_gdpr_sent_to_nbr', $data['gdpr_sent_to_nbr'], '', 'Completed', '');
          }

          // ** withdrawal data
          if ((!empty($data['withdrawn_date']) && ($this->_dataSource != 'rare_migration' or $new_volunteer == 1))
            ||
            ($this->_dataSource == 'strides' && isset($data['withdrawal_request_date']) && !empty($data['withdrawal_request_date']))) {
            $this->withdrawVolunteer($contactId, $data, $this->_dataSource);
          } elseif ((!empty($data['withdrawn_date']) && $this->_dataSource == 'rare_migration' && $new_volunteer == 0)) {
            $this->_logger->logMessage('Volunteer ' . $project_identifier . ' (' . $contactId . ') flagged as withdrawn on rare civi but active on orca.', 'WARNING');
          }

          // ** redundant data - rare migration only
          if (!empty($data['redundant_date']) and $this->_dataSource == 'rare_migration') {
            if ($new_volunteer == 1) {
              $this->processRedundant($contactId, $data);
            } else {
              $this->_logger->logMessage('Volunteer ' . $project_identifier . ' (' . $contactId . ') flagged as redundant on rare civi but active on orca.', 'WARNING');
            }
          }

          // ** deceased
          if ((isset($data['deceased']) and !empty($data['deceased']))
            or (isset($data['deceased_date']) and !empty($data['deceased_date']))) {
            $deceasedDate = '';
            if (isset($data['deceased_date']) and !empty($data['deceased_date'])) {
              $deceasedDate = $data['deceased_date'];
            }
            $deceased = $this->processDeceased($contactId, $deceasedDate);
            // set volunteer status to deceased
            $this->setVolunteerStatus($contactId, Civi::service('nbrBackbone')->getDeceasedVolunteerStatus());
            if (!$deceased) {
              $message = "Error trying to set contact ID " . $contactId . " to deceased with deceased date: " . $data['death_reported_date'] . ". Migrated but no deceased processing.";
              CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'warning');
            }
          }

          // *** tags
          if (isset($data['temporarily_non_recallable'])) {
            if ($data['temporarily_non_recallable'] == 'Yes') {
              // only set tag if volunteer is not linked to any other panel
              $cntQuery = "SELECT count(*)
                    from civicrm_value_nihr_volunteer_panel
                    where entity_id = %1";
              $cntQueryParam = [
                1 => [$contactId, "Integer"],
              ];
              $cnt = CRM_Core_DAO::singleValueQuery($cntQuery, $cntQueryParam);
              if ($cnt < 2) {
                // for rare migration - only add, if volunteer not already on database
                if ($this->_dataSource != 'rare_migration' or $new_volunteer == 1) {
                  $this->addTag($contactId, 'Temporarily non-recallable');
                }
              } else {
                $this->removeTag($contactId, 'Temporarily non-recallable');
              }
            } elseif ($data['temporarily_non_recallable'] == 'No' || $data['temporarily_non_recallable'] == '') {
              $this->removeTag($contactId, 'Temporarily non-recallable');
            }
          }
          // activities
          if (isset($data['activity']) && !empty($data['activity'])) {
            $this->addActivity($contactId, $data['activity'], $data['activity_datetime'],
                $data['activity_subject'], $data['activity_status'], $data['activity_location']);
          }
          if (isset($data['rec_activity']) && !empty($data['rec_activity'])) {
            $this->addRecruitmentCaseActivity($contactId, $data['rec_activity'], $data['rec_activity_datetime'],
              $data['rec_activity_subject'], $data['rec_activity_status'], $data['rec_activity_location']);
          }
        }
      }
    }
  }


  /**
   * Method to map data according to loaded mapping
   *
   * @param $preMappingData
   * @return array
   */
  private function applyMapping($preMappingData)
  {
    $mappedData = [];

    // *** initialise with all fields
    foreach ($this->_mapping as $item) {
      if ($item <> 'temporarily_non_recallable') {
        $mappedData[$item] = '';
      }
    }

    foreach ($preMappingData as $key => $value) {
      $header = $this->_columnHeaders[$key];
      if (isset($this->_mapping[$header])) {
        $newKey = $this->_mapping[$header];
      } else {
        // todo add to logfile
        $newKey = $key;
      }

      // NOTE: keep names for aliases, need to be added to the database separately

      // *** custom group 'general observations'
      if ($newKey == 'ethnicity') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_ethnicity_id', 'id');
      }
      if ($newKey == 'weight_kg') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_weight_kg', 'id');
      }
      if ($newKey == 'height_m') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_height_m', 'id');
      }
      if ($newKey == 'hand_preference') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_hand_preference', 'id');
      }
      if ($newKey == 'abo_group') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_abo_group', 'id');
      }
      if ($newKey == 'rhesus_factor') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_rhesus_factor', 'id');
      }
      if ($newKey == 'proband') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_proband', 'id');
      }
      if ($newKey == 'family_history') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_family_history', 'id');
      }
      // CYP only
      if ($newKey == 'school') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getGeneralObservationCustomField('nvgo_school_id', 'id');
      }


      // *** custom group 'Lifestyle'
      if ($newKey == 'alcohol') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_alcohol', 'id');
      }
      if ($newKey == 'alcohol_amount') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_alcohol_amount', 'id');
      }
      if ($newKey == 'alcohol_notes') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_alcohol_notes', 'id');
      }

      if ($newKey == 'smoker') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker', 'id');
      }
      if ($newKey == 'smoker_amount') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_amount', 'id');
      }
      if ($newKey == 'smoker_years') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_years', 'id');
      }
      if ($newKey == 'smoker_past') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_past', 'id');
      }
      if ($newKey == 'smoker_past_amount') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_past_amount', 'id');
      }
      if ($newKey == 'smoker_past_years') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_past_years', 'id');
      }
      if ($newKey == 'smoker_gave_up') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoker_gave_up', 'id');
      }
      if ($newKey == 'smoker_notes') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_smoking_notes', 'id');
      }
      if ($newKey == 'diet') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_diet', 'id');
      }
      if ($newKey == 'diet_notes') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getLifestyleCustomField('nvl_diet_notes', 'id');
      }

      // *** selection eligibility

      // possible values for willing_to_travel, willing_to_give_blood, willing_commercial are 'yes' or 'no'
      // NOTE: '1' will be saved as 'yes', but '0' as empty!!
      if ($newKey == 'willing_to_travel') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_willing_to_travel', 'id');
      }
      if ($newKey == 'willing_to_give_blood') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_willing_to_give_blood', 'id');
      }
      if ($newKey == 'willing_commercial') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_willing_commercial', 'id');
      }
      if ($newKey == 'exclude_from_drug_studies') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_no_drug_studies', 'id');
      }
      if ($newKey == 'exclude_from_studies_with_mri') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_no_mri', 'id');
      }
      if ($newKey == 'genotypic_sex') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_genotypic_sex', 'id');
      }
      if ($newKey == 'gender_at_birth') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerSelectionEligibilityCustomField('nvse_gender_at_birth', 'id');
      }

      // *** custom group 'Quality of life'
      if ($newKey == 'life_quality_overall') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_overall', 'id');
      }
      if ($newKey == 'life_quality_happiness') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_happiness', 'id');
      }
      if ($newKey == 'life_quality_energy') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_energy', 'id');
      }
      if ($newKey == 'life_quality_opportunity') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_opportunity', 'id');
      }
      if ($newKey == 'life_quality_money') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_money', 'id');
      }
      if ($newKey == 'employment_status') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerLifeQualityCustomField('nvlq_employment_status', 'id');
      }

      // *** participation in other study ***
      if ($newKey == 'other_study') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerParticipationInStudiesCustomField('nvpis_other_study', 'id');
      }
      if ($newKey == 'other_study_type') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerParticipationInStudiesCustomField('nvpis_other_study_type', 'id');
        // multiple types possible
        if (!empty($value)) {
          $x = explode('-', $value);
          $value = CRM_Core_DAO::VALUE_SEPARATOR . implode(CRM_Core_DAO::VALUE_SEPARATOR, $x);
        }
      }

      //
      if ($newKey == 'non_recallable_reason') {
        $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerStatusCustomField('nvs_nonrecallable_reason', 'id');
      }


      // todo don't use hardcoded
      if ($newKey == 'pack_id') {
        // todo $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_ucl_br_local', 'id');
        // &&& $mappedData['nva_alias_type'] = 'alias_type_packid';
        // &&& $mappedData['nva_external_id'] = $value;


        $mappedData['identifier_type'] = 'cih_type_packid';
        $mappedData['identifier'] = $value;


        /*  $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('alias_type_ibd_id', 'id');
          $mappedData[$newKey] = $value;
          $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_external_id', 'id');
          $mappedData[$newKey] = $value;
          // mapping ID is entered twice, once for insert (custom ID) and once for mapping (local_ucl_id)
          $newKey = 'custom_'.CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_alias_type', 'id');
          ; */
      }

      if ($newKey == 'pack_id') {
        // todo $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_ucl_br_local', 'id');
        // &&& $mappedData['nva_alias_type'] = 'alias_type_packid';
        // &&& $mappedData['nva_external_id'] = $value;


        $mappedData['identifier_type'] = 'cih_type_packid';
        $mappedData['identifier'] = $value;


        /*  $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('alias_type_ibd_id', 'id');
          $mappedData[$newKey] = $value;
          $newKey = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_external_id', 'id');
          $mappedData[$newKey] = $value;
          // mapping ID is entered twice, once for insert (custom ID) and once for mapping (local_ucl_id)
          $newKey = 'custom_'.CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerAliasCustomField('nva_alias_type', 'id');
          ; */
      }

      $mappedData[$newKey] = $value;
    }
    return $mappedData;
  }

  private function formatData($xData)
  {
    $this->formatDataItem($xData['first_name']);
    $this->formatDataItem($xData['last_name']);
    if (isset($xData['email'])) {
      $xData['email'] = strtolower($xData['email']);
    }
    $this->formatDataItem($xData['address_1']);
    $this->formatDataItem($xData['address_2']);
    $this->formatDataItem($xData['address_3']);
    $this->formatDataItem($xData['address_4']);
    return $xData;
  }

  private function formatDataItem(&$dataItem)
  {
    if ($dataItem <> '') {
      $dataItem = ucwords(strtolower($dataItem), '- ');
      $dataItem = trim($dataItem);
    }
  }


  /**
   * @param $data
   * @return array
   * @throws Exception
   *
   * *** add new volunteer or update data of existing volunteer
   */
  private function addContact(&$data)
  {
    $new_volunteer = 1;
    $storeData = 1;

    // todo move to volunteer class (?)

    $data['contact_type'] = 'Individual';
    $data['contact_sub_type'] = 'nihr_volunteer';

    if (($this->_dataSource == 'pibd' || $this->_dataSource == 'cyp') &&
        isset($data['cih_type_guardian_id']) &&
          $data['cih_type_guardian_id'] <> '') {
      $data['contact_sub_type'] = 'nbr_guardian';
    }

    // NOTE: these two settings are only used for migration and only have any effect if the numbergenerator
    // is disabled when the data is loaded!
    if (isset($data['participant_id']) && $data['participant_id'] <> '') {
      $participant_custom_id = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerIdsCustomField('nva_participant_id')['id'];
      $data[$participant_custom_id] = $data['participant_id'];
    }
    if (isset($data['bioresource_id']) && $data['bioresource_id'] <> '') {
      $bioresource_custom_id = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerIdsCustomField('nva_bioresource_id')['id'];
      $data[$bioresource_custom_id] = $data['bioresource_id'];
    }

    // prepare data for 'preferred contact' (multiple options)
    if ($data['preferred_communication_method'] <> '') {
      $xpref = explode('-', $data['preferred_communication_method']);
      $data['preferred_communication_method'] = CRM_Core_DAO::VALUE_SEPARATOR . implode(CRM_Core_DAO::VALUE_SEPARATOR, $xpref);
    }

    $volunteer = new CRM_Nihrbackbone_NihrVolunteer();
    $contactId = '';
    $identifier = '';
    $identifier_type = '';

    switch ($this->_dataSource) {
      case "ucl":
        // use national identifier as main identifier
        if ($data['cih_type_ucl'] <> '') {
          $identifier_type = 'cih_type_ucl';
          $identifier = $data['cih_type_ucl'];
        }
        break;
      case "gstt":
        // use national identifier as main identifier
        if ($data['cih_type_gstt'] <> '') {
          $identifier_type = 'cih_type_gstt';
          $identifier = $data['cih_type_gstt'];
        }
        break;
      case "ncl":
        // either pack ID or national ID is provided
        if ($data['cih_type_packid'] <> '') {
          $identifier_type = 'cih_type_packid';
          $identifier = $data['cih_type_packid'];
        } elseif ($data['cih_type_newcastle'] <> '') {
          $identifier_type = 'cih_type_newcastle';
          $identifier = $data['cih_type_newcastle'];
        } elseif ($data['cih_type_newcastle_local'] <> '') {
          $identifier_type = 'cih_type_newcastle_local';
          $identifier = $data['cih_type_newcastle_local'];
        } else {
          $this->_logger->logMessage('Neither National ID nor Pack ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "cns":
        if ($data['cih_type_covid_cns_id'] <> '') {
          $identifier_type = 'cih_type_covid_cns_id';
          $identifier = $data['cih_type_covid_cns_id'];
        }
        break;
      case "ibd":
      case "pibd":
        if (!empty($data['pat_bio_no'])) {
          $identifier = $data['pat_bio_no'];
          if (strpos($identifier, 'IBD') !== false) {
            $identifier_type = 'cih_type_ibd_id';
          } else {
            $identifier_type = 'cih_type_packid';
          }
        } elseif (!empty($data['cih_type_guardian_id'])) {
          $identifier = $data['cih_type_guardian_id'];
          $identifier_type = 'cih_type_guardian_id';
        } else {
          $this->_logger->logMessage('IBD/PIBD project ID missing, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "strides":
        // either cih_type_strides_pid or cih_type_pack_id_din needs to be provided, if not, don't store the record
        if ($data['cih_type_strides_pid'] <> '') {
          $identifier_type = 'cih_type_strides_pid';
          $identifier = $data['cih_type_strides_pid'];
        } elseif ($data['cih_type_pack_id_din'] <> '') {
          $identifier_type = 'cih_type_pack_id_din';
          $identifier = $data['cih_type_pack_id_din'];
        } else {
          $this->_logger->logMessage('Neither STRIDES pid nor pack ID provided, no data loaded: ' . $data['last_name'] . ' ' . $data['cih_type_blood_donor_id'], 'ERROR');
        }
        break;
      case "nafld":
      case "imid":
        // packid
        if ($data['cih_type_packid'] <> '') {
          $identifier_type = 'cih_type_packid';
          $identifier = $data['cih_type_packid'];
        } else {
          $this->_logger->logMessage('No packID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "glad":
        // cih_type_glad_id
        if ($data['cih_type_glad_id'] <> '') {
          $identifier_type = 'cih_type_glad_id';
          $identifier = $data['cih_type_glad_id'];
        } else {
          $this->_logger->logMessage('No GLAD ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "edgi":
        // cih_type_edgi_id
        if ($data['cih_type_edgi_id'] <> '') {
          $identifier_type = 'cih_type_edgi_id';
          $identifier = $data['cih_type_edgi_id'];
        } else {
          $this->_logger->logMessage('No EDGI ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "slam":
        // cih_type_slam
        if ($data['cih_type_slam'] <> '') {
          $identifier_type = 'cih_type_slam';
          $identifier = $data['cih_type_slam'];
        } else {
          $this->_logger->logMessage('No SLAM ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "hlq":
        // cih_type_participant_id
        if ($data['cih_type_participant_id'] <> '') {
          $identifier_type = 'cih_type_participant_id';
          $identifier = $data['cih_type_participant_id'];
          // never create a new record using the participant ID as identifier, only add to existing record
          $this->_createRecord = 0;
        } else {
          $this->_logger->logMessage('No Participant ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "rare_migration":
        // cih_type_rare_migration_id
        if ($data['cih_type_rare_migration_id'] <> '') {
          $identifier_type = 'cih_type_rare_migration_id';
          $identifier = $data['cih_type_rare_migration_id'];
        } else {
          $this->_logger->logMessage('No Rare Migration ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;
      case "cyp":
        if ($data['cih_type_dcyphr_id'] <> '') {
          $identifier_type = 'cih_type_dcyphr_id';
          $identifier = $data['cih_type_dcyphr_id'];
        }
        elseif (!empty($data['cih_type_guardian_id'])) {
          $identifier = $data['cih_type_guardian_id'];
          $identifier_type = 'cih_type_guardian_id';
        } else {
          $this->_logger->logMessage('No guardian ID/CYP ID provided, no data loaded: ' . $data['last_name'], 'ERROR');
        }
        break;

      default:
        $this->_logger->logMessage('no default mapping for ' . $this->_dataSource, 'ERROR');
    }

    // only continue if identifier for project is provided
    if ($identifier <> '' and $identifier_type <> '') {
      $data[$identifier_type] = $identifier;

      // check if ID already on database
      $contactId = $volunteer->findVolunteerByAlias($identifier, $identifier_type, $this->_logger);
      if (!$contactId) {
        // check if volunteer/guardian is already on Civi under a different panel/without the given ID
        if ($data['contact_sub_type'] == 'nihr_volunteer') {
          $contactId = $volunteer->findVolunteer($data, $this->_logger);
        } elseif ($data['contact_sub_type'] == 'nbr_guardian') {
          $contactId = $this->findGuardian($data, $this->_logger);
        }
      }

      if ($contactId) {
        // volunteer already exists
        // do not save data if volunteer is not active or pending
        if (!$volunteer->VolunteerStatusActiveOrPending($contactId, $this->_logger) &&
          $this->_dataSource != 'rare_migration' &&
          $this->_dataSource != 'cns' &&
          // slam: 'consent outdate' should be added to the function in the backbone; this is
          // a shortcut to upload one-off legacy slam data
          $this->_dataSource != 'slam') {
          $this->_logger->logMessage('volunteer ' . $identifier . ' (' . $contactId .
            ') has status other than active or pending, no data loaded', 'WARNING');
          return array(0, 0, 0, $identifier);
        }

        $data['id'] = $contactId;
        $new_volunteer = 0;

        // check if surnames in database and data file match - if not, add existing surname to former
        // names
        if (isset($data['last_name']) && $data['last_name'] <> '') {
          $dbLastName = civicrm_api3('Contact', 'getvalue', [
            'return' => "last_name",
            'id' => $contactId,
          ]);
          if ($dbLastName <> $data['last_name'] && $dbLastName <> '' && $dbLastName <> 'x') {
            if ($this->checkFormerSurname($contactId, $data['last_name']) > 0) {
              // surname is already stored as former surname - don't overwrite in this case
              $data['last_name'] = $dbLastName;
              $this->_logger->logMessage("$identifier ($contactId): surname already stored as 'former', not overwritten", 'WARNING');
            } else {
              $this->addAlias($contactId, 'cih_type_former_surname', $dbLastName, 2);
            }
          }
        }
      } else { // new record

        // ... but not for HLQ data (as this might be linked to withdrawn volunteers but not deleted from the cum file)
        if ($this->_createRecord == 0) {
          $this->_logger->logMessage("$identifier: ID does not exist on the database, no data loaded", 'WARNING');
          return array(0, 0, 0, $identifier);
        }

        // for records with missing names (e.g. loading from sample receipts) a fake first name and surname needs to be added
        if ($data['first_name'] == '') {
          $data['first_name'] = 'x';
        }
        if ($data['last_name'] == '') {
          $data['last_name'] = 'x';
        }

        // set volunteer status to 'pending' for IBD and 'active' for other projects
        $volunteerStatus = 'volunteer_status_active';
        if ($this->_dataSource == 'ibd' ||
          ($this->_dataSource == 'pibd' && $data['consent_type'] == 'consent_type_face_to_face')) {
          $volunteerStatus = 'volunteer_status_pending';
        }

        // give warning if consent is missing for new records
        if ((!isset($data['consent_date']) || $data['consent_date'] == '') &&
          $data['contact_sub_type'] == 'nihr_volunteer') {
          $this->_logger->logMessage("$identifier: consent missing", 'WARNING');
        }
        $volunteerStatusCustomField = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerStatusCustomField('nvs_volunteer_status', 'id');
        $data[$volunteerStatusCustomField] = $volunteerStatus;
      }

      foreach ($data as $key => $value) {
        if (!empty($value)) {
          $params[$key] = $value;
        }
      }

      // *** dob - only enter for glad if age >= 16 and <100 to avoid overwriting corrected data
      if (isset($params['birth_date'])) {
        $params['birth_date'] = $this->checkDOB($identifier, $params['birth_date'], $params['consent_date'], $this->_dataSource);
      }

      try {
        // create/update volunteer record
        $params['debug'] = 1;
        $result = civicrm_api3("Contact", "create", $params);
        $this->_logger->logMessage('Volunteer ' . $data['participant_id'] . ' ' . $identifier . ' (' . (int)$result['id'] . ') successfully loaded/updated. New volunteer: ' . $new_volunteer, 'INFO');
        $contactId = $result['id'];
      } catch (CiviCRM_API3_Exception $ex) {
        $this->_logger->logMessage('when adding volunteer ' . $data['last_name'] . " " . $ex->getMessage(), 'ERROR');
      }

      // **** if no name is available ('x' inserted) - TODO: use participant ID instead
    } else {
      $this->_logger->logMessage('local identifier missing, data not loaded ' . $data['last_name'], 'ERROR');
      $storeData = 0;
    }
    return array($contactId, $storeData, $new_volunteer, $identifier);
  }

  /**
   * Method to add email
   * @param $contactID
   * @param $data
   */
  private function addEmail($contactID, $data)
  {
    // *** add or update volunteer email address
    if ($data['email'] <> '') {
      // --- only add if the format is correct
      if (!filter_var($data['email'], FILTER_VALIDATE_EMAIL)) {
        $this->_logger->logMessage('addEmail ' . $contactID . ': invalid format, not added ' . $data['email'], 'WARNING');
      } else {

        // --- only add if not already on database either as mail or as former communication data
        $query = "SELECT COUNT(*) as emailCount,
          (SELECT COUNT(*) FROM civicrm_value_fcd_former_comm_data
          WHERE entity_id = %1 AND fcd_communication_type = %2 AND fcd_details LIKE %3) AS fcdCount
          FROM civicrm_email WHERE contact_id = %1 and email = %4";
        $dao = CRM_Core_DAO::executeQuery($query, [
          1 => [(int)$contactID, "Integer"],
          2 => ["email", "String"],
          3 => ["%" . $data['email'] . "%", "String"],
          4 => [$data['email'], "String"],
        ]);
        if ($dao->fetch()) {
          if ($dao->emailCount == 0 && $dao->fcdCount == 0) {
            $primary = 0;
            if (isset($data['is_primary']) && $data['is_primary'] == 1) {
              $primary = 1;
            }
            $location = Civi::service('nbrBackbone')->getHomeLocationTypeId();
            if (isset($data['contact_location']) && $data['contact_location'] <> '') {
              $location = $data['contact_location'];
            }
            $insert = "INSERT INTO civicrm_email (contact_id, location_type_id, email, is_primary, is_billing, is_bulkmail, on_hold)
              VALUES(%1, %2, %3, %4, 0, 0, 0)";
            try {
              CRM_Core_DAO::executeQuery($insert, [
                1 => [(int)$contactID, "Integer"],
                2 => [(int)$location, "Integer"],
                3 => [$data['email'], "String"],
                4 => [(int)$primary, "Integer"],
              ]);
            } catch (CiviCRM_API3_Exception $ex) {
              $this->_logger->logMessage("addEmail $contactID " . $ex->getMessage(), 'ERROR');
            }
          }
        }
      }
    }
  }



  /**
   * Method to add addresss, contains recursive function loop for guardian dependents
   *
   * @param $contactID
   * @param $data
   */
  private function addAddress($contactID, $data)
  {
    // *** add or update volunteer home address
    if ($data['address_1'] <> '' && $data['postcode'] <> '') {
      // compare address line and postcode on lowercase without special chars
      $address_1_comp = preg_replace('/[^a-z0-9]/', '', strtolower($data['address_1']));
      $postcode_comp = preg_replace('/[^a-z0-9]/', '', strtolower($data['postcode']));

      // --- only add if not already on database as address or former communication data
      $query = "SELECT COUNT(*) as addressCount, (SELECT COUNT(*) FROM civicrm_value_fcd_former_comm_data
        WHERE entity_id = %1 AND fcd_communication_type = %2 AND (fcd_details LIKE %3 AND fcd_details LIKE %4))
            AS fcdCount
        FROM civicrm_address
        WHERE contact_id = %1
        and REGEXP_REPLACE(LOWER(street_address), '[^a-z0-9]', '') = %5
        and REGEXP_REPLACE(LOWER(postal_code), '[^a-z0-9]', '') = %6";

      $dao = CRM_Core_DAO::executeQuery($query, [
        1 => [(int)$contactID, "Integer"],
        2 => ["address", "String"],
        3 => ["%" . $data['address_1'] . "%", "String"],
        4 => ["%" . $data['postcode'] . "%", "String"],
        5 => [$address_1_comp, "String"],
        6 => [$postcode_comp, "String"],
      ]);
      if ($dao->fetch()) {

        if ($dao->addressCount == 0 && $dao->fcdCount == 0) {
          $primary = 0;
          if (isset($data['is_primary']) && $data['is_primary'] == 1) {
            $primary = 1;
          }

          // **** Manage primary flag in accordance with existing addresses
          $query2 = "SELECT a.id
                    from civicrm_address a
                    where a.contact_id = %1
                    and is_primary  = 1";
          $queryParams2 = [
            1 => [$contactID, "Integer"],
          ];
          $address_id = CRM_Core_DAO::singleValueQuery($query2, $queryParams2);

          // * if (primary=1 and other address already set to 'primary') update other flag to 0
          if ($primary == 1 && isset($address_id)) {
            try {
              $query3 = "update civicrm_address
                      set is_primary = 0
                      where id = %1";
              $queryParams3 = [
                1 => [$address_id, "String"],
              ];
              CRM_Core_DAO::executeQuery($query3, $queryParams3);
            } catch (Exception $ex) {
            }
          } // * if (primary=0 and no other address set as 'primary') set current address to primary nevertheless
          elseif ($primary == 0 && !isset($address_id)) {
            $primary = 1;
          }

          $location = Civi::service('nbrBackbone')->getHomeLocationTypeId();
          if (isset($data['location_type_id']) && $data['location_type_id'] <> '') {
            $location = $data['location_type_id'];
          }
          $columns = ['%1', '%2', '%3', '%4', '%5', '%6', '%7'];
          $insertParams = [
            1 => [(int)$contactID, "Integer"],
            2 => [(int)$location, "Integer"],
            3 => [(int)$primary, "Integer"],
            4 => [$data['address_1'], "String"],
            5 => [$data['address_4'], "String"],
            6 => [$data['postcode'], "String"],
            7 => [0, "Integer"],
          ];
          $index = 7;
          $insert = "INSERT INTO civicrm_address (contact_id, location_type_id, is_primary, street_address,
            city, postal_code, is_billing";
          // optional fields, only add if there is data
          if ($data['address_2'] <> '') {
            $index++;
            $insertParams[$index] = [$data['address_2'], "String"];
            $insert .= ", supplemental_address_1";
            $columns[] = "%" . $index;
          }
          if ($data['address_3'] <> '') {
            $index++;
            $insertParams[$index] = [$data['address_3'], "String"];
            $insert .= ", supplemental_address_2";
            $columns[] = "%" . $index;
          }

          if ($data['county']) {
            $mappedCountyId = CRM_Nihrbackbone_NihrAddress::getCountyIdForSynonym($data['county']);
            if ($mappedCountyId) {
              $index++;
              $insertParams[$index] = [(int)$mappedCountyId, "Integer"];
              $insert .= ", state_province_id";
              $columns[] = "%" . $index;
            }
          }
          if ($data['master_id'] <> '') {
            $index++;
            $insertParams[$index] = [$data['master_id'], "Integer"];
            $insert .= ", master_id";
            $columns[] = "%" . $index;
          }
          $insert .= ") VALUES(" . implode(", ", $columns) . ")";
          try {
            CRM_Core_DAO::executeQuery($insert, $insertParams);
          } catch (CiviCRM_API3_Exception $ex) {
            $this->_logger->logMessage("addAddress $contactID " . $ex->getMessage(), 'ERROR');
          }
        }
      }

      /**
       * After address is found or added, use this address for any existing dependants of guardian
       *  Only call if user has a dependant and is going to link address
       */

      if($data['link_address_to_dependant'] == 1 && $data['guardian_of']) {
        // Use addresses set in previous query
        $decypherId=$data['guardian_of'];
        $getDependantIdQuery= "SELECT entity_id FROM civicrm_value_contact_id_history WHERE identifier =%1";
        $getdependantIdParams = [ 1 => [$decypherId, "String"]];
        $dependantId = CRM_Core_DAO::singleValueQuery($getDependantIdQuery, $getdependantIdParams);

        // If contact_id exists for decypher id
        if($dependantId){
          // Get ID to be used as master_id (Links to guardians address ID)
          $getAddressIdQuery = "SELECT id FROM civicrm_address WHERE contact_id=%1 AND street_address=%2 AND postal_code=%3 LIMIT 1";
          $getAddressParams = [
            1 => [(int) $contactID, "Integer"],
            2 => [$data['address_1'], "String"],
            3 => [$data['postcode'], "String"]
          ];
          $masterId = CRM_Core_DAO::singleValueQuery($getAddressIdQuery, $getAddressParams);
          $this->_logger->logMessage("Adding new dependant address for guardian: ".$contactID . " Dependant id is ".$dependantId . " master id is ".$masterId, "INFO");

          if ($masterId) {
            // Make new dataset but for the dependant
            $newData = $data;
            $newData['contact_id'] = $dependantId;
            $newData['guardian_of'] = NULL; // probably not needed
            $newData['link_address_to_dependant'] = 0;
            $newData['master_id'] = $masterId;
            // Use as recursive function to avoid code repeat, add the new address + data, but next loop it will not repeat
            $this->addAddress($dependantId, $newData);
          }else{
            $this->_logger->logMessage("No master address ID found for : ".$contactID . " No address added to dependant: ".$dependantId  , "WARNING");
          }
        }
      }
    }
  }

  /**
   * Method to add phones
   *
   * @param $contactID
   * @param $data
   * @param $fieldName
   * @param $phoneLocation
   * @param $phoneType
   * @param int $isPrimary
   */
  private function addPhone($contactID, $data, $fieldName, $phoneLocation, $phoneType, $isPrimary = 0)
  {
    // *** add or update volunteer phone

    if ($data[$fieldName] <> '') {
      $phoneNumber = $data[$fieldName];

      // set phoneType, if unknown ('x')
      if ($phoneType == 'x') {
        if (substr($phoneNumber, 0, 2) == '07') {
          $phoneType = 2; // Mobile
        } else {
          $phoneType = 1;
        }
      }

      // only add if not already on database (do ignore type and location)
      // (remove blanks for comparison)
      $xPhoneNumber = str_replace(" ", "", $phoneNumber);

      $query = "SELECT COUNT(*) as phoneCount, (SELECT COUNT(*) FROM civicrm_value_fcd_former_comm_data
        WHERE entity_id = %1 AND fcd_communication_type = %2 AND replace(fcd_details, ' ', '') LIKE %3) AS fcdCount
        FROM civicrm_phone WHERE contact_id = %1 and replace(phone, ' ', '') = %4";
      $queryParams = [
        1 => [(int)$contactID, "Integer"],
        2 => ["phone", "String"],
        3 => ["%" . $xPhoneNumber . "%", "String"],
        4 => [$xPhoneNumber, "String"],
      ];
      $dao = CRM_Core_DAO::executeQuery($query, $queryParams);
      if ($dao->fetch()) {
        if ($dao->phoneCount == 0 && $dao->fcdCount == 0) {
          try {
            $insert = "INSERT INTO civicrm_phone (contact_id, phone, is_primary, location_type_id, phone_type_id) VALUES(%1, %2, %3, %4, %5)";
            $insertParams = [
              1 => [(int)$contactID, "Integer"],
              2 => [$phoneNumber, "String"],
              3 => [(int)$isPrimary, "Integer"],
              4 => [(int)$phoneLocation, "Integer"],
              5 => [(int)$phoneType, "Integer"],
            ];
            CRM_Core_DAO::executeQuery($insert, $insertParams);
          } catch (Exception $ex) {
            $this->_logger->logMessage('addPhone ' . $contactID . ' ' . $ex->getMessage(), 'ERROR');
          }
        }
      }
    }
  }

  /**
   * Method to add notes
   *
   * @param $contactID
   * @param $note
   */
  private function addNote($contactID, $note, $date)
  {
    if (isset($note) && $note <> '') {
      // only add if note does not already exists; only exact match
      $query = "SELECT count(*)
                  from civicrm_note
                  where entity_id = %1
                  and note = %2";
      $queryParams = [
        1 => [$contactID, "Integer"],
        2 => [$note, "String"],
      ];
      $cnt = CRM_Core_DAO::singleValueQuery($query, $queryParams);

      if ($cnt == 0) {

        if ($date == '') {
          // use today's date, if no date is given
          $date = date("Y-m-d");
        }
        $insert = "INSERT INTO civicrm_note (entity_table, entity_id, note, contact_id, modified_date) VALUES(%1, %2, %3, %2, %4)";
        $insertParams = [
          1 => ["civicrm_contact", "String"],
          2 => [(int)$contactID, "Integer"],
          3 => [$note, "String"],
          4 => [$date, "String"],
        ];
        CRM_Core_DAO::executeQuery($insert, $insertParams);
      }
    }
  }

  /**
   * Method to add an alias
   *
   * @param $contactID
   * @param $aliasType
   * @param $externalID
   * @param $update
   */
  private function addAlias($contactID, $aliasType, $externalID, $update)
  {
    // *** add alias aka contact_id_history_type

    // *** update=0 - do not update if alias already set
    // *** update=1 - update, if alias exists
    // *** update=2 - multiple aliases of this type possible, always add (avoiding duplicates)

    if (isset($aliasType) && $aliasType <> '') // todo add check if aliasType exists
    {
      if (isset($externalID) && $externalID <> '') {
        $doInsert = 0;

        $table = Civi::service('nbrBackbone')->getContactIdentityTableName();
        $identifierColumn = Civi::service('nbrBackbone')->getIdentifierColumnName();
        $identifierTypeColumn = Civi::service('nbrBackbone')->getIdentifierTypeColumnName();

        // --- check if civicrm_value_contact_id_history alias TYPE already exists ---------------------------------------------------------------------
        $query = "SELECT count(*)
                    FROM " . $table . "
                    where entity_id = %1
                    and " . $identifierTypeColumn . " = %2";
        $queryParams = [
          1 => [$contactID, "Integer"],
          2 => [$aliasType, "String"],
        ];

        $cnt = CRM_Core_DAO::singleValueQuery($query, $queryParams);
        if ($cnt > 0) {
          // identifier type exists, check if given identifier is already on the database
          $id = strtolower(str_replace(' ', '', $externalID));
          $query2 = "SELECT count(*)
                    FROM " . $table . "
                    where entity_id = %1
                    and " . $identifierTypeColumn . " = %2
                    and lower(replace(" . $identifierColumn . ", ' ', '')) = %3";
          $queryParams2 = [
            1 => [$contactID, "Integer"],
            2 => [$aliasType, "String"],
            3 => [$id, "String"],
          ];
          $cnt2 = CRM_Core_DAO::singleValueQuery($query2, $queryParams2);

          if ($cnt2 == 0) {
            if ($aliasType == 'cih_type_nhs_number') {
              // todo check if nhs number format is correct (subroutine to be written by JB)
            }
            if ($update == 0) {
              $this->_logger->logMessage("Contact ID $contactID: different identifier for $aliasType provided, not updated.", 'WARNING');
            } elseif ($update == 1) {
              // update identifier
              try {
                $query = "update " . $table . "
                        set " . $identifierColumn . " = %1, used_since = current_timestamp()
                        where entity_id = %2
                        and " . $identifierTypeColumn . " = %3";
                $queryParams = [
                  1 => [$externalID, "String"],
                  2 => [$contactID, "Integer"],
                  3 => [$aliasType, "String"],
                ];
                CRM_Core_DAO::executeQuery($query, $queryParams);
              } catch (Exception $ex) {
              }
            } else {
              // insert
              $doInsert = 1;
            }
          }
        }

        if ($cnt == 0 || $doInsert == 1) {
          // insert alias if type does not exist or if multiples are allowed
          try {
            $query = "insert into " . $table . " (entity_id, " . $identifierTypeColumn . ", " . $identifierColumn . ", used_since)
                             values (%1,%2,%3, current_timestamp())";
            $queryParams = [
              1 => [$contactID, "Integer"],
              2 => [$aliasType, "String"],
              3 => [$externalID, "String"],
            ];
            CRM_Core_DAO::executeQuery($query, $queryParams);
          } catch (Exception $ex) {
          }
        }
      }
    }
  }

  /**
   * Method to add disease
   *
   * @param $contactID
   * @param $familyMember
   * @param $disease
   * @param $diagnosisYear
   * @param $diagnosisAge
   * @param $diseaseNotes
   * @param $takingMedication
   */
  private function addDisease($contactID, $familyMember, $disease, $diagnosisYear, $diagnosisAge, $diseaseNotes, $takingMedication)
  {
    // *** add disease/conditions

    if ($familyMember <> '' and $disease <> '') {
      // todo check if disease and family member exists

      $table = Civi::service('nbrBackbone')->getDiseaseTableName();
      $diagnosisAgeColumn = Civi::service('nbrBackbone')->getDiagnosisAgeColumnName();
      $diagnosisYearColumn = Civi::service('nbrBackbone')->getDiagnosisYearColumnName();
      $diseaseColumn = Civi::service('nbrBackbone')->getDiseaseColumnName();
      $diseaseNotesColumn = Civi::service('nbrBackbone')->getDiseaseNotesColumnName();
      $familyMemberColumn = Civi::service('nbrBackbone')->getFamilyMemberColumnName();
      $takingMedicationColumn = Civi::service('nbrBackbone')->getTakingMedicationColumnName();
      // --- check if disease already exists ---------------------------------------------------------------------

      // todo: add more fields; only one brother, sister etc possible per disease!!!!
      // 16/6/2021 - added notes as one of the cnt criteria - e.g. 'other conditions' can be listed multiple times
      $query = "SELECT count(*)
                  from " . $table . "
                  where entity_id = %1
                  and " . $familyMemberColumn . " = %2
                  and " . $diseaseColumn . " = %3
                  and " . $diseaseNotesColumn . " = %4";
      $queryParams = [
        1 => [$contactID, "Integer"],
        2 => [$familyMember, "String"],
        3 => [$disease, "String"],
        4 => [$diseaseNotes, "String"],
      ];
      $cnt = CRM_Core_DAO::singleValueQuery($query, $queryParams);

      if ($cnt == 0) {
        // --- insert --------------------------------------------------------------------------------------------
        $query = "insert into " . $table . " (entity_id, " . $familyMemberColumn . ", " . $diseaseColumn
          . ", " . $diseaseNotesColumn;
        if ($diagnosisYear <> '') {
          $query .= ", " . $diagnosisYearColumn;
        }
        if ($diagnosisAge <> '') {
          $query .= ", " . $diagnosisAgeColumn;
        }
        if ($takingMedication <> '') {
          $query .= ", " . $takingMedicationColumn;
        }
        $query .= ") values (%1,%2,%3,%4";
        $i = 5;
        if ($diagnosisYear <> '') {
          $query .= ",%$i";
          $i++;
        }
        if ($diagnosisAge <> '') {
          $query .= ",%$i";
          $i++;
        }
        if ($takingMedication <> '') {
          $query .= ",%$i";
        }
        $query .= ")";

        $queryParams = [
          1 => [$contactID, "Integer"],
          2 => [$familyMember, "String"],
          3 => [$disease, "String"],
          4 => [$diseaseNotes, "String"]
        ];

        if ($diagnosisYear <> '') {
          $ref = [$diagnosisYear, "Integer"];
          array_push($queryParams, $ref);
        }
        if ($diagnosisAge <> '') {
          $ref = [$diagnosisAge, "Integer"];
          array_push($queryParams, $ref);
        }
        if ($takingMedication <> '') {
          $ref = [$takingMedication, "Integer"];
          array_push($queryParams, $ref);
        }
        CRM_Core_DAO::executeQuery($query, $queryParams);
      }
    }
  }

  private function addMedication($contactID, $starfishData, $drugFamily, $medicationName, $medicationDate)
  {
    // --- check if medication already exists ---------------------------------------------------------------------
    $query = "SELECT count(*)
                  from civicrm_value_nihr_volunteer_medication
                  where entity_id = %1
                  and nvm_starfish_data = %2
                  and nvm_medication_drug_family = %3
                  and nvm_medication_name = %4";
    $queryParams = [
      1 => [$contactID, "Integer"],
      2 => [$starfishData, "String"],
      3 => [$drugFamily, "String"],
      4 => [$medicationName, "String"],
    ];
    $cnt = CRM_Core_DAO::singleValueQuery($query, $queryParams);

    if ($cnt == 0) {
      // --- insert --------------------------------------------------------------------------------------------
      $query = "insert into civicrm_value_nihr_volunteer_medication (entity_id, nvm_starfish_data, nvm_medication_drug_family,
                    nvm_medication_name, nvm_medication_date)
            values (%1,%2,%3,%4,%5)";

      $queryParams = [
        1 => [$contactID, "Integer"],
        2 => [$starfishData, "String"],
        3 => [$drugFamily, "String"],
        4 => [$medicationName, "String"],
        5 => [$medicationDate, "Date"]
      ];
      CRM_Core_DAO::executeQuery($query, $queryParams);
    }
  }


  /**
   * Method to get the id of the panel
   *
   * @param $type
   * @param $name
   * @param $siteAliasType
   * @return false|int
   */
  public function getIdCentrePanelSite($type, $name, $siteAliasType = NULL)
  {
    $foundId = 0;

    if ($type == "site") {
      // site can be sic code or site alias - if site alias is provided, try this first
      if (isset($siteAliasType)) {
        $table = Civi::service('nbrBackbone')->getSiteAliasTableName();
        $siteAliasColumn = Civi::service('nbrBackbone')->getSiteAliasColumnName();
        $siteAliasTypeColumn = Civi::service('nbrBackbone')->getSiteAliasTypeColumnName();
        $query = "SELECT entity_id FROM " . $table . " WHERE " . $siteAliasColumn . " = %1 AND "
          . $siteAliasTypeColumn . " = %2 LIMIT 1";
        $queryParams = [
          1 => [$name, "String"],
          2 => [$siteAliasType, "String"],
        ];
        $foundId = CRM_Core_DAO::singleValueQuery($query, $queryParams);
      }
      if (!$foundId) {
        $query = "SELECT id FROM civicrm_contact WHERE contact_type = %1 AND sic_code = %2 AND contact_sub_type = %3";
        $queryParams = [
          1 => ["Organization", "String"],
          2 => [$name, "String"],
          3 => ['nbr_site', "String"],
        ];
        $foundId = CRM_Core_DAO::singleValueQuery($query, $queryParams);
      }
    } else {
      // panel or centre provided
      $query = "SELECT id FROM civicrm_contact WHERE contact_type = %1 AND organization_name = %2 AND contact_sub_type = %3";
      $queryParams = [
        1 => ["Organization", "String"],
        2 => [$name, "String"],
        3 => ['nbr_' . $type, "String"],
      ];
      $foundId = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    }

    if ($foundId) {
      return (int)$foundId;
    }

    // no panel/centre/site mapping found
    return FALSE;
  }

  /**
   * Method to add panel
   * @param $contactID
   * @param $panel
   * @param $site
   * @param $centre
   * @param $source
   */
  private function addPanel($contactID, $panel, $site, $centre, $source, $dataSource)
  {
    //
    // ---

    if ($panel == 'IBD Main' || $panel == 'IBD Inception') {
      $siteAliasTypeValue = "nbr_site_alias_type_ibd";
    } elseif ($panel == 'STRIDES') {
      $siteAliasTypeValue = "nbr_site_alias_type_strides";
    } elseif ($panel == 'PIBD'  || $panel == 'PIBD Inception') {
      $siteAliasTypeValue = "nbr_site_alias_type_pibd";
    }

    $panelData = [];
    // *** centre/panel/site: usually two of each are set per record, all of them are contact organisation
    // *** records; check if given values are on the - if any is missing do not insert any 'panel' data
    if (isset($panel) && !empty($panel)) {
      $panelID = $this->getIdCentrePanelSite('panel', $panel);
      if (!$panelID) {
        $this->_logger->logMessage('Panel does not exist on database: ' . $panel, 'ERROR');
        return;
      }
      $panelData['panel_id'] = $panelID;
    } else {
      $panelData['panel_id'] = '';
    }

    if (isset($centre) && !empty($centre)) {
      $centreID = $this->getIdCentrePanelSite('centre', $centre);
      if (!$centreID) {
        $this->_logger->logMessage('Centre does not exist on database: ' . $centre, 'ERROR');
        return;
      }
      $panelData['centre_id'] = $centreID;
    } else {
      $panelData['centre_id'] = '';
    }

    if (isset($site) && !empty($site)) {
      $siteID = $this->getIdCentrePanelSite('site', $site, $siteAliasTypeValue);
      if (!$siteID) {
        $this->_logger->logMessage('Site does not exist on database: ' . $site, 'ERROR');
        return;
      }
      $panelData['site_id'] = $siteID;
    } else {
      $panelData['site_id'] = '';
    }

    // --- check that data was provided
    $mandatories = ['centre_id', 'panel_id', 'site_id'];
    $countMandatory = 0;
    foreach ($mandatories as $mandatory) {
      if ($panelData[$mandatory] <> '') {
        $countMandatory++;
      }
    }
    if ($countMandatory < 2 &&
      // for rare migration data, only providing the panel without the site is sufficient
      // same for DCYPHR
      (($dataSource != 'rare_migration' && $dataSource != 'cyp')
        || $panelData['panel_id'] == '')) {
      $this->_logger->logMessage('No panel information provided for : ' . $contactID, 'ERROR');
      return;
    }
    $panelData['contact_id'] = $contactID;
    // --- check if panel/site/centre combination is already linked to volunteer ------------------
    if (!$this->hasPanelSiteCentre($panelData)) {
      // *** add source, if given
      if (isset($source) && !empty($source)) {
        $panelData['source'] = $source;
      } else {
        $panelData['source'] = '';
      }

      $this->insertPanel($panelData);
    }
  }

  /**
   * Method to insert a panel record
   *
   * @param $panelData
   */
  private function insertPanel($panelData)
  {
    $table = Civi::service('nbrBackbone')->getVolunteerPanelTableName();
    $centreColumn = Civi::service('nbrBackbone')->getVolunteerCentreColumnName();
    $panelColumn = Civi::service('nbrBackbone')->getVolunteerPanelColumnName();
    $siteColumn = Civi::service('nbrBackbone')->getVolunteerSiteColumnName();
    $sourceColumn = Civi::service('nbrBackbone')->getVolunteerSourceColumnName();

    // check if panel already exists
    $query = "select count(*)
                from " . $table . "
                where entity_id = %1
                and if (%2 = '', " . $panelColumn . " is null,  " . $panelColumn . " = %2)
                and if (%3 = '', " . $centreColumn . " is null,  " . $centreColumn . " = %3)
                and if (%4 = '', " . $siteColumn . " is null, " . $siteColumn . " = %4)";

    $queryParams = [
      1 => [(int)$panelData['contact_id'], "Integer"],
      2 => [(int)$panelData['panel_id'], "Integer"],
      3 => [(int)$panelData['centre_id'], "Integer"],
      4 => [(int)$panelData['site_id'], "Integer"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count == 0) {

      // +++
      $queryParams = [
        1 => [$panelData['contact_id'], "Integer"]
      ];

      $query = "INSERT INTO " . $table . " (entity_id ";
      $query2 = "values (%1";
      $i = 2;

      if ($panelData['centre_id'] <> '') {
        $query = $query . ", " . $centreColumn;
        $query2 .= ",%$i";
        $ref = [$panelData['centre_id'], "Integer"];
        array_push($queryParams, $ref);
        $i++;
      }
      if ($panelData['panel_id'] <> '') {
        $query = $query . ", " . $panelColumn;
        $query2 .= ",%$i";
        $ref = [$panelData['panel_id'], "Integer"];
        array_push($queryParams, $ref);
        $i++;
      }
      if ($panelData['site_id'] <> '') {
        $query = $query . ", " . $siteColumn;
        $query2 .= ",%$i";
        $ref = [$panelData['site_id'], "Integer"];
        array_push($queryParams, $ref);
        $i++;
      }
      if ($panelData['source'] <> '') {
        $query = $query . ", " . $sourceColumn;
        $query2 .= ",%$i";
        $ref = [$panelData['source'], "String"];
        array_push($queryParams, $ref);
      }

      $query = $query . ") " . $query2 . ")";
      CRM_Core_DAO::executeQuery($query, $queryParams);
    }
  }

  /**
   * Method to check if volunteer already has panel, centre, site
   *
   * @param $panelData
   * @return bool
   */
  private function hasPanelSiteCentre($panelData)
  {
    $table = Civi::service('nbrBackbone')->getVolunteerPanelTableName();
    $centreColumn = Civi::service('nbrBackbone')->getVolunteerCentreColumnName();
    $panelColumn = Civi::service('nbrBackbone')->getVolunteerPanelColumnName();
    $siteColumn = Civi::service('nbrBackbone')->getVolunteerSiteColumnName();
    $query = "SELECT COUNT(*) FROM " . $table . " WHERE entity_id = %1 AND
      " . $centreColumn . " = %2 AND " . $panelColumn . " = %3 AND " . $siteColumn . " = %4";
    $queryParams = [
      1 => [(int)$panelData['contact_id'], "Integer"],
      2 => [(int)$panelData['centre_id'], "Integer"],
      3 => [(int)$panelData['panel_id'], "Integer"],
      4 => [(int)$panelData['site_id'], "Integer"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count > 0) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * Method to add activity
   *
   * @param $contactId
   * @param $activityType
   * @param $dateTime
   */
  private function addActivity($contactId, $activityType, $dateTime, $subject, $status, $location)
  {
    // only enter if not already on the volunteer record
    $params = [
      "activity_type_id" => $activityType,
      "target_contact_id" => $contactId,
    ];
    if (isset($dateTime) && $dateTime <> '') {
      $params['activity_date_time'] = $dateTime;
    }

    try {
      $cnt = civicrm_api3('Activity', 'getcount', $params);
    } catch (CiviCRM_API3_Exception $ex) {
      $this->_logger->logMessage('checking on $activityType activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
    }

    if ($cnt == 0) {
      try {
        civicrm_api3('Activity', 'create', [
          'activity_type_id' => $activityType,
          'activity_date_time' => $dateTime,
          'target_id' => $contactId,
          'subject' => $subject,
          'status_id' => $status,
          'location' => $location
        ]);
      } catch (CiviCRM_API3_Exception $ex) {
        $this->_logger->logMessage('inserting ' . $activityType . ' activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
      }
    }
  }

  private function addTag($contactId, $tagName)
  {
    // add tag if not already exist
    $cnt = civicrm_api3('EntityTag', 'getcount', [
      'tag_id' => $tagName,
      'contact_id' => $contactId,
    ]);
    if ($cnt == 0) {
      try {
        civicrm_api3('EntityTag', 'create', [
          'tag_id' => $tagName,
          'contact_id' => $contactId,
        ]);
      } catch (CiviCRM_API3_Exception $ex) {
        $this->_logger->logMessage('adding tag ' . $tagName . ' to volunteer record ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
      }
    }
  }

  private function removeTag($contactId, $tagName)
  {
    try {
      // remove tag if exists
      $result = civicrm_api3('EntityTag', 'get', [
        'sequential' => 1,
        'tag_id' => $tagName,
        'contact_id' => $contactId,
      ]);

      if (isset($result['id'])) {
        civicrm_api3('EntityTag', 'delete', [
          'id' => $result['id'],
          'contact_id' => $contactId,
        ]);
      }

      // remove non-recallable reason
      $update = "UPDATE civicrm_value_nihr_volunteer_status set nvs_nonrecallable_reason  = null where entity_id = %1";
      CRM_Core_DAO::executeQuery($update, [
        1 => [(int)$contactId, "Integer"],
      ]);

    } catch (CiviCRM_API3_Exception $ex) {
      $this->_logger->logMessage('deleting tag ' . $tagName . ' on volunteer record ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
    }
  }

  /**
   * Method to add an activity to the recruitment case
   *
   * @param $contactId
   * @param $activityType
   * @param $dateTime
   * @param null $caseId
   */
  private function addRecruitmentCaseActivity($contactId, $activityType, $dateTime, $subject, $status, $location, $caseId = NULL)
  {
    // TODO - check dateTime param has got correct format

    // get latest recruitment case for contact, if not provided
    if (is_null($caseId)) {
      $caseId = CRM_Nihrbackbone_NbrRecruitmentCase::getActiveRecruitmentCaseId($contactId);
    }
    $params = [
      "activity_type_id" => $activityType,
      "target_contact_id" => $contactId,
      "case_id" => $caseId,
    ];
    if (isset($dateTime) && $dateTime <> '') {
      $params['activity_date_time'] = $dateTime;
    }

    // only enter if not already on the case
    try {
      $cnt = civicrm_api3('Activity', 'getcount', $params);
    } catch (CiviCRM_API3_Exception $ex) {
      $this->_logger->logMessage('checking on $activityType activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
    }

    if ($cnt == 0) {
      try {
        civicrm_api3('Activity', 'create', [
          'activity_type_id' => $activityType,
          'activity_date_time' => $dateTime,
          'target_id' => $contactId,
          'case_id' => $caseId,
          'status_id' => $status,
          'subject' => $subject,
          'location' => $location
        ]);
      } catch (CiviCRM_API3_Exception $ex) {
        $this->_logger->logMessage('inserting $activityType activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
      }
    }
  }

  /**
   * Method to create the recruitment case
   *
   * @param $contactId
   * @return mixed|string
   * @throws Exception
   */
  private function createRecruitmentCase($contactId, $consent_date = NULL)
  {
    $caseId = '';

    // check if recruitment case already exists
    $params = [
      1 => [Civi::service('nbrBackbone')->getRecruitmentCaseTypeName(), 'String'],
      2 => [$contactId, 'Integer'],
    ];
    $sql = "select cc.case_id
            from civicrm_case_contact cc, civicrm_case cas, civicrm_case_type cct
            where cc.case_id = cas.id
            and cas.case_type_id  = cct.id
            and cct.name = %1
            and contact_id = %2";

    try {
      $caseId = CRM_Core_DAO::singleValueQuery($sql, $params);
    } catch (Exception $ex) {
    }

    if (!isset($caseId)) {
      // create recruitment case
      try {
        $result = civicrm_api3('NbrVolunteerCase', 'create', [
          'contact_id' => $contactId,
          'case_type' => 'recruitment',
          'subject' => "Recruitment",
          'start_date' => $consent_date
        ]);
        $caseId = $result['case_id'];
        $message = E::ts('Recruitment case for volunteer ' . $contactId . '  added');
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName);
      } catch (CiviCRM_API3_Exception $ex) {
        $message = E::ts('Error when creating recruitment case for volunteer ') . $contactId
          . E::ts(' from API NbrVolunteerCase create : ') . $ex->getMessage();
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'error');
      }
    }
    return $caseId;
  }

  /**
   * Method to withdraw volunteer
   *
   * @param $contactId
   * @param $sourceDate
   * @param $sourceReason
   * @param $sourceBy
   * @param string $sourceDestroyData
   * @param string $sourceDestroySamples
   * @throws Exception
   */
  private function withdrawVolunteer($contactId, $data, $dataSource)
  {
    // *** Withdraw volunteer from the BioResource

    // only add information if not already withdrawn
    try {
      $cnt = civicrm_api3('Activity', 'getcount', [
        'activity_type_id' => Civi::service('nbrBackbone')->getWithdrawnActivityTypeId(),
        'target_contact_id' => $contactId,
      ]);

    } catch (CiviCRM_API3_Exception $ex) {
      $this->_logger->logMessage('checking on withdrawn activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
    }

    if ($cnt == 0) {

      $activity = new CRM_Nihrbackbone_NbrActivity();
      $activityParams = ['target_contact_id' => $contactId];

      $activityParams['activity_type_id'] = Civi::service('nbrBackbone')->getWithdrawnActivityTypeId();
      $reasonCustomField = "custom_" . Civi::service('nbrBackbone')->getWithdrawnReasonCustomFieldId();
      $destroyDataCustomField = "custom_" . Civi::service('nbrBackbone')->getWithdrawnDestroyDataCustomFieldId();
      $destroySamplesCustomField = "custom_" . Civi::service('nbrBackbone')->getWithdrawnDestroySamplesCustomFieldId();
      if (!empty($data['withdrawn_reason'])) {
        $activityParams[$reasonCustomField] = $activity->findOrCreateStatusReasonValue("withdrawn", $data['withdrawn_reason']);
      }
      if ($data['request_to_destroy_data'] == '') {
        $message = "No request to destroy data flag found for volunteer " . $contactId . ", assumed FALSE.";
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'warning');
      }
      if ($data['request_to_destroy_samples'] == '') {
        $message = "No request to destroy flag found for volunteer " . $contactId . ", assumed FALSE.";
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'warning');
      }
      $activityParams[$destroyDataCustomField] = 0;
      $activityParams[$destroySamplesCustomField] = 0;
      if ($data['request_to_destroy_data'] == TRUE) {
        $activityParams[$destroyDataCustomField] = 1;
      }
      if ($data['request_to_destroy_samples'] == TRUE) {
        $activityParams[$destroySamplesCustomField] = 1;
      }

      $activityParams['activity_date_time'] = $this->formatActivityDate($data['withdrawn_date']);

      if (!empty($data['withdrawn_by'])) {
        $resourcer = new CRM_Nihrbackbone_NbrResourcer();
        $sourceContactId = $resourcer->findWithName($data['withdrawn_by']);
        if ($sourceContactId) {
          $activityParams['source_contact_id'] = $sourceContactId;
        } else {
          $activityParams['details'] = "Withdrawn by: " . $data['withdrawn_by'];
        }
      }
      $activityParams['status_id'] = 'Completed';

      // STRIDES - status is 'scheduled', date +1 week, and additional custom fields
      if ($dataSource == 'strides') {
        $activityParams['status_id'] = 'Scheduled';
        $activityParams['activity_date_time'] = $this->currentDatePlus(7);
      }
      $addFields = array('withdrawal_request_date', 'withdrawal_confirmation_date',
        'withdrawal_initiated_by', 'withdrawal_authorising_team');
      foreach ($addFields as &$field) {
        if (isset($data[$field]) && !empty($data[$field])) {
          $newKey = $this->getCustomFieldKey("avw_$field");
          if ($newKey != '') {
            $activityParams[$newKey] = $data[$field];
          }
        }
      }

      if (!empty($activityParams)) {
        $activityParams['priority_id'] = Civi::service('nbrBackbone')->getNormalPriorityId();
        $activityParams['subject'] = 'Withdrawn';
        $new = $activity->createActivity($activityParams);
        if ($new != TRUE) {
          CRM_Nihrbackbone_Utils::logMessage($this->_importId, $new, $this->_originalFileName, 'warning');
        }
        // set volunteer status to withdrawn (exeption: STRIDES)
        if ($dataSource != 'strides') {
          $this->setVolunteerStatus($contactId, Civi::service('nbrBackbone')->getWithdrawnVolunteerStatus());
        }
      }
    }
  }

  private function processRedundant($contactId, $data)
  {
    // *** make volunteer redundant

    // only add information if not already done so
    try {
      $cnt = civicrm_api3('Activity', 'getcount', [
        'activity_type_id' => Civi::service('nbrBackbone')->getRedundantActivityTypeId(),
        'target_contact_id' => $contactId,
      ]);

    } catch (CiviCRM_API3_Exception $ex) {
      $this->_logger->logMessage('checking on redundant activity for volunteer ' . $contactId . ': ' . $ex->getMessage(), 'ERROR');
    }

    if ($cnt == 0) {

      $activity = new CRM_Nihrbackbone_NbrActivity();
      $activityParams = ['target_contact_id' => $contactId];

      $activityParams['activity_type_id'] = Civi::service('nbrBackbone')->getRedundantActivityTypeId();
      $reasonCustomField = "custom_" . Civi::service('nbrBackbone')->getRedundantReasonCustomFieldId();
      $redundantDestroyDataCustomField = "custom_" . Civi::service('nbrBackbone')->getRedundantDestroyDataCustomFieldId();
      $redundantDestroySamplesCustomField = "custom_" . Civi::service('nbrBackbone')->getRedundantDestroySamplesCustomFieldId();
      if (!empty($data['redundant_reason'])) {
        $activityParams[$reasonCustomField] = $activity->findOrCreateStatusReasonValue("redundant", $data['redundant_reason']);
      }

      $activityParams[$redundantDestroyDataCustomField] = null;
      $activityParams[$redundantDestroySamplesCustomField] = null;
      if ($data['redundant_request_to_destroy_data'] == TRUE) {
        $activityParams[$redundantDestroyDataCustomField] = 1;
      }
      if ($data['redundant_request_to_destroy_samples'] == TRUE) {
        $activityParams[$redundantDestroySamplesCustomField] = 1;
      }

      $activityParams['activity_date_time'] = $this->formatActivityDate($data['redundant_date']);

      if (!empty($data['redundant_by'])) {
        $resourcer = new CRM_Nihrbackbone_NbrResourcer();
        $sourceContactId = $resourcer->findWithName($data['redundant_by']);
        if ($sourceContactId) {
          $activityParams['source_contact_id'] = $sourceContactId;
        } else {
          $activityParams['details'] = "Redundant by: " . $data['redundant_by'];
        }
      }

      $activityParams['status_id'] = 'Completed';

      $activityParams['priority_id'] = Civi::service('nbrBackbone')->getNormalPriorityId();
      $activityParams['subject'] = 'Redundant';
      $new = $activity->createActivity($activityParams);
      if ($new != TRUE) {
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $new, $this->_originalFileName, 'warning');
      }
      // set volunteer status to redundant
      $this->setVolunteerStatus($contactId, Civi::service('nbrBackbone')->getRedundantVolunteerStatus());
    }
  }


  /**
   * Method to set a volunteer to deceased
   * - tick CiviCRM deceased box and set date if not empty
   * - set volunteer status deceased
   *
   * @param $volunteerId
   * @param $deceasedDate
   * @return bool
   * @throws Exception
   */
  private function processDeceased($volunteerId, $deceasedDate): bool
  {
    if (empty($volunteerId)) {
      return FALSE;
    }

    // tick the deceased box in CiviCRM and set deceased date if applicable
    if (!$deceasedDate instanceof DateTime && !empty($deceasedDate)) {
      $deceasedDate = new DateTime($deceasedDate);
      $query = "UPDATE civicrm_contact SET is_deceased = %1, deceased_date = %2 WHERE id = %3";
      $queryParams = [
        1 => [1, "Integer"],
        2 => [$deceasedDate->format("Y-m-d"), "String"],
        3 => [(int)$volunteerId, "Integer"],
      ];
    } else {
      // no deceased date provided
      $query = "UPDATE civicrm_contact SET is_deceased = %1 WHERE id = %2";
      $queryParams = [
        1 => [1, "Integer"],
        2 => [(int)$volunteerId, "Integer"],
      ];
    }
    CRM_Core_DAO::executeQuery($query, $queryParams);
    return TRUE;
  }

  /**
   * Method to format the activity date time
   *
   * @param $sourceDate
   * @return string
   * @throws Exception
   */
  private function formatActivityDate($sourceDate)
  {
    $activityDate = new DateTime();
    if (!empty($sourceDate)) {
      try {
        $activityDate = new DateTime($sourceDate);
      } catch (Exception $ex) {
        $message = "Could not transfer date " . $sourceDate . " to a valid DateTime in " . __METHOD__ . ", defaulted to today";
        CRM_Nihrbackbone_Utils::logMessage($this->_importId, $message, $this->_originalFileName, 'warning');
      }
    }
    return $activityDate->format("Y-m-d");
  }

  private function currentDatePlus($addDays)
  {
    // return current date plus $addDays days, time set to 08:00
    try {
      return CRM_Core_DAO::singleValueQuery("
        SELECT concat(date_add(curdate(), interval $addDays day), ' 08:00')");
    } catch (Exception $ex) {
    }
  }

  /**
   * Method to set the volunteer status of a volunteer
   *
   * @param $volunteerId
   * @param $sourceStatus
   * @return bool
   */
  private function setVolunteerStatus($volunteerId, $sourceStatus)
  {
    $sourceStatus = strtolower($sourceStatus);
    // first check if status exists, use pending if not
    $query = "SELECT COUNT(*) FROM civicrm_option_value WHERE option_group_id = %1 AND value = %2";
    $queryParams = [
      1 => [Civi::service('nbrBackbone')->getVolunteerStatusOptionGroupId(), "Integer"],
      2 => [$sourceStatus, "String"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count == 0) {
      $sourceStatus = Civi::service('nbrBackbone')->getPendingVolunteerStatus();
    }
    $update = "UPDATE " . Civi::service('nbrBackbone')->getVolunteerStatusTableName() . " SET "
      . Civi::service('nbrBackbone')->getVolunteerStatusColumnName() . " = %1 WHERE entity_id = %2";
    $updateParams = [
      1 => [$sourceStatus, "String"],
      2 => [(int)$volunteerId, "Integer"],
    ];
    CRM_Core_DAO::executeQuery($update, $updateParams);
    return TRUE;
  }

  private function checkDOB($id, $dob, $consent_date, $dataSource)
  {

    if ($dob <> '') {
      // return NULL if age is out of given range
      try {
        $query = "SELECT timestampdiff(year, %1, curdate())";
        $queryParams = [
          1 => [$dob, "String"],
        ];
        $age = CRM_Core_DAO::singleValueQuery($query, $queryParams);
        if ($dataSource == 'glad' || $dataSource == 'edgi') {
          if ($age < 16 || $age > 100) {
            $this->_logger->logMessage("$id DOB incorrect, not stored: $dob", 'WARNING');
            $dob = '';
          }
        } else {
          // other consents allow recruitment of children and babies (e.g PIBD, CYP)
          if ($age > 110 || ($consent_date <> '' and $consent_date < $dob)) {
            $this->_logger->logMessage("$id DOB incorrect, not stored: $dob", 'WARNING');
            $dob = '';
          }
        }
      } catch (Exception $ex) {
        $this->_logger->logMessage("Error $id calculating age: " . $ex->getMessage(), 'ERROR');
      }
    }
    return $dob;
  }


  private function checkFormerSurname($id, $name)
  {
    // check if given surname is already saved as 'former surname' for the volunteer

    try {
      $query = "
        SELECT count(*) as cnt
        FROM civicrm_value_contact_id_history
        where entity_id = %1
        and identifier_type = 'cih_type_former_surname'
        and identifier = %2";
      $queryParams = [
        1 => [$id, "Integer"],
        2 => [$name, "String"],
      ];

      return CRM_Core_DAO::singleValueQuery($query, $queryParams);
    } catch (Exception $ex) {
      $this->_logger->logMessage('$id retrieving former surname: ' . $ex->getMessage(), 'ERROR');
    }
  }

  public function getCustomFieldKey($fieldName)
  {
    try {
      $result = civicrm_api3('CustomField', 'get', [
        'sequential' => 1,
        'name' => $fieldName,
      ]);
    } catch (Exception $ex) {
      $this->_logger->logMessage("retrieving custom field key for $fieldName " . $ex->getMessage(), 'ERROR');
      return '';
    }

    if (!isset($result['id']) || $result['id'] == '') {
      $this->_logger->logMessage("INVALID CUSTOM FIELD $fieldName ", 'ERROR');
      return '';
    } else {
      return "custom_" . $result['id'];
    }
  }

  public function findGuardian($data, $logger)
  {
    $id = '';
    $cnt = 0;

    // Guardian data usually does not contain DOB but email is mandatory
    // check on subtype guardian only - even if the person is registered as a volunteer, create a new record

    if (isset($data['first_name']) && $data['first_name'] <> '' &&
      isset($data['last_name']) && $data['last_name'] <> '' &&
      isset($data['email']) && $data['email'] <> '') {
      $sql = "
          select count(*) as cnt, c.id as id
          from civicrm_contact c, civicrm_email e
          where c.contact_type = 'Individual'
          and c.contact_sub_type = 'nbr_guardian'
          and c.first_name = %1
          and c.last_name = %2
          and c.id = e.contact_id
          and e.email = %3";

      $queryParams = [
        1 => [$data['first_name'], 'String'],
        2 => [$data['last_name'], 'String'],
        3 => [$data['email'], 'String']
      ];

      try {
        CRM_Core_DAO::disableFullGroupByMode();
        $xdata = CRM_Core_DAO::executeQuery($sql, $queryParams);
        CRM_Core_DAO::reenableFullGroupByMode();
        if ($xdata->fetch()) {
          $count = $xdata->cnt;
          $id = $xdata->id;
        }
      } catch (Exception $ex) {
        $logger->logMessage('Select FindGuardian (email) failed ' . $data['first_name'] . ' ' . $data['first_name']);
      }
    }

    if ($cnt == 0 &&
        isset($data['first_name']) && $data['first_name'] <> '' &&
        isset($data['last_name']) && $data['last_name'] <> '' &&
        isset($data['dob']) && $data['dob'] <> '') {
      $sql = "
        select count(*) as cnt, c.id as id
        from civicrm_contact c
        where c.contact_type = 'Individual'
        and c.contact_sub_type = 'nbr_guardian'
        and c.first_name = %1
        and c.last_name = %2
        and c.birth_date = %3";

      $queryParams = [
        1 => [$data['first_name'], 'String'],
        2 => [$data['last_name'], 'String'],
        3 => [$data['dob'], 'String']
      ];

      try {
        CRM_Core_DAO::disableFullGroupByMode();
        $xdata = CRM_Core_DAO::executeQuery($sql, $queryParams);
        CRM_Core_DAO::reenableFullGroupByMode();
        if ($xdata->fetch()) {
          $count = $xdata->cnt;
          $id = $xdata->id;
        }
      } catch (Exception $ex) {
        $logger->logMessage('Select FindGuardian (dob) failed ' . $data['first_name'] . ' ' . $data['first_name']);
      }
    }

    // cnt = 1 -> ID unique for this volunteer
    if ($count == 0) {
      $id = ''; // just in case
    } elseif ($count > 1) {
      // there are already duplicated records of the volunteer - use one of these but give warning
      $logger->logMessage('Multiple records linked to identifier ' . $data['first_name'] . ' ' . $data['last_name'] . ', used first one (' . $id . ')');
    }
    return $id;
  }

    public function addRelationship($contactId, $contact2, $relationshipType)
  {
    // test if second contact is on orca and retrieve the contact ID
    $sql = "select count(*) as cnt, c.id as id2
            from civicrm_contact c, civicrm_value_contact_id_history h
            where c.contact_type = 'Individual'
            and c.id = h.entity_id
            and h.identifier = %1";

    /* if ($relationshipType == 'nbr_guardian_of') {
      $sql .= " and c.contact_sub_type = 'nbr_guardian'";
    } */

    $queryParams = [
      1 => [$contact2, 'String'],
    ];

    try {
      CRM_Core_DAO::disableFullGroupByMode();
      $xdata = CRM_Core_DAO::executeQuery($sql, $queryParams);
      CRM_Core_DAO::reenableFullGroupByMode();
      if ($xdata->fetch()) {
        $cnt = $xdata->cnt;
        $id2 = $xdata->id2;
      }
    } catch (Exception $ex) {
      $this->_logger->logMessage("$contact2 retrieving ID to create relationship failed: " . $ex->getMessage(), 'ERROR');
    }

    if ($cnt == 0) {
      $this->_logger->logMessage("$contact2 does not exist on orca, could not create relationship", 'ERROR');
    } elseif ($cnt > 1) {
      $this->_logger->logMessage("$contact2 is duplicated on orca, could not create relationship", 'ERROR');
    } else {
      try {
        $sql = "select id from civicrm_relationship_type where name_b_a = '$relationshipType'";
        $relTypeId = CRM_Core_DAO::singleValueQuery($sql);

        // NOTE: this creates the relationship 'backwards', using name_B_A (rather than name_a_b)
        // rewrite if used for any other relationships
        civicrm_api3('Relationship', 'create', [
          'contact_id_a' => $id2,
          'contact_id_b' => $contactId,
          'relationship_type_id' => $relTypeId,
        ]);
      } catch (CiviCRM_API3_Exception $ex) {
        // not checked if relationship is already in place...
        if($ex->getMessage() <> 'Duplicate Relationship') {
          $this->_logger->logMessage("$contactId failed to create relationship: " . $ex->getMessage(), 'ERROR');
        }
      }
    }
  }
}
