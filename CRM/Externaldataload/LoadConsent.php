<?php
use CRM_Externaldataload_ExtensionUtil as E;

/**
 * Class for loading consents
 *
 * @author Carola Kanz
 * @date 11/02/2020
 * @license AGPL-3.0
 */
class CRM_Externaldataload_LoadConsent
{
  /**
   * Method to add consent
   *
   * @param $contactId
   * @param $caseID
   * @param $consentStatus
   * @param $subject
   * @param $data
   * @param $logger
   * @throws Exception
   */
  public function addConsent($contactId, $caseID, $consentStatus, $subject, $data, $logger)
  {
    // caseID cannot be empty, a consent is always linked to a case
    if ($caseID == '') {
      Civi::log()->error("Case ID is missing in " . __METHOD__);
    } else {
      $existingLeafletVersion = $this->isExistingLeafletVersion($data['information_leaflet_version']);
      $existingConsentVersion = $this->isExistingConsentVersion($data['consent_version']);
      if (!$existingLeafletVersion && isset($data['information_leaflet_version']) && $data['information_leaflet_version'] <> '') {
        $logger->logMessage( $contactId . ' leaflet version '
          . $data['information_leaflet_version'] . ' does not exist.' ,'warning');
        $data['information_leaflet_version'] = '';
      }
      if (!$existingConsentVersion) {
        $logger->logMessage('Could not add consent for contact ID ' . $contactId . ' because consent version '
          . $data['consent_version'] . ' does not exist.' ,'error');
      }
      if ($existingConsentVersion) {
        $consentDate = date('Y-m-d', strtotime($data['consent_date']));
        if ($this->countExistingConsent($contactId, $consentDate, $data['information_leaflet_version'], $data['consent_version']) == 0) {
          $consentVersion = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_consent_version', 'id');
          $informationLeafletVersion = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_information_leaflet_version', 'id');
          $consentStatusField = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_consent_status', 'id');
          $consentedByField = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_consented_by', 'id');
          $geneticFeedback = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_genetic_feedback', 'id');
          $inviteType = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getVolunteerConsentCustomField('nvc_invite_type', 'id');
          // consent not yet on Civi - add
          // *** check if 'consented by' has got a 'BioResourcer' record; if not, add name to details
          $details = '';
          $consentedBy = '';

          $names = explode(' ', $data['consented_by']);
          if ($names[0] <> '' && isset($names[1])) {
            $consentedById = Civi::service('nbrBackbone')->getGroupMemberContactIdWithName(Civi::service('nbrBackbone')->getBioResourcersGroupId(), $names[0], $names[1]);
            if ($consentedById) {
              $consentedBy = $consentedById;
            }
            else {
              $details = 'consented by ' . $data['consented_by'];
            }
          }

          // **** --- add consent to case
          $consentDate = new DateTime($data['consent_date']);
          try {
            $result2 = civicrm_api3('Activity', 'create', [
              'source_contact_id' => "user_contact_id",
              'target_id' => $contactId,
              'activity_type_id' => "nihr_consent",
              'status_id' => "Completed",
              $consentVersion => $data['consent_version'],
              $informationLeafletVersion => $data['information_leaflet_version'],
              'activity_date_time' => $consentDate->format('Y-m-d'),
              $consentStatusField => $consentStatus,
              'case_id' => (int)$caseID,
              $consentedByField => $consentedBy,
              'details' => $details,
              $geneticFeedback => $data['genetic_feedback'],
              $inviteType => $data['invite_type'],
              'subject' => $subject,
            ]);
          } catch (CiviCRM_API3_Exception $ex) {
            $logger->logMessage('Error message when adding volunteer consent ' . $contactId . ' ' . $ex->getMessage(), 'error');
          }
        }
      }
    }
  }

  /**
   * Method to check if information leaflet version exists
   *
   * @param $leafletVersion
   * @return bool
   */
  public function isExistingLeafletVersion($leafletVersion) {
    $query = "SELECT COUNT(*) FROM civicrm_option_value WHERE option_group_id = %1 AND value = %2";
    $queryParams = [
      1 => [Civi::service('nbrBackbone')->getLeafletVersionOptionGroupId(), "Integer"],
      2 => [$leafletVersion, "String"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count > 0) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * Method to check if consent version exists
   *
   * @param $consentVersion
   * @return bool
   */
  public function isExistingConsentVersion($consentVersion) {
    $query = "SELECT COUNT(*) FROM civicrm_option_value WHERE option_group_id = %1 AND value = %2";
    $queryParams = [
      1 => [Civi::service('nbrBackbone')->getConsentVersionOptionGroupId(), "Integer"],
      2 => [$consentVersion, "String"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count > 0) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * Count existing consents for contact, date, consent version and leaflet version
   *
   * @param $contactId
   * @param $consentDate
   * @param $leafletVersion
   * @param $consentVersion
   * @return string|null
   */
  public function countExistingConsent($contactId, $consentDate, $leafletVersion, $consentVersion) {
    $tableName = Civi::service('nbrBackbone')->getConsentTableName();
    $consentVersionColumn = Civi::service('nbrBackbone')->getConsentVersionColumnName();
    $leafletVersionColumn = Civi::service('nbrBackbone')->getLeafletVersionColumnName();
    $countQuery = "SELECT COUNT(*)
            FROM civicrm_activity AS a
                JOIN civicrm_activity_contact AS b ON a.id = b.activity_id AND b.record_type_id = %1
                LEFT JOIN " . $tableName . " AS c ON a.id = c.entity_id
            WHERE a.is_deleted = %2 AND a.is_current_revision = %3 AND a.is_test = %2
              AND a.activity_type_id = %4 AND a.activity_date_time LIKE %5 AND b.contact_id = %6
              AND c." . $leafletVersionColumn . " = %7 AND c." . $consentVersionColumn ." = %8";
    $countParams = [
      1 => [Civi::service('nbrBackbone')->getTargetRecordTypeId(), "Integer"],
      2 => [0, "Integer"],
      3 => [1, "Integer"],
      4 => [Civi::service('nbrBackbone')->getConsentActivityTypeId(), "Integer"],
      5 => [$consentDate . "%", "String"],
      6 => [(int) $contactId, "Integer"],
      7 => [$leafletVersion, "String"],
      8 => [$consentVersion, "String"],
    ];
    return CRM_Core_DAO::singleValueQuery($countQuery, $countParams);
  }

}
