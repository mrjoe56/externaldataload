<?php
use CRM_Externaldataload_ExtensionUtil as E;

/**
 * Class for loading assents
 *
 * @author Carola Kanz
 * @date 17/03/2023
 * @license AGPL-3.0
 */
class CRM_Externaldataload_LoadAssent
{
  /**
   * Method to add assent
   *
   * @param $contactId
   * @param $caseID
   * @param $subject
   * @param $data
   * @param $logger
   * @throws Exception
   */
  public function addAssent($contactId, $caseID, $subject, $data, $logger)
  {
    // caseID cannot be empty, a assent is always linked to a (recruitment) case
    if ($caseID == '') {
      Civi::log()->error("Case ID is missing in " . __METHOD__);
    } else {
      $existingAssentVersion = $this->isExistingAssentVersion($data['assent_version']);
      $existingAssentPisVersion = $this->isExistingAssentPisVersion($data['assent_pis_version']);
      if (!$existingAssentPisVersion && isset($data['assent_pis_version']) && $data['assent_pis_version'] <> '') {
        $logger->logMessage( $contactId . ' assent pis version '
          . $data['assent_pis_version'] . ' does not exist.' ,'warning');
        $data['assent_pis_version'] = '';
      }
      if (!$existingAssentVersion) {
        $logger->logMessage('Could not add assent for contact ID ' . $contactId . ' because assent version '
          . $data['assent_version'] . ' does not exist.' ,'error');
      }
      else {
        $assentDate = date('Y-m-d', strtotime($data['assent_date']));
        if ($this->countExistingAssent($contactId, $assentDate, $data['assent_pis_version'], $data['assent_version']) == 0) {
          $assentVersion = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getNbrAssentDataCustomField('nbr_assent_version', 'id');
          $assentPisVersion = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getNbrAssentDataCustomField('nbr_assent_pis_version', 'id');
          $assentStatus = 'custom_' . CRM_Nihrbackbone_BackboneConfig::singleton()->getNbrAssentDataCustomField('nbr_assent_status', 'id');

          // assent not yet on Civi - add assent to case
          $assentDate = new DateTime($data['assent_date']);
          try {
            $result2 = civicrm_api3('Activity', 'create', [
              'source_contact_id' => "user_contact_id",
              'target_id' => $contactId,
              'activity_type_id' => "nbr_assent",
              'status_id' => "Completed",
              $assentVersion => $data['assent_version'],
              $assentPisVersion => $data['assent_pis_version'],
              'activity_date_time' => $assentDate->format('Y-m-d'),
              $assentStatus => $data['assent_status'],
              'case_id' => (int)$caseID,
              'subject' => $subject,
            ]);
          } catch (CiviCRM_API3_Exception $ex) {
            $logger->logMessage('Error message when adding volunteer assent ' . $contactId . ' ' . $ex->getMessage(), 'error');
          }
        }
      }
    }
  }

  /**
   * Method to check if information leaflet version exists
   *
   * @param $assentPisVersion
   * @return bool
   */
  public function isExistingAssentPisVersion($assentPisVersion) {
    $query = "SELECT COUNT(*) FROM civicrm_option_value WHERE option_group_id = %1 AND value = %2";
    $queryParams = [
      1 => [Civi::service('nbrBackbone')->getAssentPisVersionOptionGroupId(), "Integer"],
      2 => [$assentPisVersion, "String"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count > 0) {
      return TRUE;
    }
    return FALSE;
  }
  /**

   * Method to check if assent version exists
   *
   * @param $assentVersion
   * @return bool
   */
  public function isExistingAssentVersion($assentVersion) {
    $query = "SELECT COUNT(*) FROM civicrm_option_value WHERE option_group_id = %1 AND value = %2";
    $queryParams = [
      1 => [Civi::service('nbrBackbone')->getAssentVersionOptionGroupId(), "Integer"],
      2 => [$assentVersion, "String"],
    ];
    $count = CRM_Core_DAO::singleValueQuery($query, $queryParams);
    if ($count > 0) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * Count existing assents for contact, date, assent version and assent pis version
   *
   * @param $contactId
   * @param $assentDate
   * @param $assentPisVersion
   * @param $assentVersion
   * @return string|null
   */
  public function countExistingAssent($contactId, $assentDate, $assentPisVersion, $assentVersion) {
    $tableName = Civi::service('nbrBackbone')->getAssentTableName();
    $assentVersionColumn = Civi::service('nbrBackbone')->getAssentVersionColumnName();
    $assentPisVersionColumn = Civi::service('nbrBackbone')->getAssentPisVersionColumnName();
    $countQuery = "SELECT COUNT(*)
            FROM civicrm_activity AS a
                JOIN civicrm_activity_contact AS b ON a.id = b.activity_id AND b.record_type_id = %1
                LEFT JOIN " . $tableName . " AS c ON a.id = c.entity_id
            WHERE a.is_deleted = %2 AND a.is_current_revision = %3 AND a.is_test = %2
              AND a.activity_type_id = %4 AND a.activity_date_time LIKE %5 AND b.contact_id = %6
              AND c." . $assentPisVersionColumn . " = %7 AND c." . $assentVersionColumn ." = %8";
    $countParams = [
      1 => [Civi::service('nbrBackbone')->getTargetRecordTypeId(), "Integer"],
      2 => [0, "Integer"],
      3 => [1, "Integer"],
      4 => [Civi::service('nbrBackbone')->getassentActivityTypeId(), "Integer"],
      5 => [$assentDate . "%", "String"],
      6 => [(int) $contactId, "Integer"],
      7 => [$assentPisVersion, "String"],
      8 => [$assentVersion, "String"],
    ];
    return CRM_Core_DAO::singleValueQuery($countQuery, $countParams);
  }
}
