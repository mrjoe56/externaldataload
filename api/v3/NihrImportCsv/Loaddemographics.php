<?php
use CRM_Externaldataload_ExtensionUtil as E;

/**
 * NihrImportCsv.Loaddemographics API
 *
 * @param array $params
 * @return array API result descriptor
 * @see civicrm_api3_create_success
 * @see civicrm_api3_create_error
 * @throws API_Exception
 */
function civicrm_api3_nihr_import_csv_Loaddemographics($params) {

  // get the csv import and processed folders
  $folder = 'nbr_folder_'.$params['dataSource'];

  $loadFolder = Civi::settings()->get($folder);
  if ($loadFolder && !empty($loadFolder)) {
    // 1) upload PID data file
    processFile($loadFolder, $params['dataSource'] . '_pid_data_export*', $params);
    // 2) contact data
    processFile($loadFolder, $params['dataSource'] . '_contacts_export*', $params);

    #if ($params['dataSource'] == 'pibd') {
    if (in_array($params['dataSource'], ['pibd', 'cyp'])) {
      // in addition to the volunteer data files (aka children's data) load the guardian data as well
      // NOTE: for CYP the guardian data is collected before the child's data
      processFile($loadFolder, $params['dataSource'] . '_guardian_pid_data_export*', $params);
      processFile($loadFolder, $params['dataSource'] . '_guardian_contacts_export*', $params);
    }

    // 3) hlq data - do not create new records for these data files!
    $params['createRecord'] = 0;
    processFile($loadFolder, $params['dataSource'] . '_hlq_export*', $params);

  }
  else {
    throw new API_Exception(E::ts('Folder for import (' . $folder . ') not found or empty'),  1001);
  }
}

/**
 * Function to process the actual file
 *
 * @param $Folder
 * @param $FilePrefix
 * @param $params
 * @return array
 * @throws API_Exception
 * @throws CiviCRM_API3_Exception
 */
function processFile($Folder, $FilePrefix, $params)
{
  $csvFiles = glob($Folder . DIRECTORY_SEPARATOR . $FilePrefix);

  // sort files with given prefix
  sort($csvFiles);

  // only use newest - last - file
  $csvFile = array_pop($csvFiles);

  if (!$csvFile) {
    throw new API_Exception(E::ts('Folder for import (' . $Folder . ') does not contain ' . $FilePrefix . ' files'), 1001);
  }

  // process file
  $import = new CRM_Externaldataload_NihrImportDemographicsCsv($csvFile, $params);
  if ($import->validImportData()) {
    $returnValues = $import->processImport();
    return civicrm_api3_create_success($returnValues, $params, 'NihrImportCsv', 'loaddemographics');
  }

}
