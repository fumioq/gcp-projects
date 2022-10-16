/**
 * @OnlyCurrentDoc
 */

 function updateCaseWhen() {
  const ssUrl = SpreadsheetApp.getActive().getUrl()
  const token = "tws6h8srt1u9eg19"
  const tableId = "your-table-id" // project_id.dataset_name.table_name
  const cloudRunFunctionUrl = 'your-url'
  
  const response = UrlFetchApp.fetch(`${cloudRunFunctionUrl}/?token=${token}&case_when_sheet_url=${ssUrl}&table_id=${tableId}`)
  Logger.log(response)
}
