package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-inspection-plan-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-inspection-plan-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var headerDoc *[]dpfm_api_output_formatter.HeaderDoc
	var operationDoc *[]dpfm_api_output_formatter.OperationDoc

	for _, fn := range accepter {
		switch fn {
		case "HeaderDoc":
			func() {
				headerDoc = c.HeaderDoc(input, output, errs, log)
			}()
		case "OperationDoc":
			func() {
				operationDoc = c.OperationDoc(input, output, errs, log)
			}()
		}
	}

	data := &dpfm_api_output_formatter.Message{
		HeaderDoc: 		headerDoc,
		OperationDoc:   operationDoc,
	}

	return data
}

func (c *DPFMAPICaller) HeaderDoc(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.HeaderDoc {
	where := "WHERE 1 = 1"

	if input.HeaderDoc.InspectionPlan != nil {
		where = fmt.Sprintf("%s\nAND InspectionPlan = %d", where, *input.HeaderDoc.InspectionPlan)
	}
	if input.HeaderDoc.DocType != nil && len(*input.HeaderDoc.DocType) != 0 {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.HeaderDoc.DocType)
	}
	if input.HeaderDoc.DocIssuerBusinessPartner != nil && *input.HeaderDoc.DocIssuerBusinessPartner != 0 {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.HeaderDoc.DocIssuerBusinessPartner)
	}
	groupBy := "\nGROUP BY InspectionPlan, DocType, DocIssuerBusinessPartner "

	rows, err := c.db.Query(
		`SELECT
    InspectionPlan, DocType, MAX(DocVersionID), DocID, FileExtension, FileName, FilePath, DocIssuerBusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_inspection_plan_header_doc_data
		` + where + groupBy + `;`)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeaderDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) OperationDoc(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.OperationDoc {
	where := "WHERE 1 = 1"

	if input.HeaderDoc.InspectionPlan != nil {
		where = fmt.Sprintf("%s\nAND InspectionPlan = %d", where, *input.HeaderDoc.InspectionPlan)
	}
	if input.HeaderDoc.OperationDoc.Operations != nil {
		where = fmt.Sprintf("%s\nAND Operations = %d", where, *input.HeaderDoc.OperationDoc.Operations)
	}
	if input.HeaderDoc.OperationDoc.OperationsItem != nil {
		where = fmt.Sprintf("%s\nAND OperationsItem = %d", where, *input.HeaderDoc.OperationDoc.OperationsItem)
	}
	if input.HeaderDoc.OperationDoc.OperationID != nil {
		where = fmt.Sprintf("%s\nAND OperationID = %d", where, *input.HeaderDoc.OperationDoc.OperationID)
	}
	if input.HeaderDoc.OperationDoc.DocType != nil {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.HeaderDoc.OperationDoc.DocType)
	}
	if input.HeaderDoc.OperationDoc.DocIssuerBusinessPartner != nil {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.HeaderDoc.OperationDoc.DocIssuerBusinessPartner)
	}
	groupBy := "\nGROUP BY InspectionPlan, Operations, OperationsItem, OperationID, DocType, DocIssuerBusinessPartner "

	rows, err := c.db.Query(
		`SELECT
    InspectionPlan, Operations, OperationsItem, OperationID, DocType, MAX(DocVersionID), DocID, FileExtension, FileName, FilePath, DocIssuerBusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_inspection_plan_operation_doc_data
		` + where + groupBy + `;`)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToOperationDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
