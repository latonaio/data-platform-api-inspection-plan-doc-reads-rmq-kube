package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	Result              bool        `json:"result"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	HeaderDoc 		*[]HeaderDoc	`json:"HeaderDoc"`
	OperationDoc	*[]OperationDoc	`json:"OperationDoc"`
}

type HeaderDoc struct {
	InspectionPlan		 	 int	`json:"InspectionPlan"`
	DocType                  string `json:"DocType"`
	DocVersionID             int    `json:"DocVersionID"`
	DocID                    string `json:"DocID"`
	FileExtension            string `json:"FileExtension"`
	FileName                 string `json:"FileName"`
	FilePath                 string `json:"FilePath"`
	DocIssuerBusinessPartner int    `json:"DocIssuerBusinessPartner"`
}

type OperationDoc struct {
	InspectionPlan		 	 int	`json:"InspectionPlan"`
	Operations			 	 int	`json:"Operations"`
	OperationsItem	         int	`json:"OperationsItem"`
	OperationID	         	 int	`json:"OperationID"`
	DocType                  string `json:"DocType"`
	DocVersionID             int    `json:"DocVersionID"`
	DocID                    string `json:"DocID"`
	FileExtension            string `json:"FileExtension"`
	FileName                 string `json:"FileName"`
	FilePath                 string `json:"FilePath"`
	DocIssuerBusinessPartner int    `json:"DocIssuerBusinessPartner"`
}
package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToHeaderDoc(rows *sql.Rows) (*[]HeaderDoc, error) {
	defer rows.Close()
	headerDoc := make([]HeaderDoc, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &HeaderDoc{}

		err := rows.Scan(
			&pm.InspectionPlan,
			&pm.DocType,
			&pm.DocVersionID,
			&pm.DocID,
			&pm.FileExtension,
			&pm.FileName,
			&pm.FilePath,
			&pm.DocIssuerBusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &headerDoc, err
		}

		data := pm
		headerDoc = append(headerDoc, HeaderDoc{
			InspectionPlan:           data.InspectionPlan,
			DocType:                  data.DocType,
			DocVersionID:             data.DocVersionID,
			DocID:                    data.DocID,
			FileExtension:            data.FileExtension,
			FileName:                 data.FileName,
			FilePath:                 data.FilePath,
			DocIssuerBusinessPartner: data.DocIssuerBusinessPartner,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &headerDoc, nil
	}

	return &headerDoc, nil
}

func ConvertToOperationDoc(rows *sql.Rows) (*[]OperationDoc, error) {
	defer rows.Close()
	operationDoc := make([]OperationDoc, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &OperationDoc{}

		err := rows.Scan(
			&pm.InspectionPlan,
			&pm.Operations,
			&pm.OperationsItem,
			&pm.OperationID,
			&pm.DocType,
			&pm.DocVersionID,
			&pm.DocID,
			&pm.FileExtension,
			&pm.FileName,
			&pm.FilePath,
			&pm.DocIssuerBusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &operationDoc, err
		}

		data := pm
		operationDoc = append(operationDoc, OperationDoc{
			InspectionPlan:           data.InspectionPlan,
			Operations:      		  data.Operations,
			OperationsItem:      	  data.OperationsItem,
			OperationID:      	 	  data.OperationID,
			DocType:                  data.DocType,
			DocVersionID:             data.DocVersionID,
			DocID:                    data.DocID,
			FileExtension:            data.FileExtension,
			FileName:                 data.FileName,
			FilePath:                 data.FilePath,
			DocIssuerBusinessPartner: data.DocIssuerBusinessPartner,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &operationDoc, nil
	}

	return &operationDoc, nil
}
