package requests

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
