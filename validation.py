from typing import Dict,Optional,List,Any
import re
import pandas as pd

class Validation:
    def __init__(
        self,
        df: object,
        sotingregex: Dict[str, str],
        errortypes: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        self.df = df
        self.sortingregex = sotingregex
        self.errortypes = errortypes

    def get_sorted_data(self):
        errors = []
        sortedata = []
        for k, v in self.df.iterrows():
            vali1 = self.sortingregex["Artical"]
            vali2 = self.sortingregex["EAN"]
            vali3 = self.sortingregex["Cost"]
            vali4 = self.sortingregex["MRP"]
            vali1d = re.findall(vali1, v["Artical"])
            if len(vali1d) > 0:
                vali2d = re.findall(vali2, v["EAN"])
                if len(vali2d) > 0:
                    vali3d = re.findall(vali3, v["Cost"])
                    if len(vali3d) > 0:
                        vali4d = re.findall(vali4, v["MRP"])
                        if len(vali4d) > 0:
                            sortedata.append(v.to_dict())
                        else:
                            errors.append(v.to_dict())
                    else:
                        errors.append(v.to_dict())
                else:
                    errors.append(v.to_dict())
            else:
                errors.append(v.to_dict())
        return [sortedata, errors]

    def get_error_types(self, df):
        listdata = []
        if self.errortypes is None:
            return "error required for indentification"
        else:
            for k, v in df.iterrows():
                for f, b in self.errortypes.items():
                    initialpoint = b["initialerrorpoint"]
                    error = v[initialpoint]
                    if "indexrange" not in b["identificationprams"].keys():
                        validationregex = b["identificationprams"]["validation"]
                        vadatedata = re.findall(validationregex, error)
                        if len(vadatedata) > 0:
                            if b["errortype"] == "formetting":
                                errors = {}
                                errors["errortypes"] = f
                                errors["data"] = v.to_dict()
                                listdata.append(errors)
        return listdata

    def get_formetted_data(self, newdf=None):
        if newdf is None:
            newdf = self.get_sorted_data()
            errorsdf = newdf[1]
            dsdf = newdf[0]
            newdf = pd.DataFrame.from_records(dsdf)
            erdf = pd.DataFrame.from_records(errorsdf)
            alldf = pd.concat([erdf])
            rew = self.get_error_types(alldf)
        else:
            rew = self.get_error_types(newdf)
        for s, x in enumerate(rew):
            newdata = {}
            if x["errortypes"] == 1:
                newdata["Sr.No"] = x["data"]["Artical"]
                newdata["Artical"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[1]
                )
                newdata["Article Description"] = "#{} {}".format(
                    x["data"]["Article Description"].split("#")[2], x["data"]["HSN"]
                )
                newdata["HSN"] = x["data"]["EAN"]
                newdata["EAN"] = x["data"]["Quantity Ordered"]
                newdata["Quantity Ordered"] = x["data"]["UOM"]
                newdata["UOM"] = x["data"]["Pack"]
                newdata["Pack"] = x["data"]["MRP"]
                newdata["MRP"] = x["data"]["Cost"]
                newdata["Cost"] = x["data"]["Line Cost Excl Tax"]
                newdata["Line Cost Excl Tax"] = x["data"]["Tax Details"]
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 2:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[1]
                )
                newdata["HSN"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[2]
                )
                newdata["EAN"] = x["data"]["HSN"]
                newdata["Quantity Ordered"] = x["data"]["EAN"]
                newdata["UOM"] = x["data"]["Quantity Ordered"]
                newdata["Pack"] = x["data"]["UOM"]
                newdata["MRP"] = x["data"]["Pack"]
                newdata["Cost"] = x["data"]["MRP"]
                newdata["Line Cost Excl Tax"] = x["data"]["Cost"]
                newdata["Tax Details"] = (
                    x["data"]["Line Cost Excl Tax"] + x["data"]["Tax Details"]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 3:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[1]
                )
                newdata["HSN"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[2]
                )
                newdata["EAN"] = x["data"]["HSN"]
                newdata["Quantity Ordered"] = x["data"]["EAN"]
                newdata["UOM"] = x["data"]["Quantity Ordered"]
                newdata["Pack"] = x["data"]["UOM"]
                newdata["MRP"] = x["data"]["Pack"]
                newdata["Cost"] = x["data"]["MRP"]
                newdata["Line Cost Excl Tax"] = x["data"]["Cost"]
                newdata["Tax Details"] = (
                    x["data"]["Line Cost Excl Tax"] + x["data"]["Tax Details"]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 4:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = x["data"]["Article Description"]
                newdata["HSN"] = x["data"]["HSN"]
                newdata["EAN"] = x["data"]["EAN"]
                newdata["Quantity Ordered"] = x["data"]["Quantity Ordered"]
                newdata["UOM"] = x["data"]["UOM"]
                newdata["Pack"] = x["data"]["Pack"]
                newdata["MRP"] = x["data"]["MRP"] + x["data"]["Cost"]
                newdata["Cost"] = x["data"]["Line Cost Excl Tax"]
                newdata["Line Cost Excl Tax"] = x["data"]["Tax Details"].split(":")[0]
                newdata["Tax Details"] = x["data"]["Tax Details"].split(":")[1]
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 5:
                newdata["Sr.No"] = x["data"]["Article Description"].split("#")[0]
                newdata["Artical"] = x["data"]["Article Description"].split("#")[1]
                newdata["Article Description"] = x["data"]["HSN"] + x["data"]["EAN"]
                newdata["HSN"] = x["data"]["Quantity Ordered"]
                newdata["EAN"] = x["data"]["UOM"]
                newdata["Quantity Ordered"] = x["data"]["Pack"]
                newdata["UOM"] = x["data"]["MRP"]
                newdata["Pack"] = x["data"]["Cost"]
                newdata["MRP"] = x["data"]["Line Cost Excl Tax"]
                newdata["Cost"] = x["data"]["Tax Details"].split(" ")[0][
                    0 : int(float(len(x["data"]["Tax Details"].split(" ")[0]) / 2))
                ]
                newdata["Line Cost Excl Tax"] = x["data"]["Tax Details"].split(" ")[0][
                    int(float(len(x["data"]["Tax Details"].split(" ")[0]) / 2)) : int(
                        float(len(x["data"]["Tax Details"].split(" ")[0]))
                    )
                ]
                newdata["Tax Details"] = "".join(
                    x["data"]["Tax Details"].split(" ")[1:]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 6:
                newdata["Sr.No"] = x["data"]["Tax Details"][
                    len(x["data"]["Tax Details"]) - 8 : len(x["data"]["Tax Details"])
                ].split("#")[0]
                newdata["Artical"] = "#{}".format(
                    x["data"]["Tax Details"][
                        len(x["data"]["Tax Details"])
                        - 8 : len(x["data"]["Tax Details"])
                    ].split("#")[1]
                )
                newdata["Article Description"] = (
                    x["data"]["Artical"] + x["data"]["Artical"]
                )
                newdata["HSN"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[1]
                )
                newdata["EAN"] = "#{}".format(
                    x["data"]["Article Description"].split("#")[2]
                )
                newdata["Quantity Ordered"] = x["data"]["HSN"]
                newdata["UOM"] = x["data"]["EAN"]
                newdata["Pack"] = x["data"]["Quantity Ordered"]
                newdata["MRP"] = x["data"]["UOM"]
                newdata["Cost"] = x["data"]["Pack"]
                newdata["Line Cost Excl Tax"] = x["data"]["MRP"]
                newdata["Tax Details"] = "".join([x["data"]["Cost"], x["data"]["MRP"]])
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 7:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = (
                    x["data"]["Article Description"] + x["data"]["HSN"]
                )
                newdata["HSN"] = x["data"]["EAN"]
                newdata["EAN"] = x["data"]["Quantity Ordered"]
                newdata["Quantity Ordered"] = x["data"]["UOM"]
                newdata["UOM"] = x["data"]["Pack"]
                newdata["Pack"] = x["data"]["MRP"]
                newdata["MRP"] = x["data"]["Cost"]
                newdata["Cost"] = x["data"]["Line Cost Excl Tax"]
                newdata["Line Cost Excl Tax"] = x["data"]["Tax Details"].split(" ")[0]
                newdata["Tax Details"] = "".join(
                    x["data"]["Tax Details"].split(" ")[1:]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 8:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = (
                    x["data"]["Article Description"] + x["data"]["HSN"]
                )
                newdata["HSN"] = x["data"]["EAN"]
                newdata["EAN"] = x["data"]["Quantity Ordered"]
                newdata["Quantity Ordered"] = x["data"]["UOM"]
                newdata["UOM"] = x["data"]["Pack"]
                newdata["Pack"] = x["data"]["MRP"]
                newdata["MRP"] = x["data"]["Cost"]
                newdata["Cost"] = x["data"]["Line Cost Excl Tax"]
                newdata["Line Cost Excl Tax"] = x["data"]["Tax Details"].split(" ")[0]
                newdata["Tax Details"] = "".join(
                    x["data"]["Tax Details"].split(" ")[1:]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())
            elif x["errortypes"] == 9:
                newdata["Sr.No"] = x["data"]["Sr.No"]
                newdata["Artical"] = x["data"]["Artical"]
                newdata["Article Description"] = x["data"]["Article Description"]
                newdata["HSN"] = x["data"]["HSN"]
                newdata["EAN"] = x["data"]["EAN"]
                newdata["Quantity Ordered"] = x["data"]["Quantity Ordered"]
                newdata["UOM"] = x["data"]["UOM"]
                newdata["Pack"] = x["data"]["Pack"]
                newdata["MRP"] = x["data"]["MRP"]
                newdata["Cost"] = x["data"]["MRP"]
                newdata["Line Cost Excl Tax"] = x["data"]["Cost"].split(" ")[0]
                newdata["Tax Details"] = "".join(x["data"]["Cost"].split(" ")[1:])
                newdata["Tax Details"] += (
                    x["data"]["Line Cost Excl Tax"] + newdata["Tax Details"]
                )
                x["data"].update(newdata)
                newdf.loc[newdf.index[-1] + s] = list(x["data"].values())

        return newdf

    def validate(self, df, **kwargs):
        for key, val in df.iterrows():
            for k, v in kwargs.items():
                if val[k]:
                    row = r"{}".format(v)
                    vali = re.findall(row, val[k])
                    if len(vali) > 0:
                        print(f"========validating {k} {key} ===========")
                        return True
                    else:
                        raise ValueError(
                            "patterns does not matched rowindex {} column name{}".format(
                                key, k
                            )
                        )

    def validate_po_wise_transections(self, df, PoNo, validationlength=None):
        rslt_df = df[df["PURCHASE ORDER NO"].str.contains(PoNo)]
        if len(rslt_df) == validationlength:
            return True
        else:
            return False

    def statistical_validation_po_wise(self, df, PoNo, staistics: Dict[str, Any] = {}):
        rslt_df = df.loc[df["PURCHASE ORDER NO"] == PoNo]
        for key, value in staistics.items():
            if df[key] == value:
                return True
            else:
                return False

    def get_tans(self, invoice):
        existingtrans = []
        for f, hj in invoice.iterrows():
            if len(hj["Sr.No"]) <= 3:
                existingtrans.append(int(hj["Sr.No"]))
        return existingtrans

    

    def datafillerdict(self, records, columns, ied, mj):
        dictdata = {}
        dsqw = None
        newrecords = None
        for dwq in columns:
            reqw = None
            dictdata[dwq] = records[columns.index(dwq)]
            if dwq == "HSN":
                reg = r"^#\d+$"
                valo = re.findall(reg, records[columns.index(dwq)])
                if len(valo) > 0:
                    dictdata[dwq] = records[columns.index(dwq)]
                else:
                    dictdata[columns[columns.index(dwq) - 1]] += records[
                        columns.index(dwq)
                    ]
                    valo2 = re.findall(reg, records[columns.index(dwq) + 1])
                    if len(valo2) > 0:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 1
                        ]
                    else:
                        dictdata[columns[columns.index(dwq) - 1]] += records[
                            columns.index(dwq) + 1
                        ]
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 2
                        ]
            elif dwq == "EAN":
                reg = r"^#\d{13}+$"
                valo3 = re.findall(reg, records[columns.index(dwq) + 1])
                if len(valo3) > 0:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
                else:
                    if "CS" in records[columns.index(dwq) + 2]:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dsqw = columns[columns.index(dwq) + 1]
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 2
                        ]
            elif dwq == "Quantity Ordered":
                if dwq != dsqw:
                    if "CS" in records[columns.index(dwq) + 1]:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 1
                        ]
                else:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 2
                    ]
                    dsqw = columns[columns.index(dwq) + 1]
            elif dwq == "UOM":
                if dwq != dsqw:
                    if (
                        "EA" in records[columns.index(dwq) + 1]
                        and "CS" in records[columns.index(dwq) + 1]
                    ):
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 1
                        ]
                else:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 2
                    ]
                    dsqw = columns[columns.index(dwq) + 1]
            elif dwq == "Pack":
                if records[columns.index(dwq) + 1] == "CS":
                    dictdata[columns[columns.index(dwq)]] = "".join(
                        [records[columns.index(dwq) + 2].split("CS")[0], "CS"]
                    )
                elif (
                    "EA" in records[columns.index(dwq) + 1]
                    and "CS" not in records[columns.index(dwq) + 1]
                ):
                    dictdata[columns[columns.index(dwq)]] = records[columns.index(dwq)]
                else:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
            elif dwq == "MRP":
                validationMRP = r"^\d+\.\d{2}/[A-Z]{2}$"
                rewq = re.findall(validationMRP, records[columns.index(dwq) + 1])
                if len(rewq) > 0:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
                else:
                    try:
                        dwqa = float(records[columns.index(dwq) + 1])
                    except ValueError as e:
                        dwqa = e
                    if "CS" in records[columns.index(dwq) + 1]:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 2
                        ]
                    elif (
                        "EA" not in records[columns.index(dwq) + 1]
                        and type(dwqa) is float
                    ):
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = (
                            records[columns.index(dwq) + 1]
                            + records[columns.index(dwq) + 2]
                        )

            elif dwq == "Cost":
                if len(rewq) > 0:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
                else:
                    if "IN: GST Comp." in records[columns.index(dwq) + 2]:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 2
                        ]
            elif dwq == "Line Cost Excl Tax":
                if len(rewq) > 0:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
                else:
                    valos = r"^\d+\.\d{2} IN: IGST\(\d+%\) - \d+\.\d{2}$"
                    valos = re.findall(valos, records[columns.index(dwq) + 2])
                    if len(valos) == 0:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq)
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = records[
                            columns.index(dwq) + 2
                        ]
            elif dwq == "Tax Details":
                if len(rewq) > 0:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 1
                    ]
                    dictdata[columns[columns.index(dwq)]] += records[
                        columns.index(dwq) + 2
                    ]
                    dictdata[columns[columns.index(dwq)]] += records[
                        columns.index(dwq) + 3
                    ]
                else:
                    dictdata[columns[columns.index(dwq)]] = records[
                        columns.index(dwq) + 2
                    ]
                    dictdata[columns[columns.index(dwq)]] += records[
                        columns.index(dwq) + 3
                    ]
                valod = r"IN: GST Comp\. CESS\(\d+\.\d+%\) - \d+\.\d+\.\d+"
                valis = re.findall(valod, dictdata[columns[columns.index(dwq)]])
                try:
                    dswqs = float(dictdata[columns[columns.index(dwq)]])
                except ValueError as e:
                    dswqs = e
                if (
                    len(valis) == 0
                    and type(dswqs) is not float
                    and "#" not in dictdata[columns[columns.index(dwq)]]
                ):
                    if len(dictdata[columns[columns.index(dwq)]].split(".")) != 3:
                        record2 = ied[ied.index(str(mj)) : ied.index(str(mj)) + 16]
                        dictdata[columns[columns.index(dwq)]] += record2[
                            columns.index(dwq) + 4
                        ]
                    else:
                        dictdata[columns[columns.index(dwq)]] = (
                            records[columns.index(dwq)]
                            + records[columns.index(dwq) + 1]
                            + records[columns.index(dwq) + 2]
                        )
                elif type(dswqs) is float:
                    dictdata[columns[columns.index(dwq)]] = (
                        records[columns.index(dwq)]
                        + records[columns.index(dwq) + 1]
                        + records[columns.index(dwq) + 2]
                    )
                if "#" in dictdata[columns[columns.index(dwq)]]:
                    dictdata[columns[columns.index(dwq)]] = (
                        records[columns.index(dwq) + 2]
                        + records[columns.index(dwq) + 3]
                        + records[columns.index(dwq) + 4]
                    )
        return dictdata

    def fillmissingdata(self, wallmart: object, invlist, validation={}):
        rowdata = list(wallmart.transections_organigation())
        ret = invlist
        for i in rowdata:
            for x in invlist:
                invoice = None
                for j, k in x[0].iterrows():
                    if k["PURCHASE ORDER NO"] == i["PURCHASE ORDER NO"]:
                        invoice = x
                if invoice is not None:
                    rowdatafinal = []
                    for ixd, ied in enumerate(i["invoicedata"]):
                        for mj in x[1]:
                            if str(mj) in ied:
                                red = None
                                indexs = [
                                    (ied.index(str(mj)), mj)
                                    for x in ied
                                    if x == str(mj)
                                ]
                                if len(indexs) > 1:
                                    pandass = pd.Series(ied)
                                    listd = pandass[pandass == str(mj)].index.to_list()
                                    for j in listd:
                                        if ied[j] == str(mj) and len(
                                            re.findall(r"^#\d{5}$", ied[j + 1])
                                        ):
                                            red = ied[j : j + 16]
                                            if "." not in red[-1]:
                                                red = red[:-1]
                                if red is None:
                                    records = ied[
                                        ied.index(str(mj)) : ied.index(str(mj)) + 15
                                    ]
                                else:
                                    records = red
                                columns = x[0].columns.to_list()[0:-4]
                                dictdata = self.datafillerdict(
                                    records, columns, ied, mj
                                )
                                dictdata["PURCHASE ORDER NO"] = i["PURCHASE ORDER NO"]
                                dictdata["ORDER DATE"] = i["ORDER DATE"]
                                dictdata["PO CANCEL DATE"] = i["PO CANCEL DATE"]
                                dictdata["BILL TO"] = i["BILL TO"]
                                rowdatafinal.append(dictdata)
                    yield rowdatafinal
