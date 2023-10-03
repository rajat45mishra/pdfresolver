from wallmartclass import Wallmart
from validation import Validation

import pandas as pd
import fitz
import json

indexmap = {
    "Sr.No": 1,
    "Artical": 2,
    "Article Description": [3, 4],
    "HSN": 5,
    "EAN": 6,
    "Quantity Ordered": 7,
    "UOM": 8,
    "Pack": 9,
    "MRP": 10,
    "Cost": 11,
    "Line Cost Excl Tax": 12,
    "Tax Details": [13, 14, 15],
}

obj = Wallmart(
    "./pdfs/",
    fitz,
    configpath="./configs/veriations.json",
    indexmap=indexmap,
    jsonencoder=json,
)

dataobj = obj.get_records()
rowdf = pd.DataFrame.from_records(dataobj)

validation = {
    "Artical": r"^#\d{5}$",
    "EAN": r"^#\d{13}$",
    "Cost": r"[+-]?[0-9]+\.[0-9]+",
    "MRP": r"^\d+(\.\d{2})?\/EA$",
}
errortypes = {
    1: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^#\d+#+[A-Z ]+$"},
    },
    2: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {
            "validation": r"^#[A-Z ]+?[0-9]?[A-Z]?[0-9][A-Z]?+#+[0-9]+$"
        },
    },
    3: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {
            "validation": r"^#[A-Z ]+?[0-9]?[A-Z]?[0-9][0-9 ][0-9][A-Z]?+#+[0-9]+$"
        },
    },
    4: {
        "initialerrorpoint": "MRP",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^\d+(\.\d{2})?/$"},
    },
    5: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^\d#\d{5}$"},
    },
    6: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^#\d{8}#\d{13}$"},
    },
    7: {
        "initialerrorpoint": "HSN",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^\d$"},
    },
    8: {
        "initialerrorpoint": "MRP",
        "errortype": "formetting",
        "identificationprams": {
            "validation": r"^[0-9]+\.[0-9]{2}\/[A-Z]{2}[0-9]+\.[0-9]{2}$"
        },
    },
    9: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {"validation": r"#\d+#\d{13}+"},
    },
    10: {
        "initialerrorpoint": "Article Description",
        "errortype": "formetting",
        "identificationprams": {"validation": r"^#\d+#[A-Z ]+$"},
    },
}

sorting = Validation(rowdf, validation, errortypes=errortypes)
data = sorting.get_formetted_data()
fixtures = None
with open("./configs/fixtures.json", "rb") as td:
    fixtures = json.loads(td.read())
errors = []
maindf = []
if fixtures is not None:
    for i in fixtures:
        if sorting.validate_po_wise_transections(
            data, i["PoNo"], validationlength=i["validationlength"]
        ):
            maindf.append(i)
        else:
            errors.append(i)
df_list2 = []
for fd in maindf:
    rslt_df = rowdf[rowdf["PURCHASE ORDER NO"].str.contains(fd["PoNo"])]
    df_list2.append(rslt_df)
df_list = []
for fd in errors:
    rslt_df = rowdf[rowdf["PURCHASE ORDER NO"].str.contains(fd["PoNo"])]
    df_list.append(rslt_df)
newdf = pd.concat(df_list)
maindf = pd.concat(df_list2)
sorteddata = Validation(newdf, validation, errortypes=errortypes)
maindf = pd.concat([pd.DataFrame.from_records(sorteddata.get_sorted_data()[0]), maindf])
newdf = pd.DataFrame.from_records(sorteddata.get_sorted_data()[1])
maindf = Validation(maindf, validation, errortypes=errortypes)
maindf = pd.DataFrame.from_records(maindf.get_sorted_data()[0])
missingdatadf = []
maindfdata = []
for jh in fixtures:
    if sorteddata.validate_po_wise_transections(
        maindf, jh["PoNo"], validationlength=jh["validationlength"]
    ):
        rslt_df = maindf[maindf["PURCHASE ORDER NO"].str.contains(jh["PoNo"])]
        maindfdata.append(rslt_df)
    else:
        datadict = {}
        rslt_df = maindf[maindf["PURCHASE ORDER NO"].str.contains(jh["PoNo"])]
        existingdata = [int(x) for x in rslt_df["Sr.No"].to_list()]
        missing = [x + 1 for x in range(jh["validationlength"])]
        finalmissingdata = []
        for x in missing:
            if x not in existingdata:
                finalmissingdata.append(x)
        missingdatadf.append([rslt_df, finalmissingdata])

missingdata = sorteddata.fillmissingdata(obj, missingdatadf, fixtures)
missingdf = [pd.DataFrame.from_records(x) for x in list(missingdata)]
finaldf = []
for x in missingdatadf:
    for y in missingdf:
        srno = [int(r) for r in y["Sr.No"].to_list()]
        s = x[0]["PURCHASE ORDER NO"].to_list()
        sx = y["PURCHASE ORDER NO"].to_list()
        if s != []:
            if s[0] in sx:
                finaldfd = pd.concat([x[0], y])
                finaldf.append(finaldfd)


finalmissingdata = pd.concat(finaldf)

finaldfdaat = pd.concat([finalmissingdata, maindf])
totallist = []
for uyte, jhcfsdj in finaldfdaat.iterrows():
    total = jhcfsdj["Tax Details"].split("-")[-1]
    total = total.replace("0.00", "")
    linecostex = jhcfsdj["Line Cost Excl Tax"].split(" ")[0]
    texdetails = jhcfsdj["Tax Details"].split("-")[0] + "- 0.00"
    rowtax = " ".join(jhcfsdj["Line Cost Excl Tax"].split(" ")[1:])
    texdetails = "{} - {}".format(rowtax, texdetails)
    jhcfsdj["Line Cost Excl Tax"] = float(linecostex)
    jhcfsdj["Total Amount incl tax"] = float(total)
    jhcfsdj["Tax Details"] = texdetails
    totallist.append(jhcfsdj.to_dict())

derst = pd.DataFrame.from_records(totallist)
df = derst.drop_duplicates(
    subset=[
        "EAN",
        "Artical",
        "HSN",
        "Pack",
        "PURCHASE ORDER NO",
        "Total Amount incl tax",
        "Quantity Ordered",
        "Pack",
    ]
)
dfew = []
for jh in fixtures:
    if sorteddata.validate_po_wise_transections(
        df, jh["PoNo"], validationlength=jh["validationlength"]
    ):
        rslt_df = df[df["PURCHASE ORDER NO"].str.contains(jh["PoNo"])]
        costwithoutax = rslt_df["Line Cost Excl Tax"].sum()
        costwithtax = rslt_df["Total Amount incl tax"].sum()
        totalgfd = []
        for k, v in rslt_df.iterrows():
            v["Total Cost Without Tax"] = costwithoutax
            v["Grand Total Amount incl tax"] = costwithtax
            v["Total tax Amount"] = costwithtax - costwithoutax
            v["Vendor Stock"] = ""
            v["ORDER DATE"] = "".join(v["ORDER DATE"].split(":"))
            v["PO CANCEL DATE"] = "".join(v["PO CANCEL DATE"].split(":"))
            totalgfd.append(v.to_dict())
        dfew.append(pd.DataFrame.from_records(totalgfd))
    else:
        print(jh["PoNo"])
df = pd.concat(dfew)
df.set_index(["PURCHASE ORDER NO", "ORDER DATE", "PO CANCEL DATE"])
df.to_csv("final.csv")
