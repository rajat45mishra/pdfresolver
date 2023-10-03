from typing import Optional, List, Any
import os


class Wallmart:
    def __init__(
        self,
        pdffolderpath,
        pdfreader: object,
        configpath: Optional[str] = ".",
        indexmap: Optional[Any] = None,
        jsonencoder: Optional[object] = None,
    ) -> None:
        self.pdffolderpath = pdffolderpath
        self.pdfreader = pdfreader
        self.indexmap = indexmap
        self.configpath = configpath
        self.jsonencoder = jsonencoder

    def get_row_data(self):
        pdfs = os.listdir(self.pdffolderpath)
        files = []
        for i in pdfs:
            fit = self.pdfreader.open("".join([self.pdffolderpath, i]))
            pages = []
            for page in fit:
                pages.append(page.get_text())
            files.append(pages[:-1])
        return files

    def transections_organigation(self):
        files = self.get_row_data()
        for i in files:
            fipag = []
            invdetails = {}
            for q, c in enumerate(i):
                if [x for x in i[q].split("\n") if x.startswith("PURCHASE")] != []:
                    testa = [x for x in i[q].split("\n")]
                    indexs = testa.index(
                        [x for x in i[q].split("\n") if x.startswith("PURCHASE")][0]
                    )
                    invdetails["PURCHASE ORDER NO"] = testa[indexs - 1][1:]
                if [x for x in i[q].split("\n") if x.startswith("ORDER")] != []:
                    testa = [x for x in i[q].split("\n")]
                    indexs = testa.index(
                        [x for x in i[q].split("\n") if x.startswith("ORDER")][0]
                    )
                    invdetails["ORDER DATE"] = testa[indexs - 1]
                if [x for x in i[q].split("\n") if x.startswith("PO CANCEL")] != []:
                    testa = [x for x in i[q].split("\n")]
                    indexs = testa.index(
                        [x for x in i[q].split("\n") if x.startswith("PO CANCEL")][0]
                    )
                    invdetails["PO CANCEL DATE"] = testa[indexs - 1]
                if [x for x in i[q].split("\n") if x.startswith("BILL TO")] != []:
                    testa = [x for x in i[q].split("\n")]
                    indexs = testa.index(
                        [x for x in i[q].split("\n") if x.startswith("BILL TO")][0]
                    )
                    invdetails["BILL TO"] = testa[indexs + 1]
                if q == 0:
                    try:
                        if int(i[q].split("\n")[71:][0]) == 1:
                            fipag.append(i[q].split("\n")[71:])
                    except Exception as e:
                        fipag.append(i[q].split("\n")[69:])
                else:
                    if c.split("\n")[44:] != []:
                        fipag.append(c.split("\n")[44:])
            invdetails["invoicedata"] = fipag
            yield invdetails

    def indexsplit(self, l, n, info):
        records = {}
        records.update(info)
        for i in range(0, len(l), n):
            records["Sr.No"] = l[0]
            records["Artical"] = l[1]
            records["Article Description"] = " ".join([l[2], l[3]])
            records["HSN"] = l[4]
            records["Vendor Stock"] = ""
            records["EAN"] = l[5]
            records["Quantity Ordered"] = l[6]
            records["UOM"] = l[7]
            records["Pack"] = l[8]
            records["MRP"] = l[9]
            records["Cost"] = l[10]
            records["Line Cost Excl Tax"] = l[11]
            records["Tax Details"] = " ".join([l[12], l[13], l[14]])
            records["Total"] = ""
            yield records

    def trans_validation(self, length):
        returnitem = []
        for k, v in enumerate(length):
            yield dict(
                (klk, v[klk])
                for klk in [
                    "Sr.No",
                    "Artical",
                    "Article Description",
                    "HSN",
                    "EAN",
                    "Quantity Ordered",
                    "UOM",
                    "Pack",
                    "MRP",
                    "Cost",
                    "Line Cost Excl Tax",
                    "Tax Details",
                    "Total",
                ]
                if klk in v
            )

    def datafiller(self, data, row):
        data["Sr.No"] = row[0]
        data["Artical"] = row[1]
        data["Article Description"] = row[2] + row[3]
        data["HSN"] = row[4]
        data["EAN"] = row[5]
        data["Quantity Ordered"] = row[6]
        data["UOM"] = row[7]
        data["Pack"] = row[8]
        data["MRP"] = row[9]
        data["Cost"] = row[10]
        data["Line Cost Excl Tax"] = row[11]
        data["Tax Details"] = row[12] + row[13] + row[14]
        return data

    def datafilterfromlist(self, uy, length2, indexmap, veriations, ing):
        records = []
        lasttotal = 14
        try:
            lr = int(round(len(uy) / 15))
        except ValueError as e:
            lr = None
        for k, v in veriations.items():
            if int(k) == lr:
                for i, d in enumerate(v):
                    data = {}
                    lrt = length2
                    if d == {}:
                        hy = i * lrt
                        row = uy[hy : hy + lrt]
                        if len(row) >= 15:
                            self.datafiller(data, row)
                        else:
                            if len(row) == 14:
                                self.datafiller(data, uy[hy - 2 : hy + lrt - 1])
                            elif len(row) == 13:
                                self.datafiller(data, uy[hy - 3 : hy + lrt - 1])
                    else:
                        if "e" not in d.keys():
                            diff = lasttotal - 14
                            yu = i * lrt
                            row = None
                            if diff == 0 and list(d.values())[-1][-1] == 14:
                                fr = yu - 1
                                row = uy[fr : yu + list(d.values())[-1][-1] + 1]
                                lasttotal = list(d.values())[-1][-1]
                            elif diff == -1:
                                fr = yu - 1
                                row = uy[fr : fr + list(d.values())[-1][-1] + 1]
                                lasttotal = list(d.values())[-1][-1]
                            elif list(d.values())[-1][-1] < 14:
                                row = uy[yu : yu + list(d.values())[-1][-1] + 1]
                                lasttotal = list(d.values())[-1][-1]
                            else:
                                row = uy[yu : yu + list(d.values())[-1][-1] + 1]
                            if len(row) > 15:
                                for hj, kl in d.items():
                                    if type(kl) != list:
                                        data[hj] = row[kl]
                                    else:
                                        finaldata = ""
                                        for kuh in kl:
                                            finaldata += row[kuh]
                                        data[hj] = finaldata
                    data.update(ing)
                    records.append(data)
        return records

    def transection_formetter(self, rowdata, inv):
        lastsr = "1"
        miss = []
        for i in inv:
            if "Sr.No" not in list(i.keys()):
                if len(lastsr) <= 3:
                    lastsr = str(int(lastsr) + 1)
                    for k in rowdata["invoicedata"]:
                        if lastsr in k:
                            row = k[k.index(lastsr) : k.index(lastsr) + 15]
                            if len(row) == 15:
                                da = self.datafiller({}, row)
                                da.update(i)
                                miss.append(da)
            else:
                lastsr = i["Sr.No"]
                miss.append(i)
        return miss

    def get_records(self):
        files = []
        filesdata = list(self.transections_organigation())
        for ied in filesdata:
            inv = []
            for uy in ied["invoicedata"]:
                ing = dict(
                    (k, ied[k])
                    for k in [
                        "PURCHASE ORDER NO",
                        "ORDER DATE",
                        "PO CANCEL DATE",
                        "BILL TO",
                    ]
                    if k in ied
                )
                print(ing)
                trans = list(self.indexsplit(uy, 15, ing))[:-1]
                trans = self.datafilterfromlist(
                    uy, 15, self.indexmap, self.get_configs()[0], ing
                )
                trans = self.transection_formetter(ied, trans)
                inv += trans
            files += inv
        return files

    def get_configs(self):
        with open(self.configpath) as fd:
            veriations = self.jsonencoder.loads(fd.read())
        return [veriations]