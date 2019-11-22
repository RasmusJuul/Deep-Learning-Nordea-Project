import requests
import json
from datetime import datetime
import pytz
import pandas as pd
import traceback
import pprint

from liquer import *

HEADERS_FOR_JSON = {
    'accept': "application/json; charset=utf-8",
    'content-type': "application/json"
}

SEARCH_URL = "http://distribution.virk.dk/offentliggoerelser/_search"


def to_datetime(d):
    if isinstance(d, datetime):
        return d.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    if d in (None, "NOW"):
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


NS = "virk"


@first_command(ns=NS)
def offentliggoerelser_raw(day=None, size=2999):
    """Get public register as JSON document.

    Arguments:
    day -- date string in YYYYMMDD format
    size -- size argument in the query
    """
    if day in (None, "TODAY"):
        d = datetime.utcnow()
    else:
        d = datetime.strptime(day, "%Y%m%d")
    dd = d.strftime("%Y-%m-%d")
    gt = f"{dd}T00:00:00.001Z"
    lt = f"{dd}T23:59:59.505Z"
    data = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "dokumenter.dokumentMimeType": "application"
                        }
                    },
                    {
                        "term": {
                            "dokumenter.dokumentMimeType": "xml"
                        }
                    },
                    {
                        "range": {
                            "offentliggoerelsesTidspunkt": {
                                "gt": gt,
                                "lt": lt
                            }
                        }
                    }
                ],
                "must_not": [],
                "should": []
            }
        },
        "size": size
    }

    response = requests.post(SEARCH_URL, data=json.dumps(data),
                             headers=HEADERS_FOR_JSON)
    return response.json()


@command(ns=NS)
def register2df(data):
    """Convert JSON response from offentliggoerelser_raw to DataFrame"""
    df = pd.DataFrame(columns=["_id", "_index", "_score", "cvrNummer"])
    for r in data["hits"]["hits"]:
        d = dict(
            _id=r["_id"],
            _index=r["_index"],
            _score=r["_score"],
            cvrNummer=str(r["_source"]["cvrNummer"]),
            indlaesningsId=r["_source"]["indlaesningsId"],
            indlaesningsTidspunkt=r["_source"]["indlaesningsTidspunkt"],
            offentliggoerelsesTidspunkt=r["_source"]["offentliggoerelsesTidspunkt"],
            offentliggoerelsestype=r["_source"]["offentliggoerelsestype"],
            omgoerelse=r["_source"]["omgoerelse"],
            regNummer=r["_source"]["regNummer"],
            regnskabsperiode_startDato=r["_source"]["regnskab"]["regnskabsperiode"]["startDato"],
            regnskabsperiode_slutDato=r["_source"]["regnskab"]["regnskabsperiode"]["slutDato"],
            sagsNummer=r["_source"]["sagsNummer"],
            sidstOpdateret=r["_source"]["sidstOpdateret"],
        )
        for doc in r["_source"]["dokumenter"]:
            t = {"application/xhtml+xml": "html", "application/pdf": "pdf",
                 "application/xml": "xml"}.get(doc["dokumentMimeType"])
            if t is not None:
                if doc["dokumentType"] == "AARSRAPPORT":
                    d[f"AARSRAPPORT_{t}"] = doc["dokumentUrl"]
        df = df.append(d, ignore_index=True)
    return df


@first_command(ns=NS)
def register(day="TODAY", size=2999):
    """Get public register as dataframe.
    Uses offentliggoerelser_raw to fetch the data and register2df to convert the data afterwards.

    Arguments:
    day -- date string in YYYYMMDD format
    size -- size argument in the query
    """
    return evaluate(f"ns-{NS}/offentliggoerelser_raw-{day}-{size}/register2df").get()


@first_command(ns=NS)
def cvr(cvr, ext="xml", day="TODAY", size=2999):
    """
    Get the document for a specified CVR number.
    Document has to be found in an index (obtained by the 'index' function taking day and size parameters).
    Multiple formats of document are supported by VIRK, format is specified by 'ext' (xml by default).
    """
    df = register(day=day, size=size)
    url = list(df.loc[df.cvrNummer.map(str) == str(cvr), f"AARSRAPPORT_{ext}"])
    if len(url):
        try:
            return requests.get(url[0]).text
        except:
            import traceback
            traceback.print_exc()
            return ""
    else:
        return ""


@command(ns=NS, volatile=True)
def tojson(xml):
    """Convert XML document with financial statements to JSON"""
    from xmljson import yahoo
    from lxml import etree
    try:
        root = etree.fromstring(xml.encode("utf-8"))
    except:
        return {}
    for elem in root.getiterator():
        try:
            tag = etree.QName(elem.tag)
        except:
            # Spam when downloading stuff !! # TODO
            # traceback.print_exc()
            continue
        elem.tag = tag.localname
        d = {}
        for key, value in elem.attrib.items():
            nkey = etree.QName(key).localname
            d[nkey] = value
            del elem.attrib[key]
        elem.attrib.update(d)

    d = yahoo.data(root)
    return d["xbrl"]


@command(ns=NS, volatile=True)
def json2df(doc, keep_multiline_values=False, init=None):
    """Convert JSON document with financial statements to DataFrame

    Arguments:
    keep_multiline_values -- if True, arguments with multiline strings are kept
    init -- parameter for internal use
    """
    if not isinstance(init, dict):
        init = {}
    df = pd.DataFrame(columns=["entity", "start_date", "end_date", "context"])
    context = {c["id"]: c for c in doc.get("context", [])}
    cdates = []
    for c in doc.get("context", []):
        period = c.get("period", {})
        cid = c["id"]
        if "instant" in period:
            cdates.append((period["instant"], period["instant"], cid))
        elif "startDate" in period and "endDate" in period:
            cdates.append((period["startDate"], period["endDate"], cid))
        else:
            raise Exception("Context without known dates")

    cdates = sorted(cdates)

    cols = sorted(key for key in doc.keys() if key not in ["context", "unit"])
    for start, end, identifier in cdates:
        c = context[identifier]
        entity = c["entity"]["identifier"]["content"]
        d = dict(init, start_date=start, end_date=end,
                 entity=entity, context=identifier)
        for key in cols:
            keydata = doc[key]
            if isinstance(keydata, list):
                row = [r for r in keydata if r["contextRef"] == identifier]
                if len(row) > 1:
                    pass
                    # TODO kinda anoying
                    # print(
                    #     f"Warning: multiple context entries for {entity} {key}, context:{identifier}")
                if len(row):
                    keydata = row[0]
                else:
                    keydata = None
            if keydata is not None:
                if isinstance(keydata, dict) and "content" in keydata:
                    use = keep_multiline_values or "\n" not in keydata["content"]
                    if use:
                        value = keydata["content"]
                        try:
                            value = float(value)
                            if int(value) == value:
                                value = int(value)
                        except:
                            pass
                        d[key] = value
        df = df.append(d, ignore_index=True)
    return df


@first_command(ns=NS)
def cvrdf(cvr, keep_multiline_values=False, day="TODAY", size=2999):
    """
    Get the document for a specified CVR number as DataFrame.
    Document has to be found in an index (obtained by the 'index' function taking day and size parameters).
    If keep_multiline_values is True, arguments with multiline strings are kept, otherwise they are removed (default).
    """
    register_df = register(day=day, size=size)
    register_df = register_df.loc[register_df.cvrNummer.map(
        str) == str(cvr), :]
    df = pd.DataFrame()
    for index, row in register_df.iterrows():
        cvr = str(row.cvrNummer)
        doc = evaluate(f"ns-{NS}/cvr-{cvr}-xml-{day}-{size}/tojson").get()
        cvr_df = json2df(
            doc, keep_multiline_values=keep_multiline_values, init=dict(row))
        df = df.append(cvr_df, ignore_index=True)
    return df


if __name__ == "__main__":
    import liquer.ext.basic
    import liquer.ext.lq_pandas
    from liquer.cache import FileCache, set_cache

    # TO save data you requested alread (save to cashe)
    # set_cache(FileCache("cache"))
