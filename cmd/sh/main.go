package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"gitlab.com/greyxor/slogor"
)

var auth_token = "eyJraWQiOiJkaWQ6a2V5OnpEbmFlWmYxOHNuSGpQd2tvSEJwMkRCVUVmVFpLNU5KZEJYM0Z2QjVqcUZCbnB1Ym8iLCJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJhdWQiOiJkaWQ6a2V5OnpEbmFlcVhYNWhYb0hiWDhkU2pTZGdaa3pjSDFTYTZSY1Nzdm52TFpVdWtxRlJHeDkiLCJzdWIiOiJkaWQ6a2V5OnpEbmFlY0x2NWF5Q3J6V1JpWXlGM3ZhSGhLcVRTa0ZHV2lKSmRNZkFyV25aV2NxUm4iLCJzY29wZSI6Im9wZW5pZCBsZWFyY3JlZGVudGlhbCIsImlzcyI6Imh0dHBzOi8vdmVyaWZpZXIuZG9tZS1tYXJrZXRwbGFjZS1kZXYyLm9yZyIsImV4cCI6MTczNjk0NTExNSwiaWF0IjoxNzM2OTQxNTE1LCJ2YyI6eyJAY29udGV4dCI6WyJodHRwczovL3d3dy53My5vcmcvbnMvY3JlZGVudGlhbHMvdjIiLCJodHRwczovL3RydXN0LWZyYW1ld29yay5kb21lLW1hcmtldHBsYWNlLmV1L2NyZWRlbnRpYWxzL2xlYXJjcmVkZW50aWFsZW1wbG95ZWUvdjEiXSwiY3JlZGVudGlhbFN1YmplY3QiOnsibWFuZGF0ZSI6eyJpZCI6IjUyY2VjZjc1LTc2YmEtNGRhZC04ZTg2LTBlNWMyY2FhNDA5NCIsImxpZmVfc3BhbiI6eyJlbmRfZGF0ZV90aW1lIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIiwic3RhcnRfZGF0ZV90aW1lIjoiMjAyNC0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sIm1hbmRhdGVlIjp7ImVtYWlsIjoiamVzdXNAYWxhc3RyaWEuaW8iLCJmaXJzdF9uYW1lIjoiSmVzdXMiLCJpZCI6ImRpZDprZXk6ekRuYWVjTHY1YXlDcnpXUmlZeUYzdmFIaEtxVFNrRkdXaUpKZE1mQXJXblpXY3FSbiIsImxhc3RfbmFtZSI6IlJ1aXoiLCJtb2JpbGVfcGhvbmUiOiIifSwibWFuZGF0b3IiOnsiY29tbW9uTmFtZSI6Ikplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IklOMiBJTkdFTklFUklBIERFIExBIElORk9STUFDSU9OIFNPQ0lFREFEIExJTUlUQURBIiwib3JnYW5pemF0aW9uSWRlbnRpZmllciI6IlZBVEVTLUI2MDY0NTkwMCIsInNlcmlhbE51bWJlciI6Ijg3NjU0MzIxSyJ9LCJwb3dlciI6W3siaWQiOiIwNTRiOGY2Zi0wZjE3LTQ2NjItYWUyNi0zMTcxZGZhZGRiODciLCJ0bWZfYWN0aW9uIjoiRXhlY3V0ZSIsInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiT25ib2FyZGluZyIsInRtZl90eXBlIjoiRG9tYWluIn0seyJpZCI6IjNmNjQyZTg4LWQzYzAtNDI2My04NzE4LWE0MzYzNzJkMWE1NiIsInRtZl9hY3Rpb24iOlsiQ3JlYXRlIiwiVXBkYXRlIiwiRGVsZXRlIl0sInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiUHJvZHVjdE9mZmVyaW5nIiwidG1mX3R5cGUiOiJEb21haW4ifV0sInNpZ25lciI6eyJjb21tb25OYW1lIjoiNTY1NjU2NTZQIEplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IkRPTUUgQ3JlZGVudGlhbCBJc3N1ZXIiLCJvcmdhbml6YXRpb25JZGVudGlmaWVyIjoiVkFURVMtUTAwMDAwMDBKIiwic2VyaWFsTnVtYmVyIjoiSURDRVMtNTY1NjU2NTZQIn19fSwiaWQiOiIxNjI3NGZjYy0wNzQ0LTQyNjYtYjhiMS00ZjQ4ZmY4MjdlMTQiLCJpc3N1ZXIiOiJkaWQ6ZWxzaTpWQVRFUy1RMDAwMDAwMEoiLCJ0eXBlIjpbIkxFQVJDcmVkZW50aWFsRW1wbG95ZWUiLCJWZXJpZmlhYmxlQ3JlZGVudGlhbCJdLCJ2YWxpZEZyb20iOiIyMDI0LTEyLTIzVDEwOjE1OjQ4Ljg2ODI5Mzg2M1oiLCJ2YWxpZFVudGlsIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sImp0aSI6IjNjODE0MjAzLWZjOGItNDE4Zi05NWU1LTJlMjQzYTdmMTEyNSJ9.m3pvRPPt2O-YhpdkO1A91871PK5NxfdcWCNQzNjwt3_LJp9JdkPEB-YQrvxbCi8R3smOnb_--ZFbaPsdyDj6UQ"

var obj = `{
    "name": "Magic On Demand",
    "description": "Add magic to your business and get more customers.\n\nOur product, powered by **AI Stupid Decision Engine&trade;**  will replace your Executive Board with an AI driven decission engine, which will drive your business into the future.\n\nFire all the guys in your Executive Board and relax. \n\n\n",
    "lifecycleStatus": "Launched",
    "isBundle": false,
    "place": [],
    "version": "0.1",
    "attachment": [
        {
            "name": "organizationIdentifier",
            "description": "VATES-B60645900"
        }
    ],
    "category": [
        {
            "id": "urn:ngsi-ld:category:da621401-9f17-477b-aa63-103a4b1543aa",
            "href": "urn:ngsi-ld:category:da621401-9f17-477b-aa63-103a4b1543aa"
        },
        {
            "id": "urn:ngsi-ld:category:2abfaba5-76c7-4cc1-85dc-03bc66e905f1",
            "href": "urn:ngsi-ld:category:2abfaba5-76c7-4cc1-85dc-03bc66e905f1"
        },
        {
            "id": "urn:ngsi-ld:category:bbd43390-5314-46a9-87ec-63599a859acd",
            "href": "urn:ngsi-ld:category:bbd43390-5314-46a9-87ec-63599a859acd"
        }
    ],
    "productOfferingPrice": [
        {
            "id": "urn:ngsi-ld:product-offering-price:e3c1f9b0-9fad-4a20-a511-0412ec31de57",
            "href": "urn:ngsi-ld:product-offering-price:e3c1f9b0-9fad-4a20-a511-0412ec31de57"
        },
        {
            "id": "urn:ngsi-ld:product-offering-price:5f7abbc8-c183-40fc-83e1-ccb579e5dd25",
            "href": "urn:ngsi-ld:product-offering-price:5f7abbc8-c183-40fc-83e1-ccb579e5dd25"
        },
        {
            "id": "urn:ngsi-ld:product-offering-price:b8453cc6-f87f-4244-94fe-c08095788c48",
            "href": "urn:ngsi-ld:product-offering-price:b8453cc6-f87f-4244-94fe-c08095788c48"
        },
        {
            "id": "urn:ngsi-ld:product-offering-price:a792c978-a965-4f49-9df1-772aa787228a",
            "href": "urn:ngsi-ld:product-offering-price:a792c978-a965-4f49-9df1-772aa787228a"
        }
    ],
    "validFor": {
        "startDateTime": "2025-01-15T11:46:56.446Z"
    },
    "productOfferingTerm": [
        {
            "name": "",
            "description": "",
            "validFor": {}
        }
    ]
}`

func main() {

	remote_url := "https://dome-marketplace-dev2.org/catalog/productOffering/urn:ngsi-ld:product-offering:54a590bd-db5d-4ae9-8fa6-80a27d741342"

	req_body := bytes.NewReader([]byte(obj))

	// Send the request to the server
	httpclient := http.Client{}

	req, err := http.NewRequest("PATCH", remote_url, req_body)
	if err != nil {
		panic(err)
	}

	req.Header.Set("X-Organization", "VATES-B60645900")
	req.Header.Set("Authorization", "Bearer "+auth_token)
	// req.Header.Set("Cookie", cookie)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")

	res, err := httpclient.Do(req)
	if err != nil {
		slog.Error("sending request", "object", remote_url, slogor.Err(err))
		panic(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		slog.Error("retrieving object", "status code", res.StatusCode)
		fmt.Println(string(body))
		os.Exit(1)
	}
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	var oMap = map[string]any{}
	err = json.Unmarshal(body, &oMap)
	if err != nil {
		panic(err)
	}
	out, err := json.MarshalIndent(oMap, "", "   ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(out))

}
