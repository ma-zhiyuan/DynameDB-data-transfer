#-*-coding:utf8-*-

from __future__ import print_function # Python 2/3 compatibility
import boto3
import time
import json
import decimal
import datetime
import json
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

class DetailInfoOnline(object):

    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name)

    def query_item(self, docid):
        try:
            response = self._table.query(
                    KeyConditionExpression=Key('id').eq(docid)
                    )  
            if len(response) > 0:
                return response['Items']
            else:
                return []
        except Exception,e:
            print (str(e))
            return []

    def scan_all_with_wait(self):
        try:
            pe = "id, info"
            response = self._table.scan(
                    ProjectionExpression=pe,
                    )
            result = []
            while "Items" in response and 0 < len(response["Items"]):
                if 0 != len(response["Items"]):
                    result += response["Items"]
                if "LastEvaluatedKey" not in response:
                    break
                lek = response["LastEvaluatedKey"]
                response = self._table.scan(
                        ProjectionExpression=pe,
                        ExclusiveStartKey=lek
                        )
                time.sleep(2)
                print(len(result))
                #break
            return result
        except Exception, e:
            print (str(e))
            return []
        
    def scan_all(self):
        try:
            pe = "id, info"
            response = self._table.scan(
                    ProjectionExpression=pe,
                    )
            result = []
            while "Items" in response and 0 < len(response["Items"]):
                if 0 != len(response["Items"]):
                    result += response["Items"]
                if "LastEvaluatedKey" not in response:
                    break
                lek = response["LastEvaluatedKey"]
                print(lek)
                response = self._table.scan(
                        ProjectionExpression=pe,
                        ExclusiveStartKey=lek
                        )
            return result
        except Exception, e:
            print (str(e))
            return []       

    def query_mul_item(self, docid_list):
        try:
            fe = Attr("id").is_in(docid_list)
            response = self._table.scan(
                FilterExpression=fe
                )
            result = []
            while "Items" in response and 0 < len(response["Items"]):
                if 0 != len(response["Items"]):
                    result += response["Items"]
                if "LastEvaluatedKey" not in response:
                    break
                lek = response['LastEvaluatedKey']
                response = self._table.scan(
                        FilterExpression=fe,
                        ExclusiveStartKey=lek
                )
            return result
        except Exception,e:
            print(str(e))
            return []                
            
if __name__ =="__main__":
    print("start...")
    data_obj = DetailInfoOnline('dynamodb', 'ap-northeast-2', "https://dynamodb.ap-northeast-2.amazonaws.com", 'RelevantRecommendation')
    res = data_obj.scan_all()
    print(len(res))
    print(res[0])
    print("end...")