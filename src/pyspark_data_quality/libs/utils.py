import json
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import boto3

class ssm_client:
    def __init__(self) -> None:
        self.ssm_client = boto3.client("ssm", region_name='ap-northeast-1')

    def get_parameter_value(self, 
                            ssn_name: str, 
                            is_decryption: bool=True) -> str:
        response = self.ssm_client.get_parameter(
            Name=ssn_name,
            WithDecryption=is_decryption
        )
        corresponding_value = response['Parameter']['Value']
        
        return corresponding_value.replace('\"','')

class s3_client:
    def __init__(self):
        s3_session = boto3.session.Session(region_name='ap-northeast-1')
        self.s3_client = s3_session.client("s3", region_name='ap-northeast-1')
        
    def get_object_content(self,
                   bucket_name: str,
                   object_key_name: str) -> bytes:
        
        response = self.s3_client.get_object(
            Bucket=bucket_name, 
            Key=object_key_name
        )
        content = response["Body"].read()
        
        return content
    
    def save_to_s3(self,
                   data: Union[dict, List[dict]],
                   bucket_name: str,
                   object_key_name: str) -> None:
        
        response = self.s3_client.put_object(
            Body=json.dumps(data),
            Bucket=bucket_name,
            Key=object_key_name,
        )
        
        if response["ResponseMetadata"].get("HTTPStatusCode") != 200:
            raise RuntimeError('Upload data s3 have some problems!')
        
class Environment(Enum):
    develop = "develop"
    staging = "staging"
    production = "production"

    def __str__(self):
        return self.value
    
