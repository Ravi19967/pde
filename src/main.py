from typing import List
from sqlalchemy import create_engine

import datetime
import json
import pandas as pd
import pydantic as pyd
import requests

engine = create_engine('postgresql://postgres:my_password@db:5432/my_database')

input_path_users = 'https://raw.githubusercontent.com/perseusEngineering/candidate-coding-challenges/master/data-engineer/users.json'
input_path_certificates = 'https://raw.githubusercontent.com/perseusEngineering/candidate-coding-challenges/master/data-engineer/certificates.json'
input_path_courses = 'https://raw.githubusercontent.com/perseusEngineering/candidate-coding-challenges/master/data-engineer/courses.json'

output_path_users = 'users'
output_path_certificates = 'certificates'
output_path_courses = 'courses'

users_df_columns = ['user_id','email','firstName','lastName']
certificates_df_columns = ['course_id','user_id','completedDate','startDate']
courses_df_columns = ['course_id','title','description','publishedAt']

class users(pyd.BaseModel):
    id: str
    email: str
    firstName: str
    lastName: str

class courses(pyd.BaseModel):
    id: str
    title: str
    description: str
    publishedAt: datetime.datetime

class certificates(pyd.BaseModel):
    course: str
    user: str
    completedDate: datetime.datetime
    startDate: datetime.datetime

## ETL Class interface
class _ingress_data_interface(): 
    def __init__(self, input, output):
        self._input_path = input
        self._output_path = output

    def read_data(self):
        raise NotImplementedError("read_data method is not implemented")

    def transform_data(self):
        raise NotImplementedError("transform_data method is not implemented")

    def write_data(self):
        raise NotImplementedError("write_data method is not implemented")


## JSON ETL Class
class json_ingress(_ingress_data_interface):
    def __init__(self, input, output, json_schema, columns):
        self.json_schema = json_schema
        self.columns = columns
        super().__init__(input, output)
    
    def get_data(self):
        r = requests.get(self._input_path)
        self._json_data = r.content
            
    
    def transform_data(self):
        try:
            json_item = json.loads(self._json_data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON: {exc.msg}, line {exc.lineno}, column {exc.colno}")

        try:
            self._pyd_output = pyd.parse_obj_as(List[self.json_schema], json_item)
        except pyd.ValidationError as exc:
            raise ValueError(f"ERROR: Invalid schema: {exc}")

    def write_data(self):
        data = pd.DataFrame([dict(s) for s in self._pyd_output])
        data.columns = self.columns
        data.to_sql(self._output_path, engine, if_exists='append', index=False)


def process_data(input_path: str, output_path: str, JSON_Schema, columns: List[str]):
    worker_etl = json_ingress(input_path, output_path, JSON_Schema, columns)
    worker_etl.get_data()
    worker_etl.transform_data()
    worker_etl.write_data()

def main():
    process_data(input_path_users, output_path_users, users, users_df_columns)
    process_data(input_path_courses, output_path_courses, courses, courses_df_columns)
    process_data(input_path_certificates, output_path_certificates, certificates, certificates_df_columns)

if __name__ == main():
    main()